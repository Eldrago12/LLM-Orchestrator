import os
import json
import asyncio
import pandas as pd
import requests
import re
import logging
import uuid
from threading import Thread
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import aio_pika
import google.generativeai as genai
from groq import AsyncGroq
import redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typing import Optional, List, Dict, Any, Union

# --- Configuration ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY_FALLBACK")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "YOUR_GROQ_API_KEY_FALLBACK")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RESULTS_QUEUE_NAME = "results_queue"
TASK_EXPIRY_SECONDS = 3600

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ORCHESTRATOR - %(levelname)s - %(message)s')
app = Flask(__name__)
UPLOAD_FOLDER = '/app/uploads'
ALLOWED_EXTENSIONS = {'csv'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True
    )
    redis_client.ping()
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except RedisConnectionError as e:
    logging.error(f"FATAL: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT} - {e}")
    redis_client = None

AVAILABLE_TASKS = {
    "data_cleaner_image": {
        "description": "Clean Data",
        "capabilities": [],
        "queue": "data_cleaner_queue"
    },
    "sentiment_analyzer_image": {
        "description": "Analyze Sentiment",
        "capabilities": [],
        "queue": "sentiment_analyzer_queue"
    }
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

async def async_call_groq_api(prompt_text, csv_data=None):
    if not GROQ_API_KEY or "FALLBACK" in GROQ_API_KEY: raise ValueError("Groq API Key not configured.")
    groq_client = AsyncGroq(api_key=GROQ_API_KEY)
    content_text = prompt_text
    if csv_data is not None:
        try: sample_data = csv_data.head(3).to_string(); content_text += f"\nCSV Data Sample:\n{sample_data}"
        except Exception: pass
    logging.info("Calling Groq API...")
    try:
        chat_completion = await groq_client.chat.completions.create(
            messages=[ {"role": "system", "content": "Respond ONLY in the requested JSON format."}, {"role": "user", "content": content_text}],
            model="llama3-8b-8192", temperature=0.2)
        content = chat_completion.choices[0].message.content
        logging.info("Groq API response received.")
        return content
    except Exception as e: logging.error(f"Error calling Groq API: {e}"); raise

def call_gemini_api(prompt_text, csv_data=None):
    if not GEMINI_API_KEY or "FALLBACK" in GEMINI_API_KEY: return json.dumps([])
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash-001')
    content_text = prompt_text
    if csv_data is not None:
        try: sample_data = csv_data.head(5).to_string(); content_text += f"\n\nHere's a sample of the CSV data:\n{sample_data}"
        except Exception as e: logging.warning(f"Could not sample CSV data for Gemini: {e}")
    logging.info("Calling Gemini API for cleaning instructions...")
    try:
        cleaning_prompt = f"Analyze dataset sample and user request '{user_request}'. Suggest cleaning steps as JSON array (action, parameters). Example: [{{'action': 'drop_duplicates', 'parameters': {{'subset': ['colA']}} }}]. Return [] if none needed." # Concise prompt
        response = model.generate_content(cleaning_prompt)
        logging.info("Gemini API response received.")
        return response.text
    except Exception as e: logging.error(f"Error calling Gemini API: {e}"); return json.dumps([])

# --- Core Logic ---
async def decide_and_prepare_tasks(user_request: str, csv_data: Optional[pd.DataFrame] = None, filename: Optional[str] = None) -> List[Dict[str, Any]]:
    containers_info = "\n".join([f"- {name}: {d['description']} ({', '.join(d['capabilities'])})" for name, d in AVAILABLE_TASKS.items()])
    task_selection_prompt = f"""
User Request: "{user_request}"
Available Processing Tools:
{containers_info}
Return *only* a single JSON object: {{"container": "[image_name]", "task_description": "[desc]", "parameters": {{...}} }}
# Include input_data for sentiment, check cleaning_instructions for data_cleaner
"""
    if csv_data is not None: task_selection_prompt += "\nNote: A CSV file has been provided."
    llm_response_raw = await async_call_groq_api(task_selection_prompt)
    logging.debug(f"Raw Groq Response for task decision: {llm_response_raw}")
    try: decision = json.loads(llm_response_raw); container_decisions = [decision]
    except Exception as e: logging.error(f"Error parsing Groq JSON: {e}"); container_decisions = []
    if not container_decisions: return []

    tasks_to_dispatch = []
    decision = container_decisions[0]
    task_name = decision.get("container")
    task_info = AVAILABLE_TASKS.get(task_name)
    if not task_info or not task_info.get("queue"): return []

    task_id = str(uuid.uuid4())
    logging.info(f"Generated Task ID: {task_id}")

    task_payload = { "task_id": task_id, "task_description": decision.get("task_description", task_info["description"]), **decision.get("parameters", {}) }

    if task_name == "data_cleaner_image":
        if not filename: return []
        task_payload["input_file"] = filename
        if "cleaning_instructions" not in task_payload or not task_payload.get("cleaning_instructions"):
             logging.info("Querying Gemini for cleaning instructions...")
             cleaning_prompt = f"Analyze dataset sample and user request '{user_request}'. Suggest cleaning steps as JSON array (action, parameters). Example: [{{'action': 'drop_duplicates', 'parameters': {{'subset': ['colA']}} }}]. Return [] if none needed."
             gemini_response_text = call_gemini_api(cleaning_prompt, csv_data)
             try:
                 match = re.search(r'```json\s*(\[.*?\])\s*```', gemini_response_text, re.DOTALL)
                 task_payload["cleaning_instructions"] = json.loads(match.group(1)) if match else json.loads(gemini_response_text)
                 if not isinstance(task_payload["cleaning_instructions"], list): task_payload["cleaning_instructions"] = []
             except (json.JSONDecodeError, TypeError): task_payload["cleaning_instructions"] = []
        if "cleaning_instructions" not in task_payload: task_payload["cleaning_instructions"] = []

    elif task_name == "sentiment_analyzer_image":
        if "input_data" not in task_payload or not task_payload["input_data"]: task_payload["input_data"] = user_request
    if redis_client:
        try:
            initial_status = {"status": "pending", "result": None, "error": None}
            redis_client.set(task_id, json.dumps(initial_status), ex=TASK_EXPIRY_SECONDS)
            logging.info(f"Initialized status for Task ID {task_id} in Redis.")
        except RedisConnectionError as redis_err:
             logging.error(f"Redis connection error initializing task {task_id}: {redis_err}")
        except Exception as e:
             logging.error(f"Error initializing task {task_id} in Redis: {e}")
    else:
         logging.warning("Redis client not available. Cannot track task status.")


    tasks_to_dispatch.append({
        "task_id": task_id,
        "queue": task_info["queue"],
        "message_body": json.dumps(task_payload).encode()
    })
    return tasks_to_dispatch


async def publish_tasks_to_workers(tasks_to_dispatch):
    connection = None; published_count = 0
    if not tasks_to_dispatch: return 0
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", timeout=30)
        async with connection:
            channel = await connection.channel(); declared_queues = set()
            for task_info in tasks_to_dispatch:
                queue_name = task_info["queue"]
                message_body = task_info["message_body"]
                if queue_name not in declared_queues: await channel.declare_queue(queue_name, durable=True); declared_queues.add(queue_name)
                message = aio_pika.Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
                await channel.default_exchange.publish(message, routing_key=queue_name)
                logging.info(f"Published Task ID {task_info['task_id']} to queue '{queue_name}'.")
                published_count += 1
            logging.info(f"Successfully published {published_count} task(s).")
            return published_count
    except aio_pika.exceptions.AMQPConnectionError as e: logging.error(f"RabbitMQ Connection Error: {e}. Task(s) not published."); raise
    except Exception as e: logging.error(f"Error publishing task(s) via RabbitMQ: {str(e)}"); raise


async def listen_for_results():
    loop = asyncio.get_running_loop()
    connection = None
    logging.info("[ResultListener] Starting...")
    while True:
        if redis_client is None:
             logging.error("[ResultListener] Redis client not available. Listener stopped.")
             break
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", loop=loop, timeout=30)
            logging.info("[ResultListener] Connected to RabbitMQ.")
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(RESULTS_QUEUE_NAME, durable=True)
                logging.info(f"[ResultListener] Waiting for messages on '{RESULTS_QUEUE_NAME}'.")
                async for message in queue:
                    async with message.process(ignore_processed=True):
                        task_id = None
                        try:
                            result_data = json.loads(message.body.decode())
                            task_id = result_data.get("task_id")
                            result = result_data.get("result")
                            error = result_data.get("error")

                            if task_id:
                                if error:
                                    new_status = {"status": "error", "error": error}
                                    log_level = logging.ERROR
                                else:
                                    new_status = {"status": "completed", "result": result}
                                    log_level = logging.INFO

                                try:
                                    redis_client.set(task_id, json.dumps(new_status), ex=TASK_EXPIRY_SECONDS)
                                    logging.log(log_level, f"[ResultListener] Updated status for Task ID {task_id} in Redis.")
                                except RedisConnectionError as redis_err:
                                    logging.error(f"[ResultListener] Redis connection error updating {task_id}: {redis_err}")
                                except Exception as e:
                                    logging.error(f"[ResultListener] Failed updating Redis for {task_id}: {e}")
                            else:
                                logging.warning("[ResultListener] Received result message without Task ID.")

                        except json.JSONDecodeError: logging.error("[ResultListener] Failed to decode result message body.")
                        except Exception as e: logging.exception(f"[ResultListener] Error processing result message: {e}")

        except aio_pika.exceptions.AMQPConnectionError as e: logging.error(f"[ResultListener] Connection error: {e}. Retrying...")
        except Exception as e: logging.exception(f"[ResultListener] Unexpected error: {e}. Retrying...")
        finally:
            if connection and not connection.is_closed: await connection.close()
            await asyncio.sleep(10)


def run_result_listener_in_thread():
    logging.info("Starting result listener thread.")
    try:
        asyncio.run(listen_for_results())
    except Exception as e:
         logging.exception("Result listener thread encountered fatal error.")

@app.route('/orchestrate', methods=['POST'])
def orchestrate():
    user_request = request.form.get("request", "")
    if not user_request: return jsonify({"error": "No request text provided"}), 400
    logging.info(f"Received request: {user_request[:100]}...")
    csv_data = None; filename = None; file_path = None
    if 'file' in request.files:
        file = request.files['file']
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            try: file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename); file.save(file_path); csv_data = pd.read_csv(file_path)
            except Exception as e: return jsonify({"error": f"Error processing CSV: {e}"}), 500
        elif file and file.filename: return jsonify({"error": "Invalid file type."}), 400

    task_id_generated = None
    message = "Request processing failed."
    status_code = 500

    if redis_client is None:
        return jsonify({"error": "Result storage (Redis) is not available."}), 503

    try:
        tasks_to_dispatch = asyncio.run(decide_and_prepare_tasks(user_request, csv_data, filename))

        if tasks_to_dispatch:
            task_info = tasks_to_dispatch[0]
            task_id_generated = task_info["task_id"]
            published_count = asyncio.run(publish_tasks_to_workers(tasks_to_dispatch))
            if published_count > 0:
                message = f"{published_count} task(s) dispatched successfully."
                status_code = 202
            else:
                 message = "Task identified, but failed to publish to queue."
                 try: redis_client.delete(task_id_generated)
                 except: pass
                 task_id_generated = None
        else:
            message = "Request received, but no processing task identified or prepared."
            status_code = 202

    except ValueError as e: message = f"Configuration error: {e}"; status_code = 500
    except Exception as e: logging.error(f"Error during task decision/dispatch: {e}"); message = f"Failed to process request: {e}"; status_code = 500

    response_data = { "message": message, "task_id": task_id_generated }
    if task_id_generated is None: response_data["task_id"] = None
    return jsonify(response_data), status_code


@app.route('/results/<task_id>', methods=['GET'])
def get_result(task_id):
    logging.info(f"Received request for result of Task ID: {task_id}")
    if redis_client is None:
        return jsonify({"error": "Result storage (Redis) is not available."}), 503

    try:
        result_json = redis_client.get(task_id)
        if result_json:
            result_info = json.loads(result_json)
            response = {
                "task_id": task_id,
                "status": result_info.get("status", "unknown")
            }
            if result_info.get("status") == "completed":
                response["result"] = result_info.get("result")
            elif result_info.get("status") == "error":
                response["error"] = result_info.get("error")
            return jsonify(response), 200
        else:
            return jsonify({"task_id": task_id, "status": "not_found", "error": "Task ID not found (or expired)."}), 404
    except RedisConnectionError as redis_err:
         logging.error(f"Redis connection error fetching result for {task_id}: {redis_err}")
         return jsonify({"error": "Could not connect to result store"}), 503
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON data from Redis for task {task_id}.")
        return jsonify({"task_id": task_id, "status": "error", "error": "Failed to decode stored result."}), 500
    except Exception as e:
         logging.error(f"Error fetching result for {task_id} from Redis: {e}")
         return jsonify({"error": "Failed to retrieve result"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    redis_status = "unhealthy"
    if redis_client:
        try:
            if redis_client.ping(): redis_status = "healthy"
        except RedisConnectionError: pass
    return jsonify({"status": "healthy", "redis_status": redis_status}), 200 if redis_status == "healthy" else 503

if __name__ == "__main__":
    logging.info("Starting Orchestrator Flask App with Redis support...")
    if GEMINI_API_KEY and "FALLBACK" not in GEMINI_API_KEY: genai.configure(api_key=GEMINI_API_KEY); logging.info("Gemini API configured.")
    else: logging.warning("Gemini API Key not configured.")
    if not GROQ_API_KEY or "FALLBACK" in GROQ_API_KEY: logging.warning("Groq API Key not configured.")
    else: logging.info("Groq API Key found.")

    if redis_client:
        listener_thread = Thread(target=run_result_listener_in_thread, daemon=True)
        listener_thread.start()
    else:
         logging.warning("Redis not connected. Result listener thread NOT started.")

    app.run(host="0.0.0.0", port=8080, debug=os.environ.get("FLASK_DEBUG", False))
