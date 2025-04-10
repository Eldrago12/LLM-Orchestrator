# containers/sentiment_analyzer/sentiment_analyzer_consumer.py
# MODIFIED: To publish results back to results_queue

import os
import json
import asyncio
import aio_pika
import subprocess
import logging
import uuid
from typing import Optional, Dict, Any, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ANALYZER_CONSUMER - %(levelname)s - %(message)s')
QUEUE_NAME = "sentiment_analyzer_queue"
ANALYZER_SCRIPT_PATH = "/app/analyzer.py"
RESULTS_QUEUE_NAME = "results_queue" # Target queue for results

# --- Result Publishing ---
async def publish_result(channel: aio_pika.Channel, task_id: str, result: Optional[str] = None, error: Optional[str] = None):
    """Publishes the task result or error back to the results queue."""
    result_payload = {
        "task_id": task_id,
        "result": result, # Should be the analysis string
        "error": error
    }
    try:
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(result_payload).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=RESULTS_QUEUE_NAME
        )
        status = "error" if error else "result"
        logging.info(f"Published {status} for Task ID {task_id} to '{RESULTS_QUEUE_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to publish result/error for Task ID {task_id}: {e}")

# (Keep run_analyzer_script function as before)
def run_analyzer_script(input_text):
    # ... (implementation from previous answer) ...
    logging.info(f"Executing analyzer script...")
    command = ["python", ANALYZER_SCRIPT_PATH]
    script_env = os.environ.copy(); script_env["INPUT_DATA"] = input_text
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, env=script_env, timeout=180)
        if result.stderr: logging.warning(f"analyzer.py stderr:\n{result.stderr}")
        if result.returncode != 0:
            logging.error(f"analyzer.py failed (code {result.returncode}).")
            error_message = (result.stdout.strip().splitlines() or ["Unknown script error"])[0]
            raise RuntimeError(f"Analyzer script failed: {error_message}")
        logging.info("analyzer.py executed successfully.")
        return result.stdout.strip() # Return result string
    except subprocess.TimeoutExpired: raise TimeoutError("Analyzer script timed out.")
    except Exception as e: logging.exception("Error running analyzer subprocess."); raise

# --- RabbitMQ Message Handling ---
async def on_message(message: aio_pika.IncomingMessage, result_channel: aio_pika.Channel):
    """Processes message and publishes result."""
    task_id = None
    try:
        async with message.process(requeue=False): # Auto-ack
            task_payload = json.loads(message.body.decode())
            task_id = task_payload.get("task_id", f"unknown_{uuid.uuid4()}")
            logging.info(f"Received task ID {task_id}: {task_payload.get('task_description', 'N/A')}")

            input_data = task_payload.get("input_data")
            if not input_data: raise ValueError("Task missing 'input_data'")

            # --- Execute Analysis ---
            analysis_result_string = run_analyzer_script(input_data)
            logging.info(f"Task ID {task_id} completed. Result: {analysis_result_string}")

            # --- Publish Success Result ---
            # Check if the result indicates an internal script error
            if "Error:" in analysis_result_string:
                 await publish_result(result_channel, task_id, error=analysis_result_string)
            else:
                 await publish_result(result_channel, task_id, result=analysis_result_string)

    # --- Error Handling and Publishing Error Result ---
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Data/Message Error for Task ID {task_id or 'N/A'}: {e}. Discarding.")
        if task_id: await publish_result(result_channel, task_id, error=f"Bad task data: {e}")
    except RuntimeError as e: # Script failed
        logging.error(f"Script execution failed for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except TimeoutError as e:
        logging.error(f"Script timed out for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except Exception as e:
        logging.exception(f"Unhandled error processing Task ID {task_id}. Reporting error.")
        if task_id: await publish_result(result_channel, task_id, error=f"Unhandled worker error: {e}")

# --- Main Execution Loop ---
async def main():
    logging.info(f"Analyzer Consumer starting. Listening on '{QUEUE_NAME}', publishing to '{RESULTS_QUEUE_NAME}'.")
    loop = asyncio.get_event_loop()
    connection = None
    while True:
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", loop=loop, timeout=30)
            logging.info("Connected to RabbitMQ.")
            async with connection:
                consume_channel = await connection.channel()
                await consume_channel.set_qos(prefetch_count=5) # Allow concurrent analyses
                consume_queue = await consume_channel.declare_queue(QUEUE_NAME, durable=True)

                publish_channel = await connection.channel()
                await publish_channel.declare_queue(RESULTS_QUEUE_NAME, durable=True)

                logging.info(" [*] Waiting for analysis tasks...")
                await consume_queue.consume(lambda msg: on_message(msg, publish_channel))
                await asyncio.Future()
        # (Keep connection error handling and retry loop as before)
        except aio_pika.exceptions.AMQPConnectionError as e: logging.error(f"RabbitMQ connection error: {e}. Retrying..."); await asyncio.sleep(10)
        except KeyboardInterrupt: logging.info("CTRL+C pressed. Shutting down."); break
        except Exception as e: logging.exception(f"Unexpected error in main loop."); await asyncio.sleep(15)
        finally:
            if connection and not connection.is_closed: await connection.close(); logging.info("RabbitMQ connection closed.")

    logging.info("Analyzer Consumer stopped.")

if __name__ == "__main__":
    asyncio.run(main())
