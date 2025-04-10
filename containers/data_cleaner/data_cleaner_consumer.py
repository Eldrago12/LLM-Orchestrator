# containers/data_cleaner/data_cleaner_consumer.py
# MODIFIED: To publish results back to results_queue

import os
import json
import asyncio
import aio_pika
import subprocess
import logging
import uuid
from typing import Optional, Dict, Any, Union # Just in case task_id is missing, though unlikely

logging.basicConfig(level=logging.INFO, format='%(asctime)s - CLEANER_CONSUMER - %(levelname)s - %(message)s')
UPLOAD_DIR = '/app/uploads'
QUEUE_NAME = "data_cleaner_queue"
CLEANER_SCRIPT_PATH = "/app/cleaner.py"
RESULTS_QUEUE_NAME = "results_queue" # Target queue for results

# --- Result Publishing ---
async def publish_result(channel: aio_pika.Channel, task_id: str, result: Optional[str] = None, error: Optional[str] = None):
    """Publishes the task result or error back to the results queue."""
    result_payload = {
        "task_id": task_id,
        "result": result,
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


# (Keep run_cleaner_script function as before)
def run_cleaner_script(input_file_name, cleaning_instructions_list):
    # ... (implementation from previous answer) ...
    input_file_path = os.path.join(UPLOAD_DIR, input_file_name)
    logging.info(f"Executing cleaner script for: {input_file_path}")
    cleaning_instructions_str = json.dumps(cleaning_instructions_list)
    command = ["python", CLEANER_SCRIPT_PATH, "--input_file", input_file_path, "--cleaning_instructions", cleaning_instructions_str]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=600)
        if result.stderr: logging.warning(f"cleaner.py stderr:\n{result.stderr}")
        if result.returncode != 0:
            logging.error(f"cleaner.py failed (code {result.returncode}).")
            try: error_message = json.loads(result.stdout).get("error", result.stdout[:500])
            except: error_message = result.stdout[:500] or "Unknown script error"
            raise RuntimeError(f"Cleaner script failed: {error_message}")
        logging.info("cleaner.py executed successfully.")
        return result.stdout # Return JSON string output
    except subprocess.TimeoutExpired: raise TimeoutError("Cleaner script timed out.")
    except Exception as e: logging.exception("Error running cleaner subprocess."); raise

# --- RabbitMQ Message Handling ---
async def on_message(message: aio_pika.IncomingMessage, result_channel: aio_pika.Channel):
    """Processes message and publishes result."""
    task_id = None # Ensure task_id is defined for finally block
    try:
        async with message.process(requeue=False): # Auto-ack
            task_payload = json.loads(message.body.decode())
            task_id = task_payload.get("task_id", f"unknown_{uuid.uuid4()}") # Get task_id
            logging.info(f"Received task ID {task_id}: {task_payload.get('task_description', 'N/A')}")

            input_file = task_payload.get("input_file")
            cleaning_instructions = task_payload.get("cleaning_instructions", [])
            if not input_file: raise ValueError("Task missing 'input_file'")

            # --- Execute Cleaning ---
            cleaned_json_output = run_cleaner_script(input_file, cleaning_instructions)
            logging.info(f"Task ID {task_id} completed. Output length: {len(cleaned_json_output)}")

            # --- Publish Success Result ---
            # Send the JSON string as the result (or parse/reformat if needed)
            await publish_result(result_channel, task_id, result=cleaned_json_output)

    # --- Error Handling and Publishing Error Result ---
    except (json.JSONDecodeError, ValueError) as e: # Bad message or missing fields
        logging.error(f"Data/Message Error for Task ID {task_id or 'N/A'}: {e}. Discarding.")
        if task_id: await publish_result(result_channel, task_id, error=f"Bad task data: {e}")
    except FileNotFoundError as e: # Input file missing
         logging.error(f"Input file error for Task ID {task_id}: {e}. Reporting error.")
         await publish_result(result_channel, task_id, error=f"Input file not found: {e}")
    except RuntimeError as e: # Script failed
        logging.error(f"Script execution failed for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except TimeoutError as e:
        logging.error(f"Script timed out for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except Exception as e: # Catch-all for unexpected errors
        logging.exception(f"Unhandled error processing Task ID {task_id}. Reporting error.")
        if task_id: await publish_result(result_channel, task_id, error=f"Unhandled worker error: {e}")


# --- Main Execution Loop ---
async def main():
    logging.info(f"Cleaner Consumer starting. Listening on '{QUEUE_NAME}', publishing to '{RESULTS_QUEUE_NAME}'.")
    loop = asyncio.get_event_loop()
    connection = None
    while True:
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", loop=loop, timeout=30)
            logging.info("Connected to RabbitMQ.")
            async with connection:
                # Channel for consuming tasks
                consume_channel = await connection.channel()
                await consume_channel.set_qos(prefetch_count=1) # One clean task at a time
                consume_queue = await consume_channel.declare_queue(QUEUE_NAME, durable=True)

                # Channel for publishing results (can use same connection)
                publish_channel = await connection.channel()
                await publish_channel.declare_queue(RESULTS_QUEUE_NAME, durable=True) # Ensure results queue exists

                logging.info(" [*] Waiting for cleaning tasks...")
                # Pass the publish_channel to the callback
                await consume_queue.consume(lambda msg: on_message(msg, publish_channel))
                await asyncio.Future() # Keep running
        # (Keep connection error handling and retry loop as before)
        except aio_pika.exceptions.AMQPConnectionError as e: logging.error(f"RabbitMQ connection error: {e}. Retrying..."); await asyncio.sleep(10)
        except KeyboardInterrupt: logging.info("CTRL+C pressed. Shutting down."); break
        except Exception as e: logging.exception(f"Unexpected error in main loop."); await asyncio.sleep(15)
        finally:
            if connection and not connection.is_closed: await connection.close(); logging.info("RabbitMQ connection closed.")

    logging.info("Cleaner Consumer stopped.")

if __name__ == "__main__":
    asyncio.run(main())
