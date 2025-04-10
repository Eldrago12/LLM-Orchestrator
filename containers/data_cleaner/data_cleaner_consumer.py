import os
import json
import asyncio
import aio_pika
import subprocess
import logging
import uuid
from typing import Optional, Dict, Any, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - CLEANER_CONSUMER - %(levelname)s - %(message)s')
UPLOAD_DIR = '/app/uploads'
QUEUE_NAME = "data_cleaner_queue"
CLEANER_SCRIPT_PATH = "/app/cleaner.py"
RESULTS_QUEUE_NAME = "results_queue"

async def publish_result(channel: aio_pika.Channel, task_id: str, result: Optional[str] = None, error: Optional[str] = None):
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

def run_cleaner_script(input_file_name, cleaning_instructions_list):
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
        return result.stdout
    except subprocess.TimeoutExpired: raise TimeoutError("Cleaner script timed out.")
    except Exception as e: logging.exception("Error running cleaner subprocess."); raise

async def on_message(message: aio_pika.IncomingMessage, result_channel: aio_pika.Channel):
    task_id = None
    try:
        async with message.process(requeue=False): # Auto-ack
            task_payload = json.loads(message.body.decode())
            task_id = task_payload.get("task_id", f"unknown_{uuid.uuid4()}")
            logging.info(f"Received task ID {task_id}: {task_payload.get('task_description', 'N/A')}")

            input_file = task_payload.get("input_file")
            cleaning_instructions = task_payload.get("cleaning_instructions", [])
            if not input_file: raise ValueError("Task missing 'input_file'")

            cleaned_json_output = run_cleaner_script(input_file, cleaning_instructions)
            logging.info(f"Task ID {task_id} completed. Output length: {len(cleaned_json_output)}")

            await publish_result(result_channel, task_id, result=cleaned_json_output)

    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Data/Message Error for Task ID {task_id or 'N/A'}: {e}. Discarding.")
        if task_id: await publish_result(result_channel, task_id, error=f"Bad task data: {e}")
    except FileNotFoundError as e:
         logging.error(f"Input file error for Task ID {task_id}: {e}. Reporting error.")
         await publish_result(result_channel, task_id, error=f"Input file not found: {e}")
    except RuntimeError as e:
        logging.error(f"Script execution failed for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except TimeoutError as e:
        logging.error(f"Script timed out for Task ID {task_id}: {e}. Reporting error.")
        await publish_result(result_channel, task_id, error=str(e))
    except Exception as e:
        logging.exception(f"Unhandled error processing Task ID {task_id}. Reporting error.")
        if task_id: await publish_result(result_channel, task_id, error=f"Unhandled worker error: {e}")


async def main():
    logging.info(f"Cleaner Consumer starting. Listening on '{QUEUE_NAME}', publishing to '{RESULTS_QUEUE_NAME}'.")
    loop = asyncio.get_event_loop()
    connection = None
    while True:
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", loop=loop, timeout=30)
            logging.info("Connected to RabbitMQ.")
            async with connection:
                consume_channel = await connection.channel()
                await consume_channel.set_qos(prefetch_count=1)
                consume_queue = await consume_channel.declare_queue(QUEUE_NAME, durable=True)
                publish_channel = await connection.channel()
                await publish_channel.declare_queue(RESULTS_QUEUE_NAME, durable=True)

                logging.info(" [*] Waiting for cleaning tasks...")
                await consume_queue.consume(lambda msg: on_message(msg, publish_channel))
                await asyncio.Future()
        except aio_pika.exceptions.AMQPConnectionError as e: logging.error(f"RabbitMQ connection error: {e}. Retrying..."); await asyncio.sleep(10)
        except KeyboardInterrupt: logging.info("CTRL+C pressed. Shutting down."); break
        except Exception as e: logging.exception(f"Unexpected error in main loop."); await asyncio.sleep(15)
        finally:
            if connection and not connection.is_closed: await connection.close(); logging.info("RabbitMQ connection closed.")

    logging.info("Cleaner Consumer stopped.")

if __name__ == "__main__":
    asyncio.run(main())
