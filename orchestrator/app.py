# import os
# import json
# import asyncio
# import requests
# from flask import Flask, request, jsonify
# import aio_pika
# import google.generativeai as genai
# from groq import AsyncGroq
# from config import GEMINI_API_KEY, GROQ_API_KEY

# app = Flask(__name__)

# async def async_call_groq_api(prompt_text):
#     groq_client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))
#     chat_completion = await groq_client.chat.completions.create(
#         messages=[
#             {"role": "user", "content": prompt_text}
#         ],
#         model="llama3-8b-8192",
#     )
#     content = chat_completion.choices[0].message.content
#     try:
#         tasks = json.loads(content)
#     except json.JSONDecodeError:
#         tasks = [{"description": content}]
#     return tasks

# def call_gemini_api(prompt_text):
#     model = genai.GenerativeModel('gemini-pro')
#     response = model.generate_content(prompt_text)
#     try:
#         tasks = json.loads(response)
#     except json.JSONDecodeError:
#         tasks = [{"description": response}]
#     return tasks

# def decide_tasks(user_request):
#     lower_req = user_request.lower()
#     if "csv" in lower_req or "data file" in lower_req:
#         # Use Gemini exclusively for data cleaning tasks when CSV-related input is detected.
#         return call_gemini_api(
#             f"Based on the request '{user_request}', generate tasks for cleaning CSV data files. "
#             "Return a JSON array where one task should be 'Clean Data'."
#         )
#     elif "sentiment" in lower_req:
#         # Use Groq for tasks that need low latency (like sentiment analysis tasks).
#         return asyncio.run(async_call_groq_api(
#             f"Based on the request '{user_request}', generate tasks for sentiment analysis. "
#             "Return a JSON array where one task should be 'Analyze Sentiment'."
#         ))
#     else:
#         # For other general requests, combine the outputs intelligently.
#         try:
#             tasks_groq = asyncio.run(async_call_groq_api(
#                 f"Generate a task list for the following request: '{user_request}'"
#             ))
#             tasks_gemini = call_gemini_api(
#                 f"Generate a task list for the following request: '{user_request}'"
#             )
#             # Example logic: if tasks from one API contain 'Clean Data', prefer that.
#             combined_tasks = []
#             for task in tasks_groq + tasks_gemini:
#                 desc = task.get("description", "").lower()
#                 if ("clean" in desc or "csv" in desc) and not any("Clean Data" in t.get("description", "") for t in combined_tasks):
#                     combined_tasks.append({"description": "Clean Data"})
#                 elif ("sentiment" in desc) and not any("Analyze Sentiment" in t.get("description", "") for t in combined_tasks):
#                     combined_tasks.append({"description": "Analyze Sentiment"})
#                 else:
#                     # Append any additional non-duplicate tasks
#                     if task not in combined_tasks:
#                         combined_tasks.append(task)
#             return combined_tasks
#         except Exception as e:
#             raise Exception(f"Error deciding tasks: {str(e)}")

# #########################################
# # RabbitMQ Publisher function
# #########################################

# async def publish_tasks(tasks):
#     """
#     Publishes each task as a message to RabbitMQ.
#     """
#     connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
#     async with connection:
#         channel = await connection.channel()
#         queue = await channel.declare_queue("task_queue", durable=True)
#         for task in tasks:
#             message_body = json.dumps(task).encode()
#             message = aio_pika.Message(
#                 body=message_body,
#                 delivery_mode=aio_pika.DeliveryMode.PERSISTENT
#             )
#             await channel.default_exchange.publish(message, routing_key="task_queue")
#     print("Published all tasks to RabbitMQ.")

# #########################################
# # Flask Route for Orchestration
# #########################################

# @app.route('/orchestrate', methods=['POST'])
# def orchestrate():
#     """
#     Receives a user's request, applies decision logic to determine tasks,
#     and publishes them to the message queue.
#     """
#     data = request.get_json()
#     user_request = data.get("request", "")
#     if not user_request:
#         return jsonify({"error": "No request provided"}), 400

#     prompt = (
#         f"Parse the following request into tasks with descriptions: '{user_request}'. "
#         "Return a JSON array of tasks, each containing at least a 'description' field."
#     )

#     try:
#         # Use decision logic to select tasks:
#         tasks = decide_tasks(user_request)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

#     # Publish tasks asynchronously to RabbitMQ
#     try:
#         asyncio.run(publish_tasks(tasks))
#     except Exception as e:
#         return jsonify({"error": f"Error publishing tasks: {str(e)}"}), 500

#     return jsonify({
#         "message": "Tasks have been published to the queue.",
#         "tasks": tasks
#     }), 200


# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=8080)






# Main code

# import os
# import json
# import asyncio
# import pandas as pd
# import requests
# from flask import Flask, request, jsonify, send_file
# from werkzeug.utils import secure_filename
# import aio_pika
# import google.generativeai as genai
# from groq import AsyncGroq
# from config import GEMINI_API_KEY, GROQ_API_KEY

# app = Flask(__name__)
# UPLOAD_FOLDER = 'uploads'
# os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# ALLOWED_EXTENSIONS = {'csv'}

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# async def async_call_groq_api(prompt_text, csv_data=None):
#     groq_client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))

#     content_text = prompt_text
#     if csv_data is not None:
#         # Add the first few rows of the CSV to the prompt
#         sample_data = csv_data.head(5).to_string()
#         content_text = f"{prompt_text}\n\nHere's a sample of the CSV data:\n{sample_data}"

#     chat_completion = await groq_client.chat.completions.create(
#         messages=[
#             {"role": "user", "content": content_text}
#         ],
#         model="llama3-8b-8192",
#     )
#     content = chat_completion.choices[0].message.content
#     try:
#         tasks = json.loads(content)
#     except json.JSONDecodeError:
#         tasks = [{"description": content}]
#     return tasks

# def call_gemini_api(prompt_text, csv_data=None):
#     model = genai.GenerativeModel('gemini-1.5-flash-001')

#     content_text = prompt_text
#     if csv_data is not None:
#         # Add the first few rows of the CSV to the prompt
#         sample_data = csv_data.head(5).to_string()
#         content_text = f"{prompt_text}\n\nHere's a sample of the CSV data:\n{sample_data}"

#     response = model.generate_content(content_text)
#     try:
#         tasks = json.loads(response.text)
#     except (json.JSONDecodeError, AttributeError):
#         # Handle both JSON decode errors and potential attribute errors
#         if hasattr(response, 'text'):
#             tasks = [{"description": response.text}]
#         else:
#             tasks = [{"description": str(response)}]
#     return tasks

# def clean_csv_data(df, cleaning_instructions=None):
#     """
#     Clean the CSV data based on general best practices or specific instructions
#     """
#     # Make a copy to avoid modifying the original
#     cleaned_df = df.copy()

#     # Basic cleaning operations
#     # 1. Drop duplicates
#     cleaned_df = cleaned_df.drop_duplicates()

#     # 2. Handle missing values (fill with mean for numeric, mode for categorical)
#     for col in cleaned_df.columns:
#         if cleaned_df[col].dtype.kind in 'ifc':  # numeric columns
#             cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mean())
#         else:  # categorical columns
#             cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mode()[0] if not cleaned_df[col].mode().empty else "Unknown")

#     # 3. Strip whitespace from string columns
#     for col in cleaned_df.select_dtypes(include=['object']).columns:
#         cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

#     # 4. Convert date columns if detected
#     for col in cleaned_df.columns:
#         if 'date' in col.lower():
#             try:
#                 cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='ignore')
#             except:
#                 pass

#     # Apply custom cleaning based on AI recommendations if provided
#     if cleaning_instructions:
#         # This would be expanded based on the AI model's recommendations
#         # For now, we'll just log that we would apply them
#         print(f"Would apply custom cleaning: {cleaning_instructions}")

#     return cleaned_df

# def decide_tasks(user_request, csv_data=None):
#     lower_req = user_request.lower()

#     if "csv" in lower_req or "data file" in lower_req or "clean" in lower_req:
#         # Use Gemini exclusively for data cleaning tasks when CSV-related input is detected.
#         cleaning_prompt = (
#             f"Based on the request '{user_request}', provide specific instructions for cleaning a CSV dataset. "
#             "Focus on handling missing values, outliers, duplicates, and data formatting. "
#             "Return a JSON array of cleaning tasks with 'action' and 'reason' fields."
#         )

#         if csv_data is not None:
#             # Get cleaning recommendations tailored to the specific dataset
#             return call_gemini_api(cleaning_prompt, csv_data)
#         else:
#             # Generic cleaning recommendations without data
#             return call_gemini_api(cleaning_prompt)

#     elif "sentiment" in lower_req:
#         # Use Groq for tasks that need low latency (like sentiment analysis tasks).
#         sentiment_prompt = (
#             f"Based on the request '{user_request}', generate tasks for sentiment analysis. "
#             "Return a JSON array where one task should be 'Analyze Sentiment'."
#         )
#         return asyncio.run(async_call_groq_api(sentiment_prompt, csv_data))
#     else:
#         # For other general requests, combine the outputs intelligently.
#         try:
#             tasks_groq = asyncio.run(async_call_groq_api(
#                 f"Generate a task list for the following request: '{user_request}'", csv_data
#             ))
#             tasks_gemini = call_gemini_api(
#                 f"Generate a task list for the following request: '{user_request}'", csv_data
#             )

#             # Example logic: if tasks from one API contain 'Clean Data', prefer that.
#             combined_tasks = []
#             for task in tasks_groq + tasks_gemini:
#                 desc = task.get("description", "").lower()
#                 if ("clean" in desc or "csv" in desc) and not any("Clean Data" in t.get("description", "") for t in combined_tasks):
#                     combined_tasks.append({"description": "Clean Data"})
#                 elif ("sentiment" in desc) and not any("Analyze Sentiment" in t.get("description", "") for t in combined_tasks):
#                     combined_tasks.append({"description": "Analyze Sentiment"})
#                 else:
#                     # Append any additional non-duplicate tasks
#                     if task not in combined_tasks:
#                         combined_tasks.append(task)
#             return combined_tasks
#         except Exception as e:
#             raise Exception(f"Error deciding tasks: {str(e)}")

# async def publish_tasks(tasks):
#     """
#     Publishes each task as a message to RabbitMQ.
#     """
#     try:
#         connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
#         async with connection:
#             channel = await connection.channel()
#             queue = await channel.declare_queue("task_queue", durable=True)
#             for task in tasks:
#                 message_body = json.dumps(task).encode()
#                 message = aio_pika.Message(
#                     body=message_body,
#                     delivery_mode=aio_pika.DeliveryMode.PERSISTENT
#                 )
#                 await channel.default_exchange.publish(message, routing_key="task_queue")
#         print("Published all tasks to RabbitMQ.")
#     except Exception as e:
#         print(f"Error connecting to RabbitMQ: {str(e)}")
#         # If RabbitMQ is unavailable, we'll still process but log the error
#         pass

# @app.route('/orchestrate', methods=['POST'])
# def orchestrate():
#     """
#     Receives a user's request and optional CSV file, applies decision logic to determine tasks,
#     and processes the CSV if needed.
#     """
#     # Check if the post request has the request field
#     user_request = request.form.get("request", "")
#     if not user_request:
#         return jsonify({"error": "No request provided"}), 400

#     csv_data = None
#     output_file_path = None

#     # Check if a file was uploaded
#     if 'file' in request.files:
#         file = request.files['file']
#         if file and file.filename and allowed_file(file.filename):
#             # Save the file temporarily
#             filename = secure_filename(file.filename)
#             file_path = os.path.join(UPLOAD_FOLDER, filename)
#             file.save(file_path)

#             try:
#                 # Read the CSV data
#                 csv_data = pd.read_csv(file_path)

#                 # Check if it's a cleaning request
#                 if "clean" in user_request.lower() or "csv" in user_request.lower():
#                     # Get cleaning recommendations
#                     cleaning_tasks = call_gemini_api(
#                         f"Based on the dataset I'm about to show you, suggest specific data cleaning steps. "
#                         f"The user requested: '{user_request}'",
#                         csv_data
#                     )

#                     # Clean the data based on recommendations
#                     cleaned_data = clean_csv_data(csv_data, cleaning_tasks)

#                     # Save the cleaned data
#                     output_filename = f"cleaned_{filename}"
#                     output_file_path = os.path.join(UPLOAD_FOLDER, output_filename)
#                     cleaned_data.to_csv(output_file_path, index=False)

#                     # Add cleaning result to tasks
#                     tasks = cleaning_tasks
#                     tasks.append({"description": "CSV data cleaned and saved", "output_file": output_filename})
#                 else:
#                     # Determine tasks using the CSV data
#                     tasks = decide_tasks(user_request, csv_data)
#             except Exception as e:
#                 return jsonify({"error": f"Error processing CSV file: {str(e)}"}), 500
#         else:
#             return jsonify({"error": "Invalid file. Only CSV files are allowed."}), 400
#     else:
#         # No file provided, just process the request
#         try:
#             tasks = decide_tasks(user_request)
#         except Exception as e:
#             return jsonify({"error": str(e)}), 500

#     # Publish tasks asynchronously to RabbitMQ
#     try:
#         asyncio.run(publish_tasks(tasks))
#     except Exception as e:
#         print(f"Warning: Tasks could not be published to queue: {str(e)}")
#         # We continue processing even if publishing fails

#     response_data = {
#         "message": "Request processed successfully.",
#         "tasks": tasks
#     }

#     # If we have a cleaned file to return
#     if output_file_path and os.path.exists(output_file_path):
#         return send_file(output_file_path, as_attachment=True,
#                          download_name=os.path.basename(output_file_path),
#                          mimetype='text/csv')

#     return jsonify(response_data), 200

# @app.route('/health', methods=['GET'])
# def health_check():
#     """
#     Simple health check endpoint
#     """
#     return jsonify({"status": "healthy"}), 200

# if __name__ == "__main__":
#     # Set up Gemini API key
#     genai.configure(api_key=os.environ.get("GEMINI_API_KEY", GEMINI_API_KEY))

#     # Ensure Groq API key is available
#     if not os.environ.get("GROQ_API_KEY") and GROQ_API_KEY:
#         os.environ["GROQ_API_KEY"] = GROQ_API_KEY

#     app.run(host="0.0.0.0", port=8080, debug=True)










# orchestrator/app.py
# import os
# import json
# import asyncio
# import pandas as pd
# import requests
# import re
# from flask import Flask, request, jsonify, send_file
# from werkzeug.utils import secure_filename
# import aio_pika
# import google.generativeai as genai
# from groq import AsyncGroq
# from config import GEMINI_API_KEY, GROQ_API_KEY
# from typing import Optional, List, Dict, Any

# app = Flask(__name__)
# UPLOAD_FOLDER = 'uploads'
# os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# ALLOWED_EXTENSIONS = {'csv'}

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# async def async_call_groq_api(prompt_text, csv_data=None):
#     groq_client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))

#     content_text = prompt_text
#     if csv_data is not None:
#         # Add the first few rows of the CSV to the prompt
#         try:
#             sample_data = csv_data.head(5).to_string()
#             content_text = f"{prompt_text}\n\nHere's a sample of the CSV data:\n{sample_data}"
#         except AttributeError:
#             content_text = f"{prompt_text}\n\nCSV data provided but could not sample."

#     chat_completion = await groq_client.chat.completions.create(
#         messages=[
#             {"role": "user", "content": content_text}
#         ],
#         model="llama3-8b-8192",
#     )
#     content = chat_completion.choices[0].message.content
#     try:
#         tasks = json.loads(content)
#     except json.JSONDecodeError:
#         tasks = [{"description": content}]
#     return tasks

# def call_gemini_api(prompt_text, csv_data=None):
#     model = genai.GenerativeModel('gemini-1.5-flash-001')

#     content_text = prompt_text
#     if csv_data is not None:
#         # Add the first few rows of the CSV to the prompt
#         try:
#             sample_data = csv_data.head(5).to_string()
#             content_text = f"{prompt_text}\n\nHere's a sample of the CSV data:\n{sample_data}"
#         except AttributeError:
#             content_text = f"{prompt_text}\n\nCSV data provided but could not sample."

#     response = model.generate_content(content_text)
#     cleaning_instructions = []
#     try:
#         match = re.search(r'```json\n(\[.*?\n\])\n```', response.text, re.DOTALL)
#         if match:
#             cleaning_instructions = json.loads(match.group(1))
#         else:
#             print(f"Warning: Could not find JSON array in Gemini's response: {response.text}")
#     except (json.JSONDecodeError, AttributeError) as e:
#         print(f"Error decoding JSON from Gemini's response: {e}")
#         print(f"Raw Gemini response: {response.text}")
#         # Try to parse even without the ```json block
#         try:
#             cleaning_instructions = json.loads(response.text)
#         except (json.JSONDecodeError, AttributeError) as e2:
#             print(f"Error decoding JSON (attempt 2): {e2}")

#     return cleaning_instructions

# def clean_csv_data(df, cleaning_instructions=None):
#     """
#     Clean the CSV data based on general best practices or specific instructions
#     """
#     # Make a copy to avoid modifying the original
#     cleaned_df = df.copy()

#     # Basic cleaning operations
#     # 1. Drop duplicates
#     cleaned_df = cleaned_df.drop_duplicates()

#     # 2. Handle missing values (fill with mean for numeric, mode for categorical)
#     for col in cleaned_df.columns:
#         if cleaned_df[col].dtype.kind in 'ifc':  # numeric columns
#             cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mean())
#         else:  # categorical columns
#             cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mode()[0] if not cleaned_df[col].mode().empty else "Unknown")

#     # 3. Strip whitespace from string columns
#     for col in cleaned_df.select_dtypes(include=['object']).columns:
#         cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

#     # 4. Convert date columns if detected
#     for col in cleaned_df.columns:
#         if 'date' in col.lower():
#             try:
#                 cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='ignore')
#             except:
#                 pass

#     # Apply custom cleaning based on AI recommendations if provided
#     if cleaning_instructions:
#         print(f"Would apply custom cleaning: {cleaning_instructions}")

#     return cleaned_df

# def decide_tasks(user_request: str, csv_data: Optional[pd.DataFrame] = None, filename: Optional[str] = None) -> List[Dict[str, Any]]:
#     lower_req = user_request.lower()
#     tasks_to_publish = []

#     if "csv" in lower_req or "data file" in lower_req or "clean" in lower_req:
#         cleaning_prompt = (
#             f"Based on the dataset I'm about to show you, suggest specific data cleaning steps. "
#             f"The user requested: '{user_request}'. "
#             "Return a JSON array of cleaning tasks where each task has an 'action' and 'parameters' field. "
#             "Example: [{'action': 'drop_duplicates', 'parameters': {'subset': ['columnA', 'columnB']}}, {'action': 'fill_na', 'parameters': {'columns': ['column1'], 'value': 'mean'}}]"
#         )
#         cleaning_instructions = call_gemini_api(cleaning_prompt, csv_data)
#         if filename:
#             tasks_to_publish.append({
#                 "description": "Clean Data",
#                 "input_file": filename,
#                 "cleaning_instructions": cleaning_instructions
#             })
#         else:
#             print("Warning: Filename not provided for Clean Data task.")
#             tasks_to_publish.append({
#                 "description": "Clean Data",
#                 "cleaning_instructions": cleaning_instructions
#             })
#     elif "sentiment" in lower_req:
#         sentiment_prompt = (
#             f"Based on the request '{user_request}', generate a JSON array containing tasks for sentiment analysis. "
#             "Each task should have a 'task' field (e.g., 'Analyze Sentiment') and a field containing the text to analyze, either 'text' or 'input'."
#         )
#         groq_tasks = asyncio.run(async_call_groq_api(sentiment_prompt, csv_data))
#         for task_payload in groq_tasks:
#             if isinstance(task_payload, dict) and "description" in task_payload:
#                 description = task_payload["description"]
#                 match_json = re.search(r'```json\n(\[.*?\n\])\n```', description, re.DOTALL)
#                 if match_json:
#                     try:
#                         json_array_str = match_json.group(1)
#                         tasks_from_json = json.loads(json_array_str)
#                         for task_from_json in tasks_from_json:
#                             if isinstance(task_from_json, dict) and task_from_json.get("task") == "Analyze Sentiment":
#                                 text_to_analyze = task_from_json.get("text")
#                                 if not text_to_analyze:
#                                     text_to_analyze = task_from_json.get("input")
#                                 if text_to_analyze:
#                                     tasks_to_publish.append({"description": "Analyze Sentiment", "input_data": text_to_analyze})
#                                     break # Assuming only one main sentiment analysis task
#                         if tasks_to_publish:
#                             break
#                     except json.JSONDecodeError as e:
#                         print(f"Error decoding JSON from Groq's response: {e}")
#                         print(f"Raw description: {description}")
#                 else:
#                     print(f"Warning: Could not find JSON array in Groq's response description: {description}")
#             else:
#                 print(f"Warning: Unexpected format in Groq's response: {task_payload}")

#         if not tasks_to_publish:
#             print(f"Warning: Could not extract 'Analyze Sentiment' task from Groq's response: {groq_tasks}")

#     else:
#         try:
#             tasks_groq = asyncio.run(async_call_groq_api(
#                 f"Generate a task list for the following request: '{user_request}'", csv_data
#             ))
#             tasks_gemini = call_gemini_api(
#                 f"Generate a task list for the following request: '{user_request}'", csv_data
#             )

#             seen_tasks = set()
#             for task in tasks_groq + tasks_gemini:
#                 desc = task.get("description")
#                 if desc and desc not in seen_tasks:
#                     tasks_to_publish.append(task)
#                     seen_tasks.add(desc)

#         except Exception as e:
#             print(f"Error deciding general tasks: {str(e)}")

#     return tasks_to_publish

# async def publish_tasks(tasks):
#     try:
#         connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
#         async with connection:
#             channel = await connection.channel()
#             queue = await channel.declare_queue("task_queue", durable=True)
#             for task in tasks:
#                 message_body = json.dumps(task).encode()
#                 message = aio_pika.Message(
#                     body=message_body,
#                     delivery_mode=aio_pika.DeliveryMode.PERSISTENT
#                 )
#                 await channel.default_exchange.publish(message, routing_key="task_queue")
#         print("Published all tasks to RabbitMQ.")
#     except Exception as e:
#         print(f"Error connecting to RabbitMQ: {str(e)}")
#         # If RabbitMQ is unavailable, we'll still process but log the error
#         pass

# @app.route('/orchestrate', methods=['POST'])
# def orchestrate():
#     # Check if the post request has the request field
#     user_request = request.form.get("request", "")
#     if not user_request:
#         return jsonify({"error": "No request provided"}), 400

#     csv_data = None
#     output_file_path = None
#     tasks_for_response = []
#     filename = None

#     # Check if a file was uploaded
#     if 'file' in request.files:
#         file = request.files['file']
#         if file and file.filename and allowed_file(file.filename):
#             # Save the file temporarily
#             filename = secure_filename(file.filename)
#             file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#             file.save(file_path)

#             try:
#                 # Read the CSV data
#                 csv_data = pd.read_csv(file_path)

#                 # Check if it's a cleaning request
#                 if "clean" in user_request.lower() or "csv" in user_request.lower():
#                     # Get cleaning recommendations
#                     tasks_to_publish = decide_tasks(user_request, csv_data, filename)
#                     if tasks_to_publish and tasks_to_publish[0].get("description") == "Clean Data":
#                         cleaning_instructions = tasks_to_publish[0].get("cleaning_instructions", [])
#                         print(f"Gemini suggested cleaning tasks: {cleaning_instructions}")
#                         print(f"Tasks being published: {tasks_to_publish}") # For debugging
#                         asyncio.run(publish_tasks(tasks_to_publish))
#                         tasks_for_response = tasks_to_publish
#                     else:
#                         # Fallback to basic cleaning if no specific AI instructions
#                         cleaned_data = clean_csv_data(csv_data)
#                         output_filename = f"cleaned_{filename}"
#                         output_file_path = os.path.join(UPLOAD_FOLDER, output_filename)
#                         cleaned_data.to_csv(output_file_path, index=False)
#                         tasks_for_response = [{"description": "Basic cleaning performed by orchestrator"}]
#                 else:
#                     # Determine tasks using the CSV data for other types of requests
#                     tasks_to_publish = decide_tasks(user_request, csv_data, filename)
#                     asyncio.run(publish_tasks(tasks_to_publish))
#                     tasks_for_response = tasks_to_publish

#             except Exception as e:
#                 return jsonify({"error": f"Error processing CSV file: {str(e)}"}), 500
#         else:
#             return jsonify({"error": "Invalid file. Only CSV files are allowed."}), 400
#     else:
#         # No file provided, just process the request
#         try:
#             tasks_to_publish = decide_tasks(user_request)
#             asyncio.run(publish_tasks(tasks_to_publish))
#             tasks_for_response = tasks_to_publish
#         except Exception as e:
#             return jsonify({"error": str(e)}), 500

#     response_data = {
#         "message": "Request processed successfully.",
#         "tasks": tasks_for_response
#     }

#     # If we have a cleaned file to return (from basic cleaning in orchestrator)
#     if output_file_path and os.path.exists(output_file_path) and "clean" in user_request.lower():
#         return send_file(output_file_path, as_attachment=True,
#                          download_name=os.path.basename(output_file_path),
#                          mimetype='text/csv')

#     return jsonify(response_data), 200

# @app.route('/health', methods=['GET'])
# def health_check():
#     """
#     Simple health check endpoint
#     """
#     return jsonify({"status": "healthy"}), 200

# if __name__ == "__main__":
#     # Set up Gemini API key
#     genai.configure(api_key=os.environ.get("GEMINI_API_KEY", GEMINI_API_KEY))

#     # Ensure Groq API key is available
#     if not os.environ.get("GROQ_API_KEY") and GROQ_API_KEY:
#         os.environ["GROQ_API_KEY"] = GROQ_API_KEY

#     app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
#     app.run(host="0.0.0.0", port=8080, debug=True)





























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
import redis # <-- Import Redis client
from redis.exceptions import ConnectionError as RedisConnectionError # <-- For specific error handling
from typing import Optional, List, Dict, Any, Union

# --- Configuration ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY_FALLBACK")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "YOUR_GROQ_API_KEY_FALLBACK")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis") # Get Redis host from env or default
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RESULTS_QUEUE_NAME = "results_queue"
TASK_EXPIRY_SECONDS = 3600 # Store task results in Redis for 1 hour

logging.basicConfig(level=logging.INFO, format='%(asctime)s - ORCHESTRATOR - %(levelname)s - %(message)s')
app = Flask(__name__)
UPLOAD_FOLDER = '/app/uploads'
ALLOWED_EXTENSIONS = {'csv'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- Initialize Redis Client ---
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0, # Default Redis database
        decode_responses=True # Decode responses from bytes to strings (usually UTF-8)
    )
    redis_client.ping() # Check connection on startup
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except RedisConnectionError as e:
    logging.error(f"FATAL: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT} - {e}")
    # Depending on requirements, you might exit or try to reconnect later
    redis_client = None # Indicate connection failure

# --- Remove Global Dictionary ---
# task_results: Dict[str, Dict[str, Union[str, Any]]] = {} # NO LONGER USED
# task_results_lock = threading.Lock() # NO LONGER USED

AVAILABLE_TASKS = {
    "data_cleaner_image": {
        "description": "Clean Data",
        "capabilities": [], # Use an empty list
        "queue": "data_cleaner_queue"
    },
    "sentiment_analyzer_image": {
        "description": "Analyze Sentiment",
        "capabilities": [], # Use an empty list
        "queue": "sentiment_analyzer_queue"
    }
}

# --- Helper Functions (keep allowed_file, async_call_groq_api, call_gemini_api) ---
# ... (No changes needed in these helpers) ...
def allowed_file(filename): # ... as before ...
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

async def async_call_groq_api(prompt_text, csv_data=None): # ... as corrected before ...
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

def call_gemini_api(prompt_text, csv_data=None): # ... as before ...
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
    """Uses LLM, prepares task messages with task_id, and initializes status in Redis."""
    # REMOVED global task_results access

    # --- LLM Decision (keep as before) ---
    containers_info = "\n".join([f"- {name}: {d['description']} ({', '.join(d['capabilities'])})" for name, d in AVAILABLE_TASKS.items()])
    task_selection_prompt = f"""
User Request: "{user_request}"
Available Processing Tools:
{containers_info}
Return *only* a single JSON object: {{"container": "[image_name]", "task_description": "[desc]", "parameters": {{...}} }}
# Include input_data for sentiment, check cleaning_instructions for data_cleaner
""" # Corrected {{}}
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

    # --- Generate Task ID ---
    task_id = str(uuid.uuid4())
    logging.info(f"Generated Task ID: {task_id}")

    # --- Prepare Task Payload (includes task_id) ---
    task_payload = { "task_id": task_id, "task_description": decision.get("task_description", task_info["description"]), **decision.get("parameters", {}) }

    # --- Add task-specific details ---
    if task_name == "data_cleaner_image":
        if not filename: return []
        task_payload["input_file"] = filename
        if "cleaning_instructions" not in task_payload or not task_payload.get("cleaning_instructions"):
            # (Keep Gemini call logic as before)
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

    # --- Initialize task status in Redis ---
    if redis_client:
        try:
            initial_status = {"status": "pending", "result": None, "error": None}
            # Use set with an expiry time (e.g., 1 hour)
            redis_client.set(task_id, json.dumps(initial_status), ex=TASK_EXPIRY_SECONDS)
            logging.info(f"Initialized status for Task ID {task_id} in Redis.")
        except RedisConnectionError as redis_err:
             logging.error(f"Redis connection error initializing task {task_id}: {redis_err}")
             # Decide how to handle: fail request or proceed without result tracking? For now, proceed.
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


# --- RabbitMQ Publisher (keep as before) ---
async def publish_tasks_to_workers(tasks_to_dispatch):
    # ... (No changes needed here) ...
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


# --- RabbitMQ Result Listener (Modified for Redis) ---
async def listen_for_results():
    """Connects to RabbitMQ, listens for results, and updates Redis."""
    loop = asyncio.get_running_loop()
    connection = None
    logging.info("[ResultListener] Starting...")
    while True:
        if redis_client is None:
             logging.error("[ResultListener] Redis client not available. Listener stopped.")
             break # Stop listener if no Redis
        try:
            connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/", loop=loop, timeout=30)
            logging.info("[ResultListener] Connected to RabbitMQ.")
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(RESULTS_QUEUE_NAME, durable=True)
                logging.info(f"[ResultListener] Waiting for messages on '{RESULTS_QUEUE_NAME}'.")
                async for message in queue:
                    async with message.process(ignore_processed=True):
                        task_id = None # Define outside try block
                        try:
                            result_data = json.loads(message.body.decode())
                            task_id = result_data.get("task_id")
                            result = result_data.get("result")
                            error = result_data.get("error")

                            if task_id:
                                # --- Update status in Redis ---
                                if error:
                                    new_status = {"status": "error", "error": error}
                                    log_level = logging.ERROR
                                else:
                                    new_status = {"status": "completed", "result": result}
                                    log_level = logging.INFO

                                try:
                                    # Update Redis entry, retain expiry if possible or reset
                                    redis_client.set(task_id, json.dumps(new_status), ex=TASK_EXPIRY_SECONDS)
                                    logging.log(log_level, f"[ResultListener] Updated status for Task ID {task_id} in Redis.")
                                except RedisConnectionError as redis_err:
                                    logging.error(f"[ResultListener] Redis connection error updating {task_id}: {redis_err}")
                                    # Consider retry logic or marking task as unknown state
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
            await asyncio.sleep(10) # Wait before retrying connection


def run_result_listener_in_thread():
    """Runs the asyncio result listener in a separate thread."""
    logging.info("Starting result listener thread.")
    try:
        asyncio.run(listen_for_results())
    except Exception as e:
         logging.exception("Result listener thread encountered fatal error.")

# --- Flask Routes ---
@app.route('/orchestrate', methods=['POST'])
def orchestrate():
    # (Keep file handling as before)
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
    message = "Request processing failed." # Default message
    status_code = 500 # Default error

    if redis_client is None: # Check if Redis is available before proceeding
        return jsonify({"error": "Result storage (Redis) is not available."}), 503

    try:
        tasks_to_dispatch = asyncio.run(decide_and_prepare_tasks(user_request, csv_data, filename))

        if tasks_to_dispatch:
            task_info = tasks_to_dispatch[0] # Assuming one task
            task_id_generated = task_info["task_id"]
            published_count = asyncio.run(publish_tasks_to_workers(tasks_to_dispatch))
            if published_count > 0:
                message = f"{published_count} task(s) dispatched successfully."
                status_code = 202 # Accepted
            else:
                 message = "Task identified, but failed to publish to queue."
                 # Clean up pending status in Redis?
                 try: redis_client.delete(task_id_generated)
                 except: pass # Ignore errors during cleanup
                 task_id_generated = None # Don't return ID if publish failed
        else:
            message = "Request received, but no processing task identified or prepared."
            status_code = 202 # Accepted, but nothing done

    except ValueError as e: message = f"Configuration error: {e}"; status_code = 500
    except Exception as e: logging.error(f"Error during task decision/dispatch: {e}"); message = f"Failed to process request: {e}"; status_code = 500

    # --- Response (Includes Task ID) ---
    response_data = { "message": message, "task_id": task_id_generated }
    if task_id_generated is None: response_data["task_id"] = None # Explicitly null if failed
    return jsonify(response_data), status_code


@app.route('/results/<task_id>', methods=['GET'])
def get_result(task_id):
    """Polling endpoint using Redis."""
    logging.info(f"Received request for result of Task ID: {task_id}")
    if redis_client is None:
        return jsonify({"error": "Result storage (Redis) is not available."}), 503

    try:
        result_json = redis_client.get(task_id) # Get raw JSON string from Redis
        if result_json:
            result_info = json.loads(result_json) # Parse the JSON
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
            # Key doesn't exist in Redis
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
    # Add Redis ping to health check
    redis_status = "unhealthy"
    if redis_client:
        try:
            if redis_client.ping(): redis_status = "healthy"
        except RedisConnectionError: pass
    return jsonify({"status": "healthy", "redis_status": redis_status}), 200 if redis_status == "healthy" else 503


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting Orchestrator Flask App with Redis support...")
    # API Config (keep as before)
    if GEMINI_API_KEY and "FALLBACK" not in GEMINI_API_KEY: genai.configure(api_key=GEMINI_API_KEY); logging.info("Gemini API configured.")
    else: logging.warning("Gemini API Key not configured.")
    if not GROQ_API_KEY or "FALLBACK" in GROQ_API_KEY: logging.warning("Groq API Key not configured.")
    else: logging.info("Groq API Key found.")

    if redis_client:
        # Start listener thread only if Redis connected
        listener_thread = Thread(target=run_result_listener_in_thread, daemon=True)
        listener_thread.start()
    else:
         logging.warning("Redis not connected. Result listener thread NOT started.")

    # Start Flask App (Consider Gunicorn/Waitress for production)
    app.run(host="0.0.0.0", port=8080, debug=os.environ.get("FLASK_DEBUG", False))
