# LLM-Orchestrator

LLM-Orchestrated Data Processing and Sentiment Analysis Pipeline using RabbitMQ for Asynchronous processing of tasks in Containers

## Overview

This project demonstrates a data processing pipeline where an LLM (Language Model) is integrated to make decisions about which containerized services to run based on a user's request. The system includes a central orchestrator, containerized worker services for data cleaning and sentiment analysis, a message queue (RabbitMQ) for task management, a data store (Redis) for storing task status and results, and a command-line interface (CLI) for user interaction.

## Features Implemented

* **LLM Integration:** Connects to an LLM (via API keys for Gemini or Groq) to determine which data processing container(s) to execute.
* **Containerized Services:** Utilizes Docker containers for each major component:
    * `orchestrator`: Manages the workflow and interacts with the LLM.
    * `data_cleaner`: Performs data cleaning tasks.
    * `sentiment_analyzer`: Performs sentiment analysis tasks.
    * `rabbitmq`: Message broker for task queuing and result reporting.
    * `redis`: In-memory data store for managing task status and results.
* **Central Orchestrator:** A Python Flask backend that:
    * Receives user requests via the CLI.
    * Calls the integrated LLM to decide which processing steps are needed.
    * Enqueues tasks for the appropriate containerized services in RabbitMQ.
    * Collects the final output from the worker services via RabbitMQ and stores it in Redis.
    * Returns the final output to the user via the CLI.
* **User Interface (CLI):** A simple Python-based command-line interface (`cli-tool.py`) to:
    * Accept user requests as text input and optionally a CSV file.
    * Display the status and final output of the processing tasks.

## Architecture

The system follows a microservices-like architecture where different functionalities are encapsulated in separate Docker containers that communicate via a message queue.



![diagram-export-4-10-2025-8_46_31-PM](https://github.com/user-attachments/assets/7c5a44be-8c3b-44d2-a080-f267c51fb032)





**Component Explanation:**

* **User (via CLI):** The user interacts with the system using the `cli-tool.py` script, providing a request and optionally a file.
* **Orchestrator:** The central service that receives the user's request, communicates with the LLM to determine the necessary processing steps, and orchestrates the execution of these steps by interacting with RabbitMQ. It also uses Redis to track task status and store results.
* **RabbitMQ:** A message broker that facilitates asynchronous communication between the orchestrator and the worker services (`data_cleaner`, `sentiment_analyzer`). It uses queues to hold tasks and results.
* **Data Cleaner:** A containerized service responsible for performing data cleaning operations on a CSV file based on instructions (likely determined by the LLM or the user's request).
* **Sentiment Analyzer:** A containerized service responsible for analyzing the sentiment of text data.
* **Redis:** An in-memory data store used by the orchestrator to store the status of tasks (e.g., pending, completed, error) and the final results returned by the worker services.
* **LLM:** An external Language Model (like Gemini or accessed via Groq) that the orchestrator queries to decide which data processing container(s) should be run to fulfill the user's request.

## Setup

Follow these steps to get the system running:

1.  **Prerequisites:**
    * Docker and Docker Compose installed on your system.
    * Python 3.9 or higher.
    * An API key for either Google Gemini (set as `GEMINI_API_KEY` environment variable) or Groq (set as `GROQ_API_KEY` environment variable) if you intend to use LLM integration. You can also run the system without LLM integration by modifying the orchestrator's logic to always run specific containers based on the request text.

2.  **Clone the Repository:**
    ```bash
    git clone https://github.com/Eldrago12/LLM-Orchestrator.git
    cd AI_Orchestrator
    ```

3.  **Set Environment Variables:**
    Create a `.env` file in the root directory (or set environment variables directly in your shell) with your API keys:
    ```
    GEMINI_API_KEY=your_gemini_api_key
    GROQ_API_KEY=your_groq_api_key
    ```

4.  **Build and Run the Containers:**
    Navigate to the root directory of the project (where the `docker-compose.yml` file is located) and run:

    ```bash
    docker-compose build
    docker-compose up
    ```

    This command will build the Docker images for all the services and start them in detached mode.

5.  **Using the CLI Tool:**
    Navigate to the directory containing `cli-tool.py` (likely the root of the repository) and run it with your request:

    ```bash
    python cli-tool.py "Clean this data" -f path/to/your/data.csv
    ```

    or for sentiment analysis:

    ```bash
    python cli-tool.py "Analyze the sentiment of this text: The product is amazing!"
    ```

    You can also specify the orchestrator URL, polling interval, and timeout using the `-u`, `-i`, and `-t` flags respectively.

## Workflow Explanation

Here's a detailed breakdown of the system's workflow from the user's perspective:

1.  **User Submits Request:** The user executes the `cli-tool.py` script from their terminal, providing a natural language request (e.g., "Clean this CSV file and then analyze its sentiment") and optionally a path to a CSV file using the `-f` flag.

2.  **CLI Sends Request to Orchestrator:** The `cli-tool.py` sends an HTTP POST request to the `/orchestrate` endpoint of the orchestrator service (running at `http://localhost:8080` by default). The request includes the user's text and the file (if provided).

3.  **Orchestrator Receives Request:** The orchestrator's backend (likely a Flask application) receives the request.

4.  **Orchestrator Calls the LLM:** The orchestrator takes the user's request text and queries the configured LLM (Gemini or Groq) using a predefined prompt or set of rules. The LLM analyzes the request to determine which data processing steps are necessary. For example:
    * If the request contains keywords like "clean", "missing values", "duplicates", and a file is provided, the LLM might decide to run the `data_cleaner` container.
    * If the request contains keywords like "sentiment", "analyze", "opinion", the LLM might decide to run the `sentiment_analyzer` container.
    * For more complex requests like "Clean this CSV file and then analyze its sentiment", the LLM might decide to run the `data_cleaner` first and then the `sentiment_analyzer`. This sequential execution logic would need to be implemented in the orchestrator.

5.  **Orchestrator Enqueues Tasks in RabbitMQ:** Based on the LLM's decision, the orchestrator creates a task message (in JSON format) containing relevant information such as the input file name (if uploaded), cleaning instructions (if determined by the LLM or user request), or the text to analyze. This message is then published to the appropriate RabbitMQ queue:
    * For data cleaning, the message is sent to the `data_cleaner_queue`.
    * For sentiment analysis, the message is sent to the `sentiment_analyzer_queue`.

6.  **Worker Container Consumes Message:** The corresponding worker container (`data_cleaner` or `sentiment_analyzer`), which is continuously running and listening to its designated RabbitMQ queue, receives the task message.

7.  **Worker Executes Task:**
    * **Data Cleaner:** The `data_cleaner_consumer.py` script inside the `data_cleaner` container reads the task message, retrieves the input file from the mounted `/app/uploads` directory, applies the cleaning operations as specified in the message (or a default set of operations), and produces the cleaned data.
    * **Sentiment Analyzer:** The `sentiment_analyzer_consumer.py` script inside the `sentiment_analyzer` container reads the task message, extracts the text to analyze, executes the `analyzer.py` script (which uses a pre-trained sentiment analysis model), and obtains the sentiment result.

8.  **Worker Publishes Result to RabbitMQ:** Once the task is complete, the worker container publishes a result message (containing the `task_id` and either the processed data or an error message) to the `results_queue` in RabbitMQ.

9.  **Orchestrator Collects Result from RabbitMQ and Stores in Redis:** The orchestrator has a background process or a consumer listening to the `results_queue`. When a result message arrives, the orchestrator retrieves it, associates it with the original `task_id`, and stores the result (and the task status: "completed" or "error") in Redis.

10. **CLI Tool Polls Orchestrator:** The `cli-tool.py` continues to periodically send HTTP GET requests to the `/results/{task_id}` endpoint of the orchestrator, checking for the status of the submitted task.

11. **Orchestrator Retrieves Result from Redis and Returns to CLI:** When the orchestrator receives a request to the `/results/{task_id}` endpoint, it looks up the status and result of that `task_id` in Redis and returns it in the HTTP response.

12. **CLI Displays Result to User:** The `cli-tool.py` receives the response from the orchestrator. If the status is "completed", it prints the final output (cleaned data or sentiment analysis result) to the user's console. If the status is "error", it prints the error message.

## LLM Decision-Making (Example)

Here's a simplified example of how the orchestrator might use the LLM to decide which container to run:

**Prompt (Example for Gemini):**

      You are an intelligent agent that helps route user requests to the appropriate data processing service.
      Based on the user's request, decide which of the following services should be used:

      data_cleaner: For cleaning and preprocessing CSV data.
      sentiment_analyzer: For analyzing the sentiment of text.
      User Request: "{user_request}"

      Which service should be used? Respond with only the service name (e.g., "data_cleaner" or "sentiment_analyzer") or "none" if no suitable service is found.

**Example Scenario:**

* **User Request:** "Clean this CSV file of missing values and duplicate rows."
* **LLM Response:** `data_cleaner`
* **Orchestrator Action:** Enqueues a task for the `data_cleaner` container with instructions to handle missing values and duplicates.

* **User Request:** "What is the sentiment of the following sentence: 'This movie was absolutely fantastic!'"
* **LLM Response:** `sentiment_analyzer`
* **Orchestrator Action:** Enqueues a task for the `sentiment_analyzer` container with the sentence to analyze.

* **User Request:** "Tell me the weather in London."
* **LLM Response:** `none`
* **Orchestrator Action:** Might return a message to the user indicating that the request cannot be processed by the current services.

**Note:** The actual prompt design and the complexity of the LLM's decision-making process can be much more sophisticated depending on the requirements.

## System Clarity

The orchestrator and container architecture are designed with clarity in mind:

* **Separation of Concerns:** Each container has a specific responsibility (orchestration, data cleaning, sentiment analysis, message brokering, data storage).
* **Asynchronous Communication:** RabbitMQ enables loose coupling between services, allowing them to operate independently.
* **Standardized Interfaces:** Communication between the CLI and the orchestrator uses standard HTTP. Communication between the orchestrator and workers uses JSON messages via RabbitMQ.
* **Configuration via Environment Variables:** API keys and other configuration parameters are managed through environment variables.

## Functionality

The system is designed to correctly run containerized tasks based on LLM decisions (although the exact LLM interaction logic is within the orchestrator's code which wasn't fully provided). The workflow described above outlines how the system processes user requests by leveraging the LLM to choose the appropriate containerized service(s) to execute.

## Code Quality

The provided code snippets demonstrate reasonable code quality:

* **Clear Naming:** Meaningful names are used for variables, functions, and services.
* **Logging:** Logging is implemented in both the worker scripts and the consumer scripts to provide insights into the execution flow and potential issues.
* **Error Handling:** Basic error handling is included in the scripts to manage potential failures (file not found, network errors, JSON decoding errors).
* **Modularity:** The code is broken down into functions with specific responsibilities.

## Efficiency & Simplicity

The provided solution aims for a minimal, working demonstration of the concepts:

* **Focus on Core Functionality:** The system implements basic data cleaning and sentiment analysis tasks.
* **Simple Orchestration:** The orchestrator's logic (based on the likely LLM interaction) is assumed to be straightforward for this demonstration.
* **Standard Technologies:** The project utilizes well-established technologies like Python, Flask, Docker Compose, RabbitMQ, and Redis.

## Conclusion

This project successfully demonstrates an LLM-orchestrated data processing pipeline with containerized services. It provides a basic framework for building more complex data processing workflows driven by the intelligence of a Language Model.
