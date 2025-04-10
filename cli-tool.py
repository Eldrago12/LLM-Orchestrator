#!/usr/bin/env python3
import requests
import argparse
import time
import json
import os
import sys

# --- Configuration ---
DEFAULT_BASE_URL = "http://localhost:8080"  # Adjust if your orchestrator runs elsewhere
DEFAULT_POLL_INTERVAL = 2  # Seconds
DEFAULT_POLL_TIMEOUT = 60  # Seconds

# --- Helper Functions ---

def print_error(message):
    """Prints an error message to stderr."""
    print(f"ERROR: {message}", file=sys.stderr)

def submit_task(base_url, request_text, file_path=None):
    """Sends the task request to the orchestrator."""
    orchestrate_url = f"{base_url}/orchestrate"
    data = {'request': request_text}
    files = None
    upload_file = None

    print(f"Submitting task to {orchestrate_url}...")
    print(f"Request text: '{request_text[:100]}{'...' if len(request_text)>100 else ''}'")

    if file_path:
        if not os.path.exists(file_path):
            print_error(f"File not found: {file_path}")
            return None
        try:
            # File must be opened in binary mode for requests
            upload_file = open(file_path, 'rb')
            files = {'file': (os.path.basename(file_path), upload_file)}
            print(f"Attaching file: {file_path}")
        except Exception as e:
            print_error(f"Could not open file {file_path}: {e}")
            if upload_file:
                upload_file.close()
            return None

    try:
        response = requests.post(orchestrate_url, data=data, files=files, timeout=30) # Add timeout for POST

        # Ensure file handle is closed even if request fails
        if upload_file:
            upload_file.close()

        if response.status_code == 202: # Status code for "Accepted"
            try:
                result_json = response.json()
                task_id = result_json.get("task_id")
                if task_id:
                    print(f"Server response: {result_json.get('message', 'No message received.')}")
                    return task_id
                else:
                    print_error(f"Server accepted request but did not return a task_id. Response: {response.text}")
                    return None
            except json.JSONDecodeError:
                print_error(f"Failed to decode JSON response from server (Status 202). Response: {response.text}")
                return None
        else:
            # Try to get error details from response if possible
            try:
                 error_details = response.json()
                 print_error(f"Server returned error (Status {response.status_code}): {error_details}")
            except json.JSONDecodeError:
                 print_error(f"Server returned error (Status {response.status_code}): {response.text}")
            return None

    except requests.exceptions.ConnectionError as e:
        print_error(f"Connection refused. Is the orchestrator running at {base_url}? Details: {e}")
        return None
    except requests.exceptions.Timeout:
        print_error("Request timed out while trying to connect to the orchestrator.")
        return None
    except requests.exceptions.RequestException as e:
        print_error(f"An unexpected error occurred during submission: {e}")
        return None
    finally:
        # Ensure file is closed in case of unexpected exceptions during request
        if upload_file and not upload_file.closed:
            upload_file.close()


def poll_for_result(base_url, task_id, interval, timeout):
    """Polls the results endpoint for task completion."""
    results_url = f"{base_url}/results/{task_id}"
    start_time = time.time()

    print(f"Polling {results_url} every {interval}s (timeout {timeout}s)...")

    while True:
        # --- Check for timeout ---
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print_error(f"Polling timed out after {timeout} seconds.")
            return None

        try:
            response = requests.get(results_url, timeout=10) # Add timeout for GET

            if response.status_code == 200:
                try:
                    result_data = response.json()
                    status = result_data.get("status")

                    if status == "completed":
                        print("\nTask completed successfully!")
                        return result_data
                    elif status == "error":
                        print_error("\nTask failed!")
                        return result_data
                    elif status == "pending":
                        print("Status: pending...")
                        # Continue polling
                    elif status == "not_found":
                         print_error(f"Task ID {task_id} not found by the server (or expired).")
                         return None # Stop polling, task is gone
                    else:
                        # Handle unexpected statuses if needed
                        print(f"Status: {status}...")

                except json.JSONDecodeError:
                    print_error(f"Failed to decode JSON response from results endpoint. Response: {response.text}")
                    # Decide whether to retry or fail - let's retry for now
            elif response.status_code == 404:
                 print_error(f"Results endpoint not found (404). Task ID {task_id} might be invalid or results expired.")
                 return None # Stop polling
            else:
                # Handle other potential server errors (e.g., 500, 503)
                print_error(f"Received unexpected status code {response.status_code} while polling. Response: {response.text}")
                # Decide whether to retry or fail - let's retry cautiously

        except requests.exceptions.ConnectionError as e:
            print_error(f"Connection refused while polling. Is the orchestrator running? Retrying... Details: {e}")
            # Continue polling after sleep
        except requests.exceptions.Timeout:
            print_error("Request timed out while polling. Retrying...")
            # Continue polling after sleep
        except requests.exceptions.RequestException as e:
            print_error(f"An unexpected error occurred during polling: {e}. Retrying...")
            # Continue polling after sleep


        # --- Wait before next poll ---
        time.sleep(interval)


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="CLI tool to interact with the Orchestrator Service.")

    parser.add_argument("request_text", help="The text request to send to the orchestrator.")
    parser.add_argument("-f", "--file", help="Path to a CSV file to upload (optional).")
    parser.add_argument("-u", "--url", default=DEFAULT_BASE_URL, help=f"Base URL of the orchestrator service (default: {DEFAULT_BASE_URL}).")
    parser.add_argument("-i", "--interval", type=int, default=DEFAULT_POLL_INTERVAL, help=f"Polling interval in seconds (default: {DEFAULT_POLL_INTERVAL}).")
    parser.add_argument("-t", "--timeout", type=int, default=DEFAULT_POLL_TIMEOUT, help=f"Polling timeout in seconds (default: {DEFAULT_POLL_TIMEOUT}).")

    args = parser.parse_args()

    # 1. Submit Task
    task_id = submit_task(args.url, args.request_text, args.file)

    # 2. Poll for Results if submission was successful
    if task_id:
        final_result = poll_for_result(args.url, task_id, args.interval, args.timeout)

        if final_result:
            print("\n--- Final Result ---")
            print(json.dumps(final_result, indent=4))
            # Exit with non-zero status if the *task itself* failed
            if final_result.get("status") == "error":
                sys.exit(1)
        else:
            # Polling failed or timed out
            sys.exit(1) # Exit with error code because we didn't get a final result
    else:
        # Submission failed
        sys.exit(1) # Exit with error code because submission failed

if __name__ == "__main__":
    main()
