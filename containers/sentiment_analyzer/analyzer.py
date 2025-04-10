# import os
# from textblob import TextBlob

# def analyze_sentiment(text):
#     try:
#         blob = TextBlob(text)
#         sentiment = blob.sentiment
#         return f"Polarity: {sentiment.polarity}, Subjectivity: {sentiment.subjectivity}"
#     except Exception as e:
#         return f"Error in sentiment analysis: {str(e)}"

# if __name__ == "__main__":
#     input_text = os.environ.get("INPUT_DATA", "")
#     result = analyze_sentiment(input_text)
#     print(result)

# containers/sentiment_analyzer/analyzer.py
# (Your original code is fine here)

# containers/sentiment_analyzer/analyzer.py
# Role: Performs sentiment analysis based on INPUT_DATA environment variable.
# Called by: sentiment_analyzer_consumer.py

import os
from transformers import pipeline, AutoTokenizer # Import tokenizer if needed for truncation check
import logging
import time
import torch # Check for GPU availability

# Configure logging for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - ANALYZER_SCRIPT - %(levelname)s - %(message)s')

# --- Model Loading ---
SENTIMENT_PIPELINE = None
MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"
TOKENIZER = None # Load tokenizer to check length

def load_model_and_tokenizer():
    """Loads the Hugging Face pipeline and tokenizer."""
    global SENTIMENT_PIPELINE, TOKENIZER
    if SENTIMENT_PIPELINE is None:
        start_time = time.time()
        logging.info(f"Loading sentiment analysis model: {MODEL_NAME}...")
        try:
            # Check for GPU
            device_num = 0 if torch.cuda.is_available() else -1
            device_name = "GPU" if device_num == 0 else "CPU"
            logging.info(f"Using device: {device_name}")

            SENTIMENT_PIPELINE = pipeline("sentiment-analysis", model=MODEL_NAME, device=device_num)
            TOKENIZER = AutoTokenizer.from_pretrained(MODEL_NAME)
            elapsed = time.time() - start_time
            logging.info(f"Model and tokenizer loaded successfully in {elapsed:.2f} seconds.")
        except Exception as e:
            logging.exception("CRITICAL: Failed to load sentiment analysis model/tokenizer.")
            # SENTIMENT_PIPELINE remains None

load_model_and_tokenizer() # Attempt to load when script starts

def analyze_sentiment_transformers(text):
    """
    Analyzes sentiment using the pre-loaded Hugging Face pipeline.
    """
    if SENTIMENT_PIPELINE is None or TOKENIZER is None:
         logging.error("Sentiment analysis resources not loaded. Cannot analyze.")
         return "Error: Sentiment analysis model/tokenizer not available."

    if not text or not isinstance(text, str):
        logging.warning("Received empty or invalid text for analysis.")
        return "Error: No valid text provided for analysis."

    logging.info(f"Analyzing sentiment for text (first 80 chars): {text[:80]}...")
    try:
        start_time = time.time()
        # Use truncation=True within the pipeline call - it's generally safer
        # The tokenizer loaded isn't strictly needed here if pipeline handles truncation
        result = SENTIMENT_PIPELINE(text, truncation=True, max_length=TOKENIZER.model_max_length)[0]
        elapsed = time.time() - start_time
        logging.info(f"Analysis completed in {elapsed:.4f} seconds.")

        label = result['label']
        score = result['score']

        # --- Interpretation (adjust based on MODEL_NAME if needed) ---
        sentiment = label # Directly POSITIVE or NEGATIVE for distilbert-sst-2-english
        confidence_percentage = score * 100

        return f"Sentiment: {sentiment} (Confidence: {confidence_percentage:.2f}%) [Model: {MODEL_NAME}]"

    except Exception as e:
        # Log full traceback for debugging
        logging.exception("Error during sentiment analysis execution:")
        return f"Error performing sentiment analysis: {str(e)}"

# --- Main Execution Block (for direct script call) ---
if __name__ == "__main__":
    logging.info("analyzer.py script executed directly.")
    exit_code = 0
    analysis_result_text = ""

    try:
        # Read input text from environment variable
        input_text_to_analyze = os.environ.get("INPUT_DATA", "")

        if not input_text_to_analyze:
            logging.error("INPUT_DATA environment variable not set or empty.")
            analysis_result_text = "Error: No input text provided via INPUT_DATA environment variable."
            exit_code = 2 # Specific exit code for missing input
        else:
            # Perform analysis
            analysis_result_text = analyze_sentiment_transformers(input_text_to_analyze)
            if "Error:" in analysis_result_text:
                 # Analysis function returned an error message
                 exit_code = 1 # General error if analysis failed
            else:
                 logging.info("Analysis successful.")

    except Exception as e:
         logging.exception("Script Error: An unexpected error occurred during main execution.")
         analysis_result_text = f"Error: An unexpected error occurred in analyzer: {str(e)}"
         exit_code = 1 # General error exit code
    finally:
         # Always print the result (analysis string or error string) to stdout
         print(analysis_result_text)
         # Exit with the appropriate code
         exit(exit_code)
