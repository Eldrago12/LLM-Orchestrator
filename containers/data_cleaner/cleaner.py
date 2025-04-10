# containers/data_cleaner/cleaner.py
# Role: Performs data cleaning based on arguments passed via command line.
# Called by: data_cleaner_consumer.py

import os
import pandas as pd
import json
import argparse # Takes command-line arguments
import subprocess
import logging

# Configure logging for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - CLEANER_SCRIPT - %(levelname)s - %(message)s')

def clean_data(input_file, cleaning_instructions):
    """
    Cleans the CSV data based on specific instructions.
    Returns the cleaned DataFrame or raises an error on failure.
    """
    try:
        logging.info(f"Starting cleaning process for file: {input_file}")
        if not os.path.exists(input_file):
             logging.error(f"Input file not found at path: {input_file}")
             # Raise specific error for the caller (consumer) to potentially handle
             raise FileNotFoundError(f"Input file not found at path: {input_file}")

        logging.info(f"File permissions: {subprocess.getoutput(f'ls -l {input_file}')}")
        df = pd.read_csv(input_file)
        logging.info(f"Read CSV. Original DataFrame shape: {df.shape}")
        logging.info(f"Received {len(cleaning_instructions)} cleaning instructions.")

        original_dtypes = df.dtypes # Store original types for logging changes

        # --- Apply Instructions ---
        if cleaning_instructions:
            logging.info("Applying specific cleaning instructions...")
            for i, instruction in enumerate(cleaning_instructions):
                action = instruction.get('action')
                parameters = instruction.get('parameters', {})
                logging.info(f"Instruction {i+1}: action='{action}', params={parameters}")

                try:
                    # --- Action Implementations ---
                    if action == 'fill_na':
                        columns_to_fill = parameters.get('columns')
                        fill_value = parameters.get('value')
                        method = parameters.get('method')

                        if not columns_to_fill:
                            columns_to_fill = df.columns.tolist()
                            logging.info(f"No columns specified for fill_na, applying to all.")

                        valid_columns = [col for col in columns_to_fill if col in df.columns]
                        if len(valid_columns) != len(columns_to_fill):
                             logging.warning(f"Some columns for fill_na not found: {set(columns_to_fill) - set(valid_columns)}")
                        if not valid_columns: continue # Skip if no valid columns

                        if fill_value is not None:
                            # Basic type conversion attempt for fill_value
                            try:
                                if any(pd.api.types.is_numeric_dtype(df[col]) for col in valid_columns):
                                    fill_value = float(fill_value)
                            except (ValueError, TypeError): pass # Keep original type if conversion fails
                            df[valid_columns] = df[valid_columns].fillna(fill_value)
                            logging.info(f"Filled NA in {valid_columns} with value: {fill_value}")
                        elif method in ['mean', 'median', 'mode']:
                            for col in valid_columns:
                                if method == 'mean' and pd.api.types.is_numeric_dtype(df[col]):
                                    fill_val = df[col].mean()
                                    df[col] = df[col].fillna(fill_val)
                                    logging.info(f"Filled NA in '{col}' with mean: {fill_val}")
                                elif method == 'median' and pd.api.types.is_numeric_dtype(df[col]):
                                    fill_val = df[col].median()
                                    df[col] = df[col].fillna(fill_val)
                                    logging.info(f"Filled NA in '{col}' with median: {fill_val}")
                                elif method == 'mode':
                                    col_mode = df[col].mode()
                                    if not col_mode.empty:
                                        fill_val = col_mode[0]
                                        df[col] = df[col].fillna(fill_val)
                                        logging.info(f"Filled NA in '{col}' with mode: {fill_val}")
                                    else: logging.warning(f"Cannot fill NA in '{col}' with mode (no mode found).")
                                else: # Handle non-numeric for mean/median
                                     logging.warning(f"Cannot fill non-numeric column '{col}' with method '{method}'.")
                        else:
                            logging.warning(f"Invalid parameters for fill_na: Provide 'value' or 'method' ('mean', 'median', 'mode').")

                    elif action == 'drop_duplicates':
                        subset_columns = parameters.get('subset')
                        keep = parameters.get('keep', 'first')
                        valid_subset = None
                        if subset_columns:
                             valid_subset = [col for col in subset_columns if col in df.columns]
                             if len(valid_subset) != len(subset_columns):
                                 logging.warning(f"Some subset columns for drop_duplicates not found: {set(subset_columns) - set(valid_subset)}")
                             if not valid_subset: valid_subset = None # Don't use subset if none are valid

                        initial_rows = len(df)
                        df.drop_duplicates(subset=valid_subset, keep=keep, inplace=True)
                        rows_dropped = initial_rows - len(df)
                        logging.info(f"Dropped {rows_dropped} duplicate rows. Subset={valid_subset}, Keep={keep}")

                    elif action == 'convert_dtype':
                        columns_to_convert = parameters.get('columns', [])
                        target_dtype = parameters.get('dtype')
                        if not columns_to_convert or not target_dtype:
                            logging.warning("Missing 'columns' or 'dtype' for convert_dtype.")
                            continue

                        valid_columns = [col for col in columns_to_convert if col in df.columns]
                        if len(valid_columns) != len(columns_to_convert):
                             logging.warning(f"Some columns for convert_dtype not found: {set(columns_to_convert) - set(valid_columns)}")
                        if not valid_columns: continue

                        for col in valid_columns:
                             try:
                                 if target_dtype in ['datetime', 'datetime64', 'datetime64[ns]']:
                                     df[col] = pd.to_datetime(df[col], errors='coerce')
                                     logging.info(f"Converted '{col}' to datetime (errors coerced).")
                                 # Add other specific type conversions if needed (e.g., int with errors='ignore')
                                 else:
                                     df[col] = df[col].astype(target_dtype)
                                     logging.info(f"Converted '{col}' to dtype: {target_dtype}")
                             except Exception as e:
                                 logging.error(f"Failed converting '{col}' to '{target_dtype}': {e}. Skipping.")

                    elif action == 'strip_whitespace':
                        columns_to_strip = parameters.get('columns')
                        if not columns_to_strip:
                            columns_to_strip = df.select_dtypes(include=['object', 'string']).columns.tolist()
                            logging.info(f"No columns specified for strip_whitespace, applying to object/string columns.")

                        valid_columns = [col for col in columns_to_strip if col in df.columns]
                        if not valid_columns: continue

                        for col in valid_columns:
                             # Check if column is actually string-like before stripping
                             if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
                                 try:
                                     df[col] = df[col].astype(str).str.strip() # Ensure string type then strip
                                     logging.info(f"Stripped whitespace from column '{col}'.")
                                 except Exception as e:
                                     logging.warning(f"Could not strip whitespace from column '{col}': {e}")
                             else:
                                 logging.warning(f"Column '{col}' is not string/object type, cannot strip whitespace.")

                    else:
                        logging.warning(f"Unknown cleaning action received: '{action}'")

                except Exception as action_error:
                     logging.error(f"Error during action '{action}': {action_error}. Skipping instruction.")
                     # Decide if script should fail entirely or continue

        else:
             logging.info("No specific instructions. Applying basic cleaning (drop duplicates).")
             initial_rows = len(df)
             df.drop_duplicates(inplace=True)
             rows_dropped = initial_rows - len(df)
             logging.info(f"Dropped {rows_dropped} duplicate rows (basic cleaning).")

        # --- Final Steps ---
        df.reset_index(drop=True, inplace=True)
        logging.info(f"Cleaning process finished. Final DataFrame shape: {df.shape}")

        # Log dtype changes
        final_dtypes = df.dtypes
        changed_dtypes = {col: f"{original_dtypes.get(col)} -> {final_dtypes.get(col)}" for col in df.columns if original_dtypes.get(col) != final_dtypes.get(col)}
        if changed_dtypes: logging.info(f"Data types changed: {changed_dtypes}")

        return df # Return the cleaned DataFrame

    # --- Error Handling for the whole function ---
    except FileNotFoundError as e:
        logging.error(f"Fatal Error: {e}")
        raise # Re-raise for consumer to handle
    except pd.errors.EmptyDataError:
        logging.error(f"Fatal Error: Input file {input_file} is empty.")
        raise ValueError(f"Input file {input_file} is empty.") # Raise different error type
    except Exception as e:
        logging.exception(f"Fatal Error during data cleaning process: {str(e)}") # Log full traceback
        raise # Re-raise unexpected errors

# --- Main Execution Block (for direct script call) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean CSV data based on instructions passed as arguments.")
    parser.add_argument("--input_file", required=True, help="Path to the input CSV file.")
    parser.add_argument("--cleaning_instructions", required=True, help="JSON string of cleaning instructions (list of actions).")
    args = parser.parse_args()

    logging.info("cleaner.py script executed directly.")

    output_json = "" # Initialize output
    exit_code = 0 # Default success

    try:
        # Load instructions from the JSON string argument
        cleaning_instructions_list = json.loads(args.cleaning_instructions)
        if not isinstance(cleaning_instructions_list, list):
             raise ValueError("Cleaning instructions must be a JSON list (array).")

        # Perform cleaning by calling the function
        cleaned_df = clean_data(args.input_file, cleaning_instructions_list)

        # Prepare successful JSON output
        output_json = cleaned_df.to_json(orient='records', date_format='iso')
        logging.info("Cleaning successful. Prepared JSON output.")

    except FileNotFoundError as e:
        logging.error(f"Script Error: {e}")
        output_json = json.dumps({"error": str(e)})
        exit_code = 2 # Specific exit code for file not found
    except ValueError as e: # Catch empty file error or bad JSON instructions
         logging.error(f"Script Error: {e}")
         output_json = json.dumps({"error": str(e)})
         exit_code = 3 # Specific exit code for value error
    except json.JSONDecodeError:
        logging.error("Script Error: Invalid JSON format for cleaning instructions argument.")
        output_json = json.dumps({"error": "Invalid JSON format for cleaning instructions"})
        exit_code = 3
    except Exception as e:
        logging.exception("Script Error: An unexpected error occurred during main execution.") # Log traceback
        output_json = json.dumps({"error": f"An unexpected error occurred in cleaner: {str(e)}"})
        exit_code = 1 # General error exit code
    finally:
        # Always print the result (either JSON data or error JSON) to stdout
        print(output_json)
        # Exit with the appropriate code
        exit(exit_code)
