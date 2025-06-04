import requests
import time
import psycopg2
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import Json
import os
from dotenv import load_dotenv
import warnings
from io import StringIO
import json # For temp table
import csv # Import csv module for quoting control
import random

# Load environment variables
load_dotenv('auth.env')

# Database configuration
DATABASE_CONFIG = {
    'host': 'prd-usc-itworks-postgresql-65ba.postgres.database.azure.com',
    'database': 'sd_reports',
    'user': os.getenv('azure_user'),
    'password': os.getenv('azure_pw'),
    'port': '5432'
}

# API configuration
BASE_API_URL = "https://api.moveworks.ai/export/v1beta2/records/conversations"
TABLE_NAME = os.getenv('TABLE_NAME', 'sigi.conversations')
HEADERS = {
    'Authorization': f"Bearer {os.getenv('api_token')}",
    'Content-Type': 'application/json'
}

def get_all_api_data():
    """Fetch all paginated data from the API with improved retry logic."""
    all_records = []
    next_url = BASE_API_URL
    MAX_RETRIES = 10  # Increased max retries for more robustness
    INITIAL_DELAY = 1  # Initial delay in seconds
    MAX_DELAY = 120  # Maximum delay in seconds (e.g., 2 minutes)
    
    # Store the state of the last fetched URL in case of interruption
    # (Optional, but good for very large fetches)
    last_successful_url = None

    while next_url:
        retries = 0
        current_url = next_url
        while retries < MAX_RETRIES:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", requests.packages.urllib3.exceptions.InsecureRequestWarning)
                    response = requests.get(current_url, headers=HEADERS, verify=False)

                if response.status_code == 429:
                    wait_time = INITIAL_DELAY * (2 ** retries)
                    
                    # 1. Respect Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            # The header can be a number of seconds or a date.
                            # For simplicity, assume it's seconds if it's a number.
                            wait_time = int(retry_after)
                            print(f"API requested retry after {wait_time} seconds (from Retry-After header).")
                        except ValueError:
                            # If it's a date string, you'd parse it to a timestamp and calculate delay.
                            # For now, fall back to exponential backoff if parsing fails.
                            print(f"Could not parse Retry-After header '{retry_after}', using exponential backoff.")
                    
                    # Add jitter to the wait time
                    wait_time = min(wait_time + random.uniform(0, 0.5 * wait_time), MAX_DELAY) # Jitter up to 50% of wait_time
                    
                    print(f"Received 429 (Too Many Requests). Retrying in {wait_time:.2f} seconds. Attempt {retries + 1}/{MAX_RETRIES} for URL: {current_url}")
                    time.sleep(wait_time)
                    retries += 1
                    continue # Try the same URL again

                response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
                
                data = response.json()
                if 'value' in data:
                    all_records.extend(data['value'])
                next_url = data.get('@odata.nextLink')
                
                print(f"Fetched {len(data.get('value', []))} records (total: {len(all_records)}) from: {current_url}")
                last_successful_url = current_url # Record successful fetch
                break # Break from retry loop, move to next_url

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - for URL: {current_url}")
                # For other HTTP errors (e.g., 401, 404, 500), you might want to stop or log more aggressively
                if 500 <= response.status_code < 600: # Server errors, worth retrying
                    wait_time = min(INITIAL_DELAY * (2 ** retries) + random.uniform(0, 0.5 * INITIAL_DELAY * (2 ** retries)), MAX_DELAY)
                    print(f"Retrying in {wait_time:.2f} seconds due to server error. Attempt {retries + 1}/{MAX_RETRIES}")
                    time.sleep(wait_time)
                    retries += 1
                    continue
                else: # Client errors or other non-retryable errors
                    print(f"Non-retryable HTTP error: {http_err}. Stopping.")
                    next_url = None # Stop fetching
                    break # Break from retry loop

            except requests.exceptions.ConnectionError as conn_err:
                print(f"Connection error occurred: {conn_err} - for URL: {current_url}")
                wait_time = min(INITIAL_DELAY * (2 ** retries) + random.uniform(0, 0.5 * INITIAL_DELAY * (2 ** retries)), MAX_DELAY)
                print(f"Retrying in {wait_time:.2f} seconds due to connection error. Attempt {retries + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                retries += 1
                continue

            except requests.exceptions.Timeout as timeout_err:
                print(f"Timeout error occurred: {timeout_err} - for URL: {current_url}")
                wait_time = min(INITIAL_DELAY * (2 ** retries) + random.uniform(0, 0.5 * INITIAL_DELAY * (2 ** retries)), MAX_DELAY)
                print(f"Retrying in {wait_time:.2f} seconds due to timeout. Attempt {retries + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                retries += 1
                continue

            except requests.exceptions.RequestException as e:
                # Catch-all for any other request-related exceptions
                print(f"General request error: {e} for URL: {current_url}. Stopping.")
                next_url = None # Stop fetching
                break # Break from retry loop
        
        # If the inner while loop completed all retries without success for the current_url
        if retries == MAX_RETRIES and response.status_code != 200: # Check if last attempt was not 200 OK
            print(f"Failed to fetch data after {MAX_RETRIES} retries for URL: {current_url}. Stopping.")
            # You might want to save `last_successful_url` here for debugging or manual resume
            break # Exit the outer while loop (stop fetching)
    
    return all_records

def connect_to_database():
    """Establish a connection to the database"""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def insert_data_into_db(conn, records):
    """Insert data into the database using temporary table for bulk operations"""
    if not records:
        print("No records to process.")
        return 0

    # Convert records to DataFrame
    df = pd.DataFrame(records)

    # Rename columns to match database schema
   # df = df.rename(columns={
    #    'created_time': 'created_at',
     #   'last_updated_time': 'last_updated_at',
      #  'detail': 'details'
    #})

    # Convert all column names to lowercase
    df.columns = df.columns.str.lower()

    cursor = conn.cursor()
    try:
        # Create temporary table
        temp_table = "temp_import_table"
        cursor.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(
            sql.Identifier(temp_table),
            sql.Identifier(*TABLE_NAME.split('.'))
        ))
        
        # Prepare CSV data for COPY command
        output = StringIO()
        
        # Explicitly handle JSON/dict columns and other column types for CSV output
        df_copy = df.copy() # Work on a copy to avoid SettingWithCopyWarning
        
        # Determine which columns are JSON/dict type from the API response
        # (This is an assumption based on your 'details' column)
        # More robust: check actual schema or sample data
        json_cols = []
        for col in df_copy.columns:
            # A pragmatic check: if any non-null value is a dict or list
            if not df_copy[col].dropna().empty and df_copy[col].dropna().apply(lambda x: isinstance(x, (dict, list))).any():
                json_cols.append(col)

        for col in df_copy.columns:
            if col in json_cols:
                # Convert Python dict/list to JSON string. Handle None specifically for JSON null.
                df_copy[col] = df_copy[col].apply(lambda x: json.dumps(x, separators=(',', ':')) if x is not None else 'null')
            else:
                # For non-JSON columns, replace Python None with empty string or specific NULL marker
                # For CSV mode, empty string is often treated as NULL if the column is nullable.
                # Or you can use a string that COPY NULL option recognizes, e.g., ''
                df_copy[col] = df_copy[col].fillna('') # Fill None with empty string

        # Now, df_copy.to_csv will write data compatible with PostgreSQL's CSV mode
        # Key change: Use quoting=csv.QUOTE_MINIMAL and ensure doublequote=True (default for QUOTE_MINIMAL)
        # This will quote fields only if necessary (contain delimiter, newline, quotechar)
        # And escape internal quotes by doubling them. This is standard CSV behavior.
        df_copy.to_csv(
            output,
            sep='\t',            # Still use tab as delimiter
            header=False,
            index=False,
            # na_rep='\\N' for NULLs can be used here if you prefer,
            # but for CSV mode with nullable columns, empty strings often work.
            # If `details` column cannot be truly NULL but must be `json null`, 'null' string is better.
            # We already handled `None` to `'null'` for JSON columns.
            # For other columns, if you want actual SQL NULL, use `na_rep=''` here
            # and `WITH NULL AS ''` in COPY.
            # Let's keep it simple and rely on `fillna('')` and then `WITH NULL AS ''` in COPY.
            quoting=csv.QUOTE_MINIMAL, # Quote fields only when necessary
            doublequote=True,          # Standard CSV behavior for internal quotes
            quotechar='"'              # Default quote character
        )
        
        output.seek(0)

        # Get column names
        columns = df_copy.columns.tolist() # Use columns from df_copy

        # Copy data to temp table
        # NEW COPY COMMAND: Use CSV mode, specify DELIMITER, NULL, and ESCAPE char (if internal quotes need escaping)
        # PostgreSQL's COPY CSV mode handles internal quoting by doubling the quotechar by default.
        # So, ESCAPE is often not explicitly needed here unless your data contains literal backslashes that need escaping.
        copy_sql = sql.SQL("""
            COPY {} ({})
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER E'\t', NULL '', HEADER FALSE)
        """).format( # Removed ESCAPE, changed to FORMAT CSV
            sql.Identifier(temp_table),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        cursor.copy_expert(copy_sql, output)

        # Insert only new records using NOT EXISTS
        insert_sql = sql.SQL("""
            INSERT INTO {}
            SELECT t.* FROM {} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {} p
                WHERE p.id = t.id
            )
        """).format(
            sql.Identifier(*TABLE_NAME.split('.')),
            sql.Identifier(temp_table),
            sql.Identifier(*TABLE_NAME.split('.'))
        )
        cursor.execute(insert_sql)
        
        inserted_count = cursor.rowcount
        conn.commit()
        skipped_count = len(df) - inserted_count
        print(f"Successfully inserted {inserted_count} records into {TABLE_NAME}.")
        print(f"Skipped {skipped_count} duplicate records.")
        return inserted_count
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into database: {e}")
        raise
    finally:
        cursor.close()

def main():
    print("Starting data ingestion process...")
    # Step 1: Fetch all data from API
    print("Fetching data from API (this may take a while)...")
    all_records = get_all_api_data()
    if not all_records:
        print("No data received from API. Exiting.")
        return
    print(f"Total records to process: {len(all_records)}")

    # Step 2: Connect to database
    print("Connecting to database...")
    conn = connect_to_database()
    if not conn:
        print("Database connection failed. Exiting.")
        return

    # Step 3: Insert data in batches
    BATCH_SIZE = 5000  # Increased batch size since we're using bulk operations
    total_inserted = 0
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i+BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} records)...")
        inserted = insert_data_into_db(conn, batch)
        total_inserted += inserted
    conn.close()
    print(f"Database connection closed. Total records inserted: {total_inserted}")
    print("Script execution completed.")

if __name__ == "__main__":
    # Suppress only the InsecureRequestWarning
    warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    main()