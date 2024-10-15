import json
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime
from google.api_core import retry
from google.api_core import exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from itertools import islice
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# BigQuery settings
PROJECT_ID = 'faqta-datalake'
DATASET_ID = 'raw_events'
TABLE_ID = 'events_23_24'

# Path to your service account key file
KEY_PATH = "./keys/faqta-datalake-b58e5ef55323.json"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Load the optimized reference JSON files
reference_files = ['orgs', 'course', 'tiles', 'roles', 'channels']
lookups = {}

for ref in reference_files:
    try:
        with open(f'./reference/optimized_{ref}.json', 'r', encoding='utf-8') as f:
            lookups[ref] = json.load(f)
        logging.info(f"Loaded reference file: optimized_{ref}.json")
    except FileNotFoundError:
        logging.error(f"Reference file not found: optimized_{ref}.json")
        raise

def get_lookup_id(lookup_type, name):
    return lookups[lookup_type].get(name)

# Get table reference
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

# Function to ensure JSON serializable
def ensure_serializable(value):
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (list, tuple)):
        return [ensure_serializable(item) for item in value]
    elif isinstance(value, dict):
        return {k: ensure_serializable(v) for k, v in value.items()}
    else:
        return str(value)

# Function to map Mixpanel data to BigQuery schema
def map_to_schema(event):
    properties = event.get('properties', {})
    event_name = event.get('event', '')
    org_name = properties.get('Organisation name', [''])[0]
    
    mapped = {
        'moduleName': event_name,
        'type': properties.get('type'),
        'action': properties.get('action'),
        'operatingSystem': properties.get('$os'),
        'browser': properties.get('$browser'),
        'browserVersion': properties.get('$browser_version'),
        'screenHeight': properties.get('$screen_height'),
        'screenWidth': properties.get('$screen_width'),
        'groupId': properties.get('groupId'),
        'organisationId': get_lookup_id('orgs', org_name),
        'channelId': get_lookup_id('channels', properties.get('channelName', '')),
        'courseId': get_lookup_id('course', properties.get('courseName', '')),
        'userId': properties.get('distinct_id'),
        'detail': properties.get('detail'),
        'amount': properties.get('amount'),
        'page': properties.get('$current_url'),
        'roleId': get_lookup_id('roles', properties.get('roleName', '')),
        'tileId': get_lookup_id('tiles', properties.get('tileName', '')),
        'assignmentId': properties.get('assignmentId'),
        'assignmentOrder': None,
        'teachingLevel': properties.get('teachingLevel'),
        'timestamp': datetime.fromtimestamp(properties.get('time', 0)).isoformat()
    }
    return {k: ensure_serializable(v) for k, v in mapped.items()}

# Function to check if the table exists
def table_exists():
    try:
        client.get_table(table_ref)
        # logging.info("BigQuery table exists")
        return True
    except exceptions.NotFound:
        logging.error("BigQuery table not found")
        return False

# Function to insert rows with retry logic
@retry.Retry(predicate=retry.if_exception_type(
    exceptions.ServerError,
    exceptions.BadRequest,
    exceptions.NotFound
))
def insert_rows_with_retry(rows):
    if not table_exists():
        raise Exception("Table does not exist")
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logging.error(f"Errors encountered while inserting batch: {errors}")
        raise Exception(f"Encountered errors while inserting batch: {errors}")
    # logging.info(f"Successfully inserted {len(rows)} rows")

# Global variables
total_rows_inserted = 0
rows_to_insert_queue = []

# Function to process a chunk of lines
def process_chunk(chunk, filename):
    global rows_to_insert_queue
    rows_to_insert = []
    for line in chunk:
        try:
            event = json.loads(line)
            row = map_to_schema(event)
            rows_to_insert.append(row)
        except json.JSONDecodeError:
            logging.warning(f"Encountered invalid JSON in {filename}")
    
    if rows_to_insert:
        rows_to_insert_queue.extend(rows_to_insert)
    
    return len(rows_to_insert)

# Function to insert rows in batches
def insert_rows_batch(rows):
    global total_rows_inserted
    try:
        insert_rows_with_retry(rows)
        total_rows_inserted += len(rows)
        #logging.info(f"Inserted batch of {len(rows)} rows. Total rows inserted: {total_rows_inserted}")
    except Exception as e:
        logging.error(f"Error inserting batch: {str(e)}")

# Function to process a single JSON file
def process_file(filename):
    global rows_to_insert_queue
    start_time = time.time()
    total_lines = 1_000_000  # Assuming each file has exactly 1,000,000 lines
    chunk_size = 1000
    processed_lines = 0
    insertion_threshold = 10000  # Number of rows to accumulate before inserting

    with open(filename, 'r') as file:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            while True:
                chunk = list(islice(file, chunk_size))
                if not chunk:
                    break
                future = executor.submit(process_chunk, chunk, filename)
                futures.append(future)
                
                processed_lines += len(chunk)
                progress = (processed_lines / total_lines) * 100
                print(f"\rReading {filename}: {progress:.2f}% complete.", end="")
                
                if len(rows_to_insert_queue) >= insertion_threshold:
                    batch = rows_to_insert_queue[:insertion_threshold]
                    rows_to_insert_queue = rows_to_insert_queue[insertion_threshold:]
                    executor.submit(insert_rows_batch, batch)
            
            for future in as_completed(futures):
                pass
    
    while rows_to_insert_queue:
        batch = rows_to_insert_queue[:insertion_threshold]
        rows_to_insert_queue = rows_to_insert_queue[insertion_threshold:]
        insert_rows_batch(batch)
        insertion_progress = (total_rows_inserted / total_lines) * 100
        print(f"\rInserting into BigQuery: {insertion_progress:.2f}% complete.", end="")
    
    logging.info(f"Completed processing {filename}. Total time: {time.time() - start_time:.2f} seconds")
    logging.info(f"Total rows inserted: {total_rows_inserted}")

# Main execution
def main():
    json_files = [f for f in os.listdir('.') if f.endswith('.json')]
    total_files = len(json_files)

    for i, filename in enumerate(json_files, 1):
        logging.info(f"Processing file {i} of {total_files}: {filename}")
        process_file(filename)

    logging.info("Import process completed.")
    logging.info(f"Grand total of rows inserted: {total_rows_inserted}")

if __name__ == "__main__":
    main()