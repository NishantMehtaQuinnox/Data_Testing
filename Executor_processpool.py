from itertools import repeat
from uuid import uuid1 
import tempfile
import os
from utils.async_s3utils import async_S3Utils
from utils.s3utils import S3Utils
import time
from connectors.redshift.connect import AsyncRedshiftConnector,RedshiftConnector
from connectors.sql.connect import AsyncMysqlConnector,MysqlConnector
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import json
from decimal import Decimal
from datetime import date, datetime
import traceback
import pandas as pd
import pathlib

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # Optionally convert decimal instances to strings
            return str(obj)
        return super(CustomJsonEncoder, self).default(obj)

async_s3utils = async_S3Utils()
s3utils = S3Utils()


def write_batch_to_json_pandas(batch_tuples, headers, batch_number):
    st_time = time.time()
    file_name = f"batch_{batch_number}_{uuid1()}.json"

    # Ensure that headers are not None or empty

    # Create Pandas DataFrame
    df = pd.DataFrame(batch_tuples, columns=headers)
    
    # Convert the DataFrame to JSON and write to a file
    file_path = pathlib.Path(tempfile.gettempdir()) / file_name
    df.to_json(file_path, orient='records', lines=True)
    
    duration = time.time() - st_time
    # print(f"Time Taken: {duration:.4f} seconds")
    return str(file_path)


# This function will convert asyncpg.Record objects into tuples (which are picklable)
def record_to_tuple(record): 
    return tuple(record.values()) if type(record) != tuple else record

def connector_selector(db,db_params):
    if db.lower() == 'mysql':
        connector = MysqlConnector(**db_params)
    elif db.lower() in ['postgresql','redshift']:
        connector = RedshiftConnector(**db_params)
        # connector = AsyncRedshiftConnector(**db_params)
    return connector

# def fetch_records_count(db,db_params, query):
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
    
#     connector = connector_selector(db,db_params)
    
#     loop.run_until_complete(connector.open_connection())
#     count = loop.run_until_complete(connector.fetch_count(query))
#     batch_size = connector.batch_size
#     loop.run_until_complete(connector.close_connection())
#     loop.close()
#     return count,batch_size


def fetch_records_count(db,db_params, query):
    connector = connector_selector(db,db_params)
    connector.open_connection()
    count = connector.fetch_count(query)
    batch_size = connector.batch_size
    connector.close_connection()
    return count,batch_size

def run_fetch_batch(db, db_params, query, batch_number):
    connector = connector_selector(db, db_params)
    connector.open_connection()
    batch, headers = connector.fetch_batch(query, batch_number)
    print(headers)
    connector.close_connection()

    # Check if headers is None or empty, setting default headers if needed
    if not headers:
        headers = ['column' + str(idx) for idx, _ in enumerate(batch[0])] if batch else []

    return batch, headers


# def run_fetch_batch(db,db_params, query, batch_number):
    
#     connector = connector_selector(db,db_params)

#     connector.open_connection()

#     batch, headers = connector.fetch_batch(query, batch_number)
    
#     # Close the DB connection
#     connector.close_connection()

#     return batch, headers

# def batch_pipeline(db,db_params, query, s3_key_prefix, batch_number):
#     batch, headers = run_fetch_batch(db,db_params, query, batch_number)
#     if not batch:
#         return 0  # No more records to fetch, end the processing for this worker

#     batch_tuples = [record_to_tuple(record) for record in batch]
#     csv_path = write_batch_to_csv(batch_tuples, headers)

#     s3_key = f"{s3_key_prefix}_part_{batch_number}.csv"
#     data = open(csv_path, 'rb').read()

#     # Upload the file in the current process' event loop
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)

#     # Asynchronously upload the file to S3
#     loop.run_until_complete(async_s3utils.upload_file(s3_key, data))

#     # Clean up files
#     os.remove(csv_path)
   
#     # Close the event loop
#     loop.close()
   
#     return 1

# def batch_pipeline(db, db_params, query, s3_key_prefix, batch_number):
#     batch, headers = run_fetch_batch(db, db_params, query, batch_number)
#     if not batch:
#         return 0  # No more records to fetch, end the processing for this worker

#     # Convert records to the desired format
#     batch_tuples = [record_to_tuple(record) for record in batch]
#     # Write batch to CSV and include headers
#     csv_path = write_batch_to_csv(batch_tuples, headers)

#     s3_key = f"{s3_key_prefix}_part_{batch_number}.csv"
#     with open(csv_path, 'rb') as f:
#         data = f.read()

#     # Upload the file to S3
#     s3utils.upload_file(s3_key, data)

#     # Clean up files
#     os.remove(csv_path)

#     return 1


# def batch_pipeline(db, db_params, query, s3_key_prefix, batch_number):
#     batch, headers = run_fetch_batch(db, db_params, query, batch_number)
#     if not batch:
#         return 0  # No more records to fetch, end the processing for this worker

#     batch_tuples = [record_to_tuple(record) for record in batch]
#     json_path = write_batch_to_json_pandas(batch_tuples, headers, batch_number)

#     s3_key = f"{s3_key_prefix}_part_{batch_number}.json"
#     with open(json_path, 'rb') as f:
#         data = f.read()

#     # Upload the file to S3
#     s3utils.upload_file(s3_key, data)
    
#     # Clean up files
#     os.remove(json_path)
    
#     return 1

def batch_pipeline(db, db_params, query, s3_key_prefix, batch_number):
    batch, headers = run_fetch_batch(db, db_params, query, batch_number)
    if not batch:
        # print(f"No more records to fetch for batch {batch_number}.")
        return 0

    batch_tuples = [record_to_tuple(record) for record in batch]
    json_path = write_batch_to_json_pandas(batch_tuples, headers, batch_number)

    # Define the S3 key and upload the JSON file
    s3_key = f"{s3_key_prefix}_part_{batch_number}.json"
    with open(json_path, 'rb') as f:
        data = f.read()

    # Upload the file to S3
    s3utils.upload_file(s3_key, data)
    
    # Clean up local temporary JSON file
    os.remove(json_path)
    # print(f"Batch {batch_number} processing and upload completed")
    
    return 1

# def batch_pipeline(db,db_params, query, s3_key_prefix, batch_number):
#     batch, headers = run_fetch_batch(db,db_params, query, batch_number)
#     if not batch:
#         return 0  # No more records to fetch, end the processing for this worker

#     batch_tuples = [record_to_tuple(record) for record in batch]
#     csv_path = write_batch_to_csv(batch_tuples, headers)

#     s3_key = f"{s3_key_prefix}_part_{batch_number}.csv"
#     data = open(csv_path, 'rb').read()

#     # Upload the file in the current process' event loop
#     # loop = asyncio.new_event_loop()
#     # asyncio.set_event_loop(loop)

#     # Asynchronously upload the file to S3
#     # loop.run_until_complete(async_s3utils.upload_file(s3_key, data))
#     s3utils.upload_file(s3_key, data)

#     # Clean up files
#     os.remove(csv_path)
#     # loop.close()
   
#     return 1



def main():
    redshiftparams = {
        'database': 'dev',
        'user': 'admin',
        'password': 'Qyrus#789',
        'host': 'ai-rs-poc.293963594940.ap-south-1.redshift-serverless.amazonaws.com',
        'port': 5439
    }
    
    mysqlparmas = {
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword',
    'host': '127.0.0.1'
    }
    
    db_params = redshiftparams

    db = "mysql" if db_params == mysqlparmas else "redshift"
    db_query = "SELECT * FROM transactions"
    uuid = str(uuid1())
    print(uuid)
    s3_key_prefix = f'data_testing/{uuid}/{uuid}'
    
    rows_count, batch_size = fetch_records_count(db,db_params, db_query)
    batch_numbers = range(-(-rows_count // batch_size))

    # Create a progress bar to track the process
    progress_bar = tqdm(total=len(batch_numbers), desc='Processing Batches', unit='batch', unit_scale=True)

    # Start processing and timing
    st_time = time.time()
    max_workers = os.cpu_count() - 1 or 1
    
    try:

        # Use ProcessPoolExecutor to process each batch in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for batch_processed in executor.map(
                batch_pipeline,
                repeat(db),
                repeat(db_params),
                repeat(db_query),
                repeat(s3_key_prefix),
                batch_numbers
            ):
                progress_bar.update(batch_processed)

        # Clean up progress bar
        progress_bar.close()
        
        #print(f"Total Time: {time.time() - st_time}")
        
    except Exception as e:
        traceback.print_exc() 
        
if __name__ == "__main__":
    main()
