from itertools import repeat
import csv
import uuid
import tempfile
import os
from utils.async_s3utils import async_S3Utils
from utils.s3utils import S3Utils
import time
from connectors.redshift.connect import AsyncRedshiftConnector,RedshiftConnector
from connectors.sql.connect import AsyncMysqlConnector,MysqlConnector
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import asyncio



async_s3utils = async_S3Utils()
s3utils = S3Utils()

# No longer an async function
def write_batch_to_csv(batch, headers):
    fd, path = tempfile.mkstemp(suffix='.csv', prefix='batch_', text=True)
    with open(fd, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if headers:  # Write headers only for the first batch
            writer.writerow(headers)
        for record in batch:
            writer.writerow(record)
    return path

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

# def run_fetch_batch(db,db_params, query, batch_number):
#     # Create a new event loop for each new process
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
    
#     connector = connector_selector(db,db_params)

#     loop.run_until_complete(connector.open_connection())

#     # Fetch the batch
#     batch, headers = loop.run_until_complete(connector.fetch_batch(query, batch_number))
    
#     # Close the DB connection
#     loop.run_until_complete(connector.close_connection())

#     # Close the event loop
#     loop.close()

#     return batch, headers


def run_fetch_batch(db,db_params, query, batch_number):
    
    connector = connector_selector(db,db_params)

    connector.open_connection()

    # Fetch the batch
    batch, headers = connector.fetch_batch(query, batch_number)
    
    # Close the DB connection
    connector.close_connection()

    return batch, headers

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


def batch_pipeline(db,db_params, query, s3_key_prefix, batch_number):
    batch, headers = run_fetch_batch(db,db_params, query, batch_number)
    if not batch:
        return 0  # No more records to fetch, end the processing for this worker

    batch_tuples = [record_to_tuple(record) for record in batch]
    csv_path = write_batch_to_csv(batch_tuples, headers)

    s3_key = f"{s3_key_prefix}_part_{batch_number}.csv"
    data = open(csv_path, 'rb').read()

    # Upload the file in the current process' event loop
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)

    # Asynchronously upload the file to S3
    # loop.run_until_complete(async_s3utils.upload_file(s3_key, data))
    s3utils.upload_file(s3_key, data)

    # Clean up files
    os.remove(csv_path)
    # loop.close()
   
    return 1



def main():
    # db_params = {
    #     'database': 'dev',
    #     'user': 'admin',
    #     'password': 'Qyrus#789',
    #     'host': 'ai-rs-poc.293963594940.ap-south-1.redshift-serverless.amazonaws.com',
    #     'port': 5439
    # }
    
    
    db_params = {
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword',
    'host': '127.0.0.1'
    }


    db = "mysql"
    db_query = "SELECT * FROM transactions"
    s3_key_prefix = f'data_testing/{uuid.uuid1()}'
    
    rows_count, batch_size = fetch_records_count(db,db_params, db_query)
    batch_numbers = range(-(-rows_count // batch_size))

    # Create a progress bar to track the process
    progress_bar = tqdm(total=len(batch_numbers), desc='Processing Batches', unit='batch', unit_scale=True)

    # Start processing and timing
    st_time = time.time()

    # Use ProcessPoolExecutor to process each batch in parallel
    with ProcessPoolExecutor() as executor:
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
    
    print(f"Total Time: {time.time() - st_time}")
 
if __name__ == "__main__":
    main()

# import concurrent.futures

# def main():
#     db_params = {
#         'database': 'dev',
#         'user': 'admin',
#         'password': 'Qyrus#789',
#         'host': 'ai-rs-poc.293963594940.ap-south-1.redshift-serverless.amazonaws.com',
#         'port': 5439
#     }

#     db_query = "SELECT * FROM transactions"
#     s3_key_prefix = f'data_testing/{uuid.uuid1()}'
    
#     # Start processing and timing
#     st_time = time.time()

#     connector = AsyncRedshiftConnector(**db_params)
#     asyncio.run(connector.open_connection())

#     try:
#         # Fetch the total number of rows to process
#         total_rows = asyncio.run(connector.fetch_count(db_query))
#         asyncio.run(connector.close_connection())
#         # Calculate the required number of batches
#         batch_size = connector.batch_size  # Assuming 100000 rows per batch as per constructor definition
#         total_batches = -(-total_rows // batch_size)  # Ceiling division for total batch count

#         # Start processing batches with a progress bar
#         with tqdm(total=total_batches, desc='Processing Batches', unit='batch') as progress_bar:
            
#             # Use ProcessPoolExecutor to process each batch in parallel
#             with ProcessPoolExecutor() as executor:
#                 futures = {
#                     executor.submit(batch_pipeline, db_params, db_query, s3_key_prefix, i): i
#                     for i in range(total_batches)
#                 }

#                 # Process completed futures
#                 for future in concurrent.futures.as_completed(futures):
#                     progress_bar.update(1)  # Increment the progress bar for each completed batch

#     finally:
#         # Clean up connection and progress bar
#         progress_bar.close()
#         print(f"Total Time: {time.time() - st_time}")

# if __name__ == "__main__":
#     main()
