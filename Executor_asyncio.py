import asyncio
import asyncpg
import csv
import uuid
from tqdm import tqdm
import tempfile
import os
from utils.async_s3utils import async_S3Utils
import time
from connectors.redshift.connect import AsyncRedshiftConnector
from connectors.sql.connect import AsyncMysqlConnector


async_s3utils = async_S3Utils()

async def write_batch_to_csv(batch, headers):
    fd, path = tempfile.mkstemp(suffix='.csv', prefix='batch_', text=True)
    with open(fd, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if headers:  # Write headers only for the first batch
            writer.writerow(headers)
        for record in batch:
            writer.writerow(record)
    return path

def record_to_tuple(record): 
    return tuple(record.values()) if type(record) != tuple else record

def open_read_close(file_path):
    with open(file_path, 'r') as file:
        return file.read()

async def batch_pipeline(connection, query, s3_key_prefix, progress_bar):
    batch_number = 0

    while True:
        batch, headers = await connection.fetch_batch(query, batch_number)
        if not batch:
            break

        # Convert asyncpg.Record objects to tuples (assuming they aren't already)
        batch_tuples = [record_to_tuple(record) for record in batch]

        # Asynchronously write batch to CSV
        csv_path = await write_batch_to_csv(batch_tuples, headers)

        s3_key = f"{s3_key_prefix}_part_{batch_number}.csv"
        data = await asyncio.to_thread(open_read_close, csv_path)
        upload_task = asyncio.create_task(async_s3utils.upload_file(s3_key, data))

        await upload_task
        os.remove(csv_path)
        progress_bar.update(1)

        batch_number += 1


async def main():
    # Set the connection params for asyncpg
    conn_params = {
    'database': 'dev',
    'user': 'admin',
    'password': 'Qyrus#789',
    'host': 'ai-rs-poc.293963594940.ap-south-1.redshift-serverless.amazonaws.com',
    'port': 5439
}

#     conn_params = {
#     'database': 'mydatabase',
#     'user': 'myuser',
#     'password': 'mypassword',
#     'host': '127.0.0.1'
# }


    db = "redshift"
    db_query = "SELECT * FROM transactions"
    if db.lower() == 'mysql':
        connector = AsyncMysqlConnector(**conn_params)
    elif db.lower() in ['postgresql','redshift']:
        connector = AsyncRedshiftConnector(**conn_params)
    # Open MySQL connection
    await connector.open_connection()

    # S3 bucket details
    s3_key_prefix = f'data_testing/{uuid.uuid1()}'
    st_time = time.time()
    try:
        # We use tqdm with unknown total, using the 'unit_scale' to show the iterations
        with tqdm(desc='Processing Batches', unit='batch', unit_scale=True) as progress_bar:
            # No total is passed to tqdm
            await batch_pipeline(connector,db_query, s3_key_prefix, progress_bar)
    finally:    
        print("Total Time : " + str(time.time()-st_time))
        await connector.close_connection()
asyncio.run(main())


# Imports and utility functions remain the same

# The 'record_to_tuple' utility function remains the same


# The `main` function remains mostly unchanged.
