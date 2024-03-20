import os
import time
from itertools import repeat
import pandas as pd
import pathlib
import tempfile
from uuid import uuid1
from utils.s3utils import S3Utils
from connectors.redshift.connect import RedshiftConnector
from connectors.sql.connect import MysqlConnector
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import traceback

s3utils = S3Utils()

class DataFetchingPipeline:
    def __init__(self, db, db_params, db_query, s3_key_prefix):
        self.db = db
        self.db_params = db_params
        self.db_query = db_query
        self.s3_key_prefix = s3_key_prefix
        self.uuid = str(uuid1())

    def write_batch_to_json_pandas(self, batch_tuples, headers, batch_number):
        st_time = time.time()
        file_name = f"batch_{batch_number}_{uuid1()}.json"
        df = pd.DataFrame(batch_tuples, columns=headers)
        file_path = pathlib.Path(tempfile.gettempdir()) / file_name
        df.to_json(file_path, orient='records', lines=True)
        duration = time.time() - st_time
        return str(file_path)

    def record_to_tuple(self, record):
        return tuple(record.values()) if type(record) != tuple else record

    def connector_selector(self, db):
        if db.lower() == 'mysql':
            connector = MysqlConnector(**self.db_params)
        elif db.lower() in ['postgresql', 'redshift']:
            connector = RedshiftConnector(**self.db_params)
        return connector

    def fetch_records_count(self):
        connector = self.connector_selector(self.db)
        connector.open_connection()
        count = connector.fetch_count(self.db_query)
        batch_size = connector.batch_size
        connector.close_connection()
        return count, batch_size

    def run_fetch_batch(self, batch_number):
        connector = self.connector_selector(self.db)
        connector.open_connection()
        batch, headers = connector.fetch_batch(self.db_query, batch_number)
       
        connector.close_connection()

        return batch, headers

    def batch_pipeline(self, batch_number):
        batch, headers = self.run_fetch_batch(batch_number)
        if not batch:
            return 0

        batch_tuples = [self.record_to_tuple(record) for record in batch]
        json_path = self.write_batch_to_json_pandas(batch_tuples, headers, batch_number)
        s3_key = f"{self.s3_key_prefix}_part_{batch_number}.json"
        
        with open(json_path, 'rb') as f:
            data = f.read()
        
        s3utils.upload_file(s3_key, data)
        os.remove(json_path)
        return 1

    def start_migration(self):
        
        st_time = time.time()
        rows_count, batch_size = self.fetch_records_count()
        batch_numbers = range(-(-rows_count // batch_size))

        progress_bar = tqdm(total=len(batch_numbers), desc='Processing Batches', unit='batch', unit_scale=True)
        max_workers = os.cpu_count() - 1 or 1

        try:
            # Use ProcessPoolExecutor to process each batch in parallel
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                for batch_processed in executor.map(
                    self.batch_pipeline,
                    batch_numbers
                ):
                    progress_bar.update(batch_processed)

            print(f"Total Time: {time.time() - st_time}")
            
        except:
            traceback.print_exc()

if __name__ == "__main__":
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

    db_params = redshiftparams  # This should be set based on the DB you want to use
    db_query = "SELECT * FROM transactions"
    uuid_str = str(uuid1())
    print(uuid_str)
    s3_key_prefix = f'data_testing/{uuid_str}/{uuid_str}'
    db = "mysql" if db_params == mysqlparmas else "redshift"
    migration_pipeline = DataFetchingPipeline(db , db_params, db_query, s3_key_prefix)
    migration_pipeline.start_migration()
