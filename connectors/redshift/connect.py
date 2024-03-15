
import asyncpg
import time

class AsyncRedshiftConnector:
    def __init__(self, host, database, user, password, port=5439):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.batch_size = 100000
        
    async def add_limit_offset(self,query,batch_size,offset):
        return f"{query} LIMIT {batch_size} OFFSET {offset}"

    async def open_connection(self):
        try:
            self.connection = await asyncpg.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            print(f"Connected to Redshift cluster {self.host}")
        except Exception as e:
            print(f"Error while connecting to Redshift: {e}")

    async def fetch_all_records(self, table_name):
        return await self.fetch_specific_records(f"SELECT * FROM {table_name}")
    
    async def fetch_count(self, base_query):
        """
        Fetch the count of rows for a given query.
        
        :param base_query: The base query to count rows for.
        :return: The count of rows.
        """
        # Construct the count query from the base query
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_subquery"

        async with self.connection.transaction():
            # Execute the count query
            count = await self.connection.fetchval(count_query)
            return count

    async def fetch_batch(self, query, batch_number):
        offset = batch_number * self.batch_size
        # Modify query to include LIMIT and OFFSET
        # Ensure that your query_template includes '{}' placeholders for LIMIT and OFFSET
        batch_query = await self.add_limit_offset(query,self.batch_size,offset)
        async with self.connection.transaction():
            # Execute the batch query
            records = await self.connection.fetch(batch_query)
            if records:
                # Extract headers only if we have records, and only for the first batch
                headers = records[0].keys() if batch_number == 0 else None
                return records, headers
            return [], None

    async def record_to_tuple(record):
        return tuple(record.values())

    async def close_connection(self):
        if self.connection:
            await self.connection.close()
            print("Redshift connection is closed")
