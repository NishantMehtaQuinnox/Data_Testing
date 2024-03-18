import time
import aiomysql     
import mysql.connector


class AsyncMysqlConnector:
    def __init__(self, host, database, user, password, port=3306):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.batch_size = 10000

    async def open_connection(self):
        try:
            self.connection = await aiomysql.connect(
                host=self.host,
                port=self.port,
                db=self.database,
                user=self.user,
                password=self.password
            )
            db_info = self.connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
        except Exception as e:
            print(f"Error while connecting to MySQL: {e}")

    async def fetch_all_records(self, table_name):
        return await self.fetch_specific_records(f"SELECT * FROM {table_name}")

    async def add_limit_offset(self,query,batch_size,offset):
        return f"{query} LIMIT {batch_size} OFFSET {offset}"
    
    async def fetch_count(self, base_query):
        """
        Fetch the count of rows for a given query.

        :param base_query: The base query to count rows for.
        :return: The count of rows.
        """
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_subquery"
        async with self.connection.cursor() as cursor:
            await cursor.execute(count_query)
            count = await cursor.fetchone()
            return count[0]
        
    async def fetch_batch(self, query, batch_number):
        offset = batch_number * self.batch_size
        # Modify query to include LIMIT and OFFSET
        batch_query = await self.add_limit_offset(query, self.batch_size, offset)
        async with self.connection.cursor() as cursor:
            await cursor.execute(batch_query)
            records = await cursor.fetchall()
            if records:
                # Extract headers only if we have records, and only for the first batch
                headers = [description[0] for description in cursor.description] if batch_number == 0 else None
                return records, headers
            return [], None
     
    async def fetch_specific_records(self, query):
        async with self.connection.cursor(aiomysql.DictCursor) as cursor:
            st_time = time.time()
            await cursor.execute(query)
            print(f"Time for execute query : {time.time()-st_time}")
            st_time = time.time()
            records = await cursor.fetchall()
            
            print(f"Time for fetching all records from query : {time.time()-st_time}")
            print(f"Number of rows returned: {len(records)}")
            return records

    async def close_connection(self):
        if self.connection:
            self.connection.close()
            print("MySQL connection is closed")
            
      
class MysqlConnector:
    def __init__(self, host, database, user, password, port=3306):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None
        self.batch_size = 10000
        
    def open_connection(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print(f"Connected to MySQL Server version {db_info}")
                self.cursor = self.connection.cursor()
        except Exception as e:
            print(f"Error while connecting to MySQL: {e}")

    def fetch_all_records(self, table_name):
        return self.fetch_specific_records(f"SELECT * FROM {table_name}")

    def fetch_count(self, base_query):
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_subquery"
        self.cursor.execute(count_query)
        count = self.cursor.fetchone()
        return count[0]

    def add_limit_offset(self, query, batch_size, offset):
        return f"{query} LIMIT {batch_size} OFFSET {offset}"

    def fetch_batch(self, query, batch_number):
        offset = batch_number * self.batch_size
        batch_query = self.add_limit_offset(query, self.batch_size, offset)
        self.cursor.execute(batch_query)
        records = self.cursor.fetchall()

        headers = [description[0] for description in self.cursor.description] if batch_number == 0 else None
        return records, headers

    def fetch_specific_records(self, query):
        st_time = time.time()
        self.cursor.execute(query)
        print(f"Time for execute query : {time.time() - st_time}")
        st_time = time.time()
        records = self.cursor.fetchall()
        print(f"Time for fetching all records from query : {time.time() - st_time}")
        print(f"Number of rows returned: {len(records)}")
        return records

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("MySQL connection is closed")
