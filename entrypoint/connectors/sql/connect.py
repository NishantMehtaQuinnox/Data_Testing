import aiomysql     

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
            
    async def execute_query(self, query, params=None):
        """
        Execute a specific query with optional parameters, commit changes if it's an action query,
        and fetch all rows if it's a selection query.

        :param query: The query to execute.
        :param params: Optional parameters for the query.
        :return: A tuple with affected rows, last insert ID, or fetched rows as applicable.
        """
        async with self.connection.cursor(aiomysql.DictCursor) as cursor:
            try:
                await cursor.execute(query, params)
                
                # Check if the operation is a SELECT query
                if query.strip().upper().startswith("SELECT"):
                    # It's a SELECT query, so fetch data
                    records = await cursor.fetchall()
                    print("Query executed successfully: fetched {} rows.".format(len(records)))
                    return records
                else:
                    # It's an action query, so commit the changes and return affected rows
                    await self.connection.commit()
                    affected = cursor.rowcount
                    print("Query executed: {} rows affected.".format(affected))
                    return affected
            except Exception as e:
                await self.connection.rollback()
                print("Error executing query: {}".format(e))
                raise  # Re-raising the exception is preferable over returning None for failure cases
            
    async def close_connection(self):
        if self.connection:
            self.connection.close()
            print("MySQL connection is closed")