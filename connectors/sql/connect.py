import mysql.connector
from mysql.connector import Error

class MysqlConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def open_connection(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print(f"Connected to MySQL Server version {db_info}")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")

    def fetch_all_records(self, table_name):
        return self.fetch_specific_records(f"SELECT * FROM {table_name}")

    def fetch_specific_records(self, query):
        records = []
        if self.connection is not None and self.connection.is_connected():
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                records = cursor.fetchall()
                print(f"Number of rows returned: {cursor.rowcount}")
                cursor.close()
            except Error as e:
                print(f"Error executing query: {e}")
        return records

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")


# host = '127.0.0.1'
# database = 'mydatabase'
# user = 'myuser'
# password = 'mypassword'
# table_name = 'users_table'


# db_connection = MysqlConnector(host, database, user, password)
# db_connection.open_connection()

# # Fetch all records
# all_records = db_connection.fetch_all_records(table_name)
# for record in all_records:
#     print(record)

# # Fetch specific records with custom query
# custom_query = "SELECT * FROM users_table WHERE id > 12"
# specific_records = db_connection.fetch_specific_records(custom_query)
# for record in specific_records:
#     print(record)

# db_connection.close_connection()
