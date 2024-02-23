import psycopg2
from psycopg2 import Error

class RedshiftConnector:
    def __init__(self, host, database, user, password, port=5439):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None

    def open_connection(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                dbname=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            if self.connection:
                print(f"Connected to Redshift cluster {self.host}")
        except Error as e:
            print(f"Error while connecting to Redshift: {e}")

    def fetch_all_records(self, table_name):
        return self.fetch_specific_records(f"SELECT * FROM {table_name}")

    def fetch_specific_records(self, query):
        records = []
        if self.connection is not None:
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
        if self.connection:
            self.connection.close()
            print("Redshift connection is closed")


host = 'your-redshift-cluster-url'
database = 'your-database-name'
user = 'your-username'
password = 'your-password'
table_name = 'your-table-name'
port = 5439
rs_connection = RedshiftConnector(host, database, user, password, port)
rs_connection.open_connection()

# Fetch all records from Redshift
all_records = rs_connection.fetch_all_records(table_name)
for record in all_records:
    print(record)

# Fetch specific records from Redshift with a custom query
custom_query = "SELECT * FROM your-table-name WHERE some-column > some-value"
specific_records = rs_connection.fetch_specific_records(custom_query)
for record in specific_records:
    print(record)

rs_connection.close_connection()
