mysql = AsyncMysqlConnector(host='127.0.0.1', database='mydatabase', user='myuser', password='mypassword')

# Fetch records from Redshift
#redshift_query = "SELECT * FROM your_redshift_table"
#redshift_data = redshift.fetch_specific_records(redshift_query)

# Fetch records from SQL (MySQL)
mysql.open_connection()
mysql_query = "SELECT * FROM users_table"