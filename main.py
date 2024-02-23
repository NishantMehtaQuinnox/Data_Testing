# main.py
import boto3
import csv
from io import StringIO
from connectors.redshift.connect import RedshiftConnector
from connectors.sql.connect import MysqlConnector

# Initialize connectors
#redshift = RedshiftConnector(host='127.0.0.1', port = '', database='mydatabase', user='myuser', password='password')
mysql = MysqlConnector(host='127.0.0.1', database='mydatabase', user='myuser', password='mypassword')

# Fetch records from Redshift
#redshift_query = "SELECT * FROM your_redshift_table"
#redshift_data = redshift.fetch_specific_records(redshift_query)

# Fetch records from SQL (MySQL)
mysql.open_connection()
mysql_query = "SELECT * FROM users_table"
mysql_data = mysql.fetch_specific_records(mysql_query)

# Convert to CSV format
def convert_to_csv(data):
    si = StringIO()
    cw = csv.writer(si)
    cw.writerows(data)
    return si.getvalue().strip('\\r\\n')

# Upload to S3
def upload_to_s3(bucket_name, s3_file_name, data):
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, s3_file_name).put(Body=data)

# Usage
bucket_name = 'your-bucket-name'

# Convert data to CSV
#redshift_csv_data = convert_to_csv(redshift_data)
mysql_csv_data = convert_to_csv(mysql_data)

# Upload Redshift data to S3
upload_to_s3(bucket_name, 'redshift_data.csv', redshift_csv_data)

# Upload MySQL data to S3
upload_to_s3(bucket_name, 'mysql_data.csv', mysql_csv_data)
