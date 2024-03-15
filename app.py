from fastapi import FastAPI,HTTPException,Request
from fastapi.responses import JSONResponse
from connectors.sql.connect import AsyncMysqlConnector
from connectors.redshift.connect import AsyncRedshiftConnector
import boto3
from io import StringIO
import csv
import pandas as pd
import time
import utils.s3utils as s3utils
import uuid

app = FastAPI()

# Initialize an asynchronous MySQL connector
async_mysql = AsyncMysqlConnector(host='127.0.0.1', database='mydatabase', user='myuser', password='mypassword')
async_redshift = AsyncRedshiftConnector(host='ai-rs-poc.293963594940.ap-south-1.redshift-serverless.amazonaws.com', port = 5439, database='dev', user='admin', password='Qyrus#789')

def convert_to_csv(data, header=None):
    # Create a StringIO object to hold the CSV data
    si = StringIO()
    cw = csv.writer(si)

    # Write the header if provided
    if header is not None:
        cw.writerow(header)

    # Write the data rows
    cw.writerows(data)

    # Get the CSV data as a string and return it
    return si.getvalue().rstrip('\\r\\n')

def read_file(file_path):
    try:
        with open(file_path, 'r') as file:  # 'r' is for read mode
            content = file.read()
        return content
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage
file_path = '/path/to/your/file.txt'
file_content = read_file(file_path)
if file_content is not None:
    print(file_content)


def upload_to_s3(bucket_name, s3_file_name, data):
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, s3_file_name).put(Body=data)

def save_to_file(filename, data, header=None):
    st_time = time.time()
    df = pd.DataFrame(data)
    df.to_csv(filename,index=False)
    print(f"Time for saving to csv : {time.time()-st_time}")

# Define API endpoint
@app.get('/fetch_and_upload_to_s3')
async def fetch_and_upload_to_s3(request: Request):
    req_json = await request.json()
    db = req_json.get('db')
    db_query = req_json.get('query')
    if db.lower() == 'mysql':
        connector = async_mysql
    elif db.lower() in ['postgresql','redshift']:
        connector = async_redshift
    # Open MySQL connection
    await connector.open_connection()
    
    fetched_data = await connector.fetch_specific_records(db_query)
    
    # Assuming mysql_data is a list of tuples containing the result rows
    if not fetched_data:
        return JSONResponse(content={'message': 'No data found.'}, status_code=404)
    filename = str(uuid.uuid1()) + ".csv"
    save_to_file(filename,fetched_data)
    
    # Upload MySQL data to S3
    upload_file(filename)
    
    # Close MySQL connection
    await async_redshift.close_connection()
    
    return JSONResponse(content={'message': 'MySQL data uploaded to S3 successfully.'})


@app.post('/upload_file')
async def upload_file(filepath: str, include_url: bool = True):
    """
    Endpoint to upload a single file to S3 using a presigned URL.
    """
    try:
        contents = read_file(filepath)
        st_time = time()
        key_to_upload = f'data_testing/{uuid.uuid1()}/{filepath}'
        # Generate presigned URL if needed
        _ = s3utils.__generate_presigned_url__(key_to_upload) if include_url else None
        # Upload file
        presigned_url, key, success = s3utils.upload_file(key_to_upload, contents)
        en_time = time()
        
        if success:
            upload_result = {
                "filename": filepath,
                "message": "File uploaded successfully",
                "upload_time": f"{en_time - st_time}",
                "s3_key": key,
                "presigned_url": presigned_url if include_url else None
            }
            return upload_result
        else:
            raise HTTPException(status_code=500, detail="Failed to upload file")

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "filename": filepath,
                "error": str(e)
            }
        )   

# Run the ASGI server
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
