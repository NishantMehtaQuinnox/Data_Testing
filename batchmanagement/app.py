import json
import requests
from utils.s3utils import S3Utils

def lambda_handler(event, context):
    # Process each SQS message
    for record in event['Records']:
        # Extract the message body (you might need to update this depending on your payload structure)
        message_body = json.loads(record['body'])
        s3_utils = S3Utils()
        try:
            job_status = await get_job_status(job_id)
            config_s3_link = job_status.get("configs")
            job_config = json.loads(s3_utils.download_file_get_content(config_s3_link))
            if job_config.get("job_type") == "evaluate":
                

        return True
        # Define the URL of the FastAPI endpoint you want to hit
        api_endpoint = 'https://yourfastapiapp.com/api/start'
        
        # Assuming the SQS message body is the data you want to send to your FastAPI endpoint
        response = requests.post(api_endpoint, json=message_body)
        
        if response.status_code == 200:
            # Handle successful request
            print(f'Successfully started process with message: {message_body}')
        else:
            # Handle unsuccessful request/response
            print(f'Failed to start process. Status code: {response.status_code}, Message: {message_body}')

    # Return a successful response
    return {
        'statusCode': 200,
        'body': json.dumps('Processed SQS messages and triggered FastAPI endpoint.')
    }

