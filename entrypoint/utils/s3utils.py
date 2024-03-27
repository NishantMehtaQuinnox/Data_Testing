import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ConnectTimeoutError, ClientError
import traceback
import requests
from urllib.parse import urlparse, unquote

load_dotenv()

class BUCKET_CONFIGS:
    BUCKET_NAME = os.environ.get("BUCKET_NAME", "ai-services-stg")
    BUCKET_REGION = os.environ.get("BUCKET_REGION", "us-west-2")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
    AWS_SECRET_ACCESS_ID = os.environ.get("AWS_SECRET_ACCESS_ID")
    ROLE_ARN = os.environ.get("ROLE_ARN")
    ROLE_SESSION = os.environ.get("ROLE_SESSION_BUCKET",
                                  "APIRecordCollector_Bucket_Role_Session")
ENV = os.environ.get("ENV", "LOCAL")


class S3Utils:
    def __init__(self):
        """Interface to Store and Download reports from bucket"""
        self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                     BUCKET_CONFIGS.ROLE_SESSION)

    
    def __get_bucket_resource__(self, bucket_location: str,
                                bucket_role_session: str):
        """Initialize the bucket resource

        Args:
            bucket_name (str): Name of the bucket
            bucket_location (str): Location of the bucket
            bucket_role_session (str): Session Name of the bucket
        """

        if ENV in ["STG", "CLIENT"]:
            print("PROD")
            self.__obtain_sts_creds__()
            self.assumed_role = self.sts_client.assume_role(
                RoleArn=BUCKET_CONFIGS.ROLE_ARN,
                RoleSessionName=bucket_role_session)

            self.creds = self.assumed_role.get("Credentials")
            self.bucket_resource = boto3.client(
                "s3",
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=bucket_location)
            # self.bucket = self.bucket_resource.Bucket(bucket_name)

        else:
            self.bucket_resource = boto3.client("s3")
            
    def list_files(self, prefix):
        try:
            response = self.bucket_resource.list_objects_v2(Bucket=BUCKET_CONFIGS.BUCKET_NAME, Prefix=prefix)
            files = response.get('Contents', [])
            return [self.__generate_presigned_url__(file['Key']) for file in files]
        except ClientError as cerr:
            print(f'CLIENT ERROR WHEN UPLOADING FILE: {cerr}')
            try:
                self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                             BUCKET_CONFIGS.ROLE_SESSION)
                return self.list_files(prefix)
            except Exception as e:
                print(
                    f'EXCEPTION IN INITIALIZING THE BUCKET RESOURCE IN CLIENT ERROR BLOCK (UPLOAD FILE): {str(e)}'
                )
                print(f'TRACEBACK: {traceback.format_exc()}')
                return False, False, None
        except Exception as e:
            print(f'EXCEPTION WHILE UPLOADING THE FILE: {str(e)}')
            print(f'TRACEBACK: {traceback.format_exc()}')
            return False, False, None

    def download_file_get_content(self,url:str):
        file_name = unquote(urlparse(url).path.split('/')[-1]) 
        response = requests.get(url)
        if response.ok:
            # with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmpfile:
            #     tmpfile.write(response.content)
            #     temp_file_path = tmpfile.name
            # or response.text if the content is text
            # Now you can work with file_content as needed
            return response.content
        # file_path = 'tmp/'+key_name
        # os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # self.bucket_resource.download_file(BUCKET_CONFIGS.BUCKET_NAME, key_name, file_path)
        # return file_path
    
    def upload_file(self, key_name: str, data):
        """Uploads the file to s3

        Args:
            file_name (str): Name of the file
            file_path (str): Path to the file

        Returns:
            bool: Based on the upload status will return a boolean value
        """
        try:
            key = f'{key_name}'
            self.bucket_resource.put_object(
                Body=data,
                Bucket=BUCKET_CONFIGS.BUCKET_NAME,
                Key=key_name,
                # ACL='public-read'
            )
            # obj.put(Body=data, ACL='public-read')
            purl = self.__generate_presigned_url__(key)
            # url = f'https://s3-{BUCKET_CONFIGS.BUCKET_REGION}.amazonaws.com/{BUCKET_CONFIGS.BUCKET_NAME}/{key}'

            return purl, key, True

        except ClientError as cerr:
            print(f'CLIENT ERROR WHEN UPLOADING FILE: {cerr}')
            try:
                self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                             BUCKET_CONFIGS.ROLE_SESSION)
                return self.upload_file(key_name, data)
            except Exception as e:
                print(
                    f'EXCEPTION IN INITIALIZING THE BUCKET RESOURCE IN CLIENT ERROR BLOCK (UPLOAD FILE): {str(e)}'
                )
                print(f'TRACEBACK: {traceback.format_exc()}')
                return False, False, None
        except Exception as e:
            print(f'EXCEPTION WHILE UPLOADING THE FILE: {str(e)}')
            print(f'TRACEBACK: {traceback.format_exc()}')
            return False, False, None

    def __generate_presigned_url__(self, object_name, expiration=3600):
        # s3_client = boto3.client('s3')
        try:
            response = self.bucket_resource.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': BUCKET_CONFIGS.BUCKET_NAME,
                    'Key': object_name
                },
                ExpiresIn=expiration)
            return response
        except ClientError as cerr:
            print(f'CLIENT ERROR WHEN GENERATING PRESIGNED URL: {cerr}')
            try:
                self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                             BUCKET_CONFIGS.ROLE_SESSION)
                return self.generate_presigned_url(object_name)
            except Exception as e:
                print(
                    f'EXCEPTION IN INITIALIZING THE BUCKET RESOURCE IN CLIENT ERROR BLOCK (UPLOAD FILE): {str(e)}'
                )
                print(f'TRACEBACK: {traceback.format_exc()}')
                return False
        except Exception as e:
            print(f'EXCEPTION WHILE UPLOADING THE FILE: {str(e)}')
            print(f'TRACEBACK: {traceback.format_exc()}')
            return False