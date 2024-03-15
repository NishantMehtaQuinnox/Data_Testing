import os
from dotenv import load_dotenv
import aioboto3
import traceback
import aiohttp

load_dotenv()


class BUCKET_CONFIGS:
    BUCKET_NAME = os.environ.get("BUCKET_NAME", "ctc-qa-ai")
    BUCKET_REGION = os.environ.get("BUCKET_REGION", "us-west-2")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
    AWS_SECRET_ACCESS_ID = os.environ.get("AWS_SECRET_ACCESS_ID")
    ROLE_ARN = os.environ.get("ROLE_ARN")
    ROLE_SESSION = os.environ.get("ROLE_SESSION_BUCKET",
                                  "APIRecordCollector_Bucket_Role_Session")
ENV = os.environ.get("ENV", "LOCAL")

class async_S3Utils:
    def __init__(self):
        """Interface to Store and Download reports from bucket"""
        self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                     BUCKET_CONFIGS.ROLE_SESSION)

    async def __get_bucket_resource__(self, bucket_location: str,
                                      bucket_role_session: str):
        """Initialize the bucket resource
        [Unchanged Docstring]
        """
        if ENV in ["STG", "CLIENT"]:
            print("PROD")
            self.sts_client = aioboto3.client('sts')
            self.assumed_role = await self.sts_client.assume_role(
                RoleArn=BUCKET_CONFIGS.ROLE_ARN,
                RoleSessionName=bucket_role_session)
            # Note that assume_role(...) is not an operation in boto3's STS client

            self.creds = self.assumed_role.get("Credentials")
            self.bucket_resource = aioboto3.client(
                "s3",
                aws_access_key_id=self.creds.get("AccessKeyId"),
                aws_secret_access_key=self.creds.get("SecretAccessKey"),
                aws_session_token=self.creds.get("SessionToken"),
                region_name=bucket_location)
        else:
            self.bucket_resource = aioboto3.client("s3")

    async def download_file(self, url: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    file_content = await response.read()
                    file_name = os.path.basename(url)
                    return file_name, file_content
                else:
                    response.raise_for_status()

    async def upload_file(self, key_name: str, data):
        try:
            key = f'{key_name}'
            purl = await self.__generate_presigned_url__(key)
            await self.bucket_resource.put_object(
                Body=data,
                Bucket=BUCKET_CONFIGS.BUCKET_NAME,
                Key=key_name)
            # url = f'https://s3-{BUCKET_CONFIGS.BUCKET_REGION}.amazonaws.com/{BUCKET_CONFIGS.BUCKET_NAME}/{key}'
            return purl, key, True
        
        except Exception as e:
            print(f'CLIENT ERROR WHEN DOWNLOADING FILE: {e}')
            try:
                self.__get_bucket_resource__(BUCKET_CONFIGS.BUCKET_REGION,
                                             BUCKET_CONFIGS.ROLE_SESSION)
                return self.upload_file(key, data)
            except Exception as e:
                print(f'EXCEPTION WHILE UPLOADING THE FILE: {str(e)}')
                print(f'TRACEBACK: {traceback.format_exc()}')
                return False, False, None

    async def __generate_presigned_url__(self, object_name, expiration=3600):
        try:
            response = await self.bucket_resource.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': BUCKET_CONFIGS.BUCKET_NAME,
                    'Key': object_name
                },
                ExpiresIn=expiration)
            return response

        # [Error handling code remains unchanged below this line]
        except Exception as e:
            print(f'EXCEPTION WHILE UPLOADING THE FILE: {str(e)}')
            print(f'TRACEBACK: {traceback.format_exc()}')
            return False