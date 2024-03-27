
from utils.s3utils import S3Utils
from pydantic import BaseModel

class Job(BaseModel):
    username: str
    job_name : str
    job_description : str
    db_configs: dict
    job_type: dict

async def store_config_to_s3(config: Job, job_id: str):
    try:
        s3_utils = S3Utils()
        json_data = config.json()
        s3_key = f'data_testing/jobs/{job_id}/configs/configs.json'
        config_path, s3_key, status = s3_utils.upload_file(s3_key, json_data)
        if not status:
            raise Exception("Failed to store configuration data to S3")
        return config_path
    except Exception as e:
        raise Exception(f"Failed to store configuration data to S3 {e}")
        