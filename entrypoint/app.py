import os
import uuid
import aiomysql
from typing import Optional,List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.s3utils import S3Utils
import uvicorn
from connectors.sql.connect import AsyncMysqlConnector

app = FastAPI()

jobs_db = AsyncMysqlConnector(
    host=os.getenv("MYSQL_HOST","127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "myuser"),
    password=os.getenv("MYSQL_PASS", "mypassword"),
    database=os.getenv("MYSQL_DB", "mydatabase"),
)

class Job(BaseModel):
    username: str
    job_name : str
    job_description : str
    db_configs: dict
    column_to_join : str
    job_type: str

@app.post("/start_job")
async def create_job(
    config: Job,
    job_id: Optional[str] = None
):
    if job_id is None:
        job_id = str(uuid.uuid4())

    config_path = await store_config_to_s3(config, job_id)

    response = await add_job_to_db(config, job_id, config_path)

    return response

async def store_config_to_s3(config: Job, job_id: str):
    s3_utils = S3Utils()
    json_data = config.json()
    s3_key = f'data_testing/jobs/{job_id}/configs/configs.json'
    config_path, s3_key, status = s3_utils.upload_file(s3_key, json_data)
    if not status:
        raise HTTPException(status_code=500, detail="Failed to store configuration data to S3")
    return config_path

async def add_job_to_db(config: Job, job_id: str, config_path):
    try:
        # It is better to use a connection pool instead of raw connect
        await jobs_db.open_connection()
        affected_rows = await jobs_db.execute_query(
                    """
                    INSERT INTO DataTestingJobs 
                    (job_id, username, status, job_type, configs, job_name, job_description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, config.username, "STARTED", config.job_type, config_path, config.job_name, config.job_description)
                )
        await jobs_db.close_connection()
        if affected_rows:
            return {"message": "Test Started successfully", "job_id": job_id}
        else:
            return {"message": "Test Starting Failed", "job_id": job_id}

    except Exception as e:
        print(e)

@app.get("/get_jobs_for_user/{username}")
async def get_jobs_for_user(username: str):
    try:
        await jobs_db.open_connection()
        query = "SELECT * FROM DataTestingJobs WHERE username = %s"
        jobs =  await jobs_db.execute_query(query, (username,))
        return jobs

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve user jobs from the database") from e

    finally:
        # Close the connection
        await jobs_db.close_connection()

# Get Job Status
@app.get("/get_job_status/{request_id}")
async def get_job_status(request_id: str):
    try:
        await jobs_db.open_connection()
        jobs = await jobs_db.execute_query(
                    "SELECT * FROM DataTestingJobs WHERE job_id = %s",
                    (request_id,)
                )
        if jobs:
            return jobs[0]
        else:
            raise HTTPException(status_code=404, detail="Job not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve job status from the database") from e
    finally:
        # Close the connection
        await jobs_db.close_connection()

if __name__ == "__main__":
    # Use this for development with automatic reload
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)