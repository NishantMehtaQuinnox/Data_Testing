import os
import uuid
import aiomysql
from typing import Optional,List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.s3utils import S3Utils
import uvicorn

app = FastAPI()

class Job(BaseModel):
    username: str
    job_name : str
    job_description : str
    db_configs: List[dict]
    job_type: str
    mappings: List[dict]

@app.post("/start_job")
async def create_job(
    config: Job,
    job_id: Optional[str] = None
):
    if job_id is None:
        job_id = str(uuid.uuid4())

    config_path = await store_config_to_s3(config, job_id)

    response = await add_job_to_db(config.username, config.job_type, job_id, config_path, config.job_name, config.job_description)

    return response

async def store_config_to_s3(config: Job, job_id: str):
    s3_utils = S3Utils()
    json_data = config.json()
    s3_key = f'data_testing/jobs/{job_id}/configs/configs.json'
    config_path, s3_key, status = s3_utils.upload_file(s3_key, json_data)
    if not status:
        raise HTTPException(status_code=500, detail="Failed to store configuration data to S3")
    return config_path

async def add_job_to_db(username: str, job_type: str, job_id: str, config_path, job_name, job_description):
    MYSQL_HOST = os.getenv("MYSQL_HOST","127.0.0.1")
    MYSQL_USER = os.getenv("MYSQL_USER", "myuser")
    MYSQL_PASS = os.getenv("MYSQL_PASS", "mypassword")
    MYSQL_DB = os.getenv("MYSQL_DB", "mydatabase")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

    try:
        # It is better to use a connection pool instead of raw connect
        pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO DataTestingJobs 
                    (job_id, username, status, job_type, configs, job_name, job_description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, username, "STARTED", job_type, config_path, job_name, job_description)
                )
                await conn.commit()
    except aiomysql.Error as e:
        raise HTTPException(status_code=500, detail="Failed to insert job details into the database") from e
    finally:
        pool.close()
        await pool.wait_closed()

    return {"message": "Test Started successfully", "job_id": job_id}

# Get Jobs for a User
@app.get("/get_jobs_for_user/{username}")
async def get_jobs_for_user(username: str):
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_USER = os.getenv("MYSQL_USER", "myuser")
    MYSQL_PASS = os.getenv("MYSQL_PASS", "mypassword")
    MYSQL_DB = os.getenv("MYSQL_DB", "mydatabase")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

    try:
        pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT * FROM DataTestingJobs WHERE username = %s
                    """,
                    (username,)
                )
                jobs = await cursor.fetchall()
                return jobs
    except aiomysql.Error as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve user jobs from the database") from e
    finally:
        pool.close()
        await pool.wait_closed()

# Get Job Status
@app.get("/get_job_status/{request_id}")
async def get_job_status(request_id: str):
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_USER = os.getenv("MYSQL_USER", "myuser")
    MYSQL_PASS = os.getenv("MYSQL_PASS", "mypassword")
    MYSQL_DB = os.getenv("MYSQL_DB", "mydatabase")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

    try:
        pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    """
                    SELECT * FROM DataTestingJobs WHERE job_id = %s
                    """,
                    (request_id,)
                )
                job = await cursor.fetchone()
                if job:
                    return job
                else:
                    raise HTTPException(status_code=404, detail="Job not found")
    except aiomysql.Error as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve job status from the database") from e
    finally:
        pool.close()
        await pool.wait_closed()

if __name__ == "__main__":
    # Use this for development with automatic reload
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)