import uuid
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import jobs
from config_to_s3 import store_config_to_s3

app = FastAPI()

class Job(BaseModel):
    username: str
    job_name : str
    job_description : str
    db_configs: dict
    job_type: dict

@app.post("/start_job")
async def create_job(
    config: Job,
    job_id: Optional[str] = None
):
    if job_id is None:
        job_id = str(uuid.uuid4())

    config_path = await store_config_to_s3(config, job_id)

    response = await jobs.add_job_to_db(config, job_id, config_path)

    return response

@app.get("/get_jobs_for_user/{username}")
async def get_jobs_for_user(username: str):
    try:
        response = await jobs.get_jobs_for_user(username)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve job status from the database") from e

# Get Job Status
@app.get("/get_job_status/{request_id}")
async def get_job_status(request_id: str):
    try:
        response = await jobs.get_job_status(request_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve job status from the database") from e

if __name__ == "__main__":
    # Use this for development with automatic reload
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)