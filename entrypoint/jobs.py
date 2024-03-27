from pydantic import BaseModel
from connectors.sql.connect import AsyncMysqlConnector
import os

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
    job_type: dict

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
                    (job_id, config.username, "STARTED", config.job_type.get("job_type"), config_path, config.job_name, config.job_description)
                )
        await jobs_db.close_connection()
        if affected_rows:
            return {"message": "Test Started successfully", "job_id": job_id}
        else:
            return {"message": "Test Starting Failed", "job_id": job_id}

    except Exception as e:
        raise Exception(f"Failed to add jobs to the database {e}")

async def get_jobs_for_user(username: str):
    try:
        await jobs_db.open_connection()
        query = "SELECT * FROM DataTestingJobs WHERE username = %s"
        jobs =  await jobs_db.execute_query(query, (username,))
        return jobs

    except Exception as e:
        raise Exception(f"Failed to retrieve user jobs from the database {e}")
    
    finally:
        await jobs_db.close_connection()

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
            raise Exception("Job not found")
    except Exception as e:
        raise Exception(f"Failed to retrieve job status from the database {e}")
    finally:
        # Close the connection
        await jobs_db.close_connection()