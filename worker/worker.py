import time
import random
from datetime import datetime, timedelta
import sys
import os
from sqlalchemy.exc import OperationalError
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from api.main import SessionLocal, Job, JobStatus

from sqlalchemy import text

LEASE_TIME = 30

def fetch_job(db):
    result = db.execute(text("""
        SELECT job_id FROM jobs
        WHERE status = 'PENDING'
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """)).fetchone()

    if not result:
        return None

    job_id = result[0]
    job = db.query(Job).filter(Job.job_id == job_id).first()
    job.status = JobStatus.RUNNING
    db.commit()
    return job


def wait_for_db():
    while True:
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            db.close()
            break
        except OperationalError:
            print("Waiting for DB...")
            time.sleep(2)


def process(job):
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("Random failure")


def main():
    wait_for_db()
    while True:
        db = SessionLocal()
        try:
            job = fetch_job(db)

            if not job:
                time.sleep(1)
                continue

            print(f"Processing job {job.job_id}")

            try:
                process(job)
                job.status = JobStatus.DONE
                print(f"Job {job.job_id} DONE")

            except Exception:
                job.retry_count += 1
                if job.retry_count >= job.max_retries:
                    job.status = JobStatus.DLQ
                    print(f"Job {job.job_id} moved to DLQ")
                else:
                    job.status = JobStatus.PENDING
                    print(f"Retrying job {job.job_id}")

            db.commit()

        finally:
            db.close()


if __name__ == "__main__":
    main()
