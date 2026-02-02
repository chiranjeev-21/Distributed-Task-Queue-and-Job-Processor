import time
import random
from datetime import datetime, timedelta

from api.main import SessionLocal, Job, JobStatus

from sqlalchemy import text

LEASE_TIME = 30

def fetch_job(db):
    now = datetime.utcnow()
    lease_until = now + timedelta(seconds=LEASE_TIME)

    result = db.execute(
        text("""
        UPDATE jobs
        SET status = :running,
            lease_until = :lease_until
        WHERE job_id = (
            SELECT job_id FROM jobs
            WHERE status = :pending
            LIMIT 1
        )
        RETURNING job_id
        """),
        {
            "pending": JobStatus.PENDING.value,
            "running": JobStatus.RUNNING.value,
            "lease_until": lease_until
        }
    ).fetchone()

    if not result:
        return None

    db.commit()

    job_id = result[0]
    return db.query(Job).filter(Job.job_id == job_id).first()



def process(job):
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("Random failure")


def main():
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
