import time
import random
import threading
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from api.main import SessionLocal, Job, JobStatus
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from prometheus_client import Counter, Gauge, Histogram, make_wsgi_app
from socketserver import ThreadingMixIn
from wsgiref.simple_server import make_server, WSGIRequestHandler, WSGIServer

# -------------------------
# Prometheus Metrics
# -------------------------
jobs_processed = Counter("worker_jobs_processed", "Total jobs successfully processed")
jobs_failed = Counter("worker_jobs_failed", "Total jobs failed")
jobs_picked = Counter("worker_jobs_picked", "Total jobs picked")
jobs_retried = Counter("worker_jobs_retried", "Total retries")
jobs_dlq = Counter("worker_jobs_dlq", "Total jobs moved to DLQ")
jobs_in_progress = Gauge("worker_jobs_in_progress", "Jobs in progress")
job_processing_seconds = Histogram("worker_job_processing_seconds", "Processing time")
job_status_transitions = Counter(
    "worker_job_status_transitions",
    "Job status transitions",
    ["from_status", "to_status"]
)

metrics_requests = Counter(
    "worker_metrics_requests",
    "HTTP requests to worker metrics",
    ["method", "path", "status"]
)

# -------------------------
# Constants
# -------------------------
LEASE_TIME = 30
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
WORKER_NAME = os.getenv("WORKER_NAME", os.getenv("HOSTNAME", "worker"))
LOG_METRICS_REQUESTS = os.getenv("METRICS_LOG_REQUESTS", "1") == "1"


# -------------------------
# Helpers
# -------------------------
def wait_for_db():
    while True:
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            db.close()
            print(f"[{WORKER_NAME}] Connected to DB", flush=True)
            break
        except OperationalError:
            print(f"[{WORKER_NAME}] Waiting for DB...", flush=True)
            time.sleep(2)


def record_transition(previous_status, next_status):
    if previous_status and next_status and previous_status != next_status:
        job_status_transitions.labels(
            from_status=previous_status.value,
            to_status=next_status.value,
        ).inc()


def fetch_and_lease_job(db):
    """
    Atomically:
    - find pending job OR expired running job
    - respect next_run_at
    - claim lease
    """

    result = db.execute(text("""
        UPDATE jobs
        SET
            status = 'RUNNING',
            worker_id = :worker_id,
            lease_until = :lease_until
        WHERE job_id = (
            SELECT job_id FROM jobs
            WHERE
                (
                    status = 'PENDING'
                    OR (status = 'RUNNING' AND lease_until < now())
                )
                AND (next_run_at IS NULL OR next_run_at <= now())
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING job_id
    """), {
        "worker_id": WORKER_NAME,
        "lease_until": datetime.utcnow() + timedelta(seconds=LEASE_TIME)
    }).fetchone()

    if not result:
        return None

    job_id = result[0]
    job = db.query(Job).filter(Job.job_id == job_id).first()
    return job


def process(job):
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("Random failure")


# -------------------------
# Metrics Server
# -------------------------
class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class MetricsRequestHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        if LOG_METRICS_REQUESTS:
            print(f"[{WORKER_NAME}] METRICS {format % args}", flush=True)


def metrics_app_with_logging(app):
    def _app(environ, start_response):
        method = environ.get("REQUEST_METHOD", "")
        path = environ.get("PATH_INFO", "")

        def _start_response(status, headers, exc_info=None):
            status_code = status.split(" ")[0]
            metrics_requests.labels(method=method, path=path, status=status_code).inc()
            return start_response(status, headers, exc_info)

        return app(environ, _start_response)
    return _app


def start_metrics_server():
    app = metrics_app_with_logging(make_wsgi_app())
    with make_server(
        "0.0.0.0",
        METRICS_PORT,
        app,
        server_class=ThreadedWSGIServer,
        handler_class=MetricsRequestHandler,
    ) as httpd:
        print(f"[{WORKER_NAME}] Metrics at /metrics", flush=True)
        httpd.serve_forever()


# -------------------------
# Main Worker Loop
# -------------------------
def main():
    threading.Thread(target=start_metrics_server, daemon=True).start()
    wait_for_db()

    print(f"[{WORKER_NAME}] Worker started", flush=True)

    while True:
        db = SessionLocal()
        try:
            job = fetch_and_lease_job(db)

            if not job:
                time.sleep(1)
                continue

            jobs_picked.inc()
            jobs_in_progress.inc()
            print(f"[{WORKER_NAME}] Processing job {job.job_id}", flush=True)

            try:
                with job_processing_seconds.time():
                    process(job)

                # Safe ACK
                previous_status = job.status
                updated = db.query(Job).filter(
                    Job.job_id == job.job_id,
                    Job.worker_id == WORKER_NAME
                ).update({
                    Job.status: JobStatus.DONE,
                    Job.worker_id: None,
                    Job.lease_until: None
                })

                if updated:
                    jobs_processed.inc()
                    record_transition(previous_status, JobStatus.DONE)
                    print(f"[{WORKER_NAME}] Job {job.job_id} DONE", flush=True)

            except Exception:
                jobs_failed.inc()
                job.retry_count += 1

                if job.retry_count >= job.max_retries:
                    previous_status = job.status
                    job.status = JobStatus.DLQ
                    job.worker_id = None
                    job.lease_until = None
                    jobs_dlq.inc()
                    record_transition(previous_status, JobStatus.DLQ)
                    print(f"[{WORKER_NAME}] Job {job.job_id} -> DLQ", flush=True)

                else:
                    previous_status = job.status
                    backoff = 2 ** job.retry_count
                    job.status = JobStatus.PENDING
                    job.worker_id = None
                    job.lease_until = None
                    job.next_run_at = datetime.utcnow() + timedelta(seconds=backoff)
                    jobs_retried.inc()
                    record_transition(previous_status, JobStatus.PENDING)
                    print(
                        f"[{WORKER_NAME}] Retrying job {job.job_id} in {backoff}s",
                        flush=True
                    )

            finally:
                jobs_in_progress.dec()
                db.commit()

        finally:
            db.close()


if __name__ == "__main__":
    main()
