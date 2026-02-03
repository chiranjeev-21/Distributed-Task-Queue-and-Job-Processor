import time
import random
import threading
import sys
import os
from datetime import datetime

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
jobs_processed = Counter(
    "worker_jobs_processed",
    "Total jobs successfully processed by worker"
)

jobs_failed = Counter(
    "worker_jobs_failed",
    "Total jobs failed by worker"
)

jobs_picked = Counter(
    "worker_jobs_picked",
    "Total jobs picked for processing by worker"
)

jobs_retried = Counter(
    "worker_jobs_retried",
    "Total job retries by worker"
)

jobs_dlq = Counter(
    "worker_jobs_dlq",
    "Total jobs moved to DLQ by worker"
)

jobs_in_progress = Gauge(
    "worker_jobs_in_progress",
    "Jobs currently in progress by worker"
)

job_processing_seconds = Histogram(
    "worker_job_processing_seconds",
    "Job processing time in seconds"
)

job_status_transitions = Counter(
    "worker_job_status_transitions",
    "Job status transitions",
    ["from_status", "to_status"]
)

metrics_requests = Counter(
    "worker_metrics_requests",
    "HTTP requests to worker metrics endpoint",
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
    """Block until Postgres is reachable."""
    while True:
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            db.close()
            print(f"[{WORKER_NAME}] Worker connected to DB", flush=True)
            break
        except OperationalError:
            print(f"[{WORKER_NAME}] Worker waiting for DB...", flush=True)
            time.sleep(2)


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
    previous_status = job.status
    job.status = JobStatus.RUNNING
    db.commit()
    record_transition(previous_status, job.status)
    return job


def process(job):
    """Simulated job processing."""
    time.sleep(2)
    if random.random() < 0.3:
        raise Exception("Random failure")


# -------------------------
# Main worker loop
# -------------------------
class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class MetricsRequestHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        if LOG_METRICS_REQUESTS:
            message = format % args
            print(f"[{WORKER_NAME}] METRICS {message}", flush=True)


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
        print(
            f"[{WORKER_NAME}] Worker metrics available at http://0.0.0.0:{METRICS_PORT}/metrics",
            flush=True,
        )
        httpd.serve_forever()


def record_transition(previous_status, next_status):
    if previous_status and next_status and previous_status != next_status:
        job_status_transitions.labels(
            from_status=previous_status.value,
            to_status=next_status.value,
        ).inc()


def main():
    # Start metrics server
    threading.Thread(target=start_metrics_server, daemon=True).start()

    # Wait for DB
    wait_for_db()

    print(f"[{WORKER_NAME}] Worker started processing jobs", flush=True)

    while True:
        db = SessionLocal()
        try:
            job = fetch_job(db)

            if not job:
                time.sleep(1)
                continue

            jobs_picked.inc()
            jobs_in_progress.inc()
            print(
                f"[{WORKER_NAME}] [{datetime.utcnow()}] Processing job {job.job_id}",
                flush=True,
            )

            try:
                with job_processing_seconds.time():
                    process(job)
                previous_status = job.status
                job.status = JobStatus.DONE
                jobs_processed.inc()
                record_transition(previous_status, job.status)
                print(
                    f"[{WORKER_NAME}] [{datetime.utcnow()}] Job {job.job_id} DONE",
                    flush=True,
                )

            except Exception:
                job.retry_count += 1
                jobs_failed.inc()

                if job.retry_count >= job.max_retries:
                    previous_status = job.status
                    job.status = JobStatus.DLQ
                    jobs_dlq.inc()
                    record_transition(previous_status, job.status)
                    print(
                        f"[{WORKER_NAME}] [{datetime.utcnow()}] Job {job.job_id} moved to DLQ",
                        flush=True,
                    )
                else:
                    previous_status = job.status
                    job.status = JobStatus.PENDING
                    jobs_retried.inc()
                    record_transition(previous_status, job.status)
                    print(
                        f"[{WORKER_NAME}] [{datetime.utcnow()}] Retrying job {job.job_id}",
                        flush=True,
                    )

            finally:
                jobs_in_progress.dec()

            db.commit()

        finally:
            db.close()


if __name__ == "__main__":
    main()
