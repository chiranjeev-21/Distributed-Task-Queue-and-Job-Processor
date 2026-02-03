import uuid
import enum
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Enum, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError

from prometheus_client import Counter, Gauge, generate_latest
import os

DATABASE_URL = "postgresql+psycopg2://jobuser:jobpass@postgres:5432/jobqueue"

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    DLQ = "DLQ"


class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(String, primary_key=True, index=True)
    user_id = Column(String, index=True)
    payload = Column(Text)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    lease_until = Column(DateTime, nullable=True)
    idempotency_key = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


# --- REAL idempotency guarantee (DB level) ---
Index(
    "uniq_user_idem_key",
    Job.user_id,
    Job.idempotency_key,
    unique=True,
    postgresql_where=Job.idempotency_key.isnot(None),
)

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Distributed Job Queue")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Prometheus metrics
jobs_total = Counter("jobs_total", "Total jobs submitted")
jobs_done = Counter("jobs_done", "Jobs completed")
jobs_dlq = Counter("jobs_dlq", "Jobs moved to DLQ")
jobs_running = Gauge("jobs_running", "Running jobs")


MAX_CONCURRENT_JOBS = 5
MAX_JOBS_PER_MINUTE = 10


def enforce_concurrent_limit(db, user_id):
    count = db.query(Job).filter(
        Job.user_id == user_id,
        Job.status == JobStatus.RUNNING
    ).count()

    if count >= MAX_CONCURRENT_JOBS:
        raise HTTPException(429, "Max concurrent jobs exceeded")


def enforce_rate_limit(db, user_id):
    one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
    count = db.query(Job).filter(
        Job.user_id == user_id,
        Job.created_at >= one_minute_ago
    ).count()

    if count >= MAX_JOBS_PER_MINUTE:
        raise HTTPException(429, "Rate limit exceeded")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs")
def submit_job(
    user_id: str,
    payload: str,
    idempotency_key: Optional[str] = None,
    db=Depends(get_db)
):
    # Step 1: fast-path check (per user)
    if idempotency_key:
        existing = db.query(Job).filter(
            Job.user_id == user_id,
            Job.idempotency_key == idempotency_key
        ).first()
        if existing:
            return {"job_id": existing.job_id}

    enforce_rate_limit(db, user_id)
    enforce_concurrent_limit(db, user_id)

    job = Job(
        job_id=str(uuid.uuid4()),
        user_id=user_id,
        payload=payload,
        status=JobStatus.PENDING,
        idempotency_key=idempotency_key
    )

    db.add(job)

    try:
        db.commit()
    except IntegrityError:
        # Step 2: race-safe fallback
        db.rollback()
        existing = db.query(Job).filter(
            Job.user_id == user_id,
            Job.idempotency_key == idempotency_key
        ).first()
        return {"job_id": existing.job_id}

    db.refresh(job)
    jobs_total.inc()
    print(f"JOB_SUBMITTED job_id={job.job_id}")

    return {"job_id": job.job_id}


@app.get("/jobs/{job_id}")
def get_status(job_id: str, db=Depends(get_db)):
    job = db.query(Job).filter(Job.job_id == job_id).first()

    if not job:
        raise HTTPException(404, "Job not found")

    return {
        "job_id": job.job_id,
        "status": job.status,
        "retry_count": job.retry_count
    }


@app.get("/jobs")
def list_jobs(status: Optional[JobStatus] = Query(None), db=Depends(get_db)):
    query = db.query(Job)
    if status:
        query = query.filter(Job.status == status)
    return query.all()


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest())
