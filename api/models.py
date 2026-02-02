# import enum
# from sqlalchemy import Column, String, Integer, Enum, DateTime, Text
# from sqlalchemy.sql import func
# from api.db import Base

# class JobStatus(str, enum.Enum):
#     PENDING = "PENDING"
#     RUNNING = "RUNNING"
#     DONE = "DONE"
#     FAILED = "FAILED"
#     DLQ = "DLQ"

# class Job(Base):
#     __tablename__ = "jobs"

#     job_id = Column(String, primary_key=True, index=True)
#     user_id = Column(String, index=True)
#     payload = Column(Text)
#     status = Column(Enum(JobStatus), default=JobStatus.PENDING)
#     retry_count = Column(Integer, default=0)
#     max_retries = Column(Integer, default=3)
#     lease_until = Column(DateTime, nullable=True)
#     idempotency_key = Column(String, nullable=True)
#     created_at = Column(DateTime, server_default=func.now())
#     updated_at = Column(DateTime, onupdate=func.now())
