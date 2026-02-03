# Distributed Task Queue & Job Processor

A prototype **distributed job queue and worker system** inspired by real-world systems like AWS SQS, Celery, Temporal, and Kubernetes controllers.

This project demonstrates how to build a **fault-tolerant, horizontally scalable, lease-based job processor** from scratch using Python, FastAPI, PostgreSQL, and Prometheus.

---

## 🚀 Problem Statement

Modern backend systems often need to handle:

* Long-running background tasks
* Asynchronous processing
* Retry on failures
* Horizontal scaling of workers
* Fault tolerance when workers crash

Examples:

* Sending emails
* Processing payments
* Video transcoding
* ML inference jobs
* Webhook delivery

Traditional synchronous APIs **cannot handle these reliably**.

This project solves this by implementing a **distributed task queue** where:

* Jobs are persisted in a database
* Workers poll and process jobs asynchronously
* Jobs are retried on failure
* Crashed workers are automatically recovered from
* System state is observable via metrics

---

## 🧠 Why Use This Instead of a Simple Queue?

| Approach          | Limitations            |
| ----------------- | ---------------------- |
| In-memory queue   | Loses data on crash    |
| Cron jobs         | No retries, no scaling |
| Simple DB polling | No fault tolerance     |
| Redis list        | No ownership or leases |

This system provides:

* **Durability** – Jobs survive restarts
* **Fault tolerance** – Workers can crash safely
* **Horizontal scaling** – Add more workers anytime
* **Exactly-once processing** – No duplicate execution
* **Backoff + retries** – Exponential retry logic
* **Dead Letter Queue (DLQ)** – Failed jobs are isolated
* **Idempotency** – Duplicate submissions avoided
* **Observability** – Prometheus + Grafana metrics

This is the **core algorithm used by real systems** like:

* Kubernetes controllers
* Temporal workflows
* Celery workers
* SQS consumers
* Airflow schedulers

---

## 🏗️ System Architecture

```
Client
  |
  v
FastAPI (Job API)
  |
PostgreSQL (Source of Truth)
  |
Workers (stateless, lease-based)
  |
Prometheus → Grafana (Metrics)
```

Key design principles:

* Database is the **single source of truth**
* Workers are **stateless**
* Job ownership is enforced via **leases**
* Recovery happens automatically using **time-based stealing**

---

## 🔑 Core Features

### 1. Job Submission API

* Submit jobs via REST
* Supports idempotency keys
* Rate limited per user

### 2. Distributed Leasing

* Workers acquire jobs using time-bound leases
* Prevents double execution
* Allows job stealing after crashes

### 3. Crash Recovery

* If worker dies mid-job
* Lease expires
* Another worker automatically picks it up

### 4. Retry + Backoff

* Failed jobs retried
* Exponential backoff
* After N retries → DLQ

### 5. Dead Letter Queue (DLQ)

* Permanently failed jobs isolated
* Prevents infinite loops

### 6. Observability

* Prometheus metrics
* Grafana dashboards
* Swagger UI for API

---

## 🛠 Tech Stack

| Component        | Technology              |
| ---------------- | ----------------------- |
| API              | FastAPI                 |
| Database         | PostgreSQL              |
| Workers          | Python                  |
| ORM              | SQLAlchemy              |
| Metrics          | Prometheus              |
| Dashboards       | Grafana                 |
| Containerization | Docker + Docker Compose |

---

## 📦 Project Structure

```
.
├── api/
│   └── main.py        # FastAPI + DB models
├── worker/
│   └── worker.py     # Lease-based worker
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚙️ How to Run Locally

### Prerequisites

* Docker
* Docker Compose

---

### 1. Clone the repo

```bash
git clone https://github.com/chiranjeev-21/Distributed-Task-Queue-and-Job-Processor
cd Distributed-Task-Queue-and-Job-Processor
```

---

### 2. Start the system

```bash
docker compose up -d --build
```

This starts:

* FastAPI on `http://localhost:8000`
* PostgreSQL
* Worker
* Prometheus
* Grafana

---

### 3. Submit a job

```bash
curl "http://localhost:8000/jobs?user_id=cj&payload=hello"
```

---

### 4. Check job status

```bash
curl "http://localhost:8000/jobs/<job_id>"
```

---

### 5. View API Docs (Swagger)

```
http://localhost:8000/docs
```

---

### 6. View Metrics

Worker metrics:

```
http://localhost:8001/metrics
```

Grafana dashboard:

```
http://localhost:3000
```

(Default Grafana creds: `admin / admin`)

---

## 🧪 Testing Distributed Behavior

### Crash Recovery Test

1. Submit job
2. Kill worker:

```bash
docker kill worker-1
```

3. Wait 30s
4. Restart worker:

```bash
docker compose up -d worker
```

Job is automatically recovered.

---

### Horizontal Scaling

```bash
docker compose up -d --scale worker=3
```

Jobs are distributed across workers.

---

### Retry + Backoff

Force failures in worker:

```python
raise Exception("fail")
```

Observe:

```
Retrying job in 2s
Retrying job in 4s
Retrying job in 8s
Job -> DLQ
```

---

## 🧩 Design Decisions

### Why PostgreSQL instead of Redis?

* Strong consistency
* Transactions
* Durable leases
* Easier debugging

### Why leases instead of locks?

* Locks break on crashes
* Leases expire safely
* Enables job stealing

### Why Prometheus + Grafana instead of custom UI?

* Production-grade observability
* Real-time dashboards
* No toy frontend

---

## 🧠 Learning Outcomes

This project demonstrates:

* Distributed coordination
* Lease-based ownership
* Crash recovery
* Exactly-once semantics
* Exponential retry strategies
* Real observability patterns

This is the **same architecture pattern used in production systems** like:

* Kubernetes
* Temporal
* Airflow
* SQS-based workers

---

## 📌 Future Improvements

* Priority queues
* Job scheduling (cron)
* Multi-tenant isolation
* Workflow DAGs
* Auto-scaling workers
* Web UI for DLQ

---

## 👨‍💻 Author

Chiranjeev (CJ)
Software Development Engineer (Amazon)
Built as a distributed systems learning project.
