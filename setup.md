Distributed Task Queue & Job Processor

A simplified distributed job queue and worker system built as a prototype to demonstrate core backend system design concepts such as durability, leasing, retries, rate limiting, and observability.

This project is designed to be:

Reliable (jobs survive restarts)

Observable (metrics, logs, traces)

Multi-tenant (per-user quotas)

Simple enough for a prototype

Tech Stack
Core

Language: Python 3.10

API Framework: FastAPI

Database / Queue: SQLite

Workers: Separate Python service or process

Observability

Metrics: Prometheus

Dashboard: Grafana

Logs: Stdout with Loki

Tracing: OpenTelemetry

Environment

OS: Windows

Development Environment: WSL 2 (Ubuntu)

Python Environment: Conda

Containers: Docker and Docker Compose

Architecture (High Level)

Client
→ FastAPI (API Service)
→ SQLite (Jobs Table)
→ Worker Service(s)
→ Prometheus → Grafana

Logs flow to Loki and traces are exported via OpenTelemetry.

The API service is stateless.
Workers poll jobs from SQLite using lease-based locking.
The observability stack runs via Docker.

Prerequisites
1. Install WSL 2 with Ubuntu

From Windows PowerShell (run as Administrator):

Run: wsl --install -d Ubuntu

Restart your system if prompted.

Verify installation:

Run: wsl -l -v

You should see Ubuntu listed with version 2.

2. Install Docker Desktop

Download and install Docker Desktop for Windows.

Open Docker Desktop and go to:

Settings → Resources → WSL Integration

Enable integration for Ubuntu and click Apply & Restart.

Verify from inside Ubuntu (WSL):

Run: docker ps

If no error occurs, Docker is correctly set up.

Project Setup

All steps below must be executed inside the Ubuntu (WSL) terminal.

Step 1: Create Project Directory

Navigate to your Linux home directory:

Run: cd ~

Create project folder:

Run: mkdir job-queue

Enter project folder:

Run: cd job-queue

Important: Always work inside the Linux filesystem (home directory), not inside /mnt/c.

Step 2: Install Miniconda in WSL

Download Miniconda:

Run: wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

Install Miniconda:

Run: bash Miniconda3-latest-Linux-x86_64.sh

During installation:

Accept license

Use default installation path

Say yes when asked to initialize Conda

Restart shell:

Run: exec bash

Verify Conda:

Run: conda --version

Step 3: Create Conda Environment

Create environment:

Run: conda create -n jobqueue python=3.10

Activate environment:

Run: conda activate jobqueue

Your terminal prompt should now show:

(jobqueue) username@machine:~/job-queue

Step 4: Install Python Dependencies

Install core packages:

Run: pip install fastapi uvicorn sqlalchemy aiosqlite

Install observability packages:

Run: pip install prometheus-client opentelemetry-api opentelemetry-sdk

Step 5: Start Observability Stack

Create a file named docker-compose.yml in the project root.

Add services for:

Prometheus on port 9090

Grafana on port 3000

Loki on port 3100

Start containers:

Run: docker compose up

Access dashboards:

Grafana: http://localhost:3000

Login: admin / admin

Prometheus: http://localhost:9090

Step 6: Run API Service

Create an api folder inside project.

Create main.py inside api folder.

Add a basic FastAPI application with a health endpoint.

Start the server:

Run: uvicorn api.main:app --reload

Verify:

Health endpoint: http://localhost:8000/health

API docs: http://localhost:8000/docs

SQLite Database

SQLite is used as a file-based database.

The database file will be automatically created inside the project directory.

No database server setup is required.

Development Workflow

Every time you start working on the project:

Open Ubuntu (WSL)

Navigate to project directory

Activate Conda environment

Commands:

cd ~/job-queue
conda activate jobqueue

Never:

Use PowerShell for development

Work inside /mnt/c directories

Use Windows Python

Always use Ubuntu terminal.

Why This Setup?
WSL

Provides a real Linux environment on Windows and avoids OS-specific issues.

Conda

Ensures isolated and reproducible Python environments.

SQLite

Offers durability and ACID guarantees with zero operational overhead.

Separate Workers

Allows independent scaling of API and background processing.

Grafana

Provides operational dashboards similar to real production systems.

Production Notes and Tradeoffs

This system is a prototype.

In a production environment:

SQLite would be replaced with Postgres or Redis

Polling workers would be replaced with event-driven queues

Docker Compose would be replaced with Kubernetes

Local metrics would be replaced with centralized monitoring

Key Concepts Demonstrated

Lease-based locking

At-least-once processing

Retry with Dead Letter Queue

Idempotency

Rate limiting

Observability-first design

Multi-tenant quotas

How to Know Everything Is Correct

Your terminal should display:

(jobqueue) username@machine:~/job-queue

You should be able to:

Run Python inside Ubuntu

Run Docker commands inside Ubuntu

Start FastAPI successfully

If all of these work, the system is set up correctly.