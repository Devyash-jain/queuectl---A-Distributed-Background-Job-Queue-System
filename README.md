# queuectl - A Distributed Background Job Queue System
queuectl is a CLI-based job queue system that executes background commands using worker processes. It supports retries with exponential backoff, Dead Letter Queue, persistent storage using SQLite, and a minimal web dashboard.
This system is similar in concept to:
Celery • Sidekiq • BullMQ — but intentionally lightweight, single-binary, and simple.

## Features
| Feature                         | Description                                                   |
| ------------------------------- | ------------------------------------------------------------- |
| **Queue + Workers**             | Run multiple workers in parallel to process jobs              |
| **Persistent Storage**          | SQLite WAL ensures no job is lost after restart               |
| **Retry + Exponential Backoff** | Failed jobs retry with configurable delay logic               |
| **Dead Letter Queue (DLQ)**     | Permanently failed jobs are moved to DLQ for later inspection |
| **Graceful Shutdown**           | Workers finish running jobs before exiting                    |
| **Configurable Behavior**       | `max_retries`, `backoff_base`, etc. through CLI               |
| **Simple CLI**                  | Use `queuectl run "command"` instead of raw JSON              |
| **Optional Web Dashboard**      | Monitor jobs & workers visually                               |

## Project Structure
```
queuectl/
│
├── queuectl.py          # CLI entry point
├── README.md            # Documentation (this file)
│
└── queuectl/
    ├── cli.py           # Command-line interface
    ├── worker.py        # Worker process execution logic
    ├── storage.py       # SQLite persistence layer
    ├── backoff.py       # Exponential backoff implementation
    └── dashboard.py     # (Optional) Web dashboard
```

## Installation
```
pip install -e .
```
This registers queuectl as a system command.

## Quick Start
### 1. Start Workers
```
queuectl worker start --count 1
```
### 2. Enqueue a Job (Simple Mode)
```
queuectl run "echo Hello World"
```
### 3. View Job Status
```
queuectl status
```

## Job Model
```
{
  "id": "unique-job-id",
  "command": "echo Hello",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}
```

## Job States
| State          | Meaning                              |
| -------------- | ------------------------------------ |
| **pending**    | Waiting to be processed              |
| **processing** | Currently running                    |
| **completed**  | Execution succeeded                  |
| **failed**     | Temporary failure; will retry later  |
| **dead**       | Moved to DLQ after retries exhausted |

## Dead Letter Queue (DLQ)
List DLQ jobs:
```
queuectl dlq list
```
Retry a failed job:
```
queuectl dlq retry job_id
```

## Configuration
Command
```
queuectl config show
queuectl config set max_retries 5
queuectl config set backoff_base 3
```

## Optional Web Dashboard
Start dashboard:
```
queuectl dashboard --port 8000
```

## System Workflow
```
┌────────────────────┐       ┌───────────────────────┐
│ queuectl run/cmd   │─────▶│   Insert job into DB   |
└────────────────────┘       └─────────┬─────────────┘
                                        │ pending
                              ┌─────────▼───────────┐
                              │     Worker(s)       │
                              │  pop pending jobs   │
                              └─────────┬───────────┘
                                        │ run command
                           ┌────────────▼───────────────┐
                           │ Exit Code = 0  → completed │
                           │ Exit Code ≠ 0  → retry     │
                           └────────────┬───────────────┘
                                        │ attempts > max_retries
                                        ▼
                                   Move to DLQ
```

## Demo Test Script
```
./demo_test.ps1
```
Automatically verifies:
* Success path
* Retry path
* DLQ handling
* Requeue from DLQ


