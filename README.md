# Distributed Task Queue System

A lightweight distributed task queue for parallel task processing with Python, Flask, and SQLite.

## Quick Start

```bash
# Install
pip install flask requests python-dotenv

# Start dispatcher (terminal 1)
./start_dispatcher.sh

# Start worker (terminal 2)
./start_worker.sh worker-1

# Submit tasks (terminal 3)
python3 add_task.py 5 100000
```

## Features

- ⚡ **Parallel Processing** - Multiple workers process tasks simultaneously
- 🔄 **Fault Tolerance** - Automatic task reassignment if workers fail
- 💾 **Result Caching** - Workers cache results when dispatcher is down
- �� **Atomic Operations** - No race conditions with task claiming
- 📊 **Monitoring** - Heartbeat tracking and statistics

## Architecture

```
Client → Dispatcher (Flask + SQLite) → Workers
         ↓
    Task Queue (atomic claiming)
         ↓
    Results Storage
```

**Flow:**

1. Client submits task via REST API
2. Dispatcher stores task in SQLite queue
3. Worker claims task atomically (with lease)
4. Worker executes task and returns result
5. Dispatcher saves result

## Project Structure

```
.
├── dispatcher/          # Flask server + SQLite
│   ├── app.py          # Server entry point
│   ├── routes.py       # API endpoints
│   └── db.py           # Database operations
├── worker/             # Worker nodes
│   └── worker.py       # Worker logic
├── tasks/              # Task implementations
│   ├── prime_task.py   # Prime number finding
│   └── compute_task.py # General compute
├── add_task.py         # CLI to submit tasks
└── .env                # Configuration
```

## Running Multiple Workers

```bash
# Terminal 1 - Dispatcher
./start_dispatcher.sh

# Terminal 2 - Worker 1
./start_worker.sh worker-1

# Terminal 3 - Worker 2
./start_worker.sh worker-2

# Terminal 4 - Worker 3
./start_worker.sh worker-3
```

Tasks are distributed across all workers automatically.

## API Endpoints

| Endpoint         | Method | Purpose                 |
| ---------------- | ------ | ----------------------- |
| `/submit-task`   | POST   | Submit a new task       |
| `/get-task`      | POST   | Claim task (workers)    |
| `/submit-result` | POST   | Submit result (workers) |
| `/heartbeat`     | POST   | Worker heartbeat        |
| `/task/:id`      | GET    | Get task status         |
| `/stats`         | GET    | System statistics       |
| `/health`        | GET    | Health check            |

## Configuration

Edit `.env` file to customize settings:

```bash
DISPATCHER_PORT=5000
WORKER_POLL_INTERVAL=5
WORKER_HEARTBEAT_INTERVAL=30
CACHE_TTL_SECONDS=3600
PRIMES_MAX_LIMIT=100000000
```

## Example: Prime Numbers

```bash
# Find primes up to 100,000 using fast algorithm
python3 add_task.py 1 100000 sieve

# Find primes using slower algorithm
python3 add_task.py 1 100000 trial_division

# Submit 10 tasks
python3 add_task.py 10 50000
```

### Task Claiming (Atomic)

Workers claim tasks using `BEGIN IMMEDIATE` transaction to prevent race conditions. Each task has:

- Lease duration (default 120s)
- Worker assignment
- Status tracking (pending → in_progress → completed)

### Fault Tolerance

- **Heartbeat**: Workers send heartbeat every 30s
- **Dead Worker Detection**: No heartbeat for 60s → worker marked dead
- **Task Reassignment**: Expired leases → task returns to queue
- **Result Caching**: Workers cache results locally if dispatcher is down

### Result Caching

When dispatcher is unavailable:

1. Worker saves result to local cache
2. Worker retries submission every 20s
3. Cache expires after 1 hour (configurable)
4. Successful submission deletes cache

## Requirements

- Python 3.7+
- Flask
- Requests
- python-dotenv

```bash
pip install -r requirements.txt
```

## License

This priject is open source - use for learning and projects.
