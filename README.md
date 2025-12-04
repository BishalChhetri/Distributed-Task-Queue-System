# Distributed Task Queue System

A fault-tolerant distributed task queue system built with Python, Flask, and SQLite. The system supports parallel task processing across multiple workers with automatic failure recovery and checkpoint-based resumption.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the dispatcher (terminal 1)
./start_dispatcher.sh

# Start workers (terminal 2, 3, 4...)
./start_worker.sh worker-1
./start_worker.sh worker-2

# Submit tasks (terminal N)
python3 add_task.py 5 100000
```

## Features

- ⚡ **Parallel Processing** - Multiple workers process tasks simultaneously
- 🔄 **Fault Tolerance** - Automatic detection and recovery from worker failures
- 💾 **Checkpoint Support** - Tasks can resume from last checkpoint after failure
- 🔒 **Atomic Operations** - Database-level locking prevents race conditions
- 📊 **Performance Analysis** - Built-in throughput and recovery time analysis tools
- 💓 **Heartbeat Monitoring** - Real-time worker health tracking

## Architecture

The system consists of three main components:

**Dispatcher** - Central coordinator that manages the task queue

- Receives tasks from clients via REST API
- Distributes tasks to available workers
- Monitors worker health through heartbeat messages
- Handles task reassignment when workers fail

**Workers** - Processing nodes that execute tasks

- Poll dispatcher for available tasks
- Execute computation tasks (like finding prime numbers)
- Send results back to dispatcher
- Support checkpoint/resume for long-running tasks

**Database** - SQLite database with four tables:

- `tasks` - Stores all submitted tasks and their status
- `task_results` - Stores computation results from workers
- `checkpoints` - Saves intermediate progress for resumable tasks
- `workers` - Tracks worker health and heartbeat status

## Project Structure

```
.
├── dispatcher/              # Central coordinator
│   ├── app.py              # Flask server with background monitor
│   ├── routes.py           # REST API endpoints
│   ├── db.py               # Database operations
│   ├── models.py           # Database table schemas
│   ├── heartbeat_monitor.py # Worker health monitoring
│   └── utils.py            # Helper functions
│
├── worker/                 # Worker nodes
│   ├── worker.py           # Main worker loop
│   ├── cache_manager.py    # Local result caching
│   ├── tasks.py            # Task execution logic
│   └── config.py           # Worker configuration
│
├── tasks/                  # Task implementations
│   ├── prime_task.py       # Prime number algorithms
│   └── compute_task.py     # General computation tasks
│
├── analysis/               # Performance analysis tools
│   ├── task_throughput_analysis.py      # Throughput test runner
│   ├── throughput_analysis_plot.py      # Throughput visualization
│   ├── recovery_time_analysis.py        # Recovery test runner
│   ├── recovery_analysis_plot.py        # Recovery visualization
│   ├── results/                         # Test results (JSON)
│   └── recovery_plots/                  # Generated plots
│
├── shared/                 # Shared utilities
│   ├── config.py           # System-wide configuration
│   └── utils.py            # Common helper functions
│
├── add_task.py             # CLI tool to submit tasks
├── start_dispatcher.sh     # Script to start dispatcher
├── start_worker.sh         # Script to start workers
├── cleanup.sh              # Cleanup script
└── .env                    # Configuration file
```

## Running the System

### Basic Operation

```bash
# 1. Start the dispatcher
./start_dispatcher.sh

# 2. Start multiple workers (in separate terminals)
./start_worker.sh worker-1
./start_worker.sh worker-2
./start_worker.sh worker-3

# 3. Submit tasks
python3 add_task.py 10 100000      # 10 tasks, find primes up to 100,000
python3 add_task.py 5 1000000      # 5 tasks, find primes up to 1,000,000
```

### With Analysis Tools

```bash
# Run throughput analysis
python3 analysis/task_throughput_analysis.py

# Generate throughput plots
python3 analysis/throughput_analysis_plot.py

# Run recovery time analysis (baseline - no failures)
python3 analysis/recovery_time_analysis.py --dead-workers 0

# Run recovery tests with failures
python3 analysis/recovery_time_analysis.py --dead-workers 2
python3 analysis/recovery_time_analysis.py --dead-workers 4

# Generate recovery plots
python3 analysis/recovery_analysis_plot.py
```

## API Endpoints

The dispatcher exposes these REST API endpoints:

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

Please edit the `.env` file by yourself to customize system behavior.

## How It Works

### Task Execution Flow

1. Client submits a task via REST API
2. Dispatcher stores the task in the database with `pending` status
3. Worker polls for tasks and claims one atomically using database locks
4. Task status changes to `in-progress` with a lease expiration time
5. Worker executes the task and sends results back
6. Dispatcher saves results and marks task as `completed`

### Fault Tolerance

The system handles worker failures automatically:

**Heartbeat Monitoring**

- Workers send heartbeat messages every 1 seconds
- Dispatcher marks workers as dead if no heartbeat for 2 seconds

**Task Recovery**

- Tasks assigned to dead workers are automatically reclaimed
- Reclaimed tasks return to `pending` status
- Other healthy workers can pick them up
- If checkpoints exist, tasks resume from last checkpoint

**Lease Expiration**

- Each task has a lease duration (default 100 seconds)
- If a task isn't completed before lease expires, it goes back to the queue
- Prevents tasks from being stuck forever

### Checkpoint System

For long-running tasks:

1. Worker periodically saves progress to the checkpoints table
2. If the worker dies, another worker can load the checkpoint
3. Task resumes from where it left off instead of starting over
4. Checkpoint includes: last checked number, primes found so far, elapsed time

## Analysis Tools

The system includes two analysis tools for performance evaluation:

### Throughput Analysis

Tests how execution time scales with number of workers:

```bash
# Edit configuration in task_throughput_analysis.py:
# - TASK_SIZE: Size of computation (e.g., 1000, 1000000)
# - WORKER_COUNTS: List of worker counts to test [1, 2, 4, 8]
# - NUM_TASKS: Number of tasks per test

python3 analysis/task_throughput_analysis.py
python3 analysis/throughput_analysis_plot.py
```

Generates plots showing:

- Execution time vs number of workers
- Throughput (tasks/second) vs number of workers
- Speedup vs number of workers (actual vs ideal)

### Recovery Time Analysis

Tests system recovery from worker failures:

```bash
# Run baseline (no failures)
python3 analysis/recovery_time_analysis.py --dead-workers 0

# Test with different failure scenarios
python3 analysis/recovery_time_analysis.py --dead-workers 2
python3 analysis/recovery_time_analysis.py --dead-workers 4
python3 analysis/recovery_time_analysis.py --dead-workers 6
python3 analysis/recovery_time_analysis.py --dead-workers 8

# Generate plots
python3 analysis/recovery_analysis_plot.py
```

Generates plots showing:

- Execution time with failures vs baseline
- Recovery overhead in seconds
- Recovery overhead as percentage
- Impact of failure rate on performance

## Example Usage

### Finding Prime Numbers

```bash
# Find primes up to 100,000 using fast sieve algorithm
python3 add_task.py 1 100000 sieve

# Find primes using trial division (slower but simpler)
python3 add_task.py 1 100000 trial_division

# Submit 10 tasks for parallel processing
python3 add_task.py 10 50000
```

### Checking Task Status

```bash
# Get status of task with ID 1
curl http://localhost:5000/task/1

# Get system statistics
curl http://localhost:5000/stats
```

### Cleanup

```bash
# Stop all workers and dispatcher
./cleanup.sh
```

## Requirements

- Python 3.7 or higher
- Flask
- Requests
- python-dotenv
- matplotlib (for analysis plots)
- numpy (for analysis calculations)

```bash
pip install -r requirements.txt
```

## License

This project is open source and available for learning and research purposes.
