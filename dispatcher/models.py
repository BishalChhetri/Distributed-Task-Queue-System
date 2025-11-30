import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_LEASE_DURATION,
    DEFAULT_HEARTBEAT_TIMEOUT
)

TASKS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    assigned_to TEXT,
    claimed_at TIMESTAMP,
    lease_expires TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 5,
    created_at TIMESTAMP DEFAULT (datetime('now')),
    updated_at TIMESTAMP DEFAULT (datetime('now'))
);
"""

TASK_RESULTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    primes TEXT,
    status TEXT NOT NULL,
    computation_time REAL,
    was_resumed INTEGER DEFAULT 0,
    checkpoint_time REAL,
    resume_time REAL,
    method TEXT,
    created_at TIMESTAMP DEFAULT (datetime('now')),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
"""

TASK_RESULTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_task_results_task_id ON task_results(task_id);",
    "CREATE INDEX IF NOT EXISTS idx_task_results_worker_id ON task_results(worker_id);"
]

CHECKPOINTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    task_id INTEGER PRIMARY KEY,
    last_checked INTEGER NOT NULL,
    primes TEXT NOT NULL,
    elapsed_time REAL NOT NULL,
    method TEXT,
    updated_at TIMESTAMP DEFAULT (datetime('now')),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
"""

WORKERS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    last_heartbeat TIMESTAMP DEFAULT (datetime('now')),
    status TEXT DEFAULT 'alive',
    metadata TEXT
);
"""

STATUS_PENDING = 'pending'
STATUS_IN_PROGRESS = 'in-progress'
STATUS_COMPLETED = 'completed'
STATUS_FAILED = 'failed'

WORKER_STATUS_ALIVE = 'alive'
WORKER_STATUS_DEAD = 'dead'
