# dispatcher/db.py
import sqlite3
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config import DB_TIMEOUT
from models import (
    TASKS_TABLE_SCHEMA, 
    TASK_RESULTS_TABLE_SCHEMA,
    TASK_RESULTS_INDEXES,
    WORKERS_TABLE_SCHEMA,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_LEASE_DURATION,
    STATUS_PENDING,
    STATUS_IN_PROGRESS,
    STATUS_COMPLETED,
    STATUS_FAILED
)

DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, "queue_db.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# I initialized db here 
def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    cursor.execute(TASKS_TABLE_SCHEMA)
    cursor.execute(TASK_RESULTS_TABLE_SCHEMA)
    for index_sql in TASK_RESULTS_INDEXES:
        cursor.execute(index_sql)
    cursor.execute(WORKERS_TABLE_SCHEMA)

    conn.commit()
    conn.close()
    print("[DB] Database initialized successfully")

# To insert task route
def insert_task(task_type, payload, max_attempts=DEFAULT_MAX_ATTEMPTS):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO tasks (task_type, payload, status, max_attempts)
            VALUES (?, ?, ?, ?)
        """, (task_type, payload, STATUS_PENDING, max_attempts))
        
        task_id = cursor.lastrowid
        conn.commit()
        
        print(f"[DB] Task inserted - ID: {task_id}, Type: {task_type}")
        
        return {
            "task_id": task_id,
            "task_type": task_type,
            "status": STATUS_PENDING,
            "max_attempts": max_attempts
        }
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to insert task: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Database error: {e}")
        
    finally:
        if conn:
            conn.close()


def claim_task(worker_id, lease_duration_seconds=DEFAULT_LEASE_DURATION):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        # BEGIN IMMEDIATE ensures atomic operation - acquires write lock immediately
        cursor.execute("BEGIN IMMEDIATE")
        
        # Find the oldest pending task OR tasks with expired leases
        cursor.execute("""
            SELECT id, task_type, payload, attempts, max_attempts, created_at
            FROM tasks
            WHERE (status = 'pending' OR (status = 'in-progress' AND lease_expires < datetime('now')))
              AND attempts < max_attempts
            ORDER BY created_at ASC
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        
        if not row:
            conn.rollback()
            print(f"[DB] No tasks available for worker: {worker_id}")
            return None
        
        task_id = row['id']
        task_type = row['task_type']
        payload = row['payload']
        attempts = row['attempts']
        max_attempts = row['max_attempts']
        created_at = row['created_at']
        
        cursor.execute("""
            UPDATE tasks
            SET status = 'in-progress',
                assigned_to = ?,
                claimed_at = datetime('now'),
                lease_expires = datetime('now', '+' || ? || ' seconds'),
                attempts = attempts + 1,
                updated_at = datetime('now')
            WHERE id = ?
        """, (worker_id, lease_duration_seconds, task_id))
        
        conn.commit()
        
        print(f"[DB] Task {task_id} claimed by worker {worker_id} (attempt {attempts + 1}/{max_attempts})")
        
        return {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload,
            "attempt": attempts + 1,
            "max_attempts": max_attempts,
            "lease_duration": lease_duration_seconds,
            "assigned_to": worker_id,
            "status": "in-progress",
            "created_at": created_at
        }
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to claim task: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Database error: {e}")
        
    finally:
        if conn:
            conn.close()


def update_heartbeat(worker_id, status='alive', metadata=None):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        # Use INSERT OR REPLACE to update or create worker record
        cursor.execute("""
            INSERT INTO workers (worker_id, last_heartbeat, status, metadata)
            VALUES (?, datetime('now'), ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                last_heartbeat = datetime('now'),
                status = excluded.status,
                metadata = excluded.metadata
        """, (worker_id, status, metadata))
        
        conn.commit()
        
        print(f"[DB] Heartbeat updated for worker: {worker_id}, status: {status}")
        
        return {
            "worker_id": worker_id,
            "status": status,
            "last_heartbeat": datetime.now().isoformat()
        }
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to update heartbeat: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Database error: {e}")
        
    finally:
        if conn:
            conn.close()


def save_result(task_id, primes, computation_time, method, status='completed', worker_id=None):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        
        if not row:
            raise Exception(f"Task {task_id} not found")
        
        current_status = row['status']
        
        if current_status == 'completed':
            print(f"[DB] Task {task_id} already completed (idempotent - no update)")
            return {
                "task_id": task_id,
                "status": "completed",
                "saved": False,
                "message": "Task already completed",
                "timestamp": datetime.now().isoformat()
            }
        
        primes_json = json.dumps(primes) if primes else None
        
        cursor.execute("""
            INSERT INTO task_results (task_id, worker_id, primes, status, computation_time, method)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (task_id, worker_id, primes_json, status, computation_time, method))
        
        cursor.execute("""
            UPDATE tasks
            SET status = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (status, task_id))
        
        conn.commit()
        
        print(f"[DB] Result saved for task {task_id}, status: {status}, worker: {worker_id}")
        
        return {
            "task_id": task_id,
            "status": status,
            "saved": True,
            "timestamp": datetime.now().isoformat()
        }
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to save result: {e}")
        if conn:
            conn.rollback()
        raise Exception(f"Database error: {e}")
        
    finally:
        if conn:
            conn.close()


def initialize_database():
    init_db()



def get_task_status(task_id):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT t.id, t.task_type, t.status, t.attempts, t.max_attempts, t.assigned_to,
                   tr.primes, tr.computation_time, tr.method, tr.created_at as result_ts
            FROM tasks t
            LEFT JOIN task_results tr ON t.id = tr.task_id
            WHERE t.id = ?
        """, (task_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        result_data = {
            "task_id": row['id'],
            "task_type": row['task_type'],
            "status": row['status'],
            "attempts": row['attempts'],
            "max_attempts": row['max_attempts'],
            "assigned_to": row['assigned_to']
        }
        
        if row['primes']:
            result_data['primes'] = json.loads(row['primes'])
            result_data['computation_time'] = row['computation_time']
            result_data['method'] = row['method']
            result_data['result_ts'] = row['result_ts']
        
        return result_data
        
    finally:
        if conn:
            conn.close()


def get_pending_tasks_count():
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM tasks
            WHERE status = 'pending'
        """)
        
        return cursor.fetchone()['count']
        
    finally:
        if conn:
            conn.close()


def get_active_workers():
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT worker_id, last_heartbeat, status, metadata
            FROM workers
            WHERE last_heartbeat > datetime('now', '-60 seconds')
              AND status = 'alive'
        """)
        
        return [dict(row) for row in cursor.fetchall()]
        
    finally:
        if conn:
            conn.close()


def reclaim_expired_tasks():
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE tasks
            SET status = 'pending',
                assigned_to = NULL,
                updated_at = datetime('now')
            WHERE status = 'in-progress'
              AND lease_expires < datetime('now')
              AND attempts < max_attempts
        """)
        
        reclaimed = cursor.rowcount
        conn.commit()
        
        if reclaimed > 0:
            print(f"[DB] Reclaimed {reclaimed} expired task(s)")
        
        return reclaimed
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to reclaim tasks: {e}")
        if conn:
            conn.rollback()
        return 0
        
    finally:
        if conn:
            conn.close()


def mark_dead_workers():
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE workers
            SET status = 'dead'
            WHERE last_heartbeat < datetime('now', '-60 seconds')
              AND status != 'dead'
        """)
        
        marked = cursor.rowcount
        conn.commit()
        
        if marked > 0:
            print(f"[DB] Marked {marked} worker(s) as dead")
        
        return marked
        
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to mark dead workers: {e}")
        if conn:
            conn.rollback()
        return 0
        
    finally:
        if conn:
            conn.close()
