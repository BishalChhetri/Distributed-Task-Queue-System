#!/usr/bin/env python3

import os
import sys
import time
import json
import signal
import sqlite3
import subprocess
import requests
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NUM_WORKERS = 16
NUM_TASKS = 16
TASK_SIZE = 1000000
METHOD = 'trial_division'

DISPATCHER_URL = 'http://localhost:5000'
RESULTS_DIR = Path(__file__).parent / 'results'
RESULTS_DIR.mkdir(exist_ok=True)
DB_PATH = Path(__file__).parent.parent / 'dispatcher' / 'queue_db.db'

def cleanup_database():
    print("\n Cleaning database...", end='', flush=True)
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM task_results")
        cursor.execute("DELETE FROM tasks")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='task_results'")
        conn.commit()
        conn.close()
        print(" Done")
    except Exception as e:
        print(f" Error: {e}")

def kill_all_workers():
    print(" Killing all workers...", end='', flush=True)
    subprocess.run(['pkill', '-f', 'recovery_worker'], 
                  stdout=subprocess.DEVNULL, 
                  stderr=subprocess.DEVNULL)
    time.sleep(1)
    print(" Done")

class WorkerManager:
    def __init__(self):
        self.workers = []
    
    def start_workers(self, count):
        print(f" Starting {count} workers...", end='', flush=True)
        for i in range(count):
            worker_id = f"recovery_worker_{i+1}"
            env = os.environ.copy()
            env['WORKER_ID'] = worker_id
            
            process = subprocess.Popen(
                ['python3', 'worker/worker.py'],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid
            )
            
            self.workers.append({
                'id': worker_id,
                'process': process,
                'pid': process.pid,
                'pgid': os.getpgid(process.pid)
            })
        
        time.sleep(2)
        print(" Done")
    
    def get_workers_with_inprogress_tasks(self):
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT assigned_to, COUNT(*) as task_count
                FROM tasks
                WHERE status = 'in-progress' AND assigned_to IS NOT NULL
                GROUP BY assigned_to
                ORDER BY task_count DESC
            """)
            
            workers_with_tasks = cursor.fetchall()
            conn.close()
            
            return [w[0] for w in workers_with_tasks]
        except Exception as e:
            print(f"Error getting workers with tasks: {e}")
            return []
    
    def kill_specific_workers(self, num_to_kill, delay_before_kill=1):
        workers_with_tasks = self.get_workers_with_inprogress_tasks()
        
        if len(workers_with_tasks) < num_to_kill:
            print(f"  Only {len(workers_with_tasks)} workers have in-progress tasks, but need to kill {num_to_kill}")
            return []
        
        workers_to_kill = workers_with_tasks[:num_to_kill]
        
        print(f" Killing {num_to_kill} workers with in-progress tasks: {workers_to_kill}")
        
        if delay_before_kill > 0:
            print(f" Waiting {delay_before_kill}s to let workers process...", end='', flush=True)
            time.sleep(delay_before_kill)
            print(" Done")
        
        killed = []
        for worker_id in workers_to_kill:
            worker_info = next((w for w in self.workers if w['id'] == worker_id), None)
            if worker_info:
                try:
                    os.killpg(worker_info['pgid'], signal.SIGKILL)
                    killed.append(worker_id)
                    print(f"   ✓ Killed {worker_id}")
                except Exception as e:
                    print(f"   ✗ Failed to kill {worker_id}: {e}")
        
        return killed
    
    def stop_all_workers(self):
        print(f" Stopping {len(self.workers)} workers...", end='', flush=True)
        for worker in self.workers:
            try:
                os.killpg(worker['pgid'], signal.SIGTERM)
            except:
                pass
        
        time.sleep(1)
        
        for worker in self.workers:
            try:
                if worker['process'].poll() is None:
                    os.killpg(worker['pgid'], signal.SIGKILL)
            except:
                pass
        
        self.workers = []
        time.sleep(1)
        print(" Done")

class TaskSubmitter:
    def __init__(self, dispatcher_url):
        self.dispatcher_url = dispatcher_url
    
    def submit_task(self, limit):
        response = requests.post(
            f'{self.dispatcher_url}/submit-task',
            json={'task_type': 'compute', 'task_data': {'limit': limit, 'method': METHOD}},
            timeout=10
        )
        return response.json()['data']['task_id']
    
    def get_task_status(self, task_id):
        response = requests.get(f'{self.dispatcher_url}/task/{task_id}', timeout=10)
        return response.json()['data']
    
    def get_all_tasks_status(self):
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM tasks 
                GROUP BY status
            """)
            
            status_counts = dict(cursor.fetchall())
            conn.close()
            return status_counts
        except Exception as e:
            print(f"\n  Database error: {e}")
            print(f"    DB Path: {DB_PATH}")
            return {}
    
    def wait_for_inprogress_tasks(self, min_tasks=2, timeout=60):
        print(f" Waiting for at least {min_tasks} tasks to be in progress...", end='', flush=True)
        start_time = time.time()
        last_status = {}
        
        while time.time() - start_time < timeout:
            status = self.get_all_tasks_status()
            in_progress = status.get('in-progress', 0)
            
            if status != last_status:
                print(f"\n   Status: {status}", end='', flush=True)
                last_status = status.copy()
            
            if in_progress >= min_tasks:
                print(f"\n   Done ({in_progress} tasks in progress)")
                return True
            
            time.sleep(0.2)
        
        print(f"\n   Timeout (only {status.get('in-progress', 0)} tasks in progress)")
        return False
    
    def wait_for_completion(self, task_ids, timeout=600):
        print(f" Waiting for all tasks to complete...", end='', flush=True)
        start_time = time.time()
        completed = set()
        
        while time.time() - start_time < timeout:
            for task_id in task_ids:
                if task_id in completed:
                    continue
                
                try:
                    status = self.get_task_status(task_id)
                    if status['status'] == 'completed':
                        completed.add(task_id)
                except:
                    pass
            
            if len(completed) == len(task_ids):
                elapsed = time.time() - start_time
                print(f" Done ({elapsed:.2f}s)")
                return True
            
            time.sleep(0.5)
        
        print(f" Timeout ({len(completed)}/{len(task_ids)} completed)")
        return False

def load_baseline_time():
    baseline_file = RESULTS_DIR / 'recovery_results_0.json'
    if baseline_file.exists():
        try:
            with open(baseline_file, 'r') as f:
                data = json.load(f)
                return data['total_time']
        except Exception as e:
            print(f"  Could not load baseline: {e}")
    return None


def run_recovery_test(num_dead_workers):
    print(f"\n{'='*60}")
    print(f"Recovery Time Analysis - {num_dead_workers} Dead Workers")
    print(f"{'='*60}")
    print(f"Workers: {NUM_WORKERS} | Tasks: {NUM_TASKS} | Task Size: {TASK_SIZE:,}")
    
    cleanup_database()
    kill_all_workers()
    
    worker_manager = WorkerManager()
    task_submitter = TaskSubmitter(DISPATCHER_URL)
    
    worker_manager.start_workers(NUM_WORKERS)
    
    print(f"\n Submitting {NUM_TASKS} tasks...", end='', flush=True)
    task_ids = []
    submit_start = time.time()
    exec_start = time.time()  # Start timing from task submission
    for _ in range(NUM_TASKS):
        task_id = task_submitter.submit_task(TASK_SIZE)
        task_ids.append(task_id)
    submit_time = time.time() - submit_start
    print(f" Done ({submit_time:.2f}s)")
    
    if not task_submitter.wait_for_inprogress_tasks(min_tasks=num_dead_workers):
        print(" Failed to get enough tasks in progress")
        worker_manager.stop_all_workers()
        return None
    
    print(f"\n Inducing failure...")
    kill_time = time.time()
    killed_workers = worker_manager.kill_specific_workers(num_dead_workers)
    
    if len(killed_workers) != num_dead_workers:
        print(f" Failed to kill {num_dead_workers} workers")
        worker_manager.stop_all_workers()
        return None
    
    success = task_submitter.wait_for_completion(task_ids, timeout=600)
    total_time = time.time() - exec_start  # Total time from task submission to completion
    
    if not success:
        print(" Tasks did not complete in time")
        worker_manager.stop_all_workers()
        return None
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*)
            FROM tasks
            WHERE id IN (
                SELECT task_id 
                FROM task_results 
                WHERE status = 'completed'
            )
            AND attempts > 1
        """)
        
        reassigned_tasks = cursor.fetchone()[0]
        
        cursor.execute("SELECT computation_time FROM task_results WHERE computation_time IS NOT NULL")
        task_times = [row[0] for row in cursor.fetchall()]
        
        conn.close()
    except Exception as e:
        print(f"Error reading database: {e}")
        task_times = []
        reassigned_tasks = 0
    
    # Try to load actual baseline from recovery_results_0.json
    baseline_time = load_baseline_time()
    
    if baseline_time:
        print(f" Using actual baseline from recovery_results_0.json: {baseline_time:.2f}s")
    else:
        print(f" No baseline file found, using simulated baseline")
        if task_times:
            sorted_times = sorted(task_times, reverse=True)
            baseline_worker_loads = [0.0] * NUM_WORKERS
            for task_time in sorted_times:
                min_idx = min(range(NUM_WORKERS), key=lambda i: baseline_worker_loads[i])
                baseline_worker_loads[min_idx] += task_time
            baseline_time = max(baseline_worker_loads)
        else:
            baseline_time = total_time
    
    recovery_overhead = total_time - baseline_time
    overhead_percentage = (recovery_overhead / baseline_time * 100) if baseline_time > 0 else 0
    
    results = {
        'num_workers': NUM_WORKERS,
        'num_tasks': NUM_TASKS,
        'task_size': TASK_SIZE,
        'num_dead_workers': num_dead_workers,
        'killed_workers': killed_workers,
        'baseline_time': round(baseline_time, 2),
        'total_time': round(total_time, 2),
        'recovery_overhead': round(recovery_overhead, 2),
        'overhead_percentage': round(overhead_percentage, 2),
        'reassigned_tasks': reassigned_tasks,
        'timestamp': datetime.now().isoformat()
    }
    
   
    print(f" Results:")
    print(f"   Baseline Time: {baseline_time:.2f}s")
    print(f"   Total Time: {total_time:.2f}s")
    print(f"   Recovery Overhead: {recovery_overhead:.2f}s ({overhead_percentage:.1f}%)")
    print(f"   Tasks Reassigned: {reassigned_tasks}")
    
    worker_manager.stop_all_workers()
    cleanup_database()
    
    return results

def run_baseline_test():
    print(f"\n{'='*60}")
    print(f"Baseline Test - No Failures")
    print(f"{'='*60}")
    print(f"Workers: {NUM_WORKERS} | Tasks: {NUM_TASKS} | Task Size: {TASK_SIZE:,}")
    
    cleanup_database()
    kill_all_workers()
    
    worker_manager = WorkerManager()
    task_submitter = TaskSubmitter(DISPATCHER_URL)
    
    worker_manager.start_workers(NUM_WORKERS)
    
    print(f"\n Submitting {NUM_TASKS} tasks...", end='', flush=True)
    task_ids = []
    submit_start = time.time()
    for _ in range(NUM_TASKS):
        task_id = task_submitter.submit_task(TASK_SIZE)
        task_ids.append(task_id)
    submit_time = time.time() - submit_start
    print(f" Done ({submit_time:.2f}s)")
    
    exec_start = time.time()
    success = task_submitter.wait_for_completion(task_ids, timeout=600)
    total_time = time.time() - exec_start
    
    if not success:
        print(" Tasks did not complete in time")
        worker_manager.stop_all_workers()
        return None
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute("SELECT computation_time FROM task_results WHERE computation_time IS NOT NULL")
        task_times = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"Error reading database: {e}")
        task_times = []
    
    results = {
        'num_workers': NUM_WORKERS,
        'num_tasks': NUM_TASKS,
        'task_size': TASK_SIZE,
        'num_dead_workers': 0,
        'killed_workers': [],
        'baseline_time': round(total_time, 2),
        'total_time': round(total_time, 2),
        'recovery_overhead': 0.0,
        'overhead_percentage': 0.0,
        'reassigned_tasks': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"\n{'='*60}")
    print(f" Results:")
    print(f"   Execution Time: {total_time:.2f}s")
    print(f"   Tasks Completed: {len(task_times)}/{NUM_TASKS}")
    print(f"{'='*60}")
    
    worker_manager.stop_all_workers()
    cleanup_database()
    
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dead-workers', type=int, required=True, 
                       help='Number of workers to kill during test (use 0 for baseline)')
    args = parser.parse_args()
    
    if args.dead_workers < 0 or args.dead_workers >= NUM_WORKERS:
        print(f" Invalid number of dead workers. Must be between 0 and {NUM_WORKERS-1}")
        sys.exit(1)
    
    if args.dead_workers == 0:
        results = run_baseline_test()
    else:
        results = run_recovery_test(args.dead_workers)
    
    if results:
        output_file = RESULTS_DIR / f'recovery_results_{args.dead_workers}.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n Results saved to: {output_file}")
    else:
        print("\n Test failed")
        sys.exit(1)

if __name__ == '__main__':
    main()
