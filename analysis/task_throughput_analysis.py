#!/usr/bin/env python3

import os
import sys
import time
import json
import signal
import sqlite3
import subprocess
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TASK_SIZE = 1000
NUM_TASKS = 16
WORKER_COUNTS = [1, 2, 4, 8]
METHOD = 'trial_division'

DISPATCHER_URL = 'http://localhost:5000'
RESULTS_DIR = Path(__file__).parent / 'results'
RESULTS_DIR.mkdir(exist_ok=True)
DB_PATH = Path(__file__).parent.parent / 'dispatcher' / 'queue_db.db'

def cleanup_database():
    print("\n  Cleaning database...", end='', flush=True)
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
    print("  Killing all workers...", end='', flush=True)
    subprocess.run(['pkill', '-f', 'throughput_worker'], 
                  stdout=subprocess.DEVNULL, 
                  stderr=subprocess.DEVNULL)
    time.sleep(1)
    print(" Done")


class WorkerManager:
    def __init__(self):
        self.workers = []
    
    def start_workers(self, count):
        print(f"  Starting {count} workers...", end='', flush=True)
        for i in range(count):
            worker_id = f"throughput_worker_{i+1}"
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
                'pgid': os.getpgid(process.pid)
            })
        
        time.sleep(2)
        print(" Done")
    
    def stop_all_workers(self):
        print(f"  Stopping {len(self.workers)} workers...", end='', flush=True)
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
    
    def wait_for_completion(self, task_ids, timeout=600):
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
                return True
            
            time.sleep(0.5)
        
        return False


def run_test(task_size, num_workers):
    print(f"\n  Workers: {num_workers}")
    
    worker_manager = WorkerManager()
    task_submitter = TaskSubmitter(DISPATCHER_URL)
    
    worker_manager.start_workers(num_workers)
    
    print(f" Submitting {NUM_TASKS} tasks...", end='', flush=True)
    task_ids = []
    submit_start = time.time()
    for _ in range(NUM_TASKS):
        task_id = task_submitter.submit_task(task_size)
        task_ids.append(task_id)
    submit_time = time.time() - submit_start
    print(f" Done ({submit_time:.2f}s)")
    
    print(f"  Waiting for completion...", end='', flush=True)
    exec_start = time.time()
    success = task_submitter.wait_for_completion(task_ids)
    execution_time = time.time() - exec_start
    
    if success:
        print(f" Done ({execution_time:.2f}s)")
    else:
        print(" TIMEOUT!")
    
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dispatcher', 'queue_db.db')
    
    task_times = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for task_id in task_ids:
            cursor.execute("SELECT computation_time FROM task_results WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            if row and row[0]:
                task_times.append(row[0])
        
        conn.close()
    except Exception as e:
        print(f"\n  Warning: Could not get computation times: {e}")
    
    if task_times and len(task_times) == len(task_ids):
        sorted_times = sorted(task_times, reverse=True)
        
        if num_workers >= len(task_ids):
            actual_execution_time = max(task_times)
        else:
            worker_loads = [0.0] * num_workers
            for task_time in sorted_times:
                min_idx = min(range(num_workers), key=lambda i: worker_loads[i])
                worker_loads[min_idx] += task_time
            actual_execution_time = max(worker_loads)
    else:
        actual_execution_time = execution_time
    
    total_time = time.time() - submit_start
    throughput = len(task_ids) / actual_execution_time if success and actual_execution_time > 0 else 0
    
    worker_manager.stop_all_workers()
    cleanup_database()
    kill_all_workers()
    
    result = {
        'task_size': task_size,
        'num_workers': num_workers,
        'num_tasks': len(task_ids),
        'task_ids': task_ids,
        'submit_time': submit_time,
        'execution_time': actual_execution_time,
        'wall_clock_time': execution_time,
        'total_time': total_time,
        'throughput': throughput,
        'completed': len(task_ids) if success else 0,
        'success': success,
        'task_computation_times': task_times,
        'avg_computation_time': sum(task_times) / len(task_times) if task_times else 0,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"  ✓ Execution Time: {actual_execution_time:.4f}s (actual parallel time)")
    print(f"    Wall-clock: {execution_time:.2f}s (includes polling overhead)")
    print(f"  ✓ Throughput: {throughput:.2f} tasks/sec")
    
    return result


def main():
    print("="*80)
    print("SINGLE TASK SIZE ANALYSIS")
    print("="*80)
    print(f"\nTask Size: {TASK_SIZE:,}")
    print(f"Worker Counts: {WORKER_COUNTS}")
    print(f"Method: {METHOD}")
    print(f"Tasks per test: 10")
    print(f"\nTotal tests: {len(WORKER_COUNTS)}")
    print("="*80)
    all_results = []
    test_count = 0
    start_time = datetime.now()
    
    try:
        print(f"\n{'='*80}")
        print(f"TESTING TASK SIZE: {TASK_SIZE:,}")
        print(f"{'='*80}")
        
        for num_workers in WORKER_COUNTS:
            test_count += 1
            print(f"\n[Test {test_count}/{len(WORKER_COUNTS)}]")
            
            try:
                result = run_test(TASK_SIZE, num_workers)
                all_results.append(result)
                time.sleep(2)
                
            except KeyboardInterrupt:
                print("\n\n Test interrupted by user")
                raise
            except Exception as e:
                print(f"\n Error: {e}")
                continue
    
    finally:
        print("\n" + "="*80)
        print("FINAL CLEANUP")
        print("="*80)
        kill_all_workers()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    output_file = RESULTS_DIR / f'results_{TASK_SIZE}.json'
    results_data = {
        'test_start': start_time.isoformat(),
        'test_end': end_time.isoformat(),
        'duration_seconds': duration,
        'task_size': TASK_SIZE,
        'worker_counts': WORKER_COUNTS,
        'method': METHOD,
        'total_tests': len(all_results),
        'results': all_results
    }
    
    with open(output_file, 'w') as f:
        json.dump(results_data, f, indent=2)
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    print(f"Task Size: {TASK_SIZE:,}")
    print(f"Total Duration: {duration:.1f} seconds")
    print(f"Tests Completed: {len(all_results)}/{len(WORKER_COUNTS)}")
    print(f"Results saved: {output_file}")


if __name__ == '__main__':
    main()
