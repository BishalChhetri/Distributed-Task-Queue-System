#!/usr/bin/env python3
import requests
import json
import time
import os
import sys
import threading
import pickle
import subprocess
import signal
import importlib
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DISPATCHER_URL = os.getenv('DISPATCHER_URL', 'http://localhost:5000')
WORKER_ID = os.getenv('WORKER_ID', f'worker_{os.getpid()}')
POLL_INTERVAL = int(os.getenv('WORKER_POLL_INTERVAL', '2'))
HEARTBEAT_INTERVAL = int(os.getenv('WORKER_HEARTBEAT_INTERVAL', '30'))
CACHE_RETRY_INTERVAL = int(os.getenv('CACHE_RETRY_INTERVAL', '10'))
CACHE_TTL = int(os.getenv('CACHE_TTL_SECONDS', '3600'))
USE_FORK_EXECUTION = os.getenv('USE_FORK_EXECUTION', 'False').lower() == 'true'
CHECKPOINT_ENABLED = os.getenv('CHECKPOINT_ENABLED', 'False').lower() == 'true'
CHECKPOINT_INTERVAL = int(os.getenv('CHECKPOINT_INTERVAL', '30'))

CACHE_DIR = Path(__file__).parent / os.getenv('CACHE_DIR', 'cache')
CACHE_DIR.mkdir(exist_ok=True)

CheckpointManager = None
if CHECKPOINT_ENABLED and USE_FORK_EXECUTION:
    from checkpoint_manager import CheckpointManager


def execute_task(task):
    task_type = task.get('task_type')
    task_id = task['task_id']
    payload = json.loads(task.get('payload', '{}'))
    
    try:
        task_module = importlib.import_module(f'tasks.{task_type}_task')
        
        if hasattr(task_module, 'execute'):
            result = task_module.execute(task_id, payload, result_file_path=None)
            return result
        else:
            return {
                "status": "failed",
                "error": f"Task module '{task_type}' does not have execute() function"
            }
    except ModuleNotFoundError:
        return {
            "status": "failed",
            "error": f"Task type '{task_type}' not found. Create tasks/{task_type}_task.py"
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Task execution error: {str(e)}"
        }


class Worker:
    def __init__(self, dispatcher_url, worker_id):
        self.dispatcher_url = dispatcher_url
        self.worker_id = worker_id
        self.running = False
        self.cache_dir = CACHE_DIR / worker_id
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        if CHECKPOINT_ENABLED and USE_FORK_EXECUTION:
            checkpoint_dir = os.getenv('CHECKPOINT_DIR', 'checkpoints')
            self.checkpoint_manager = CheckpointManager(checkpoint_dir, worker_id)
        else:
            self.checkpoint_manager = None
    
    def start(self):
        self.running = True
        
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        cache_retry_thread = threading.Thread(target=self._cache_retry_loop, daemon=True)
        cache_retry_thread.start()
        
        try:
            self._task_loop()
        except KeyboardInterrupt:
            print(f"\n  Interrupted by user\n")
            self.stop()
    
    def stop(self):
        print(f" Stopping worker: {self.worker_id}")
        self.running = False
    
    def _heartbeat_loop(self):
        while self.running:
            try:
                response = requests.post(
                    f"{self.dispatcher_url}/heartbeat",
                    json={
                        "worker_id": self.worker_id,
                        "status": "alive",
                        "metadata": {"timestamp": datetime.now().isoformat()}
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    print(f" Heartbeat sent")
            except Exception as e:
                print(f"  Heartbeat failed: {e}")
            
            time.sleep(HEARTBEAT_INTERVAL)
    
    def _cache_retry_loop(self):
        while self.running:
            time.sleep(CACHE_RETRY_INTERVAL)
            
            if not self.cache_dir.exists():
                continue
            
            cache_files = list(self.cache_dir.glob('*.cache'))
            
            for cache_file in cache_files:
                try:
                    with open(cache_file, 'rb') as f:
                        cached_data = pickle.load(f)
                    
                    cache_age = time.time() - cached_data['timestamp']
                    
                    if cache_age > CACHE_TTL:
                        print(f"   Cache expired: {cache_file.name}")
                        cache_file.unlink()
                        continue
                    
                    success = self._submit_result(
                        cached_data['task_id'],
                        cached_data['result'],
                        cached_data['status'],
                        retry_from_cache=True
                    )
                    
                    if success:
                        cache_file.unlink()
                        
                except Exception as e:
                    print(f"   Error processing cache file {cache_file.name}: {e}")
    
    def _save_to_cache(self, task_id, result, status):
        cache_file = self.cache_dir / f"task_{task_id}_{int(time.time())}.cache"
        
        cached_data = {
            'task_id': task_id,
            'result': result,
            'status': status,
            'worker_id': self.worker_id,
            'timestamp': time.time()
        }
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(cached_data, f)
            print(f"   Result cached: {cache_file.name}")
        except Exception as e:
            print(f"   Failed to cache result: {e}")
    
    def _task_loop(self):
        """Main task processing loop"""
        while self.running:
            task = self._get_task()
            
            if task:
                self._process_task(task)
            else:
                print(f" No tasks available, waiting {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
    
    def _get_task(self):
        try:
            response = requests.post(
                f"{self.dispatcher_url}/get-task",
                json={"worker_id": self.worker_id},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('task') is None and 'task_id' not in data:
                    return None
                
                return data
            else:
                print(f" Failed to get task: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f" Failed to poll for task: {e}")
            return None
    
    def _process_task(self, task):
        """Execute task and send results back to dispatcher"""
        task_id = task['task_id']
        
        try:
            print(f"\n{'='*60}")
            print(f" TASK RECEIVED")
            print(f"   Task ID: {task_id}")
            print(f"   Type: {task.get('task_type', 'unknown')}")
            print(f"{'='*60}")
            
            if USE_FORK_EXECUTION:
                result = self._execute_task_forked(task)
            else:
                result = execute_task(task)
            
            print(f" Task {task_id} execution completed")
            
            print(f" Submitting result to dispatcher...")
            success = self._submit_result(task_id, result, 'completed')
            
            if success:
                print(f" Result successfully submitted for task {task_id}")
                print(f"{'='*60}\n")
            else:
                print(f"   Dispatcher unavailable - result cached for later retry")
                print(f"{'='*60}\n")
                
        except Exception as e:
            print(f" Task {task_id} failed with error: {e}")
            
            error_result = {
                "error": str(e),
                "status": "failed"
            }
            
            print(f" Submitting failure result...")
            self._submit_result(task_id, error_result, 'failed')
    
    def _execute_task_forked(self, task):
        """Execute task in isolated child process"""
        task_id = task['task_id']
        
        if CHECKPOINT_ENABLED and self.checkpoint_manager:
            checkpoints = self.checkpoint_manager.list_checkpoints()
            existing_checkpoint = next((c for c in checkpoints if c['task_id'] == task_id), None)
            
            if existing_checkpoint:
                print(f" Found existing checkpoint for task {task_id}")
                print(f" Checkpoint created by: {existing_checkpoint.get('worker_id', 'unknown')}")
                print(f" Attempting to restore from checkpoint...")
                
                checkpoint_dir = os.getenv('CHECKPOINT_DIR', 'checkpoints')
                result_file = os.path.join(checkpoint_dir, 'shared', f'task_{task_id}', 'result.json')
                
                if os.path.exists(result_file):
                    with open(result_file, 'r') as f:
                        partial_result = json.load(f)
                    
                    if partial_result.get('status') == 'completed':
                        print(f" Task already completed in checkpoint")
                        self.checkpoint_manager.delete_checkpoint(task_id)
                        return partial_result
                    else:
                        print(f" Found partial progress: {partial_result.get('last_checked', 0)} numbers checked")
                
                success, checkpoint_path = self.checkpoint_manager.restore_process(task_id)
                
                if success:
                    print(f" CRIU restore successful - process is running")
                    
                    # Get the restored PID from checkpoint metadata
                    metadata_file = os.path.join(checkpoint_path, "metadata.json")
                    restored_pid = None
                    if os.path.exists(metadata_file):
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                            restored_pid = metadata.get('pid')
                    
                    print(f" Waiting for restored process to complete...")
                    
                    timeout = 300
                    start_wait = time.time()
                    process_died = False
                    
                    while time.time() - start_wait < timeout:
                        # Check if process is still alive
                        if restored_pid:
                            try:
                                os.kill(restored_pid, 0)  # Check if process exists
                            except OSError as e:
                                print(f" [ERROR] Restored process {restored_pid} died unexpectedly")
                                print(f" [ERROR] OS Error: {e}")
                                
                                # Try to get process exit status
                                try:
                                    pid, status = os.waitpid(restored_pid, os.WNOHANG)
                                    if pid == restored_pid:
                                        if os.WIFEXITED(status):
                                            exit_code = os.WEXITSTATUS(status)
                                            print(f" [ERROR] Process exited with code: {exit_code}")
                                        elif os.WIFSIGNALED(status):
                                            signal_num = os.WTERMSIG(status)
                                            print(f" [ERROR] Process killed by signal: {signal_num}")
                                except:
                                    pass
                                
                                # Check if there's an error in result file
                                if os.path.exists(result_file):
                                    try:
                                        with open(result_file, 'r') as f:
                                            error_result = json.load(f)
                                        print(f" [ERROR] Task error from result file: {error_result}")
                                    except:
                                        pass
                                
                                process_died = True
                                break
                        
                        # Check for completed result
                        if os.path.exists(result_file):
                            with open(result_file, 'r') as f:
                                result = json.load(f)
                            
                            if result.get('status') == 'completed':
                                print(f" Restored task completed successfully")
                                self.checkpoint_manager.delete_checkpoint(task_id)
                                return result
                            elif result.get('status') == 'failed':
                                print(f" [ERROR] Restored task failed: {result.get('error')}")
                                self.checkpoint_manager.delete_checkpoint(task_id)
                                break
                        
                        time.sleep(2)
                    
                    if process_died:
                        print(f" Process died - attempting fresh execution")
                    else:
                        print(f" Timeout waiting for restored process")
                else:
                    print(f" CRIU restore failed")
                
                print(f" Restore failed or incomplete, starting fresh execution")
                self.checkpoint_manager.delete_checkpoint(task_id)
        
        task_json = json.dumps(task)
        executor_path = os.path.join(os.path.dirname(__file__), 'task_executor.py')
        
        proc = subprocess.Popen(
            [sys.executable, executor_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        child_pid = proc.pid
        print(f" Child process started: PID {child_pid}")
        
        if CHECKPOINT_ENABLED and self.checkpoint_manager:
            checkpoint_thread = threading.Thread(
                target=self._checkpoint_child_periodically,
                args=(child_pid, task_id),
                daemon=True
            )
            checkpoint_thread.start()
        
        checkpoint_dir = os.getenv('CHECKPOINT_DIR', 'checkpoints')
        result_file = os.path.join(checkpoint_dir, 'shared', f'task_{task_id}', 'result.json')
        
        stdout, stderr = proc.communicate(input=task_json, timeout=300)
        
        if CHECKPOINT_ENABLED and self.checkpoint_manager:
            self.checkpoint_manager.delete_checkpoint(task_id)
        
        if proc.returncode == 0:
            if os.path.exists(result_file):
                try:
                    with open(result_file, 'r') as f:
                        result = json.load(f)
                    return result
                except Exception as e:
                    print(f" Warning: Could not read result from disk: {e}")
            
            try:
                lines = stdout.strip().split('\n')
                json_line = lines[-1]
                result = json.loads(json_line)
                return result
            except (json.JSONDecodeError, IndexError) as e:
                return {
                    "status": "failed",
                    "error": f"Failed to parse child output: {e}, stdout: {stdout[:200]}"
                }
        else:
            return {
                "status": "failed",
                "error": f"Child process failed: {stderr}"
            }
    
    def _checkpoint_child_periodically(self, child_pid, task_id):
        """Periodically checkpoint the child process"""
        time.sleep(CHECKPOINT_INTERVAL)
        
        try:
            os.kill(child_pid, 0)
        except OSError:
            return
        
        print(f" Checkpointing child process {child_pid}...")
        success, checkpoint_path = self.checkpoint_manager.checkpoint_process(child_pid, task_id)
        
        if success:
            print(f" Checkpoint created: {checkpoint_path}")
        else:
            print(f" Checkpoint failed")
    
    def _submit_result(self, task_id, result, status='completed', retry_from_cache=False):
        try:
            payload = {
                "task_id": task_id,
                "worker_id": self.worker_id,
                "status": status,
                "primes": result.get('primes', []),
                "computation_time": result.get('computation_time'),
                "method": result.get('method')
            }
            
            response = requests.post(
                f"{self.dispatcher_url}/submit-result",
                json=payload,
                timeout=5
            )
            
            if response.status_code == 200:
                if retry_from_cache:
                    print(f"   Cached result for task {task_id} successfully sent")
                return True
            else:
                print(f"   ✗ HTTP {response.status_code} - Result rejected")
                print(f"   Response: {response.text}")
                
                if not retry_from_cache:
                    self._save_to_cache(task_id, result, status)
                
                return False
                
        except requests.exceptions.RequestException as e:
            if not retry_from_cache:
                print(f"   ✗ Dispatcher unreachable: {e}")
                print(f"    Caching result for later submission...")
                self._save_to_cache(task_id, result, status)
            else:
                print(f"   ✗ Still unreachable: {e}")
            
            return False
            
        except Exception as e:
            print(f"   ✗ Unexpected error during submission: {e}")
            
            if not retry_from_cache:
                self._save_to_cache(task_id, result, status)
            
            return False


def main():
    print("="*60)
    print(f"  Worker ID: {WORKER_ID}")
    print(f"  Dispatcher: {DISPATCHER_URL}")
    print(f"  Poll Interval: {POLL_INTERVAL}s")
    print(f"  Heartbeat Interval: {HEARTBEAT_INTERVAL}s")
    print("="*60 + "\n")
    
    worker = Worker(DISPATCHER_URL, WORKER_ID)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        worker.stop()
    
    print(" Worker stopped\n")


if __name__ == '__main__':
    main()
