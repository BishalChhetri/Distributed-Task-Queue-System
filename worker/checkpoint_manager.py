import os
import subprocess
import json
from datetime import datetime
import traceback
import time
import shutil
from datetime import timedelta

class CheckpointManager:
    def __init__(self, checkpoint_dir, worker_id):
        self.checkpoint_dir = checkpoint_dir
        self.worker_id = worker_id
        self.shared_checkpoint_dir = os.path.join(checkpoint_dir, "shared")
        os.makedirs(self.shared_checkpoint_dir, exist_ok=True)
    
    def checkpoint_process(self, pid, task_id):
        try:
            task_checkpoint_dir = os.path.join(self.shared_checkpoint_dir, f"task_{task_id}")
            
            # Ensure directory exists with proper permissions
            os.makedirs(task_checkpoint_dir, exist_ok=True)
            os.chmod(task_checkpoint_dir, 0o777)  # Make sure CRIU (running as sudo) can write
            
            metadata = {
                "task_id": task_id,
                "pid": pid,
                "worker_id": self.worker_id,
                "checkpoint_time": datetime.now().isoformat(),
                "checkpoint_dir": task_checkpoint_dir
            }
            
            metadata_file = os.path.join(task_checkpoint_dir, "metadata.json")
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Create log file path - use absolute path
            log_file_path = os.path.abspath(os.path.join(task_checkpoint_dir, 'dump.log'))
            images_dir = os.path.abspath(task_checkpoint_dir)
            
            # Use --leave-running so task continues after checkpoint
            cmd = [
                'sudo', 'criu', 'dump',
                '-t', str(pid),
                '--images-dir', images_dir,
                '--shell-job',
                '--leave-running',
                '-v4',  # Verbose logging for debugging
                '--log-file', log_file_path
            ]
            
            # Use Popen to avoid blocking
            result = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for checkpoint to complete (this is quick, usually < 1 second)
            stdout, stderr = result.communicate(timeout=10)
            
            if result.returncode == 0:
                return True, task_checkpoint_dir
            else:
                print(f"[CHECKPOINT ERROR] Failed: {stderr[:500]}")
                # Also check if log file was created
                if os.path.exists(log_file_path):
                    try:
                        os.chmod(log_file_path, 0o666)
                        with open(log_file_path, 'r') as f:
                            log_content = f.read()
                            print(f"[CHECKPOINT ERROR] Log file content:\n{log_content[-1000:]}")
                    except:
                        pass
                return False, None
                
        except subprocess.TimeoutExpired:
            print(f"[CHECKPOINT ERROR] Timeout after 10 seconds")
            result.kill()
            return False, None
        except Exception as e:
            print(f"[CHECKPOINT ERROR] Exception: {e}")
            traceback.print_exc()
            return False, None
    
    def restore_process(self, task_id):
        try:
            task_checkpoint_dir = os.path.join(self.shared_checkpoint_dir, f"task_{task_id}")
            
            if not os.path.exists(task_checkpoint_dir):
                print(f"[RESTORE] No checkpoint found for task {task_id}")
                return False, None
            
            metadata_file = os.path.join(task_checkpoint_dir, "metadata.json")
            original_worker_id = None
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    print(f"[RESTORE] Restoring task {task_id} from {metadata['checkpoint_time']}")
                    original_worker_id = metadata.get('worker_id')
            
            # Use absolute path
            images_dir = os.path.abspath(task_checkpoint_dir)
            log_file_path = os.path.abspath(os.path.join(task_checkpoint_dir, 'restore.log'))
            
            # SIMPLIFIED: Remove unshare for now - accept PID conflicts as rare edge case
            # The broken pipes issue is the real blocker, not PID conflicts
            cmd = [
                'sudo', 'criu', 'restore',
                '--images-dir', images_dir,
                '--shell-job',
                '-d',
                '--inherit-fd', 'fd[1]:/tmp/restore_stdout.log', # Redirect stdout
                '--inherit-fd', 'fd[2]:/tmp/restore_stderr.log', # Redirect stderr
                '-v4'
            ]
            
            print(f"[RESTORE] Attempting restore (original worker: {original_worker_id}, current: {self.worker_id})")
            
            # Non-blocking: Launch restore and return immediately
            result = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            time.sleep(2)
            
            poll_result = result.poll()
            
            if poll_result is None or poll_result == 0:
                print(f"[RESTORE] CRIU command completed (exit code: {poll_result})")
                return True, task_checkpoint_dir
            else:
                stdout, stderr = result.communicate()
                print(f"[RESTORE ERROR] Failed with exit code {poll_result}")
                print(f"[RESTORE ERROR] stderr: {stderr[:500]}")
                
                if os.path.exists(log_file_path):
                    try:
                        os.chmod(log_file_path, 0o666)
                        with open(log_file_path, 'r') as f:
                            log_content = f.read()
                            print(f"[RESTORE ERROR] Last 1000 chars of restore.log:\n{log_content[-1000:]}")
                    except:
                        pass
                
                return False, None
                
        except Exception as e:
            print(f"[RESTORE ERROR] Exception: {e}")
            traceback.print_exc()
            return False, None
    
    def list_checkpoints(self):
        checkpoints = []
        
        if not os.path.exists(self.shared_checkpoint_dir):
            return checkpoints
        
        for item in os.listdir(self.shared_checkpoint_dir):
            if item.startswith('task_'):
                task_checkpoint_dir = os.path.join(self.shared_checkpoint_dir, item)
                metadata_file = os.path.join(task_checkpoint_dir, "metadata.json")
                
                if os.path.exists(metadata_file):
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                        checkpoints.append(metadata)
        
        return checkpoints
    
    def delete_checkpoint(self, task_id):
        try:
            task_checkpoint_dir = os.path.join(self.shared_checkpoint_dir, f"task_{task_id}")
            
            if os.path.exists(task_checkpoint_dir):
                shutil.rmtree(task_checkpoint_dir)
                return True
            
            return False
            
        except Exception as e:
            print(f"[CHECKPOINT ERROR] Failed to delete: {e}")
            return False
    
    def cleanup_old_checkpoints(self, max_age_hours=24):
        try:
            now = datetime.now()
            max_age = timedelta(hours=max_age_hours)
            
            for checkpoint in self.list_checkpoints():
                checkpoint_time = datetime.fromisoformat(checkpoint['checkpoint_time'])
                age = now - checkpoint_time
                
                if age > max_age:
                    self.delete_checkpoint(checkpoint['task_id'])
                    print(f"[CHECKPOINT] Cleaned up old checkpoint for task {checkpoint['task_id']}")
            
            return True
            
        except Exception as e:
            print(f"[CHECKPOINT ERROR] Cleanup failed: {e}")
            return False
