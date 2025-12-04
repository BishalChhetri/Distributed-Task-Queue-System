import time
import os
import sys
import threading
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dispatcher_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dispatcher')
sys.path.insert(0, dispatcher_path)

from dispatcher.db import save_checkpoint, load_checkpoint, delete_checkpoint

# Thread-safe checkpoint saving
def save_checkpoint_async(task_id, last_checked, primes, elapsed_time, method):
    """
    We just save checkpoint in background thread to avoid blocking computation and creates a copy of the primes list to prevent race conditions.
    """
    primes_copy = primes.copy()
    thread = threading.Thread(
        target=save_checkpoint,
        args=(task_id, last_checked, primes_copy, elapsed_time, method),
        daemon=True
    )
    thread.start()
    return thread

MAX_LIMIT = int(os.getenv('PRIMES_MAX_LIMIT', '1000000'))
CHECKPOINT_INTERVAL = int(os.getenv('CHECKPOINT_INTERVAL', '100000'))

# I use two methods to find the prime the first one is sieve and second is trail division

# find prime using sieve
def find_primes_sieve(limit):
    if limit < 2:
        return []
    
    sieve = [True] * (limit + 1)
    sieve[0] = sieve[1] = False
    
    p = 2
    while p * p <= limit:
        if sieve[p]:
            for i in range(p * p, limit + 1, p):
                sieve[i] = False
        p += 1
    
    primes = [num for num in range(2, limit + 1) if sieve[num]]
    return primes


# find prime using trial division
def find_primes_trial_division(limit):
    if limit < 2:
        return []
    
    primes = []
    for num in range(2, limit + 1):
        is_prime = True
        for i in range(2, int(num ** 0.5) + 1):
            if num % i == 0:
                is_prime = False
                break
        if is_prime:
            primes.append(num)
    
    return primes

# this will execute the task
def execute(task_id, payload):
    limit = payload.get('limit', 100000)
    method = payload.get('method', 'sieve')
    
    if limit > MAX_LIMIT:
        original_limit = limit
        limit = MAX_LIMIT
    else:
        original_limit = None
    
    checkpoint = load_checkpoint(task_id)
    was_resumed = False
    checkpoint_time = None
    resume_start_time = None
    
    if checkpoint:
        print(f"\n  CHECKPOINT FOUND!")
        print(f"  Last checked: {checkpoint['last_checked']:,} numbers")
        print(f"  Primes found: {len(checkpoint['primes']):,}")
        print(f"  Time elapsed: {checkpoint['elapsed_time']:.2f}s")
        print(f"  Resuming computation...\n")
        primes = checkpoint['primes']
        start_num = checkpoint['last_checked'] + 1
        start_time = time.time()
        checkpoint_time = checkpoint['elapsed_time']
        resume_start_time = start_time
        was_resumed = True
    else:
        print(f"   Finding prime numbers up to {limit} using {method} method...")
        primes = []
        start_num = 2
        start_time = time.time()
    
    try:
        if method == 'trial_division':
            # Track checkpoint threads to ensure final checkpoint completes
            checkpoint_threads = []
            
            for num in range(start_num, limit + 1):
                is_prime = True
                for i in range(2, int(num ** 0.5) + 1):
                    if num % i == 0:
                        is_prime = False
                        break
                if is_prime:
                    primes.append(num)
                
                if num % CHECKPOINT_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    if was_resumed:
                        elapsed += checkpoint_time
                    # Save checkpoint asynchronously (non-blocking)
                    thread = save_checkpoint_async(task_id, num, primes, elapsed, method)
                    checkpoint_threads.append(thread)
                    progress_pct = (num / limit) * 100
                    print(f"  Checkpoint saved (async): {num:,}/{limit:,} ({progress_pct:.1f}%) - {len(primes):,} primes - {elapsed:.2f}s")
            
            # Wait for any pending checkpoint saves before finishing
            for thread in checkpoint_threads:
                thread.join(timeout=2.0)
        else:
            if not was_resumed:
                primes = find_primes_sieve(limit)
            else:
                print(f"   Warning: Sieve method doesn't support resume, recomputing from scratch")
                primes = find_primes_sieve(limit)
        
        total_elapsed = time.time() - start_time
        if was_resumed:
            total_elapsed += checkpoint_time
        
        delete_checkpoint(task_id)
        print(f"  Checkpoint cleaned up")
        
        print(f"\n   Found {len(primes):,} prime numbers")
        print(f"   Computation time: {total_elapsed:.4f}s")
        
        result = {
            "status": "completed",
            "primes": primes,
            "computation_time": round(total_elapsed, 4),
            "was_resumed": was_resumed,
            "method": method
        }
        
        if was_resumed:
            resume_time = time.time() - resume_start_time
            result["checkpoint_time"] = round(checkpoint_time, 4)
            result["resume_time"] = round(resume_time, 4)
            print(f"\n  RESUME SUMMARY:")
            print(f"  Before checkpoint: {checkpoint_time:.2f}s")
            print(f"  After resume: {resume_time:.2f}s")
            print(f"  Total time: {total_elapsed:.2f}s\n")
        
        if original_limit:
            result["warning"] = f"Requested limit {original_limit} was capped to {MAX_LIMIT}"
            result["requested_limit"] = original_limit
        
        return result
        
    except MemoryError:
        return {
            "status": "failed",
            "primes": [],
            "computation_time": time.time() - start_time,
            "method": method,
            "error": f"Memory allocation failed for limit {limit}"
        }
    except Exception as e:
        return {
            "status": "failed",
            "primes": [],
            "computation_time": time.time() - start_time,
            "method": method,
            "error": str(e)
        }
