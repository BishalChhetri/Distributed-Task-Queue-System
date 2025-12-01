import time
import os
from dotenv import load_dotenv

load_dotenv()

MAX_LIMIT = int(os.getenv('PRIMES_MAX_LIMIT', '1000000'))

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
    
    print(f"   Finding prime numbers up to {limit} using {method} method...")
    
    start_time = time.time()
    
    try:
        if method == 'trial_division':
            primes = find_primes_trial_division(limit)
        else:
            primes = find_primes_sieve(limit)
        
        elapsed = time.time() - start_time
        
        print(f"   Found {len(primes)} prime numbers")
        print(f"   Computation time: {elapsed:.4f}s")
        
        result = {
            "status": "completed",
            "primes": primes,
            "computation_time": round(elapsed, 4),
            "method": method
        }
        
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
