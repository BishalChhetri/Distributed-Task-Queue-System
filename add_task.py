#!/usr/bin/env python3
import sys
import time
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

DISPATCHER_URL = "http://localhost:5000"
PRIMES_MAX_LIMIT = int(os.getenv('PRIMES_MAX_LIMIT', '1000000'))

USAGE = """ To use this script: you need to use the following commands: (sieve is the default one)
  add_task.py 5
  add_task.py 3 200000
  add_task.py 2 100000 trial_division
"""
def main():
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)

    try:
        count = int(sys.argv[1])
    except ValueError:
        print("count must be an integer")
        sys.exit(1)

    limit = 100000
    if len(sys.argv) >= 3:
        try:
            limit = int(sys.argv[2])
        except ValueError:
            print("limit must be an integer, using default 100000")

    method = 'sieve'
    if len(sys.argv) >= 4:
        method = sys.argv[3]
        if method not in ['sieve', 'trial_division']:
            print(f"Unknown method '{method}', using 'sieve'")
            method = 'sieve'

    if limit > PRIMES_MAX_LIMIT:
        print(f"\n⚠️  WARNING: Requested limit {limit} exceeds PRIMES_MAX_LIMIT ({PRIMES_MAX_LIMIT})")
        print(f"⚠️  Due to memory constraints, tasks will be capped to {PRIMES_MAX_LIMIT}")
        print(f"⚠️  To increase this limit, update PRIMES_MAX_LIMIT in .env file")
        print(f"\nDo you want to continue? (y/n): ", end='')
        
        response = input().strip().lower()
        if response != 'y':
            print("Task submission cancelled.")
            sys.exit(0)
        
        print(f"\nNote: Workers will process up to {PRIMES_MAX_LIMIT} primes, not {limit}\n")

    print(f"Submitting {count} prime-finding tasks to {DISPATCHER_URL}")
    print(f"  Limit: {limit}, Method: {method}")
    print()

    for i in range(count):
        payload = {
            "task_type": "compute",
            "task_data": {"limit": limit, "method": method}
        }

        try:
            r = requests.post(f"{DISPATCHER_URL}/submit-task", json=payload, timeout=5)
            if r.status_code in (200, 201):
                try:
                    data = r.json()
                    task_id = data.get('task_id') or data.get('id') or data.get('task')
                except Exception:
                    task_id = None

                if task_id:
                    print(f"  ✓ Submitted task {i+1}: HTTP {r.status_code}, id={task_id}")
                else:
                    print(f"  ✓ Submitted task {i+1}: HTTP {r.status_code}")
            else:
                print(f"  ✗ Failed to submit task {i+1}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  ✗ Error submitting task {i+1}: {e}")
        time.sleep(0.05)


if __name__ == '__main__':
    main()
