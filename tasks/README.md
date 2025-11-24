# Task Functions

This folder contains task execution functions. Each task type has its own Python file.

## Structure

```
tasks/
├── __init__.py
├── prime_task.py        # Prime number finding
├── compute_task.py      # General compute tasks
└── your_task.py         # Add your custom tasks here
```

## How to Add a New Task Type

### 1. Create a new file: `tasks/{task_type}_task.py`

For example, to create a "matrix" task type, create `tasks/matrix_task.py`

### 2. Implement the `execute()` function

Every task file must have an `execute(task_id, payload)` function:

```python
"""
Matrix multiplication task
"""
import time

def execute(task_id, payload):
    """
    Execute matrix multiplication task

    Args:
        task_id: Unique task identifier
        payload: Task parameters (e.g., matrix_size)

    Returns:
        dict: Task result with computation details
    """
    size = payload.get('size', 100)

    print(f"   Multiplying {size}x{size} matrices...")

    start_time = time.time()

    # Your computation here
    # result = your_computation(size)

    elapsed = time.time() - start_time

    return {
        "status": "success",
        "result": f"Multiplied {size}x{size} matrices",
        "computation_time": round(elapsed, 2)
    }
```

### 3. Submit tasks with the new task type

```bash
# In your task submission:
payload = {
    "task_type": "matrix",  # Must match filename: matrix_task.py
    "payload": json.dumps({"size": 100})
}
```

## Existing Task Types

### `compute` - General computation

- **File**: `compute_task.py`
- **Parameters**:
  - `limit` (int): Find primes up to this number (default: 100000)
  - `method` (str): 'sieve' or 'trial_division' (default: 'sieve')
- **Example**:
  ```python
  payload = {"task_type": "compute", "payload": {"limit": 100000, "method": "sieve"}}
  ```

### `prime` - Prime number finding

- **File**: `prime_task.py`
- **Parameters**: Same as compute
- **Example**:
  ```python
  payload = {"task_type": "prime", "payload": {"limit": 50000, "method": "trial_division"}}
  ```

## Worker Behavior

The worker automatically:

1. Receives task with `task_type` field
2. Imports `tasks.{task_type}_task` module
3. Calls `module.execute(task_id, payload)`
4. Returns result to dispatcher

If task type not found, worker returns error:

```json
{
  "status": "failed",
  "error": "Task type 'xyz' not implemented"
}
```

## Best Practices

1. **Error Handling**: Wrap your computation in try-except
2. **Logging**: Use print statements for progress updates
3. **Timing**: Track computation time with `time.time()`
4. **Return Format**: Always return a dict with at least:
   - `"status"`: "success" or "failed"
   - `"result"`: Description of what was done
5. **CPU-Intensive**: Ensure real computation, not just `time.sleep()`

## Example: Adding a Sorting Task

**File**: `tasks/sort_task.py`

```python
import time
import random

def execute(task_id, payload):
    size = payload.get('size', 1000000)

    print(f"   Sorting {size} random numbers...")

    start_time = time.time()

    # Generate random data
    data = [random.randint(1, 1000000) for _ in range(size)]

    # Sort it
    sorted_data = sorted(data)

    elapsed = time.time() - start_time

    return {
        "status": "success",
        "result": f"Sorted {size} numbers",
        "size": size,
        "computation_time": round(elapsed, 2)
    }
```

**Submit**:

```bash
python3 add_task.py  # Then modify to use task_type="sort"
```
