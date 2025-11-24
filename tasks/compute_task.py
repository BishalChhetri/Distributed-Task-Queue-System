from . import prime_task


def execute(task_id, payload):
    computation_type = payload.get('type', 'prime')
    
    if computation_type == 'prime':
        return prime_task.execute(task_id, payload)
    else:
        return {
            "status": "success",
            "result": f"Processed compute task {task_id}",
            "computation_type": computation_type
        }
