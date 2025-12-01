#!/usr/bin/env python3
import sys
import os
import json
import importlib

def execute_task_isolated(task):
    task_type = task.get('task_type')
    task_id = task['task_id']
    payload = json.loads(task.get('payload', '{}'))
    
    try:
        task_module = importlib.import_module(f'tasks.{task_type}_task')
        
        if hasattr(task_module, 'execute'):
            result = task_module.execute(task_id, payload)
            
            return result
        else:
            return {
                "status": "failed",
                "error": f"Task module '{task_type}' does not have execute() function"
            }
    except ModuleNotFoundError:
        return {
            "status": "failed",
            "error": f"Task type '{task_type}' not found"
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Task execution error: {str(e)}"
        }

if __name__ == '__main__':
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    task_json = sys.stdin.read()
    task = json.loads(task_json)
    
    result = execute_task_isolated(task)
    
    print(json.dumps(result))
    sys.stdout.flush()

