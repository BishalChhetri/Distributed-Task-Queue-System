from flask import request, jsonify
import json
from db import (
    insert_task, 
    claim_task, 
    update_heartbeat, 
    save_result,
    get_task_status, 
    get_pending_tasks_count, 
    get_active_workers
)
from task_distribution import get_worker_pool


def register_routes(app):
    @app.route('/submit-task', methods=['POST'])
    def submit_task():
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({"error": "No data provided"}), 400
            
            if 'task_type' not in data:
                return jsonify({"error": "task_type is required"}), 400
            
            task_type = data['task_type']
            task_data = data.get('task_data', {})
            
            payload_json = json.dumps(task_data)
            
            result = insert_task(task_type, payload_json)
            
            return jsonify({
                "status": "success",
                "message": "Task submitted successfully",
                "data": result
            }), 201
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/get-task', methods=['POST'])
    def get_task():
        try:
            data = request.get_json()
            
            if not data or 'worker_id' not in data:
                return jsonify({"error": "worker_id is required"}), 400
            
            worker_id = data['worker_id']
            
            task = claim_task(worker_id)
            
            if task:
                return jsonify({
                    "task_id": task['task_id'],
                    "task_type": task['task_type'],
                    "payload": task['payload']
                }), 200
            else:
                return jsonify({
                    "task": None
                }), 200
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/heartbeat', methods=['POST'])
    def heartbeat():
        try:
            data = request.get_json()
            
            if not data or 'worker_id' not in data:
                return jsonify({"error": "worker_id is required"}), 400
            
            worker_id = data['worker_id']
            status = data.get('status', 'alive')
            metadata = data.get('metadata')
            
            metadata_json = json.dumps(metadata) if metadata else None
            
            result = update_heartbeat(worker_id, status, metadata_json)
            
            pool = get_worker_pool()
            pool.register_worker(worker_id, status, metadata)
            
            return jsonify({
                "status": "success",
                "message": "Heartbeat received",
                "data": result
            }), 200
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/submit-result', methods=['POST'])
    def submit_result():
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({"error": "No data provided"}), 400
            
            if 'task_id' not in data:
                return jsonify({"error": "task_id is required"}), 400
            
            task_id = data['task_id']
            worker_id = data.get('worker_id')
            primes = data.get('primes', [])
            computation_time = data.get('computation_time')
            method = data.get('method')
            status = data.get('status', 'completed')
            was_resumed = data.get('was_resumed', False)
            checkpoint_time = data.get('checkpoint_time')
            resume_time = data.get('resume_time')
            
            print(f"\n{'='*60}")
            print(f" RESULT RECEIVED - Task ID: {task_id}")
            print(f"   Worker: {worker_id}")
            print(f"   Status: {status}")
            print(f"   Primes count: {len(primes) if primes else 0}")
            print(f"   Computation time: {computation_time}s")
            if was_resumed:
                print(f"   Resumed from checkpoint (checkpoint: {checkpoint_time}s, resume: {resume_time}s)")
            print(f"   Method: {method}")
            print(f"{'='*60}\n")
    
            if status not in ['completed', 'failed']:
                return jsonify({"error": "status must be 'completed' or 'failed'"}), 400
            
            result = save_result(task_id, primes, computation_time, method, status, worker_id,
                               was_resumed, checkpoint_time, resume_time)
            
            print(f" DATABASE UPDATED - Task {task_id} marked as {status}")
            print(f"   Full result: {result}\n")
            
            return jsonify({
                "status": "success",
                "message": "Result saved successfully",
                "data": result
            }), 200
            
        except Exception as e:
            print(f" ERROR in submit_result: {str(e)}\n")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/health', methods=['GET'])
    def health_check():
        return jsonify({
            "status": "healthy",
            "service": "dispatcher"
        }), 200


    @app.route('/stats', methods=['GET'])
    def get_stats():
        try:
            pending_count = get_pending_tasks_count()
            active_workers = get_active_workers()
            
            return jsonify({
                "status": "success",
                "data": {
                    "pending_tasks": pending_count,
                    "active_workers": len(active_workers),
                    "workers": active_workers
                }
            }), 200
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/task/<int:task_id>', methods=['GET'])
    def get_task_info(task_id):
        try:
            task = get_task_status(task_id)
            
            if not task:
                return jsonify({
                    "status": "error",
                    "message": "Task not found"
                }), 404
            
            return jsonify({
                "status": "success",
                "data": task
            }), 200
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


    @app.route('/worker-pool/stats', methods=['GET'])
    def get_worker_pool_stats():
        try:
            pool = get_worker_pool()
            pool_stats = pool.get_pool_stats()
            
            return jsonify({
                "status": "success",
                "data": pool_stats
            }), 200
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

