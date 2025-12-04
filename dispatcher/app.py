import sys
import os
import threading
import time
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config import DISPATCHER_HOST, DISPATCHER_PORT, HEARTBEAT_MONITOR_INTERVAL
from db import init_db, mark_dead_workers, reclaim_expired_tasks, reclaim_tasks_from_dead_workers
from routes import register_routes

app = Flask(__name__)

print("Initializing the database...")
init_db()

print("Registering routes...")
register_routes(app)

print("Application ready, Let's go!")


def background_monitor():
    print(f"[MONITOR] Starting background monitor (interval: {HEARTBEAT_MONITOR_INTERVAL}s)")
    
    while True:
        try:
            time.sleep(HEARTBEAT_MONITOR_INTERVAL)
            
            # Mark workers as dead if they haven't sent heartbeat
            mark_dead_workers()
            
            # Reclaim tasks from dead workers
            reclaim_tasks_from_dead_workers()
            
            # Reclaim tasks from expired leases
            reclaim_expired_tasks()
            
        except Exception as e:
            print(f"[MONITOR ERROR] {e}")


if __name__ == '__main__':
    # Start background monitoring thread
    monitor_thread = threading.Thread(target=background_monitor, daemon=True)
    monitor_thread.start()
    
    print(f"[DISPATCHER] Starting server on http://{DISPATCHER_HOST}:{DISPATCHER_PORT}")
    app.run(host=DISPATCHER_HOST, port=DISPATCHER_PORT, debug=False)

