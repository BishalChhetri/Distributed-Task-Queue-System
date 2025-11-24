import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self, env_file='.env'):
        self._config = {}
        self._load_env_file(env_file)
    
    def _load_env_file(self, env_file):
        # jst try to find .env file in project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env_path = os.path.join(project_root, env_file)
        
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove inline comments
                        if '#' in value:
                            value = value.split('#')[0].strip()
                        
                        self._config[key] = value
    
    def get(self, key, default=None, value_type=str):
        # Check environment variable first (highest priority)
        env_value = os.getenv(key)
        if env_value is not None:
            value = env_value
        # Then check .env file
        elif key in self._config:
            value = self._config[key]
        # Finally use default
        else:
            return default
        
        # Convert to requested type
        try:
            if value_type == bool:
                return value.lower() in ('true', '1', 'yes', 'on')
            elif value_type == int:
                return int(value)
            elif value_type == float:
                return float(value)
            else:
                return str(value)
        except (ValueError, AttributeError):
            return default
    
    def get_int(self, key, default=0):
        return self.get(key, default, int)
    
    def get_float(self, key, default=0.0):
        return self.get(key, default, float)
    
    def get_bool(self, key, default=False):
        return self.get(key, default, bool)
    
    def get_str(self, key, default=''):
        return self.get(key, default, str)



config = Config()
# for the server settings
DISPATCHER_HOST = config.get_str('DISPATCHER_HOST', '0.0.0.0')
DISPATCHER_PORT = config.get_int('DISPATCHER_PORT', 5000)

# for the database settings
DB_PATH = config.get_str('DB_PATH', 'dispatcher/queue_db.db')
DB_TIMEOUT = config.get_int('DB_TIMEOUT', 5)

# for task settings
DEFAULT_MAX_ATTEMPTS = config.get_int('DEFAULT_MAX_ATTEMPTS', 5)
DEFAULT_LEASE_DURATION = config.get_int('DEFAULT_LEASE_DURATION', 120)
DEFAULT_HEARTBEAT_TIMEOUT = config.get_int('DEFAULT_HEARTBEAT_TIMEOUT', 60)

# for heartbeat monitor settings
HEARTBEAT_MONITOR_INTERVAL = config.get_int('HEARTBEAT_MONITOR_INTERVAL', 15)

# for connection settings
WORKER_DISPATCHER_URL = config.get_str('WORKER_DISPATCHER_URL', 'http://localhost:5000')
WORKER_ID = config.get_str('WORKER_ID', 'worker_auto')

# for the polling settings
WORKER_POLL_INTERVAL = config.get_int('WORKER_POLL_INTERVAL', 2)
WORKER_HEARTBEAT_INTERVAL = config.get_int('WORKER_HEARTBEAT_INTERVAL', 30)

# for cache directory
CACHE_DIR = config.get_str('CACHE_DIR', 'worker/cache')

# for cache expiry time
CACHE_TTL_SECONDS = config.get_int('CACHE_TTL_SECONDS', 3600)

# for saved cache retry settings
CACHE_RETRY_INTERVAL = config.get_int('CACHE_RETRY_INTERVAL', 10)

LOG_LEVEL = config.get_str('LOG_LEVEL', 'INFO')
ENABLE_DEBUG_LOGGING = config.get_bool('ENABLE_DEBUG_LOGGING', False)

PRIMES_DEFAULT_METHOD = config.get_str('PRIMES_DEFAULT_METHOD', 'sieve')
PRIMES_MAX_LIMIT = config.get_int('PRIMES_MAX_LIMIT', 1000000)

# Print loaded configuration (for debugging)
if ENABLE_DEBUG_LOGGING:
    print("Loaded configuration:")
    print(f"  Dispatcher: {DISPATCHER_HOST}:{DISPATCHER_PORT}")
    print(f"  Database: {DB_PATH}")
    print(f"  Max Attempts: {DEFAULT_MAX_ATTEMPTS}")
    print(f"  Lease Duration: {DEFAULT_LEASE_DURATION}s")
    print(f"  Cache TTL: {CACHE_TTL_SECONDS}s")
    print(f"  Worker Poll Interval: {WORKER_POLL_INTERVAL}s")
    print(f"  Worker Heartbeat Interval: {WORKER_HEARTBEAT_INTERVAL}s")
