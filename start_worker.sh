#!/bin/bash

cd "$(dirname "$0")/worker"

if ! python3 -c "import requests" 2>/dev/null; then
    echo "Error: requests library not installed"
    echo "Please run: pip install -r ../requirements.txt"
    exit 1
fi

if [ -f "../.env" ]; then
    set -a
    source <(grep -v '^#' ../.env | grep -v '^$' | sed 's/#.*//' | sed 's/[[:space:]]*$//')
    set +a
fi

if [ -n "$1" ]; then
    export WORKER_ID="$1"
elif [ "$WORKER_ID" == "worker_auto" ] || [ -z "$WORKER_ID" ]; then
    export WORKER_ID="worker_$$"
fi

export DISPATCHER_URL=${DISPATCHER_URL:-${WORKER_DISPATCHER_URL:-"http://localhost:5000"}}
export WORKER_POLL_INTERVAL=${WORKER_POLL_INTERVAL:-"5"}
export WORKER_HEARTBEAT_INTERVAL=${WORKER_HEARTBEAT_INTERVAL:-"30"}
export CACHE_TTL=${CACHE_TTL:-${CACHE_TTL_SECONDS:-"3600"}}

echo "   Configuration:"
echo "   Dispatcher URL: $DISPATCHER_URL"
echo "   Worker ID: $WORKER_ID"
echo "   Poll Interval: ${WORKER_POLL_INTERVAL}s"
echo "   Heartbeat Interval: ${WORKER_HEARTBEAT_INTERVAL}s"
echo "   Cache TTL: ${CACHE_TTL}s"
echo ""
echo "   Starting worker..."
echo "   Press Ctrl+C to stop"
echo ""

python3 worker.py
