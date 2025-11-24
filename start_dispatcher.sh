#!/bin/bash
cd "$(dirname "$0")/dispatcher"

if ! python3 -c "import flask" 2>/dev/null; then
    echo "  Error: Flask not installed"
    echo "Please run: pip install -r ../requirements.txt"
    exit 1
fi

echo "Starting dispatcher on http://0.0.0.0:5000"
echo "Press Ctrl+C to stop"
echo ""

python3 app.py
