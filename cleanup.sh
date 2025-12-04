#!/bin/bash
echo "This will:"
echo "  1. Kill all worker processes"
echo "  2. Kill dispatcher process"
echo "  3. Kill any analysis scripts"
echo "  4. Reset ALL database tables"

cd "$(dirname "$0")"

# Kill all worker processes
echo " Killing all worker processes..."
pkill -9 -f "worker.py"
pkill -9 -f "throughput_worker"
sleep 1
echo "   ✓ Workers killed"

# Kill dispatcher
echo " Killing dispatcher..."
pkill -9 -f "dispatcher/app.py"
pkill -9 -f "python3 app.py"
sleep 1
echo "   ✓ Dispatcher killed"

# Kill any analysis scripts
echo " Killing analysis scripts..."
pkill -9 -f "task_throughput_analysis"
sleep 1
echo "   ✓ Analysis scripts killed"

# Reset database
echo "  Resetting database..."
DB_PATH="dispatcher/queue_db.db"

if [ -f "$DB_PATH" ]; then
    sqlite3 "$DB_PATH" 2>/dev/null <<EOF
-- Delete all data from main tables
DELETE FROM task_results;
DELETE FROM tasks;
DELETE FROM checkpoints;
DELETE FROM workers;

-- Delete from optional tables if they exist (may not exist in all schemas)
DELETE FROM worker_pool WHERE 1=1;
DELETE FROM heartbeats WHERE 1=1;

-- Reset autoincrement counters
DELETE FROM sqlite_sequence WHERE name='tasks';
DELETE FROM sqlite_sequence WHERE name='task_results';
DELETE FROM sqlite_sequence WHERE name='checkpoints';
DELETE FROM sqlite_sequence WHERE name='workers';

-- Verify cleanup
.mode line
SELECT COUNT(*) as 'Tasks remaining' FROM tasks;
SELECT COUNT(*) as 'Task results remaining' FROM task_results;
SELECT COUNT(*) as 'Checkpoints remaining' FROM checkpoints;
SELECT COUNT(*) as 'Workers remaining' FROM workers;
EOF
    echo "   ✓ Database reset complete"
else
    echo "    Database not found at $DB_PATH"
fi

# Show any remaining Python processes (for verification)
echo ""
echo " Checking for remaining Python processes..."
REMAINING=$(ps aux | grep -E "python.*worker|python.*app.py|python.*analysis" | grep -v grep)

if [ -z "$REMAINING" ]; then
    echo "   ✓ No remaining processes found"
else
    echo "   Some processes still running:"
    echo "$REMAINING"
fi

echo ""
echo " ✓ CLEANUP COMPLETE"
