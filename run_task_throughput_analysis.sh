#!/bin/bash

echo "Task Throughput Analysis"
echo "Tests ONE task size with multiple worker counts"
echo "To change task size:"
echo "  Edit TASK_SIZE in analysis/task_throughput_analysis.py"
echo "Results: analysis/results/results_<TASK_SIZE>.json"

cd "$(dirname "$0")"

python3 analysis/task_throughput_analysis.py

echo ""
echo "Analysis complete!"
