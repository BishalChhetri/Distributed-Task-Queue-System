#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / 'results'
PLOTS_DIR = Path(__file__).parent / 'recovery_plots'
PLOTS_DIR.mkdir(exist_ok=True)

def load_recovery_results():
    results = []
    baseline_time_actual = None
    
    for file in sorted(RESULTS_DIR.glob('recovery_results_*.json')):
        with open(file, 'r') as f:
            data = json.load(f)
            if data['num_dead_workers'] == 0:
                baseline_time_actual = data['total_time']
            else:
                results.append(data)
    
    if baseline_time_actual:
        for r in results:
            r['baseline_time'] = baseline_time_actual
            r['recovery_overhead'] = r['total_time'] - baseline_time_actual
            r['overhead_percentage'] = (r['recovery_overhead'] / baseline_time_actual * 100) if baseline_time_actual > 0 else 0
    
    return sorted(results, key=lambda x: x['num_dead_workers']), baseline_time_actual

def plot_recovery_analysis(results, baseline_time_actual):
    if not results:
        print("No recovery results found")
        return
    
    num_workers = results[0]['num_workers']
    dead_workers = [r['num_dead_workers'] for r in results]
    failure_percentages = [(dw / num_workers) * 100 for dw in dead_workers]
    total_times = [r['total_time'] for r in results]
    overheads = [r['recovery_overhead'] for r in results]
    overhead_pcts = [r['overhead_percentage'] for r in results]
    
    x_labels = [f"{dw}\n({fp:.1f}%)" for dw, fp in zip(dead_workers, failure_percentages)]
    
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    ax1.axhline(y=baseline_time_actual, color='#2E86AB', linestyle='--', linewidth=2, 
                label=f'Baseline (No Failures): {baseline_time_actual:.2f}s', alpha=0.7)
    ax1.plot(dead_workers, total_times, 'o-', linewidth=2, markersize=10, 
             color='#C73E1D', label='With Failures')
    ax1.set_xlabel('Dead Workers (Failure %)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
    ax1.set_title(f'Execution Time vs Worker Failures\n({num_workers} Workers Total)', 
                  fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(fontsize=11, loc='best')
    ax1.set_xticks(dead_workers)
    ax1.set_xticklabels(x_labels)
    for dw, tt in zip(dead_workers, total_times):
        ax1.text(dw, tt, f'{tt:.1f}s', ha='center', va='bottom', fontsize=9, fontweight='bold')
    plt.tight_layout()
    output_file1 = PLOTS_DIR / 'recovery_execution_time.png'
    plt.savefig(output_file1, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file1.name}")
    plt.close()
    
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.plot(dead_workers, overheads, 'o-', linewidth=2, markersize=10, color='#F18F01')
    ax2.set_xlabel('Dead Workers (Failure %)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Recovery Overhead (seconds)', fontsize=12)
    ax2.set_title(f'Recovery Overhead vs Worker Failures\n({num_workers} Workers Total)', 
                  fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(dead_workers)
    ax2.set_xticklabels(x_labels)
    for dw, oh in zip(dead_workers, overheads):
        ax2.text(dw, oh, f'{oh:.2f}s', ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.tight_layout()
    output_file2 = PLOTS_DIR / 'recovery_overhead.png'
    plt.savefig(output_file2, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file2.name}")
    plt.close()
    
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    colors = plt.cm.RdYlGn_r(np.array(overhead_pcts) / max(overhead_pcts) if max(overhead_pcts) > 0 else [0])
    bars = ax3.bar(dead_workers, overhead_pcts, color=colors, edgecolor='black', linewidth=1.2)
    ax3.set_xlabel('Dead Workers (Failure %)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Overhead Percentage (%)', fontsize=12)
    ax3.set_title(f'Recovery Overhead Percentage vs Worker Failures\n({num_workers} Workers Total)', 
                  fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.set_xticks(dead_workers)
    ax3.set_xticklabels(x_labels)
    for bar, pct in zip(bars, overhead_pcts):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{pct:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.tight_layout()
    output_file3 = PLOTS_DIR / 'recovery_overhead_percentage.png'
    plt.savefig(output_file3, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file3.name}")
    plt.close()
    
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f'Recovery Time Analysis ({num_workers} Workers)', fontsize=16, fontweight='bold')
    
    ax1.axhline(y=baseline_time_actual, color='#2E86AB', linestyle='--', linewidth=2, alpha=0.7)
    ax1.plot(dead_workers, total_times, 'o-', linewidth=2, markersize=8, 
             color='#C73E1D', label='With Failures')
    ax1.set_xlabel('Dead Workers (%)', fontsize=11)
    ax1.set_ylabel('Execution Time (s)', fontsize=11)
    ax1.set_title('Execution Time', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend([f'Baseline: {baseline_time_actual:.2f}s', 'With Failures'], fontsize=9)
    ax1.set_xticks(dead_workers)
    ax1.set_xticklabels([f"{dw}\n{fp:.0f}%" for dw, fp in zip(dead_workers, failure_percentages)], fontsize=9)
    
    ax2.plot(dead_workers, overheads, 'o-', linewidth=2, markersize=8, color='#F18F01')
    ax2.set_xlabel('Dead Workers (%)', fontsize=11)
    ax2.set_ylabel('Recovery Overhead (s)', fontsize=11)
    ax2.set_title('Recovery Overhead', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(dead_workers)
    ax2.set_xticklabels([f"{dw}\n{fp:.0f}%" for dw, fp in zip(dead_workers, failure_percentages)], fontsize=9)
    
    colors = plt.cm.RdYlGn_r(np.array(overhead_pcts) / max(overhead_pcts) if max(overhead_pcts) > 0 else [0])
    bars = ax3.bar(dead_workers, overhead_pcts, color=colors, edgecolor='black', linewidth=1.2)
    ax3.set_xlabel('Dead Workers (%)', fontsize=11)
    ax3.set_ylabel('Overhead %', fontsize=11)
    ax3.set_title('Overhead Percentage', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.set_xticks(dead_workers)
    ax3.set_xticklabels([f"{dw}\n{fp:.0f}%" for dw, fp in zip(dead_workers, failure_percentages)], fontsize=9)
    
    plt.tight_layout()
    output_file_combined = PLOTS_DIR / 'recovery_analysis_combined.png'
    plt.savefig(output_file_combined, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file_combined.name}")
    plt.close()
    
    report_file = PLOTS_DIR / 'recovery_report.txt'
    with open(report_file, 'w') as f:
        f.write("Recovery Time Analysis Report\n")
        f.write("=" * 60 + "\n\n")
        
        for r in results:
            f.write(f"Dead Workers: {r['num_dead_workers']}\n")
            f.write(f"  Baseline Time: {r['baseline_time']:.2f}s\n")
            f.write(f"  Total Time: {r['total_time']:.2f}s\n")
            f.write(f"  Recovery Overhead: {r['recovery_overhead']:.2f}s ({r['overhead_percentage']:.1f}%)\n")
            f.write(f"  Tasks Reassigned: {r['reassigned_tasks']}\n")
            f.write(f"  Killed Workers: {', '.join(r['killed_workers'])}\n")
            f.write("\n")
    
    print(f"✓ Saved: {report_file.name}")

def main():
    print("\n Generating Recovery Analysis Plots...")
    print("=" * 60)
    
    results, baseline_time_actual = load_recovery_results()
    
    if not results:
        print(" No recovery results found in results/ directory")
        print("   Run recovery_time_analysis.py first with --dead-workers option")
        return
    
    if baseline_time_actual:
        print(f"Found actual baseline: {baseline_time_actual:.2f}s (from recovery_results_0.json)")
    
    print(f"Found {len(results)} recovery test results")
    
    plot_recovery_analysis(results, baseline_time_actual)

if __name__ == '__main__':
    main()
