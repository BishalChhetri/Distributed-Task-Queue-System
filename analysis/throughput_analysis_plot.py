#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / 'results'
PLOTS_DIR = RESULTS_DIR / 'throughput_plots'
PLOTS_DIR.mkdir(exist_ok=True)


def load_results(filename):
    filepath = RESULTS_DIR / filename
    with open(filepath, 'r') as f:
        return json.load(f)


def plot_throughput_analysis(results_data, task_size):
    results = results_data['results']
    
    workers = [r['num_workers'] for r in results]
    exec_times = [r['execution_time'] for r in results]
    throughputs = [r['throughput'] for r in results]
    
    speedups = [exec_times[0] / t for t in exec_times]
    efficiencies = [(speedup / w) * 100 for speedup, w in zip(speedups, workers)]
    
    # Plot 1: Execution Time
    fig1, ax1 = plt.subplots(figsize=(8, 6))
    ax1.plot(workers, exec_times, 'o-', linewidth=2, markersize=10, color='#2E86AB')
    ax1.set_xlabel('Number of Workers', fontsize=12)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
    ax1.set_title(f'Execution Time vs Workers', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(workers)
    for w, t in zip(workers, exec_times):
        ax1.text(w, t, f'{t:.2f}s', ha='center', va='bottom', fontsize=10)
    plt.tight_layout()
    output_file1 = PLOTS_DIR / f'execution_time_{task_size}.png'
    plt.savefig(output_file1, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file1.name}")
    plt.close()
    
    # Plot 2: Throughput
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    ax2.plot(workers, throughputs, 'o-', linewidth=2, markersize=10, color='#A23B72')
    ax2.set_xlabel('Number of Workers', fontsize=12)
    ax2.set_ylabel('Throughput (tasks/sec)', fontsize=12)
    ax2.set_title(f'Throughput vs Workers', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(workers)
    for w, tp in zip(workers, throughputs):
        ax2.text(w, tp, f'{tp:.2f}', ha='center', va='bottom', fontsize=10)
    plt.tight_layout()
    output_file2 = PLOTS_DIR / f'throughput_{task_size}.png'
    plt.savefig(output_file2, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file2.name}")
    plt.close()
    
    # Plot 3: Speedup
    fig3, ax3 = plt.subplots(figsize=(8, 6))
    ideal_speedup = workers
    ax3.plot(workers, speedups, 'o-', linewidth=2, markersize=10, color='#F18F01', label='Actual Speedup')
    ax3.plot(workers, ideal_speedup, '--', linewidth=2, color='#C73E1D', alpha=0.6, label='Ideal (Linear)')
    ax3.set_xlabel('Number of Workers', fontsize=12)
    ax3.set_ylabel('Speedup', fontsize=12)
    ax3.set_title(f'Speedup vs Workers', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend(fontsize=11)
    ax3.set_xticks(workers)
    for w, s in zip(workers, speedups):
        ax3.text(w, s, f'{s:.2f}x', ha='center', va='bottom', fontsize=10)
    plt.tight_layout()
    output_file3 = PLOTS_DIR / f'speedup_{task_size}.png'
    plt.savefig(output_file3, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file3.name}")
    plt.close()
    
    # # Plot 4: Efficiency
    # fig4, ax4 = plt.subplots(figsize=(8, 6))
    # colors = plt.cm.RdYlGn(np.array(efficiencies) / 100)
    # bars = ax4.bar(workers, efficiencies, color=colors, edgecolor='black', linewidth=1.2)
    # ax4.axhline(y=100, color='red', linestyle='--', linewidth=2, alpha=0.5, label='100% Efficiency')
    # ax4.set_xlabel('Number of Workers', fontsize=12)
    # ax4.set_ylabel('Parallel Efficiency (%)', fontsize=12)
    # ax4.set_title(f'Parallel Efficiency vs Workers', fontsize=14, fontweight='bold')
    # ax4.set_ylim(0, 110)
    # ax4.grid(True, alpha=0.3, axis='y')
    # ax4.set_xticks(workers)
    # ax4.legend(fontsize=11)
    # for bar, eff in zip(bars, efficiencies):
    #     height = bar.get_height()
    #     ax4.text(bar.get_x() + bar.get_width()/2., height,
    #             f'{eff:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
    # plt.tight_layout()
    # output_file4 = PLOTS_DIR / f'efficiency_{task_size}.png'
    # plt.savefig(output_file4, dpi=300, bbox_inches='tight')
    # print(f"✓ Saved: {output_file4.name}")
    # plt.close()
    
    # Combined plot (2x2 grid)
    # fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(f'Throughput Analysis', fontsize=16, fontweight='bold')
    
    ax1.plot(workers, exec_times, 'o-', linewidth=2, markersize=8, color='#2E86AB')
    ax1.set_xlabel('Number of Workers', fontsize=11)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=11)
    ax1.set_title('Execution Time vs Workers', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(workers)
    for w, t in zip(workers, exec_times):
        ax1.text(w, t, f'{t:.2f}s', ha='center', va='bottom', fontsize=9)
    
    ax2.plot(workers, throughputs, 'o-', linewidth=2, markersize=8, color='#A23B72')
    ax2.set_xlabel('Number of Workers', fontsize=11)
    ax2.set_ylabel('Throughput (tasks/sec)', fontsize=11)
    ax2.set_title('Throughput vs Workers', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(workers)
    for w, tp in zip(workers, throughputs):
        ax2.text(w, tp, f'{tp:.2f}', ha='center', va='bottom', fontsize=9)
    
    ax3.plot(workers, speedups, 'o-', linewidth=2, markersize=8, color='#F18F01', label='Actual Speedup')
    ax3.plot(workers, ideal_speedup, '--', linewidth=2, color='#C73E1D', alpha=0.6, label='Ideal (Linear)')
    ax3.set_xlabel('Number of Workers', fontsize=11)
    ax3.set_ylabel('Speedup', fontsize=11)
    ax3.set_title('Speedup vs Workers', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend()
    ax3.set_xticks(workers)
    # for w, s in zip(workers, speedups):
    #     ax3.text(w, s, f'{s:.2f}x', ha='center', va='bottom', fontsize=9)
    
    # colors = plt.cm.RdYlGn(np.array(efficiencies) / 100)
    # bars = ax4.bar(workers, efficiencies, color=colors, edgecolor='black', linewidth=1.2)
    # ax4.axhline(y=100, color='red', linestyle='--', linewidth=2, alpha=0.5, label='100% Efficiency')
    # ax4.set_xlabel('Number of Workers', fontsize=11)
    # ax4.set_ylabel('Parallel Efficiency (%)', fontsize=11)
    # ax4.set_title('Parallel Efficiency vs Workers', fontsize=12, fontweight='bold')
    # ax4.set_ylim(0, 110)
    # ax4.grid(True, alpha=0.3, axis='y')
    # ax4.set_xticks(workers)
    # ax4.legend()
    # for bar, eff in zip(bars, efficiencies):
    #     height = bar.get_height()
    #     ax4.text(bar.get_x() + bar.get_width()/2., height,
    #             f'{eff:.1f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    
    output_file_combined = PLOTS_DIR / f'throughput_analysis_{task_size}.png'
    plt.savefig(output_file_combined, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file_combined.name}")
    plt.close()


def generate_report(results_data, task_size):
    results = results_data['results']
    
    report_file = PLOTS_DIR / f'report_{task_size}.txt'
    
    with open(report_file, 'w') as f:
        f.write("="*70 + "\n")
        f.write(f"THROUGHPUT ANALYSIS REPORT - Task Size: {task_size:,}\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Test Configuration:\n")
        f.write(f"  Task Size:        {task_size:,}\n")
        f.write(f"  Number of Tasks:  {results[0]['num_tasks']}\n")
        f.write(f"  Method:           {results_data['method']}\n")
        f.write(f"  Test Duration:    {results_data['duration_seconds']:.1f} seconds\n")
        f.write(f"  Test Start:       {results_data['test_start']}\n")
        f.write("\n" + "="*70 + "\n\n")
        
        f.write("In here, we calculated efficiency using the equation: Eff. = (SpeedUp / No. of workers) * 100\n\n")
        
        f.write("Results Summary:\n\n")
        f.write(f"{'Workers':<10} {'Exec Time':<15} {'Throughput':<18} {'Speedup':<12} {'Efficiency':<12}\n")
        f.write("-"*70 + "\n")
        
        baseline = results[0]['execution_time']
        
        for r in results:
            workers = r['num_workers']
            exec_time = r['execution_time']
            throughput = r['throughput']
            speedup = baseline / exec_time
            efficiency = (speedup / workers) * 100
            
            f.write(f"{workers:<10} {exec_time:<15.4f} {throughput:<18.2f} "
                   f"{speedup:<12.2f} {efficiency:<12.1f}%\n")
        
        f.write("\n" + "="*70 + "\n\n")
        
        f.write("Key Metrics:\n\n")
        
        best_speedup_idx = max(range(len(results)), 
                               key=lambda i: baseline / results[i]['execution_time'])
        best_speedup = baseline / results[best_speedup_idx]['execution_time']
        best_workers = results[best_speedup_idx]['num_workers']
        
        f.write(f"  Best Speedup:     {best_speedup:.2f}x with {best_workers} workers\n")
        
        best_eff_idx = max(range(len(results)), 
                          key=lambda i: (baseline / results[i]['execution_time']) / results[i]['num_workers'] * 100)
        best_eff = (baseline / results[best_eff_idx]['execution_time']) / results[best_eff_idx]['num_workers'] * 100
        best_eff_workers = results[best_eff_idx]['num_workers']
        
        f.write(f"  Best Efficiency:  {best_eff:.1f}% with {best_eff_workers} workers\n")
        
        max_throughput = max(r['throughput'] for r in results)
        max_throughput_workers = [r['num_workers'] for r in results if r['throughput'] == max_throughput][0]
        
        f.write(f"  Max Throughput:   {max_throughput:.2f} tasks/sec with {max_throughput_workers} workers\n")
        
        f.write("\n" + "="*70 + "\n\n")
        
        f.write("Individual Task Computation Times:\n\n")
        for r in results:
            f.write(f"  {r['num_workers']} Workers:\n")
            f.write(f"    Average:  {r['avg_computation_time']:.4f}s\n")
            if r['task_computation_times']:
                f.write(f"    Min:      {min(r['task_computation_times']):.4f}s\n")
                f.write(f"    Max:      {max(r['task_computation_times']):.4f}s\n")
            f.write("\n")
        
        f.write("="*70 + "\n")
    
    print(f"✓ Saved: {report_file}")


def main():
    print("THROUGHPUT ANALYSIS PLOTTING")
    print()
    
    result_files = list(RESULTS_DIR.glob('results_*.json'))
    
    if not result_files:
        print(" No results files found in analysis/results/")
        print("   Expected files: results_1000.json, results_1000000.json")
        return
    
    print(f"Found {len(result_files)} result file(s):\n")
    
    for result_file in sorted(result_files):
        print(f"Processing: {result_file.name}")
        
        try:
            results_data = load_results(result_file.name)
            task_size = results_data['task_size']
            
            plot_throughput_analysis(results_data, task_size)
            generate_report(results_data, task_size)
            
            print()
        except Exception as e:
            print(f" Error processing {result_file.name}: {e}\n")
    
   
    print("PLOTTING COMPLETE")
    print("="*70)
    print(f"\nOutput directory: {PLOTS_DIR}")
    print("\nGenerated files:")
    print("  - throughput_analysis_<task_size>.png")
    print("  - report_<task_size>.txt")
    print("="*70)


if __name__ == '__main__':
    main()
