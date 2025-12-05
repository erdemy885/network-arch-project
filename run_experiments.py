import subprocess
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Configuration
ALGORITHMS = ["ECMP", "HEDERA", "PFABRIC", "FASTPASS"]
# Using just MIXED and SMALL_DOMINATED for clearer demonstration of pFabric logic
WORKLOADS = ["MIXED", "SMALL_DOMINATED", "LARGE_DOMINATED"]
OUTPUT_FILE = "experiment_results.csv"

def run_simulation():
    """
    Runs the simulation script for every combination of Algo and Workload.
    """
    # Remove old results
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    print(f"--- Starting Experiments ---")
    print(f"Results will be saved to {OUTPUT_FILE}")

    for algo in ALGORITHMS:
        for load in WORKLOADS:
            print(f"Running: {algo} with {load}...")
            
            # Call the network_simulation.py as a subprocess
            cmd = [
                "python", "network_simulation.py",
                "--algo", algo,
                "--workload", load,
                "--out", OUTPUT_FILE
            ]
            
            subprocess.run(cmd)
            print(f"Finished {algo} - {load}")

    print("--- All Experiments Complete ---")

def plot_results():
    """
    Reads the CSV and generates:
    1. Average FCT Bar Chart
    2. CDF (Cumulative Distribution Function) for Tail Latency
    """
    if not os.path.exists(OUTPUT_FILE):
        print("No data file found!")
        return

    df = pd.read_csv(OUTPUT_FILE)
    
    # Clean data (remove failed flows with negative or zero time)
    df = df[df['FCT'] > 0]

    # --- Plot 1: Average Flow Completion Time ---
    plt.figure(figsize=(10, 6))
    
    # Group by Algo and Workload to get mean FCT
    grouped = df.groupby(['Algorithm', 'Workload'])['FCT'].mean().unstack()
    
    grouped.plot(kind='bar', alpha=0.85)
    plt.title("Average Flow Completion Time (Lower is Better)")
    plt.ylabel("Time (seconds)")
    plt.xticks(rotation=0)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("plot_average_fct.png")
    print("Saved plot_average_fct.png")
    plt.show()

    # --- Plot 2: CDF of Tail Latency (For Mixed Workload) ---
    # We focus on Mixed workload to see how pFabric handles small flows amidst large ones.
    target_workload = "MIXED"
    subset = df[df['Workload'] == target_workload]

    plt.figure(figsize=(10, 6))
    
    for algo in ALGORITHMS:
        algo_data = subset[subset['Algorithm'] == algo]['FCT']
        if len(algo_data) == 0: continue
        
        # Calculate CDF
        sorted_data = np.sort(algo_data)
        yvals = np.arange(len(sorted_data)) / float(len(sorted_data) - 1)
        
        plt.plot(sorted_data, yvals, label=algo, linewidth=2)

    plt.title(f"CDF of Flow Completion Time ({target_workload} Workload)")
    plt.xlabel("Flow Completion Time (s)")
    plt.ylabel("CDF (Probability <= x)")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig("plot_cdf_latency.png")
    print("Saved plot_cdf_latency.png")
    plt.show()

if __name__ == "__main__":
    # 1. Run the simulations
    run_simulation()
    
    # 2. Generate Plots
    plot_results()