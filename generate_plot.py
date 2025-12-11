import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the new results
results_df = pd.read_csv('experiment_results.csv')
stats_df = pd.read_csv('experiment_stats.csv')

# Rename FCT to Packet Latency for clarity
results_df.rename(columns={'FCT': 'Packet Latency (s)'}, inplace=True)

# --- 1. Sanity Check: Buffers and Drops ---
print("Stats Summary:")
print(stats_df.groupby('Metric')['Value'].describe())

if 'BUFFER' in stats_df['Metric'].unique():
    max_buffer = stats_df[stats_df['Metric'] == 'BUFFER']['Value'].max()
    print(f"\nMax Buffer Occupancy: {max_buffer}")

if 'DROP' in stats_df['Metric'].unique():
    total_drops = stats_df[stats_df['Metric'] == 'DROP'].shape[0]
    print(f"Total Packet Drops: {total_drops}")
    
    # Drops by Algorithm
    drops_by_algo = stats_df[stats_df['Metric'] == 'DROP'].groupby('Algorithm').size()
    print("\nDrops by Algorithm:")
    print(drops_by_algo)

# --- 2. Performance Analysis: Latency ---
# Average Latency per Algorithm and Workload
latency_summary = results_df.groupby(['Algorithm', 'Workload'])['Packet Latency (s)'].describe()
print("\nLatency Summary:")
print(latency_summary)

# --- 3. Visualization ---

# Plot 1: Average Latency
plt.figure(figsize=(10, 6))
sns.barplot(data=results_df, x='Workload', y='Packet Latency (s)', hue='Algorithm', errorbar=None)
plt.title('Average Packet Latency (New Simulation)')
plt.ylabel('Latency (s)')
plt.savefig('new_avg_latency.png')

# Plot 2: Buffer Occupancy Boxplot
buffer_data = stats_df[stats_df['Metric'] == 'BUFFER']
plt.figure(figsize=(10, 6))
sns.boxplot(data=buffer_data, x='Algorithm', y='Value')
plt.title('Buffer Occupancy Distribution (New Simulation)')
plt.ylabel('Packets in Buffer')
plt.savefig('new_buffer_dist.png')

# Plot 3: Drops per Algorithm
if 'DROP' in stats_df['Metric'].unique():
    drop_counts = stats_df[stats_df['Metric'] == 'DROP']['Algorithm'].value_counts().reset_index()
    drop_counts.columns = ['Algorithm', 'Drops']
    plt.figure(figsize=(8, 5))
    sns.barplot(data=drop_counts, x='Algorithm', y='Drops', color='salmon')
    plt.title('Total Packet Drops by Algorithm')
    plt.savefig('new_drop_counts.png')

print("Plots generated.")