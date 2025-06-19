import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Read the CSV file
df = pd.read_csv('askalon_ee_trace_processed.csv', sep=';')

# First plot set: max_concurrent_tasks and total_queries_count
fig1, axes1 = plt.subplots(1, 2, figsize=(12, 5))

# Plot 1: max_concurrent_tasks
ax1 = axes1[0]
n, bins, patches = ax1.hist(df['max_concurrent_tasks'], bins=8, alpha=0.7, color='lightcoral', edgecolor='black')
mean_val = df['max_concurrent_tasks'].mean()
ax1.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_val:.1f}')
ax1.set_title('Max Concurrent Tasks', fontsize=12, fontweight='bold')
ax1.set_xlabel('Count')
ax1.set_ylabel('Frequency')
ax1.legend()
ax1.grid(True, alpha=0.3)
min_val = df['max_concurrent_tasks'].min()
max_val = df['max_concurrent_tasks'].max()
ax1.text(0.02, 0.95, f'Range: {min_val}-{max_val}', 
         transform=ax1.transAxes, fontsize=10, 
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
         verticalalignment='top')

# Plot 2: total_queries_count
ax2 = axes1[1]
n, bins, patches = ax2.hist(df['total_queries_count'], bins=8, alpha=0.7, color='lightgreen', edgecolor='black')
mean_val = df['total_queries_count'].mean()
ax2.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_val:.1f}')
ax2.set_title('Total Queries Count', fontsize=12, fontweight='bold')
ax2.set_xlabel('Count')
ax2.set_ylabel('Frequency')
ax2.legend()
ax2.grid(True, alpha=0.3)
min_val = df['total_queries_count'].min()
max_val = df['total_queries_count'].max()
ax2.text(0.02, 0.95, f'Range: {min_val}-{max_val}', 
         transform=ax2.transAxes, fontsize=10, 
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
         verticalalignment='top')

plt.tight_layout()
plt.show()

# Second plot set: different query types
query_types = [
    'standard_queries_count',
    'first_name_queries_count',
    'last_name_queries_count',
    'country_queries_count',
    'other_queries_count',
    'aggregation_queries_count'
]

fig2, axes2 = plt.subplots(2, 3, figsize=(15, 10))
axes2 = axes2.flatten()

colors = ['skyblue', 'lightpink', 'lightgray', 'wheat', 'lightsteelblue', 'lightsalmon']

for i, col in enumerate(query_types):
    ax = axes2[i]
    
    # Create histogram
    n, bins, patches = ax.hist(df[col], bins=8, alpha=0.7, color=colors[i], edgecolor='black')
    
    # Calculate and plot mean
    mean_val = df[col].mean()
    ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, 
               label=f'Mean: {mean_val:.1f}')
    
    # Set title and labels
    ax.set_title(f'{col.replace("_", " ").title()}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Count')
    ax.set_ylabel('Frequency')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add range info as text
    min_val = df[col].min()
    max_val = df[col].max()
    ax.text(0.02, 0.95, f'Range: {min_val}-{max_val}', 
            transform=ax.transAxes, fontsize=10, 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            verticalalignment='top')

plt.tight_layout()
plt.show()

# Print summary statistics for both sets
print("SUMMARY STATISTICS")
print("="*60)
print("\nFirst Set - Concurrent Tasks & Total Queries:")
print("-" * 45)
for col in ['max_concurrent_tasks', 'total_queries_count']:
    mean_val = df[col].mean()
    min_val = df[col].min()
    max_val = df[col].max()
    print(f"{col.replace('_', ' ').title():<25} Mean: {mean_val:>6.1f}  Range: {min_val}-{max_val}")

print("\nSecond Set - Query Types:")
print("-" * 25)
for col in query_types:
    mean_val = df[col].mean()
    min_val = df[col].min()
    max_val = df[col].max()
    print(f"{col.replace('_', ' ').title():<25} Mean: {mean_val:>6.1f}  Range: {min_val}-{max_val}")