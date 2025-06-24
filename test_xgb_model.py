# -*- coding: utf-8 -*-
"""Test Saved XGBoost Model on New Dataset

This script loads the pre-trained model and tests it on galaxy_trace_processed.csv
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Set plot style
plt.style.use('seaborn-v0_8-whitegrid')

def create_query_features(df):
    """
    Creates the same query-based features as used in training
    Must match exactly the feature engineering from training
    """
    df_feat = df.copy()
    
    # Query type proportions (very important for understanding load patterns)
    df_feat['aggregation_ratio'] = df_feat['aggregation_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['standard_ratio'] = df_feat['standard_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['firstname_ratio'] = df_feat['first_name_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['lastname_ratio'] = df_feat['last_name_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['country_ratio'] = df_feat['country_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['other_ratio'] = df_feat['other_queries_count'] / (df_feat['total_queries_count'] + 1)
    
    # Query complexity indicators
    df_feat['complex_queries'] = df_feat['aggregation_queries_count'] + df_feat['country_queries_count']
    df_feat['simple_queries'] = df_feat['first_name_queries_count'] + df_feat['last_name_queries_count']
    df_feat['complexity_ratio'] = df_feat['complex_queries'] / (df_feat['total_queries_count'] + 1)
    df_feat['simplicity_ratio'] = df_feat['simple_queries'] / (df_feat['total_queries_count'] + 1)
    
    # Load intensity and efficiency metrics
    df_feat['queries_per_task'] = df_feat['total_queries_count'] / (df_feat['max_concurrent_tasks'] + 1)
    df_feat['task_efficiency'] = df_feat['max_concurrent_tasks'] / (df_feat['total_queries_count'] + 1)
    
    # Query diversity indicators
    query_types = ['aggregation_queries_count', 'standard_queries_count', 'first_name_queries_count', 
                  'last_name_queries_count', 'country_queries_count', 'other_queries_count']
    df_feat['query_diversity'] = (df_feat[query_types] > 0).sum(axis=1)
    df_feat['dominant_query_ratio'] = df_feat[query_types].max(axis=1) / (df_feat['total_queries_count'] + 1)
    
    # Lag features for key metrics (query patterns)
    key_columns = ['total_queries_count', 'aggregation_queries_count', 'complexity_ratio', 'max_concurrent_tasks']
    for col in key_columns:
        for lag in [1, 2, 3, 5, 10]:
            df_feat[f'{col}_lag_{lag}'] = df_feat[col].shift(lag)
    
    # Rolling statistics for pattern recognition
    for col in ['total_queries_count', 'complexity_ratio', 'max_concurrent_tasks']:
        for window in [3, 5, 10, 20]:
            # Rolling mean and std
            df_feat[f'{col}_rolling_mean_{window}'] = df_feat[col].rolling(window=window).mean()
            df_feat[f'{col}_rolling_std_{window}'] = df_feat[col].rolling(window=window).std()
            
            # Rolling quantiles for pattern detection
            df_feat[f'{col}_rolling_q75_{window}'] = df_feat[col].rolling(window=window).quantile(0.75)
            df_feat[f'{col}_rolling_q90_{window}'] = df_feat[col].rolling(window=window).quantile(0.90)
    
    # Recent trend indicators (using simple slopes)
    df_feat['tasks_trend_3'] = df_feat['max_concurrent_tasks'].rolling(3).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 3 else 0, raw=False)
    df_feat['queries_trend_5'] = df_feat['total_queries_count'].rolling(5).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 5 else 0, raw=False)
    df_feat['complexity_trend_3'] = df_feat['complexity_ratio'].rolling(3).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 3 else 0, raw=False)
    
    # Load burst indicators
    df_feat['is_high_load'] = (df_feat['total_queries_count'] > df_feat['total_queries_count_rolling_q90_10']).astype(int)
    df_feat['is_complex_load'] = (df_feat['complexity_ratio'] > df_feat['complexity_ratio_rolling_q75_10']).astype(int)
    df_feat['is_peak_tasks'] = (df_feat['max_concurrent_tasks'] > df_feat['max_concurrent_tasks_rolling_q90_10']).astype(int)
    
    # Advanced interaction features
    df_feat['complex_load_interaction'] = df_feat['complexity_ratio'] * df_feat['total_queries_count']
    df_feat['efficiency_load_interaction'] = df_feat['task_efficiency'] * df_feat['total_queries_count']
    df_feat['diversity_complexity_interaction'] = df_feat['query_diversity'] * df_feat['complexity_ratio']
    
    # Recent change indicators
    df_feat['queries_pct_change'] = df_feat['total_queries_count'].pct_change()
    df_feat['tasks_pct_change'] = df_feat['max_concurrent_tasks'].pct_change()
    df_feat['complexity_pct_change'] = df_feat['complexity_ratio'].pct_change()
    
    return df_feat

# --- 1. Load Pre-trained Model and Scaler ---

print("--- Loading Pre-trained Model ---")
try:
    model = joblib.load('xgb_query_model.pkl')
    scaler = joblib.load('query_scaler.pkl')
    print("✓ Model and scaler loaded successfully")
except FileNotFoundError as e:
    print(f"Error: {e}")
    print("Please ensure you have run the training script first to generate the model files.")
    exit()

# Load feature names
try:
    with open('feature_names.txt', 'r') as f:
        FEATURES = [line.strip() for line in f.readlines()]
    print(f"✓ {len(FEATURES)} feature names loaded")
except FileNotFoundError:
    print("Warning: feature_names.txt not found. Will use all available features.")
    FEATURES = None

# --- 2. Load New Dataset ---

print("\n--- Loading New Dataset ---")
try:
    df_new = pd.read_csv('./datasets/galaxy_trace_processed.csv', sep=',', index_col=0)
    print(f"✓ New dataset loaded: {df_new.shape}")
except FileNotFoundError:
    print("Error: './datasets/galaxy_trace_processed.csv' not found.")
    print("Please ensure the file exists in the correct path.")
    exit()

# Keep all query-related columns
query_columns = ['max_concurrent_tasks', 'total_queries_count', 'aggregation_queries_count', 
                'standard_queries_count', 'first_name_queries_count', 'last_name_queries_count', 
                'country_queries_count', 'other_queries_count']

# Check if all required columns exist
missing_cols = [col for col in query_columns if col not in df_new.columns]
if missing_cols:
    print(f"Error: Missing columns in new dataset: {missing_cols}")
    print(f"Available columns: {list(df_new.columns)}")
    exit()

df_new = df_new[query_columns]

print(f"--- New Dataset Overview ---")
print(f"Date range: {df_new.index.min()} to {df_new.index.max()}")
print(f"Max concurrent tasks: {df_new['max_concurrent_tasks'].max()}")
print(f"Total samples: {len(df_new)}")

# --- 3. Feature Engineering ---

print("\n--- Creating Features for New Dataset ---")
df_new_featured = create_query_features(df_new)

# Drop rows with NaN values created by lag and rolling features
df_new_featured = df_new_featured.dropna()
print(f"✓ Features created, samples after dropna: {len(df_new_featured)}")

# --- 4. Prepare Features ---

# Define target and features
TARGET = 'max_concurrent_tasks'
exclude_cols = ['max_concurrent_tasks', 'aggregation_queries_count', 'total_queries_count', 
                'standard_queries_count', 'first_name_queries_count', 'last_name_queries_count', 
                'country_queries_count', 'other_queries_count']

if FEATURES is None:
    FEATURES = [col for col in df_new_featured.columns if col not in exclude_cols]

# Check if all required features exist
missing_features = [f for f in FEATURES if f not in df_new_featured.columns]
if missing_features:
    print(f"Warning: Missing features in new dataset: {missing_features}")
    FEATURES = [f for f in FEATURES if f in df_new_featured.columns]
    print(f"Using {len(FEATURES)} available features")

X_new = df_new_featured[FEATURES]
y_new = df_new_featured[TARGET]

# Scale features using the pre-trained scaler
X_new_scaled = scaler.transform(X_new)
X_new_scaled = pd.DataFrame(X_new_scaled, columns=FEATURES, index=X_new.index)

print(f"✓ Features prepared and scaled")

# --- 5. Make Predictions ---

print("\n--- Making Predictions ---")
y_pred_new = model.predict(X_new_scaled)

# --- 6. Evaluate Performance ---

rmse = np.sqrt(mean_squared_error(y_new, y_pred_new))
mae = mean_absolute_error(y_new, y_pred_new)
r2 = r2_score(y_new, y_pred_new)

print(f"\n=== MODEL PERFORMANCE ON NEW DATASET ===")
print(f"Root Mean Squared Error (RMSE): {rmse:.3f}")
print(f"Mean Absolute Error (MAE): {mae:.3f}")
print(f"R² Score: {r2:.3f}")

# Detailed performance analysis
errors = np.abs(y_new - y_pred_new)
print(f"\n--- Detailed Error Analysis ---")
print(f"Mean Absolute Error: {np.mean(errors):.3f}")
print(f"Median Absolute Error: {np.median(errors):.3f}")
print(f"90th Percentile Error: {np.percentile(errors, 90):.3f}")
print(f"95th Percentile Error: {np.percentile(errors, 95):.3f}")
print(f"Max Error: {np.max(errors):.3f}")

# Performance by load levels
print(f"\n--- Performance by Load Level ---")
low_load_mask = y_new <= 5
medium_load_mask = (y_new > 5) & (y_new <= 15)
high_load_mask = y_new > 15

if low_load_mask.sum() > 0:
    low_load_mae = np.mean(np.abs(y_new[low_load_mask] - y_pred_new[low_load_mask]))
    print(f"Low Load (≤5 tasks) MAE: {low_load_mae:.3f} ({low_load_mask.sum()} samples)")

if medium_load_mask.sum() > 0:
    medium_load_mae = np.mean(np.abs(y_new[medium_load_mask] - y_pred_new[medium_load_mask]))
    print(f"Medium Load (6-15 tasks) MAE: {medium_load_mae:.3f} ({medium_load_mask.sum()} samples)")

if high_load_mask.sum() > 0:
    high_load_mae = np.mean(np.abs(y_new[high_load_mask] - y_pred_new[high_load_mask]))
    print(f"High Load (>15 tasks) MAE: {high_load_mae:.3f} ({high_load_mask.sum()} samples)")

# Dataset comparison
print(f"\n--- Dataset Comparison ---")
print(f"New dataset max tasks: {y_new.max():.0f}")
print(f"New dataset mean tasks: {y_new.mean():.2f}")
print(f"New dataset std tasks: {y_new.std():.2f}")

# --- 7. Visualization ---

fig, axes = plt.subplots(2, 2, figsize=(20, 12))

# Plot 1: Time series comparison
axes[0, 0].plot(y_new.index, y_new, label='Actual', alpha=0.8, linewidth=2)
axes[0, 0].plot(y_new.index, y_pred_new, label='Predicted', alpha=0.8, linewidth=2)
axes[0, 0].set_title('Model Predictions on New Dataset (Galaxy Trace)', fontsize=14)
axes[0, 0].set_xlabel('Timestamp')
axes[0, 0].set_ylabel('Max Concurrent Tasks')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Plot 2: Scatter plot
axes[0, 1].scatter(y_new, y_pred_new, alpha=0.6, s=50)
axes[0, 1].plot([y_new.min(), y_new.max()], [y_new.min(), y_new.max()], 'r--', lw=2)
axes[0, 1].set_title('Predicted vs Actual Scatter Plot')
axes[0, 1].set_xlabel('Actual Max Concurrent Tasks')
axes[0, 1].set_ylabel('Predicted Max Concurrent Tasks')
axes[0, 1].grid(True, alpha=0.3)

# Add correlation coefficient to scatter plot
correlation = np.corrcoef(y_new, y_pred_new)[0, 1]
axes[0, 1].text(0.05, 0.95, f'Correlation: {correlation:.3f}', transform=axes[0, 1].transAxes, 
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# Plot 3: Residuals
residuals = y_new - y_pred_new
axes[1, 0].scatter(y_pred_new, residuals, alpha=0.6)
axes[1, 0].axhline(y=0, color='r', linestyle='--')
axes[1, 0].set_title('Residuals Analysis')
axes[1, 0].set_xlabel('Predicted Values')
axes[1, 0].set_ylabel('Residuals (Actual - Predicted)')
axes[1, 0].grid(True, alpha=0.3)

# Plot 4: Error distribution
axes[1, 1].hist(errors, bins=30, alpha=0.7, edgecolor='black')
axes[1, 1].axvline(np.mean(errors), color='red', linestyle='--', linewidth=2, label=f'Mean: {np.mean(errors):.2f}')
axes[1, 1].axvline(np.median(errors), color='green', linestyle='--', linewidth=2, label=f'Median: {np.median(errors):.2f}')
axes[1, 1].set_title('Error Distribution')
axes[1, 1].set_xlabel('Absolute Error')
axes[1, 1].set_ylabel('Frequency')
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# --- 8. Additional Analysis ---

print(f"\n--- Query Pattern Analysis ---")
print("Query type correlations in new dataset:")
query_types = ['total_queries_count', 'aggregation_queries_count', 'standard_queries_count', 
               'first_name_queries_count', 'last_name_queries_count', 'country_queries_count', 'other_queries_count']

for qtype in query_types:
    if qtype in df_new.columns:
        corr = df_new[qtype].corr(df_new['max_concurrent_tasks'])
        print(f"  {qtype}: {corr:.3f}")

# Identify worst predictions for analysis
worst_errors_idx = np.argsort(errors)[-10:]
print(f"\n--- Top 10 Worst Predictions ---")
for i, idx in enumerate(worst_errors_idx):
    actual = y_new.iloc[idx]
    predicted = y_pred_new[idx]
    error = errors.iloc[idx]
    timestamp = y_new.index[idx]
    print(f"{i+1:2d}. {timestamp}: Actual={actual:.1f}, Predicted={predicted:.1f}, Error={error:.1f}")

print(f"\n=== TESTING COMPLETE ===")
print("✓ Model successfully tested on new dataset")
print("✓ Performance metrics calculated")
print("✓ Visualizations generated")
print("✓ Analysis complete")