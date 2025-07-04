# -*- coding: utf-8 -*-
"""XGBoost Model with Time-Shifted Predictions for MongoDB Resharding

Focus:
1. Time-shifted predictions to anticipate resharding needs
2. Configurable anticipation time (default: 10 minutes)
3. Overprovisioning bias for safety
4. Clean visualization of shifted predictions
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import RobustScaler
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-whitegrid')

# --- CONFIGURATION ---
ANTICIPATION_MINUTES = 10  # How many minutes ahead to predict (configurable)
RESHARDING_DELAY_MINUTES = 5  # MongoDB resharding delay for reference

print(f"--- TIME-SHIFTED PREDICTION CONFIGURATION ---")
print(f"Anticipation time: {ANTICIPATION_MINUTES} minutes")
print(f"MongoDB resharding delay: {RESHARDING_DELAY_MINUTES} minutes")
print(f"Safety buffer: {ANTICIPATION_MINUTES - RESHARDING_DELAY_MINUTES} minutes")

# --- 1. Load and Prepare Data ---
df = pd.read_csv('./datasets/combined_trace_processed.csv', index_col=0)

# Convert timestamp and set as index
df['ts_submit_dt'] = pd.to_datetime(df['ts_submit_dt'])
df = df.set_index('ts_submit_dt')
df = df.sort_index()

# Keep query-related columns
query_columns = ['max_concurrent_tasks', 'total_queries_count', 'aggregation_queries_count', 
                'standard_queries_count', 'first_name_queries_count', 'last_name_queries_count', 
                'country_queries_count', 'other_queries_count']
df = df[query_columns]

print(f"Data shape: {df.shape}")
print(f"Date range: {df.index.min()} to {df.index.max()}")

# Handle variable time frequency for shifting
time_diffs = []
for i in range(1, min(100, len(df))):  # Sample first 100 intervals
    diff = (df.index[i] - df.index[i-1]).total_seconds() / 60
    time_diffs.append(diff)

median_frequency_minutes = np.median(time_diffs)
shift_periods = int(ANTICIPATION_MINUTES / median_frequency_minutes)

print(f"Variable sample frequency - median: {median_frequency_minutes:.1f} minutes")
print(f"Shift periods needed: {shift_periods} samples")

# --- 2. Feature Engineering with Shard-Aware Features ---
def create_query_features(df):
    df_feat = df.copy()
    
    # Add shard-based features early in pipeline
    df_feat['current_shards'] = df_feat['max_concurrent_tasks'].apply(
        lambda x: 1 if x <= 6 else 2 if x <= 8 else 3 if x <= 10 else 
                 4 if x <= 12 else 5 if x == 13 else 6 if x == 14 else 7)
    
    # Query ratios
    df_feat['aggregation_ratio'] = df_feat['aggregation_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['standard_ratio'] = df_feat['standard_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['firstname_ratio'] = df_feat['first_name_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['lastname_ratio'] = df_feat['last_name_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['country_ratio'] = df_feat['country_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['other_ratio'] = df_feat['other_queries_count'] / (df_feat['total_queries_count'] + 1)
    
    # Query complexity
    df_feat['complex_queries'] = df_feat['aggregation_queries_count'] + df_feat['country_queries_count']
    df_feat['simple_queries'] = df_feat['first_name_queries_count'] + df_feat['last_name_queries_count']
    df_feat['complexity_ratio'] = df_feat['complex_queries'] / (df_feat['total_queries_count'] + 1)
    
    # Shard-aware load metrics
    df_feat['queries_per_shard'] = df_feat['total_queries_count'] / df_feat['current_shards']
    df_feat['complex_queries_per_shard'] = df_feat['complex_queries'] / df_feat['current_shards']
    df_feat['shard_utilization'] = df_feat['max_concurrent_tasks'] / (df_feat['current_shards'] * 6)  # Max 6 threads per shard
    
    # Load metrics
    df_feat['queries_per_task'] = df_feat['total_queries_count'] / (df_feat['max_concurrent_tasks'] + 1)
    df_feat['task_efficiency'] = df_feat['max_concurrent_tasks'] / (df_feat['total_queries_count'] + 1)
    
    # Shard transition detection
    df_feat['shard_change'] = df_feat['current_shards'].diff().fillna(0)
    df_feat['near_shard_boundary'] = 0
    # Flag when close to shard boundaries
    boundary_threads = [6, 8, 10, 12, 13, 14]
    for bt in boundary_threads:
        df_feat.loc[abs(df_feat['max_concurrent_tasks'] - bt) <= 1, 'near_shard_boundary'] = 1
    
    # Peak load indicators with shard context
    df_feat['is_max_shard_capacity'] = (df_feat['max_concurrent_tasks'] >= df_feat['current_shards'] * 6).astype(int)
    
    # Lag features with shard awareness
    key_columns = ['total_queries_count', 'aggregation_queries_count', 'complexity_ratio', 
                   'max_concurrent_tasks', 'current_shards', 'shard_utilization']
    for col in key_columns:
        for lag in [1, 2, 3, 5, 10]:
            df_feat[f'{col}_lag_{lag}'] = df_feat[col].shift(lag)
    
    # Rolling statistics
    for col in ['total_queries_count', 'complexity_ratio', 'max_concurrent_tasks', 'current_shards']:
        for window in [3, 5, 10, 20]:
            df_feat[f'{col}_rolling_mean_{window}'] = df_feat[col].rolling(window=window).mean()
            df_feat[f'{col}_rolling_std_{window}'] = df_feat[col].rolling(window=window).std()
            df_feat[f'{col}_rolling_q90_{window}'] = df_feat[col].rolling(window=window).quantile(0.90)
            df_feat[f'{col}_rolling_max_{window}'] = df_feat[col].rolling(window=window).max()
    
    # Trend indicators
    df_feat['tasks_trend_3'] = df_feat['max_concurrent_tasks'].rolling(3).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 3 else 0, raw=False)
    df_feat['queries_trend_5'] = df_feat['total_queries_count'].rolling(5).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 5 else 0, raw=False)
    df_feat['shard_trend_5'] = df_feat['current_shards'].rolling(5).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == 5 else 0, raw=False)
    
    # Peak detection with more aggressive thresholds
    df_feat['is_high_load'] = (df_feat['total_queries_count'] > df_feat['total_queries_count_rolling_q90_10']).astype(int)
    df_feat['is_peak_tasks'] = (df_feat['max_concurrent_tasks'] > df_feat['max_concurrent_tasks_rolling_q90_10']).astype(int)
    df_feat['is_extreme_peak'] = (df_feat['max_concurrent_tasks'] > df_feat['max_concurrent_tasks_rolling_max_20'] * 0.9).astype(int)
    
    return df_feat

df_featured = create_query_features(df)
df_featured = df_featured.dropna()

print(f"Features created: {df_featured.shape[1]}")
print(f"Samples after cleaning: {df_featured.shape[0]}")

# --- 3. Create Time-Shifted Targets ---
print(f"\n--- Creating Time-Shifted Targets ---")

# Shift target forward to predict future needs
df_featured['max_concurrent_tasks_future'] = df_featured['max_concurrent_tasks'].shift(-shift_periods)
df_featured = df_featured.dropna(subset=['max_concurrent_tasks_future'])

print(f"Samples after time shifting: {df_featured.shape[0]}")
print(f"Current target range: {df_featured['max_concurrent_tasks'].min():.0f} to {df_featured['max_concurrent_tasks'].max():.0f}")
print(f"Future target range: {df_featured['max_concurrent_tasks_future'].min():.0f} to {df_featured['max_concurrent_tasks_future'].max():.0f}")

# --- 4. Train-Test Split ---
split_ratio = 0.80
split_index = int(len(df_featured) * split_ratio)

train_df = df_featured.iloc[:split_index]
test_df = df_featured.iloc[split_index:]

print(f"\nTraining samples: {len(train_df)}")
print(f"Test samples: {len(test_df)}")

# Define features (exclude shard-related targets but keep shard features)
exclude_cols = ['max_concurrent_tasks', 'max_concurrent_tasks_future', 'aggregation_queries_count', 
                'total_queries_count', 'standard_queries_count', 'first_name_queries_count', 
                'last_name_queries_count', 'country_queries_count', 'other_queries_count']
FEATURES = [col for col in df_featured.columns if col not in exclude_cols]

X_train = train_df[FEATURES]
y_train_future = train_df['max_concurrent_tasks_future']
X_test = test_df[FEATURES]
y_test_future = test_df['max_concurrent_tasks_future']
y_test_current = test_df['max_concurrent_tasks']

print(f"Using {len(FEATURES)} features")

# --- 5. Apply Aggressive Overprovisioning Bias for High Peaks ---
print(f"\n--- Applying Enhanced Overprovisioning Bias ---")

y_train_biased = y_train_future.copy()

# More aggressive bias, especially for high loads
low_load_mask = y_train_future <= 5
medium_load_mask = (y_train_future > 5) & (y_train_future <= 15)
high_load_mask = (y_train_future > 15) & (y_train_future <= 25)
extreme_load_mask = y_train_future > 25

# Enhanced bias for underprovisioning issues
y_train_biased[low_load_mask] = y_train_future[low_load_mask] * 1.02        # 2% bias
y_train_biased[medium_load_mask] = y_train_future[medium_load_mask] * 1.35   # 35% bias  
y_train_biased[high_load_mask] = y_train_future[high_load_mask] * 1.50       # 50% bias
y_train_biased[extreme_load_mask] = y_train_future[extreme_load_mask] * 1.65 # 65% bias

print(f"Enhanced bias applied:")
print(f"- Low load: {low_load_mask.sum()} samples (2% bias)")
print(f"- Medium load: {medium_load_mask.sum()} samples (35% bias)")
print(f"- High load: {high_load_mask.sum()} samples (50% bias)")
print(f"- Extreme load: {extreme_load_mask.sum()} samples (65% bias)")

# --- 6. Scale Features ---
scaler_X = RobustScaler()
X_train_scaled = scaler_X.fit_transform(X_train)
X_test_scaled = scaler_X.transform(X_test)

# --- 7. Train XGBoost Model ---
print(f"\n--- Training XGBoost Model ---")

xgb_model = xgb.XGBRegressor(
    n_estimators=4000,      # Increased for better peak learning
    max_depth=14,           # Deeper for complex shard interactions
    learning_rate=0.01,     # Slightly lower for more careful learning
    subsample=0.8,          
    colsample_bytree=0.8,
    reg_alpha=0.0001,       # Very low regularization for aggressive predictions
    reg_lambda=0.01,        
    gamma=0.001,            # Very low to allow aggressive splits
    min_child_weight=0.5,   # Lower to capture extreme peaks
    objective='reg:squarederror',
    random_state=42,
    early_stopping_rounds=300
)

xgb_model.fit(
    X_train_scaled, y_train_biased,
    eval_set=[(X_test_scaled, y_test_future)],
    verbose=100
)

# --- 8. Make Predictions ---
y_pred_future_raw = xgb_model.predict(X_test_scaled)
y_pred_future = np.maximum(np.round(y_pred_future_raw).astype(int), 1)

# Calculate metrics
rmse = np.sqrt(mean_squared_error(y_test_future, y_pred_future))
mae = mean_absolute_error(y_test_future, y_pred_future)
r2 = xgb_model.score(X_test_scaled, y_test_future)

print(f"\n--- Enhanced Model Performance ---")
print(f"RMSE: {rmse:.3f}")
print(f"MAE: {mae:.3f}")
print(f"R² Score: {r2:.3f}")

# Bias analysis
prediction_bias = y_pred_future - y_test_future
over_provision = (prediction_bias > 0).sum()
under_provision = (prediction_bias < 0).sum()

print(f"\nOverprovisioning: {over_provision} ({over_provision/len(y_test_future)*100:.1f}%)")
print(f"Underprovisioning: {under_provision} ({under_provision/len(y_test_future)*100:.1f}%)")
print(f"Average bias: {np.mean(prediction_bias):.3f}")

# --- 9. Enhanced Visualization with Zoomed Plot ---
fig, axes = plt.subplots(3, 2, figsize=(18, 16))

# Plot 1: Time-shifted predictions (full timeline)
test_timestamps = test_df.index
axes[0, 0].plot(test_timestamps, y_test_current, label='Current Actual', linewidth=2, alpha=0.8)
axes[0, 0].plot(test_timestamps, y_test_future, label=f'Future Actual (+{ANTICIPATION_MINUTES}min)', linewidth=2, alpha=0.8)
axes[0, 0].plot(test_timestamps, y_pred_future, label=f'Predicted Future (+{ANTICIPATION_MINUTES}min)', 
                linewidth=2, alpha=0.8, linestyle='--')
axes[0, 0].set_title(f'Time-Shifted Predictions ({ANTICIPATION_MINUTES}-minute anticipation)')
axes[0, 0].set_xlabel('Timestamp')
axes[0, 0].set_ylabel('Max Concurrent Tasks')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Plot 2: Zoomed view showing time-shift effect clearly
# Select a specific day with peaks - manually chosen for demonstration
start_date = '2008-08-8'  # Manually selected day with good activity
end_date = '2008-08-15'    # 24-hour window

# Filter data for this specific day
day_mask = (test_timestamps >= start_date) & (test_timestamps < end_date)
if day_mask.sum() > 5:  # If we have data for this day
    day_indices = np.where(day_mask)[0]
    day_timestamps = test_timestamps[day_mask]
    day_current = y_test_current[day_mask]
    day_pred_future = y_pred_future[day_mask]
    
    # Plot the actual time-shifted effect
    axes[0, 1].plot(day_timestamps, day_current, 
                    label='Current Actual', linewidth=3, alpha=0.9, marker='o', markersize=6, color='blue')
    
    # Plot predicted future but shift it BACKWARD in time to show the anticipation effect
    shifted_timestamps = day_timestamps - pd.Timedelta(minutes=ANTICIPATION_MINUTES)
    axes[0, 1].plot(shifted_timestamps, day_pred_future, 
                    label=f'Predicted Future (displayed -{ANTICIPATION_MINUTES}min earlier)', 
                    linewidth=3, alpha=0.9, marker='^', markersize=6, color='green', linestyle='--')
    
    # Add some annotations to show the shift effect
    mid_point = len(day_timestamps) // 2
    if mid_point < len(day_timestamps):
        current_time = day_timestamps[mid_point]
        shifted_time = shifted_timestamps[mid_point]
        current_value = day_current.iloc[mid_point]
        
        # Draw arrow showing the time shift
        axes[0, 1].annotate('', 
                           xy=(current_time, current_value), 
                           xytext=(shifted_time, current_value),
                           arrowprops=dict(arrowstyle='<->', color='red', alpha=0.8, lw=2))
        
        # Add text showing the shift amount
        mid_time = shifted_time + (current_time - shifted_time) / 2
        axes[0, 1].text(mid_time, current_value + 1, f'{ANTICIPATION_MINUTES} min\nanticipation', 
                       fontsize=10, ha='center', va='bottom', 
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.8))
    
    axes[0, 1].set_title(f'Daily View: Time-Shift Anticipation ({start_date})')
    
else:
    # Fallback to a smaller sample if the specific date doesn't exist
    zoom_start = int(len(test_timestamps) * 0.3)
    zoom_samples = 50
    zoom_end = zoom_start + zoom_samples
    zoom_range = slice(zoom_start, zoom_end)
    day_timestamps = test_timestamps[zoom_range]
    day_current = y_test_current[zoom_range]
    day_pred_future = y_pred_future[zoom_range]
    
    axes[0, 1].plot(day_timestamps, day_current, 
                    label='Current Actual', linewidth=3, alpha=0.9, marker='o', markersize=6, color='blue')
    
    # Shift predictions backward in time for visualization
    shifted_timestamps = day_timestamps - pd.Timedelta(minutes=ANTICIPATION_MINUTES)
    axes[0, 1].plot(shifted_timestamps, day_pred_future, 
                    label=f'Predicted Future (displayed -{ANTICIPATION_MINUTES}min earlier)', 
                    linewidth=3, alpha=0.9, marker='^', markersize=6, color='green', linestyle='--')
    
    axes[0, 1].set_title(f'Sample View: Time-Shift Anticipation')

axes[0, 1].set_xlabel('Timestamp')
axes[0, 1].set_ylabel('Max Concurrent Tasks')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)
axes[0, 1].tick_params(axis='x', rotation=45)

# Plot 3: Prediction accuracy
axes[1, 0].scatter(y_test_future, y_pred_future, alpha=0.6, s=30)
axes[1, 0].plot([y_test_future.min(), y_test_future.max()], 
                [y_test_future.min(), y_test_future.max()], 'r--', lw=2)
axes[1, 0].set_title('Predicted vs Actual (Future)')
axes[1, 0].set_xlabel('Actual Future Tasks')
axes[1, 0].set_ylabel('Predicted Future Tasks')
axes[1, 0].grid(True, alpha=0.3)

# Plot 4: Detailed time-shift alignment view
sample_range = slice(0, min(10, len(test_timestamps)))  # First 50 samples for clarity
axes[1, 1].plot(test_timestamps[sample_range], y_test_current[sample_range], 
                'o-', label='Current Actual', linewidth=2, markersize=6, alpha=0.8, color='blue')

# Shift the prediction timeline to show alignment
shifted_timestamps = test_timestamps[sample_range] - pd.Timedelta(minutes=ANTICIPATION_MINUTES)
axes[1, 1].plot(shifted_timestamps, y_pred_future[sample_range], 
                's--', label=f'Prediction', 
                linewidth=2, markersize=6, alpha=0.8, color='red')

axes[1, 1].set_title('Time Alignment: Current vs Future Predictions')
axes[1, 1].set_xlabel('Timestamp')
axes[1, 1].set_ylabel('Max Concurrent Tasks')
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# --- 10. Enhanced Resharding Analysis ---
def map_threads_to_shards(thread_count):
    """Map thread count to required shards"""
    if thread_count <= 6: return 1
    elif thread_count <= 8: return 2
    elif thread_count <= 10: return 3
    elif thread_count <= 12: return 4
    elif thread_count == 13: return 5
    elif thread_count == 14: return 6
    else: return 7  # 6+ shards

# Shard analysis
current_shards = np.array([map_threads_to_shards(t) for t in y_test_current[:len(y_pred_future)]])
predicted_future_shards = np.array([map_threads_to_shards(t) for t in y_pred_future])
actual_future_shards = np.array([map_threads_to_shards(t) for t in y_test_future])

# Resharding triggers with refined thresholds
should_trigger = predicted_future_shards > current_shards
actually_needed = actual_future_shards > current_shards
correct_triggers = should_trigger == actually_needed

# Enhanced analysis
shard_increases_predicted = predicted_future_shards - current_shards
shard_increases_actual = actual_future_shards - current_shards

# Safety analysis: focus on avoiding critical underprovisioning
critical_underprovisioning = (predicted_future_shards < actual_future_shards) & (actual_future_shards > current_shards)
false_negatives = actually_needed & ~should_trigger

print(f"\n--- Enhanced Resharding Analysis ---")
print(f"Correct trigger decisions: {correct_triggers.sum()} ({correct_triggers.mean()*100:.1f}%)")
print(f"Critical underprovisioning events: {critical_underprovisioning.sum()}")
print(f"Missed resharding opportunities: {false_negatives.sum()}")

if should_trigger.sum() > 0:
    true_positives = (should_trigger & actually_needed).sum()
    false_positives = (should_trigger & ~actually_needed).sum()
    precision = true_positives / should_trigger.sum()
    print(f"Precision (correct when triggered): {precision:.3f}")
    print(f"False positives (unnecessary triggers): {false_positives}")

if actually_needed.sum() > 0:
    recall = true_positives / actually_needed.sum()
    print(f"Recall (caught needed resharding): {recall:.3f}")
else:
    print(f"Recall: No resharding was actually needed in test period")

print(f"\n--- Final Summary ---")
print(f"✓ Model predicts {ANTICIPATION_MINUTES} minutes ahead")
print(f"✓ Provides {ANTICIPATION_MINUTES - RESHARDING_DELAY_MINUTES}-minute buffer for {RESHARDING_DELAY_MINUTES}-minute resharding")
print(f"✓ Overprovisioning bias: {over_provision/len(y_test_future)*100:.1f}% vs {under_provision/len(y_test_future)*100:.1f}% underprovisioning")
print(f"✓ Resharding trigger accuracy: {correct_triggers.mean()*100:.1f}%")

# Model interpretation
feature_importance = pd.DataFrame({
    'feature': FEATURES,
    'importance': xgb_model.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\n--- Top 10 Most Important Features ---")
for i, (_, row) in enumerate(feature_importance.head(10).iterrows()):
    print(f"{i+1:2d}. {row['feature']}: {row['importance']:.4f}")

# Check if shard-aware features are being used
shard_features = [f for f in FEATURES if 'shard' in f.lower() or 'boundary' in f.lower()]
shard_importance = feature_importance[feature_importance['feature'].isin(shard_features)]['importance'].sum()
print(f"\nShard-aware features total importance: {shard_importance:.4f}")
if shard_importance > 0.1:
    print("✓ Model is effectively using shard-aware features")
else:
    print("⚠ Shard-aware features have low importance")