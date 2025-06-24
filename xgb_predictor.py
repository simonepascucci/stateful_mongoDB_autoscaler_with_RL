# -*- coding: utf-8 -*-
"""Streamlined XGBoost Model - Query Features Only with Overprovisioning Bias

Focused approach:
1. XGBoost only (best performing model)
2. Query features only (no datetime features)
3. 95-5 split to include peak values in training
4. Optimized for scalability and new dataset testing
5. Tuned for overprovisioning to avoid resource shortages
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import RobustScaler
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set plot style for better visualization
plt.style.use('seaborn-v0_8-whitegrid')

# --- 1. Load and Prepare the Data ---

try:
    #df = pd.read_csv('./datasets/combined_trace_processed.csv', sep=',', index_col=0)
    df = pd.read_csv('output_scaled_timestamps.csv', sep=',', index_col=0)
except FileNotFoundError:
    print("Error: 'askalon_ee_trace_processed.csv' not found. Please make sure the file is in the correct directory.")
    exit()

# Convert timestamp and set as index
#df['ts_submit'] = pd.to_datetime(df['ts_submit'], unit='ms')
df = df.set_index('ts_submit_dt')
df = df.sort_index()

# Keep all query-related columns
query_columns = ['max_concurrent_tasks', 'total_queries_count', 'aggregation_queries_count', 
                'standard_queries_count', 'first_name_queries_count', 'last_name_queries_count', 
                'country_queries_count', 'other_queries_count']
df = df[query_columns]

print("--- Data Overview ---")
print(f"Data shape: {df.shape}")
print(f"Date range: {df.index.min()} to {df.index.max()}")
print(f"Max concurrent tasks: {df['max_concurrent_tasks'].max()}")
print("\n--- Data Head ---")
print(df.head())
print("\n--- Data Statistics ---")
print(df.describe())

# --- 2. Query-Focused Feature Engineering ---

def create_query_features(df):
    """
    Creates comprehensive query-based features without datetime components
    Focus on query patterns, ratios, and interactions
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

df_featured = create_query_features(df)

# Drop rows with NaN values created by lag and rolling features
df_featured = df_featured.dropna()

print(f"\n--- Query Features Created ---")
print(f"Total features: {df_featured.shape[1]}")
print(f"Remaining samples after dropna: {df_featured.shape[0]}")

# --- 3. 80-20 Train-Test Split ---

split_ratio = 0.80
split_index = int(len(df_featured) * split_ratio)

train_df = df_featured.iloc[:split_index]
test_df = df_featured.iloc[split_index:]

print(f"\n--- Train-Test Split Analysis ---")
print(f"Training samples: {len(train_df)}")
print(f"Test samples: {len(test_df)}")
print(f"Train max_concurrent_tasks - Min: {train_df['max_concurrent_tasks'].min():.0f}, Max: {train_df['max_concurrent_tasks'].max():.0f}")
print(f"Test max_concurrent_tasks - Min: {test_df['max_concurrent_tasks'].min():.0f}, Max: {test_df['max_concurrent_tasks'].max():.0f}")

# Check if 36-thread peak is in training
peak_36_in_train = (train_df['max_concurrent_tasks'] >= 36).any()
print(f"Peak ≥36 threads in training set: {peak_36_in_train}")

# Define features (excluding target and original raw columns)
exclude_cols = ['max_concurrent_tasks', 'aggregation_queries_count', 'total_queries_count', 
                'standard_queries_count', 'first_name_queries_count', 'last_name_queries_count', 
                'country_queries_count', 'other_queries_count']
FEATURES = [col for col in df_featured.columns if col not in exclude_cols]
TARGET = 'max_concurrent_tasks'

print(f"Using {len(FEATURES)} query-based features for training")

# --- 4. Data Preprocessing with Overprovisioning Bias ---

X_train = train_df[FEATURES]
y_train = train_df[TARGET]
X_test = test_df[FEATURES]
y_test = test_df[TARGET]

# Apply overprovisioning bias to training targets
# Method 1: Add a small bias to higher loads
print("\n--- Applying Overprovisioning Bias to Training Data ---")
y_train_biased = y_train.copy()

# Apply very aggressive bias for critical loads where underprovisioning causes issues
# Low load (<=5): 1% bias (minimal since overprovisioning here is less critical)
# Medium load (6-15): 30% bias (very aggressive since these are important)
# High load (>15): 40% bias (extremely aggressive since underprovisioning here is critical)
low_load_mask = y_train <= 5
medium_load_mask = (y_train > 5) & (y_train <= 15)
high_load_mask = y_train > 15

y_train_biased[low_load_mask] = y_train[low_load_mask] * 1.01
y_train_biased[medium_load_mask] = y_train[medium_load_mask] * 1.30
y_train_biased[high_load_mask] = y_train[high_load_mask] * 1.40

print(f"Applied bias to training targets:")
print(f"- Low load samples: {low_load_mask.sum()} (1% bias)")
print(f"- Medium load samples: {medium_load_mask.sum()} (30% bias)")
print(f"- High load samples: {high_load_mask.sum()} (40% bias)")

# Scale features using RobustScaler
scaler_X = RobustScaler()
X_train_scaled = scaler_X.fit_transform(X_train)
X_test_scaled = scaler_X.transform(X_test)

# Convert back to DataFrames
X_train_scaled = pd.DataFrame(X_train_scaled, columns=FEATURES, index=X_train.index)
X_test_scaled = pd.DataFrame(X_test_scaled, columns=FEATURES, index=X_test.index)

# --- 5. Optimized XGBoost Model with Overprovisioning Tuning ---

# Fine-tuned XGBoost for query-based prediction with aggressive overprovisioning bias
# Further reduced regularization to allow more aggressive overprovisioning
xgb_model = xgb.XGBRegressor(
    n_estimators=3500,        # Increased for better learning
    max_depth=12,             # Increased depth for more complex patterns
    learning_rate=0.012,      # Slightly reduced for more careful learning
    subsample=0.85,           # Slight reduction for regularization
    colsample_bytree=0.85,    # Slight reduction for regularization
    colsample_bylevel=0.8,
    reg_alpha=0.001,          # Very low regularization
    reg_lambda=0.02,          # Very low regularization
    gamma=0.01,               # Very low gamma
    min_child_weight=1,       
    objective='reg:squarederror',
    random_state=42,
    early_stopping_rounds=200,
    tree_method='hist'
)

print("\n--- Training Optimized XGBoost Model with Overprovisioning Bias ---")
xgb_model.fit(
    X_train_scaled, y_train_biased,  # Using biased targets
    eval_set=[(X_train_scaled, y_train_biased), (X_test_scaled, y_test)],
    verbose=200
)

# --- 6. Model Evaluation ---

y_pred_raw = xgb_model.predict(X_test_scaled)
# Round predictions to integers since we're predicting number of threads
y_pred = np.round(y_pred_raw).astype(int)
# Ensure minimum of 1 thread (can't have 0 threads)
y_pred = np.maximum(y_pred, 1)

# Calculate error metrics
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)

print(f"\n--- Model Performance ---")
print(f"Root Mean Squared Error (RMSE): {rmse:.3f}")
print(f"Mean Absolute Error (MAE): {mae:.3f}")
print(f"R² Score: {xgb_model.score(X_test_scaled, y_test):.3f}")

# Analyze prediction bias (overprovisioning vs underprovisioning)
prediction_bias = y_pred - y_test
overprovisioning_count = (prediction_bias > 0).sum()
underprovisioning_count = (prediction_bias < 0).sum()
perfect_count = (prediction_bias == 0).sum()

print(f"\n--- Provisioning Analysis ---")
print(f"Overprovisioning cases: {overprovisioning_count} ({overprovisioning_count/len(y_test)*100:.1f}%)")
print(f"Underprovisioning cases: {underprovisioning_count} ({underprovisioning_count/len(y_test)*100:.1f}%)")
print(f"Perfect predictions: {perfect_count} ({perfect_count/len(y_test)*100:.1f}%)")
print(f"Average prediction bias: {np.mean(prediction_bias):.3f}")
print(f"Median prediction bias: {np.median(prediction_bias):.3f}")

# Detailed performance analysis
print(f"\n--- Detailed Performance Analysis ---")
errors = np.abs(y_test - y_pred)
print(f"Mean Absolute Error: {np.mean(errors):.3f}")
print(f"Median Absolute Error: {np.median(errors):.3f}")
print(f"90th Percentile Error: {np.percentile(errors, 90):.3f}")
print(f"Max Error: {np.max(errors):.3f}")

# Performance on different load levels
low_load_mask = y_test <= 5
medium_load_mask = (y_test > 5) & (y_test <= 15)
high_load_mask = y_test > 15

if low_load_mask.sum() > 0:
    low_load_mae = np.mean(np.abs(y_test[low_load_mask] - y_pred[low_load_mask]))
    low_load_bias = np.mean(y_pred[low_load_mask] - y_test[low_load_mask])
    print(f"Low Load (≤5 tasks) MAE: {low_load_mae:.3f}, Bias: {low_load_bias:.3f} ({low_load_mask.sum()} samples)")

if medium_load_mask.sum() > 0:
    medium_load_mae = np.mean(np.abs(y_test[medium_load_mask] - y_pred[medium_load_mask]))
    medium_load_bias = np.mean(y_pred[medium_load_mask] - y_test[medium_load_mask])
    print(f"Medium Load (6-15 tasks) MAE: {medium_load_mae:.3f}, Bias: {medium_load_bias:.3f} ({medium_load_mask.sum()} samples)")

if high_load_mask.sum() > 0:
    high_load_mae = np.mean(np.abs(y_test[high_load_mask] - y_pred[high_load_mask]))
    high_load_bias = np.mean(y_pred[high_load_mask] - y_test[high_load_mask])
    print(f"High Load (>15 tasks) MAE: {high_load_mae:.3f}, Bias: {high_load_bias:.3f} ({high_load_mask.sum()} samples)")

# --- 7. Enhanced Visualization and Analysis ---

fig, axes = plt.subplots(2, 3, figsize=(24, 12))

# Plot 1: Predictions vs Actual
axes[0, 0].plot(y_test.index, y_test, label='Actual', alpha=0.8, linewidth=2)
axes[0, 0].plot(y_test.index, y_pred, label='Predicted (Overprovisioning)', alpha=0.8, linewidth=2)
axes[0, 0].set_title('XGBoost Predictions vs Actual (Overprovisioning Bias)', fontsize=14)
axes[0, 0].set_xlabel('Timestamp')
axes[0, 0].set_ylabel('Max Concurrent Tasks')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Plot 2: Prediction scatter plot
axes[0, 1].scatter(y_test, y_pred, alpha=0.6, s=50)
axes[0, 1].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2, label='Perfect Prediction')
axes[0, 1].set_title('Predicted vs Actual Scatter Plot')
axes[0, 1].set_xlabel('Actual Max Concurrent Tasks')
axes[0, 1].set_ylabel('Predicted Max Concurrent Tasks')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# Plot 3: Prediction Bias Distribution
axes[0, 2].hist(prediction_bias, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
axes[0, 2].axvline(x=0, color='red', linestyle='--', linewidth=2, label='No Bias')
axes[0, 2].axvline(x=np.mean(prediction_bias), color='green', linestyle='-', linewidth=2, label=f'Mean Bias: {np.mean(prediction_bias):.3f}')
axes[0, 2].set_title('Prediction Bias Distribution')
axes[0, 2].set_xlabel('Prediction Bias (Predicted - Actual)')
axes[0, 2].set_ylabel('Frequency')
axes[0, 2].legend()
axes[0, 2].grid(True, alpha=0.3)

# Plot 4: Residuals
residuals = y_test - y_pred
axes[1, 0].scatter(y_pred, residuals, alpha=0.6)
axes[1, 0].axhline(y=0, color='r', linestyle='--')
axes[1, 0].set_title('Residuals vs Predicted')
axes[1, 0].set_xlabel('Predicted Values')
axes[1, 0].set_ylabel('Residuals')
axes[1, 0].grid(True, alpha=0.3)

# Plot 5: Top Feature Importances
feature_importance = pd.DataFrame({
    'feature': FEATURES,
    'importance': xgb_model.feature_importances_
}).sort_values('importance', ascending=True).tail(15)

axes[1, 1].barh(range(len(feature_importance)), feature_importance['importance'])
axes[1, 1].set_yticks(range(len(feature_importance)))
axes[1, 1].set_yticklabels(feature_importance['feature'], fontsize=10)
axes[1, 1].set_title('Top 15 Feature Importances')
axes[1, 1].set_xlabel('Importance')
axes[1, 1].grid(True, alpha=0.3)

# Plot 6: Overprovisioning vs Underprovisioning by Load Level
load_levels = ['Low (≤5)', 'Medium (6-15)', 'High (>15)']
over_counts = []
under_counts = []

for mask in [low_load_mask, medium_load_mask, high_load_mask]:
    if mask.sum() > 0:
        over_counts.append((y_pred[mask] > y_test[mask]).sum())
        under_counts.append((y_pred[mask] < y_test[mask]).sum())
    else:
        over_counts.append(0)
        under_counts.append(0)

x = np.arange(len(load_levels))
width = 0.35

axes[1, 2].bar(x - width/2, over_counts, width, label='Overprovisioning', color='lightgreen', alpha=0.8)
axes[1, 2].bar(x + width/2, under_counts, width, label='Underprovisioning', color='lightcoral', alpha=0.8)
axes[1, 2].set_title('Provisioning by Load Level')
axes[1, 2].set_xlabel('Load Level')
axes[1, 2].set_ylabel('Count')
axes[1, 2].set_xticks(x)
axes[1, 2].set_xticklabels(load_levels)
axes[1, 2].legend()
axes[1, 2].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# --- 8. Feature Analysis ---

print("\n--- Feature Importance Analysis ---")
top_features = feature_importance.tail(10)
print("Top 10 Most Important Features:")
for _, row in top_features.iterrows():
    print(f"  {row['feature']}: {row['importance']:.4f}")

# Query type correlation analysis
print("\n--- Query Type Correlations with max_concurrent_tasks ---")
query_types = ['total_queries_count', 'aggregation_queries_count', 'standard_queries_count', 
               'first_name_queries_count', 'last_name_queries_count', 'country_queries_count', 'other_queries_count']
for qtype in query_types:
    if qtype in df.columns:
        corr = df[qtype].corr(df['max_concurrent_tasks'])
        print(f"  {qtype}: {corr:.3f}")

# Feature category importance
print("\n--- Feature Category Analysis ---")
ratio_features = [f for f in FEATURES if 'ratio' in f]
lag_features = [f for f in FEATURES if 'lag' in f]
rolling_features = [f for f in FEATURES if 'rolling' in f]
trend_features = [f for f in FEATURES if 'trend' in f]

if ratio_features:
    ratio_importance = feature_importance[feature_importance['feature'].isin(ratio_features)]['importance'].sum()
    print(f"Total importance of ratio features: {ratio_importance:.4f}")

if lag_features:
    lag_importance = feature_importance[feature_importance['feature'].isin(lag_features)]['importance'].sum()
    print(f"Total importance of lag features: {lag_importance:.4f}")

if rolling_features:
    rolling_importance = feature_importance[feature_importance['feature'].isin(rolling_features)]['importance'].sum()
    print(f"Total importance of rolling features: {rolling_importance:.4f}")

# --- 9. Save Model and Scaler ---

# Save the trained model and scaler
joblib.dump(xgb_model, './model/xgb_overprovisioning_model.pkl')
joblib.dump(scaler_X, './model/overprovisioning_scaler.pkl')

# Save feature names for consistency
with open('./model/overprovisioning_feature_names.txt', 'w') as f:
    for feature in FEATURES:
        f.write(f"{feature}\n")

# Save bias parameters for future reference
bias_params = {
    'low_load_bias': 1.01,
    'medium_load_bias': 1.30,
    'high_load_bias': 1.40,
    'low_load_threshold': 5,
    'high_load_threshold': 15
}

import json
with open('./model/overprovisioning_bias_params.json', 'w') as f:
    json.dump(bias_params, f, indent=2)

print(f"\n--- Model Saved Successfully ---")
print("✓ Model saved as: xgb_overprovisioning_model.pkl")
print("✓ Scaler saved as: overprovisioning_scaler.pkl")
print("✓ Feature names saved as: overprovisioning_feature_names.txt")
print("✓ Bias parameters saved as: overprovisioning_bias_params.json")
print("✓ Ready for testing on new datasets")

print(f"\n--- Overprovisioning Model Summary ---")
print("✓ Model trained with very aggressive overprovisioning bias")
print("✓ Progressive bias: 1% (low), 30% (medium), 40% (high load)")
print("✓ Very low regularization for maximum overprovisioning flexibility")
print("✓ Enhanced bias analysis and visualization")
print("✓ Extremely strong bias toward overprovisioning for critical medium/high loads")

# --- 10. Additional Utility Functions ---

def apply_runtime_safety_margin(predictions, safety_margin=0.1):
    """Apply additional safety margin to predictions at runtime"""
    safe_predictions = predictions * (1 + safety_margin)
    # Round to integers and ensure minimum of 1 thread
    return np.maximum(np.round(safe_predictions).astype(int), 1)

def get_conservative_prediction(model, scaler, features, safety_margin=0.1):
    """Get conservative prediction with safety margin"""
    scaled_features = scaler.transform(features)
    prediction = model.predict(scaled_features)
    return apply_runtime_safety_margin(prediction, safety_margin)

def predict_threads(model, scaler, features, safety_margin=0.1):
    """Main prediction function that returns integer thread count"""
    scaled_features = scaler.transform(features)
    raw_prediction = model.predict(scaled_features)
    # Apply safety margin, round to integers, ensure minimum of 1
    safe_prediction = raw_prediction * (1 + safety_margin)
    return np.maximum(np.round(safe_prediction).astype(int), 1)

print(f"\n--- Additional Safety Features ---")
print("✓ Runtime safety margin function available")
print("✓ Conservative prediction wrapper function available")
print("✓ Recommended additional safety margin: 15-25% for critical workloads")
print("✓ Very aggressive bias for medium (30%) and high loads (40%) where underprovisioning is critical")
print("✓ Minimal bias for low loads (1%) to avoid resource waste")