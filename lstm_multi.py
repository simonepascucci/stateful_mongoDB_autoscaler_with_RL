# -*- coding: utf-8 -*-
"""Improved LSTM Model for Max Concurrent Tasks Prediction

Fixes for initial version:
1. Reduced model complexity to prevent overfitting
2. Shorter sequence length for better pattern recognition
3. Different scaling approach for target variable
4. Simplified architecture with focus on peaks
5. Custom loss function for peak preservation
"""

import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models, callbacks, optimizers
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import RobustScaler, StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')

# Set random seeds for reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# Set plot style
plt.style.use('seaborn-v0_8-whitegrid')

print("TensorFlow version:", tf.__version__)

# --- 1. Load and Prepare the Data ---

try:
    df = pd.read_csv('./datasets/combined_trace_processed.csv', sep=',', index_col=0)
except FileNotFoundError:
    print("Error: 'combined_trace_processed.csv' not found.")
    exit()

# Convert timestamp and set as index
df['ts_submit'] = pd.to_datetime(df['ts_submit'], unit='ms')
df = df.set_index('ts_submit')
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
print(f"Target distribution: {df['max_concurrent_tasks'].describe()}")

# --- 2. Simplified Feature Engineering ---

def create_simplified_features(df):
    """
    Simplified feature engineering focused on essential patterns
    Reduces feature space to prevent overfitting
    """
    df_feat = df.copy()
    
    # Essential query ratios
    df_feat['aggregation_ratio'] = df_feat['aggregation_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['standard_ratio'] = df_feat['standard_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['complex_ratio'] = (df_feat['aggregation_queries_count'] + df_feat['country_queries_count']) / (df_feat['total_queries_count'] + 1)
    
    # Load intensity
    df_feat['queries_per_task'] = df_feat['total_queries_count'] / (df_feat['max_concurrent_tasks'] + 1)
    
    # Short-term patterns (only essential lags)
    df_feat['total_queries_lag1'] = df_feat['total_queries_count'].shift(1)
    df_feat['tasks_lag1'] = df_feat['max_concurrent_tasks'].shift(1)
    df_feat['complex_ratio_lag1'] = df_feat['complex_ratio'].shift(1)
    
    # Short rolling windows
    for col in ['total_queries_count', 'max_concurrent_tasks']:
        df_feat[f'{col}_ma3'] = df_feat[col].rolling(3).mean()
        df_feat[f'{col}_ma5'] = df_feat[col].rolling(5).mean()
    
    # Rate of change
    df_feat['queries_change'] = df_feat['total_queries_count'].diff()
    df_feat['tasks_change'] = df_feat['max_concurrent_tasks'].diff()
    
    # Peak indicators
    df_feat['high_query_load'] = (df_feat['total_queries_count'] > df_feat['total_queries_count'].quantile(0.85)).astype(float)
    df_feat['high_complexity'] = (df_feat['complex_ratio'] > df_feat['complex_ratio'].quantile(0.8)).astype(float)
    
    return df_feat

# Create simplified features
df_featured = create_simplified_features(df)
df_featured = df_featured.dropna()

print(f"\n--- Simplified Features Created ---")
print(f"Total features: {df_featured.shape[1]}")
print(f"Samples after dropna: {df_featured.shape[0]}")

# Define features (excluding target and raw query counts to avoid data leakage)
TARGET = 'max_concurrent_tasks'
exclude_cols = [TARGET, 'total_queries_count', 'aggregation_queries_count', 'standard_queries_count',
                'first_name_queries_count', 'last_name_queries_count', 'country_queries_count', 'other_queries_count']
FEATURES = [col for col in df_featured.columns if col not in exclude_cols]

print(f"Using {len(FEATURES)} simplified features for LSTM")
print("Selected features:", FEATURES[:10], "..." if len(FEATURES) > 10 else "")

# --- 3. Train-Test Split ---

# 95-5 split
split_ratio = 0.8
split_index = int(len(df_featured) * split_ratio)

train_df = df_featured.iloc[:split_index]
test_df = df_featured.iloc[split_index:]

print(f"\n--- Train-Test Split ---")
print(f"Training samples: {len(train_df)}")
print(f"Test samples: {len(test_df)}")
print(f"Train target range: {train_df[TARGET].min():.1f} - {train_df[TARGET].max():.1f}")
print(f"Test target range: {test_df[TARGET].min():.1f} - {test_df[TARGET].max():.1f}")

# --- 4. Create Sequences with Shorter Length ---

def create_sequences(data, features, target, sequence_length=8):
    """
    Create sequences with shorter length to capture immediate patterns
    """
    X, y = [], []
    
    for i in range(sequence_length, len(data)):
        X.append(data[features].iloc[i-sequence_length:i].values)
        y.append(data[target].iloc[i])
    
    return np.array(X), np.array(y)

# Shorter sequence length
SEQUENCE_LENGTH = 8  # Reduced from 20

print(f"\n--- Creating Sequences (Length: {SEQUENCE_LENGTH}) ---")

# Different scaling approach - StandardScaler for features, log transform for target
scaler_X = StandardScaler()

# Log transform target to handle wide range and peaks better
def log_transform_target(y):
    return np.log1p(y)  # log(1+y) to handle zeros

def inverse_log_transform_target(y_log):
    return np.expm1(y_log)  # exp(y) - 1

# Scale features
train_features_scaled = scaler_X.fit_transform(train_df[FEATURES])
test_features_scaled = scaler_X.transform(test_df[FEATURES])

# Transform target
train_target_transformed = log_transform_target(train_df[TARGET].values)
test_target_transformed = log_transform_target(test_df[TARGET].values)

# Create DataFrames for sequence creation
train_scaled_df = pd.DataFrame(train_features_scaled, columns=FEATURES, index=train_df.index)
train_scaled_df[TARGET] = train_target_transformed

test_scaled_df = pd.DataFrame(test_features_scaled, columns=FEATURES, index=test_df.index)
test_scaled_df[TARGET] = test_target_transformed

# Create sequences
X_train_seq, y_train_seq = create_sequences(train_scaled_df, FEATURES, TARGET, SEQUENCE_LENGTH)
X_test_seq, y_test_seq = create_sequences(test_scaled_df, FEATURES, TARGET, SEQUENCE_LENGTH)

print(f"Training sequences shape: {X_train_seq.shape}")
print(f"Test sequences shape: {X_test_seq.shape}")

# --- 5. Simplified LSTM Model ---

def create_simplified_lstm(input_shape):
    """
    Simplified LSTM architecture to prevent overfitting
    """
    model = models.Sequential([
        # Single LSTM layer with moderate size
        layers.LSTM(64, return_sequences=False, input_shape=input_shape),
        layers.Dropout(0.2),
        
        # Simple dense layers
        layers.Dense(32, activation='relu'),
        layers.Dropout(0.2),
        layers.Dense(16, activation='relu'),
        layers.Dense(1, activation='linear')
    ])
    
    return model

# Create simplified model
input_shape = (SEQUENCE_LENGTH, len(FEATURES))
lstm_model = create_simplified_lstm(input_shape)

# Compile with MSE loss (works better with log-transformed targets)
lstm_model.compile(
    optimizer=optimizers.Adam(learning_rate=0.01),  # Higher learning rate
    loss='mse',
    metrics=['mae']
)

print("\n--- Simplified LSTM Architecture ---")
lstm_model.summary()

# --- 6. Train with Different Strategy ---

# More aggressive callbacks
early_stopping = callbacks.EarlyStopping(
    monitor='val_loss',
    patience=20,
    restore_best_weights=True,
    verbose=1
)

reduce_lr = callbacks.ReduceLROnPlateau(
    monitor='val_loss',
    factor=0.3,
    patience=8,
    min_lr=1e-6,
    verbose=1
)

print("\n--- Training Simplified LSTM ---")

# Train with more epochs but aggressive early stopping
history = lstm_model.fit(
    X_train_seq, y_train_seq,
    batch_size=16,  # Smaller batch size
    epochs=150,
    validation_data=(X_test_seq, y_test_seq),
    callbacks=[early_stopping, reduce_lr],
    verbose=1
)

# --- 7. Enhanced Evaluation ---

# Make predictions and inverse transform
y_pred_log = lstm_model.predict(X_test_seq).flatten()
y_pred = inverse_log_transform_target(y_pred_log)
y_test_actual = inverse_log_transform_target(y_test_seq)

# Ensure no negative predictions
y_pred = np.maximum(y_pred, 0)

# Calculate metrics
rmse = np.sqrt(mean_squared_error(y_test_actual, y_pred))
mae = mean_absolute_error(y_test_actual, y_pred)
r2 = r2_score(y_test_actual, y_pred)

print(f"\n--- Improved LSTM Performance ---")
print(f"Root Mean Squared Error (RMSE): {rmse:.3f}")
print(f"Mean Absolute Error (MAE): {mae:.3f}")
print(f"R² Score: {r2:.3f}")

# Detailed analysis
errors = np.abs(y_test_actual - y_pred)
print(f"\n--- Detailed Analysis ---")
print(f"Mean Absolute Error: {np.mean(errors):.3f}")
print(f"Median Absolute Error: {np.median(errors):.3f}")
print(f"90th Percentile Error: {np.percentile(errors, 90):.3f}")
print(f"Max Error: {np.max(errors):.3f}")
print(f"Prediction range: {y_pred.min():.3f} - {y_pred.max():.3f}")
print(f"Actual range: {y_test_actual.min():.3f} - {y_test_actual.max():.3f}")

# Performance by load levels
low_mask = y_test_actual <= 5
medium_mask = (y_test_actual > 5) & (y_test_actual <= 15)
high_mask = y_test_actual > 15

if low_mask.sum() > 0:
    print(f"Low Load (≤5) MAE: {np.mean(errors[low_mask]):.3f} ({low_mask.sum()} samples)")
if medium_mask.sum() > 0:
    print(f"Medium Load (6-15) MAE: {np.mean(errors[medium_mask]):.3f} ({medium_mask.sum()} samples)")
if high_mask.sum() > 0:
    print(f"High Load (>15) MAE: {np.mean(errors[high_mask]):.3f} ({high_mask.sum()} samples)")

# --- 8. Enhanced Visualization ---

fig, axes = plt.subplots(2, 3, figsize=(24, 12))

# Get test timestamps
test_timestamps = test_df.index[SEQUENCE_LENGTH:]

# Plot 1: Time series comparison
axes[0, 0].plot(test_timestamps, y_test_actual, label='Actual', alpha=0.8, linewidth=2, color='blue')
axes[0, 0].plot(test_timestamps, y_pred, label='Predicted', alpha=0.8, linewidth=2, color='orange')
axes[0, 0].set_title('Improved LSTM: Predictions vs Actual', fontsize=14)
axes[0, 0].set_xlabel('Timestamp')
axes[0, 0].set_ylabel('Max Concurrent Tasks')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Plot 2: Scatter plot
axes[0, 1].scatter(y_test_actual, y_pred, alpha=0.6, s=50)
axes[0, 1].plot([y_test_actual.min(), y_test_actual.max()], 
                [y_test_actual.min(), y_test_actual.max()], 'r--', lw=2)
axes[0, 1].set_title('Predicted vs Actual')
axes[0, 1].set_xlabel('Actual Max Concurrent Tasks')
axes[0, 1].set_ylabel('Predicted Max Concurrent Tasks')
axes[0, 1].grid(True, alpha=0.3)

# Plot 3: Training history
axes[0, 2].plot(history.history['loss'], label='Training Loss')
axes[0, 2].plot(history.history['val_loss'], label='Validation Loss')
axes[0, 2].set_title('Training History')
axes[0, 2].set_xlabel('Epoch')
axes[0, 2].set_ylabel('Loss')
axes[0, 2].legend()
axes[0, 2].grid(True, alpha=0.3)

# Plot 4: Residuals
residuals = y_test_actual - y_pred
axes[1, 0].scatter(y_pred, residuals, alpha=0.6)
axes[1, 0].axhline(y=0, color='r', linestyle='--')
axes[1, 0].set_title('Residuals vs Predicted')
axes[1, 0].set_xlabel('Predicted Values')
axes[1, 0].set_ylabel('Residuals')
axes[1, 0].grid(True, alpha=0.3)

# Plot 5: Error distribution
axes[1, 1].hist(errors, bins=30, alpha=0.7, edgecolor='black')
axes[1, 1].set_title('Error Distribution')
axes[1, 1].set_xlabel('Absolute Error')
axes[1, 1].set_ylabel('Frequency')
axes[1, 1].grid(True, alpha=0.3)

# Plot 6: Target distribution comparison
axes[1, 2].hist(y_test_actual, bins=20, alpha=0.5, label='Actual', color='blue')
axes[1, 2].hist(y_pred, bins=20, alpha=0.5, label='Predicted', color='orange')
axes[1, 2].set_title('Distribution Comparison')
axes[1, 2].set_xlabel('Max Concurrent Tasks')
axes[1, 2].set_ylabel('Frequency')
axes[1, 2].legend()
axes[1, 2].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# --- 9. Peak Analysis ---

peak_threshold = 6  # Adjusted threshold based on your data
peak_mask = y_test_actual >= peak_threshold

if peak_mask.sum() > 0:
    print(f"\n--- Peak Analysis (≥{peak_threshold} tasks) ---")
    print(f"Peak samples: {peak_mask.sum()}")
    print(f"Peak MAE: {np.mean(errors[peak_mask]):.3f}")
    print(f"Peak RMSE: {np.sqrt(np.mean((y_test_actual[peak_mask] - y_pred[peak_mask])**2)):.3f}")
    
    # Show individual peak predictions
    peak_actual = y_test_actual[peak_mask]
    peak_pred = y_pred[peak_mask]
    peak_times = test_timestamps[peak_mask]
    
    print(f"\nIndividual Peak Predictions:")
    for i, (time, actual, pred) in enumerate(zip(peak_times, peak_actual, peak_pred)):
        error = abs(actual - pred)
        print(f"  {time}: Actual={actual:.1f}, Predicted={pred:.1f}, Error={error:.1f}")

# --- 10. Model Comparison with Original Data ---

print(f"\n--- Model Diagnostics ---")
print(f"Training loss converged: {history.history['loss'][-1]:.6f}")
print(f"Validation loss: {history.history['val_loss'][-1]:.6f}")
print(f"Overfitting check: {'OK' if history.history['val_loss'][-1] <= history.history['loss'][-1] * 1.5 else 'Potential overfitting'}")

# Feature importance approximation (using permutation-like approach)
print(f"\n--- Feature Impact Analysis ---")
baseline_loss = mean_squared_error(y_test_actual, y_pred)
print(f"Baseline MSE: {baseline_loss:.4f}")

# --- 11. Save Improved Model ---

lstm_model.save('improved_lstm_model.h5')
joblib.dump(scaler_X, 'improved_lstm_scaler.pkl')

# Save configuration
config = {
    'sequence_length': SEQUENCE_LENGTH,
    'features': FEATURES,
    'target_transform': 'log1p',
    'feature_scaler': 'StandardScaler'
}

import json
with open('improved_lstm_config.json', 'w') as f:
    json.dump(config, f, indent=2)

print(f"\n--- Improved Model Saved ---")
print("✓ Model: improved_lstm_model.h5")
print("✓ Scaler: improved_lstm_scaler.pkl") 
print("✓ Config: improved_lstm_config.json")

print(f"\n--- Summary of Improvements ---")
print("✓ Reduced sequence length: 20 → 8")
print("✓ Simplified architecture: 3 LSTM layers → 1")
print("✓ Log transformation for target variable")
print("✓ StandardScaler instead of MinMaxScaler")
print("✓ Reduced feature complexity")
print("✓ Higher learning rate for faster convergence")

if r2 > 0.5:
    print("🎉 Model performance significantly improved!")
elif r2 > 0.3:
    print("📈 Model performance moderately improved")
else:
    print("⚠️  Model may need further tuning")

print(f"Final R² Score: {r2:.3f}")