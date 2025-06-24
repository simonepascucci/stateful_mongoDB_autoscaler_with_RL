# -*- coding: utf-8 -*-
"""LSTM Model with Valley-Filling Smoothing that Preserves Peak Heights

Key approach:
1. Identify peaks and valleys
2. Fill valleys between close peaks WITHOUT reducing peak heights
3. Train and evaluate on smoothed values
4. Simple LSTM architecture for better generalization
"""

import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import joblib
from datetime import datetime, timedelta
from scipy.signal import find_peaks
from scipy.interpolate import interp1d

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-whitegrid')

# Set random seeds
torch.manual_seed(42)
np.random.seed(42)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# --- 1. Load Data ---
try:
    df = pd.read_csv('./datasets/combined_trace_processed.csv', index_col=0)
    print("Successfully loaded combined_trace_processed.csv")
except FileNotFoundError:
    try:
        df = pd.read_csv('./datasets/askalon_ee_trace_processed.csv', sep=';', index_col=0)
        print("Loaded askalon_ee_trace_processed.csv instead")
    except FileNotFoundError:
        print("Error: No dataset found.")
        exit()

# Convert timestamp and set index
df['ts_submit_dt'] = pd.to_datetime(df['ts_submit'], unit='ms')
df = df.set_index('ts_submit_dt')
df = df.sort_index()

# Select columns
feature_columns = ['aggregation_queries_count', 'total_queries_count', 
                  'standard_queries_count', 'first_name_queries_count', 
                  'last_name_queries_count', 'country_queries_count', 
                  'other_queries_count']
target_column = 'max_concurrent_tasks'

df = df[feature_columns + [target_column]]

print("\n--- Original Data Overview ---")
print(f"Data shape: {df.shape}")
print(f"Date range: {df.index.min()} to {df.index.max()}")
print(f"Max concurrent tasks range: [{df[target_column].min()}, {df[target_column].max()}]")

# --- 2. Valley-Filling Smoothing Function ---
def valley_filling_smooth(df, target_col='max_concurrent_tasks', time_threshold_minutes=15, min_peak_height=5):
    """
    Fill valleys between peaks that are within time_threshold_minutes
    WITHOUT reducing peak heights
    """
    df_smooth = df.copy()
    values = df[target_col].values.copy()
    timestamps = df.index
    
    # Find all peaks using scipy
    peaks, properties = find_peaks(values, height=min_peak_height, distance=1)
    
    if len(peaks) < 2:
        df_smooth[f'{target_col}_smoothed'] = values
        return df_smooth
    
    # Process each pair of consecutive peaks
    smoothed_values = values.copy()
    
    for i in range(len(peaks) - 1):
        peak1_idx = peaks[i]
        peak2_idx = peaks[i + 1]
        
        # Check time difference
        time_diff = timestamps[peak2_idx] - timestamps[peak1_idx]
        
        if time_diff <= timedelta(minutes=time_threshold_minutes):
            # Find the valley between these peaks
            valley_indices = range(peak1_idx + 1, peak2_idx)
            
            if len(valley_indices) > 0:
                # Get the minimum value between peaks
                valley_min = min(values[peak1_idx + 1:peak2_idx])
                
                # Only fill if valley is significantly lower than peaks
                peak1_height = values[peak1_idx]
                peak2_height = values[peak2_idx]
                avg_peak_height = (peak1_height + peak2_height) / 2
                
                if valley_min < avg_peak_height * 0.5:  # Valley is less than 50% of average peak
                    # Create a smooth curve between peaks
                    # Using a simple approach: maintain a minimum level
                    min_fill_level = max(valley_min, avg_peak_height * 0.3)  # At least 30% of peak height
                    
                    for idx in valley_indices:
                        # Linear interpolation of the minimum level
                        progress = (idx - peak1_idx) / (peak2_idx - peak1_idx)
                        
                        # Create a smooth curve that respects both peaks
                        # Use a parabolic curve that dips in the middle
                        curve_factor = 4 * progress * (1 - progress)  # Parabola peaking at 0.5
                        interpolated_min = peak1_height * (1 - progress) + peak2_height * progress
                        target_value = interpolated_min * (1 - 0.7 * curve_factor)  # Dip up to 70% at middle
                        
                        # Only raise valleys, never lower peaks
                        smoothed_values[idx] = max(smoothed_values[idx], target_value, min_fill_level)
    
    # Ensure we preserve all original peaks
    for peak_idx in peaks:
        smoothed_values[peak_idx] = values[peak_idx]
    
    # Final clipping
    smoothed_values = np.clip(smoothed_values, 1, 36)
    
    df_smooth[f'{target_col}_smoothed'] = smoothed_values
    
    return df_smooth

# Apply valley-filling smoothing
df_smoothed = valley_filling_smooth(df, 'max_concurrent_tasks', time_threshold_minutes=15, min_peak_height=5)

# Calculate statistics
print(f"\n--- Smoothing Statistics ---")
print(f"Original mean: {df['max_concurrent_tasks'].mean():.2f}")
print(f"Smoothed mean: {df_smoothed['max_concurrent_tasks_smoothed'].mean():.2f}")
print(f"Original std: {df['max_concurrent_tasks'].std():.2f}")
print(f"Smoothed std: {df_smoothed['max_concurrent_tasks_smoothed'].std():.2f}")

# Check peak preservation
original_peaks = df['max_concurrent_tasks'].values
smoothed_peaks = df_smoothed['max_concurrent_tasks_smoothed'].values
peak_preservation = np.sum(smoothed_peaks >= original_peaks) / len(original_peaks) * 100
print(f"Peak preservation: {peak_preservation:.1f}% of values maintained or increased")

# Visualize the smoothing effect
fig, axes = plt.subplots(2, 1, figsize=(20, 12))

# Full view
sample_start = 0
sample_end = 2000
axes[0].plot(df.index[sample_start:sample_end], 
             df['max_concurrent_tasks'].values[sample_start:sample_end], 
             label='Original', alpha=0.7, linewidth=1.5, color='blue')
axes[0].plot(df_smoothed.index[sample_start:sample_end], 
             df_smoothed['max_concurrent_tasks_smoothed'].values[sample_start:sample_end], 
             label='Valley-Filled', alpha=0.8, linewidth=2, color='red')
axes[0].set_title('Valley-Filling Smoothing Effect (First 2000 samples)', fontsize=14)
axes[0].set_xlabel('Timestamp')
axes[0].set_ylabel('Max Concurrent Tasks')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Zoomed view to show detail
zoom_start = 1200
zoom_end = 1400
axes[1].plot(df.index[zoom_start:zoom_end], 
             df['max_concurrent_tasks'].values[zoom_start:zoom_end], 
             label='Original', alpha=0.7, linewidth=2, marker='o', markersize=4, color='blue')
axes[1].plot(df_smoothed.index[zoom_start:zoom_end], 
             df_smoothed['max_concurrent_tasks_smoothed'].values[zoom_start:zoom_end], 
             label='Valley-Filled', alpha=0.8, linewidth=2, marker='s', markersize=4, color='red')
axes[1].set_title('Valley-Filling Detail (Zoomed View)', fontsize=14)
axes[1].set_xlabel('Timestamp')
axes[1].set_ylabel('Max Concurrent Tasks')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# --- 3. Simple Feature Engineering ---
def create_simple_features(df):
    """Create simple but effective features"""
    df_feat = df.copy()
    
    # Basic ratios
    df_feat['aggregation_ratio'] = df_feat['aggregation_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['standard_ratio'] = df_feat['standard_queries_count'] / (df_feat['total_queries_count'] + 1)
    df_feat['complex_ratio'] = (df_feat['aggregation_queries_count'] + df_feat['country_queries_count']) / (df_feat['total_queries_count'] + 1)
    
    # Simple moving averages
    for window in [5, 10]:
        df_feat[f'total_queries_ma{window}'] = df_feat['total_queries_count'].rolling(window, min_periods=1).mean()
        df_feat[f'complex_queries_ma{window}'] = (df_feat['aggregation_queries_count'] + df_feat['country_queries_count']).rolling(window, min_periods=1).mean()
    
    # Query intensity
    df_feat['query_intensity'] = df_feat['total_queries_count'].rolling(5, min_periods=1).sum()
    
    # Query diversity
    query_cols = [c for c in feature_columns if c != 'total_queries_count']
    df_feat['active_query_types'] = (df_feat[query_cols] > 0).sum(axis=1)
    
    # Recent changes
    df_feat['total_queries_diff'] = df_feat['total_queries_count'].diff().fillna(0)
    df_feat['is_query_surge'] = (df_feat['total_queries_diff'] > df_feat['total_queries_count'].rolling(10, min_periods=1).std() * 2).astype(int)
    
    return df_feat

# Apply feature engineering
df_featured = create_simple_features(df_smoothed)
df_featured = df_featured.fillna(0)

# Use smoothed target for both training and evaluation
target_column_smoothed = 'max_concurrent_tasks_smoothed'

# Define features
feature_list = [col for col in df_featured.columns if col not in ['max_concurrent_tasks', 'max_concurrent_tasks_smoothed']]
print(f"\n--- Features ---")
print(f"Total features: {len(feature_list)}")
print(f"Features: {feature_list[:5]}... (first 5)")

# --- 4. Create Sequences ---
def create_sequences(features, target, seq_length=30):
    """Create sequences for LSTM"""
    X, y = [], []
    
    for i in range(len(features) - seq_length):
        X.append(features[i:i + seq_length])
        y.append(target[i + seq_length])
    
    return np.array(X), np.array(y)

# --- 5. Train-Test Split ---
split_ratio = 0.85
split_idx = int(len(df_featured) * split_ratio)

train_df = df_featured[:split_idx]
test_df = df_featured[split_idx:]

print(f"\n--- Train-Test Split ---")
print(f"Train samples: {len(train_df)}")
print(f"Test samples: {len(test_df)}")
print(f"Train peaks (>15): {(train_df[target_column_smoothed] > 15).sum()}")
print(f"Test peaks (>15): {(test_df[target_column_smoothed] > 15).sum()}")

# Scale features
scaler = StandardScaler()
train_features = scaler.fit_transform(train_df[feature_list])
test_features = scaler.transform(test_df[feature_list])

# Get smoothed targets (no scaling - direct prediction)
train_target = train_df[target_column_smoothed].values
test_target = test_df[target_column_smoothed].values

# Create sequences
SEQ_LENGTH = 30
X_train, y_train = create_sequences(train_features, train_target, SEQ_LENGTH)
X_test, y_test = create_sequences(test_features, test_target, SEQ_LENGTH)

print(f"\n--- Sequence Shapes ---")
print(f"X_train: {X_train.shape}, y_train: {y_train.shape}")
print(f"X_test: {X_test.shape}, y_test: {y_test.shape}")

# --- 6. PyTorch Dataset ---
class TimeSeriesDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X)
        self.y = torch.FloatTensor(y)
    
    def __len__(self):
        return len(self.X)
    
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

# Create datasets
train_dataset = TimeSeriesDataset(X_train, y_train)
test_dataset = TimeSeriesDataset(X_test, y_test)

BATCH_SIZE = 64
train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE, shuffle=False)

# --- 7. Simple LSTM Model ---
class SimpleLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, dropout=0.2):
        super(SimpleLSTM, self).__init__()
        
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0,
            bidirectional=False  # Keep it simple
        )
        
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, 1)
        )
        
    def forward(self, x):
        # LSTM
        lstm_out, (hidden, _) = self.lstm(x)
        
        # Use last hidden state
        out = hidden[-1]
        
        # Fully connected layers
        out = self.fc(out)
        
        return out

# --- 8. Training ---
# Model parameters
INPUT_SIZE = len(feature_list)
HIDDEN_SIZE = 128
NUM_LAYERS = 2
LEARNING_RATE = 0.001
NUM_EPOCHS = 100
PATIENCE = 15

# Initialize model
model = SimpleLSTM(
    input_size=INPUT_SIZE,
    hidden_size=HIDDEN_SIZE,
    num_layers=NUM_LAYERS,
    dropout=0.2
).to(device)

# Standard loss and optimizer
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)

# Training loop
print("\n--- Training LSTM on Valley-Filled Data ---")
train_losses = []
val_losses = []
best_val_loss = float('inf')
patience_counter = 0

for epoch in range(NUM_EPOCHS):
    # Training
    model.train()
    train_loss = 0
    for X_batch, y_batch in train_loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        
        optimizer.zero_grad()
        outputs = model(X_batch)
        loss = criterion(outputs.squeeze(), y_batch)
        loss.backward()
        
        # Gradient clipping
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        
        optimizer.step()
        train_loss += loss.item()
    
    # Validation
    model.eval()
    val_loss = 0
    val_predictions = []
    val_actuals = []
    
    with torch.no_grad():
        for X_batch, y_batch in test_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            outputs = model(X_batch)
            loss = criterion(outputs.squeeze(), y_batch)
            val_loss += loss.item()
            
            val_predictions.extend(outputs.cpu().numpy())
            val_actuals.extend(y_batch.cpu().numpy())
    
    avg_train_loss = train_loss / len(train_loader)
    avg_val_loss = val_loss / len(test_loader)
    
    train_losses.append(avg_train_loss)
    val_losses.append(avg_val_loss)
    
    # Calculate validation MAE for monitoring
    val_mae = mean_absolute_error(val_actuals, val_predictions)
    
    # Learning rate scheduling
    scheduler.step(avg_val_loss)
    
    # Early stopping
    if avg_val_loss < best_val_loss:
        best_val_loss = avg_val_loss
        patience_counter = 0
        torch.save(model.state_dict(), './model/best_lstm_valley_filled.pth')
    else:
        patience_counter += 1
    
    if (epoch + 1) % 10 == 0:
        print(f'Epoch [{epoch+1}/{NUM_EPOCHS}], Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}, Val MAE: {val_mae:.3f}')
    
    if patience_counter >= PATIENCE:
        print(f"Early stopping at epoch {epoch+1}")
        break

# Load best model
model.load_state_dict(torch.load('./model/best_lstm_valley_filled.pth'))

# --- 9. Evaluation ---
model.eval()
predictions = []
actuals = []

with torch.no_grad():
    for X_batch, y_batch in test_loader:
        X_batch = X_batch.to(device)
        outputs = model(X_batch)
        predictions.extend(outputs.cpu().numpy())
        actuals.extend(y_batch.numpy())

predictions = np.array(predictions).flatten()
actuals = np.array(actuals).flatten()

# Clip and round predictions
predictions = np.clip(predictions, 1, 36)
predictions_final = np.round(predictions).astype(int)
actuals_final = np.round(actuals).astype(int)

# Calculate metrics
rmse = np.sqrt(mean_squared_error(actuals_final, predictions_final))
mae = mean_absolute_error(actuals_final, predictions_final)
r2 = r2_score(actuals_final, predictions_final)

print(f"\n--- Model Performance (on smoothed values) ---")
print(f"RMSE: {rmse:.3f}")
print(f"MAE: {mae:.3f}")
print(f"R² Score: {r2:.3f}")

# Detailed performance analysis
errors = np.abs(actuals_final - predictions_final)
print(f"\n--- Detailed Performance Analysis ---")
print(f"Mean Absolute Error: {np.mean(errors):.3f}")
print(f"Median Absolute Error: {np.median(errors):.3f}")
print(f"90th Percentile Error: {np.percentile(errors, 90):.3f}")
print(f"Max Error: {np.max(errors):.3f}")

# Performance by load level
print(f"\n--- Performance by Load Level (Smoothed Values) ---")
for min_val, max_val, label in [(1, 5, "Low"), (6, 15, "Medium"), (16, 36, "High")]:
    mask = (actuals_final >= min_val) & (actuals_final <= max_val)
    if mask.sum() > 0:
        level_mae = mean_absolute_error(actuals_final[mask], predictions_final[mask])
        level_rmse = np.sqrt(mean_squared_error(actuals_final[mask], predictions_final[mask]))
        print(f"{label} Load ({min_val}-{max_val}): MAE={level_mae:.3f}, RMSE={level_rmse:.3f}, n={mask.sum()}")

# --- 10. Visualization ---
fig, axes = plt.subplots(3, 2, figsize=(20, 18))

# Plot 1: Time series comparison (full test set)
test_idx = test_df.index[SEQ_LENGTH:SEQ_LENGTH + len(predictions_final)]
axes[0, 0].plot(test_idx, actuals_final, label='Actual (Smoothed)', alpha=0.8, linewidth=2, color='blue')
axes[0, 0].plot(test_idx, predictions_final, label='Predicted', alpha=0.8, linewidth=2, color='red')
axes[0, 0].set_title('LSTM Predictions on Valley-Filled Data', fontsize=14)
axes[0, 0].set_xlabel('Timestamp')
axes[0, 0].set_ylabel('Max Concurrent Tasks')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Plot 2: Zoomed view showing detail
zoom_samples = 200
zoom_start = len(predictions_final) // 2
zoom_end = zoom_start + zoom_samples
axes[0, 1].plot(test_idx[zoom_start:zoom_end], actuals_final[zoom_start:zoom_end], 
                label='Actual', alpha=0.8, linewidth=2, marker='o', markersize=4, color='blue')
axes[0, 1].plot(test_idx[zoom_start:zoom_end], predictions_final[zoom_start:zoom_end], 
                label='Predicted', alpha=0.8, linewidth=2, marker='s', markersize=4, color='red')
axes[0, 1].set_title('Predictions Detail (Zoomed)', fontsize=14)
axes[0, 1].set_xlabel('Timestamp')
axes[0, 1].set_ylabel('Max Concurrent Tasks')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# Plot 3: Scatter plot
scatter = axes[1, 0].scatter(actuals_final, predictions_final, alpha=0.6, s=30, 
                            c=actuals_final, cmap='viridis')
axes[1, 0].plot([1, 36], [1, 36], 'r--', lw=2)
axes[1, 0].set_title('Predicted vs Actual')
axes[1, 0].set_xlabel('Actual')
axes[1, 0].set_ylabel('Predicted')
axes[1, 0].set_xlim(0, 40)
axes[1, 0].set_ylim(0, 40)
axes[1, 0].grid(True, alpha=0.3)
plt.colorbar(scatter, ax=axes[1, 0])

# Plot 4: Residuals
axes[1, 1].scatter(predictions_final, errors, alpha=0.6, c=actuals_final, cmap='viridis')
axes[1, 1].axhline(y=0, color='r', linestyle='--')
axes[1, 1].set_title('Absolute Errors vs Predicted')
axes[1, 1].set_xlabel('Predicted')
axes[1, 1].set_ylabel('Absolute Error')
axes[1, 1].grid(True, alpha=0.3)

# Plot 5: Training history
axes[2, 0].plot(train_losses, label='Train Loss', alpha=0.8)
axes[2, 0].plot(val_losses, label='Val Loss', alpha=0.8)
axes[2, 0].set_title('Training History')
axes[2, 0].set_xlabel('Epoch')
axes[2, 0].set_ylabel('Loss')
axes[2, 0].legend()
axes[2, 0].grid(True, alpha=0.3)

# Plot 6: Error distribution by load level
load_levels = []
mae_values = []
sample_counts = []

for min_val, max_val, label in [(1, 5, "Low"), (6, 10, "Med-Low"), (11, 15, "Med-High"), 
                                (16, 25, "High"), (26, 36, "Peak")]:
    mask = (actuals_final >= min_val) & (actuals_final <= max_val)
    if mask.sum() > 0:
        load_levels.append(label)
        mae_values.append(np.mean(errors[mask]))
        sample_counts.append(mask.sum())

bars = axes[2, 1].bar(range(len(load_levels)), mae_values, alpha=0.7, color='coral')
axes[2, 1].set_xticks(range(len(load_levels)))
axes[2, 1].set_xticklabels(load_levels)
axes[2, 1].set_title('Mean Absolute Error by Load Level')
axes[2, 1].set_ylabel('MAE')
axes[2, 1].grid(True, alpha=0.3)

# Add sample counts
for i, (bar, count) in enumerate(zip(bars, sample_counts)):
    axes[2, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                   f'n={count}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.show()

# --- 11. Save Components ---
joblib.dump(scaler, './model/lstm_valley_scaler.pkl')
joblib.dump(feature_list, './model/lstm_valley_features.pkl')
joblib.dump({
    'input_size': INPUT_SIZE,
    'hidden_size': HIDDEN_SIZE,
    'num_layers': NUM_LAYERS,
    'seq_length': SEQ_LENGTH,
    'time_threshold': 15,
    'min_peak_height': 5
}, './model/lstm_valley_config.pkl')

print("\n--- Model Saved ---")
print("✓ Model: ./model/best_lstm_valley_filled.pth")
print("✓ Scaler: ./model/lstm_valley_scaler.pkl")
print("✓ Features: ./model/lstm_valley_features.pkl")
print("✓ Config: ./model/lstm_valley_config.pkl")

# --- 12. Inference Function ---
def predict_new_data(df_new, apply_smoothing=True):
    """
    Predict on new data with valley-filling smoothing
    
    Args:
        df_new: DataFrame with required columns
        apply_smoothing: Whether to apply valley-filling to input
    
    Returns:
        predictions: Array of predicted values
    """
    # Load components
    config = joblib.load('./model/lstm_valley_config.pkl')
    scaler = joblib.load('./model/lstm_valley_scaler.pkl')
    features = joblib.load('./model/lstm_valley_features.pkl')
    
    # Initialize model
    model = SimpleLSTM(
        config['input_size'], 
        config['hidden_size'], 
        config['num_layers']
    ).to(device)
    model.load_state_dict(torch.load('./model/best_lstm_valley_filled.pth'))
    model.eval()
    
    # Prepare data
    if apply_smoothing and 'max_concurrent_tasks' in df_new.columns:
        df_new = valley_filling_smooth(
            df_new, 
            time_threshold_minutes=config['time_threshold'],
            min_peak_height=config['min_peak_height']
        )
    
    # Feature engineering
    df_featured = create_simple_features(df_new)
    df_featured = df_featured.fillna(0)
    
    # Scale features
    features_scaled = scaler.transform(df_featured[features])
    
    # Create sequences
    X, _ = create_sequences(features_scaled, np.zeros(len(features_scaled)), config['seq_length'])
    
    if len(X) == 0:
        print(f"Error: Need at least {config['seq_length']} samples for prediction")
        return None
    
    # Predict
    predictions = []
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X).to(device)
        outputs = model(X_tensor)
        predictions = outputs.cpu().numpy()
    
    # Post-process
    predictions = np.clip(predictions, 1, 36).round().astype(int).flatten()
    
    return predictions

# Test the model quickly
print("\n--- Quick Test on Last 200 Samples ---")
test_sample = df_smoothed[-230:]  # Need extra for sequence length
test_preds = predict_new_data(test_sample)
if test_preds is not None:
    actual_values = test_sample['max_concurrent_tasks_smoothed'].values[-len(test_preds):]
    test_mae = mean_absolute_error(actual_values, test_preds)
    print(f"Test MAE: {test_mae:.3f}")

print("\n--- Ready for prediction ---")
print("Use predict_new_data(df) to make predictions on new data")
print("The function will automatically apply valley-filling smoothing")