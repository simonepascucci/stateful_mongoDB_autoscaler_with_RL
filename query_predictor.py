# -*- coding: utf-8 -*-
"""Enhanced Multi-Target Query Count Prediction with Clean Temporal Visualization

This approach trains a single model to predict all query types simultaneously,
with clean temporal visualization showing proper time progression.
"""

import pandas as pd
import numpy as np
from sklearn.multioutput import MultiOutputRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import joblib
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Set plot style for better visualization
plt.style.use('seaborn-v0_8-whitegrid')

class MultiTargetQueryForecaster:
    """
    Multi-output regression for predicting all query types simultaneously
    """
    
    def __init__(self):
        self.model = None
        self.scaler = None
        self.feature_columns = None
        self.target_columns = [
            'total_queries_count', 'aggregation_queries_count', 
            'standard_queries_count', 'first_name_queries_count', 
            'last_name_queries_count', 'country_queries_count', 
            'other_queries_count'
        ]
        self.training_results = None
        self.train_end_date = None
    
    def create_multi_target_features(self, df):
        """Create features for multi-target prediction"""
        df_feat = df.copy()
        
        # Time-based features
        if isinstance(df_feat.index, pd.DatetimeIndex):
            df_feat['hour'] = df_feat.index.hour
            df_feat['day_of_week'] = df_feat.index.dayofweek
            df_feat['day_of_month'] = df_feat.index.day
            df_feat['hour_sin'] = np.sin(2 * np.pi * df_feat['hour'] / 24)
            df_feat['hour_cos'] = np.cos(2 * np.pi * df_feat['hour'] / 24)
            df_feat['dow_sin'] = np.sin(2 * np.pi * df_feat['day_of_week'] / 7)
            df_feat['dow_cos'] = np.cos(2 * np.pi * df_feat['day_of_week'] / 7)
        
        # Lag features for all query types
        for col in self.target_columns:
            if col in df_feat.columns:
                for lag in [1, 2, 3, 5, 10]:
                    df_feat[f'{col}_lag_{lag}'] = df_feat[col].shift(lag)
        
        # Rolling statistics for key query types
        key_queries = ['total_queries_count', 'aggregation_queries_count']
        for col in key_queries:
            if col in df_feat.columns:
                for window in [3, 5, 10, 20]:
                    df_feat[f'{col}_rolling_mean_{window}'] = df_feat[col].rolling(window).mean()
                    df_feat[f'{col}_rolling_std_{window}'] = df_feat[col].rolling(window).std()
        
        # Query ratios
        if 'total_queries_count' in df_feat.columns:
            for col in self.target_columns[1:]:
                if col in df_feat.columns:
                    df_feat[f'{col}_ratio_lag1'] = (df_feat[f'{col}_lag_1'] / 
                                                   (df_feat['total_queries_count_lag_1'] + 1))
        
        # Trend features
        for col in ['total_queries_count', 'aggregation_queries_count']:
            if col in df_feat.columns:
                for window in [5, 10, 20]:
                    df_feat[f'{col}_trend_{window}'] = df_feat[col].rolling(window).apply(
                        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == window else 0, raw=False
                    )
        
        return df_feat
    
    def train(self, df, test_size=0.2):
        """Train multi-target model with clean temporal visualization"""
        print("=== Training Multi-Target Query Forecaster ===")
        
        # Create features
        df_feat = self.create_multi_target_features(df)
        df_feat = df_feat.dropna()
        
        print(f"Dataset shape after feature engineering: {df_feat.shape}")
        print(f"Date range: {df_feat.index.min()} to {df_feat.index.max()}")
        
        # Define features and targets
        feature_cols = [col for col in df_feat.columns 
                       if col not in self.target_columns and col != 'max_concurrent_tasks' and col != 'ts_submit_dt2']
        
        available_targets = [col for col in self.target_columns if col in df_feat.columns]
        print(f"Available targets: {available_targets}")
        print(f"Total features: {len(feature_cols)}")
        
        # Temporal split for clean visualization
        split_idx = int(len(df_feat) * (1 - test_size))
        train_df = df_feat.iloc[:split_idx]
        test_df = df_feat.iloc[split_idx:]
        self.train_end_date = train_df.index[-1]
        
        print(f"\nUsing TEMPORAL split")
        print(f"Training period: {train_df.index.min()} to {train_df.index.max()}")
        print(f"Testing period: {test_df.index.min()} to {test_df.index.max()}")
        print(f"Train samples: {len(train_df)}")
        print(f"Test samples: {len(test_df)}")
        
        X_train = train_df[feature_cols]
        y_train = train_df[available_targets]
        X_test = test_df[feature_cols]
        y_test = test_df[available_targets]
        
        # Scale features
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train multi-output XGBoost with optimized hyperparameters
        print(f"\n=== Training Multi-Output XGBoost (Optimized) ===")
        base_model = xgb.XGBRegressor(
            n_estimators=2000,          # Increased trees for better learning
            max_depth=6,                # Reduced depth to prevent overfitting
            learning_rate=0.02,         # Lower learning rate for better convergence
            subsample=0.8,              # More regularization
            colsample_bytree=0.8,       # More regularization
            reg_alpha=0.5,              # Increased L1 regularization
            reg_lambda=0.5,             # Increased L2 regularization
            min_child_weight=3,         # Prevent overfitting on small samples
            gamma=0.1,                  # Minimum loss reduction for splits
            random_state=42
        )
        
        self.model = MultiOutputRegressor(base_model, n_jobs=-1)
        self.model.fit(X_train_scaled, y_train)
        self.feature_columns = feature_cols
        
        # Make predictions
        y_pred = self.model.predict(X_test_scaled)
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred = np.maximum(y_pred, 0)
        y_pred_train = np.maximum(y_pred_train, 0)
        
        # Calculate metrics
        results = {}
        print(f"\n=== Model Performance ===")
        
        for i, target in enumerate(available_targets):
            rmse = np.sqrt(mean_squared_error(y_test.iloc[:, i], y_pred[:, i]))
            mae = mean_absolute_error(y_test.iloc[:, i], y_pred[:, i])
            r2 = r2_score(y_test.iloc[:, i], y_pred[:, i])
            
            rmse_train = np.sqrt(mean_squared_error(y_train.iloc[:, i], y_pred_train[:, i]))
            r2_train = r2_score(y_train.iloc[:, i], y_pred_train[:, i])
            
            results[target] = {
                'rmse': rmse, 'mae': mae, 'r2': r2,
                'rmse_train': rmse_train, 'r2_train': r2_train,
                'predictions': y_pred[:, i],
                'actual': y_test.iloc[:, i].values,
                'test_index': y_test.index,
                'train_predictions': y_pred_train[:, i],
                'train_actual': y_train.iloc[:, i].values,
                'train_index': y_train.index
            }
            
            print(f"{target}:")
            print(f"  Test  - RMSE: {rmse:.3f}, MAE: {mae:.3f}, R²: {r2:.3f}")
            print(f"  Train - RMSE: {rmse_train:.3f}, R²: {r2_train:.3f}")
            
            if rmse_train < rmse * 0.7:
                print(f"  ⚠ Potential overfitting detected for {target}")
        
        self.training_results = results
        
        # Generate clean temporal visualizations
        self._create_clean_temporal_visualizations(available_targets)
        
        return results
    
    def _create_clean_temporal_visualizations(self, available_targets):
        """Create clean temporal visualizations with proper time ordering"""
        
        # Key targets for visualization
        key_targets = ['total_queries_count', 'aggregation_queries_count', 'standard_queries_count']
        key_targets = [t for t in key_targets if t in available_targets][:4]
        
        # Main time series plot
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        axes = axes.flatten()
        
        for i, target in enumerate(key_targets):
            if i >= 4:
                break
                
            results = self.training_results[target]
            
            # Training data
            train_data = pd.Series(results['train_actual'], index=results['train_index'])
            train_pred = pd.Series(results['train_predictions'], index=results['train_index'])
            
            # Test data
            test_data = pd.Series(results['actual'], index=results['test_index'])
            test_pred = pd.Series(results['predictions'], index=results['test_index'])
            
            # Plot training period
            axes[i].plot(train_data.index, train_data.values, 
                       label='Training Actual', alpha=0.8, color='blue', linewidth=2)
            axes[i].plot(train_pred.index, train_pred.values, 
                       label='Training Predicted', alpha=0.7, color='lightblue', linewidth=1.5)
            
            # Plot test period
            axes[i].plot(test_data.index, test_data.values, 
                       label='Test Actual', alpha=0.9, color='red', linewidth=2)
            axes[i].plot(test_pred.index, test_pred.values, 
                       label='Test Predicted', alpha=0.8, color='orange', linewidth=2)
            
            # Add train/test separator
            if self.train_end_date:
                axes[i].axvline(x=self.train_end_date, color='green', linestyle='--', 
                              linewidth=2, alpha=0.7, label='Train/Test Split')
            
            # Formatting
            axes[i].set_title(f'{target.replace("_", " ").title()}', fontsize=14, fontweight='bold')
            axes[i].set_xlabel('Date', fontsize=12)
            axes[i].set_ylabel('Query Count', fontsize=12)
            axes[i].legend(fontsize=10)
            axes[i].grid(True, alpha=0.3)
            axes[i].tick_params(axis='x', rotation=45, labelsize=10)
            
            # Add R² score
            r2 = results['r2']
            axes[i].text(0.02, 0.98, f'R² = {r2:.3f}', transform=axes[i].transAxes,
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                        fontsize=11, fontweight='bold', verticalalignment='top')
        
        # Hide empty subplots
        for i in range(len(key_targets), 4):
            axes[i].axis('off')
        
        plt.suptitle('Clean Time Series Predictions with Temporal Split', 
                     fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.show()
        
        # Performance summary
        self._plot_performance_summary(available_targets)
    
    def _plot_performance_summary(self, available_targets):
        """Plot concise performance summary"""
        metrics_data = []
        for target in available_targets:
            results = self.training_results[target]
            metrics_data.append({
                'Target': target.replace('_', ' ').title().replace('Queries Count', ''),
                'RMSE': results['rmse'],
                'R²': results['r2']
            })
        
        metrics_df = pd.DataFrame(metrics_data)
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        colors = plt.cm.Set2(np.linspace(0, 1, len(metrics_df)))
        
        # RMSE comparison
        axes[0].bar(range(len(metrics_df)), metrics_df['RMSE'], 
                   color=colors, alpha=0.8, edgecolor='white', linewidth=1)
        axes[0].set_title('Root Mean Squared Error', fontsize=14, fontweight='bold')
        axes[0].set_ylabel('RMSE', fontsize=12)
        axes[0].set_xticks(range(len(metrics_df)))
        axes[0].set_xticklabels(metrics_df['Target'], rotation=45, ha='right', fontsize=10)
        axes[0].grid(True, alpha=0.3, axis='y')
        
        # R² comparison
        axes[1].bar(range(len(metrics_df)), metrics_df['R²'], 
                   color=colors, alpha=0.8, edgecolor='white', linewidth=1)
        axes[1].set_title('R² Score', fontsize=14, fontweight='bold')
        axes[1].set_ylabel('R²', fontsize=12)
        axes[1].set_xticks(range(len(metrics_df)))
        axes[1].set_xticklabels(metrics_df['Target'], rotation=45, ha='right', fontsize=10)
        axes[1].grid(True, alpha=0.3, axis='y')
        axes[1].set_ylim(0, 1)
        
        plt.suptitle('Model Performance Summary', fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.show()
        
        # Print summary table
        print(f"\n=== Performance Summary ===")
        display_df = metrics_df.round(3)
        print(display_df.to_string(index=False))
    
    def predict(self, df, steps_ahead=1):
        """Predict query counts for next time step"""
        df_feat = self.create_multi_target_features(df)
        latest_data = df_feat.iloc[-1:][self.feature_columns]
        
        latest_scaled = self.scaler.transform(latest_data)
        predictions = self.model.predict(latest_scaled)[0]
        
        available_targets = [col for col in self.target_columns if col in df.columns]
        result = {}
        for i, target in enumerate(available_targets):
            result[target] = max(0, int(np.round(predictions[i])))
        
        return result
    
    def save(self, filepath_prefix='./model/multi_target_forecaster'):
        """Save the model"""
        import os
        os.makedirs(os.path.dirname(filepath_prefix) if os.path.dirname(filepath_prefix) else '.', exist_ok=True)
        
        joblib.dump(self.model, f'{filepath_prefix}_model.pkl')
        joblib.dump(self.scaler, f'{filepath_prefix}_scaler.pkl')
        
        import json
        with open(f'{filepath_prefix}_features.json', 'w') as f:
            json.dump(self.feature_columns, f)
        
        print(f"✓ Model saved: {filepath_prefix}")
    
    @classmethod
    def load(cls, filepath_prefix='./model/multi_target_forecaster'):
        """Load a saved model"""
        instance = cls()
        instance.model = joblib.load(f'{filepath_prefix}_model.pkl')
        instance.scaler = joblib.load(f'{filepath_prefix}_scaler.pkl')
        
        import json
        with open(f'{filepath_prefix}_features.json', 'r') as f:
            instance.feature_columns = json.load(f)
        
        return instance


# Main execution
def main():
    """Main function to demonstrate the enhanced query forecaster"""
    
    print("=== Enhanced Multi-Target Query Forecaster ===")
    
    # Load data
    try:
        df = pd.read_csv('./datasets/askalon_ee_trace_processed.csv', index_col=0)
        df['ts_submit_dt'] = pd.to_datetime(df['ts_submit_dt'])
        df = df.set_index('ts_submit_dt').sort_index()
        
        print(f"Loaded data: {df.shape}")
        print(f"Date range: {df.index.min()} to {df.index.max()}")
        
    except FileNotFoundError:
        print("Error: 'combined_trace_processed.csv' not found!")
        return
    
    # Initialize and train forecaster with temporal split
    forecaster = MultiTargetQueryForecaster()
    results = forecaster.train(df, test_size=0.2)
    
    # Save model
    forecaster.save()
    
    # Sample predictions
    print(f"\n=== Sample Predictions ===")
    sample_predictions = forecaster.predict(df)
    print("Predicted query counts for next time step:")
    for query_type, count in sample_predictions.items():
        actual_latest = df[query_type].iloc[-1] if query_type in df.columns else 0
        print(f"  {query_type}: {count} (latest actual: {actual_latest})")
    
    print(f"\n✓ Training complete with clean temporal visualization!")
    
    return forecaster, results


if __name__ == "__main__":
    forecaster, results = main()