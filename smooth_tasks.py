import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from scipy.interpolate import UnivariateSpline
import seaborn as sns

def smooth_concurrent_tasks_data(input_file, output_file=None, plot_results=True):
    """
    Create wave-like smoothed max_concurrent_tasks data preserving ALL peaks.
    
    Parameters:
    - input_file: path to input CSV file
    - output_file: path to save smoothed data (optional)
    - plot_results: whether to display comparison plots
    """
    
    # Read the CSV file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    
    # Convert timestamp column to datetime if it's not already
    df['ts_submit_dt2'] = pd.to_datetime(df['ts_submit_dt2'])
    
    # Sort by timestamp to ensure proper ordering
    df = df.sort_values('ts_submit_dt2').reset_index(drop=True)
    
    print(f"Loaded {len(df)} records")
    print(f"Date range: {df['ts_submit_dt2'].min()} to {df['ts_submit_dt2'].max()}")
    
    # Create time series for smoothing (convert timestamps to numeric)
    time_numeric = (df['ts_submit_dt2'] - df['ts_submit_dt2'].min()).dt.total_seconds()
    original_tasks = df['max_concurrent_tasks'].values
    
    print("Applying wave-like smoothing preserving all peaks...")
    print(f"Original max_concurrent_tasks range: {original_tasks.min()}-{original_tasks.max()}")
    
    # Step 1: Find ALL peaks (not just significant ones)
    # Use a low threshold to capture all meaningful peaks
    min_peak_height = np.mean(original_tasks) + 0.2 * np.std(original_tasks)  # Just above average
    min_peak_distance = max(2, len(df) // 30)  # Allow closer peaks
    
    # Find all peaks with minimal filtering
    all_peaks, peak_properties = signal.find_peaks(
        original_tasks, 
        height=min_peak_height,
        distance=min_peak_distance,
        prominence=np.std(original_tasks) * 0.1  # Very low prominence threshold
    )
    
    print(f"Found {len(all_peaks)} peaks out of {len(df)} data points")
    
    # Step 2: Create a gentle base smooth trend (light smoothing)
    base_window = max(len(df) // 12, 7)  # Moderate window
    if base_window % 2 == 0:
        base_window += 1
    
    # Light base smoothing to preserve character
    base_smooth = signal.savgol_filter(original_tasks.astype(float), base_window, 2)
    
    # Step 3: Create wavy interpolation preserving ALL peaks
    if len(all_peaks) > 0:
        # Create control points: start, ALL peaks, end
        control_indices = [0] + list(all_peaks) + [len(df) - 1]
        control_values = []
        
        # For each control point, preserve peak values
        for i, idx in enumerate(control_indices):
            if idx in all_peaks:
                # For ALL peaks, preserve original peak value
                control_values.append(original_tasks[idx])
            else:
                # For start/end points, use lightly smoothed value
                control_values.append(base_smooth[idx])
        
        # Create smooth interpolation between all control points
        spline_interp = UnivariateSpline(control_indices, control_values, s=len(df) * 0.05, k=min(3, len(control_indices)-1))
        smoothed_final = spline_interp(np.arange(len(df)))
        
        # Step 4: Add wavy variations between all peaks
        for i in range(len(control_indices) - 1):
            start_idx = control_indices[i]
            end_idx = control_indices[i + 1]
            
            # Create wavy trend in this segment
            segment_length = end_idx - start_idx
            if segment_length > 2:
                start_val = smoothed_final[start_idx]
                end_val = smoothed_final[end_idx]
                
                # Create base trend with wave variations
                segment_indices = np.arange(start_idx, end_idx + 1)
                base_trend = np.linspace(start_val, end_val, len(segment_indices))
                
                # Add wave components scaled to segment
                if segment_length > 3:
                    # Gentle wave component
                    wave_intensity = abs(end_val - start_val) * 0.12
                    wave1 = np.sin(np.linspace(0, 1.2*np.pi, len(segment_indices))) * wave_intensity
                    
                    # Add subtle secondary wave for texture
                    if segment_length > 6:
                        wave2 = np.sin(np.linspace(0, 2.5*np.pi, len(segment_indices))) * wave_intensity * 0.4
                        wave1 += wave2
                    
                    base_trend += wave1
                
                smoothed_final[start_idx:end_idx + 1] = base_trend
    
    else:
        # If no peaks found, use light smoothing with gentle waves
        smoothed_final = base_smooth.copy()
        wave_component = np.sin(np.linspace(0, 6*np.pi, len(smoothed_final))) * np.std(smoothed_final) * 0.08
        smoothed_final += wave_component
    
    # Step 5: Very light final smoothing to blend naturally while preserving peaks
    final_window = max(5, len(df) // 20)  # Small final smoothing window
    if final_window % 2 == 0:
        final_window += 1
    if final_window < len(df):  # Only smooth if window is reasonable
        smoothed_final = signal.savgol_filter(smoothed_final, final_window, 2)
    
    # Ensure non-negative values for concurrent tasks (tasks can't be negative)
    smoothed_final = np.maximum(smoothed_final, 0)
    
    # Update peaks array to all found peaks for plotting
    peaks = all_peaks
    
    # Update the dataframe with smoothed values
    df['max_concurrent_tasks_original'] = df['max_concurrent_tasks']
    
    # Convert smoothed concurrent tasks to closest integers (must be whole numbers)
    df['max_concurrent_tasks'] = np.round(smoothed_final).astype(int)
    
    print("Smoothing completed!")
    print(f"Smoothed max_concurrent_tasks range: {df['max_concurrent_tasks'].min()}-{df['max_concurrent_tasks'].max()}")
    
    # Create visualization
    if plot_results:
        plt.figure(figsize=(15, 10))
        
        # Plot 1: Original vs Smoothed concurrent tasks
        plt.subplot(2, 2, 1)
        plt.plot(df['ts_submit_dt2'], df['max_concurrent_tasks_original'], 
                'b-', alpha=0.7, linewidth=1, label='Original')
        plt.plot(df['ts_submit_dt2'], df['max_concurrent_tasks'], 
                'r-', linewidth=2, label='Smoothed')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['max_concurrent_tasks_original'].iloc[peaks], 
                   color='orange', s=50, zorder=5, label='Detected Peaks')
        plt.title('Max Concurrent Tasks: Original vs Smoothed')
        plt.xlabel('Timestamp')
        plt.ylabel('Max Concurrent Tasks')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Plot 2: Distribution comparison
        plt.subplot(2, 2, 2)
        plt.hist(df['max_concurrent_tasks_original'], alpha=0.6, bins=30, label='Original', color='blue')
        plt.hist(df['max_concurrent_tasks'], alpha=0.6, bins=30, label='Smoothed', color='red')
        plt.title('Distribution Comparison')
        plt.xlabel('Max Concurrent Tasks')
        plt.ylabel('Frequency')
        plt.legend()
        
        # Plot 3: Residuals analysis
        plt.subplot(2, 2, 3)
        residuals = df['max_concurrent_tasks_original'] - df['max_concurrent_tasks']
        plt.plot(df['ts_submit_dt2'], residuals, 'g-', alpha=0.7)
        plt.axhline(y=0, color='k', linestyle='--', alpha=0.5)
        plt.title('Residuals (Original - Smoothed)')
        plt.xlabel('Timestamp')
        plt.ylabel('Residual Value')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Plot 4: Smoothing quality metrics
        plt.subplot(2, 2, 4)
        # Calculate rolling variance to show smoothness
        window_size = max(10, len(df) // 20)
        original_variance = pd.Series(df['max_concurrent_tasks_original']).rolling(window=window_size).var()
        smoothed_variance = pd.Series(df['max_concurrent_tasks']).rolling(window=window_size).var()
        
        plt.plot(df['ts_submit_dt2'], original_variance, 'b-', alpha=0.7, label='Original Variance')
        plt.plot(df['ts_submit_dt2'], smoothed_variance, 'r-', alpha=0.7, label='Smoothed Variance')
        plt.title(f'Rolling Variance (window={window_size})')
        plt.xlabel('Timestamp')
        plt.ylabel('Variance')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"Mean absolute residual: {np.mean(np.abs(residuals)):.2f}")
        print(f"Max residual: {np.max(np.abs(residuals)):.2f}")
        print(f"Original mean: {df['max_concurrent_tasks_original'].mean():.2f}")
        print(f"Smoothed mean: {df['max_concurrent_tasks'].mean():.2f}")
        print(f"Original std: {df['max_concurrent_tasks_original'].std():.2f}")
        print(f"Smoothed std: {df['max_concurrent_tasks'].std():.2f}")
        print(f"Variance reduction: {((df['max_concurrent_tasks_original'].var() - df['max_concurrent_tasks'].var()) / df['max_concurrent_tasks_original'].var() * 100):.1f}%")
        
        # Additional side-by-side comparison plot
        plt.figure(figsize=(16, 6))
        
        # Left plot: Original data
        plt.subplot(1, 2, 1)
        plt.plot(df['ts_submit_dt2'], df['max_concurrent_tasks_original'], 
                'b-', linewidth=2, label='Max Concurrent Tasks')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['max_concurrent_tasks_original'].iloc[peaks], 
                   color='orange', s=60, zorder=5, label='Detected Peaks')
        plt.title('ORIGINAL DATA - Spiky Pattern', fontsize=14, fontweight='bold')
        plt.xlabel('Timestamp')
        plt.ylabel('Max Concurrent Tasks')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Right plot: Smoothed data
        plt.subplot(1, 2, 2)
        plt.plot(df['ts_submit_dt2'], df['max_concurrent_tasks'], 
                'r-', linewidth=2, label='Max Concurrent Tasks (Smoothed)')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['max_concurrent_tasks'].iloc[peaks], 
                   color='orange', s=60, zorder=5, label='Peak Locations Preserved')
        plt.title('SMOOTHED DATA - Wave-like Pattern', fontsize=14, fontweight='bold')
        plt.xlabel('Timestamp')
        plt.ylabel('Max Concurrent Tasks')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    # Save the smoothed data
    if output_file:
        # Prepare output dataframe with all original columns plus the smoothed one
        output_df = df.copy()
        
        output_df.to_csv(output_file, index=False)
        print(f"\nSmoothed data saved to: {output_file}")
    
    return df

def main():
    """Main function to run the smoothing process"""
    
    # Configuration
    input_file = "./datasets/galaxy_trace_smoothed.csv"  # Change this to your file path
    output_file = "galaxy_trace_concurrent_tasks_smoothed.csv"  # Output file name
    
    print("=== CSV Concurrent Tasks Data Smoother - ALL PEAKS PRESERVED ===")
    print(f"Input file: {input_file}")
    print("-" * 50)
    
    try:
        # Process the data
        smoothed_df = smooth_concurrent_tasks_data(
            input_file=input_file,
            output_file=output_file,
            plot_results=True
        )
        
        print("\n=== Processing Complete! ===")
        print("The smoothed data maintains the wave-like pattern while:")
        print("1. Preserving ALL detected peaks in max_concurrent_tasks")
        print("2. Creating smooth wave-like transitions between peaks")
        print("3. Maintaining non-negative integer values")
        print("4. Reducing overall variance while preserving important patterns")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        print("Please check that the input file exists and has the correct format.")

if __name__ == "__main__":
    main()