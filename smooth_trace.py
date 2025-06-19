import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from scipy.interpolate import UnivariateSpline
import seaborn as sns

def smooth_query_data(input_file, output_file=None, plot_results=True, max_agg_ratio=0.25):
    """
    Create wave-like smoothed query count data preserving ALL peaks.
    
    Parameters:
    - input_file: path to input CSV file
    - output_file: path to save smoothed data (optional)
    - plot_results: whether to display comparison plots
    - max_agg_ratio: maximum ratio of aggregation_queries_count to total_queries_count (default: 0.25)
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
    
    # Calculate original proportions and cap at max_agg_ratio
    original_proportion = df['aggregation_queries_count'] / df['total_queries_count']
    df['agg_proportion'] = np.minimum(original_proportion, max_agg_ratio)
    
    # Report on capping
    capped_count = np.sum(original_proportion > max_agg_ratio)
    if capped_count > 0:
        print(f"Capped {capped_count} records where aggregation ratio exceeded {max_agg_ratio*100}%")
        print(f"Original max ratio: {original_proportion.max():.3f}, New max ratio: {df['agg_proportion'].max():.3f}")
    
    # Create time series for smoothing (convert timestamps to numeric)
    time_numeric = (df['ts_submit_dt2'] - df['ts_submit_dt2'].min()).dt.total_seconds()
    original_counts = df['total_queries_count'].values
    
    print("Applying wave-like smoothing preserving all peaks...")
    
    # Step 1: Find ALL peaks (not just significant ones)
    # Use a low threshold to capture all meaningful peaks
    min_peak_height = np.mean(original_counts) + 0.2 * np.std(original_counts)  # Just above average
    min_peak_distance = max(2, len(df) // 30)  # Allow closer peaks
    
    # Find all peaks with minimal filtering
    all_peaks, peak_properties = signal.find_peaks(
        original_counts, 
        height=min_peak_height,
        distance=min_peak_distance,
        prominence=np.std(original_counts) * 0.1  # Very low prominence threshold
    )
    
    print(f"Found {len(all_peaks)} peaks out of {len(df)} data points")
    
    # Step 2: Create a gentle base smooth trend (light smoothing)
    base_window = max(len(df) // 12, 7)  # Moderate window
    if base_window % 2 == 0:
        base_window += 1
    
    # Light base smoothing to preserve character
    base_smooth = signal.savgol_filter(original_counts.astype(float), base_window, 2)
    
    # Step 3: Create wavy interpolation preserving ALL peaks
    if len(all_peaks) > 0:
        # Create control points: start, ALL peaks, end
        control_indices = [0] + list(all_peaks) + [len(df) - 1]
        control_values = []
        
        # For each control point, preserve peak values
        for i, idx in enumerate(control_indices):
            if idx in all_peaks:
                # For ALL peaks, preserve original peak value
                control_values.append(original_counts[idx])
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
    
    # Update peaks array to all found peaks for plotting
    peaks = all_peaks
    
    # Update the dataframe with smoothed values
    df['total_queries_count_original'] = df['total_queries_count']
    df['aggregation_queries_count_original'] = df['aggregation_queries_count']
    
    # Convert smoothed total queries to closest integers
    df['total_queries_count'] = np.round(smoothed_final).astype(int)
    
    # Recalculate aggregation queries maintaining the capped proportion
    df['aggregation_queries_count'] = np.round(
        df['total_queries_count'] * df['agg_proportion']
    ).astype(int)
    
    # Double-check: Ensure aggregation count doesn't exceed total count OR max_agg_ratio
    max_allowed_agg = np.round(df['total_queries_count'] * max_agg_ratio).astype(int)
    df['aggregation_queries_count'] = np.minimum(
        df['aggregation_queries_count'], 
        np.minimum(df['total_queries_count'], max_allowed_agg)
    )
    
    # Final conversion to ensure both columns are integers
    df['total_queries_count'] = df['total_queries_count'].astype(int)
    df['aggregation_queries_count'] = df['aggregation_queries_count'].astype(int)
    
    print("Smoothing completed!")
    print(f"Original total queries range: {df['total_queries_count_original'].min()}-{df['total_queries_count_original'].max()}")
    print(f"Smoothed total queries range: {df['total_queries_count'].min()}-{df['total_queries_count'].max()}")
    
    # Verify aggregation ratio constraint
    final_ratio = df['aggregation_queries_count'] / df['total_queries_count']
    print(f"Final aggregation ratio range: {final_ratio.min():.3f}-{final_ratio.max():.3f}")
    print(f"Maximum allowed ratio: {max_agg_ratio}")
    
    # Create visualization
    if plot_results:
        plt.figure(figsize=(15, 10))
        
        # Plot 1: Original vs Smoothed total queries
        plt.subplot(2, 2, 1)
        plt.plot(df['ts_submit_dt2'], df['total_queries_count_original'], 
                'b-', alpha=0.7, linewidth=1, label='Original')
        plt.plot(df['ts_submit_dt2'], df['total_queries_count'], 
                'r-', linewidth=2, label='Smoothed')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['total_queries_count_original'].iloc[peaks], 
                   color='orange', s=50, zorder=5, label='Detected Peaks')
        plt.title('Total Queries Count: Original vs Smoothed')
        plt.xlabel('Timestamp')
        plt.ylabel('Total Queries Count')
        plt.legend()
        plt.xticks(rotation=45)
        
        # Plot 2: Aggregation queries comparison
        plt.subplot(2, 2, 2)
        plt.plot(df['ts_submit_dt2'], df['aggregation_queries_count_original'], 
                'b-', alpha=0.7, linewidth=1, label='Original')
        plt.plot(df['ts_submit_dt2'], df['aggregation_queries_count'], 
                'r-', linewidth=2, label=f'Smoothed (≤{max_agg_ratio*100}%)')
        plt.title('Aggregation Queries Count: Original vs Smoothed')
        plt.xlabel('Timestamp')
        plt.ylabel('Aggregation Queries Count')
        plt.legend()
        plt.xticks(rotation=45)
        
        # Plot 3: Proportion comparison
        plt.subplot(2, 2, 3)
        original_prop = df['aggregation_queries_count_original'] / df['total_queries_count_original']
        smoothed_prop = df['aggregation_queries_count'] / df['total_queries_count']
        plt.plot(df['ts_submit_dt2'], original_prop, 'b-', alpha=0.7, label='Original Proportion')
        plt.plot(df['ts_submit_dt2'], smoothed_prop, 'r--', alpha=0.8, label='Smoothed Proportion')
        plt.axhline(y=max_agg_ratio, color='orange', linestyle=':', linewidth=2, label=f'Max Ratio ({max_agg_ratio*100}%)')
        plt.title('Aggregation/Total Proportion Comparison')
        plt.xlabel('Timestamp')
        plt.ylabel('Proportion')
        plt.legend()
        plt.xticks(rotation=45)
        
        # Plot 4: Smoothing quality metrics
        plt.subplot(2, 2, 4)
        residuals = df['total_queries_count_original'] - df['total_queries_count']
        plt.plot(df['ts_submit_dt2'], residuals, 'g-', alpha=0.7)
        plt.axhline(y=0, color='k', linestyle='--', alpha=0.5)
        plt.title('Residuals (Original - Smoothed)')
        plt.xlabel('Timestamp')
        plt.ylabel('Residual Value')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.show()
        
        # Summary statistics
        print("\nSummary Statistics:")
        print(f"Mean absolute residual: {np.mean(np.abs(residuals)):.2f}")
        print(f"Max residual: {np.max(np.abs(residuals)):.2f}")
        print(f"Proportion preservation error (RMSE): {np.sqrt(np.mean((original_prop - smoothed_prop)**2)):.6f}")
        
        # Additional side-by-side comparison plot
        plt.figure(figsize=(16, 6))
        
        # Left plot: Original data
        plt.subplot(1, 2, 1)
        plt.plot(df['ts_submit_dt2'], df['total_queries_count_original'], 
                'b-', linewidth=2, label='Total Queries')
        plt.plot(df['ts_submit_dt2'], df['aggregation_queries_count_original'], 
                'r-', linewidth=2, alpha=0.7, label='Aggregation Queries')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['total_queries_count_original'].iloc[peaks], 
                   color='orange', s=60, zorder=5, label='Detected Peaks')
        plt.title('ORIGINAL DATA - Spiky Pattern', fontsize=14, fontweight='bold')
        plt.xlabel('Timestamp')
        plt.ylabel('Query Count')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Right plot: Smoothed data
        plt.subplot(1, 2, 2)
        plt.plot(df['ts_submit_dt2'], df['total_queries_count'], 
                'b-', linewidth=2, label='Total Queries (Smoothed)')
        plt.plot(df['ts_submit_dt2'], df['aggregation_queries_count'], 
                'r-', linewidth=2, alpha=0.7, label=f'Aggregation Queries (≤{max_agg_ratio*100}%)')
        plt.scatter(df['ts_submit_dt2'].iloc[peaks], df['total_queries_count'].iloc[peaks], 
                   color='orange', s=60, zorder=5, label='Peak Locations Preserved')
        plt.title(f'SMOOTHED DATA - Wave-like Pattern (Agg ≤ {max_agg_ratio*100}%)', fontsize=14, fontweight='bold')
        plt.xlabel('Timestamp')
        plt.ylabel('Query Count')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    # Save the smoothed data
    if output_file:
        # Prepare output dataframe with only necessary columns
        output_df = df[['ts_submit_dt2', 'max_concurrent_tasks', 'aggregation_queries_count', 
                       'id', 'total_queries_count', 'ts_submit', 'ts_submit_dt']].copy()
        
        output_df.to_csv(output_file, index=False)
        print(f"\nSmoothed data saved to: {output_file}")
    
    return df

def main():
    """Main function to run the smoothing process"""
    
    # Configuration
    input_file = "./datasets/galaxy_trace.csv"  # Change this to your file path
    output_file = "galaxy_trace_smoothed.csv"  # Output file name
    max_agg_ratio = 0.25  # Maximum 25% aggregation queries
    
    print("=== CSV Query Data Smoother - ALL PEAKS PRESERVED ===")
    print(f"Input file: {input_file}")
    print(f"Maximum aggregation ratio: {max_agg_ratio*100}%")
    print("-" * 50)
    
    try:
        # Process the data
        smoothed_df = smooth_query_data(
            input_file=input_file,
            output_file=output_file,
            plot_results=True,
            max_agg_ratio=max_agg_ratio
        )
        
        print("\n=== Processing Complete! ===")
        print("The smoothed data maintains the wave-like pattern while:")
        print("1. Preserving ALL detected peaks")
        print("2. Creating smooth wave-like transitions between peaks")
        print("3. Maintaining aggregation_queries_count proportions")
        print(f"4. Ensuring aggregation queries never exceed {max_agg_ratio*100}% of total queries")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        print("Please check that the input file exists and has the correct format.")

if __name__ == "__main__":
    main()