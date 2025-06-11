import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def convert_workflow_to_mongodb_trace(input_file, output_file):
    """
    Convert general workflow trace CSV to MongoDB workflow trace format.
    
    Transformations:
    - Keep approx_max_concurrent_tasks column
    - Rename critical_path_task_count to aggregation_pipelines_queries_count
    - Remove id column
    - Rename task_count to total_queries_count
    - Normalize ts_submit to 1 hour duration starting from 10 seconds
    - Keep ts_submit_dt column
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Create a copy for transformation
    converted_df = df.copy()
    
    # Remove the id column
    converted_df = converted_df.drop('id', axis=1)
    
    # Rename columns
    converted_df = converted_df.rename(columns={
        'critical_path_task_count': 'aggregation_pipelines_queries_count',
        'task_count': 'total_queries_count'
    })
    
    # Normalize timestamps
    # Current duration calculation
    min_ts = df['ts_submit'].min()
    max_ts = df['ts_submit'].max()
    original_duration = max_ts - min_ts
    
    print(f"Original timestamp range: {min_ts} to {max_ts}")
    print(f"Original duration: {original_duration} seconds ({original_duration/3600:.2f} hours)")
    
    # Target duration: 1 hour (3600 seconds)
    target_duration = 3600
    scaling_factor = target_duration / original_duration
    
    # Normalize timestamps: start from 10 seconds, scale to 1 hour
    normalized_ts = (df['ts_submit'] - min_ts) * scaling_factor + 10
    converted_df['ts_submit'] = normalized_ts.round().astype(int)
    
    # Update the datetime column to reflect the new timestamps
    # We'll create new datetime values starting from an arbitrary base time
    base_time = datetime(2011, 9, 21, 21, 30, 17)  # Using the original first timestamp as base
    new_datetimes = [base_time + timedelta(seconds=int(ts-10)) for ts in converted_df['ts_submit']]
    converted_df['ts_submit_dt'] = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in new_datetimes]
    
    # Reorder columns to match desired output format
    column_order = [
        'approx_max_concurrent_tasks',
        'aggregation_pipelines_queries_count', 
        'total_queries_count',
        'ts_submit',
        'ts_submit_dt'
    ]
    converted_df = converted_df[column_order]
    
    # Save to new CSV file
    converted_df.to_csv(output_file, index=False)
    
    # Print summary statistics
    print(f"\nConversion completed!")
    print(f"New timestamp range: {converted_df['ts_submit'].min()} to {converted_df['ts_submit'].max()}")
    print(f"New duration: {converted_df['ts_submit'].max() - converted_df['ts_submit'].min()} seconds ({(converted_df['ts_submit'].max() - converted_df['ts_submit'].min())/3600:.2f} hours)")
    print(f"Number of records: {len(converted_df)}")
    print(f"Output saved to: {output_file}")
    
    return converted_df

# Example usage
if __name__ == "__main__":
    input_filename = "Pegasus_P7.csv"
    output_filename = "MongoDB_workflow_trace.csv"
    
    try:
        result_df = convert_workflow_to_mongodb_trace(input_filename, output_filename)
        
        # Display first few rows of the converted data
        print("\nFirst 5 rows of converted data:")
        print(result_df.head())
        
        print("\nLast 5 rows of converted data:")
        print(result_df.tail())
        
    except FileNotFoundError:
        print(f"Error: Could not find input file '{input_filename}'")
        print("Please make sure the file exists in the current directory.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")