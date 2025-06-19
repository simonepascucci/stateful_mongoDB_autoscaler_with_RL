import pandas as pd
import numpy as np
import random

def process_galaxy_data(input_file, output_file):
    """
    Process galaxy trace data according to specified requirements.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Remove the "id" column if it exists
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
        print("Removed 'id' column from dataset")
    
    print(f"Original dataset shape: {df.shape}")
    print(f"Original total_queries_count range: {df['total_queries_count'].min()} - {df['total_queries_count'].max()}")

    # Scale max_concurrent_tasks to be between 1 and 36
    if 'max_concurrent_tasks' in df.columns:
        print(f"Original max_concurrent_tasks range: {df['max_concurrent_tasks'].min()} - {df['max_concurrent_tasks'].max()}")
        
        # Check if scaling is needed
        max_value = df['max_concurrent_tasks'].max()
        min_value = df['max_concurrent_tasks'].min()
        
        if max_value > 36:
            # Find the smallest integer divisor that brings max value below 36
            divisor = int(np.ceil(max_value / 36))
            print(f"Scaling max_concurrent_tasks by dividing by {divisor}...")
            
            # Scale the values
            df['max_concurrent_tasks'] = np.round(df['max_concurrent_tasks'] / divisor).astype(int)
            
            # Ensure minimum value is at least 1
            df['max_concurrent_tasks'] = df['max_concurrent_tasks'].clip(lower=1)
            
            print(f"After scaling - max_concurrent_tasks range: {df['max_concurrent_tasks'].min()} - {df['max_concurrent_tasks'].max()}")
        elif min_value < 1:
            # If minimum is below 1, scale up to ensure minimum is 1
            print("Adjusting max_concurrent_tasks to ensure minimum value is 1...")
            df.loc[df['max_concurrent_tasks'] < 1, 'max_concurrent_tasks'] = 1
            print(f"After adjustment - max_concurrent_tasks range: {df['max_concurrent_tasks'].min()} - {df['max_concurrent_tasks'].max()}")
        else:
            print("max_concurrent_tasks already within desired range [1, 36]. No scaling needed.")
    
    # Check if there's any row with total_queries_count < 10
    has_low_count = (df['total_queries_count'] < 10).any()
    
    if has_low_count:
        print("Found rows with total_queries_count < 10. Multiplying by 10...")
        # Multiply by 10 for the specified columns
        df['total_queries_count'] *= 10
        df['aggregation_queries_count'] *= 10
        print(f"After multiplication - total_queries_count range: {df['total_queries_count'].min()} - {df['total_queries_count'].max()}")
    else:
        print("No rows with total_queries_count < 10 found. No multiplication needed.")
    
    # Add standard_queries_count column
    df['standard_queries_count'] = df['total_queries_count'] - df['aggregation_queries_count']
    print(f"Standard queries count range: {df['standard_queries_count'].min()} - {df['standard_queries_count'].max()}")
    
    # Set random seed for reproducibility (optional - remove if you want different results each time)
    np.random.seed(42)
    
    # Calculate bounds to ensure other_queries_count doesn't exceed 20%
    # Since other_queries = standard - (first_name + last_name + country)
    # And we want other_queries <= 20% of standard
    # We need: first_name + last_name + country >= 80% of standard
    
    def generate_query_counts(standard_count):
        if standard_count <= 0:
            return 0, 0, 0, 0
        
        # Target: ensure other_queries <= 20% of standard_count
        max_other = int(standard_count * 0.2)
        min_sum_others = standard_count - max_other  # Minimum sum of first_name + last_name + country
        
        # Generate first_name_queries_count (20% to 50% of standard_queries_count)
        first_name_min = max(int(standard_count * 0.2), 1)
        first_name_max = min(int(standard_count * 0.5), standard_count - 2)  # Leave room for others
        first_name_count = np.random.randint(first_name_min, first_name_max + 1)
        
        # Calculate remaining after first_name
        remaining_after_first = standard_count - first_name_count
        min_remaining_others = max(0, min_sum_others - first_name_count)
        
        # Generate last_name_queries_count (with lower bound to meet constraint)
        last_name_max = min(int(standard_count * 0.25), remaining_after_first - 1)  # Leave room for country
        last_name_min = max(0, min_remaining_others - int(standard_count * 0.25))  # Ensure constraint
        last_name_min = min(last_name_min, last_name_max)  # Ensure min <= max
        
        if last_name_min <= last_name_max:
            last_name_count = np.random.randint(last_name_min, last_name_max + 1)
        else:
            last_name_count = last_name_min
        
        # Calculate remaining after first_name and last_name
        remaining_after_both = standard_count - first_name_count - last_name_count
        min_country = max(0, min_sum_others - first_name_count - last_name_count)
        
        # Generate country_queries_count (with bounds)
        country_max = min(int(standard_count * 0.25), remaining_after_both)
        country_min = max(0, min_country)
        country_min = min(country_min, country_max)  # Ensure min <= max
        
        if country_min <= country_max:
            country_count = np.random.randint(country_min, country_max + 1)
        else:
            country_count = country_min
        
        # Calculate other_queries_count as remainder
        other_count = standard_count - first_name_count - last_name_count - country_count
        
        return first_name_count, last_name_count, country_count, other_count
    
    # Apply the function to generate all query counts
    query_counts = df['standard_queries_count'].apply(generate_query_counts)
    
    # Extract the counts into separate columns
    df['first_name_queries_count'] = [counts[0] for counts in query_counts]
    df['last_name_queries_count'] = [counts[1] for counts in query_counts]
    df['country_queries_count'] = [counts[2] for counts in query_counts]
    df['other_queries_count'] = [counts[3] for counts in query_counts]
    
    # Verify that the sum equals standard_queries_count
    total_check = (df['first_name_queries_count'] + 
                   df['last_name_queries_count'] + 
                   df['country_queries_count'] + 
                   df['other_queries_count'])
    
    # Check that other_queries_count doesn't exceed 20% of standard_queries_count
    other_percentage = df['other_queries_count'] / df['standard_queries_count'].replace(0, 1) * 100
    max_other_percentage = other_percentage.max()
    
    if not (total_check == df['standard_queries_count']).all():
        print("Warning: Sum of query type counts doesn't equal standard_queries_count for some rows")
    else:
        print("Verification passed: All query counts sum correctly")
    
    print(f"Maximum other_queries_count percentage: {max_other_percentage:.1f}%")
    if max_other_percentage <= 20:
        print("✓ Constraint satisfied: other_queries_count ≤ 20% of standard_queries_count")
    else:
        print("⚠ Warning: other_queries_count exceeds 20% for some rows")
    
    # Save to output file
    df.to_csv(output_file, index=False)
    print(f"Processed data saved to: {output_file}")
    print(f"Final dataset shape: {df.shape}")
    
    # Display summary statistics
    print("\nSummary of new columns:")
    print(f"first_name_queries_count: {df['first_name_queries_count'].min()} - {df['first_name_queries_count'].max()}")
    print(f"last_name_queries_count: {df['last_name_queries_count'].min()} - {df['last_name_queries_count'].max()}")
    print(f"country_queries_count: {df['country_queries_count'].min()} - {df['country_queries_count'].max()}")
    print(f"other_queries_count: {df['other_queries_count'].min()} - {df['other_queries_count'].max()}")
    
    return df

# Example usage
if __name__ == "__main__":
    # Process the galaxy trace data
    input_filename = "askalon_ee_trace_concurrent_tasks_smoothed.csv"
    output_filename = "askalon_ee_trace_processed.csv"
    
    try:
        processed_df = process_galaxy_data(input_filename, output_filename)
        
        # Display first few rows of the processed data
        print("\nFirst 5 rows of processed data:")
        print(processed_df.head())
        
    except FileNotFoundError:
        print(f"Error: Could not find input file '{input_filename}'")
        print("Please make sure the file exists in the current directory.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")