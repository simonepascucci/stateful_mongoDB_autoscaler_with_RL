import pandas as pd
import numpy as np

def adjust_aggregation_queries(csv_file_path, output_file_path=None):
    """
    Adjust aggregation_queries_count to be between 5% and 20% of total_queries_count
    
    Args:
        csv_file_path (str): Path to the input CSV file
        output_file_path (str): Path for the output CSV file (optional)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Set random seed for reproducibility (optional)
    np.random.seed(42)
    
    # Generate random percentages between 5% and 20% for each row
    random_percentages = np.random.uniform(0.05, 0.20, size=len(df))
    
    # Calculate new aggregation_queries_count values
    df['aggregation_queries_count'] = (df['total_queries_count'] * random_percentages).astype(int)
    
    # Ensure minimum value of 1 where total_queries_count > 0
    df.loc[df['total_queries_count'] > 0, 'aggregation_queries_count'] = np.maximum(
        df.loc[df['total_queries_count'] > 0, 'aggregation_queries_count'], 1
    )
    
    # Set output file path if not provided
    if output_file_path is None:
        output_file_path = csv_file_path.replace('.csv', '_adjusted.csv')
    
    # Save the modified dataset
    df.to_csv(output_file_path, index=False)
    
    print(f"Dataset adjusted and saved to: {output_file_path}")
    print(f"Total rows processed: {len(df)}")
    
    # Display some statistics
    print("\nAggregation queries count statistics:")
    print(f"Min percentage: {(df['aggregation_queries_count'] / df['total_queries_count']).min():.2%}")
    print(f"Max percentage: {(df['aggregation_queries_count'] / df['total_queries_count']).max():.2%}")
    print(f"Mean percentage: {(df['aggregation_queries_count'] / df['total_queries_count']).mean():.2%}")

# Example usage
if __name__ == "__main__":
    # Replace 'your_dataset.csv' with the path to your full CSV file
    input_file = 'combined.csv'
    output_file = 'combined.csv'  # Optional: specify output file name
    
    adjust_aggregation_queries(input_file, output_file)