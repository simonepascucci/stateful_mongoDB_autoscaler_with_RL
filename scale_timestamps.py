import pandas as pd
import os # Import os to check for file existence

def scale_timestamps_auto_duration(input_csv_path, target_duration_hours):
    """
    Scales the 'ts_submit_dt' column in a DataFrame to a new duration
    by automatically detecting the original total duration and preserving
    relative time differences.

    Args:
        input_csv_path (str): Path to the input CSV file.
        target_duration_hours (float): The desired new total duration of the
                                        timestamps in hours.

    Returns:
        pandas.DataFrame: The DataFrame with the new 'ts_submit_dt2' column,
                          or None if an error occurs.
    """
    try:
        df = pd.read_csv(input_csv_path)
    except FileNotFoundError:
        print(f"Error: Input CSV file not found at '{input_csv_path}'")
        return None
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

    # Convert 'ts_submit_dt' to datetime objects
    df['ts_submit_dt'] = pd.to_datetime(df['ts_submit_dt'])

    # Calculate the minimum and maximum 'ts_submit_dt' to find the original total duration
    min_ts_submit_dt = df['ts_submit_dt'].min()
    max_ts_submit_dt = df['ts_submit_dt'].max()

    # Calculate the original total elapsed time in seconds
    original_total_elapsed_seconds = (max_ts_submit_dt - min_ts_submit_dt).total_seconds()

    # Handle case where original duration is zero (e.g., only one timestamp or all timestamps are identical)
    if original_total_elapsed_seconds == 0:
        print("Warning: Original total duration is zero. 'ts_submit_dt2' will be identical to 'ts_submit_dt'.")
        df['ts_submit_dt2'] = df['ts_submit_dt']
        return df

    # Calculate the elapsed time from the minimum 'ts_submit_dt' for each row
    df['elapsed_time'] = df['ts_submit_dt'] - min_ts_submit_dt

    # Convert target duration to seconds
    target_duration_seconds = target_duration_hours * 3600

    # Calculate the scaling factor
    scaling_factor = target_duration_seconds / original_total_elapsed_seconds

    # Apply the scaling factor to the elapsed time
    df['scaled_elapsed_time'] = df['elapsed_time'] * scaling_factor

    # Convert the scaled elapsed time back to datetime objects for 'ts_submit_dt2',
    # starting from the minimum 'ts_submit_dt'
    df['ts_submit_dt2'] = min_ts_submit_dt + df['scaled_elapsed_time']

    return df

if __name__ == "__main__":
    # Get input CSV file path from the user
    while True:
        input_file = input("Enter the path to your existing CSV file: ")
        if os.path.exists(input_file):
            break
        else:
            print(f"Error: The file '{input_file}' does not exist. Please enter a valid path.")

    # Get target duration from the user
    while True:
        try:
            target_duration = float(input("Enter the desired total duration after scaling (in hours): "))
            if target_duration <= 0:
                print("Please enter a positive value for the target duration.")
            else:
                break
        except ValueError:
            print("Invalid input. Please enter a numerical value for hours.")

    scaled_df = scale_timestamps_auto_duration(input_file, target_duration)

    if scaled_df is not None:
        print("\n--- Scaling Results ---")
        print("Original and Scaled Timestamps (first 5 rows):")
        print(scaled_df[['ts_submit_dt', 'ts_submit_dt2']].head().to_markdown(index=False))
        if len(scaled_df) > 5:
            print("...")
            print(scaled_df[['ts_submit_dt', 'ts_submit_dt2']].tail().to_markdown(index=False))


        output_file = "output_scaled_timestamps.csv"
        scaled_df.to_csv(output_file, index=False)
        print(f"\nSuccessfully scaled timestamps! The complete scaled data has been saved to '{output_file}'.")