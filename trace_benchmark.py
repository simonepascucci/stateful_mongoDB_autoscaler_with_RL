#!/usr/bin/env python3
"""
MongoDB Benchmark Script
Performs benchmarking based on CSV trace file structure
"""

import sys
import pandas as pd
import pymongo
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import argparse
from statistics import mean

class MongoDBBenchmark:
    def __init__(self, host: str = "localhost", port: int = 27017, database: str = "ycsb_sharded"):
        """Initialize MongoDB connection"""
        self.client = pymongo.MongoClient(host, port)
        self.db = self.client[database]
        self.collection = None
        
        # Define query templates
        self.find_queries = [
            {"first_name": "Charles"},
            {"last_name": "Miller"}
        ]
        
        self.aggregation_queries = [
            [
                {"$match": {"first_name": "Charles", "last_name": "Miller", "age": {"$lt": 35}}},
                {"$lookup": {"from": "U2", "localField": "first_name", "foreignField": "first_name", "as": "joined_data"}},
                {"$limit": 1000}
            ],
            [
                {"$group": {"_id": "$country", "count": {"$sum": 1}, "avg_age": {"$avg": "$age"}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
        ]
    
    def set_collection(self, collection_name: str):
        """Set the collection to use for benchmarking"""
        self.collection = self.db[collection_name]
        print(f"Using collection: {collection_name}")
    
    def execute_find_query(self) -> float:
        """Execute a random find query and return execution time"""
        query = random.choice(self.find_queries)
        start_time = time.time()
        
        try:
            # Execute query and consume results
            results = list(self.collection.find(query))
            execution_time = time.time() - start_time
            return execution_time
        except Exception as e:
            print(f"Error executing find query {query}: {e}")
            return 0.0
    
    def execute_aggregation_query(self) -> float:
        """Execute a random aggregation query and return execution time"""
        pipeline = random.choice(self.aggregation_queries)
        start_time = time.time()
        
        try:
            # Execute aggregation pipeline and consume results
            results = list(self.collection.aggregate(pipeline))
            execution_time = time.time() - start_time
            return execution_time
        except Exception as e:
            print(f"Error executing aggregation query {pipeline}: {e}")
            return 0.0
    
    def execute_batch(self, total_queries: int, aggregation_queries: int) -> Dict[str, Any]:
        """Execute a batch of queries and return performance metrics"""
        find_queries_count = total_queries - aggregation_queries
        
        find_times = []
        aggregation_times = []
        
        batch_start = time.time()
        
        # Execute find queries
        for _ in range(find_queries_count):
            exec_time = self.execute_find_query()
            find_times.append(exec_time)
        
        # Execute aggregation queries
        for _ in range(aggregation_queries):
            exec_time = self.execute_aggregation_query()
            aggregation_times.append(exec_time)
        
        batch_end = time.time()
        
        return {
            'batch_execution_time': batch_end - batch_start,
            'find_avg_latency': mean(find_times) if find_times else 0.0,
            'aggregation_avg_latency': mean(aggregation_times) if aggregation_times else 0.0,
            'find_queries_count': find_queries_count,
            'aggregation_queries_count': aggregation_queries,
            'total_queries': total_queries
        }
    
    def run_benchmark(self, csv_file: str):
        """Run the complete benchmark based on CSV file"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, sep=';')
            
            # Convert ts_submit_dt2 to datetime
            df['ts_submit_dt2'] = pd.to_datetime(df['ts_submit_dt2'])
            
            # Sort by ts_submit_dt2 to ensure correct order
            df = df.sort_values('ts_submit_dt2').reset_index(drop=True)
            
            print(f"Loaded {len(df)} batches from {csv_file}")
            print("Starting benchmark...")
            print("-" * 80)
            
            # Get the starting timestamp (minimum ts_submit_dt2)
            start_timestamp = df['ts_submit_dt2'].min()
            benchmark_start = time.time()
            
            for index, row in df.iterrows():
                # Calculate when this batch should be executed
                time_offset = (row['ts_submit_dt2'] - start_timestamp).total_seconds()
                
                # Wait until it's time to execute this batch
                elapsed_real_time = time.time() - benchmark_start
                if time_offset > elapsed_real_time:
                    sleep_time = time_offset - elapsed_real_time
                    wait_time_str = format_time(sleep_time)
                    print(f"Waiting {wait_time_str} before batch {row['id']}...")
                    time.sleep(sleep_time)
                
                # Execute the batch
                print(f"Executing batch {row['id']} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                metrics = self.execute_batch(
                    total_queries=int(row['total_queries_count']),
                    aggregation_queries=int(row['aggregation_queries_count'])
                )
                
                # Print batch results
                print(f"Batch {row['id']} Results:")
                print(f"  - Total queries: {metrics['total_queries']}")
                print(f"  - Find queries: {metrics['find_queries_count']} (avg latency: {metrics['find_avg_latency']*1000:.2f}ms)")
                print(f"  - Aggregation queries: {metrics['aggregation_queries_count']} (avg latency: {metrics['aggregation_avg_latency']*1000:.2f}ms)")
                print(f"  - Batch execution time: {metrics['batch_execution_time']:.4f}s)")
                print(f"  - Max concurrent tasks: {row['max_concurrent_tasks']}")
                print("-" * 80)
        
        except Exception as e:
            print(f"Error running benchmark: {e}")
            sys.exit(1)
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()

def format_time(seconds):
    """Format time duration for display"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    else:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds:.2f} sec"

def main():
    parser = argparse.ArgumentParser(description='MongoDB Benchmark Tool')
    parser.add_argument('--csv', required=True, help='Path to the CSV trace file')
    parser.add_argument('--host', default='localhost', help='MongoDB host (default: localhost)')
    parser.add_argument('--port', type=int, default=27017, help='MongoDB port (default: 27017)')
    parser.add_argument('--database', default='ycsb_sharded', help='Database name (default: ycsb_sharded)')
    
    args = parser.parse_args()
    
    # Initialize benchmark
    benchmark = MongoDBBenchmark(args.host, args.port, args.database)
    
    try:
        # Test connection
        benchmark.client.admin.command('ping')
        print(f"Connected to MongoDB at {args.host}:{args.port}")
        print(f"Using database: {args.database}")
        
        # Get collection name from user
        collection_name = input("Enter the collection name (default: usertable): ").strip()
        
        if not collection_name:
            collection_name = "usertable"
        
        benchmark.set_collection(collection_name)
        
        # Verify collection exists
        if collection_name not in benchmark.db.list_collection_names():
            print(f"Warning: Collection '{collection_name}' does not exist in database '{args.database}'")
            proceed = input("Do you want to continue anyway? (y/N): ").strip().lower()
            if proceed != 'y':
                sys.exit(1)
        
        # Run benchmark
        benchmark.run_benchmark(args.csv)
        
        print("Benchmark completed successfully!")
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print(f"Error: Could not connect to MongoDB at {args.host}:{args.port}")
        print("Please ensure MongoDB is running and accessible.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        benchmark.close()

if __name__ == "__main__":
    main()