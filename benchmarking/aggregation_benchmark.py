#!/usr/bin/env python3
"""
MongoDB Aggregation Benchmark Script
Benchmarks aggregation operations with customizable threading
"""

import threading
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

class MongoDBAggregateBenchmark:
    def __init__(self, host="localhost", port=27017, db_name="ycsb_sharded"):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        
    def connect(self):
        """Establish connection to MongoDB"""
        try:
            print(f"Connecting to MongoDB at {self.host}:{self.port}...")
            self.client = MongoClient(f"mongodb://{self.host}:{self.port}", 
                                    serverSelectionTimeoutMS=5000)
            
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✓ Successfully connected to database '{self.db_name}'")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            print(f"✗ Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error during connection: {e}")
            return False
    
    def check_collection_and_fields(self, collection_name, name_field):
        """Check if collection exists and fields have data"""
        try:
            collection = self.db[collection_name]
            
            # Check if collection exists and has documents
            doc_count = collection.estimated_document_count()
            if doc_count == 0:
                print(f"⚠ Warning: Collection '{collection_name}' is empty or doesn't exist")
                return False
            
            # Check if fields exist in at least one document
            missing_fields = []
            for field_name in [name_field, "age"]:
                sample_doc = collection.find_one({field_name: {"$exists": True}})
                if not sample_doc:
                    missing_fields.append(field_name)
            
            if missing_fields:
                print(f"⚠ Warning: Fields {missing_fields} not found in collection '{collection_name}'")
                return False
            
            print(f"✓ Collection '{collection_name}' has {doc_count} documents")
            print(f"✓ Fields '{name_field}' and 'age' exist in the collection")
            return True
            
        except Exception as e:
            print(f"✗ Error checking collection/fields: {e}")
            return False
    
    def create_aggregation_pipeline(self, name_field):
        """Create the aggregation pipeline"""
        pipeline = [
            {
                "$group": {
                    "_id": {
                        name_field: f"${name_field}",
                        "age": "$age"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {
                    "count": -1,
                    f"_id.{name_field}": 1,
                    "_id.age": 1
                }
            }
        ]
        return pipeline
    
    def print_selected_pipeline(self, name_field, pipeline):
        """Print the selected aggregation pipeline"""
        print(f"\n{'='*60}")
        print(f"AGGREGATION PIPELINE")
        print(f"{'='*60}")
        
        print(f"Pipeline: Group by '{name_field}' and 'age', sort and count")
        print(f"\nStages:")
        print(f"1. $group:")
        print(f"   - Group by: {name_field} and age")
        print(f"   - Count: documents per group")
        print(f"2. $sort:")
        print(f"   - Primary: count (descending)")
        print(f"   - Secondary: {name_field} (ascending)")
        print(f"   - Tertiary: age (ascending)")
        
        print(f"\nMongoDB Pipeline:")
        for i, stage in enumerate(pipeline, 1):
            stage_name = list(stage.keys())[0]
            print(f"  Stage {i}: {stage_name}")
            if stage_name == "$group":
                print(f"    _id: {stage['$group']['_id']}")
                print(f"    count: {stage['$group']['count']}")
            elif stage_name == "$sort":
                print(f"    sort fields: {stage['$sort']}")
        
        print(f"{'='*60}")
    
    def aggregation_worker(self, thread_id, collection_name, pipeline, operations_per_thread):
        """Worker function for each thread"""
        results = {
            'thread_id': thread_id,
            'operations': 0,
            'total_time': 0,
            'errors': 0,
            'individual_times': [],
            'min_latency': float('inf'),
            'max_latency': 0,
            'avg_latency': 0
        }
        
        try:
            # Each thread gets its own connection
            local_client = MongoClient(f"mongodb://{self.host}:{self.port}")
            local_db = local_client[self.db_name]
            local_collection = local_db[collection_name]
            
            # Warm up connection
            local_collection.find_one()
            
            total_query_time = 0
            
            for i in range(operations_per_thread):
                try:
                    # Time individual aggregation execution
                    query_start = time.time()
                    
                    # Perform aggregation operation and force execution
                    cursor = local_collection.aggregate(pipeline)
                    result_list = list(cursor)  # Force execution by consuming cursor
                    
                    query_end = time.time()
                    
                    # Calculate individual query time in milliseconds
                    query_time_ms = (query_end - query_start) * 1000
                    total_query_time += (query_end - query_start)
                    
                    # Track individual query times for statistics
                    results['individual_times'].append(query_time_ms)
                    results['min_latency'] = min(results['min_latency'], query_time_ms)
                    results['max_latency'] = max(results['max_latency'], query_time_ms)
                    
                    results['operations'] += 1
                    
                except Exception as e:
                    results['errors'] += 1
                    if results['errors'] <= 3:  # Only print first few errors
                        print(f"Thread {thread_id} error: {e}")
            
            results['total_time'] = total_query_time
            
            # Calculate average latency
            if results['operations'] > 0:
                results['avg_latency'] = sum(results['individual_times']) / results['operations']
            else:
                results['min_latency'] = 0
            
            local_client.close()
            
        except Exception as e:
            print(f"Thread {thread_id} fatal error: {e}")
            results['errors'] += 1
        
        return results
    
    def run_benchmark(self, collection_name, name_field, num_threads, total_operations):
        """Run the benchmark with specified parameters"""
        print(f"\n{'='*60}")
        print(f"MONGODB AGGREGATION BENCHMARK")
        print(f"{'='*60}")
        print(f"Collection: {collection_name}")
        print(f"Name Field: {name_field}")
        print(f"Age Field: age")
        print(f"Threads: {num_threads}")
        print(f"Total Operations: {total_operations}")
        print(f"Operations per thread: {total_operations // num_threads}")
        print(f"{'='*60}")
        
        # Create the aggregation pipeline
        pipeline = self.create_aggregation_pipeline(name_field)
        
        # Print the pipeline
        self.print_selected_pipeline(name_field, pipeline)
        
        operations_per_thread = total_operations // num_threads
        
        print(f"\nStarting aggregation benchmark with {num_threads} threads...")
        print(f"Each thread will execute the same aggregation pipeline {operations_per_thread} times.\n")
        
        overall_start = time.time()
        
        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all tasks
            futures = []
            for i in range(num_threads):
                future = executor.submit(
                    self.aggregation_worker, 
                    i + 1, 
                    collection_name, 
                    pipeline,
                    operations_per_thread
                )
                futures.append(future)
            
            # Collect results as they complete
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    thread_query_time_ms = result['total_time'] * 1000
                    avg_latency = result['avg_latency'] if result['operations'] > 0 else 0
                    print(f"Thread {result['thread_id']} completed: {result['operations']} ops, "
                          f"total query time: {thread_query_time_ms:.2f} ms, "
                          f"avg latency: {avg_latency:.2f} ms")
                except Exception as e:
                    print(f"Thread execution error: {e}")
        
        overall_end = time.time()
        overall_time = overall_end - overall_start
        
        self.print_results(results, overall_time, total_operations, name_field)
    
    def print_results(self, results, overall_time, total_operations, name_field):
        """Print benchmark results"""
        print(f"\n{'='*60}")
        print(f"AGGREGATION BENCHMARK RESULTS")
        print(f"{'='*60}")
        
        total_ops_completed = sum(r['operations'] for r in results)
        total_errors = sum(r['errors'] for r in results)
        total_query_time = sum(r['total_time'] for r in results)
        
        # Collect all individual query times for overall statistics
        all_query_times = []
        for result in results:
            all_query_times.extend(result['individual_times'])
        
        # Convert times to milliseconds
        overall_time_ms = overall_time * 1000
        total_query_time_ms = total_query_time * 1000
        
        print(f"Pipeline: Group by '{name_field}' and 'age', sort and count")
        print(f"Wall clock time (parallel execution): {overall_time_ms:.2f} ms")
        print(f"Total query execution time (sum of all threads): {total_query_time_ms:.2f} ms")
        print(f"Operations completed: {total_ops_completed}/{total_operations}")
        print(f"Total errors: {total_errors}")
        
        if total_ops_completed > 0 and all_query_times:
            # Overall latency statistics
            avg_latency_ms = sum(all_query_times) / len(all_query_times)
            min_latency_ms = min(all_query_times)
            max_latency_ms = max(all_query_times)
            
            # Throughput calculations
            overall_ops_per_sec = total_ops_completed / overall_time
            effective_ops_per_sec = total_ops_completed / total_query_time  # Based on actual query time
            
            print(f"\nLATENCY STATISTICS:")
            print(f"Average latency per aggregation: {avg_latency_ms:.2f} ms")
            print(f"Minimum latency: {min_latency_ms:.2f} ms")
            print(f"Maximum latency: {max_latency_ms:.2f} ms")
            
            # Calculate percentiles
            sorted_times = sorted(all_query_times)
            p50 = sorted_times[len(sorted_times) // 2]
            p95 = sorted_times[int(len(sorted_times) * 0.95)]
            p99 = sorted_times[int(len(sorted_times) * 0.99)]
            
            print(f"50th percentile (median): {p50:.2f} ms")
            print(f"95th percentile: {p95:.2f} ms")
            print(f"99th percentile: {p99:.2f} ms")
            
            print(f"\nTHROUGHPUT:")
            print(f"Overall throughput (wall clock): {overall_ops_per_sec:.2f} aggregations/second")
            print(f"Effective throughput (query time): {effective_ops_per_sec:.2f} aggregations/second")
            
            # Threading efficiency
            theoretical_speedup = len(results)
            actual_speedup = total_query_time / overall_time
            efficiency = (actual_speedup / theoretical_speedup) * 100
            
            print(f"\nTHREADING EFFICIENCY:")
            print(f"Theoretical speedup: {theoretical_speedup:.1f}x")
            print(f"Actual speedup: {actual_speedup:.1f}x")
            print(f"Threading efficiency: {efficiency:.1f}%")
        
        # Per-thread breakdown
        print(f"\nPER-THREAD BREAKDOWN:")
        for result in sorted(results, key=lambda x: x['thread_id']):
            thread_query_time_ms = result['total_time'] * 1000
            ops_per_sec = result['operations'] / result['total_time'] if result['total_time'] > 0 else 0
            
            print(f"  Thread {result['thread_id']}: {result['operations']} ops, "
                  f"total query time: {thread_query_time_ms:.2f} ms, "
                  f"{ops_per_sec:.2f} ops/sec")
            
            if result['operations'] > 0:
                print(f"    Avg: {result['avg_latency']:.2f} ms, "
                      f"Min: {result['min_latency']:.2f} ms, "
                      f"Max: {result['max_latency']:.2f} ms, "
                      f"Errors: {result['errors']}")
            else:
                print(f"    No successful operations, Errors: {result['errors']}")
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()

def get_user_input():
    """Get benchmark parameters from user input"""
    print("MongoDB Aggregation Benchmark Configuration")
    print("-" * 45)
    
    # Collection name
    collection = input("Enter collection name (default: usertable): ").strip()
    if not collection:
        collection = "usertable"
    
    # Name field choice
    print("\nSelect name field:")
    print("1. first_name")
    print("2. last_name")
    
    while True:
        choice = input("Enter choice (1 or 2, default: 2): ").strip()
        if choice == "1":
            name_field = "first_name"
            break
        elif not choice or choice == "2":
            name_field = "last_name"
            break
        else:
            print("Please enter 1 or 2")
    
    # Number of threads
    while True:
        try:
            threads = input("Enter number of threads (default: 4): ").strip()
            threads = int(threads) if threads else 4
            if threads <= 0:
                print("Number of threads must be positive")
                continue
            break
        except ValueError:
            print("Please enter a valid number")
    
    # Number of operations
    while True:
        try:
            operations = input("Enter total number of operations (default: 200): ").strip()
            operations = int(operations) if operations else 200
            if operations <= 0:
                print("Number of operations must be positive")
                continue
            break
        except ValueError:
            print("Please enter a valid number")
    
    return collection, name_field, threads, operations

def main():
    """Main function"""
    print("MongoDB Aggregation Benchmark Tool")
    print("=" * 45)
    
    # Initialize benchmark
    benchmark = MongoDBAggregateBenchmark()
    
    # Test connection first
    if not benchmark.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        collection_name, name_field, num_threads, total_operations = get_user_input()
        
        # Validate collection and fields
        if not benchmark.check_collection_and_fields(collection_name, name_field):
            response = input("Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        # Run benchmark
        benchmark.run_benchmark(collection_name, name_field, num_threads, total_operations)
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    finally:
        benchmark.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())