#!/usr/bin/env python3
"""
MongoDB Read Benchmark Script
Benchmarks read operations with customizable threading
"""

import threading
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

class MongoDBBenchmark:
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
    
    def check_collection_and_field(self, collection_name, field_name):
        """Check if collection exists and field has data"""
        try:
            collection = self.db[collection_name]
            
            # Check if collection exists and has documents
            doc_count = collection.estimated_document_count()
            if doc_count == 0:
                print(f"⚠ Warning: Collection '{collection_name}' is empty or doesn't exist")
                return False
            
            # Check if field exists in at least one document
            sample_doc = collection.find_one({field_name: {"$exists": True}})
            if not sample_doc:
                print(f"⚠ Warning: Field '{field_name}' not found in collection '{collection_name}'")
                return False
            
            print(f"✓ Collection '{collection_name}' has {doc_count} documents")
            print(f"✓ Field '{field_name}' exists in the collection")
            return True
            
        except Exception as e:
            print(f"✗ Error checking collection/field: {e}")
            return False
    
    def get_sample_values(self, collection_name, field_name, sample_size=100):
        """Get sample values from the field for realistic queries"""
        try:
            collection = self.db[collection_name]
            pipeline = [
                {"$match": {field_name: {"$exists": True, "$ne": None}}},
                {"$sample": {"size": sample_size}},
                {"$project": {field_name: 1, "_id": 0}}
            ]
            
            values = []
            for doc in collection.aggregate(pipeline):
                if field_name in doc and doc[field_name] is not None:
                    values.append(doc[field_name])
            
            if not values:
                # Fallback: get first few values
                for doc in collection.find({field_name: {"$exists": True}}).limit(sample_size):
                    if field_name in doc and doc[field_name] is not None:
                        values.append(doc[field_name])
            
            print(f"✓ Collected {len(values)} sample values for benchmarking")
            return values
            
        except Exception as e:
            print(f"✗ Error getting sample values: {e}")
            return []
    
    def read_worker(self, thread_id, collection_name, field_name, sample_values, operations_per_thread):
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
            # Each thread gets its own connection to ensure independence
            local_client = MongoClient(f"mongodb://{self.host}:{self.port}")
            local_db = local_client[self.db_name]
            local_collection = local_db[collection_name]
            
            # Warm up connection
            local_collection.find_one()
            
            total_query_time = 0
            
            for i in range(operations_per_thread):
                try:
                    # Randomly select a value to query
                    query_value = random.choice(sample_values)
                    
                    # Time individual query execution
                    query_start = time.time()
                    
                    # Perform read operation and force execution
                    cursor = local_collection.find({field_name: query_value})
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
                    
                    # Optional: small delay between queries to simulate real usage
                    # time.sleep(0.001)  # 1ms delay - uncomment if needed
                    
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
    
    def run_benchmark(self, collection_name, field_name, num_threads, total_operations):
        """Run the benchmark with specified parameters"""
        print(f"\n{'='*60}")
        print(f"MONGODB READ BENCHMARK")
        print(f"{'='*60}")
        print(f"Collection: {collection_name}")
        print(f"Field: {field_name}")
        print(f"Threads: {num_threads}")
        print(f"Total Operations: {total_operations}")
        print(f"Operations per thread: {total_operations // num_threads}")
        print(f"{'='*60}")
        
        # Get sample values for realistic queries
        sample_values = self.get_sample_values(collection_name, field_name)
        if not sample_values:
            print("✗ Cannot proceed without sample values")
            return
        
        operations_per_thread = total_operations // num_threads
        
        print(f"\nStarting benchmark with {num_threads} threads...")
        overall_start = time.time()
        
        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all tasks
            futures = []
            for i in range(num_threads):
                future = executor.submit(
                    self.read_worker, 
                    i + 1, 
                    collection_name, 
                    field_name, 
                    sample_values, 
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
        
        self.print_results(results, overall_time, total_operations)
    
    def print_results(self, results, overall_time, total_operations):
        """Print benchmark results"""
        print(f"\n{'='*60}")
        print(f"BENCHMARK RESULTS")
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
            print(f"Average latency per query: {avg_latency_ms:.2f} ms")
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
            print(f"Overall throughput (wall clock): {overall_ops_per_sec:.2f} operations/second")
            print(f"Effective throughput (query time): {effective_ops_per_sec:.2f} operations/second")
            
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
    print("MongoDB Read Benchmark Configuration")
    print("-" * 40)
    
    # Collection name
    collection = input("Enter collection name (default: usertable): ").strip()
    if not collection:
        collection = "usertable"
    
    # Field name
    field = input("Enter field name to query (default: first_name): ").strip()
    if not field:
        field = "first_name"
    
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
            operations = input("Enter total number of operations (default: 1000): ").strip()
            operations = int(operations) if operations else 1000
            if operations <= 0:
                print("Number of operations must be positive")
                continue
            break
        except ValueError:
            print("Please enter a valid number")
    
    return collection, field, threads, operations

def main():
    """Main function"""
    print("MongoDB Read Benchmark Tool")
    print("=" * 40)
    
    # Initialize benchmark
    benchmark = MongoDBBenchmark()
    
    # Test connection first
    if not benchmark.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        collection_name, field_name, num_threads, total_operations = get_user_input()
        
        # Validate collection and field
        if not benchmark.check_collection_and_field(collection_name, field_name):
            response = input("Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        # Run benchmark
        benchmark.run_benchmark(collection_name, field_name, num_threads, total_operations)
        
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