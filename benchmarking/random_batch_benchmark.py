#!/usr/bin/env python3
"""
MongoDB Randomized Mixed Workload Benchmark Script
Performs a comprehensive benchmark with reads, aggregations, and joins in randomized batches
"""

import threading
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from datetime import datetime, timedelta
from collections import defaultdict

class MongoDBRandomizedBenchmark:
    def __init__(self, host="localhost", port=27017, db_name="ycsb_sharded"):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self.benchmark_results = defaultdict(list)
        self.overall_stats = {}
        
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
    
    def check_collection(self, collection_name):
        """Check if collection exists and has data"""
        try:
            collection = self.db[collection_name]
            doc_count = collection.estimated_document_count()
            if doc_count == 0:
                print(f"⚠ Warning: Collection '{collection_name}' is empty or doesn't exist")
                return False
            
            print(f"✓ Collection '{collection_name}' has {doc_count} documents")
            return True
            
        except Exception as e:
            print(f"✗ Error checking collection: {e}")
            return False
    
    def execute_read_batch(self, collection_name, field_name, field_value, batch_size, batch_id, query_type):
        """Execute a batch of read operations"""
        collection = self.db[collection_name]
        latencies = []
        
        for i in range(batch_size):
            try:
                query_start = time.time()
                cursor = collection.find({field_name: field_value})
                result_list = list(cursor)  # Force execution
                query_end = time.time()
                
                query_time_ms = (query_end - query_start) * 1000
                latencies.append(query_time_ms)
                
            except Exception as e:
                print(f"Error in {query_type} batch {batch_id}, operation {i + 1}: {e}")
        
        return latencies, query_type
    
    def execute_aggregation_batch(self, collection_name, batch_size, batch_id, query_type):
        """Execute a batch of aggregation operations"""
        collection = self.db[collection_name]
        latencies = []
        
        # Define aggregation pipeline
        pipeline = [
            {"$group": {
                "_id": "$country",
                "count": {"$sum": 1},
                "avg_age": {"$avg": "$age"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        for i in range(batch_size):
            try:
                query_start = time.time()
                cursor = collection.aggregate(pipeline)
                result_list = list(cursor)  # Force execution
                query_end = time.time()
                
                query_time_ms = (query_end - query_start) * 1000
                latencies.append(query_time_ms)
                
            except Exception as e:
                print(f"Error in {query_type} batch {batch_id}, operation {i + 1}: {e}")
        
        return latencies, query_type
    
    def execute_lookup_batch(self, main_collection, lookup_collection, batch_size, batch_id, query_type):
        """Execute a batch of lookup operations"""
        collection = self.db[main_collection]
        latencies = []
        
        # Define lookup pipeline
        pipeline = [
            {"$match": {"first_name": "Charles", "last_name": "Miller"}},  # Filter first
            {"$lookup": {
                "from": lookup_collection,
                "localField": "country",
                "foreignField": "country",
                "as": "related_data"
            }}
        ]
        
        for i in range(batch_size):
            try:
                query_start = time.time()
                cursor = collection.aggregate(pipeline)
                result_list = list(cursor)  # Force execution
                query_end = time.time()
                
                query_time_ms = (query_end - query_start) * 1000
                latencies.append(query_time_ms)
                
            except Exception as e:
                print(f"Error in {query_type} batch {batch_id}, operation {i + 1}: {e}")
        
        return latencies, query_type
    
    def create_batch_plan(self, total_operations):
        """Create a randomized batch execution plan"""
        # Calculate total operations for each query type
        read_lastname_total = int(total_operations * 0.4)
        read_firstname_total = int(total_operations * 0.4)
        aggregation_total = int(total_operations * 0.1)
        lookup_total = total_operations - read_lastname_total - read_firstname_total - aggregation_total
        
        # Calculate batch size (10% of total operations for each type)
        batch_size = max(1, total_operations // 100)  # At least 1 operation per batch
        
        print(f"\nBatch Configuration:")
        print(f"  Batch size: {batch_size} operations")
        print(f"  Total operations per type:")
        print(f"    Last name reads: {read_lastname_total}")
        print(f"    First name reads: {read_firstname_total}")
        print(f"    Aggregations: {aggregation_total}")
        print(f"    Lookups: {lookup_total}")
        
        # Create batches for each query type
        batches = []
        batch_id = 1
        
        # Create read_lastname batches
        remaining_lastname = read_lastname_total
        while remaining_lastname > 0:
            current_batch_size = min(batch_size, remaining_lastname)
            batches.append({
                'type': 'read_lastname',
                'size': current_batch_size,
                'id': batch_id,
                'description': f"Read Last Name (Miller) - Batch {batch_id}"
            })
            remaining_lastname -= current_batch_size
            batch_id += 1
        
        # Create read_firstname batches
        remaining_firstname = read_firstname_total
        while remaining_firstname > 0:
            current_batch_size = min(batch_size, remaining_firstname)
            batches.append({
                'type': 'read_firstname',
                'size': current_batch_size,
                'id': batch_id,
                'description': f"Read First Name (Charles) - Batch {batch_id}"
            })
            remaining_firstname -= current_batch_size
            batch_id += 1
        
        # Create aggregation batches
        remaining_aggregation = aggregation_total
        while remaining_aggregation > 0:
            current_batch_size = min(batch_size, remaining_aggregation)
            batches.append({
                'type': 'aggregation',
                'size': current_batch_size,
                'id': batch_id,
                'description': f"Aggregation Pipeline - Batch {batch_id}"
            })
            remaining_aggregation -= current_batch_size
            batch_id += 1
        
        # Create lookup batches
        remaining_lookup = lookup_total
        while remaining_lookup > 0:
            current_batch_size = min(batch_size, remaining_lookup)
            batches.append({
                'type': 'lookup',
                'size': current_batch_size,
                'id': batch_id,
                'description': f"Lookup Join - Batch {batch_id}"
            })
            remaining_lookup -= current_batch_size
            batch_id += 1
        
        # Randomize the batch order
        random.shuffle(batches)
        
        print(f"  Total batches created: {len(batches)}")
        print(f"  Batch execution order randomized")
        
        return batches
    
    def execute_batch(self, batch, main_collection, lookup_collection):
        """Execute a single batch based on its type"""
        batch_type = batch['type']
        batch_size = batch['size']
        batch_id = batch['id']
        
        if batch_type == 'read_lastname':
            return self.execute_read_batch(
                main_collection, "last_name", "Miller", 
                batch_size, batch_id, "Read Last Name"
            )
        elif batch_type == 'read_firstname':
            return self.execute_read_batch(
                main_collection, "first_name", "Charles", 
                batch_size, batch_id, "Read First Name"
            )
        elif batch_type == 'aggregation':
            return self.execute_aggregation_batch(
                main_collection, batch_size, batch_id, "Aggregation"
            )
        elif batch_type == 'lookup':
            return self.execute_lookup_batch(
                main_collection, lookup_collection, 
                batch_size, batch_id, "Lookup Join"
            )
        else:
            raise ValueError(f"Unknown batch type: {batch_type}")
    
    def calculate_stats(self, latencies, total_time, operations, benchmark_name):
        """Calculate benchmark statistics"""
        if not latencies:
            return {
                'name': benchmark_name,
                'operations': operations,
                'total_time': total_time,
                'avg_latency': 0,
                'min_latency': 0,
                'max_latency': 0,
                'throughput': 0,
                'p50': 0,
                'p95': 0,
                'p99': 0
            }
        
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        throughput = operations / total_time if total_time > 0 else 0
        
        # Calculate percentiles
        sorted_latencies = sorted(latencies)
        p50 = sorted_latencies[len(sorted_latencies) // 2]
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        
        return {
            'name': benchmark_name,
            'operations': operations,
            'total_time': total_time,
            'avg_latency': avg_latency,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'throughput': throughput,
            'p50': p50,
            'p95': p95,
            'p99': p99
        }
    
    def print_progress(self, completed_batches, total_batches, elapsed_time):
        """Print progress information"""
        progress_percent = (completed_batches / total_batches) * 100
        if completed_batches > 0:
            estimated_total_time = elapsed_time * total_batches / completed_batches
            time_left = estimated_total_time - elapsed_time
            print(f"Progress: {completed_batches}/{total_batches} batches completed "
                  f"({progress_percent:.1f}%) - ETA: {time_left:.1f} seconds")
    
    def print_batch_summary(self):
        """Print summary of all completed batches by type"""
        print(f"\n{'='*80}")
        print(f"BATCH EXECUTION SUMMARY")
        print(f"{'='*80}")
        
        # Aggregate results by query type
        type_stats = {}
        for query_type, all_latencies in self.benchmark_results.items():
            flattened_latencies = [lat for batch_latencies in all_latencies for lat in batch_latencies]
            
            if flattened_latencies:
                type_stats[query_type] = {
                    'operations': len(flattened_latencies),
                    'avg_latency': sum(flattened_latencies) / len(flattened_latencies),
                    'min_latency': min(flattened_latencies),
                    'max_latency': max(flattened_latencies),
                    'p50': sorted(flattened_latencies)[len(flattened_latencies) // 2],
                    'p95': sorted(flattened_latencies)[int(len(flattened_latencies) * 0.95)],
                    'p99': sorted(flattened_latencies)[int(len(flattened_latencies) * 0.99)]
                }
        
        # Print per-type statistics
        print(f"{'Query Type':<25} {'Operations':<12} {'Avg Latency':<15} {'P50':<10} {'P95':<10} {'P99':<10}")
        print(f"{'-'*25} {'-'*12} {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
        
        for query_type, stats in type_stats.items():
            print(f"{query_type:<25} {stats['operations']:<12} "
                  f"{stats['avg_latency']:<15.2f} {stats['p50']:<10.2f} "
                  f"{stats['p95']:<10.2f} {stats['p99']:<10.2f}")
    
    def print_final_summary(self, total_time, total_operations):
        """Print final benchmark summary"""
        print(f"\n{'='*80}")
        print(f"FINAL RANDOMIZED BENCHMARK SUMMARY")
        print(f"{'='*80}")
        
        print(f"Total operations executed: {total_operations}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Overall throughput: {total_operations / total_time:.2f} operations/second")
        
        # Calculate overall statistics
        all_latencies = []
        for query_type, batch_latencies_list in self.benchmark_results.items():
            for batch_latencies in batch_latencies_list:
                all_latencies.extend(batch_latencies)
        
        if all_latencies:
            overall_avg = sum(all_latencies) / len(all_latencies)
            sorted_all = sorted(all_latencies)
            overall_p50 = sorted_all[len(sorted_all) // 2]
            overall_p95 = sorted_all[int(len(sorted_all) * 0.95)]
            overall_p99 = sorted_all[int(len(sorted_all) * 0.99)]
            
            print(f"\nOverall Latency Statistics:")
            print(f"  Average: {overall_avg:.2f} ms")
            print(f"  50th percentile: {overall_p50:.2f} ms")
            print(f"  95th percentile: {overall_p95:.2f} ms")
            print(f"  99th percentile: {overall_p99:.2f} ms")
            print(f"  Minimum: {min(all_latencies):.2f} ms")
            print(f"  Maximum: {max(all_latencies):.2f} ms")
    
    def run_randomized_benchmark(self, main_collection, lookup_collection, total_operations):
        """Run the randomized mixed workload benchmark"""
        print(f"\n{'='*80}")
        print(f"MONGODB RANDOMIZED MIXED WORKLOAD BENCHMARK")
        print(f"{'='*80}")
        print(f"Main collection: {main_collection}")
        print(f"Lookup collection: {lookup_collection}")
        print(f"Total operations: {total_operations}")
        print(f"Workload distribution (same as original):")
        print(f"  40% Read queries on 'last_name' field (Miller)")
        print(f"  40% Read queries on 'first_name' field (Charles)")
        print(f"  10% Aggregation pipeline operations")
        print(f"  10% Join operations ($match + $lookup)")
        print(f"Execution method: Randomized small batches")
        print(f"{'='*80}")
        
        # Create batch execution plan
        batches = self.create_batch_plan(total_operations)
        
        # Execute batches in randomized order
        overall_start = time.time()
        completed_batches = 0
        progress_interval = max(1, len(batches) // 20)  # Show progress every 5%
        
        print(f"\nStarting randomized batch execution...")
        print(f"Total batches to execute: {len(batches)}")
        
        try:
            for i, batch in enumerate(batches):
                batch_start = time.time()
                
                # Execute the batch
                latencies, query_type = self.execute_batch(batch, main_collection, lookup_collection)
                
                # Store results
                self.benchmark_results[query_type].append(latencies)
                
                batch_end = time.time()
                completed_batches += 1
                
                # Print progress periodically
                if completed_batches % progress_interval == 0 or completed_batches == len(batches):
                    elapsed_time = batch_end - overall_start
                    self.print_progress(completed_batches, len(batches), elapsed_time)
                
                # Print batch completion info
                batch_time = batch_end - batch_start
                avg_latency = sum(latencies) / len(latencies) if latencies else 0
                print(f"  Completed: {batch['description']} - "
                      f"{batch['size']} ops in {batch_time:.2f}s "
                      f"(avg: {avg_latency:.2f}ms)")
        
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user")
            return
        except Exception as e:
            print(f"Benchmark error: {e}")
            return
        
        overall_end = time.time()
        total_time = overall_end - overall_start
        
        # Print results
        self.print_batch_summary()
        self.print_final_summary(total_time, total_operations)
        
        print(f"\nRandomized benchmark completed successfully!")
        print(f"Execution pattern: Small batches executed in random order")
        print(f"This simulates a more realistic mixed workload scenario")
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()

def get_user_input():
    """Get benchmark parameters from user input"""
    print("MongoDB Randomized Mixed Workload Benchmark Configuration")
    print("-" * 60)
    
    # Main collection name
    main_collection = input("Enter main collection name (default: usertable): ").strip()
    if not main_collection:
        main_collection = "usertable"
    
    # Lookup collection name
    lookup_collection = input("Enter lookup collection name (default: usertable_lookup): ").strip()
    if not lookup_collection:
        lookup_collection = "usertable_lookup"
    
    # Total operations
    while True:
        try:
            operations = input("Enter total number of operations (default: 1000): ").strip()
            operations = int(operations) if operations else 1000
            if operations <= 0:
                print("Number of operations must be positive")
                continue
            if operations < 10:
                print("Minimum 10 operations required for meaningful distribution")
                continue
            break
        except ValueError:
            print("Please enter a valid number")
    
    return main_collection, lookup_collection, operations

def main():
    """Main function"""
    print("MongoDB Randomized Mixed Workload Benchmark Tool")
    print("=" * 60)
    
    # Set random seed for reproducible results (optional)
    # random.seed(42)  # Uncomment for reproducible batch order
    
    # Initialize benchmark
    benchmark = MongoDBRandomizedBenchmark()
    
    # Test connection first
    if not benchmark.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        main_collection, lookup_collection, total_operations = get_user_input()
        
        # Validate collections
        print(f"\nValidating collections...")
        main_valid = benchmark.check_collection(main_collection)
        lookup_valid = benchmark.check_collection(lookup_collection)
        
        if not main_valid:
            response = input(f"Main collection '{main_collection}' validation failed. Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        if not lookup_valid:
            response = input(f"Lookup collection '{lookup_collection}' validation failed. Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        # Run randomized benchmark
        benchmark.run_randomized_benchmark(main_collection, lookup_collection, total_operations)
        
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