#!/usr/bin/env python3
"""
MongoDB Mixed Workload Benchmark Script
Performs a comprehensive benchmark with reads, aggregations, and joins
"""

import threading
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from datetime import datetime, timedelta

class MongoDBMixedBenchmark:
    def __init__(self, host="localhost", port=27017, db_name="ycsb_sharded"):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self.benchmark_results = {}
        
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
    
    def run_read_benchmark(self, collection_name, field_name, field_value, operations, benchmark_name):
        """Run read benchmark for a specific field/value"""
        print(f"\n{'='*60}")
        print(f"STARTING: {benchmark_name}")
        print(f"Query: {{{field_name}: \"{field_value}\"}}")
        print(f"Operations: {operations}")
        print(f"{'='*60}")
        
        collection = self.db[collection_name]
        results = []
        start_time = time.time()
        
        # Progress tracking
        progress_interval = 100
        last_progress_time = start_time
        
        for i in range(operations):
            try:
                query_start = time.time()
                
                # Perform read operation
                cursor = collection.find({field_name: field_value})
                result_list = list(cursor)  # Force execution
                
                query_end = time.time()
                query_time_ms = (query_end - query_start) * 1000
                results.append(query_time_ms)
                
                # Progress reporting
                if (i + 1) % progress_interval == 0:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    ops_per_sec = (i + 1) / elapsed
                    estimated_total_time = operations / ops_per_sec
                    time_left = estimated_total_time - elapsed
                    
                    print(f"Progress: {i + 1}/{operations} operations completed "
                          f"({((i + 1) / operations * 100):.1f}%) - "
                          f"ETA: {time_left:.1f} seconds")
                
            except Exception as e:
                print(f"Error in operation {i + 1}: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Calculate statistics
        stats = self.calculate_stats(results, total_time, operations, benchmark_name)
        self.benchmark_results[benchmark_name] = stats
        self.print_benchmark_results(stats)
        
        return stats
    
    def run_aggregation_benchmark(self, collection_name, operations, benchmark_name):
        """Run aggregation benchmark"""
        print(f"\n{'='*60}")
        print(f"STARTING: {benchmark_name}")
        print(f"Pipeline: Single collection aggregation with grouping")
        print(f"Operations: {operations}")
        print(f"{'='*60}")
        
        collection = self.db[collection_name]
        results = []
        start_time = time.time()
        
        # Progress tracking
        progress_interval = 100
        
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
        
        print(f"Pipeline: {pipeline}")
        
        for i in range(operations):
            try:
                query_start = time.time()
                
                # Perform aggregation
                cursor = collection.aggregate(pipeline)
                result_list = list(cursor)  # Force execution
                
                query_end = time.time()
                query_time_ms = (query_end - query_start) * 1000
                results.append(query_time_ms)
                
                # Progress reporting
                if (i + 1) % progress_interval == 0:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    ops_per_sec = (i + 1) / elapsed
                    estimated_total_time = operations / ops_per_sec
                    time_left = estimated_total_time - elapsed
                    
                    print(f"Progress: {i + 1}/{operations} operations completed "
                          f"({((i + 1) / operations * 100):.1f}%) - "
                          f"ETA: {time_left:.1f} seconds")
                
            except Exception as e:
                print(f"Error in operation {i + 1}: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Calculate statistics
        stats = self.calculate_stats(results, total_time, operations, benchmark_name)
        self.benchmark_results[benchmark_name] = stats
        self.print_benchmark_results(stats)
        
        return stats
    
    def run_lookup_benchmark(self, main_collection, lookup_collection, operations, benchmark_name):
        """Run aggregation benchmark with $lookup (join operation)"""
        print(f"\n{'='*60}")
        print(f"STARTING: {benchmark_name}")
        print(f"Pipeline: $match + $lookup join operation")
        print(f"Main collection: {main_collection}")
        print(f"Lookup collection: {lookup_collection}")
        print(f"Operations: {operations}")
        print(f"{'='*60}")
        
        collection = self.db[main_collection]
        results = []
        start_time = time.time( )
        
        # Progress tracking
        progress_interval = 100
        
        # Define lookup pipeline
        pipeline = [
            {"$match": {"first_name": "Charles", "last_name": "Miller"}},  # Filter first
            {"$lookup": {
                "from": lookup_collection,
                "localField": "first_name",
                "foreignField": "first_name",
                "as": "related_data"
            }}
        ]
        
        print(f"Pipeline: {pipeline}")
        
        for i in range(operations):
            try:
                query_start = time.time()
                
                # Perform lookup aggregation
                cursor = collection.aggregate(pipeline)
                result_list = list(cursor)  # Force execution
                
                query_end = time.time()
                query_time_ms = (query_end - query_start) * 1000
                results.append(query_time_ms)
                
                # Progress reporting
                if (i + 1) % progress_interval == 0:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    ops_per_sec = (i + 1) / elapsed
                    estimated_total_time = operations / ops_per_sec
                    time_left = estimated_total_time - elapsed
                    
                    print(f"Progress: {i + 1}/{operations} operations completed "
                          f"({((i + 1) / operations * 100):.1f}%) - "
                          f"ETA: {time_left:.1f} seconds")
                
            except Exception as e:
                print(f"Error in operation {i + 1}: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Calculate statistics
        stats = self.calculate_stats(results, total_time, operations, benchmark_name)
        self.benchmark_results[benchmark_name] = stats
        self.print_benchmark_results(stats)
        
        return stats
    
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
        throughput = operations / total_time
        
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
    
    def print_benchmark_results(self, stats):
        """Print results for a single benchmark"""
        print(f"\n{'='*50}")
        print(f"COMPLETED: {stats['name']}")
        print(f"{'='*50}")
        print(f"Total time: {stats['total_time']:.2f} seconds")
        print(f"Operations: {stats['operations']}")
        print(f"Throughput: {stats['throughput']:.2f} operations/second")
        print(f"\nLatency Statistics (milliseconds):")
        print(f"  Average: {stats['avg_latency']:.2f} ms")
        print(f"  Minimum: {stats['min_latency']:.2f} ms")
        print(f"  Maximum: {stats['max_latency']:.2f} ms")
        print(f"  50th percentile: {stats['p50']:.2f} ms")
        print(f"  95th percentile: {stats['p95']:.2f} ms")
        print(f"  99th percentile: {stats['p99']:.2f} ms")
    
    def print_final_summary(self):
        """Print final benchmark summary"""
        print(f"\n{'='*80}")
        print(f"FINAL BENCHMARK SUMMARY")
        print(f"{'='*80}")
        
        total_operations = sum(stats['operations'] for stats in self.benchmark_results.values())
        total_time = sum(stats['total_time'] for stats in self.benchmark_results.values())
        
        print(f"Total operations across all benchmarks: {total_operations}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Overall average throughput: {total_operations / total_time:.2f} operations/second")
        
        print(f"\nPer-benchmark breakdown:")
        print(f"{'Benchmark':<30} {'Operations':<12} {'Time (s)':<10} {'Throughput':<15} {'Avg Latency':<15}")
        print(f"{'-'*30} {'-'*12} {'-'*10} {'-'*15} {'-'*15}")
        
        for stats in self.benchmark_results.values():
            print(f"{stats['name']:<30} {stats['operations']:<12} "
                  f"{stats['total_time']:<10.2f} {stats['throughput']:<15.2f} "
                  f"{stats['avg_latency']:<15.2f}")
        
        # Find best and worst performing benchmarks
        if self.benchmark_results:
            best_throughput = max(self.benchmark_results.values(), key=lambda x: x['throughput'])
            worst_throughput = min(self.benchmark_results.values(), key=lambda x: x['throughput'])
            best_latency = min(self.benchmark_results.values(), key=lambda x: x['avg_latency'])
            worst_latency = max(self.benchmark_results.values(), key=lambda x: x['avg_latency'])
            
            print(f"\nPerformance Analysis:")
            print(f"  Highest throughput: {best_throughput['name']} ({best_throughput['throughput']:.2f} ops/sec)")
            print(f"  Lowest throughput: {worst_throughput['name']} ({worst_throughput['throughput']:.2f} ops/sec)")
            print(f"  Best latency: {best_latency['name']} ({best_latency['avg_latency']:.2f} ms)")
            print(f"  Worst latency: {worst_latency['name']} ({worst_latency['avg_latency']:.2f} ms)")
    
    def run_mixed_benchmark(self, main_collection, lookup_collection, total_operations):
        """Run the complete mixed workload benchmark"""
        print(f"\n{'='*80}")
        print(f"MONGODB MIXED WORKLOAD BENCHMARK")
        print(f"{'='*80}")
        print(f"Main collection: {main_collection}")
        print(f"Lookup collection: {lookup_collection}")
        print(f"Total operations: {total_operations}")
        print(f"Workload distribution:")
        print(f"  40% Read queries on 'last_name' field (Miller)")
        print(f"  40% Read queries on 'first_name' field (Charles)")
        print(f"  10% Aggregation pipeline operations")
        print(f"  10% Join operations ($match + $lookup)")
        print(f"{'='*80}")
        
        # Calculate operations for each benchmark
        read_lastname_ops = int(total_operations * 0.4)
        read_firstname_ops = int(total_operations * 0.4)
        aggregation_ops = int(total_operations * 0.1)
        lookup_ops = total_operations - read_lastname_ops - read_firstname_ops - aggregation_ops  # Remaining
        
        print(f"\nOperations per benchmark:")
        print(f"  Last name reads: {read_lastname_ops}")
        print(f"  First name reads: {read_firstname_ops}")
        print(f"  Aggregations: {aggregation_ops}")
        print(f"  Lookups: {lookup_ops}")
        
        overall_start = time.time()
        
        # Run benchmarks in sequence
        try:
            # 1. Read benchmark on last_name field
            self.run_read_benchmark(
                main_collection, 
                "last_name", 
                "Miller", 
                read_lastname_ops,
                "Read Queries - Last Name (Miller)"
            )
            
            # 2. Read benchmark on first_name field
            self.run_read_benchmark(
                main_collection, 
                "first_name", 
                "Charles", 
                read_firstname_ops,
                "Read Queries - First Name (Charles)"
            )
            
            # 3. Aggregation benchmark
            self.run_aggregation_benchmark(
                main_collection,
                aggregation_ops,
                "Aggregation Pipeline - Single Collection"
            )
            
            # 4. Lookup benchmark
            self.run_lookup_benchmark(
                main_collection,
                lookup_collection,
                lookup_ops,
                "Join Operations - Match + Lookup"
            )
            
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user")
            return
        except Exception as e:
            print(f"Benchmark error: {e}")
            return
        
        overall_end = time.time()
        overall_time = overall_end - overall_start
        
        print(f"\nTotal benchmark execution time: {overall_time:.2f} seconds")
        
        # Print final summary
        self.print_final_summary()
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()

def get_user_input():
    """Get benchmark parameters from user input"""
    print("MongoDB Mixed Workload Benchmark Configuration")
    print("-" * 50)
    
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
    print("MongoDB Mixed Workload Benchmark Tool")
    print("=" * 50)
    
    # Initialize benchmark
    benchmark = MongoDBMixedBenchmark()
    
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
        
        # Run mixed benchmark
        benchmark.run_mixed_benchmark(main_collection, lookup_collection, total_operations)
        
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