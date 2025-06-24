#!/usr/bin/env python3
"""
MongoDB Join Operations Benchmark Script
Configurable benchmark for testing $match + $lookup operations with user-defined operation count
Supports multi-threading for concurrent execution
"""

import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import sys
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import copy


class MongoDBJoinBenchmark:
    def __init__(self, host="localhost", port=27017, db_name="ycsb_sharded"):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self.benchmark_results = {}
        self.results_lock = threading.Lock()
        
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
    
    def create_thread_connection(self):
        """Create a new MongoDB connection for thread-safe operations"""
        try:
            client = MongoClient(f"mongodb://{self.host}:{self.port}", 
                               serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client[self.db_name]
        except Exception as e:
            print(f"✗ Failed to create thread connection: {e}")
            return None
    
    def check_collection(self, collection_name):
        """Check if collection exists and has data"""
        try:
            collection = self.db[collection_name]
            doc_count = collection.estimated_document_count()
            if doc_count == 0:
                print(f"⚠ Warning: Collection '{collection_name}' is empty or doesn't exist")
                return False, 0
            
            print(f"✓ Collection '{collection_name}' has {doc_count} documents")
            return True, doc_count
            
        except Exception as e:
            print(f"✗ Error checking collection: {e}")
            return False, 0
    
    def run_single_join_operation(self, from_collection, to_collection, match_conditions, 
                                 join_field, operation_id, thread_id=0, db_connection=None):
        """Run a single join operation with $match + $lookup"""
        if db_connection is None:
            db_connection = self.db
            
        thread_info = f"[Thread-{thread_id}]" if thread_id > 0 else ""
        
        collection = db_connection[from_collection]
        
        # Define the join pipeline
        pipeline = [
            {"$match": match_conditions},
            {"$lookup": {
                "from": to_collection,
                "localField": join_field,
                "foreignField": join_field,
                "as": "joined_data"
            }},
            {"$limit": 1000}  # Limit to avoid excessive memory usage
        ]
        
        try:
            start_time = time.time()
            
            # Perform the join operation
            cursor = collection.aggregate(pipeline)
            result_list = list(cursor)  # Force execution
            
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000
            
            print(f"{thread_info} Operation {operation_id}: {execution_time_ms:.2f}ms, {len(result_list)} docs")
            
            # Store results (thread-safe)
            result = {
                'operation_id': operation_id,
                'from_collection': from_collection,
                'to_collection': to_collection,
                'match_conditions': match_conditions,
                'join_field': join_field,
                'execution_time_ms': execution_time_ms,
                'documents_matched': len(result_list),
                'success': True,
                'thread_id': thread_id,
                'timestamp': datetime.now()
            }
            
            with self.results_lock:
                self.benchmark_results[f"op_{operation_id}_thread_{thread_id}"] = result
            
            return result
            
        except Exception as e:
            execution_time_ms = 0
            print(f"{thread_info} Operation {operation_id}: FAILED - {e}")
            
            result = {
                'operation_id': operation_id,
                'from_collection': from_collection,
                'to_collection': to_collection,
                'match_conditions': match_conditions,
                'join_field': join_field,
                'execution_time_ms': execution_time_ms,
                'documents_matched': 0,
                'success': False,
                'error': str(e),
                'thread_id': thread_id,
                'timestamp': datetime.now()
            }
            
            with self.results_lock:
                self.benchmark_results[f"op_{operation_id}_thread_{thread_id}"] = result
            
            return result
    
    def worker_thread_function(self, operations_to_execute, thread_id, from_collection, 
                              to_collection, match_conditions, join_field, results_queue):
        """Function executed by each worker thread"""
        thread_results = []
        
        # Create thread-specific database connection
        db_connection = self.create_thread_connection()
        if db_connection is None:
            print(f"[Thread-{thread_id}] Failed to create database connection")
            results_queue.put([])
            return
        
        print(f"[Thread-{thread_id}] Starting {operations_to_execute} operations")
        
        try:
            for i in range(operations_to_execute):
                operation_id = thread_id * 1000 + i + 1  # Unique operation ID
                
                result = self.run_single_join_operation(
                    from_collection=from_collection,
                    to_collection=to_collection,
                    match_conditions=match_conditions,
                    join_field=join_field,
                    operation_id=operation_id,
                    thread_id=thread_id,
                    db_connection=db_connection
                )
                thread_results.append(result)
                
        except Exception as e:
            print(f"[Thread-{thread_id}] Error: {e}")
        finally:
            # Close thread-specific connection
            if hasattr(db_connection, 'client'):
                db_connection.client.close()
            
            print(f"[Thread-{thread_id}] Completed {len(thread_results)} operations")
        
        results_queue.put(thread_results)
    
    def distribute_operations_to_threads(self, total_operations, num_threads):
        """Distribute operations across threads"""
        operations_per_thread = total_operations // num_threads
        remainder = total_operations % num_threads
        
        distribution = []
        for i in range(num_threads):
            # Some threads get one extra operation if there's a remainder
            current_thread_ops = operations_per_thread + (1 if i < remainder else 0)
            distribution.append(current_thread_ops)
        
        return distribution
    
    def print_final_summary(self, num_threads, total_operations, total_execution_time, 
                           from_collection, to_collection, match_conditions, join_field):
        """Print comprehensive final summary including threading information"""
        print(f"\n{'='*100}")
        print(f"MONGODB JOIN BENCHMARK RESULTS")
        print(f"{'='*100}")
        
        if not self.benchmark_results:
            print("No benchmark results to display")
            return
        
        successful_operations = [r for r in self.benchmark_results.values() if r['success']]
        failed_operations = [r for r in self.benchmark_results.values() if not r['success']]
        
        print(f"EXECUTION OVERVIEW:")
        print(f"  Total threads used: {num_threads}")
        print(f"  Total operations executed: {len(self.benchmark_results)}")
        print(f"  Successful operations: {len(successful_operations)}")
        print(f"  Failed operations: {len(failed_operations)}")
        print(f"  Total execution time: {total_execution_time:.2f} seconds")
        print(f"  Operations per second: {len(successful_operations) / total_execution_time:.2f} ops/sec")
        
        print(f"\nOPERATION CONFIGURATION:")
        print(f"  From collection: {from_collection}")
        print(f"  To collection: {to_collection}")
        print(f"  Join field: {join_field}")
        print(f"  Match conditions: {match_conditions}")
        
        # Thread distribution analysis
        thread_distribution = {}
        for result in self.benchmark_results.values():
            thread_id = result.get('thread_id', 0)
            if thread_id not in thread_distribution:
                thread_distribution[thread_id] = {'total': 0, 'successful': 0, 'failed': 0, 'total_time': 0}
            thread_distribution[thread_id]['total'] += 1
            if result['success']:
                thread_distribution[thread_id]['successful'] += 1
                thread_distribution[thread_id]['total_time'] += result['execution_time_ms']
            else:
                thread_distribution[thread_id]['failed'] += 1
        
        print(f"\nTHREAD DISTRIBUTION:")
        print(f"{'Thread':<8} {'Total':<8} {'Success':<8} {'Failed':<8} {'Avg Time(ms)':<15}")
        print(f"{'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*15}")
        for thread_id in sorted(thread_distribution.keys()):
            stats = thread_distribution[thread_id]
            avg_time = stats['total_time'] / stats['successful'] if stats['successful'] > 0 else 0
            print(f"{thread_id:<8} {stats['total']:<8} {stats['successful']:<8} {stats['failed']:<8} {avg_time:<15.2f}")
        
        if successful_operations:
            # Performance analysis
            print(f"\nPERFORMANCE ANALYSIS:")
            
            # Overall statistics
            all_times = [r['execution_time_ms'] for r in successful_operations]
            all_docs = [r['documents_matched'] for r in successful_operations]
            
            avg_time = sum(all_times) / len(all_times)
            min_time = min(all_times)
            max_time = max(all_times)
            avg_docs = sum(all_docs) / len(all_docs)
            
            print(f"\nOVERALL PERFORMANCE:")
            print(f"  Total successful operations: {len(successful_operations)}")
            print(f"  Average execution time: {avg_time:.2f} ms")
            print(f"  Minimum execution time: {min_time:.2f} ms")
            print(f"  Maximum execution time: {max_time:.2f} ms")
            print(f"  Time standard deviation: {(sum((t - avg_time) ** 2 for t in all_times) / len(all_times)) ** 0.5:.2f} ms")
            print(f"  Average documents matched: {avg_docs:.1f}")
            print(f"  Total documents processed: {sum(all_docs):,}")
            
            # Percentiles
            sorted_times = sorted(all_times)
            p50 = sorted_times[len(sorted_times) // 2]
            p95 = sorted_times[int(len(sorted_times) * 0.95)]
            p99 = sorted_times[int(len(sorted_times) * 0.99)]
            
            print(f"\nPERFORMANCE PERCENTILES:")
            print(f"  50th percentile (median): {p50:.2f} ms")
            print(f"  95th percentile: {p95:.2f} ms")
            print(f"  99th percentile: {p99:.2f} ms")
            
            # Thread performance comparison
            if num_threads > 1:
                print(f"\nTHREAD PERFORMANCE COMPARISON:")
                thread_times = {}
                for result in successful_operations:
                    thread_id = result.get('thread_id', 0)
                    if thread_id not in thread_times:
                        thread_times[thread_id] = []
                    thread_times[thread_id].append(result['execution_time_ms'])
                
                print(f"{'Thread':<8} {'Operations':<12} {'Avg Time(ms)':<15} {'Min Time(ms)':<15} {'Max Time(ms)':<15}")
                print(f"{'-'*8} {'-'*12} {'-'*15} {'-'*15} {'-'*15}")
                
                thread_avg_times = []
                for thread_id in sorted(thread_times.keys()):
                    times = thread_times[thread_id]
                    avg_t = sum(times) / len(times)
                    min_t = min(times)
                    max_t = max(times)
                    count = len(times)
                    thread_avg_times.append(avg_t)
                    print(f"{thread_id:<8} {count:<12} {avg_t:<15.2f} {min_t:<15.2f} {max_t:<15.2f}")
                
                # Thread performance variance
                if len(thread_avg_times) > 1:
                    thread_avg = sum(thread_avg_times) / len(thread_avg_times)
                    thread_variance = sum((t - thread_avg) ** 2 for t in thread_avg_times) / len(thread_avg_times)
                    thread_std = thread_variance ** 0.5
                    print(f"\nThread performance variance: {thread_std:.2f} ms")
            
            # Concurrency analysis
            if num_threads > 1:
                print(f"\nCONCURRENCY ANALYSIS:")
                estimated_sequential_time = sum(all_times) / 1000  # Convert to seconds
                actual_parallel_time = total_execution_time
                theoretical_speedup = estimated_sequential_time / actual_parallel_time if actual_parallel_time > 0 else 0
                efficiency = (theoretical_speedup / num_threads) * 100 if num_threads > 0 else 0
                
                print(f"  Estimated sequential execution time: {estimated_sequential_time:.2f} seconds")
                print(f"  Actual parallel execution time: {actual_parallel_time:.2f} seconds")
                print(f"  Theoretical speedup: {theoretical_speedup:.2f}x")
                print(f"  Parallel efficiency: {efficiency:.1f}%")
                
                # Throughput analysis
                sequential_ops_per_sec = total_operations / estimated_sequential_time
                parallel_ops_per_sec = total_operations / actual_parallel_time
                
                print(f"  Sequential throughput: {sequential_ops_per_sec:.2f} ops/sec")
                print(f"  Parallel throughput: {parallel_ops_per_sec:.2f} ops/sec")
                print(f"  Throughput improvement: {parallel_ops_per_sec / sequential_ops_per_sec:.2f}x")
        
        if failed_operations:
            print(f"\nFAILED OPERATIONS:")
            error_summary = {}
            for result in failed_operations:
                error = result.get('error', 'Unknown error')
                thread_id = result.get('thread_id', 0)
                if error not in error_summary:
                    error_summary[error] = []
                error_summary[error].append(thread_id)
            
            for error, threads in error_summary.items():
                print(f"  ✗ {error}: {len(threads)} operation(s) in thread(s) {sorted(set(threads))}")
    
    def run_join_benchmark(self, from_collection, to_collection, match_conditions, 
                          join_field, total_operations, num_threads=1):
        """Run join benchmark with configurable operation count and threading"""
        print(f"\n{'='*100}")
        print(f"MONGODB JOIN BENCHMARK")
        print(f"{'='*100}")
        print(f"From collection: {from_collection}")
        print(f"To collection: {to_collection}")
        print(f"Join field: {join_field}")
        print(f"Match conditions: {match_conditions}")
        print(f"Total operations: {total_operations:,}")
        print(f"Number of threads: {num_threads}")
        print(f"{'='*100}")
        
        overall_start = time.time()
        
        # Distribute operations across threads
        operations_distribution = self.distribute_operations_to_threads(total_operations, num_threads)
        
        print(f"\nOPERATION DISTRIBUTION:")
        total_distributed = 0
        for i, ops_count in enumerate(operations_distribution):
            if ops_count > 0:
                print(f"  Thread {i}: {ops_count:,} operations")
                total_distributed += ops_count
        print(f"  Total distributed: {total_distributed:,} operations")
        
        try:
            if num_threads == 1:
                # Single-threaded execution
                print(f"\nExecuting {total_operations:,} operations in single-threaded mode...")
                db_connection = self.db
                
                for i in range(total_operations):
                    operation_id = i + 1
                    if (i + 1) % max(1, total_operations // 10) == 0 or i == 0:
                        print(f"Progress: {i + 1:,}/{total_operations:,} operations ({((i + 1) / total_operations * 100):.1f}%)")
                    
                    self.run_single_join_operation(
                        from_collection=from_collection,
                        to_collection=to_collection,
                        match_conditions=match_conditions,
                        join_field=join_field,
                        operation_id=operation_id,
                        thread_id=0,
                        db_connection=db_connection
                    )
            else:
                # Multi-threaded execution
                print(f"\nExecuting {total_operations:,} operations in multi-threaded mode...")
                results_queue = Queue()
                threads = []
                
                # Start threads
                for thread_id, ops_count in enumerate(operations_distribution):
                    if ops_count > 0:  # Only start threads that have operations
                        thread = threading.Thread(
                            target=self.worker_thread_function,
                            args=(ops_count, thread_id, from_collection, to_collection, 
                                 match_conditions, join_field, results_queue)
                        )
                        thread.start()
                        threads.append(thread)
                
                print(f"  Started {len(threads)} worker threads")
                
                # Wait for all threads to complete
                for thread in threads:
                    thread.join()
                
                print(f"\n✓ All {len(threads)} threads completed")
                
                # Collect results from queue
                while not results_queue.empty():
                    thread_results = results_queue.get()
                    # Results are already stored in self.benchmark_results by worker threads
            
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user")
            return
        except Exception as e:
            print(f"Benchmark error: {e}")
            return
        
        overall_end = time.time()
        overall_time = overall_end - overall_start
        
        print(f"\nBenchmark completed in {overall_time:.2f} seconds")
        
        # Print comprehensive summary
        self.print_final_summary(num_threads, total_operations, overall_time, 
                                from_collection, to_collection, match_conditions, join_field)
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()


def get_user_input():
    """Get benchmark parameters from user input"""
    print("MongoDB Join Operations Benchmark Configuration")
    print("-" * 60)
    
    # Collection 1 (from)
    from_collection = input("Enter source collection name: ").strip()
    if not from_collection:
        print("Source collection name is required")
        return None, None, None, None, None, None
    
    # Collection 2 (to)
    to_collection = input("Enter target collection name: ").strip()
    if not to_collection:
        print("Target collection name is required")
        return None, None, None, None, None, None
    
    # Join field
    join_field = input("Enter join field name (default 'first_name'): ").strip()
    if not join_field:
        join_field = 'first_name'
    
    # Match conditions
    print("\nMatch conditions examples:")
    print("  Simple: {\"first_name\": \"John\"}")
    print("  Complex: {\"first_name\": \"John\", \"age\": {\"$gte\": 25}}")
    match_input = input("Enter match conditions (JSON format, default {\"first_name\": \"John\"}): ").strip()
    if not match_input:
        match_conditions = {"first_name": "John"}
    else:
        try:
            import ast
            match_conditions = ast.literal_eval(match_input)
        except:
            print("Invalid JSON format, using default")
            match_conditions = {"first_name": "John"}
    
    # Total operations
    while True:
        try:
            ops_input = input("Enter total number of operations (default 100): ").strip()
            if not ops_input:
                total_operations = 100
                break
            
            total_operations = int(ops_input)
            if total_operations > 0:
                break
            else:
                print("Number of operations must be greater than 0")
                
        except ValueError:
            print("Please enter a valid integer")
    
    # Number of threads
    while True:
        try:
            threads_input = input("Enter number of threads (1-36, default 1): ").strip()
            if not threads_input:
                num_threads = 1
                break
            
            num_threads = int(threads_input)
            if 1 <= num_threads <= 36:
                break
            else:
                print("Number of threads must be between 1 and 36")
                
        except ValueError:
            print("Please enter a valid integer")
    
    return from_collection, to_collection, join_field, match_conditions, total_operations, num_threads


def main():
    """Main function"""
    print("MongoDB Configurable Join Operations Benchmark Tool")
    print("=" * 60)
    print("This tool performs configurable join operations:")
    print("• User-defined number of total operations")
    print("• Configurable source and target collections")
    print("• Configurable join field and match conditions")
    print("• Multi-threading support (1-36 threads)")
    print("• Operations distributed equally across threads")
    print("• Standardized pipeline: $match + $lookup + $limit")
    print("=" * 60)
    
    # Initialize benchmark
    benchmark = MongoDBJoinBenchmark()
    
    # Test connection first
    if not benchmark.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        result = get_user_input()
        if None in result:
            print("Invalid input provided")
            return 1
        
        from_collection, to_collection, join_field, match_conditions, total_operations, num_threads = result
        
        # Validate collections
        print(f"\nValidating collections...")
        from_valid, from_count = benchmark.check_collection(from_collection)
        to_valid, to_count = benchmark.check_collection(to_collection)
        
        if not from_valid or not to_valid:
            response = input("One or more collections validation failed. Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        print(f"\nBenchmark Configuration:")
        print(f"  Source collection: {from_collection} ({from_count:,} documents)")
        print(f"  Target collection: {to_collection} ({to_count:,} documents)")
        print(f"  Join field: {join_field}")
        print(f"  Match conditions: {match_conditions}")
        print(f"  Total operations: {total_operations:,}")
        print(f"  Number of threads: {num_threads}")
        
        # Show pipeline
        print(f"\nPipeline to be executed:")
        print(f"  1. $match: {match_conditions}")
        print(f"  2. $lookup: join {from_collection}.{join_field} with {to_collection}.{join_field}")
        print(f"  3. $limit: 1000 (to avoid excessive memory usage)")
        
        # Confirm test execution
        print(f"\nReady to execute benchmark:")
        print(f"• {total_operations:,} join operations will be executed")
        if num_threads > 1:
            print(f"• Operations will be distributed across {num_threads} concurrent threads")
            print(f"• Each thread will create its own database connection")
        
        response = input("\nProceed with benchmark? (Y/n): ").strip().lower()
        if response == 'n':
            print("Benchmark cancelled")
            return 0
        
        # Run join benchmark
        benchmark.run_join_benchmark(
            from_collection=from_collection,
            to_collection=to_collection,
            match_conditions=match_conditions,
            join_field=join_field,
            total_operations=total_operations,
            num_threads=num_threads
        )
        
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