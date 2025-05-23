#!/usr/bin/env python3
import pymongo
import time
import random
import statistics
from datetime import datetime
import sys
import json
from bson import json_util
import pprint
import threading
import concurrent.futures
import queue

def test_connection(host, port):
    """Test connection to MongoDB server before running benchmark."""
    try:
        client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
        # Force a connection to verify it works
        client.admin.command('ping')
        print(f"✅ Successfully connected to MongoDB at {host}:{port}")
        return True
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB at {host}:{port}")
        print(f"Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error when connecting to MongoDB: {e}")
        return False

def connect_to_mongo(host, port, database):
    """Connect to MongoDB and return the database object."""
    try:
        client = pymongo.MongoClient(host, port)
        db = client[database]
        # Verify database and collection existence
        collection_names = db.list_collection_names()
        if "sharded" not in collection_names:
            print(f"⚠️ Warning: 'sharded' collection not found in database '{database}'")
        return db
    except Exception as e:
        print(f"❌ Error connecting to database '{database}': {e}")
        return None


def generate_random_value(field_name):
    """Generate random test values based on field type."""
    if field_name == "country":
        # List of sample countries for testing
        countries = ["Italy", "USA", "Argentina", "Germany", "New Zealand", "Mexico", "Russia", "Chile", "India", "Philippines"]
        return random.choice(countries)
    elif field_name == "age":
        return str(random.randint(18, 90))
    elif field_name == "city":
        cities = ["Lincoln", "Tulsa", "Henderson", "Honolulu", "New York", "Seattle", "Dallas", "Irvine", "Nashville", "Denver"]
        return random.choice(cities)
    elif field_name == "first_name":
        first_names = ["John", "George", "Mark", "Rebecca", "Rachel", "Amy", "Emma", "Benjamin", "James", "Kevin"]
        return random.choice(first_names)
    else:
        # Default case for other field types
        return f"test_{random.randint(1, 1000)}"

def get_field_cardinality(db, collection_name, field_name, sample_size=1000):
    """Estimate field cardinality (number of unique values)."""
    try:
        collection = db[collection_name]
        
        # Use aggregation to count distinct values
        pipeline = [
            {"$sample": {"size": sample_size}},  # Sample to avoid scanning entire collection
            {"$group": {"_id": f"${field_name}"}},
            {"$count": "unique_values"}
        ]
        
        result = list(collection.aggregate(pipeline))
        if result and "unique_values" in result[0]:
            return result[0]["unique_values"]
        return "Unknown"
    except Exception as e:
        print(f"❌ Error estimating cardinality for {field_name}: {e}")
        return "Error"

def thread_worker(thread_id, db, field_name, num_operations, results_queue, progress_queue):
    """Worker function for each thread to execute queries."""
    collection = db["sharded"]
    latencies = []
    thread_start_time = time.time()
    successful_operations = 0
    
    for i in range(num_operations):
        try:
            # Generate a random value for the query
            query_value = generate_random_value(field_name)
            
            # Measure query execution time
            start_time = time.time()
            
            # Perform the query (limiting to 1000 results to avoid memory issues)
            cursor = collection.find({field_name: query_value}).limit(1000)
            # Force execution of the query
            results = list(cursor)
            
            # Calculate latency for this operation
            latency = time.time() - start_time
            latencies.append(latency)
            successful_operations += 1
            
            # Report progress occasionally
            if i % 10 == 0:
                progress_queue.put((thread_id, i, num_operations))
                
        except Exception as e:
            print(f"❌ Thread {thread_id}: Error during query {i+1}: {e}")
    
    # Calculate thread execution time
    thread_execution_time = time.time() - thread_start_time
    
    # Put results in the queue
    results_queue.put({
        "thread_id": thread_id,
        "latencies": latencies,
        "execution_time": thread_execution_time,
        "successful_operations": successful_operations
    })
    
    # Final progress update
    progress_queue.put((thread_id, num_operations, num_operations))

def progress_monitor(progress_queue, num_threads, total_operations):
    """Monitor and display progress across all threads."""
    thread_progress = {i: 0 for i in range(num_threads)}
    last_report_time = time.time()
    report_interval = 2.0  # seconds between progress reports
    
    try:
        while sum(thread_progress.values()) < total_operations:
            try:
                thread_id, current, total = progress_queue.get(timeout=0.5)
                thread_progress[thread_id] = current
                
                # Report progress at intervals
                current_time = time.time()
                if current_time - last_report_time > report_interval:
                    completed = sum(thread_progress.values())
                    percentage = (completed / total_operations) * 100
                    print(f"Progress: {completed}/{total_operations} operations ({percentage:.1f}%)")
                    last_report_time = current_time
                    
                progress_queue.task_done()
            except queue.Empty:
                continue
    except KeyboardInterrupt:
        print("\nProgress monitoring interrupted")
    
    # Final progress report
    completed = sum(thread_progress.values())
    percentage = (completed / total_operations) * 100
    print(f"Final progress: {completed}/{total_operations} operations ({percentage:.1f}%)")

def run_benchmark(db, field_name, num_operations, num_threads):
    """Run benchmark queries across multiple threads and measure performance."""
    collection = db["sharded"]
    
    # Initialize metrics
    total_start_time = time.time()
    
    try:
        # Verify collection has data
        count = collection.count_documents({})
        if count == 0:
            print(f"⚠️ Warning: Collection 'sharded' is empty")
            return None
        print(f"Found {count} documents in collection")
        
        # Check if the field exists in the collection
        sample_doc = collection.find_one()
        if sample_doc and field_name not in sample_doc:
            print(f"⚠️ Warning: Field '{field_name}' not found in sample document")
            print(f"Available fields: {list(sample_doc.keys())}")
            return None
            
    except Exception as e:
        print(f"❌ Error preparing benchmark: {e}")
        return None
    
    # Calculate operations per thread
    ops_per_thread = num_operations // num_threads
    remainder = num_operations % num_threads
    
    print(f"Starting benchmark with {num_threads} threads")
    print(f"Each thread will perform {ops_per_thread} operations")
    if remainder > 0:
        print(f"First thread will perform {ops_per_thread + remainder} operations to complete the total")
    
    # Create queues for results and progress
    results_queue = queue.Queue()
    progress_queue = queue.Queue()
    
    # Start progress monitor in a separate thread
    progress_thread = threading.Thread(
        target=progress_monitor, 
        args=(progress_queue, num_threads, num_operations)
    )
    progress_thread.daemon = True
    progress_thread.start()
    
    # Start worker threads
    threads = []
    for i in range(num_threads):
        # First thread gets any remainder operations
        thread_ops = ops_per_thread + remainder if i == 0 else ops_per_thread
        remainder = 0  # Reset remainder after first thread
        
        thread = threading.Thread(
            target=thread_worker,
            args=(i, db, field_name, thread_ops, results_queue, progress_queue)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Wait for progress monitor to finish
    progress_queue.join()
    
    # Calculate total execution time
    total_execution_time = time.time() - total_start_time
    
    # Collect results
    all_latencies = []
    successful_operations = 0
    
    while not results_queue.empty():
        result = results_queue.get()
        all_latencies.extend(result["latencies"])
        successful_operations += result["successful_operations"]
    
    if not all_latencies:
        print("❌ No successful operations completed")
        return None
    
    # Calculate metrics
    throughput = successful_operations / total_execution_time
    avg_latency = statistics.mean(all_latencies)
    min_latency = min(all_latencies)
    max_latency = max(all_latencies)
    p95_latency = sorted(all_latencies)[int(len(all_latencies) * 0.95)]
    
    return {
        "total_execution_time": total_execution_time,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "min_latency": min_latency,
        "max_latency": max_latency,
        "p95_latency": p95_latency,
        "successful_operations": successful_operations,
        "num_threads": num_threads
    }

def print_results(field_name, metrics):
    """Print benchmark results in a readable format."""
    if not metrics:
        print(f"\n⚠️ No results available for Field: {field_name}")
        return
        
    print(f"\n{'=' * 50}")
    print(f"BENCHMARK RESULTS - Field: {field_name}")
    print(f"{'=' * 50}")
    print(f"Threads Used: {metrics['num_threads']}")
    print(f"Successful Operations: {metrics['successful_operations']}")
    print(f"Total Execution Time: {metrics['total_execution_time']:.4f} seconds")
    print(f"Throughput: {metrics['throughput']:.2f} operations/second")
    print(f"Average Latency: {metrics['avg_latency'] * 1000:.2f} ms")
    print(f"Min Latency: {metrics['min_latency'] * 1000:.2f} ms")
    print(f"Max Latency: {metrics['max_latency'] * 1000:.2f} ms")
    print(f"P95 Latency: {metrics['p95_latency'] * 1000:.2f} ms")
    print(f"{'=' * 50}\n")

def run_mongodb_benchmark(host, port, database, field_names, num_operations, num_threads):
    """Run benchmarks on MongoDB for specified fields using multiple threads."""
    results = {}
    
    print(f"\nStarting MongoDB Benchmark at {datetime.now()}")
    print(f"Host: {host}:{port}")
    print(f"Database: {database}")
    print(f"Operations per test: {num_operations}")
    print(f"Number of threads: {num_threads}")
    print(f"Fields to query: {', '.join(field_names)}")
    
    # Test connection before proceeding
    if not test_connection(host, port):
        print("❌ Connection test failed. Exiting.")
        return None
    
    db = connect_to_mongo(host, port, database)
    if db is None:
        print(f"❌ Skipping benchmark due to connection error")
        return None
        
    for field_name in field_names:
        print(f"\nRunning benchmark on field '{field_name}'...")
        metrics = run_benchmark(db, field_name, num_operations, num_threads)
        if metrics:
            results[field_name] = metrics
            print_results(field_name, metrics)
        else:
            print(f"❌ Benchmark failed for field {field_name}")
    
    return results

def get_thread_count():
    """Get number of threads from user, between 1 and 128."""
    while True:
        try:
            thread_count = input("Enter number of threads to use (1-128): ")
            threads = int(thread_count)
            if 1 <= threads <= 128:
                return threads
            else:
                print("Please enter a number between 1 and 128")
        except ValueError:
            print("Please enter a valid integer")

if __name__ == "__main__":
    # Configuration
    MONGO_HOST = "192.168.178.168"
    MONGO_PORT = 32017
    DATABASE = "ycsb_sharded"
    
    # Test different fields
    FIELDS_TO_TEST = ["country", "city"]
    
    # Number of operations for each test
    NUM_OPERATIONS = 1000
    
    # Get number of threads from user
    num_threads = get_thread_count()
    
    try:
        # Run the benchmark
        results = run_mongodb_benchmark(
            MONGO_HOST, 
            MONGO_PORT, 
            DATABASE, 
            FIELDS_TO_TEST, 
            NUM_OPERATIONS,
            num_threads
        )

        if results:
            print("\n✅ Benchmark completed successfully!")
        else:
            print("\n⚠️ Benchmark completed with errors")
    except KeyboardInterrupt:
        print("\n⚠️ Benchmark interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")