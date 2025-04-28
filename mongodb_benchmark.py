#!/usr/bin/env python3
import pymongo
import time
import random
import statistics
from datetime import datetime
import sys

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
        if "usertable" not in collection_names:
            print(f"⚠️ Warning: 'usertable' collection not found in database '{database}'")
        return db
    except Exception as e:
        print(f"❌ Error connecting to database '{database}': {e}")
        return None

def generate_random_value(field_name):
    """Generate random test values based on field type."""
    if field_name == "country":
        # List of sample countries for testing
        countries = ["Italy", "USA", "France", "Germany", "Spain", "China", "Japan", "Brazil", "India", "Canada"]
        return random.choice(countries)
    elif field_name == "age":
        return str(random.randint(18, 90))
    elif field_name == "city":
        cities = ["San Jose", "Rome", "New York", "Paris", "Berlin", "Madrid", "Tokyo", "Beijing", "Mumbai", "Toronto"]
        return random.choice(cities)
    else:
        # Default case for other field types
        return f"test_{random.randint(1, 1000)}"

def run_benchmark(db, field_name, num_operations):
    """Run benchmark queries and measure performance."""
    collection = db["usertable"]
    
    # Initialize metrics
    latencies = []
    total_start_time = time.time()
    
    try:
        # Verify collection has data
        count = collection.count_documents({})
        if count == 0:
            print(f"⚠️ Warning: Collection 'usertable' is empty")
            return None
        print(f"Found {count} documents in collection")
        
        # Check if the field exists in the collection
        sample_doc = collection.find_one()
        if sample_doc and field_name not in sample_doc:
            print(f"⚠️ Warning: Field '{field_name}' not found in sample document")
            print(f"Available fields: {list(sample_doc.keys())}")
    except Exception as e:
        print(f"❌ Error preparing benchmark: {e}")
        return None
    
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
            
            # Show progress for long-running tests
            if i % 100 == 0 and i > 0:
                print(f"Progress: {i}/{num_operations} queries completed")
                
        except Exception as e:
            print(f"❌ Error during query {i+1}: {e}")
    
    if not latencies:
        print("❌ No successful operations completed")
        return None
        
    # Calculate total execution time
    total_execution_time = time.time() - total_start_time
    
    # Calculate metrics
    throughput = successful_operations / total_execution_time
    avg_latency = statistics.mean(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
    
    return {
        "total_execution_time": total_execution_time,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "min_latency": min_latency,
        "max_latency": max_latency,
        "p95_latency": p95_latency,
        "successful_operations": successful_operations
    }

def print_results(db_name, field_name, metrics):
    """Print benchmark results in a readable format."""
    if not metrics:
        print(f"\n⚠️ No results available for {db_name} - Field: {field_name}")
        return
        
    print(f"\n{'=' * 50}")
    print(f"BENCHMARK RESULTS: {db_name} - Field: {field_name}")
    print(f"{'=' * 50}")
    print(f"Successful Operations: {metrics['successful_operations']}")
    print(f"Total Execution Time: {metrics['total_execution_time']:.4f} seconds")
    print(f"Throughput: {metrics['throughput']:.2f} operations/second")
    print(f"Average Latency: {metrics['avg_latency'] * 1000:.2f} ms")
    print(f"Min Latency: {metrics['min_latency'] * 1000:.2f} ms")
    print(f"Max Latency: {metrics['max_latency'] * 1000:.2f} ms")
    print(f"P95 Latency: {metrics['p95_latency'] * 1000:.2f} ms")
    print(f"{'=' * 50}\n")

def run_comparison_benchmark(host, port, databases, field_names, num_operations):
    """Run benchmarks comparing different databases and query fields."""
    results = {}
    
    print(f"\nStarting MongoDB Benchmark at {datetime.now()}")
    print(f"Host: {host}:{port}")
    print(f"Operations per test: {num_operations}")
    print(f"Databases to test: {', '.join(databases)}")
    print(f"Fields to query: {', '.join(field_names)}")
    
    # Test connection before proceeding
    if not test_connection(host, port):
        print("❌ Connection test failed. Exiting.")
        return None
    
    for db_name in databases:
        results[db_name] = {}
        db = connect_to_mongo(host, port, db_name)
        if db is None:
            print(f"❌ Skipping database {db_name} due to connection error")
            continue
        
        for field_name in field_names:
            print(f"\nRunning benchmark on {db_name}, querying field '{field_name}'...")
            metrics = run_benchmark(db, field_name, num_operations)
            if metrics:
                results[db_name][field_name] = metrics
                print_results(db_name, field_name, metrics)
            else:
                print(f"❌ Benchmark failed for {db_name}, field {field_name}")
    
    # Compare results between databases if both were successful
    try:
        if all(db in results for db in databases) and all(field in results.get(databases[0], {}) and field in results.get(databases[1], {}) for field in field_names):
            print("\nCOMPARISON RESULTS:")
            print("=" * 50)
            for field_name in field_names:
                speedup = results[databases[0]][field_name]["avg_latency"] / results[databases[1]][field_name]["avg_latency"]
                print(f"Speedup for '{field_name}' queries in {databases[1]} vs {databases[0]}: {speedup:.2f}x")
            print("=" * 50)
    except Exception as e:
        print(f"❌ Error computing comparison: {e}")
    
    return results

if __name__ == "__main__":
    # Configuration
    MONGO_HOST = "192.168.178.168"
    MONGO_PORT = 32017
    DATABASES = ["ycsb_unsharded", "ycsb_sharded"]
    
    # Test both shard key and non-shard key fields
    FIELDS_TO_TEST = ["country", "city"]  # country is shard key, city is not
    
    # Number of operations for each test
    NUM_OPERATIONS = 1000
    
    try:
        # Run the benchmark
        results = run_comparison_benchmark(
            MONGO_HOST, 
            MONGO_PORT, 
            DATABASES, 
            FIELDS_TO_TEST, 
            NUM_OPERATIONS
        )

        if results:
            print("\n✅ Benchmark completed successfully!")
        else:
            print("\n⚠️ Benchmark completed with errors")
    except KeyboardInterrupt:
        print("\n⚠️ Benchmark interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")