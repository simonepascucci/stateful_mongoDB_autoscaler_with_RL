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
import string

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
    """Connect to MongoDB and return database object."""
    try:
        # Create client with appropriate connection settings
        client = pymongo.MongoClient(
            host=host, 
            port=port, 
            connectTimeoutMS=5000,
            socketTimeoutMS=30000,
            w=1  # Reduced write concern for better performance
        )
        
        # Test connection
        client.admin.command('ping')
        
        # Get database
        db = client[database]
        print(f"✅ Connected to MongoDB at {host}:{port}")
        return db
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return None

def generate_random_value(field_name):
    """Generate a random value appropriate for the field type."""
    if field_name in ["first_name", "last_name"]:
        # Generate a random name-like string
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(8))
    elif field_name in ["city", "country"]:
        # Generate a random location-like string
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(10))
    else:
        # Default to a random string
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(8))

def generate_update_data():
    """Generate random update data for MongoDB documents."""
    # Use $set to update specific fields (avoiding shard key fields)
    update_fields = {}
    
    # Add random fields to update
    update_fields["last_login"] = datetime.now()
    update_fields["status"] = random.choice(["active", "inactive", "pending"])
    update_fields["score"] = random.randint(1, 100)
    
    return {"$set": update_fields}



def run_read_benchmark(db, collection_name, field_name, num_operations):
    """Run read benchmark queries and measure performance."""
    collection = db[collection_name]
    
    try:
        # Verify collection has data
        count = collection.count_documents({})
        if count == 0:
            print(f"⚠️ Warning: Collection '{collection_name}' is empty")
            return None
        print(f"Found {count} documents in collection")
        
        # Check if the field exists in the collection
        sample_doc = collection.find_one()
        if sample_doc and field_name not in sample_doc:
            print(f"⚠️ Warning: Field '{field_name}' not found in sample document")
            print(f"Available fields: {list(sample_doc.keys())}")
            return None
            
    except Exception as e:
        print(f"❌ Error preparing read benchmark: {e}")
        return None
    
    # Run the benchmark
    print(f"Starting READ benchmark on field '{field_name}' with {num_operations} operations...")
    
    latencies = []
    successful_operations = 0
    start_time = time.time()
    
    for i in range(num_operations):
        try:
            # Generate a random value for the query
            query_value = generate_random_value(field_name)
            
            # Measure query execution time
            query_start_time = time.time()
            
            # Perform the query (limiting to 100 results to avoid memory issues)
            cursor = collection.find({field_name: query_value}).limit(100)
            # Force execution of the query
            results = list(cursor)
            
            # Calculate latency for this operation
            latency = time.time() - query_start_time
            latencies.append(latency)
            successful_operations += 1
            
            # Report progress occasionally
            if i % 100 == 0 or i == num_operations - 1:
                elapsed = time.time() - start_time
                ops_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
                print(f"Progress: {i+1}/{num_operations} operations ({(i+1)/num_operations*100:.1f}%) - {ops_per_sec:.2f} ops/sec")
                
        except Exception as e:
            print(f"❌ Error during read query {i+1}: {e}")
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Calculate metrics
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        throughput = successful_operations / execution_time if execution_time > 0 else 0
        
        metrics = {
            "execution_time": execution_time,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "min_latency": min_latency,
            "max_latency": max_latency,
            "p95_latency": p95_latency,
            "successful_operations": successful_operations,
            "benchmark_type": "READ"
        }
        
        return metrics
    else:
        print("❌ No successful operations completed")
        return None

def run_update_benchmark(db, collection_name, num_operations, use_bulk=False):
    """Run update benchmark and measure performance."""
    collection = db[collection_name]
    
    try:
        # Get initial document count
        initial_count = collection.count_documents({})
        if initial_count == 0:
            print(f"⚠️ Warning: Collection '{collection_name}' is empty - no documents to update")
            return None
        print(f"Found {initial_count} documents available for updates")
            
    except Exception as e:
        print(f"❌ Error preparing update benchmark: {e}")
        return None
    
    # Run the benchmark
    benchmark_type = "BULK UPDATE" if use_bulk else "SINGLE UPDATE"
    print(f"Starting {benchmark_type} benchmark with {num_operations} operations...")
    
    latencies = []
    successful_operations = 0
    start_time = time.time()
    
    if use_bulk:
        # Process in batches for better performance
        batch_size = min(10, num_operations)  # Ensure batch size doesn't exceed operations
        batches = num_operations // batch_size
        remainder = num_operations % batch_size
        
        for batch_num in range(batches):
            try:
                # Generate batch of update operations
                operations = []
                for _ in range(batch_size):
                    # Generate a query to find documents to update (avoiding shard key fields)
                    query_field = "last_name"
                    query_value = generate_random_value(query_field)
                    query = {query_field: "Miller"}
                    
                    # Generate update data (excluding shard key fields)
                    update_data = generate_update_data()
                    
                    operations.append(
                        pymongo.UpdateOne(query, update_data)
                    )
                
                # Measure batch update execution time
                batch_start_time = time.time()
                
                # Perform bulk update (unordered for better sharding performance)
                result = collection.bulk_write(operations, ordered=False)
                
                # Calculate latency for this batch
                latency = time.time() - batch_start_time
                # Average latency per document in this batch
                avg_latency_per_doc = latency / batch_size
                latencies.extend([avg_latency_per_doc] * batch_size)
                successful_operations += result.matched_count
                
                # Report progress
                if batch_num % 10 == 0 or batch_num == batches - 1:
                    elapsed = time.time() - start_time
                    ops_completed = (batch_num + 1) * batch_size
                    ops_per_sec = ops_completed / elapsed if elapsed > 0 else 0
                    print(f"Progress: {ops_completed}/{num_operations} operations ({ops_completed/num_operations*100:.1f}%) - {ops_per_sec:.2f} ops/sec")
                    
            except Exception as e:
                print(f"❌ Error during bulk update batch {batch_num+1}: {e}")
        
        # Handle remainder operations
        if remainder > 0:
            try:
                operations = []
                for _ in range(remainder):
                    query_field = "last_name"
                    query_value = generate_random_value(query_field)
                    query = {query_field: "Miller"}
                    update_data = generate_update_data()
                    operations.append(pymongo.UpdateOne(query, update_data))
                
                batch_start_time = time.time()
                result = collection.bulk_write(operations, ordered=False)
                latency = time.time() - batch_start_time
                avg_latency_per_doc = latency / remainder
                latencies.extend([avg_latency_per_doc] * remainder)
                successful_operations += result.matched_count
                
                # Final progress update
                elapsed = time.time() - start_time
                ops_per_sec = num_operations / elapsed if elapsed > 0 else 0
                print(f"Progress: {num_operations}/{num_operations} operations (100.0%) - {ops_per_sec:.2f} ops/sec")
            except Exception as e:
                print(f"❌ Error during remainder update operations: {e}")
    else:
        # Single updates
        for i in range(num_operations):
            try:
                # Generate a query to find documents to update (avoiding shard key fields)
                query_field = "last_name"
                query_value = generate_random_value(query_field)
                query = {query_field: "Perez"}
                
                # Generate update data (excluding shard key fields)
                update_data = generate_update_data()
                
                # Measure update execution time
                update_start_time = time.time()
                
                # Perform the update operation (update one document)
                result = collection.update_one(query, update_data)
                
                # Calculate latency for this operation
                latency = time.time() - update_start_time
                latencies.append(latency)
                
                # Count as successful if at least one document was matched
                if result.matched_count > 0:
                    successful_operations += 1
                
                # Report progress occasionally
                if i % 100 == 0 or i == num_operations - 1:
                    elapsed = time.time() - start_time
                    ops_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"Progress: {i+1}/{num_operations} operations ({(i+1)/num_operations*100:.1f}%) - {ops_per_sec:.2f} ops/sec")
                    
            except Exception as e:
                print(f"❌ Error during update operation {i+1}: {e}")
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Calculate metrics
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        throughput = successful_operations / execution_time if execution_time > 0 else 0
        
        metrics = {
            "execution_time": execution_time,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "min_latency": min_latency,
            "max_latency": max_latency,
            "p95_latency": p95_latency,
            "successful_operations": successful_operations,
            "benchmark_type": benchmark_type
        }
        
        return metrics
    else:
        print("❌ No successful operations completed")
        return None

def print_results(field_name, metrics):
    """Print benchmark results in a formatted way."""
    print(f"\n{'=' * 60}")
    print(f"BENCHMARK RESULTS - {metrics['benchmark_type']}")
    if field_name:
        print(f"Field: {field_name}")
        if "field_cardinality" in metrics:
            print(f"Estimated Field Cardinality: {metrics['field_cardinality']}")
    print(f"{'=' * 60}")
    print(f"Successful Operations: {metrics['successful_operations']}")
    print(f"Total Execution Time: {metrics['execution_time']:.4f} seconds")
    print(f"Throughput: {metrics['throughput']:.2f} operations/second")
    print(f"Average Latency: {metrics['avg_latency'] * 1000:.2f} ms")
    print(f"Min Latency: {metrics['min_latency'] * 1000:.2f} ms")
    print(f"Max Latency: {metrics['max_latency'] * 1000:.2f} ms")
    print(f"P95 Latency: {metrics['p95_latency'] * 1000:.2f} ms")
    print(f"{'=' * 60}\n")

def run_mongodb_benchmark(host, port, database, collection_name, operation_type, field_names, num_operations):
    """Run benchmarks on MongoDB for specified operation type and fields."""
    results = {}
    
    print(f"\nStarting MongoDB {operation_type.upper()} Benchmark at {datetime.now()}")
    print(f"Host: {host}:{port}")
    print(f"Database: {database}")
    print(f"Collection: {collection_name}")
    print(f"Operations per test: {num_operations}")
    
    # Test connection before proceeding
    if not test_connection(host, port):
        print("❌ Connection test failed. Exiting.")
        return None
    
    db = connect_to_mongo(host, port, database)
    if db is None:
        print(f"❌ Skipping benchmark due to connection error")
        return None
    
    if operation_type == "read":
        print(f"Fields to query: {', '.join(field_names)}")
        for field_name in field_names:
            print(f"\nRunning READ benchmark on field '{field_name}'...")
            metrics = run_read_benchmark(db, collection_name, field_name, num_operations)
            if metrics:
                results[field_name] = metrics
                print_results(field_name, metrics)
            else:
                print(f"❌ Read benchmark failed for field {field_name}")
    
    elif operation_type == "update":
        print("Running UPDATE benchmark...")
        print("⚠️ Note: Shard key fields (first_name) will NOT be updated to avoid errors")
        
        # Ask user for bulk vs single updates
        while True:
            bulk_choice = input("Use bulk updates for better performance? (y/n): ").lower().strip()
            if bulk_choice in ['y', 'yes']:
                use_bulk = True
                break
            elif bulk_choice in ['n', 'no']:
                use_bulk = False
                break
            else:
                print("Please enter 'y' for yes or 'n' for no")
        
        metrics = run_update_benchmark(db, collection_name, num_operations, use_bulk)
        if metrics:
            results["update_operations"] = metrics
            print_results(None, metrics)
        else:
            print(f"❌ Update benchmark failed")
    
    return results

def get_operation_type():
    """Get operation type from user input."""
    while True:
        operation = input("Enter operation type - 'r' for read, 'u' for update: ").lower().strip()
        if operation == 'r':
            return "read"
        elif operation == 'u':
            return "update"
        else:
            print("Please enter 'r' for read or 'u' for update operations")

def get_collection_name(default_name="U1"):
    """Get collection name from user input."""
    collection_name = input(f"Enter collection name (default: {default_name}): ").strip()
    if not collection_name:
        return default_name
    return collection_name

if __name__ == "__main__":
    # Configuration
    MONGO_HOST = "localhost"
    MONGO_PORT = 27017
    #MONGO_HOST = "192.168.178.168"
    #MONGO_PORT = 32017
    DATABASE = "ycsb_sharded"
    
    # Test different fields for read operations
    READ_FIELDS_TO_TEST = ["first_name", "last_name"]
    
    # Number of operations for each test
    NUM_OPERATIONS = 1000
    
    # Get operation type from user
    operation_type = get_operation_type()
    
    # Get collection name from user
    COLLECTION_NAME = get_collection_name("U1")
    
    try:
        # Run the benchmark
        results = run_mongodb_benchmark(
            MONGO_HOST, 
            MONGO_PORT, 
            DATABASE,
            COLLECTION_NAME,
            operation_type,
            READ_FIELDS_TO_TEST if operation_type == "read" else [], 
            NUM_OPERATIONS
        )
        
        if results:
            print("\n✅ Benchmark completed successfully!")
        else:
            print("\n❌ Benchmark failed or was incomplete")
            
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error during benchmark: {e}")
