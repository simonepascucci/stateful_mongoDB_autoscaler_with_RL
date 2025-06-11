#!/usr/bin/env python3
"""
MongoDB Join Operations Benchmark Script
Specialized benchmark for testing $match + $lookup operations with specific test cases
"""

import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import sys
from datetime import datetime


class MongoDBJoinBenchmark:
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
                return False, 0
            
            print(f"✓ Collection '{collection_name}' has {doc_count} documents")
            return True, doc_count
            
        except Exception as e:
            print(f"✗ Error checking collection: {e}")
            return False, 0
    
    def run_single_join_test(self, from_collection, to_collection, match_conditions, 
                            join_field, test_name):
        """Run a single join test with $match + $lookup"""
        print(f"\n{'='*80}")
        print(f"EXECUTING: {test_name}")
        print(f"From: {from_collection} → To: {to_collection}")
        print(f"Match conditions: {match_conditions}")
        print(f"Join field: {join_field}")
        print(f"{'='*80}")
        
        collection = self.db[from_collection]
        
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
        
        print(f"Pipeline:")
        for i, stage in enumerate(pipeline, 1):
            print(f"  Stage {i}: {stage}")
        
        try:
            print(f"\nExecuting single join operation...")
            start_time = time.time()
            
            # Perform the join operation
            cursor = collection.aggregate(pipeline)
            result_list = list(cursor)  # Force execution
            
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000
            
            print(f"✓ Join operation completed successfully")
            print(f"  Execution time: {execution_time_ms:.2f} ms")
            print(f"  Documents matched: {len(result_list)}")
            
            # Show sample result structure
            if result_list:
                sample_doc = result_list[0]
                joined_count = len(sample_doc.get('joined_data', []))
                print(f"  Sample document joined records: {joined_count}")
                
                # Show fields in sample document (excluding large arrays)
                sample_fields = {k: v for k, v in sample_doc.items() 
                               if k != 'joined_data'}
                print(f"  Sample document fields: {list(sample_fields.keys())}")
            
            # Store results
            result = {
                'name': test_name,
                'from_collection': from_collection,
                'to_collection': to_collection,
                'match_conditions': match_conditions,
                'join_field': join_field,
                'execution_time_ms': execution_time_ms,
                'documents_matched': len(result_list),
                'success': True
            }
            
            self.benchmark_results[test_name] = result
            return result
            
        except Exception as e:
            print(f"✗ Join operation failed: {e}")
            result = {
                'name': test_name,
                'from_collection': from_collection,
                'to_collection': to_collection,
                'match_conditions': match_conditions,
                'join_field': join_field,
                'execution_time_ms': 0,
                'documents_matched': 0,
                'success': False,
                'error': str(e)
            }
            self.benchmark_results[test_name] = result
            return result
    
    def print_test_summary(self, result):
        """Print summary for a single test"""
        print(f"\n{'='*60}")
        print(f"TEST SUMMARY: {result['name']}")
        print(f"{'='*60}")
        if result['success']:
            print(f"✓ Status: SUCCESS")
            print(f"  Execution time: {result['execution_time_ms']:.2f} ms")
            print(f"  Documents matched: {result['documents_matched']}")
            print(f"  Join direction: {result['from_collection']} → {result['to_collection']}")
            print(f"  Join field: {result['join_field']}")
            print(f"  Match conditions: {result['match_conditions']}")
        else:
            print(f"✗ Status: FAILED")
            print(f"  Error: {result.get('error', 'Unknown error')}")
    
    def print_final_summary(self):
        """Print comprehensive final summary"""
        print(f"\n{'='*100}")
        print(f"COMPREHENSIVE JOIN BENCHMARK RESULTS")
        print(f"{'='*100}")
        
        if not self.benchmark_results:
            print("No benchmark results to display")
            return
        
        successful_tests = [r for r in self.benchmark_results.values() if r['success']]
        failed_tests = [r for r in self.benchmark_results.values() if not r['success']]
        
        print(f"Total tests executed: {len(self.benchmark_results)}")
        print(f"Successful tests: {len(successful_tests)}")
        print(f"Failed tests: {len(failed_tests)}")
        
        if successful_tests:
            print(f"\nDETAILED RESULTS:")
            print(f"{'Test Name':<40} {'Direction':<20} {'Join Field':<12} {'Time(ms)':<10} {'Docs':<8}")
            print(f"{'-'*40} {'-'*20} {'-'*12} {'-'*10} {'-'*8}")
            
            for result in successful_tests:
                direction = f"{result['from_collection']} → {result['to_collection']}"
                if len(direction) > 19:
                    direction = direction[:16] + "..."
                
                print(f"{result['name']:<40} {direction:<20} "
                      f"{result['join_field']:<12} {result['execution_time_ms']:<10.2f} "
                      f"{result['documents_matched']:<8}")
            
            # Performance analysis
            print(f"\nPERFORMANCE ANALYSIS:")
            
            # Group by direction
            c1_to_c2_tests = [r for r in successful_tests if 'C1 → C2' in r['name']]
            c2_to_c1_tests = [r for r in successful_tests if 'C2 → C1' in r['name']]
            
            if c1_to_c2_tests:
                avg_time_c1_c2 = sum(r['execution_time_ms'] for r in c1_to_c2_tests) / len(c1_to_c2_tests)
                avg_docs_c1_c2 = sum(r['documents_matched'] for r in c1_to_c2_tests) / len(c1_to_c2_tests)
                print(f"\nC1 → C2 Direction (Collection 1 to Collection 2):")
                print(f"  Average execution time: {avg_time_c1_c2:.2f} ms")
                print(f"  Average documents matched: {avg_docs_c1_c2:.1f}")
                
                fastest_c1_c2 = min(c1_to_c2_tests, key=lambda x: x['execution_time_ms'])
                slowest_c1_c2 = max(c1_to_c2_tests, key=lambda x: x['execution_time_ms'])
                print(f"  Fastest test: {fastest_c1_c2['name']} ({fastest_c1_c2['execution_time_ms']:.2f} ms)")
                print(f"  Slowest test: {slowest_c1_c2['name']} ({slowest_c1_c2['execution_time_ms']:.2f} ms)")
            
            if c2_to_c1_tests:
                avg_time_c2_c1 = sum(r['execution_time_ms'] for r in c2_to_c1_tests) / len(c2_to_c1_tests)
                avg_docs_c2_c1 = sum(r['documents_matched'] for r in c2_to_c1_tests) / len(c2_to_c1_tests)
                print(f"\nC2 → C1 Direction (Collection 2 to Collection 1):")
                print(f"  Average execution time: {avg_time_c2_c1:.2f} ms")
                print(f"  Average documents matched: {avg_docs_c2_c1:.1f}")
                
                fastest_c2_c1 = min(c2_to_c1_tests, key=lambda x: x['execution_time_ms'])
                slowest_c2_c1 = max(c2_to_c1_tests, key=lambda x: x['execution_time_ms'])
                print(f"  Fastest test: {fastest_c2_c1['name']} ({fastest_c2_c1['execution_time_ms']:.2f} ms)")
                print(f"  Slowest test: {slowest_c2_c1['name']} ({slowest_c2_c1['execution_time_ms']:.2f} ms)")
            
            # Directional comparison
            if c1_to_c2_tests and c2_to_c1_tests:
                print(f"\nDIRECTIONAL COMPARISON:")
                if avg_time_c1_c2 < avg_time_c2_c1:
                    faster_direction = "C1 → C2"
                    performance_ratio = avg_time_c2_c1 / avg_time_c1_c2
                else:
                    faster_direction = "C2 → C1"
                    performance_ratio = avg_time_c1_c2 / avg_time_c2_c1
                
                print(f"  Faster direction: {faster_direction}")
                print(f"  Performance ratio: {performance_ratio:.2f}x faster")
                print(f"  Time difference: {abs(avg_time_c1_c2 - avg_time_c2_c1):.2f} ms")
            
            # Join field analysis
            first_name_tests = [r for r in successful_tests if r['join_field'] == 'first_name']
            country_tests = [r for r in successful_tests if r['join_field'] == 'country']
            
            if first_name_tests:
                avg_time_fn = sum(r['execution_time_ms'] for r in first_name_tests) / len(first_name_tests)
                print(f"\nFIRST_NAME JOIN ANALYSIS:")
                print(f"  Tests performed: {len(first_name_tests)}")
                print(f"  Average execution time: {avg_time_fn:.2f} ms")
            
            if country_tests:
                avg_time_country = sum(r['execution_time_ms'] for r in country_tests) / len(country_tests)
                print(f"\nCOUNTRY JOIN ANALYSIS:")
                print(f"  Tests performed: {len(country_tests)}")
                print(f"  Average execution time: {avg_time_country:.2f} ms")
            
            if first_name_tests and country_tests:
                if avg_time_fn < avg_time_country:
                    faster_field = "first_name"
                    field_ratio = avg_time_country / avg_time_fn
                else:
                    faster_field = "country"
                    field_ratio = avg_time_fn / avg_time_country
                
                print(f"\nJOIN FIELD COMPARISON:")
                print(f"  Faster join field: {faster_field}")
                print(f"  Performance ratio: {field_ratio:.2f}x faster")
        
        if failed_tests:
            print(f"\nFAILED TESTS:")
            for result in failed_tests:
                print(f"  ✗ {result['name']}: {result.get('error', 'Unknown error')}")
    
    def run_comprehensive_join_benchmark(self, collection1, collection2):
        """Run comprehensive join benchmark with predefined test cases"""
        print(f"\n{'='*100}")
        print(f"MONGODB COMPREHENSIVE JOIN BENCHMARK")
        print(f"{'='*100}")
        print(f"Collection 1: {collection1}")
        print(f"Collection 2: {collection2}")
        print(f"Test Plan:")
        print(f"  • 6 total tests (3 per direction)")
        print(f"  • 2 tests joining on 'first_name' per direction")
        print(f"  • 1 test joining on 'country' per direction")
        print(f"  • Multi-field match conditions to reduce results")
        print(f"  • Single operation per test for precise measurement")
        print(f"{'='*100}")
        
        overall_start = time.time()
        
        # Define test cases
        test_cases = [
            # Collection 1 → Collection 2 tests
            {
                'from_collection': collection1,
                'to_collection': collection2,
                'match_conditions': {"first_name": "Charles", "last_name": "Miller"},
                'join_field': 'first_name',
                'test_name': 'C1 → C2: first_name join (Charles Miller)'
            },
            {
                'from_collection': collection1,
                'to_collection': collection2,
                'match_conditions': {"first_name": "John", "age": {"$gte": 25}},
                'join_field': 'first_name',
                'test_name': 'C1 → C2: first_name join (John, age≥25)'
            },
            {
                'from_collection': collection1,
                'to_collection': collection2,
                'match_conditions': {"country": "USA", "age": {"$lt": 40}},
                'join_field': 'country',
                'test_name': 'C1 → C2: country join (USA, age<40)'
            },
            
            # Collection 2 → Collection 1 tests
            {
                'from_collection': collection2,
                'to_collection': collection1,
                'match_conditions': {"first_name": "Charles", "last_name": "Miller"},
                'join_field': 'first_name',
                'test_name': 'C2 → C1: first_name join (Charles Miller)'
            },
            {
                'from_collection': collection2,
                'to_collection': collection1,
                'match_conditions': {"first_name": "John", "age": {"$gte": 25}},
                'join_field': 'first_name',
                'test_name': 'C2 → C1: first_name join (John, age≥25)'
            },
            {
                'from_collection': collection2,
                'to_collection': collection1,
                'match_conditions': {"country": "USA", "age": {"$lt": 40}},
                'join_field': 'country',
                'test_name': 'C2 → C1: country join (USA, age<40)'
            }
        ]
        
        try:
            print(f"\nExecuting {len(test_cases)} join tests...")
            
            for i, test_case in enumerate(test_cases, 1):
                print(f"\n{'#'*20} TEST {i}/{len(test_cases)} {'#'*20}")
                
                result = self.run_single_join_test(
                    from_collection=test_case['from_collection'],
                    to_collection=test_case['to_collection'],
                    match_conditions=test_case['match_conditions'],
                    join_field=test_case['join_field'],
                    test_name=test_case['test_name']
                )
                
                self.print_test_summary(result)
                
                # Small delay between tests
                time.sleep(0.5)
            
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user")
            return
        except Exception as e:
            print(f"Benchmark error: {e}")
            return
        
        overall_end = time.time()
        overall_time = overall_end - overall_start
        
        print(f"\nTotal benchmark execution time: {overall_time:.2f} seconds")
        
        # Print comprehensive summary
        self.print_final_summary()
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()


def get_user_input():
    """Get benchmark parameters from user input"""
    print("MongoDB Join Operations Benchmark Configuration")
    print("-" * 60)
    
    # Collection 1
    collection1 = input("Enter first collection name: ").strip()
    if not collection1:
        print("Collection name is required")
        return None, None
    
    # Collection 2
    collection2 = input("Enter second collection name: ").strip()
    if not collection2:
        print("Collection name is required")
        return None, None
    
    return collection1, collection2


def main():
    """Main function"""
    print("MongoDB Join Operations Benchmark Tool")
    print("=" * 60)
    print("This tool performs 6 specific join tests:")
    print("• 3 tests per direction (C1→C2, C2→C1)")
    print("• 2 tests on 'first_name' field per direction")
    print("• 1 test on 'country' field per direction")
    print("• Multi-field match conditions for precise results")
    print("• Single operation per test for accurate measurement")
    print("=" * 60)
    
    # Initialize benchmark
    benchmark = MongoDBJoinBenchmark()
    
    # Test connection first
    if not benchmark.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        collection1, collection2 = get_user_input()
        
        if not all([collection1, collection2]):
            print("Invalid input provided")
            return 1
        
        # Validate collections
        print(f"\nValidating collections...")
        c1_valid, c1_count = benchmark.check_collection(collection1)
        c2_valid, c2_count = benchmark.check_collection(collection2)
        
        if not c1_valid or not c2_valid:
            response = input("One or more collections validation failed. Continue anyway? (y/N): ").strip().lower()
            if response != 'y':
                print("Benchmark cancelled")
                return 1
        
        print(f"\nCollection information:")
        print(f"  {collection1}: {c1_count:,} documents")
        print(f"  {collection2}: {c2_count:,} documents")
        
        # Confirm test execution
        print(f"\nReady to execute join benchmark tests:")
        print(f"• Testing joins between '{collection1}' and '{collection2}'")
        print(f"• 6 total tests will be executed")
        print(f"• Each test performs exactly 1 join operation")
        
        response = input("\nProceed with benchmark? (Y/n): ").strip().lower()
        if response == 'n':
            print("Benchmark cancelled")
            return 0
        
        # Run comprehensive join benchmark
        benchmark.run_comprehensive_join_benchmark(collection1, collection2)
        
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