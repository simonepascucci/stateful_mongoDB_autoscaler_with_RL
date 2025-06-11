#!/usr/bin/env python3
"""
MongoDB Age Field Converter Script
Simple script to convert string age fields to integers in a specified collection
"""

import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


class AgeFieldConverter:
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
    
    def analyze_age_field(self, collection_name):
        """Analyze the age field in the collection"""
        try:
            collection = self.db[collection_name]
            
            print(f"\nAnalyzing age field in collection '{collection_name}'...")
            
            # Check total documents
            total_docs = collection.estimated_document_count()
            print(f"Total documents in collection: {total_docs:,}")
            
            # Sample a few documents to understand the age field
            sample_docs = list(collection.find({}, {"age": 1}).limit(5))
            
            if not sample_docs:
                print("No documents found in collection")
                return False
            
            print(f"\nSample age values:")
            for i, doc in enumerate(sample_docs, 1):
                age_value = doc.get('age', 'NOT_FOUND')
                age_type = type(age_value).__name__ if age_value != 'NOT_FOUND' else 'MISSING'
                print(f"  Document {i}: age = {age_value} (type: {age_type})")
            
            # Count documents with string age fields
            string_age_count = collection.count_documents({"age": {"$type": "string"}})
            int_age_count = collection.count_documents({"age": {"$type": "int"}})
            missing_age_count = collection.count_documents({"age": {"$exists": False}})
            
            print(f"\nAge field analysis:")
            print(f"  String age fields: {string_age_count:,}")
            print(f"  Integer age fields: {int_age_count:,}")
            print(f"  Missing age fields: {missing_age_count:,}")
            
            if string_age_count == 0:
                print("✓ No string age fields found - conversion not needed")
                return False
            
            print(f"\n{string_age_count:,} documents need age field conversion")
            return True
            
        except Exception as e:
            print(f"✗ Error analyzing collection: {e}")
            return False
    
    def convert_age_fields(self, collection_name, dry_run=True):
        """Convert string age fields to integers"""
        try:
            collection = self.db[collection_name]
            
            if dry_run:
                print(f"\n{'='*60}")
                print("DRY RUN MODE - No actual changes will be made")
                print(f"{'='*60}")
            else:
                print(f"\n{'='*60}")
                print("LIVE MODE - Converting age fields")
                print(f"{'='*60}")
            
            # Find documents with string age fields
            string_age_docs = collection.find({"age": {"$type": "string"}})
            
            converted_count = 0
            error_count = 0
            
            for doc in string_age_docs:
                try:
                    age_str = doc['age']
                    age_int = int(age_str)
                    
                    if not dry_run:
                        # Update the document
                        result = collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"age": age_int}}
                        )
                        
                        if result.modified_count == 1:
                            converted_count += 1
                        else:
                            print(f"Warning: Document {doc['_id']} was not updated")
                    else:
                        print(f"Would convert: {age_str} -> {age_int} (Document ID: {doc['_id']})")
                        converted_count += 1
                    
                    # Show progress every 1000 documents
                    if converted_count % 1000 == 0:
                        print(f"Progress: {converted_count:,} documents processed...")
                
                except ValueError:
                    error_count += 1
                    print(f"✗ Cannot convert age '{doc['age']}' to integer in document {doc['_id']}")
                except Exception as e:
                    error_count += 1
                    print(f"✗ Error processing document {doc['_id']}: {e}")
            
            print(f"\n{'='*60}")
            print("CONVERSION SUMMARY")
            print(f"{'='*60}")
            
            if dry_run:
                print(f"Documents that would be converted: {converted_count:,}")
                print(f"Documents with conversion errors: {error_count:,}")
                print("\nNo actual changes were made (dry run mode)")
            else:
                print(f"Documents successfully converted: {converted_count:,}")
                print(f"Documents with conversion errors: {error_count:,}")
                print(f"✓ Age field conversion completed")
            
            return converted_count, error_count
            
        except Exception as e:
            print(f"✗ Error during conversion: {e}")
            return 0, 0
    
    def verify_conversion(self, collection_name):
        """Verify the conversion was successful"""
        try:
            collection = self.db[collection_name]
            
            print(f"\nVerifying conversion in collection '{collection_name}'...")
            
            # Count different age field types after conversion
            string_age_count = collection.count_documents({"age": {"$type": "string"}})
            int_age_count = collection.count_documents({"age": {"$type": "int"}})
            
            print(f"After conversion:")
            print(f"  String age fields remaining: {string_age_count:,}")
            print(f"  Integer age fields: {int_age_count:,}")
            
            if string_age_count == 0:
                print("✓ All age fields successfully converted to integers")
            else:
                print(f"⚠ {string_age_count:,} string age fields still remain")
            
            # Show sample converted documents
            sample_docs = list(collection.find({"age": {"$type": "int"}}, {"age": 1}).limit(3))
            if sample_docs:
                print(f"\nSample converted age values:")
                for i, doc in enumerate(sample_docs, 1):
                    print(f"  Document {i}: age = {doc['age']} (type: {type(doc['age']).__name__})")
            
        except Exception as e:
            print(f"✗ Error during verification: {e}")
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()


def get_user_input():
    """Get collection name from user input"""
    print("MongoDB Age Field Converter")
    print("-" * 40)
    
    collection_name = input("Enter collection name: ").strip()
    if not collection_name:
        print("Collection name is required")
        return None
    
    return collection_name


def main():
    """Main function"""
    print("MongoDB Age Field Converter Tool")
    print("=" * 50)
    print("This tool converts string age fields to integers")
    print("=" * 50)
    
    # Initialize converter
    converter = AgeFieldConverter()
    
    # Test connection first
    if not converter.connect():
        print("Cannot proceed without database connection")
        return 1
    
    try:
        # Get user input
        collection_name = get_user_input()
        
        if not collection_name:
            print("Invalid collection name provided")
            return 1
        
        # Analyze the age field first
        needs_conversion = converter.analyze_age_field(collection_name)
        
        if not needs_conversion:
            print("No conversion needed. Exiting.")
            return 0
        
        # Ask user if they want to proceed with dry run first
        print(f"\nOptions:")
        print(f"1. Dry run (preview changes without making them)")
        print(f"2. Live conversion (actually update the documents)")
        print(f"3. Cancel")
        
        choice = input("\nSelect option (1/2/3): ").strip()
        
        if choice == '1':
            # Dry run
            converter.convert_age_fields(collection_name, dry_run=True)
            
            # Ask if user wants to proceed with actual conversion
            proceed = input("\nProceed with actual conversion? (y/N): ").strip().lower()
            if proceed == 'y':
                converted, errors = converter.convert_age_fields(collection_name, dry_run=False)
                if converted > 0:
                    converter.verify_conversion(collection_name)
        
        elif choice == '2':
            # Direct live conversion
            confirm = input(f"\nThis will modify documents in '{collection_name}'. Are you sure? (y/N): ").strip().lower()
            if confirm == 'y':
                converted, errors = converter.convert_age_fields(collection_name, dry_run=False)
                if converted > 0:
                    converter.verify_conversion(collection_name)
        
        elif choice == '3':
            print("Conversion cancelled")
            return 0
        
        else:
            print("Invalid choice")
            return 1
    
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    finally:
        converter.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())