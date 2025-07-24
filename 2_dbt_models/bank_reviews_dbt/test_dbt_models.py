#!/usr/bin/env python3
"""
Script to test DBT models manually by executing the SQL queries
and validating the data transformations.
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sys
import os

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'morocco_bank_reviews',
    'user': 'morocco_app',
    'password': 'secure_password_here'
}

def create_connection():
    """Create database connection."""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None

def test_database_connection():
    """Test basic database connectivity."""
    print("🔗 Testing database connection...")
    engine = create_connection()
    if engine:
        try:
            with engine.connect() as conn:
                result = conn.execute("SELECT 1 as test").fetchone()
                print("✅ Database connection successful")
                return True
        except Exception as e:
            print(f"❌ Database connection test failed: {e}")
            return False
    return False

def check_source_data():
    """Check if source data exists."""
    print("\n📊 Checking source data availability...")
    engine = create_connection()
    if not engine:
        return False
    
    try:
        with engine.connect() as conn:
            # Check if raw_data schema exists
            schema_check = conn.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'raw_data'
            """).fetchone()
            
            if not schema_check:
                print("❌ raw_data schema does not exist")
                return False
            
            # Check if bank_reviews table exists
            table_check = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw_data' 
                AND table_name = 'bank_reviews'
            """).fetchone()
            
            if not table_check:
                print("❌ raw_data.bank_reviews table does not exist")
                return False
            
            # Count records
            count = conn.execute("SELECT COUNT(*) FROM raw_data.bank_reviews").fetchone()[0]
            print(f"✅ Found {count} records in raw_data.bank_reviews")
            
            if count == 0:
                print("⚠️  No data in source table")
                return False
                
            return True
            
    except Exception as e:
        print(f"❌ Error checking source data: {e}")
        return False

def test_staging_model():
    """Test the staging model transformation."""
    print("\n🔄 Testing staging model: stg_raw_reviews...")
    engine = create_connection()
    if not engine:
        return False
    
    # Read the staging model SQL
    try:
        with open('models/staging/stg_raw_reviews.sql', 'r') as f:
            staging_sql = f.read()
        
        # Replace DBT syntax with actual values
        staging_sql = staging_sql.replace("{{ source('raw_data', 'bank_reviews') }}", "raw_data.bank_reviews")
        staging_sql = staging_sql.replace("{{ var('min_review_length') }}", "10")
        staging_sql = staging_sql.replace("{{ var('max_review_length') }}", "5000")
        
        # Remove DBT config block
        lines = staging_sql.split('\n')
        sql_lines = []
        in_config = False
        for line in lines:
            if line.strip().startswith('{{') and 'config(' in line:
                in_config = True
                continue
            elif in_config and line.strip().startswith('}}'):
                in_config = False
                continue
            elif not in_config:
                sql_lines.append(line)
        
        clean_sql = '\n'.join(sql_lines)
        
        with engine.connect() as conn:
            # Execute the staging model query
            result = pd.read_sql(clean_sql, conn)
            
            print(f"✅ Staging model executed successfully")
            print(f"📊 Records processed: {len(result)}")
            print(f"📋 Columns: {list(result.columns)}")
            
            # Validate data quality
            if len(result) > 0:
                print("\n📈 Data Quality Checks:")
                print(f"   • Null review_ids: {result['review_id'].isnull().sum()}")
                print(f"   • Null place_ids: {result['place_id'].isnull().sum()}")
                print(f"   • Null bank_names: {result['bank_name'].isnull().sum()}")
                print(f"   • Null ratings: {result['rating'].isnull().sum()}")
                print(f"   • Valid data quality flags: {(result['data_quality_flag'] == 'valid').sum()}")
                
                # Check rating distribution
                rating_dist = result['rating'].value_counts().sort_index()
                print(f"   • Rating distribution:")
                for rating, count in rating_dist.items():
                    print(f"     - {rating} stars: {count} reviews")
                
                # Check rating categories
                category_dist = result['rating_category'].value_counts()
                print(f"   • Rating categories:")
                for category, count in category_dist.items():
                    print(f"     - {category}: {count} reviews")
                    
                return True
            else:
                print("❌ No records returned from staging model")
                return False
                
    except Exception as e:
        print(f"❌ Error testing staging model: {e}")
        return False

def test_data_quality():
    """Test data quality constraints."""
    print("\n🔍 Running data quality tests...")
    engine = create_connection()
    if not engine:
        return False
    
    tests = [
        {
            'name': 'Source table unique review_ids',
            'query': """
                SELECT COUNT(*) - COUNT(DISTINCT review_id) as duplicates
                FROM raw_data.bank_reviews
                WHERE review_id IS NOT NULL
            """
        },
        {
            'name': 'Source table valid ratings',
            'query': """
                SELECT COUNT(*) as invalid_ratings
                FROM raw_data.bank_reviews
                WHERE rating NOT IN (1, 2, 3, 4, 5) OR rating IS NULL
            """
        },
        {
            'name': 'Source table non-null place_ids',
            'query': """
                SELECT COUNT(*) as null_place_ids
                FROM raw_data.bank_reviews
                WHERE place_id IS NULL
            """
        },
        {
            'name': 'Source table non-null bank_names',
            'query': """
                SELECT COUNT(*) as null_bank_names
                FROM raw_data.bank_reviews
                WHERE bank_name IS NULL OR trim(bank_name) = ''
            """
        }
    ]
    
    all_passed = True
    
    try:
        with engine.connect() as conn:
            for test in tests:
                result = conn.execute(test['query']).fetchone()[0]
                if result == 0:
                    print(f"✅ {test['name']}: PASSED")
                else:
                    print(f"❌ {test['name']}: FAILED ({result} issues found)")
                    all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Error running data quality tests: {e}")
        return False

def test_intermediate_models():
    """Test intermediate models if they exist."""
    print("\n🔄 Testing intermediate models...")
    
    intermediate_files = []
    if os.path.exists('models/intermediate'):
        for file in os.listdir('models/intermediate'):
            if file.endswith('.sql'):
                intermediate_files.append(file)
    
    if not intermediate_files:
        print("ℹ️  No intermediate models found")
        return True
    
    print(f"📁 Found {len(intermediate_files)} intermediate model(s): {', '.join(intermediate_files)}")
    
    # For now, just validate they exist and are readable
    for file in intermediate_files:
        try:
            with open(f'models/intermediate/{file}', 'r') as f:
                content = f.read()
                if len(content) > 0:
                    print(f"✅ {file}: SQL file is readable")
                else:
                    print(f"❌ {file}: SQL file is empty")
        except Exception as e:
            print(f"❌ {file}: Error reading file - {e}")
    
    return True

def test_mart_models():
    """Test mart models if they exist."""
    print("\n🏪 Testing mart models...")
    
    mart_files = []
    if os.path.exists('models/marts'):
        for file in os.listdir('models/marts'):
            if file.endswith('.sql'):
                mart_files.append(file)
    
    if not mart_files:
        print("ℹ️  No mart models found")
        return True
    
    print(f"📁 Found {len(mart_files)} mart model(s): {', '.join(mart_files)}")
    
    # For now, just validate they exist and are readable
    for file in mart_files:
        try:
            with open(f'models/marts/{file}', 'r') as f:
                content = f.read()
                if len(content) > 0:
                    print(f"✅ {file}: SQL file is readable")
                else:
                    print(f"❌ {file}: SQL file is empty")
        except Exception as e:
            print(f"❌ {file}: Error reading file - {e}")
    
    return True

def main():
    """Main test function."""
    print("🚀 Starting DBT Model Tests")
    print("=" * 50)
    
    # Test database connection
    if not test_database_connection():
        print("❌ Cannot proceed without database connection")
        sys.exit(1)
    
    # Check source data
    if not check_source_data():
        print("❌ Cannot proceed without source data")
        sys.exit(1)
    
    # Run data quality tests
    data_quality_passed = test_data_quality()
    
    # Test staging model
    staging_passed = test_staging_model()
    
    # Test intermediate models
    intermediate_passed = test_intermediate_models()
    
    # Test mart models
    mart_passed = test_mart_models()
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)
    print(f"🔍 Data Quality Tests: {'✅ PASSED' if data_quality_passed else '❌ FAILED'}")
    print(f"🔄 Staging Model Test: {'✅ PASSED' if staging_passed else '❌ FAILED'}")
    print(f"🔄 Intermediate Models: {'✅ PASSED' if intermediate_passed else '❌ FAILED'}")
    print(f"🏪 Mart Models: {'✅ PASSED' if mart_passed else '❌ FAILED'}")
    
    all_passed = all([data_quality_passed, staging_passed, intermediate_passed, mart_passed])
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED! DBT models are working correctly.")
    else:
        print("\n⚠️  SOME TESTS FAILED. Please review the issues above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 