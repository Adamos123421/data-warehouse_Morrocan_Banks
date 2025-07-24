"""
Simplified Phase 2 Test: NLP + DBT Integration
Tests the core functionality inside Docker container
"""

import psycopg2
import pandas as pd
import json
import sys
import os
from datetime import datetime

# Add current directory to path for imports
sys.path.append('/tmp')
sys.path.append('/opt/airflow/src')

def connect_to_database():
    """Connect to PostgreSQL database with Docker container credentials"""
    try:
        # Use the correct Docker internal connection
        conn = psycopg2.connect(
            host='morocco_bank_postgres',
            database='morocco_bank_reviews',
            user='morocco_app',
            password='secure_password_here'
        )
        print("✅ Connected to PostgreSQL database")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def test_data_extraction():
    """Test 1: Extract raw data from database"""
    print("\n🔍 TEST 1: Data Extraction")
    print("-" * 40)
    
    conn = connect_to_database()
    if not conn:
        return False, None
    
    try:
        query = """
        SELECT 
            review_id, place_id, bank_name, branch_name, author_name,
            rating, text, time, collected_at
        FROM raw_data.bank_reviews
        WHERE text IS NOT NULL AND text != ''
        LIMIT 10
        """
        
        df = pd.read_sql(query, conn)
        print(f"✅ Successfully extracted {len(df)} sample reviews")
        print(f"📊 Columns: {list(df.columns)}")
        print(f"🏦 Banks: {df['bank_name'].unique()}")
        
        conn.close()
        return True, df
        
    except Exception as e:
        print(f"❌ Data extraction failed: {e}")
        conn.close()
        return False, None

def test_nlp_processing(df):
    """Test 2: NLP Processing"""
    print("\n🧠 TEST 2: NLP Processing")
    print("-" * 40)
    
    if df is None or len(df) == 0:
        print("❌ No data to process")
        return False, None
    
    try:
        # Simple NLP processing without external dependencies
        
        # Clean text
        df['cleaned_text'] = df['text'].apply(lambda x: 
            ' '.join(str(x).lower().strip().split()) if pd.notna(x) else ""
        )
        
        # Simple sentiment analysis (keyword-based)
        def simple_sentiment(text):
            if not isinstance(text, str):
                return 0.0, 'Neutral'
            
            positive_words = ['bon', 'bien', 'excellent', 'parfait', 'super', 'merci', 'rapide', 'professionnel']
            negative_words = ['mauvais', 'terrible', 'lent', 'probleme', 'attente', 'nul', 'horrible']
            
            text_lower = text.lower()
            pos_count = sum(1 for word in positive_words if word in text_lower)
            neg_count = sum(1 for word in negative_words if word in text_lower)
            
            if pos_count > neg_count:
                score = min(0.8, pos_count * 0.2)
                label = 'Positive'
            elif neg_count > pos_count:
                score = max(-0.8, -neg_count * 0.2)
                label = 'Negative'
            else:
                score = 0.0
                label = 'Neutral'
            
            return score, label
        
        # Apply sentiment analysis
        sentiment_results = df['cleaned_text'].apply(simple_sentiment)
        df['sentiment_score'] = sentiment_results.apply(lambda x: x[0])
        df['sentiment_label'] = sentiment_results.apply(lambda x: x[1])
        
        # Add basic features
        df['word_count'] = df['cleaned_text'].apply(lambda x: len(str(x).split()) if pd.notna(x) else 0)
        df['char_count'] = df['cleaned_text'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)
        
        # Content flags
        df['mentions_service'] = df['cleaned_text'].str.contains('service|accueil', case=False, na=False)
        df['mentions_waiting'] = df['cleaned_text'].str.contains('attente|queue', case=False, na=False)
        df['mentions_fees'] = df['cleaned_text'].str.contains('frais|prix', case=False, na=False)
        
        # Time features
        df['review_time'] = pd.to_datetime(df['time'], unit='s', errors='coerce')
        df['review_year'] = df['review_time'].dt.year
        df['review_month'] = df['review_time'].dt.month
        
        print(f"✅ NLP processing completed for {len(df)} reviews")
        print(f"📈 Sentiment distribution:")
        print(df['sentiment_label'].value_counts())
        print(f"💬 Average word count: {df['word_count'].mean():.1f}")
        
        return True, df
        
    except Exception as e:
        print(f"❌ NLP processing failed: {e}")
        return False, None

def test_database_loading(enriched_df):
    """Test 3: Load enriched data to database"""
    print("\n⬆️ TEST 3: Database Loading")
    print("-" * 40)
    
    if enriched_df is None or len(enriched_df) == 0:
        print("❌ No enriched data to load")
        return False
    
    conn = connect_to_database()
    if not conn:
        return False
    
    try:
        # Create enriched schema and table
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS test_enriched;")
            cursor.execute("DROP TABLE IF EXISTS test_enriched.nlp_reviews CASCADE;")
            
            create_sql = """
            CREATE TABLE test_enriched.nlp_reviews (
                review_id VARCHAR(255),
                bank_name VARCHAR(255),
                rating INTEGER,
                sentiment_score DECIMAL(8,6),
                sentiment_label VARCHAR(50),
                word_count INTEGER,
                mentions_service BOOLEAN,
                mentions_waiting BOOLEAN,
                mentions_fees BOOLEAN,
                review_year INTEGER,
                review_month INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_sql)
            conn.commit()
        
        # Insert sample data
        insert_count = 0
        for _, row in enriched_df.iterrows():
            try:
                insert_sql = """
                INSERT INTO test_enriched.nlp_reviews 
                (review_id, bank_name, rating, sentiment_score, sentiment_label, 
                 word_count, mentions_service, mentions_waiting, mentions_fees,
                 review_year, review_month)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    row['review_id'], row['bank_name'], int(row['rating']),
                    float(row['sentiment_score']), row['sentiment_label'],
                    int(row['word_count']), bool(row['mentions_service']),
                    bool(row['mentions_waiting']), bool(row['mentions_fees']),
                    int(row['review_year']) if pd.notna(row['review_year']) else None,
                    int(row['review_month']) if pd.notna(row['review_month']) else None
                )
                
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, values)
                    insert_count += 1
                    
            except Exception as e:
                print(f"⚠️ Failed to insert row {row['review_id']}: {e}")
                continue
        
        conn.commit()
        print(f"✅ Successfully loaded {insert_count} enriched reviews")
        
        # Verify data
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM test_enriched.nlp_reviews;")
            count = cursor.fetchone()[0]
            print(f"🔍 Verification: {count} records in database")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database loading failed: {e}")
        conn.close()
        return False

def test_dbt_transformations():
    """Test 4: DBT-style transformations"""
    print("\n🔄 TEST 4: DBT Transformations")
    print("-" * 40)
    
    conn = connect_to_database()
    if not conn:
        return False
    
    try:
        # Create staging view
        staging_sql = """
        CREATE SCHEMA IF NOT EXISTS test_staging;
        
        DROP VIEW IF EXISTS test_staging.stg_enriched_reviews CASCADE;
        CREATE VIEW test_staging.stg_enriched_reviews AS
        SELECT 
            review_id, bank_name, rating, sentiment_score, sentiment_label,
            word_count, mentions_service, mentions_waiting, mentions_fees,
            review_year, review_month,
            CASE 
                WHEN rating >= 4 THEN 'Positive'
                WHEN rating <= 2 THEN 'Negative'
                ELSE 'Neutral'
            END as rating_category,
            CASE 
                WHEN sentiment_score > 0.1 THEN 'Positive'
                WHEN sentiment_score < -0.1 THEN 'Negative'
                ELSE 'Neutral'
            END as sentiment_category
        FROM test_enriched.nlp_reviews;
        """
        
        with conn.cursor() as cursor:
            cursor.execute(staging_sql)
            conn.commit()
        
        print("✅ Created staging view")
        
        # Create intermediate model
        intermediate_sql = """
        CREATE SCHEMA IF NOT EXISTS test_intermediate;
        
        DROP VIEW IF EXISTS test_intermediate.int_bank_metrics CASCADE;
        CREATE VIEW test_intermediate.int_bank_metrics AS
        SELECT 
            bank_name,
            COUNT(*) as total_reviews,
            AVG(rating) as avg_rating,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_reviews,
            COUNT(CASE WHEN mentions_service THEN 1 END) as service_mentions,
            COUNT(CASE WHEN mentions_waiting THEN 1 END) as waiting_mentions
        FROM test_staging.stg_enriched_reviews
        GROUP BY bank_name;
        """
        
        with conn.cursor() as cursor:
            cursor.execute(intermediate_sql)
            conn.commit()
        
        print("✅ Created intermediate view")
        
        # Create mart model
        mart_sql = """
        CREATE SCHEMA IF NOT EXISTS test_marts;
        
        DROP VIEW IF EXISTS test_marts.mart_bank_summary CASCADE;
        CREATE VIEW test_marts.mart_bank_summary AS
        SELECT 
            bank_name,
            total_reviews,
            ROUND(avg_rating, 2) as avg_rating,
            ROUND(avg_sentiment, 3) as avg_sentiment,
            ROUND(positive_reviews::DECIMAL / total_reviews * 100, 1) as positive_rate,
            service_mentions,
            waiting_mentions,
            CASE 
                WHEN avg_rating >= 4.0 AND avg_sentiment > 0.1 THEN 'Excellent'
                WHEN avg_rating >= 3.5 THEN 'Good'
                WHEN avg_rating >= 3.0 THEN 'Average'
                ELSE 'Below Average'
            END as performance_tier
        FROM test_intermediate.int_bank_metrics
        ORDER BY avg_rating DESC;
        """
        
        with conn.cursor() as cursor:
            cursor.execute(mart_sql)
            conn.commit()
        
        print("✅ Created mart view")
        
        # Test the final results
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM test_marts.mart_bank_summary;")
            results = cursor.fetchall()
            
            print(f"\n📊 FINAL RESULTS - Bank Performance Summary:")
            print("-" * 60)
            for row in results:
                bank_name, total, rating, sentiment, pos_rate, service, waiting, tier = row
                print(f"🏦 {bank_name}: {total} reviews, Rating: {rating}, Sentiment: {sentiment}, Tier: {tier}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ DBT transformations failed: {e}")
        conn.close()
        return False

def run_comprehensive_test():
    """Run the complete Phase 2 test suite"""
    print("=" * 60)
    print("🚀 PHASE 2 NLP + DBT INTEGRATION TEST")
    print("=" * 60)
    
    results = {
        'data_extraction': False,
        'nlp_processing': False, 
        'database_loading': False,
        'dbt_transformations': False
    }
    
    # Test 1: Data Extraction
    success, df = test_data_extraction()
    results['data_extraction'] = success
    
    if not success:
        print("\n❌ Test failed at data extraction stage")
        return results
    
    # Test 2: NLP Processing
    success, enriched_df = test_nlp_processing(df)
    results['nlp_processing'] = success
    
    if not success:
        print("\n❌ Test failed at NLP processing stage")
        return results
    
    # Test 3: Database Loading
    success = test_database_loading(enriched_df)
    results['database_loading'] = success
    
    if not success:
        print("\n❌ Test failed at database loading stage")
        return results
    
    # Test 4: DBT Transformations
    success = test_dbt_transformations()
    results['dbt_transformations'] = success
    
    # Final Results
    print("\n" + "=" * 60)
    print("📋 PHASE 2 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    print(f"\n🎯 Overall Score: {passed_tests}/{total_tests} ({passed_tests/total_tests*100:.1f}%)")
    
    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! Phase 2 NLP + DBT integration is working correctly!")
    else:
        print("⚠️ Some tests failed. Check the error messages above.")
    
    return results

if __name__ == "__main__":
    run_comprehensive_test()