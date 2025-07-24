"""
Complete Data Pipeline Automation DAG - FIXED VERSION
Uses direct PostgreSQL connections instead of docker exec
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
import logging

# DAG Configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'complete_data_pipeline_fixed',
    default_args=default_args,
    description='Complete Data Pipeline: Direct PostgreSQL Connection (FIXED)',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=['pipeline', 'etl', 'star-schema', 'fixed'],
)

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host='morocco_bank_postgres',
            database='morocco_bank_reviews',
            user='morocco_app',
            password='secure_password_here'
        )
        return conn
    except Exception as e:
        logging.error(f"❌ Database connection failed: {e}")
        raise

def execute_sql(sql_command, description=""):
    """Execute SQL command directly on PostgreSQL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(sql_command)
        conn.commit()
        
        # Get results if it's a SELECT
        if sql_command.strip().upper().startswith('SELECT'):
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            logging.info(f"✅ {description}")
            return True, result
        else:
            cursor.close() 
            conn.close()
            logging.info(f"✅ {description}")
            return True, None
            
    except Exception as e:
        logging.error(f"❌ {description}: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False, str(e)

def check_data_availability():
    """Check if raw data is available for processing"""
    success, result = execute_sql(
        "SELECT COUNT(*) FROM raw_data.bank_reviews;",
        "Checking raw data availability"
    )
    
    if success and result:
        count = result[0][0]
        logging.info(f"✅ Found {count} records in raw data")
        return count > 0
    return False

def create_complete_star_schema():
    """Create the complete star schema in one go"""
    try:
        logging.info("🚀 Creating complete star schema...")
        
        # Drop existing tables to ensure clean state
        cleanup_sql = """
        DROP VIEW IF EXISTS vw_bank_performance_dashboard CASCADE;
        DROP VIEW IF EXISTS vw_geographic_analysis CASCADE;
        DROP VIEW IF EXISTS vw_monthly_trends CASCADE;
        DROP TABLE IF EXISTS fact_reviews CASCADE;
        DROP TABLE IF EXISTS dim_bank CASCADE;
        DROP TABLE IF EXISTS dim_branch CASCADE;
        DROP TABLE IF EXISTS dim_sentiment CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP VIEW IF EXISTS staging.stg_bank_reviews CASCADE;
        DROP SCHEMA IF EXISTS staging CASCADE;
        """
        
        success, _ = execute_sql(cleanup_sql, "Cleaning up existing tables")
        
        # Create staging schema and view
        staging_sql = """
        CREATE SCHEMA IF NOT EXISTS staging;
        
        CREATE OR REPLACE VIEW staging.stg_bank_reviews AS
        SELECT 
            place_id,
            TRIM(bank_name) as bank_name,
            TRIM(branch_name) as branch_name,
            TRIM(author_name) as author_name,
            CAST(rating AS INTEGER) as rating,
            CAST(sentiment_score AS DECIMAL(8,6)) as sentiment_score,
            LOWER(TRIM(sentiment_label)) as sentiment_label,
            CAST(word_count AS INTEGER) as word_count,
            CAST(char_count AS INTEGER) as char_count,
            CAST(review_time AS TIMESTAMP) as review_time,
            TRIM(cleaned_text) as cleaned_text,
            TRIM(dominant_topic) as dominant_topic,
            mentions_service,
            mentions_waiting,
            mentions_fees,
            review_id,
            TRIM(text) as review_text,
            CAST(collected_at AS TIMESTAMP) as collected_at
        FROM raw_data.bank_reviews
        WHERE bank_name IS NOT NULL 
          AND review_id IS NOT NULL
          AND rating IS NOT NULL;
        """
        
        success, _ = execute_sql(staging_sql, "Creating staging schema and view")
        if not success:
            raise Exception("Failed to create staging")
        
        # Create all dimension tables
        dimensions_sql = """
        -- Create dim_sentiment
        CREATE TABLE dim_sentiment AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY sentiment_id) as sentiment_key,
            sentiment_id,
            sentiment_label,
            sentiment_description,
            score_range_min,
            score_range_max
        FROM (
            VALUES 
                ('positive', 'Positive', 'Positive sentiment', 0.1, 1.0),
                ('negative', 'Negative', 'Negative sentiment', -1.0, -0.1),
                ('neutral', 'Neutral', 'Neutral sentiment', -0.1, 0.1),
                ('very_positive', 'Very Positive', 'Very positive sentiment', 0.5, 1.0),
                ('very_negative', 'Very Negative', 'Very negative sentiment', -1.0, -0.5)
        ) AS sentiment_data(sentiment_id, sentiment_label, sentiment_description, score_range_min, score_range_max);
        
        ALTER TABLE dim_sentiment ADD PRIMARY KEY (sentiment_key);
        
        -- Create dim_date
        CREATE TABLE dim_date AS
        WITH date_spine AS (
            SELECT generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval)::date AS full_date
        )
        SELECT 
            EXTRACT(YEAR FROM full_date) * 10000 + EXTRACT(MONTH FROM full_date) * 100 + EXTRACT(DAY FROM full_date) AS date_key,
            full_date,
            EXTRACT(YEAR FROM full_date) AS year,
            EXTRACT(QUARTER FROM full_date) AS quarter,
            EXTRACT(MONTH FROM full_date) AS month,
            TRIM(TO_CHAR(full_date, 'Month')) AS month_name
        FROM date_spine;
        
        ALTER TABLE dim_date ADD PRIMARY KEY (date_key);
        
        -- Create dim_bank
        CREATE TABLE dim_bank AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY bank_name) as bank_key,
            LOWER(REPLACE(bank_name, ' ', '_')) as bank_id,
            bank_name,
            'Commercial' as bank_type,
            COUNT(DISTINCT place_id) as total_branches
        FROM staging.stg_bank_reviews
        WHERE bank_name IS NOT NULL
        GROUP BY bank_name;
        
        ALTER TABLE dim_bank ADD PRIMARY KEY (bank_key);
        
        -- Create dim_branch
        CREATE TABLE dim_branch AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY place_id) as branch_key,
            place_id as branch_id,
            branch_name,
            bank_name,
            CASE 
                WHEN UPPER(branch_name) LIKE '%CASABLANCA%' THEN 'Casablanca'
                WHEN UPPER(branch_name) LIKE '%RABAT%' THEN 'Rabat'
                ELSE 'Other'
            END AS city
        FROM (
            SELECT DISTINCT place_id, branch_name, bank_name
            FROM staging.stg_bank_reviews
            WHERE place_id IS NOT NULL
        ) t;
        
        ALTER TABLE dim_branch ADD PRIMARY KEY (branch_key);
        """
        
        success, _ = execute_sql(dimensions_sql, "Creating dimension tables")
        if not success:
            raise Exception("Failed to create dimensions")
        
        # Create fact table
        fact_sql = """
        CREATE TABLE fact_reviews AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY sr.review_id) as review_key,
            db.bank_key,
            dbr.branch_key,
            CASE 
                WHEN sr.sentiment_score >= 0.5 THEN (SELECT sentiment_key FROM dim_sentiment WHERE sentiment_id = 'very_positive' LIMIT 1)
                WHEN sr.sentiment_score >= 0.1 THEN (SELECT sentiment_key FROM dim_sentiment WHERE sentiment_id = 'positive' LIMIT 1)
                WHEN sr.sentiment_score <= -0.5 THEN (SELECT sentiment_key FROM dim_sentiment WHERE sentiment_id = 'very_negative' LIMIT 1)
                WHEN sr.sentiment_score <= -0.1 THEN (SELECT sentiment_key FROM dim_sentiment WHERE sentiment_id = 'negative' LIMIT 1)
                ELSE (SELECT sentiment_key FROM dim_sentiment WHERE sentiment_id = 'neutral' LIMIT 1)
            END as sentiment_key,
            CASE 
                WHEN sr.review_time IS NOT NULL THEN
                    EXTRACT(YEAR FROM sr.review_time) * 10000 + 
                    EXTRACT(MONTH FROM sr.review_time) * 100 + 
                    EXTRACT(DAY FROM sr.review_time)
                ELSE NULL
            END as date_key,
            sr.review_id,
            sr.rating,
            sr.sentiment_score,
            sr.word_count,
            sr.mentions_service,
            sr.mentions_waiting,
            sr.mentions_fees,
            sr.review_time as review_timestamp,
            sr.collected_at,
            CURRENT_TIMESTAMP as created_at
        FROM staging.stg_bank_reviews sr
        LEFT JOIN dim_bank db ON sr.bank_name = db.bank_name
        LEFT JOIN dim_branch dbr ON sr.place_id = dbr.branch_id
        WHERE db.bank_key IS NOT NULL;
        
        ALTER TABLE fact_reviews ADD PRIMARY KEY (review_key);
        CREATE INDEX idx_fact_reviews_bank_key ON fact_reviews(bank_key);
        CREATE INDEX idx_fact_reviews_branch_key ON fact_reviews(branch_key);
        """
        
        success, _ = execute_sql(fact_sql, "Creating fact table")
        if not success:
            raise Exception("Failed to create fact table")
        
        # Create BI views
        bi_views_sql = """
        CREATE OR REPLACE VIEW vw_bank_performance_dashboard AS
        SELECT 
            db.bank_name,
            db.bank_type,
            COUNT(fr.review_key) as total_reviews,
            ROUND(AVG(fr.rating), 2) as avg_rating,
            ROUND(AVG(fr.sentiment_score), 4) as avg_sentiment,
            COUNT(DISTINCT fr.branch_key) as total_branches
        FROM fact_reviews fr
        JOIN dim_bank db ON fr.bank_key = db.bank_key
        GROUP BY db.bank_key, db.bank_name, db.bank_type
        ORDER BY total_reviews DESC;
        
        CREATE OR REPLACE VIEW vw_geographic_analysis AS
        SELECT 
            dbr.city,
            COUNT(fr.review_key) as total_reviews,
            ROUND(AVG(fr.rating), 2) as avg_rating,
            ROUND(AVG(fr.sentiment_score), 4) as avg_sentiment
        FROM fact_reviews fr
        JOIN dim_branch dbr ON fr.branch_key = dbr.branch_key
        GROUP BY dbr.city
        ORDER BY total_reviews DESC;
        """
        
        success, _ = execute_sql(bi_views_sql, "Creating BI views")
        if not success:
            raise Exception("Failed to create BI views")
        
        logging.info("✅ Complete star schema created successfully!")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to create star schema: {e}")
        raise

def validate_results():
    """Validate the pipeline results"""
    try:
        logging.info("🔍 Validating results...")
        
        # Check all table counts
        tables = ['dim_bank', 'dim_branch', 'dim_sentiment', 'dim_date', 'fact_reviews']
        
        for table in tables:
            success, result = execute_sql(
                f"SELECT COUNT(*) FROM {table};",
                f"Checking {table} count"
            )
            if success and result:
                count = result[0][0]
                logging.info(f"  📊 {table}: {count:,} records")
        
        logging.info("✅ Validation completed successfully!")
        return True
        
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        raise

# Define Airflow tasks
task_check_data = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

task_create_star_schema = PythonOperator(
    task_id='create_complete_star_schema',
    python_callable=create_complete_star_schema,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
)

# Define task dependencies
task_check_data >> task_create_star_schema >> task_validate 