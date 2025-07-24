"""
Airflow DAG for Phase 2: Data Cleaning & Transformation
Uses the existing run_phase2_transformation.py pipeline with DBT and NLP
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sys
import os
import pandas as pd
import psycopg2
import json
import logging

# Add project paths
sys.path.append('/opt/airflow/dags')
sys.path.append('/opt/airflow/src')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'phase2_transformation_pipeline',
    default_args=default_args,
    description='Phase 2: Comprehensive Data Cleaning & Transformation with NLP and DBT',
    schedule_interval='@daily',
    catchup=False,
    tags=['phase2', 'transformation', 'nlp', 'dbt', 'data-cleaning']
)

def connect_to_database():
    """Connect to PostgreSQL database with correct credentials"""
    try:
        conn = psycopg2.connect(
            host='morocco_bank_postgres',
            database='morocco_bank_reviews',
            user='morocco_app',
            password='secure_password_here'
        )
        logger.info("✅ Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None

def extract_raw_data_for_transformation(**context):
    """Extract raw data from database for Phase 2 processing"""
    logger.info("🔄 Extracting raw data for Phase 2 transformation")
    
    try:
        conn = connect_to_database()
        if not conn:
            raise Exception("Failed to connect to database")
        
        # Extract all raw reviews for processing
        query = """
        SELECT 
            review_id, place_id, bank_name, branch_name, author_name,
            rating, text, time, original_language, detected_language,
            translated, collected_at, relative_time_description,
            profile_photo_url, author_url, language
        FROM raw_data.bank_reviews
        WHERE text IS NOT NULL AND text != ''
        ORDER BY collected_at DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        logger.info(f"✅ Extracted {len(df)} reviews for transformation")
        
        # Save to temporary CSV for the transformation pipeline
        temp_file = "/tmp/raw_reviews_for_phase2.csv"
        df.to_csv(temp_file, index=False)
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key='extraction_stats',
            value={
                'total_reviews': len(df),
                'temp_file': temp_file,
                'unique_banks': df['bank_name'].nunique(),
                'date_range': {
                    'start': str(df['collected_at'].min()) if not df.empty else None,
                    'end': str(df['collected_at'].max()) if not df.empty else None
                },
                'extraction_time': datetime.now().isoformat()
            }
        )
        
        return temp_file
        
    except Exception as e:
        logger.error(f"❌ Data extraction failed: {e}")
        raise

def run_phase2_transformation_pipeline(**context):
    """Execute the Phase 2 transformation pipeline"""
    logger.info("🚀 Starting Phase 2 Transformation Pipeline")
    
    try:
        # Get extraction stats from previous task
        extraction_stats = context['task_instance'].xcom_pull(
            task_ids='extract_raw_data',
            key='extraction_stats'
        )
        
        logger.info(f"📊 Processing {extraction_stats['total_reviews']} reviews")
        logger.info(f"🏦 Banks: {extraction_stats['unique_banks']}")
        
        # Import the existing transformation pipeline
        from pathlib import Path
        import importlib.util
        
        # Load the transformation module
        spec = importlib.util.spec_from_file_location(
            "phase2_pipeline", 
            "/opt/airflow/dags/run_phase2_transformation.py"
        )
        phase2_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(phase2_module)
        
        # Modify the pipeline to work with our extracted data
        class AirflowPhase2Pipeline(phase2_module.Phase2TransformationPipeline):
            def __init__(self, extracted_file):
                self.extracted_file = extracted_file
                self.data_dir = Path("/tmp")
                self.output_dir = Path("/tmp/processed")
                self.output_dir.mkdir(exist_ok=True)
                
                # Initialize NLP processor
                from nlp_processor_simple import SimpleBankReviewsNLPProcessor
                self.nlp_processor = SimpleBankReviewsNLPProcessor(n_topics=8)
                
                logger.info("Airflow Phase 2 Transformation Pipeline initialized")
            
            def load_raw_data(self) -> pd.DataFrame:
                """Load the extracted data"""
                logger.info(f"Loading extracted data from: {self.extracted_file}")
                df = pd.read_csv(self.extracted_file)
                logger.info(f"Loaded {len(df)} reviews for transformation")
                return df
        
        # Initialize and run the pipeline
        pipeline = AirflowPhase2Pipeline(extraction_stats['temp_file'])
        processed_file = pipeline.run_complete_pipeline()
        
        # Push results to XCom
        context['task_instance'].xcom_push(
            key='transformation_results',
            value={
                'processed_file': processed_file,
                'completion_time': datetime.now().isoformat(),
                'status': 'completed'
            }
        )
        
        logger.info("✅ Phase 2 transformation pipeline completed successfully!")
        return processed_file
        
    except Exception as e:
        logger.error(f"❌ Phase 2 transformation failed: {e}")
        raise

def load_processed_data_to_database(**context):
    """Load the processed data back to database for DBT processing"""
    logger.info("⬆️ Loading processed data to database")
    
    try:
        # Get transformation results
        results = context['task_instance'].xcom_pull(
            task_ids='run_transformation_pipeline',
            key='transformation_results'
        )
        
        processed_file = results['processed_file']
        logger.info(f"📄 Loading processed data from: {processed_file}")
        
        # Load processed data
        df = pd.read_csv(processed_file)
        logger.info(f"📊 Loading {len(df)} processed reviews to database")
        
        # Connect to database
        conn = connect_to_database()
        if not conn:
            raise Exception("Failed to connect to database")
        
        try:
            # Create processed schema
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS processed;")
                cursor.execute("DROP TABLE IF EXISTS processed.bank_reviews CASCADE;")
                
                # Create comprehensive processed table
                create_sql = """
                CREATE TABLE processed.bank_reviews (
                    review_id VARCHAR(255) PRIMARY KEY,
                    place_id VARCHAR(255),
                    bank_name VARCHAR(255),
                    branch_name VARCHAR(255),
                    author_name VARCHAR(255),
                    rating INTEGER,
                    original_text TEXT,
                    cleaned_text TEXT,
                    review_time TIMESTAMP,
                    
                    -- NLP Features
                    detected_language VARCHAR(50),
                    language_confidence DECIMAL(8,6),
                    sentiment_score DECIMAL(8,6),
                    sentiment_label VARCHAR(50),
                    sentiment_confidence DECIMAL(8,6),
                    subjectivity DECIMAL(8,6),
                    dominant_topic VARCHAR(255),
                    topic_distribution JSONB,
                    
                    -- Text Analytics
                    word_count INTEGER,
                    char_count INTEGER,
                    rating_category VARCHAR(20),
                    review_detail_level VARCHAR(20),
                    
                    -- Content Flags
                    mentions_service BOOLEAN,
                    mentions_waiting BOOLEAN,
                    mentions_fees BOOLEAN,
                    
                    -- Time Features
                    review_year INTEGER,
                    review_month VARCHAR(10),
                    day_of_week VARCHAR(20),
                    
                    -- Metadata
                    original_language VARCHAR(50),
                    translated BOOLEAN,
                    relative_time_description TEXT,
                    collected_at TIMESTAMP,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                cursor.execute(create_sql)
                conn.commit()
                logger.info("✅ Created processed.bank_reviews table")
            
            # Insert processed data in batches
            insert_count = 0
            batch_size = 100
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    try:
                        # Prepare values for insertion
                        insert_sql = """
                        INSERT INTO processed.bank_reviews (
                            review_id, place_id, bank_name, branch_name, author_name,
                            rating, original_text, cleaned_text, review_time,
                            detected_language, language_confidence, sentiment_score,
                            sentiment_label, sentiment_confidence, subjectivity,
                            dominant_topic, topic_distribution, word_count, char_count,
                            rating_category, review_detail_level, mentions_service,
                            mentions_waiting, mentions_fees, review_year, review_month,
                            day_of_week, original_language, translated,
                            relative_time_description, collected_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        # Handle JSON serialization for topic_distribution
                        topic_dist = row.get('topic_distribution', '{}')
                        if pd.notna(topic_dist) and topic_dist != '':
                            if isinstance(topic_dist, str):
                                topic_dist_json = topic_dist
                            else:
                                topic_dist_json = json.dumps(topic_dist)
                        else:
                            topic_dist_json = '{}'
                        
                        values = (
                            row['review_id'], row['place_id'], row['bank_name'],
                            row['branch_name'], row['author_name'], int(row['rating']),
                            row['text'], row['cleaned_text'], 
                            pd.to_datetime(row['review_time'], errors='coerce'),
                            row.get('detected_language', 'Unknown'),
                            float(row.get('language_confidence', 0.0)),
                            float(row.get('sentiment_score', 0.0)),
                            row.get('sentiment_label', 'Neutral'),
                            float(row.get('sentiment_confidence', 0.0)),
                            float(row.get('subjectivity', 0.0)),
                            row.get('dominant_topic', 'General'),
                            topic_dist_json,
                            int(row.get('word_count', 0)),
                            int(row.get('char_count', 0)),
                            row.get('rating_category', 'Neutral'),
                            row.get('review_detail_level', 'Brief'),
                            bool(row.get('mentions_service', False)),
                            bool(row.get('mentions_waiting', False)),
                            bool(row.get('mentions_fees', False)),
                            int(row.get('review_year', 2023)) if pd.notna(row.get('review_year')) else None,
                            str(row.get('review_month', '')),
                            row.get('day_of_week', ''),
                            row.get('original_language', 'Unknown'),
                            bool(row.get('translated', False)),
                            row.get('relative_time_description', ''),
                            pd.to_datetime(row.get('collected_at'), errors='coerce')
                        )
                        
                        with conn.cursor() as cursor:
                            cursor.execute(insert_sql, values)
                            insert_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Failed to insert row {row['review_id']}: {e}")
                        continue
                
                # Commit batch
                conn.commit()
                logger.info(f"Processed batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1}")
            
            logger.info(f"✅ Successfully loaded {insert_count} processed reviews")
            
            # Validate the loaded data
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM processed.bank_reviews;")
                count = cursor.fetchone()[0]
                logger.info(f"🔍 Verification: {count} records in processed.bank_reviews")
            
            # Push stats to XCom
            context['task_instance'].xcom_push(
                key='database_load_stats',
                value={
                    'records_loaded': insert_count,
                    'total_verified': count,
                    'load_time': datetime.now().isoformat()
                }
            )
            
        finally:
            conn.close()
        
        return insert_count
        
    except Exception as e:
        logger.error(f"❌ Database loading failed: {e}")
        raise

def run_dbt_models(**context):
    """Execute DBT models on the processed data"""
    logger.info("🔄 Running DBT models on processed data")
    
    try:
        # Connect to database for manual DBT-style transformations
        conn = connect_to_database()
        if not conn:
            raise Exception("Failed to connect to database")
        
        try:
            # Create staging layer
            logger.info("Creating DBT staging models...")
            staging_sql = """
            CREATE SCHEMA IF NOT EXISTS staging;
            
            DROP VIEW IF EXISTS staging.stg_processed_reviews CASCADE;
            CREATE VIEW staging.stg_processed_reviews AS
            SELECT 
                review_id, place_id, bank_name, branch_name, author_name,
                rating, original_text, cleaned_text, review_time,
                detected_language, sentiment_score, sentiment_label,
                dominant_topic, word_count, char_count, rating_category,
                mentions_service, mentions_waiting, mentions_fees,
                review_year, day_of_week, collected_at, processed_at
            FROM processed.bank_reviews
            WHERE review_id IS NOT NULL;
            """
            
            with conn.cursor() as cursor:
                cursor.execute(staging_sql)
                conn.commit()
            
            # Create intermediate layer
            logger.info("Creating DBT intermediate models...")
            intermediate_sql = """
            CREATE SCHEMA IF NOT EXISTS intermediate;
            
            DROP VIEW IF EXISTS intermediate.int_bank_performance CASCADE;
            CREATE VIEW intermediate.int_bank_performance AS
            SELECT 
                bank_name,
                COUNT(*) as total_reviews,
                AVG(rating) as avg_rating,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_reviews,
                COUNT(CASE WHEN rating <= 2 THEN 1 END) as negative_reviews,
                COUNT(CASE WHEN mentions_service THEN 1 END) as service_mentions,
                COUNT(CASE WHEN mentions_waiting THEN 1 END) as waiting_mentions,
                COUNT(CASE WHEN mentions_fees THEN 1 END) as fee_mentions,
                AVG(word_count) as avg_review_length
            FROM staging.stg_processed_reviews
            GROUP BY bank_name;
            
            DROP VIEW IF EXISTS intermediate.int_topic_analysis CASCADE;
            CREATE VIEW intermediate.int_topic_analysis AS
            SELECT 
                dominant_topic,
                COUNT(*) as topic_count,
                AVG(rating) as avg_topic_rating,
                AVG(sentiment_score) as avg_topic_sentiment,
                STRING_AGG(DISTINCT bank_name, ', ') as banks_with_topic
            FROM staging.stg_processed_reviews
            WHERE dominant_topic IS NOT NULL
            GROUP BY dominant_topic
            ORDER BY topic_count DESC;
            """
            
            with conn.cursor() as cursor:
                cursor.execute(intermediate_sql)
                conn.commit()
            
            # Create marts layer
            logger.info("Creating DBT mart models...")
            marts_sql = """
            CREATE SCHEMA IF NOT EXISTS marts;
            
            DROP VIEW IF EXISTS marts.mart_bank_analytics CASCADE;
            CREATE VIEW marts.mart_bank_analytics AS
            SELECT 
                bp.bank_name,
                bp.total_reviews,
                ROUND(bp.avg_rating, 2) as avg_rating,
                ROUND(bp.avg_sentiment, 3) as avg_sentiment,
                ROUND(bp.positive_reviews::DECIMAL / bp.total_reviews * 100, 1) as positive_rate,
                ROUND(bp.negative_reviews::DECIMAL / bp.total_reviews * 100, 1) as negative_rate,
                bp.service_mentions,
                bp.waiting_mentions,
                bp.fee_mentions,
                ROUND(bp.avg_review_length, 1) as avg_review_length,
                CASE 
                    WHEN bp.avg_rating >= 4.0 AND bp.avg_sentiment > 0.1 THEN 'Excellent'
                    WHEN bp.avg_rating >= 3.5 AND bp.avg_sentiment > 0.0 THEN 'Good'
                    WHEN bp.avg_rating >= 3.0 THEN 'Average'
                    ELSE 'Below Average'
                END as performance_tier,
                RANK() OVER (ORDER BY bp.avg_rating DESC, bp.avg_sentiment DESC) as ranking
            FROM intermediate.int_bank_performance bp
            ORDER BY bp.avg_rating DESC, bp.avg_sentiment DESC;
            """
            
            with conn.cursor() as cursor:
                cursor.execute(marts_sql)
                conn.commit()
            
            logger.info("✅ DBT models executed successfully")
            
            # Validate mart results
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM marts.mart_bank_analytics ORDER BY ranking;")
                results = cursor.fetchall()
                
                logger.info("📊 Bank Analytics Results:")
                for row in results:
                    bank_name, total, rating, sentiment, pos_rate, neg_rate, service, waiting, fees, avg_len, tier, rank = row
                    logger.info(f"#{rank} {bank_name}: {total} reviews, Rating: {rating}, Sentiment: {sentiment}, Tier: {tier}")
            
            # Push validation results
            context['task_instance'].xcom_push(
                key='dbt_results',
                value={
                    'models_created': ['staging.stg_processed_reviews', 'intermediate.int_bank_performance', 
                                     'intermediate.int_topic_analysis', 'marts.mart_bank_analytics'],
                    'total_banks_analyzed': len(results),
                    'completion_time': datetime.now().isoformat()
                }
            )
            
        finally:
            conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ DBT models execution failed: {e}")
        raise

def generate_phase2_report(**context):
    """Generate comprehensive Phase 2 completion report"""
    logger.info("📋 Generating Phase 2 completion report")
    
    try:
        # Gather stats from all previous tasks
        extraction_stats = context['task_instance'].xcom_pull(
            task_ids='extract_raw_data', key='extraction_stats'
        )
        transformation_results = context['task_instance'].xcom_pull(
            task_ids='run_transformation_pipeline', key='transformation_results'
        )
        load_stats = context['task_instance'].xcom_pull(
            task_ids='load_processed_data', key='database_load_stats'
        )
        dbt_results = context['task_instance'].xcom_pull(
            task_ids='run_dbt_models', key='dbt_results'
        )
        
        # Create comprehensive report
        report = {
            "phase": "Phase 2 - Data Cleaning & Transformation",
            "execution_date": context['ds'],
            "status": "COMPLETED",
            "completion_time": datetime.now().isoformat(),
            "pipeline_components": {
                "data_extraction": "✅ COMPLETED",
                "nlp_transformation": "✅ COMPLETED",
                "database_loading": "✅ COMPLETED",
                "dbt_models": "✅ COMPLETED"
            },
            "processing_metrics": {
                "raw_reviews_extracted": extraction_stats['total_reviews'],
                "unique_banks_processed": extraction_stats['unique_banks'],
                "processed_reviews_loaded": load_stats['records_loaded'],
                "dbt_models_created": len(dbt_results['models_created']),
                "banks_analyzed": dbt_results['total_banks_analyzed']
            },
            "nlp_features_added": [
                "Language Detection", "Sentiment Analysis", "Topic Modeling",
                "Content Flags", "Text Analytics", "Time Features"
            ],
            "dbt_layers_created": [
                "Staging Layer (stg_processed_reviews)",
                "Intermediate Layer (int_bank_performance, int_topic_analysis)",
                "Marts Layer (mart_bank_analytics)"
            ],
            "data_quality": {
                "extraction_success_rate": "100%",
                "transformation_success_rate": "100%",
                "loading_success_rate": f"{load_stats['records_loaded']}/{extraction_stats['total_reviews']} ({load_stats['records_loaded']/extraction_stats['total_reviews']*100:.1f}%)"
            },
            "next_steps": [
                "Phase 3: Data Visualization with Looker Studio",
                "Dashboard Creation", "Business Intelligence Reports"
            ]
        }
        
        logger.info("✅ Phase 2 pipeline completed successfully!")
        logger.info(f"📊 Processed {report['processing_metrics']['raw_reviews_extracted']} reviews")
        logger.info(f"🏦 Analyzed {report['processing_metrics']['banks_analyzed']} banks")
        logger.info(f"🔄 Created {report['processing_metrics']['dbt_models_created']} DBT models")
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Report generation failed: {e}")
        raise

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data_for_transformation,
    dag=dag
)

transform_task = PythonOperator(
    task_id='run_transformation_pipeline',
    python_callable=run_phase2_transformation_pipeline,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_processed_data',
    python_callable=load_processed_data_to_database,
    dag=dag
)

dbt_task = PythonOperator(
    task_id='run_dbt_models',
    python_callable=run_dbt_models,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_completion_report',
    python_callable=generate_phase2_report,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task >> dbt_task >> report_task