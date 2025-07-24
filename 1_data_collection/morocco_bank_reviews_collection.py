"""
Apache Airflow DAG for collecting Morocco bank reviews.
This DAG automates the collection process on a scheduled basis.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import sys
import os

# Add project root to path
sys.path.append('/opt/airflow/dags/src')  # Adjust path as needed
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'catchup': False
}

# DAG definition
dag = DAG(
    'morocco_bank_reviews_collection',
    default_args=default_args,
    description='Collect bank reviews from Google Maps for Morocco banks',
    schedule_interval='@weekly',  # Run weekly
    max_active_runs=1,
    tags=['data-collection', 'google-maps', 'morocco', 'banking']
)

def check_api_connection():
    """Check if Google Maps API is accessible."""
    try:
        from collectors.google_maps_collector import GoogleMapsCollector
        collector = GoogleMapsCollector()
        # Test API with a simple query
        test_result = collector.client.places(query="bank Casablanca Morocco", language='fr')
        if test_result.get('results'):
            print("✅ Google Maps API connection successful")
            return True
        else:
            raise Exception("No results from API test query")
    except Exception as e:
        print(f"❌ Google Maps API connection failed: {e}")
        raise

def check_database_connection():
    """Check if PostgreSQL database is accessible."""
    try:
        from utils.database import DatabaseManager
        db_manager = DatabaseManager()
        # Test database connection
        session = db_manager.get_session()
        session.execute("SELECT 1")
        session.close()
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

def collect_bank_data(**context):
    """Main task to collect bank review data."""
    from collectors.google_maps_collector import GoogleMapsCollector
    from utils.database import DatabaseManager
    from dataclasses import asdict
    
    # Get execution date for logging
    execution_date = context['execution_date']
    print(f"🚀 Starting collection for execution date: {execution_date}")
    
    # Initialize collector
    collector = GoogleMapsCollector()
    
    # Get list of banks to collect (can be configured via Airflow Variables)
    banks_to_collect = Variable.get("morocco_banks", default_var=None)
    if banks_to_collect:
        banks_list = [bank.strip() for bank in banks_to_collect.split(',')]
    else:
        banks_list = None  # Will use all banks from settings
    
    print(f"📊 Collecting data for banks: {banks_list or 'all Morocco banks'}")
    
    # Collect data
    locations, reviews = collector.collect_all_bank_reviews(banks=banks_list)
    
    if not locations and not reviews:
        print("⚠️ No data collected!")
        return {"locations": 0, "reviews": 0}
    
    print(f"📈 Collection completed: {len(locations)} locations, {len(reviews)} reviews")
    
    # Store in database
    db_manager = DatabaseManager()
    
    # Convert to dictionaries
    locations_data = [asdict(location) for location in locations]
    reviews_data = [asdict(review) for review in reviews]
    
    # Insert data
    locations_inserted = db_manager.insert_locations(locations_data)
    reviews_inserted = db_manager.insert_reviews(reviews_data)
    
    print(f"💾 Database storage: {locations_inserted} locations, {reviews_inserted} new reviews")
    
    # Store metrics for monitoring
    context['task_instance'].xcom_push(key='locations_count', value=len(locations))
    context['task_instance'].xcom_push(key='reviews_count', value=len(reviews))
    context['task_instance'].xcom_push(key='new_reviews', value=reviews_inserted)
    
    return {
        "total_locations": len(locations),
        "total_reviews": len(reviews),
        "new_reviews": reviews_inserted,
        "execution_date": str(execution_date)
    }

def generate_collection_report(**context):
    """Generate a summary report of the collection process."""
    from utils.database import DatabaseManager
    
    # Get metrics from previous task
    locations_count = context['task_instance'].xcom_pull(key='locations_count', task_ids='collect_reviews')
    reviews_count = context['task_instance'].xcom_pull(key='reviews_count', task_ids='collect_reviews')
    new_reviews = context['task_instance'].xcom_pull(key='new_reviews', task_ids='collect_reviews')
    
    # Get database statistics
    db_manager = DatabaseManager()
    stats = db_manager.get_bank_statistics()
    
    # Generate report
    report = f"""
    📊 MOROCCO BANK REVIEWS COLLECTION REPORT
    ==========================================
    
    📅 Collection Date: {context['execution_date']}
    
    🆕 THIS RUN:
    - Locations found: {locations_count}
    - Reviews collected: {reviews_count}
    - New reviews added: {new_reviews}
    
    📈 DATABASE TOTALS:
    - Total locations: {stats.get('total_locations', 0)}
    - Total reviews: {stats.get('total_reviews', 0)}
    - Processed reviews: {stats.get('processed_reviews', 0)}
    
    🏦 REVIEWS BY BANK:
    """
    
    for bank, count in stats.get('reviews_by_bank', {}).items():
        avg_rating = stats.get('avg_rating_by_bank', {}).get(bank, 0)
        report += f"   • {bank}: {count} reviews (avg rating: {avg_rating:.2f})\n"
    
    print(report)
    
    # Store report in XCom for potential email notifications
    context['task_instance'].xcom_push(key='collection_report', value=report)
    
    return report

def cleanup_old_logs():
    """Clean up old log files to save disk space."""
    import glob
    import os
    from datetime import datetime, timedelta
    
    # Remove log files older than 30 days
    cutoff_date = datetime.now() - timedelta(days=30)
    log_pattern = "/opt/airflow/logs/collection_*.log"  # Adjust path as needed
    
    removed_count = 0
    for log_file in glob.glob(log_pattern):
        file_time = datetime.fromtimestamp(os.path.getctime(log_file))
        if file_time < cutoff_date:
            os.remove(log_file)
            removed_count += 1
    
    print(f"🧹 Cleaned up {removed_count} old log files")
    return removed_count

# Task definitions
check_api_task = PythonOperator(
    task_id='check_api_connection',
    python_callable=check_api_connection,
    dag=dag,
    doc_md="""
    **Check API Connection**
    
    Verifies that the Google Maps API is accessible and responding correctly.
    This task must succeed before proceeding with data collection.
    """
)

check_db_task = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag,
    doc_md="""
    **Check Database Connection**
    
    Verifies that the PostgreSQL database is accessible and ready for data storage.
    """
)

collect_reviews_task = PythonOperator(
    task_id='collect_reviews',
    python_callable=collect_bank_data,
    dag=dag,
    execution_timeout=timedelta(hours=4),  # Max 4 hours for collection
    doc_md="""
    **Collect Bank Reviews**
    
    Main task that collects bank reviews from Google Maps API for all major banks in Morocco.
    Data is automatically stored in the PostgreSQL database.
    """
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_collection_report,
    dag=dag,
    doc_md="""
    **Generate Collection Report**
    
    Creates a summary report of the collection process including statistics and metrics.
    """
)

cleanup_task = PythonOperator(
    task_id='cleanup_logs',
    python_callable=cleanup_old_logs,
    dag=dag,
    doc_md="""
    **Cleanup Old Logs**
    
    Removes log files older than 30 days to manage disk space.
    """
)

# Create a task to backup data (optional)
backup_task = BashOperator(
    task_id='backup_data',
    bash_command="""
    # Create backup directory
    mkdir -p /opt/airflow/backups/{{ ds }}
    
    # Export database tables to CSV
    psql $DB_URL -c "\\copy bank_locations to '/opt/airflow/backups/{{ ds }}/bank_locations.csv' CSV HEADER"
    psql $DB_URL -c "\\copy reviews to '/opt/airflow/backups/{{ ds }}/reviews.csv' CSV HEADER"
    
    echo "✅ Database backup completed for {{ ds }}"
    """,
    dag=dag,
    doc_md="""
    **Backup Data**
    
    Creates a backup of the collected data in CSV format.
    Backups are stored with the execution date for easy retrieval.
    """
)

# Task dependencies
check_api_task >> check_db_task >> collect_reviews_task >> generate_report_task
collect_reviews_task >> backup_task
generate_report_task >> cleanup_task

# DAG documentation
dag.doc_md = """
# Morocco Bank Reviews Collection DAG

This DAG automates the collection of customer reviews for major banks in Morocco using the Google Maps API.

## Features
- **Automated scheduling**: Runs weekly to collect fresh review data
- **Data validation**: Checks API and database connections before collection
- **Comprehensive logging**: Detailed logs for monitoring and debugging
- **Data backup**: Automatic backup of collected data
- **Reporting**: Generates summary reports after each collection run
- **Error handling**: Retries failed tasks and sends notifications

## Banks Covered
- Attijariwafa Bank
- Banque Populaire
- BMCE Bank
- Crédit Agricole du Maroc
- BMCI
- Société Générale Maroc
- And more...

## Configuration
Set the following Airflow Variables:
- `morocco_banks`: Comma-separated list of specific banks to collect (optional)

## Monitoring
Check the task logs and XCom values for detailed collection metrics and reports.
""" 