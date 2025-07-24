"""
Morocco Bank Reviews - Automated Collection DAG
Airflow DAG for collecting reviews from all major Moroccan banks daily.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
import os
import sys
import json
import logging

# Add project root to Python path
sys.path.insert(0, '/opt/airflow/dags')

# Default arguments for the DAG
default_args = {
    'owner': 'morocco-bank-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'morocco_banks_collection',
    default_args=default_args,
    description='Automated collection of Morocco bank reviews',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['morocco', 'banks', 'data-collection', 'google-maps']
)

# Moroccan Banks to collect
MOROCCAN_BANKS = [
    "Attijariwafa Bank",
    "Banque Populaire", 
    "BMCE Bank",
    "Crédit Agricole du Maroc",
    "BMCI",
    "Société Générale Maroc",
    "CIH Bank",
    "CDM",
    "Al Barid Bank"
]

# Major cities for collection
MOROCCAN_CITIES = [
    "Casablanca", "Rabat", "Fès", "Marrakech", "Agadir", "Tangier", 
    "Meknès", "Oujda", "Kenitra", "Tetouan", "Safi", "Mohammedia"
]

def collect_bank_data(**context):
    """Task to collect data for all banks."""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    # Import the collector (assuming it's in the dags directory)
    from collect_all_banks import MoroccoBankCollector, save_results
    
    # Get API key from environment
    api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
    if not api_key:
        raise ValueError("GOOGLE_MAPS_API_KEY not found in environment!")
    
    # Initialize collector
    collector = MoroccoBankCollector(api_key)
    
    # Collect data for all banks
    logging.info(f"Starting collection for {len(MOROCCAN_BANKS)} banks in {len(MOROCCAN_CITIES)} cities")
    
    all_data = collector.collect_all_banks(MOROCCAN_BANKS, MOROCCAN_CITIES)
    
    # Save results with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/opt/airflow/data/morocco_banks_{timestamp}.json"
    
    # Ensure data directory exists
    os.makedirs("/opt/airflow/data", exist_ok=True)
    
    # Save the data
    data_file, summary_file = save_results(all_data, filename)
    
    # Store file paths in XCom for next tasks
    context['task_instance'].xcom_push(key='data_file', value=data_file)
    context['task_instance'].xcom_push(key='summary_file', value=summary_file)
    
    # Log summary
    total_locations = sum(len(bank_data) for bank_data in all_data.values())
    total_reviews = sum(
        len(location.get('reviews', [])) 
        for bank_data in all_data.values() 
        for location in bank_data
    )
    
    logging.info(f"Collection completed: {total_locations} locations, {total_reviews} reviews")
    
    return {
        'total_locations': total_locations,
        'total_reviews': total_reviews,
        'banks_processed': len(all_data),
        'data_file': data_file,
        'summary_file': summary_file
    }

def validate_collection(**context):
    """Validate the collected data."""
    # Get file paths from previous task
    data_file = context['task_instance'].xcom_pull(key='data_file', task_ids='collect_banks')
    
    if not data_file or not os.path.exists(data_file):
        raise ValueError(f"Data file not found: {data_file}")
    
    # Load and validate data
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Validation checks
    if not data:
        raise ValueError("No data collected!")
    
    total_locations = sum(len(bank_data) for bank_data in data.values())
    if total_locations < 50:  # Minimum expected locations
        logging.warning(f"Low location count: {total_locations}")
    
    # Check for each bank
    for bank in MOROCCAN_BANKS:
        if bank not in data:
            logging.warning(f"No data collected for {bank}")
        elif len(data[bank]) == 0:
            logging.warning(f"Empty data for {bank}")
    
    logging.info(f"Validation passed: {total_locations} locations for {len(data)} banks")
    return True

def generate_daily_report(**context):
    """Generate daily collection report."""
    # Get data from previous tasks
    collection_result = context['task_instance'].xcom_pull(task_ids='collect_banks')
    data_file = context['task_instance'].xcom_pull(key='data_file', task_ids='collect_banks')
    
    # Load data for detailed report
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Generate report
    report = {
        'date': datetime.now().isoformat(),
        'summary': collection_result,
        'bank_breakdown': {}
    }
    
    for bank, locations in data.items():
        bank_reviews = sum(len(loc.get('reviews', [])) for loc in locations)
        bank_avg_rating = sum(
            loc.get('rating', 0) for loc in locations if loc.get('rating')
        ) / len([loc for loc in locations if loc.get('rating')]) if locations else 0
        
        report['bank_breakdown'][bank] = {
            'locations': len(locations),
            'reviews': bank_reviews,
            'avg_rating': round(bank_avg_rating, 2),
            'cities_covered': len(set(loc.get('city') for loc in locations))
        }
    
    # Save report
    report_file = data_file.replace('.json', '_report.json')
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Daily report generated: {report_file}")
    return report

def cleanup_old_files(**context):
    """Clean up old data files (keep last 7 days)."""
    import glob
    from datetime import datetime, timedelta
    
    data_dir = "/opt/airflow/data"
    cutoff_date = datetime.now() - timedelta(days=7)
    
    # Find old files
    pattern = os.path.join(data_dir, "morocco_banks_*.json")
    old_files = []
    
    for file_path in glob.glob(pattern):
        try:
            # Extract timestamp from filename
            filename = os.path.basename(file_path)
            timestamp_str = filename.split('_')[2].split('.')[0]  # Extract YYYYMMDD_HHMMSS
            file_date = datetime.strptime(timestamp_str, "%Y%m%d")
            
            if file_date < cutoff_date:
                old_files.append(file_path)
        except (ValueError, IndexError):
            continue
    
    # Remove old files
    for old_file in old_files:
        try:
            os.remove(old_file)
            logging.info(f"Removed old file: {old_file}")
        except Exception as e:
            logging.warning(f"Could not remove {old_file}: {e}")
    
    return f"Cleaned up {len(old_files)} old files"

# Define tasks
collect_banks_task = PythonOperator(
    task_id='collect_banks',
    python_callable=collect_bank_data,
    dag=dag,
    pool='google_api_pool',  # Limit concurrent API calls
    execution_timeout=timedelta(hours=4)  # Max 4 hours for full collection
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_collection,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_daily_report,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag
)

# Health check task
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "Morocco Bank Collection DAG completed successfully at $(date)"',
    dag=dag
)

# Set task dependencies
collect_banks_task >> validate_data_task >> generate_report_task >> cleanup_task >> health_check_task

# Optional: Add email notification on success
success_email = EmailOperator(
    task_id='send_success_email',
    to=['admin@example.com'],  # Replace with actual email
    subject='Morocco Bank Collection - Daily Success',
    html_content="""
    <h3>Morocco Bank Data Collection Completed Successfully</h3>
    <p>The daily collection of Morocco bank reviews has completed successfully.</p>
    <p><strong>Collection Date:</strong> {{ ds }}</p>
    <p><strong>Total Banks:</strong> 9</p>
    <p><strong>Total Cities:</strong> 12</p>
    <p>Check the data directory for the latest results.</p>
    """,
    dag=dag,
    trigger_rule='all_success'
)

# Add email to workflow (optional)
health_check_task >> success_email 