"""
Phase 2: Data Cleaning & Transformation Pipeline
Comprehensive script to process bank reviews data using DBT and NLP
"""

import os
import sys
import pandas as pd
import json
import subprocess
from pathlib import Path
from datetime import datetime
import logging

# Add src to path for imports
sys.path.append('src')
from nlp_processor_simple import SimpleBankReviewsNLPProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/phase2_transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Phase2TransformationPipeline:
    """
    Comprehensive Phase 2 transformation pipeline for bank reviews
    """
    
    def __init__(self, data_dir: str = "data", output_dir: str = "data/processed"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize NLP processor
        self.nlp_processor = SimpleBankReviewsNLPProcessor(n_topics=8)
        
        logger.info("Phase 2 Transformation Pipeline initialized")
    
    def load_raw_data(self) -> pd.DataFrame:
        """
        Load the raw bank reviews data
        """
        logger.info("Loading raw bank reviews data...")
        
        # Find the latest reviews file
        csv_files = list(self.data_dir.glob("bank_reviews_*.csv"))
        if not csv_files:
            raise FileNotFoundError("No bank reviews CSV files found in data directory")
        
        latest_file = max(csv_files, key=os.path.getctime)
        logger.info(f"Loading data from: {latest_file}")
        
        df = pd.read_csv(latest_file)
        logger.info(f"Loaded {len(df)} reviews")
        
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate the data
        """
        logger.info("Cleaning and validating data...")
        
        # Remove duplicates based on review_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=['review_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        logger.info(f"Removed {duplicates_removed} duplicate reviews")
        
        # Handle missing values
        df['text'] = df['text'].fillna('')
        df['rating'] = df['rating'].fillna(3)  # Default to neutral rating
        df['bank_name'] = df['bank_name'].fillna('Unknown')
        
        # Filter out reviews that are too short or too long
        min_length = 10
        max_length = 5000
        
        valid_reviews = (
            (df['text'].str.len() >= min_length) & 
            (df['text'].str.len() <= max_length) &
            (df['text'].str.strip() != '')
        )
        
        filtered_count = len(df) - valid_reviews.sum()
        df = df[valid_reviews].copy()
        logger.info(f"Filtered out {filtered_count} invalid reviews")
        
        # Normalize text - basic cleaning
        df['cleaned_text'] = df['text'].apply(self._normalize_text)
        
        logger.info(f"Data cleaning completed. Final dataset: {len(df)} reviews")
        return df
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text by removing extra spaces and standardizing format
        """
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def enrich_with_nlp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich data with NLP analysis (language detection, sentiment, topics)
        """
        logger.info("Starting NLP enrichment...")
        
        # Process with NLP pipeline
        enriched_df = self.nlp_processor.process_reviews_dataframe(
            df, text_column='cleaned_text'
        )
        
        # Get topic summary for analysis
        topic_summary = self.nlp_processor.get_topic_summary()
        
        # Save topic summary
        topic_summary_file = self.output_dir / 'topic_summary.json'
        with open(topic_summary_file, 'w', encoding='utf-8') as f:
            json.dump(topic_summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Topic summary saved to: {topic_summary_file}")
        
        return enriched_df
    
    def create_analysis_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create additional features for analysis
        """
        logger.info("Creating analysis features...")
        
        # Text-based features
        df['word_count'] = df['cleaned_text'].apply(lambda x: len(x.split()) if x else 0)
        df['char_count'] = df['cleaned_text'].apply(len)
        
        # Rating categories
        df['rating_category'] = df['rating'].apply(lambda x: 
            'Positive' if x >= 4 else 'Negative' if x <= 2 else 'Neutral'
        )
        
        # Time-based features
        df['review_time'] = pd.to_datetime(df['time'], unit='s', errors='coerce')
        df['review_month'] = df['review_time'].dt.to_period('M')
        df['review_year'] = df['review_time'].dt.year
        df['day_of_week'] = df['review_time'].dt.day_name()
        
        # Content analysis flags
        df['mentions_service'] = df['cleaned_text'].str.contains(
            'service|accueil|personnel', case=False, na=False
        )
        df['mentions_waiting'] = df['cleaned_text'].str.contains(
            'attente|queue|temps', case=False, na=False
        )
        df['mentions_fees'] = df['cleaned_text'].str.contains(
            'frais|cout|prix|cher', case=False, na=False
        )
        
        # Review quality metrics
        df['review_detail_level'] = df['word_count'].apply(lambda x:
            'Detailed' if x >= 50 else 'Moderate' if x >= 20 else 'Brief'
        )
        
        logger.info("Analysis features created")
        return df
    
    def save_processed_data(self, df: pd.DataFrame) -> str:
        """
        Save the processed data to files
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as CSV
        csv_file = self.output_dir / f'processed_bank_reviews_{timestamp}.csv'
        df.to_csv(csv_file, index=False)
        logger.info(f"Processed data saved to: {csv_file}")
        
        # Save as JSON for more complex data types
        json_file = self.output_dir / f'processed_bank_reviews_{timestamp}.json'
        df.to_json(json_file, orient='records', indent=2, force_ascii=False)
        logger.info(f"Processed data saved to: {json_file}")
        
        # Save summary statistics
        self._save_summary_stats(df, timestamp)
        
        return str(csv_file)
    
    def _save_summary_stats(self, df: pd.DataFrame, timestamp: str):
        """
        Save summary statistics about the processed data
        """
        stats = {
            'processing_timestamp': timestamp,
            'total_reviews': len(df),
            'unique_banks': df['bank_name'].nunique(),
            'unique_branches': df['branch_name'].nunique(),
            'date_range': {
                'start': str(df['review_time'].min()),
                'end': str(df['review_time'].max())
            },
            'language_distribution': df['detected_language'].value_counts().to_dict(),
            'sentiment_distribution': df['sentiment_label'].value_counts().to_dict(),
            'rating_distribution': df['rating'].value_counts().to_dict(),
            'topic_distribution': df['dominant_topic'].value_counts().to_dict(),
            'average_review_length': float(df['word_count'].mean()),
            'median_rating': float(df['rating'].median())
        }
        
        stats_file = self.output_dir / f'processing_summary_{timestamp}.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Summary statistics saved to: {stats_file}")
    
    def setup_dbt_data_source(self, processed_file: str):
        """
        Setup DBT data source (would typically load to database)
        """
        logger.info("Setting up DBT data source...")
        
        # In a production environment, this would load data to PostgreSQL
        # For now, we'll create a seed file for DBT
        
        seed_dir = Path("bank_reviews_dbt/seeds")
        seed_dir.mkdir(exist_ok=True)
        
        # Copy processed file to seeds directory
        import shutil
        seed_file = seed_dir / "bank_reviews_processed.csv"
        shutil.copy(processed_file, seed_file)
        
        logger.info(f"DBT seed file created: {seed_file}")
    
    def run_complete_pipeline(self) -> str:
        """
        Run the complete Phase 2 transformation pipeline
        """
        logger.info("=" * 60)
        logger.info("STARTING PHASE 2: DATA CLEANING & TRANSFORMATION")
        logger.info("=" * 60)
        
        try:
            # Step 1: Load raw data
            raw_df = self.load_raw_data()
            
            # Step 2: Clean data
            cleaned_df = self.clean_data(raw_df)
            
            # Step 3: NLP enrichment
            enriched_df = self.enrich_with_nlp(cleaned_df)
            
            # Step 4: Create analysis features
            final_df = self.create_analysis_features(enriched_df)
            
            # Step 5: Save processed data
            processed_file = self.save_processed_data(final_df)
            
            # Step 6: Setup DBT data source
            self.setup_dbt_data_source(processed_file)
            
            logger.info("=" * 60)
            logger.info("PHASE 2 TRANSFORMATION COMPLETED SUCCESSFULLY!")
            logger.info(f"Final dataset: {len(final_df)} reviews")
            logger.info(f"Processed file: {processed_file}")
            logger.info("=" * 60)
            
            return processed_file
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """
    Main execution function
    """
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize and run pipeline
    pipeline = Phase2TransformationPipeline()
    
    try:
        processed_file = pipeline.run_complete_pipeline()
        
        print("\n" + "="*60)
        print("✅ PHASE 2 COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"📊 Processed data saved to: {processed_file}")
        print("📈 Summary statistics and topic analysis generated")
        print("🔧 DBT models ready for execution")
        print("\nNext steps:")
        print("1. Review the processed data and summary statistics")
        print("2. Run DBT models: cd bank_reviews_dbt && dbt run")
        print("3. Execute DBT tests: dbt test")
        print("4. Proceed to Phase 3: Data Visualization")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 