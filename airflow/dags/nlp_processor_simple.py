"""
Simplified NLP Processor for Bank Reviews
Handles sentiment analysis and basic topic modeling using available libraries
"""

import pandas as pd
import numpy as np
import re
import json
from typing import Dict, List, Tuple, Any
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleBankReviewsNLPProcessor:
    """
    Simplified NLP processor for bank reviews data
    """
    
    def __init__(self, n_topics: int = 8, random_state: int = 42):
        self.n_topics = n_topics
        self.random_state = random_state
        self.lda_model = None
        self.vectorizer = None
        self.feature_names = None
        self.topic_labels = {
            0: "Service Quality",
            1: "Wait Times", 
            2: "Staff Behavior",
            3: "Digital Services",
            4: "Account Management",
            5: "Fees and Charges",
            6: "Branch Facilities",
            7: "Customer Support"
        }
        
    def detect_language_simple(self, text: str) -> Tuple[str, float]:
        """
        Simple language detection based on common French and Arabic words
        """
        try:
            if not text or len(text.strip()) < 3:
                return 'unknown', 0.0
            
            text_lower = text.lower()
            
            # Common French words
            french_indicators = ['le', 'la', 'les', 'de', 'du', 'des', 'un', 'une', 'et', 'est', 'avec', 'pour', 'dans', 'sur', 'très', 'bien', 'mal', 'service', 'banque']
            
            # Common Arabic words (transliterated)
            arabic_indicators = ['في', 'من', 'إلى', 'على', 'هذا', 'هذه', 'التي', 'الذي']
            
            # Count indicators
            french_count = sum(1 for word in french_indicators if word in text_lower)
            arabic_count = sum(1 for indicator in arabic_indicators if indicator in text)
            
            total_words = len(text_lower.split())
            
            if french_count > 0:
                confidence = min(french_count / max(total_words, 1), 1.0)
                return 'fr', confidence
            elif arabic_count > 0:
                confidence = min(arabic_count / max(total_words, 1), 1.0)
                return 'ar', confidence
            else:
                return 'unknown', 0.3
                
        except Exception as e:
            logger.warning(f"Language detection failed: {e}")
            return 'unknown', 0.0
    
    def analyze_sentiment(self, text: str, language: str = 'fr') -> Dict[str, Any]:
        """
        Analyze sentiment using TextBlob
        """
        try:
            if not text or len(text.strip()) < 3:
                return {
                    'sentiment_score': 0.0,
                    'sentiment_label': 'Neutral',
                    'confidence': 0.0
                }
            
            # Use TextBlob for sentiment analysis
            blob = TextBlob(text)
            
            # Get polarity (-1 to 1) and subjectivity (0 to 1)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Convert to sentiment label
            if polarity > 0.1:
                sentiment_label = 'Positive'
            elif polarity < -0.1:
                sentiment_label = 'Negative'
            else:
                sentiment_label = 'Neutral'
            
            # Confidence is based on subjectivity and absolute polarity
            confidence = min(subjectivity + abs(polarity), 1.0)
            
            return {
                'sentiment_score': polarity,
                'sentiment_label': sentiment_label,
                'confidence': confidence,
                'subjectivity': subjectivity
            }
            
        except Exception as e:
            logger.warning(f"Sentiment analysis failed: {e}")
            return {
                'sentiment_score': 0.0,
                'sentiment_label': 'Neutral',
                'confidence': 0.0,
                'subjectivity': 0.0
            }
    
    def preprocess_for_lda(self, texts: List[str]) -> List[str]:
        """
        Preprocess texts for LDA topic modeling
        """
        processed_texts = []
        
        # French stop words (basic set)
        french_stopwords = {
            'le', 'de', 'un', 'à', 'être', 'et', 'en', 'avoir', 'que', 'pour',
            'dans', 'ce', 'il', 'une', 'sur', 'avec', 'ne', 'se', 'pas', 'tout',
            'plus', 'par', 'grand', 'me', 'même', 'te', 'si', 'la', 'du', 'au',
            'des', 'les', 'cette', 'ses', 'nos', 'vos', 'leurs', 'mon', 'ton',
            'son', 'ma', 'ta', 'sa', 'mes', 'tes', 'très', 'bien', 'très',
            'est', 'sont', 'était', 'ont', 'avait', 'fait', 'peut', 'va', 'je',
            'tu', 'nous', 'vous', 'ils', 'elles', 'qui', 'quoi', 'dont', 'où'
        }
        
        for text in texts:
            if not text:
                processed_texts.append("")
                continue
                
            # Convert to lowercase and remove special characters
            text = re.sub(r'[^a-zA-ZàáâãäçèéêëìíîïñòóôõöùúûüÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜŸ\s]', ' ', text.lower())
            
            # Remove extra spaces
            text = ' '.join(text.split())
            
            # Remove stopwords
            words = text.split()
            words = [word for word in words if len(word) > 2 and word not in french_stopwords]
            
            processed_texts.append(' '.join(words))
        
        return processed_texts
    
    def train_lda_model(self, texts: List[str]) -> None:
        """
        Train LDA model for topic modeling using sklearn
        """
        logger.info("Training LDA model for topic modeling...")
        
        # Preprocess texts
        processed_texts = self.preprocess_for_lda(texts)
        
        # Filter out empty documents
        processed_texts = [text for text in processed_texts if text.strip()]
        
        if len(processed_texts) < 10:
            logger.warning("Not enough documents for topic modeling")
            return
        
        # Create TF-IDF vectorizer
        self.vectorizer = TfidfVectorizer(
            max_features=100,
            min_df=2,
            max_df=0.8,
            ngram_range=(1, 2)
        )
        
        # Fit and transform texts
        doc_term_matrix = self.vectorizer.fit_transform(processed_texts)
        self.feature_names = self.vectorizer.get_feature_names_out()
        
        # Train LDA model
        self.lda_model = LatentDirichletAllocation(
            n_components=self.n_topics,
            random_state=self.random_state,
            max_iter=10,
            learning_method='online'
        )
        
        self.lda_model.fit(doc_term_matrix)
        
        logger.info(f"LDA model trained with {self.n_topics} topics")
    
    def get_document_topics(self, text: str) -> Dict[str, Any]:
        """
        Get topic distribution for a document
        """
        if not self.lda_model or not self.vectorizer:
            return {
                'dominant_topic': 'Unknown',
                'topic_distribution': {}
            }
        
        try:
            # Preprocess text
            processed = self.preprocess_for_lda([text])[0]
            
            if not processed.strip():
                return {
                    'dominant_topic': 'Unknown', 
                    'topic_distribution': {}
                }
            
            # Transform to document-term matrix
            doc_term_matrix = self.vectorizer.transform([processed])
            
            # Get topic distribution
            topic_dist = self.lda_model.transform(doc_term_matrix)[0]
            
            # Convert to dictionary
            topic_dict = {f"topic_{i}": float(prob) for i, prob in enumerate(topic_dist)}
            
            # Find dominant topic
            dominant_topic_id = np.argmax(topic_dist)
            dominant_topic = self.topic_labels.get(dominant_topic_id, f"Topic_{dominant_topic_id}")
            
            return {
                'dominant_topic': dominant_topic,
                'topic_distribution': topic_dict
            }
            
        except Exception as e:
            logger.warning(f"Topic modeling failed: {e}")
            return {
                'dominant_topic': 'Unknown',
                'topic_distribution': {}
            }
    
    def process_reviews_dataframe(self, df: pd.DataFrame, text_column: str = 'cleaned_text') -> pd.DataFrame:
        """
        Process entire dataframe of reviews
        """
        logger.info(f"Processing {len(df)} reviews...")
        
        # Make a copy to avoid modifying original
        result_df = df.copy()
        
        # Language detection
        logger.info("Detecting languages...")
        lang_results = df[text_column].apply(self.detect_language_simple)
        result_df['detected_language'] = lang_results.apply(lambda x: x[0])
        result_df['language_confidence'] = lang_results.apply(lambda x: x[1])
        
        # Sentiment analysis
        logger.info("Analyzing sentiment...")
        sentiment_results = df.apply(
            lambda row: self.analyze_sentiment(row[text_column], row.get('language', 'fr')), 
            axis=1
        )
        
        result_df['sentiment_score'] = sentiment_results.apply(lambda x: x['sentiment_score'])
        result_df['sentiment_label'] = sentiment_results.apply(lambda x: x['sentiment_label'])
        result_df['sentiment_confidence'] = sentiment_results.apply(lambda x: x['confidence'])
        result_df['subjectivity'] = sentiment_results.apply(lambda x: x.get('subjectivity', 0.0))
        
        # Topic modeling
        logger.info("Training topic model...")
        texts = df[text_column].fillna('').tolist()
        self.train_lda_model(texts)
        
        if self.lda_model:
            logger.info("Extracting topics...")
            topic_results = df[text_column].apply(self.get_document_topics)
            result_df['dominant_topic'] = topic_results.apply(lambda x: x['dominant_topic'])
            result_df['topic_distribution'] = topic_results.apply(lambda x: json.dumps(x['topic_distribution']))
        else:
            result_df['dominant_topic'] = 'Unknown'
            result_df['topic_distribution'] = '{}'
        
        logger.info("NLP processing completed!")
        return result_df
    
    def get_topic_summary(self) -> Dict[str, Any]:
        """
        Get summary of discovered topics
        """
        if not self.lda_model or self.feature_names is None:
            return {}
        
        topics = {}
        for topic_id in range(self.n_topics):
            # Get top words for this topic
            top_words_idx = self.lda_model.components_[topic_id].argsort()[-10:][::-1]
            top_words = [self.feature_names[i] for i in top_words_idx]
            word_weights = [float(self.lda_model.components_[topic_id][i]) for i in top_words_idx]
            
            topics[f"topic_{topic_id}"] = {
                'label': self.topic_labels.get(topic_id, f"Topic_{topic_id}"),
                'words': top_words,
                'word_weights': dict(zip(top_words, word_weights))
            }
        
        return topics


def main():
    """
    Example usage and testing
    """
    # Initialize processor
    processor = SimpleBankReviewsNLPProcessor(n_topics=5)
    
    # Sample reviews for testing
    sample_reviews = [
        "Le service client est excellent et le personnel très accueillant",
        "Temps d'attente très long et service décevant",
        "Personnel non professionnel et aucune considération pour les clients",
        "Très bonne expérience, service rapide et efficace",
        "L'application mobile ne fonctionne pas bien",
        "Les frais bancaires sont trop élevés et injustifiés",
        "Agence propre et bien organisée, bon accueil",
        "Impossible de joindre quelqu'un au téléphone"
    ]
    
    # Test individual functions
    print("=== TESTING NLP PROCESSOR ===\n")
    
    for i, review in enumerate(sample_reviews[:3]):
        print(f"Review {i+1}: {review}")
        
        # Language detection
        lang, conf = processor.detect_language_simple(review)
        print(f"Language: {lang} (confidence: {conf:.2f})")
        
        # Sentiment analysis
        sentiment = processor.analyze_sentiment(review)
        print(f"Sentiment: {sentiment['sentiment_label']} (score: {sentiment['sentiment_score']:.2f})")
        print()
    
    # Test topic modeling
    print("=== TOPIC MODELING ===")
    processor.train_lda_model(sample_reviews)
    
    if processor.lda_model:
        topics_summary = processor.get_topic_summary()
        print("Topics discovered:")
        for topic_id, topic_info in topics_summary.items():
            print(f"{topic_info['label']}: {', '.join(topic_info['words'][:5])}")
        
        print("\nSample topic assignments:")
        for review in sample_reviews[:3]:
            topics = processor.get_document_topics(review)
            print(f"'{review[:50]}...' -> {topics['dominant_topic']}")
    
    print("\n✅ NLP Processor test completed successfully!")


if __name__ == "__main__":
    main() 