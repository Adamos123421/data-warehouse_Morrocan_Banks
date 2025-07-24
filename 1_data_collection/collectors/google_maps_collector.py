"""
Google Maps API collector for bank reviews in Morocco.
"""
import googlemaps
import time
import json
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from loguru import logger
from dataclasses import dataclass, asdict
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from config.settings import settings, MOROCCAN_CITIES, BANK_SEARCH_QUERIES

@dataclass
class BankLocation:
    """Data class for bank location information."""
    place_id: str
    name: str
    bank_name: str
    address: str
    city: str
    latitude: float
    longitude: float
    rating: Optional[float]
    user_ratings_total: Optional[int]
    phone_number: Optional[str]
    website: Optional[str]
    opening_hours: Optional[List[str]]

@dataclass
class Review:
    """Data class for review information."""
    place_id: str
    bank_name: str
    branch_name: str
    author_name: str
    author_url: Optional[str]
    language: str
    original_language: Optional[str]
    profile_photo_url: Optional[str]
    rating: int
    relative_time_description: str
    text: str
    time: int
    translated: bool
    review_id: str
    collected_at: datetime

class GoogleMapsCollector:
    """Collector for bank reviews using Google Maps API."""
    
    def __init__(self):
        """Initialize the Google Maps client."""
        self.client = googlemaps.Client(key=settings.google_maps_api_key)
        logger.info("Google Maps API client initialized")
    
    def search_bank_locations(self, bank_name: str, city: str) -> List[BankLocation]:
        """
        Search for bank locations in a specific city.
        
        Args:
            bank_name: Name of the bank
            city: City to search in
            
        Returns:
            List of BankLocation objects
        """
        locations = []
        
        for query_template in BANK_SEARCH_QUERIES:
            query = query_template.format(bank=bank_name, city=city)
            logger.info(f"Searching for: {query}")
            
            try:
                # Search for places
                places_result = self.client.places(
                    query=query,
                    location=self._get_city_coordinates(city),
                    radius=50000,  # 50km radius
                    language='fr'  # French for Morocco
                )
                
                for place in places_result.get('results', []):
                    location = self._extract_location_info(place, bank_name, city)
                    if location and location.place_id not in [loc.place_id for loc in locations]:
                        locations.append(location)
                
                # Respect API rate limits
                time.sleep(settings.collection_delay_seconds)
                
            except Exception as e:
                logger.error(f"Error searching for {query}: {e}")
                continue
        
        logger.info(f"Found {len(locations)} locations for {bank_name} in {city}")
        return locations
    
    def get_place_reviews(self, place_id: str, bank_name: str, branch_name: str) -> List[Review]:
        """
        Get reviews for a specific place.
        
        Args:
            place_id: Google Places ID
            bank_name: Name of the bank
            branch_name: Name of the branch
            
        Returns:
            List of Review objects
        """
        reviews = []
        
        try:
            # Get place details with reviews
            place_details = self.client.place(
                place_id=place_id,
                fields=['reviews'],
                language='fr'
            )
            
            place_reviews = place_details.get('result', {}).get('reviews', [])
            
            for review_data in place_reviews:
                review = self._extract_review_info(
                    review_data, place_id, bank_name, branch_name
                )
                if review:
                    reviews.append(review)
            
            logger.info(f"Collected {len(reviews)} reviews for {branch_name}")
            
        except Exception as e:
            logger.error(f"Error getting reviews for {place_id}: {e}")
        
        return reviews
    
    def collect_all_bank_reviews(self, banks: Optional[List[str]] = None) -> Tuple[List[BankLocation], List[Review]]:
        """
        Collect all reviews for specified banks across Morocco.
        
        Args:
            banks: List of bank names (defaults to all Morocco banks)
            
        Returns:
            Tuple of (locations, reviews)
        """
        if banks is None:
            banks = settings.morocco_banks
        
        all_locations = []
        all_reviews = []
        
        logger.info(f"Starting collection for {len(banks)} banks across {len(MOROCCAN_CITIES)} cities")
        
        for bank in banks:
            logger.info(f"Collecting data for {bank}")
            
            for city in MOROCCAN_CITIES:
                logger.info(f"Searching {bank} in {city}")
                
                # Get bank locations
                locations = self.search_bank_locations(bank, city)
                all_locations.extend(locations)
                
                # Get reviews for each location
                for location in locations:
                    reviews = self.get_place_reviews(
                        location.place_id, 
                        location.bank_name, 
                        location.name
                    )
                    all_reviews.extend(reviews)
                    
                    # Respect API rate limits
                    time.sleep(settings.collection_delay_seconds)
        
        logger.info(f"Collection complete: {len(all_locations)} locations, {len(all_reviews)} reviews")
        return all_locations, all_reviews
    
    def save_to_json(self, locations: List[BankLocation], reviews: List[Review], 
                     locations_file: str = "bank_locations.json", 
                     reviews_file: str = "bank_reviews.json"):
        """Save collected data to JSON files."""
        
        # Convert to dictionaries
        locations_data = [asdict(location) for location in locations]
        reviews_data = [asdict(review) for review in reviews]
        
        # Save locations
        with open(locations_file, 'w', encoding='utf-8') as f:
            json.dump(locations_data, f, ensure_ascii=False, indent=2, default=str)
        
        # Save reviews
        with open(reviews_file, 'w', encoding='utf-8') as f:
            json.dump(reviews_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Data saved: {locations_file}, {reviews_file}")
    
    def save_to_csv(self, locations: List[BankLocation], reviews: List[Review],
                    locations_file: str = "bank_locations.csv",
                    reviews_file: str = "bank_reviews.csv"):
        """Save collected data to CSV files."""
        
        # Convert to DataFrames
        locations_df = pd.DataFrame([asdict(location) for location in locations])
        reviews_df = pd.DataFrame([asdict(review) for review in reviews])
        
        # Save to CSV
        locations_df.to_csv(locations_file, index=False, encoding='utf-8')
        reviews_df.to_csv(reviews_file, index=False, encoding='utf-8')
        
        logger.info(f"Data saved: {locations_file}, {reviews_file}")
    
    def _get_city_coordinates(self, city: str) -> Tuple[float, float]:
        """Get coordinates for a city in Morocco."""
        # Coordinates for major Moroccan cities
        city_coords = {
            "Casablanca": (33.5731, -7.5898),
            "Rabat": (34.0209, -6.8416),
            "Fès": (34.0181, -5.0078),
            "Marrakech": (31.6295, -7.9811),
            "Agadir": (30.4278, -9.5981),
            "Tangier": (35.7595, -5.8340),
            "Meknès": (33.8935, -5.5473),
            "Oujda": (34.6814, -1.9086),
            "Kenitra": (34.2610, -6.5802),
            "Tetouan": (35.5889, -5.3626),
            "Safi": (32.2994, -9.2372),
            "Mohammedia": (33.6866, -7.3837),
            "Khouribga": (32.8811, -6.9063),
            "Beni Mellal": (32.3373, -6.3498),
            "El Jadida": (33.2316, -8.5007),
            "Taza": (34.2133, -4.0096),
            "Nador": (35.1681, -2.9287),
            "Settat": (33.0014, -7.6164)
        }
        return city_coords.get(city, (33.9716, -6.8498))  # Default to Morocco center
    
    def _extract_location_info(self, place: Dict, bank_name: str, city: str) -> Optional[BankLocation]:
        """Extract location information from Google Places result."""
        try:
            return BankLocation(
                place_id=place['place_id'],
                name=place['name'],
                bank_name=bank_name,
                address=place.get('formatted_address', ''),
                city=city,
                latitude=place['geometry']['location']['lat'],
                longitude=place['geometry']['location']['lng'],
                rating=place.get('rating'),
                user_ratings_total=place.get('user_ratings_total'),
                phone_number=place.get('formatted_phone_number'),
                website=place.get('website'),
                opening_hours=place.get('opening_hours', {}).get('weekday_text')
            )
        except KeyError:
            logger.warning(f"Missing required fields for place: {place}")
            return None
    
    def _extract_review_info(self, review: Dict, place_id: str, 
                           bank_name: str, branch_name: str) -> Optional[Review]:
        """Extract review information from Google Places review."""
        try:
            return Review(
                place_id=place_id,
                bank_name=bank_name,
                branch_name=branch_name,
                author_name=review['author_name'],
                author_url=review.get('author_url'),
                language=review.get('language', 'unknown'),
                original_language=review.get('original_language'),
                profile_photo_url=review.get('profile_photo_url'),
                rating=review['rating'],
                relative_time_description=review['relative_time_description'],
                text=review.get('text', ''),
                time=review['time'],
                translated=review.get('translated', False),
                review_id=f"{place_id}_{review['time']}_{review['author_name']}",
                collected_at=datetime.now()
            )
        except KeyError:
            logger.warning(f"Missing required fields for review: {review}")
            return None

if __name__ == "__main__":
    # Example usage
    collector = GoogleMapsCollector()
    
    # Test with one bank in one city
    locations, reviews = collector.collect_all_bank_reviews(
        banks=["Attijariwafa Bank"]
    )
    
    # Save results
    collector.save_to_json(locations, reviews)
    collector.save_to_csv(locations, reviews)
    
    print(f"Collected {len(locations)} locations and {len(reviews)} reviews") 