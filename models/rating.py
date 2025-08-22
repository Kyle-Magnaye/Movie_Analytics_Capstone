import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)

class Rating:
    """Rating class to represent movie ratings data with cleaning and validation methods."""
    
    def __init__(self, movie_id: int, ratings_data: Dict):
        self.movie_id = movie_id
        self.avg_rating = self._clean_rating(ratings_data.get('avg_rating'))
        self.total_ratings = self._clean_count(ratings_data.get('total_ratings'))
        self.std_dev = self._clean_std_dev(ratings_data.get('std_dev'), self.total_ratings)
        self.last_rated = self._clean_timestamp(ratings_data.get('last_rated'))
    
    def _clean_rating(self, rating: Union[float, str]) -> float:
        """Clean and validate rating value."""
        if pd.isna(rating) or rating is None:
            return 0.0
        
        try:
            rating_float = float(rating)
            if 0 <= rating_float <= 10:
                return round(rating_float, 2)
            else:
                logger.warning(f"Rating out of range: {rating}")
                return 0.0
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert rating {rating}: {e}")
            return 0.0
    
    def _clean_count(self, count: Union[int, str]) -> int:
        """Clean and validate rating count."""
        if pd.isna(count) or count is None:
            return 0
        
        try:
            count_int = int(float(count))
            return max(0, count_int)
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert count {count}: {e}")
            return 0
    
    def _clean_std_dev(self, std_dev: Union[float, str], total_ratings: int) -> float:
        """Clean standard deviation, set to 0 if NaN or only one rating."""
        if pd.isna(std_dev) or std_dev is None or total_ratings <= 1:
            return 0.0
        
        try:
            std_dev_float = float(std_dev)
            return round(max(0.0, std_dev_float), 4)
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert std_dev {std_dev}: {e}")
            return 0.0
    
    def _clean_timestamp(self, timestamp: Union[int, str]) -> Optional[str]:
        """Clean and convert timestamp to readable format."""
        if pd.isna(timestamp) or timestamp is None:
            return None
        
        try:
            timestamp_int = int(float(timestamp))
            dt = datetime.fromtimestamp(timestamp_int)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"Could not convert timestamp {timestamp}: {e}")
            return None
    
    def to_dict(self) -> Dict:
        """Convert rating object to dictionary."""
        return {
            'movie_id': self.movie_id,
            'avg_rating': self.avg_rating,
            'total_ratings': self.total_ratings,
            'std_dev': self.std_dev,
            'last_rated': self.last_rated
        }