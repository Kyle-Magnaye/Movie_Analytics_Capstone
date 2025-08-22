import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)

class Movie:
    """Movie class to represent individual movie data with cleaning and validation methods."""
    
    def __init__(self, movie_id: int, title: str, release_date: str, 
                 genres: str = "", production_companies: str = "", 
                 production_countries: str = "", spoken_languages: str = "",
                 budget: Union[int, str] = 0, revenue: Union[int, str] = 0):
        # Check if movie_id is date-like or invalid
        movie_id_str = str(movie_id).strip()
        if ('/' in movie_id_str or '-' in movie_id_str or 
            re.match(r'\d{1,2}/\d{1,2}/\d{4}', movie_id_str) or 
            re.match(r'\d{4}-\d{1,2}-\d{1,2}', movie_id_str)):
            raise ValueError(f"Invalid movie ID (date-like): {movie_id}")
        
        # Check if budget contains .jpg or similar file extensions
        budget_str = str(budget).strip().lower()
        if any(ext in budget_str for ext in ['.jpg', '.png', '.gif', '.pdf']):
            raise ValueError(f"Invalid budget (contains file extension): {budget}")
        
        try:
            self.id = int(float(movie_id_str))
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert movie_id to integer: {movie_id}")
        
        if self.id <= 0:
            raise ValueError(f"Movie ID must be positive: {self.id}")
        
        self.title = self._clean_text(title)
        self.release_date = self._standardize_date(release_date)
        self.genres = self._clean_and_parse_json_field(genres)
        self.production_companies = self._clean_and_parse_json_field(production_companies)
        self.production_countries = production_countries  # Will be cleaned by processor
        self.spoken_languages = spoken_languages  # Will be cleaned by processor
        self.budget = self._clean_financial_data(budget)
        self.revenue = self._clean_financial_data(revenue)
    
    def _clean_movie_id(self, movie_id: Union[int, str]) -> Optional[int]:
        """Clean and validate movie ID, return None if invalid."""
        if pd.isna(movie_id) or movie_id is None:
            return None
        
        try:
            id_str = str(movie_id).strip()
            
            # Check if it looks like a date (contains /)
            if '/' in id_str or '-' in id_str:
                # Additional check: if it has date-like patterns
                date_patterns = [r'\d{1,2}/\d{1,2}/\d{4}', r'\d{4}-\d{1,2}-\d{1,2}']
                for pattern in date_patterns:
                    if re.match(pattern, id_str):
                        logger.debug(f"Dropping movie with date-like ID: {id_str}")
                        return None
            
            # Check for file extensions or other invalid formats
            invalid_patterns = [r'\.jpg$', r'\.png$', r'\.gif$', r'\.pdf$']
            for pattern in invalid_patterns:
                if re.search(pattern, id_str, re.IGNORECASE):
                    logger.debug(f"Dropping movie with file-like ID: {id_str}")
                    return None
            
            # Try to convert to integer
            cleaned_id = int(float(id_str))
            
            # Validate range (movie IDs should be positive)
            if cleaned_id <= 0:
                logger.debug(f"Dropping movie with non-positive ID: {cleaned_id}")
                return None
            
            return cleaned_id
            
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not convert movie_id {movie_id} to integer: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean text by trimming whitespaces and handling special characters."""
        if pd.isna(text) or text is None:
            return ""
        
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('"', "'")
        text = re.sub(r'[^\w\s\-\':.,!?()\[\]{}]', '', text)
        
        return text
    
    def _standardize_date(self, date_str: str) -> Optional[str]:
        """Standardize date format to YYYY-MM-DD."""
        if pd.isna(date_str) or date_str is None or str(date_str).strip() == "":
            return None
        
        try:
            date_str = str(date_str).strip()
            
            date_formats = [
                "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", 
                "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
            if year_match:
                return f"{year_match.group()}-01-01"
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {e}")
            return None
    
    def _clean_financial_data(self, value: Union[int, str, float]) -> int:
        """Clean and validate budget/revenue data."""
        if pd.isna(value) or value is None or str(value).strip() == "":
            return 0
        
        try:
            value_str = str(value).strip().replace(',', '').replace('$', '')
            
            # Check for file extensions in financial data
            invalid_patterns = [r'\.jpg$', r'\.png$', r'\.gif$', r'\.pdf$']
            for pattern in invalid_patterns:
                if re.search(pattern, value_str, re.IGNORECASE):
                    logger.warning(f"Found file extension in financial data: {value_str}, setting to 0")
                    return 0
            
            if 'e' in value_str.lower():
                return int(float(value_str))
            
            cleaned_value = int(float(value_str))
            
            if 0 <= cleaned_value:
                return cleaned_value
            else:
                logger.warning(f"Financial value out of range: {value}")
                return 0
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert financial value {value}: {e}")
            return 0
    
    def _clean_and_parse_json_field(self, json_str: str) -> List[str]:
        """Parse JSON-like strings and extract names."""
        if pd.isna(json_str) or json_str is None or str(json_str).strip() == "":
            return []
        
        try:
            json_str = str(json_str).strip()
            name_pattern = r"'name':\s*'([^']+)'|\"name\":\s*\"([^\"]+)\""
            matches = re.findall(name_pattern, json_str)
            
            names = []
            for match in matches:
                name = match[0] if match[0] else match[1]
                if name:
                    names.append(self._clean_text(name))
            
            return names if names else []
            
        except Exception as e:
            logger.warning(f"Could not parse JSON field {json_str}: {e}")
            return []
    
    def to_dict(self) -> Dict:
        """Convert movie object to dictionary.""" 
        return {
            'id': self.id,
            'title': self.title,
            'release_date': self.release_date,
            'genres': self.genres,
            'production_companies': self.production_companies,
            'production_countries': self.production_countries,
            'spoken_languages': self.spoken_languages,
            'budget': self.budget,
            'revenue': self.revenue
        }