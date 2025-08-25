import requests
import time
from config import TMDB_API_KEY, TMDB_ACCESS_TOKEN, TMDB_BASE_URL, MAX_RETRIES, REQUEST_TIMEOUT, USE_BEARER_TOKEN
from utils.logger import log_error, log_info

class TMDbFetcher:
    def __init__(self):
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        """Setup session with proper headers and authentication"""
        # Set default headers
        self.session.headers.update({
            'User-Agent': 'Movie-Analytics-DataCleaner/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json;charset=utf-8'
        })
        
        # Configure authentication - Bearer token is preferred
        if USE_BEARER_TOKEN and TMDB_ACCESS_TOKEN and TMDB_ACCESS_TOKEN != "YOUR_TMDB_ACCESS_TOKEN":
            self.session.headers.update({
                'Authorization': f'Bearer {TMDB_ACCESS_TOKEN}'
            })
            log_info("Using Bearer token authentication (recommended)")
        elif TMDB_API_KEY and TMDB_API_KEY != "YOUR_TMDB_API_KEY":
            log_info("Using API key authentication (legacy)")
        else:
            log_error("No valid TMDb authentication found! Please set TMDB_ACCESS_TOKEN or TMDB_API_KEY")
    
    def _get_auth_params(self):
        """Get authentication parameters for legacy API key method"""
        if not USE_BEARER_TOKEN and TMDB_API_KEY != "YOUR_TMDB_API_KEY":
            return {"api_key": TMDB_API_KEY}
        return {}
    
    def fetch_movie_details(self, movie_id, append_to_response=None):
        """
        Fetch comprehensive movie details from TMDb API
        
        Args:
            movie_id: The TMDb movie ID
            append_to_response: Additional endpoints to append (e.g., "credits,videos,images")
        """
        for attempt in range(MAX_RETRIES):
            try:
                url = f"{TMDB_BASE_URL}/movie/{movie_id}"
                
                # Build parameters
                params = self._get_auth_params()
                
                # Add optional append_to_response for getting more data in one request
                if append_to_response:
                    params['append_to_response'] = append_to_response
                
                # Add language parameter for better localization
                params['language'] = 'en-US'
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=REQUEST_TIMEOUT
                )
                
                # Handle specific HTTP status codes
                if response.status_code == 401:
                    log_error("TMDb API authentication failed - check your API key or access token")
                    return {}
                elif response.status_code == 404:
                    log_error(f"Movie ID {movie_id} not found in TMDb")
                    return {}
                elif response.status_code == 429:
                    log_error("TMDb API rate limit exceeded - waiting before retry")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff for rate limits
                        continue
                
                response.raise_for_status()
                data = response.json()
                
                # Process and clean the returned data
                cleaned_data = self._clean_movie_data(data)
                log_info(f"Successfully fetched data for movie ID: {movie_id}")
                return cleaned_data
                
            except requests.exceptions.Timeout:
                log_error(f"Timeout occurred for movie ID {movie_id} (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                    
            except requests.exceptions.ConnectionError:
                log_error(f"Connection error for movie ID {movie_id} (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                    
            except requests.exceptions.RequestException as e:
                log_error(f"Request failed for movie ID {movie_id} (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                    
            except Exception as e:
                log_error(f"Unexpected error for movie ID {movie_id}: {e}")
                break
        
        log_error(f"All {MAX_RETRIES} attempts failed for movie ID {movie_id}")
        return {}
    
    def _clean_movie_data(self, data):
        """Clean and standardize TMDb movie data"""
        if not data:
            return {}
        
        cleaned = {}
        
        # Basic movie information
        basic_fields = {
            'id': data.get('id'),
            'title': data.get('title'),
            'original_title': data.get('original_title'),
            'release_date': data.get('release_date'),
            'budget': data.get('budget'),
            'revenue': data.get('revenue'),
            'runtime': data.get('runtime'),
            'vote_average': data.get('vote_average'),
            'vote_count': data.get('vote_count'),
            'popularity': data.get('popularity'),
            'overview': data.get('overview'),
            'tagline': data.get('tagline'),
            'homepage': data.get('homepage'),
            'status': data.get('status'),
            'adult': data.get('adult')
        }
        
        # Add basic fields if they exist
        for key, value in basic_fields.items():
            if value is not None and value != "":
                cleaned[key] = value
        
        # Process complex fields
        # Genres
        if 'genres' in data and data['genres']:
            cleaned['genres'] = [genre['name'] for genre in data['genres']]
        
        # Production companies
        if 'production_companies' in data and data['production_companies']:
            cleaned['production_companies'] = [company['name'] for company in data['production_companies']]
        
        # Production countries
        if 'production_countries' in data and data['production_countries']:
            cleaned['production_countries'] = [country['name'] for country in data['production_countries']]
        
        # Spoken languages
        if 'spoken_languages' in data and data['spoken_languages']:
            cleaned['spoken_languages'] = [lang['english_name'] for lang in data['spoken_languages']]
        
        return cleaned
    
    def search_movie(self, query, year=None, page=1):
        """Search for movies by title"""
        try:
            url = f"{TMDB_BASE_URL}/search/movie"
            params = self._get_auth_params()
            params.update({
                'query': query,
                'page': page,
                'language': 'en-US'
            })
            
            if year:
                params['year'] = year
                
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            log_error(f"Movie search failed for query '{query}': {e}")
            return {}
    
    def get_movie_credits(self, movie_id):
        """Get cast and crew information for a movie"""
        try:
            url = f"{TMDB_BASE_URL}/movie/{movie_id}/credits"
            params = self._get_auth_params()
            params['language'] = 'en-US'
            
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            log_error(f"Failed to fetch credits for movie ID {movie_id}: {e}")
            return {}

# Global instance
tmdb_fetcher = TMDbFetcher()

def fetch_movie_details(movie_id):
    """Wrapper function for backward compatibility"""
    return tmdb_fetcher.fetch_movie_details(movie_id)