import pandas as pd
import json
from typing import Dict, List, Optional, Tuple
import logging

# Import our custom modules (adjust imports based on your project structure)
from models.movie import Movie
from models.rating import Rating
from utils.iso_mapper import ISOMapper

logger = logging.getLogger(__name__)

class MovieDataProcessor:
    """Main processor class for handling movie and rating data operations."""
    
    def __init__(self):
        self.movies_df = None
        self.ratings_df = None
        self.processed_movies = []
        self.processed_ratings = []
    
    def read_csv_data(self, main_csv_path: str, extended_csv_path: str = None) -> pd.DataFrame:
        """Read and merge CSV files with error handling."""
        try:
            logger.info(f"Reading main CSV from {main_csv_path}")
            main_df = pd.read_csv(main_csv_path)
            
            if extended_csv_path:
                logger.info(f"Reading extended CSV from {extended_csv_path}")
                extended_df = pd.read_csv(extended_csv_path)
                
                self.movies_df = pd.merge(main_df, extended_df, on='id', how='outer')
                logger.info(f"Merged data shape: {self.movies_df.shape}")
            else:
                self.movies_df = main_df
            
            # Remove duplicates based on movie ID
            initial_rows = len(self.movies_df)
            self.movies_df = self.movies_df.drop_duplicates(subset=['id'], keep='first')
            final_rows = len(self.movies_df)
            
            if initial_rows != final_rows:
                logger.info(f"Removed {initial_rows - final_rows} duplicate movies")
            
            return self.movies_df
            
        except FileNotFoundError as e:
            logger.error(f"CSV file not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV data: {e}")
            raise
    
    def read_json_data(self, json_path: str) -> pd.DataFrame:
        """Read JSON ratings data with error handling."""
        try:
            logger.info(f"Reading JSON data from {json_path}")
            
            with open(json_path, 'r') as file:
                # Read the entire file as a single JSON array
                ratings_data = json.load(file)
            
            if not ratings_data:
                logger.warning("No valid JSON data found")
                return pd.DataFrame()
            
            self.ratings_df = pd.DataFrame(ratings_data)
            logger.info(f"Loaded {len(self.ratings_df)} rating records")
            
            return self.ratings_df
            
        except FileNotFoundError as e:
            logger.error(f"JSON file not found: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading JSON data: {e}")
            raise
    
    def process_movies(self) -> List[Movie]:
        """Process movie data using Movie class with ISO mapping."""
        if self.movies_df is None:
            raise ValueError("No movie data loaded. Please read CSV data first.")
        
        logger.info("Processing movie data with ISO mapping...")
        self.processed_movies = []
        
        for idx, row in self.movies_df.iterrows():
            try:
                # Create movie instance
                movie = Movie(
                    movie_id=row.get('id', 0),
                    title=row.get('title', ''),
                    release_date=row.get('release_date', ''),
                    genres=row.get('genres', ''),
                    production_companies=row.get('production_companies', ''),
                    production_countries=row.get('production_countries', ''),
                    spoken_languages=row.get('spoken_languages', ''),
                    budget=row.get('budget', 0),
                    revenue=row.get('revenue', 0)
                )
                
                # Apply ISO mapping for countries and languages
                movie.production_countries = ISOMapper.clean_and_map_countries(
                    row.get('production_countries', '')
                )
                movie.spoken_languages = ISOMapper.clean_and_map_languages(
                    row.get('spoken_languages', '')
                )
                
                self.processed_movies.append(movie)
                
            except Exception as e:
                logger.error(f"Error processing movie at index {idx}: {e}")
                continue
        
        logger.info(f"Processed {len(self.processed_movies)} movies")
        return self.processed_movies
    
    def process_ratings(self) -> List[Rating]:
        """Process rating data using Rating class."""
        if self.ratings_df is None:
            raise ValueError("No rating data loaded. Please read JSON data first.")
        
        logger.info("Processing rating data...")
        self.processed_ratings = []
        
        for idx, row in self.ratings_df.iterrows():
            try:
                ratings_summary = row.get('ratings_summary', {})
                if isinstance(ratings_summary, dict):
                    rating = Rating(
                        movie_id=row.get('movie_id', 0),
                        ratings_data={
                            'avg_rating': ratings_summary.get('avg_rating'),
                            'total_ratings': ratings_summary.get('total_ratings'),
                            'std_dev': ratings_summary.get('std_dev'),
                            'last_rated': row.get('last_rated')
                        }
                    )
                else:
                    rating = Rating(
                        movie_id=row.get('movie_id', 0),
                        ratings_data={
                            'avg_rating': row.get('avg_rating'),
                            'total_ratings': row.get('total_ratings'),
                            'std_dev': row.get('std_dev'),
                            'last_rated': row.get('last_rated')
                        }
                    )
                
                self.processed_ratings.append(rating)
                
            except Exception as e:
                logger.error(f"Error processing rating at index {idx}: {e}")
                continue
        
        logger.info(f"Processed {len(self.processed_ratings)} ratings")
        return self.processed_ratings
    
    def create_analysis_dataframes(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Create cleaned DataFrames for analysis."""
        movies_data = [movie.to_dict() for movie in self.processed_movies]
        ratings_data = [rating.to_dict() for rating in self.processed_ratings]
        
        clean_movies_df = pd.DataFrame(movies_data)
        clean_ratings_df = pd.DataFrame(ratings_data)
        
        return clean_movies_df, clean_ratings_df
    
    def create_merged_dataset(self) -> pd.DataFrame:
        """Create a single merged DataFrame with all movie and rating data."""
        if not self.processed_movies or not self.processed_ratings:
            raise ValueError("No processed data available. Please process movies and ratings first.")
        
        logger.info("Creating merged dataset...")
        
        # Create separate DataFrames
        clean_movies_df, clean_ratings_df = self.create_analysis_dataframes()
        
        # Debug: Check data types and sample values
        logger.info(f"Movies 'id' column type: {clean_movies_df['id'].dtype}")
        logger.info(f"Ratings 'movie_id' column type: {clean_ratings_df['movie_id'].dtype}")
        logger.info(f"Sample movies IDs: {clean_movies_df['id'].head().tolist()}")
        logger.info(f"Sample ratings movie_ids: {clean_ratings_df['movie_id'].head().tolist()}")
        
        # Ensure both ID columns are the same type (convert to int64)
        try:
            clean_movies_df['id'] = pd.to_numeric(clean_movies_df['id'], errors='coerce').astype('Int64')
            clean_ratings_df['movie_id'] = pd.to_numeric(clean_ratings_df['movie_id'], errors='coerce').astype('Int64')
        except Exception as e:
            logger.error(f"Error converting ID columns: {e}")
            # If conversion fails, try converting both to string
            clean_movies_df['id'] = clean_movies_df['id'].astype(str)
            clean_ratings_df['movie_id'] = clean_ratings_df['movie_id'].astype(str)
        
        # Remove any rows with NaN IDs after conversion
        clean_movies_df = clean_movies_df.dropna(subset=['id'])
        clean_ratings_df = clean_ratings_df.dropna(subset=['movie_id'])
        
        logger.info(f"After type conversion - Movies: {len(clean_movies_df)} rows, Ratings: {len(clean_ratings_df)} rows")
        
        # Merge on movie ID (left join to keep all movies, even without ratings)
        merged_df = pd.merge(
            clean_movies_df, 
            clean_ratings_df, 
            left_on='id', 
            right_on='movie_id', 
            how='left'
        )
        
        # Drop the redundant movie_id column
        merged_df = merged_df.drop('movie_id', axis=1, errors='ignore')
        
        # Fill missing rating values with defaults for movies without ratings
        rating_defaults = {
            'avg_rating': 0.0,
            'total_ratings': 0,
            'std_dev': 0.0,
            'last_rated': None
        }
        merged_df = merged_df.fillna(rating_defaults)
        
        # Convert list columns to string format for CSV compatibility
        list_columns = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
        for col in list_columns:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].apply(
                    lambda x: ' | '.join(x) if isinstance(x, list) and x else ''
                )
        
        # Reorder columns for better readability
        column_order = [
            'id', 'title', 'release_date', 'genres', 
            'production_companies', 'production_countries', 'spoken_languages',
            'budget', 'revenue', 'avg_rating', 'total_ratings', 'std_dev', 'last_rated'
        ]
        
        # Only include columns that exist in the DataFrame
        existing_columns = [col for col in column_order if col in merged_df.columns]
        merged_df = merged_df[existing_columns]
        
        logger.info(f"Merged dataset created with {len(merged_df)} rows and {len(merged_df.columns)} columns")
        
        return merged_df
    
    def save_datasets(self, output_dir: str = '.') -> Dict[str, str]:
        """Save all datasets to CSV files."""
        import os
        
        saved_files = {}
        
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Save separate datasets
            clean_movies_df, clean_ratings_df = self.create_analysis_dataframes()
            
            movies_path = os.path.join(output_dir, 'cleaned_movies.csv')
            ratings_path = os.path.join(output_dir, 'cleaned_ratings.csv')
            
            # Convert list columns to strings for movies CSV
            movies_export_df = clean_movies_df.copy()
            list_columns = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
            for col in list_columns:
                if col in movies_export_df.columns:
                    movies_export_df[col] = movies_export_df[col].apply(
                        lambda x: ' | '.join(x) if isinstance(x, list) and x else ''
                    )
            
            movies_export_df.to_csv(movies_path, index=False)
            clean_ratings_df.to_csv(ratings_path, index=False)
            
            saved_files['movies'] = movies_path
            saved_files['ratings'] = ratings_path
            
            # Save merged dataset
            merged_df = self.create_merged_dataset()
            merged_path = os.path.join(output_dir, 'merged_movies_ratings.csv')
            merged_df.to_csv(merged_path, index=False)
            saved_files['merged'] = merged_path
            
            logger.info(f"All datasets saved to {output_dir}")
            return saved_files
            
        except Exception as e:
            logger.error(f"Error saving datasets: {e}")
            raise
    
    def analyze_data(self) -> Dict:
        """Perform comprehensive data analysis on cleaned data."""
        if not self.processed_movies or not self.processed_ratings:
            return {"error": "No processed data available for analysis"}
        
        clean_movies_df, clean_ratings_df = self.create_analysis_dataframes()
        
        # Advanced analysis using lambda functions and pandas operations
        analysis = {
            "movies_summary": {
                "total_movies": len(clean_movies_df),
                "movies_with_budget": len(clean_movies_df[clean_movies_df['budget'] > 0]),
                "movies_with_revenue": len(clean_movies_df[clean_movies_df['revenue'] > 0]),
                "average_budget": clean_movies_df[clean_movies_df['budget'] > 0]['budget'].mean(),
                "average_revenue": clean_movies_df[clean_movies_df['revenue'] > 0]['revenue'].mean(),
                "budget_revenue_ratio": clean_movies_df.apply(
                    lambda row: row['revenue'] / row['budget'] if row['budget'] > 0 else 0, axis=1
                ).mean(),
                "top_genres": self._get_top_categories(clean_movies_df, 'genres'),
                "top_countries": self._get_top_categories(clean_movies_df, 'production_countries'),
                "top_languages": self._get_top_categories(clean_movies_df, 'spoken_languages'),
                "date_range": {
                    "earliest": clean_movies_df['release_date'].min(),
                    "latest": clean_movies_df['release_date'].max()
                }
            },
            "ratings_summary": {
                "total_ratings": len(clean_ratings_df),
                "average_rating": clean_ratings_df['avg_rating'].mean(),
                "movies_with_ratings": len(clean_ratings_df[clean_ratings_df['total_ratings'] > 0]),
                "total_user_ratings": clean_ratings_df['total_ratings'].sum(),
                "rating_distribution": clean_ratings_df['avg_rating'].describe().to_dict()
            }
        }
        
        return analysis
    
    def _get_top_categories(self, df: pd.DataFrame, column: str, top_n: int = 10) -> List[Dict]:
        """Helper method to get top categories from list columns."""
        try:
            # Flatten all lists in the column
            all_items = []
            for items_list in df[column]:
                if isinstance(items_list, list):
                    all_items.extend(items_list)
            
            # Count occurrences
            if all_items:
                item_counts = pd.Series(all_items).value_counts().head(top_n)
                return [{"name": item, "count": count} for item, count in item_counts.items()]
            return []
            
        except Exception as e:
            logger.warning(f"Error analyzing {column}: {e}")
            return []