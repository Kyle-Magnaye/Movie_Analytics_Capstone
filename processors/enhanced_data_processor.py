import pandas as pd
import json
import time
from typing import Dict, List, Optional, Tuple, Union
import logging
import numpy as np

# Import custom modules
from models.movie import Movie
from models.rating import Rating
from utils.iso_mapper import ISOMapper
from tmdb_fetcher import tmdb_fetcher

logger = logging.getLogger(__name__)

class EnhancedMovieDataProcessor:
    """Enhanced processor class with TMDB API integration for complete data processing."""
    
    def __init__(self):
        self.merged_df = None
        self.processed_movies = []
        self.tmdb_fetcher = tmdb_fetcher
    
    def load_and_merge_data(self, main_csv_path: str, extended_csv_path: str, ratings_json_path: str) -> pd.DataFrame:
        """
        Load CSV files and JSON ratings, then merge them with outer join to keep all data.
        """
        try:
            logger.info("Loading and merging all data sources...")
            
            # Load main CSV
            logger.info(f"Reading main CSV from {main_csv_path}")
            main_df = pd.read_csv(main_csv_path)
            
            # Load extended CSV
            logger.info(f"Reading extended CSV from {extended_csv_path}")
            extended_df = pd.read_csv(extended_csv_path)
            
            # Load ratings JSON
            logger.info(f"Reading ratings JSON from {ratings_json_path}")
            with open(ratings_json_path, 'r') as file:
                ratings_data = json.load(file)
            ratings_df = pd.DataFrame(ratings_data)
            
            # Flatten ratings_summary if it exists
            if 'ratings_summary' in ratings_df.columns:
                # Extract nested ratings_summary data
                ratings_summary_df = pd.json_normalize(ratings_df['ratings_summary'])
                ratings_summary_df['movie_id'] = ratings_df['movie_id']
                ratings_summary_df['last_rated'] = ratings_df['last_rated']
                ratings_df = ratings_summary_df
            
            # Merge CSVs first (outer join to keep all movies)
            logger.info("Merging CSV files...")
            movies_df = pd.merge(main_df, extended_df, on='id', how='outer', suffixes=('', '_extended'))
            
            # Handle duplicate columns from merge
            for col in movies_df.columns:
                if col.endswith('_extended'):
                    base_col = col.replace('_extended', '')
                    if base_col in movies_df.columns:
                        # Fill missing values from extended dataset
                        movies_df[base_col] = movies_df[base_col].fillna(movies_df[col])
                        movies_df.drop(col, axis=1, inplace=True)
            
            # Fix ID column types before merging
            movies_df, ratings_df = self._fix_id_column_types(movies_df, ratings_df)

            # Merge with ratings (outer join to keep all data from both sources)
            logger.info("Merging with ratings data...")
            self.merged_df = pd.merge(
                movies_df, 
                ratings_df, 
                left_on='id', 
                right_on='movie_id', 
                how='outer',
                suffixes=('', '_rating')
            )
            
            # Clean up duplicate ID column
            if 'movie_id' in self.merged_df.columns:
                # Fill missing IDs from either source
                self.merged_df['id'] = self.merged_df['id'].fillna(self.merged_df['movie_id'])
                self.merged_df.drop('movie_id', axis=1, inplace=True)
            
            # Remove exact duplicates based on ID
            initial_rows = len(self.merged_df)
            self.merged_df = self.merged_df.drop_duplicates(subset=['id'], keep='first')
            final_rows = len(self.merged_df)
            
            if initial_rows != final_rows:
                logger.info(f"Removed {initial_rows - final_rows} duplicate rows")
            
            logger.info(f"Merged dataset created with {len(self.merged_df)} rows and {len(self.merged_df.columns)} columns")
            return self.merged_df
            
        except Exception as e:
            logger.error(f"Error in load_and_merge_data: {e}")
            raise

    def _fix_id_column_types(self, movies_df, ratings_df):
        """Fix ID column type mismatches before merging."""
        
        # Function to clean and convert IDs
        def clean_id(id_val):
            if pd.isna(id_val) or id_val is None:
                return None
            
            id_str = str(id_val).strip()
            
            # Skip date-like patterns
            if '/' in id_str or '-' in id_str:
                if len(id_str) > 7:  # Likely a date if long and has separators
                    return None
            
            try:
                # Convert to int, handling floats first
                return int(float(id_str))
            except (ValueError, TypeError):
                return None
        
        # Make explicit copies to avoid SettingWithCopyWarning
        movies_df = movies_df.copy()
        ratings_df = ratings_df.copy()
        
        # Clean movie IDs
        if 'id' in movies_df.columns:
            movies_df.loc[:, 'id'] = movies_df['id'].apply(clean_id)
            movies_df = movies_df.dropna(subset=['id'])
            movies_df.loc[:, 'id'] = movies_df['id'].astype('int64')
        
        # Clean rating movie IDs  
        if 'movie_id' in ratings_df.columns:
            ratings_df.loc[:, 'movie_id'] = ratings_df['movie_id'].apply(clean_id)
            ratings_df = ratings_df.dropna(subset=['movie_id'])
            ratings_df.loc[:, 'movie_id'] = ratings_df['movie_id'].astype('int64')
        
        return movies_df, ratings_df

    def _is_missing_value(self, value) -> bool:
        """
        Check if a value should be considered as missing.
        Handles null, 0, nan, [], [ ], and other indicators.
        """
        if pd.isna(value) or value is None:
            return True
        
        if isinstance(value, (int, float)) and value == 0:
            return True
        
        if isinstance(value, str):
            cleaned_val = str(value).strip()
            if cleaned_val in ['', '0', 'null', 'NULL', 'nan', 'NaN', '[]', '[ ]', '{}']:
                return True
        
        if isinstance(value, list) and len(value) == 0:
            return True
            
        return False
    
    def fill_missing_with_tmdb(self, batch_size: int = 50) -> pd.DataFrame:
        """
        Fill missing values using TMDB API for specified columns.
        Always fetch spoken_languages regardless of existing values.
        """
        logger.info("Starting TMDB API data filling process...")
        
        target_columns = ['title', 'release_date', 'genres', 'production_companies', 
                         'production_countries', 'budget', 'revenue']
        
        # Always fetch spoken_languages
        always_fetch_columns = ['spoken_languages']
        
        total_rows = len(self.merged_df)
        updated_count = 0
        api_calls_made = 0
        
        # Process in batches to manage memory and API rate limits
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            logger.info(f"Processing batch {i//batch_size + 1}: rows {i+1} to {batch_end}")
            
            for idx in range(i, batch_end):
                row = self.merged_df.iloc[idx]
                movie_id = row['id']
                
                # Skip if no valid movie ID
                if pd.isna(movie_id) or movie_id <= 0:
                    continue
                
                # Check if we need to fetch data for this movie
                needs_update = False
                
                # Check target columns for missing values
                for col in target_columns:
                    if col in self.merged_df.columns and self._is_missing_value(row[col]):
                        needs_update = True
                        break
                
                # Always update spoken_languages
                needs_update = True  # Force update for spoken_languages
                
                if needs_update:
                    try:
                        logger.info(f"Fetching TMDB data for movie ID: {movie_id}")
                        tmdb_data = self.tmdb_fetcher.fetch_movie_details(int(movie_id))
                        api_calls_made += 1
                        
                        if tmdb_data:
                            self._update_row_with_tmdb_data(idx, tmdb_data, target_columns, always_fetch_columns)
                            updated_count += 1
                        
                        
                    except Exception as e:
                        logger.warning(f"Failed to fetch TMDB data for movie ID {movie_id}: {e}")
                        continue
            
            # Log progress
            logger.info(f"Completed batch {i//batch_size + 1}. Updated {updated_count} movies so far.")
        
        logger.info(f"TMDB data filling completed. Updated {updated_count} movies with {api_calls_made} API calls.")
        return self.merged_df
    
    def _update_row_with_tmdb_data(self, row_idx: int, tmdb_data: Dict, 
                                  target_columns: List[str], always_fetch_columns: List[str]):
        """Update a specific row with TMDB data."""
        
        # Update target columns only if they're missing
        for col in target_columns:
            if col in self.merged_df.columns and self._is_missing_value(self.merged_df.iloc[row_idx][col]):
                if col == 'genres' and 'genres' in tmdb_data:
                    # Extract only genre names
                    self.merged_df.at[row_idx, col] = tmdb_data['genres']
                elif col == 'production_companies' and 'production_companies' in tmdb_data:
                    self.merged_df.at[row_idx, col] = tmdb_data['production_companies']
                elif col == 'production_countries' and 'production_countries' in tmdb_data:
                    # Extract only country names
                    self.merged_df.at[row_idx, col] = tmdb_data['production_countries']
                elif col in tmdb_data:
                    self.merged_df.at[row_idx, col] = tmdb_data[col]
        
        # Always update spoken_languages with english_name values
        if 'spoken_languages' in tmdb_data:
            # tmdb_data['spoken_languages'] already contains just the english_name values
            self.merged_df.at[row_idx, 'spoken_languages'] = tmdb_data['spoken_languages']
    
    def clean_data_with_proper_methods(self) -> List[Movie]:
        """
        Apply PROPER cleaning methods including Rating class for timestamps and formatting.
        """
        logger.info("Applying proper cleaning methods with Rating class...")
        
        self.processed_movies = []
        dropped_count = 0
        
        for idx, row in self.merged_df.iterrows():
            try:
                # Create movie instance with existing cleaning logic
                movie = Movie(
                    movie_id=row.get('id', 0),
                    title=row.get('title', ''),
                    release_date=row.get('release_date', ''),
                    genres=row.get('genres', ''),
                    production_companies=row.get('production_companies', ''),
                    production_countries=row.get('production_countries', ''),
                    spoken_languages=row.get('spoken_languages', ''),  # This will be cleaned
                    budget=row.get('budget', 0),
                    revenue=row.get('revenue', 0)
                )
                
                # PROPERLY clean production countries with ISO mapping
                movie.production_countries = ISOMapper.clean_and_map_countries(
                    row.get('production_countries', '')
                )
                
                # PROPERLY clean spoken languages with ISO mapping  
                movie.spoken_languages = ISOMapper.clean_and_map_languages(
                    row.get('spoken_languages', '')
                )
                
                # Convert movie to dict
                movie_dict = movie.to_dict()
                
                # PROPERLY process ratings data using Rating class
                if any(col in row and not pd.isna(row[col]) for col in ['avg_rating', 'total_ratings', 'std_dev', 'last_rated']):
                    ratings_data = {
                        'avg_rating': row.get('avg_rating'),
                        'total_ratings': row.get('total_ratings'), 
                        'std_dev': row.get('std_dev'),
                        'last_rated': row.get('last_rated')
                    }
                    
                    # Use Rating class to properly clean and format ratings
                    rating = Rating(movie.id, ratings_data)
                    rating_dict = rating.to_dict()
                    
                    # Add cleaned ratings to movie dict
                    movie_dict.update({
                        'avg_rating': rating_dict['avg_rating'],
                        'total_ratings': rating_dict['total_ratings'],
                        'std_dev': rating_dict['std_dev'],
                        'last_rated': rating_dict['last_rated']  # This will be properly formatted timestamp
                    })
                else:
                    # Set defaults for missing ratings
                    movie_dict.update({
                        'avg_rating': 0.0,
                        'total_ratings': 0,
                        'std_dev': 0.0,
                        'last_rated': None
                    })
                
                # Store the properly cleaned movie data
                self.processed_movies.append(movie_dict)
                
            except ValueError as e:
                # Skip movies with invalid IDs or other validation errors
                logger.debug(f"Skipping invalid movie at index {idx}: {e}")
                dropped_count += 1
                continue
            except Exception as e:
                logger.error(f"Error processing movie at index {idx}: {e}")
                continue
        
        logger.info(f"Data cleaning completed. Processed {len(self.processed_movies)} movies, dropped {dropped_count} invalid movies")
        return self.processed_movies
    
    def save_final_dataset(self, output_path: str = 'final_cleaned_movies.csv') -> str:
        """
        Save the final cleaned and enhanced dataset to a single CSV file.
        """
        try:
            logger.info("Preparing final dataset for saving...")
            
            if not self.processed_movies:
                raise ValueError("No processed movies data available. Run the complete pipeline first.")
            
            # Convert processed movies to DataFrame
            final_df = pd.DataFrame(self.processed_movies)
            
            # Convert list columns to pipe-separated strings for CSV compatibility
            list_columns = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
            for col in list_columns:
                if col in final_df.columns:
                    final_df[col] = final_df[col].apply(
                        lambda x: ' | '.join(x) if isinstance(x, list) and x else ''
                    )
            
            # Reorder columns for better readability
            column_order = [
                'id', 'title', 'release_date', 'genres', 
                'production_companies', 'production_countries', 'spoken_languages',
                'budget', 'revenue', 'avg_rating', 'total_ratings', 'std_dev', 'last_rated'
            ]
            
            # Only include columns that exist in the DataFrame
            existing_columns = [col for col in column_order if col in final_df.columns]
            remaining_columns = [col for col in final_df.columns if col not in existing_columns]
            final_column_order = existing_columns + remaining_columns
            
            final_df = final_df[final_column_order]
            
            # Save to CSV
            final_df.to_csv(output_path, index=False)
            
            logger.info(f"Final dataset saved to {output_path}")
            logger.info(f"Dataset contains {len(final_df)} rows and {len(final_df.columns)} columns")
            
            # Log summary statistics
            logger.info("Final dataset summary:")
            logger.info(f"- Movies with complete title: {len(final_df[final_df['title'].notna() & (final_df['title'] != '')])}")
            logger.info(f"- Movies with release date: {len(final_df[final_df['release_date'].notna()])}")
            logger.info(f"- Movies with budget > 0: {len(final_df[final_df['budget'] > 0])}")
            logger.info(f"- Movies with revenue > 0: {len(final_df[final_df['revenue'] > 0])}")
            logger.info(f"- Movies with ratings: {len(final_df[final_df['total_ratings'] > 0])}")
            logger.info(f"- Movies with proper timestamps: {len(final_df[final_df['last_rated'].notna()])}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving final dataset: {e}")
            raise
    
    def run_complete_pipeline(self, main_csv_path: str, extended_csv_path: str, 
                            ratings_json_path: str, output_path: str = 'final_cleaned_movies.csv',
                            use_tmdb_api: bool = True, batch_size: int = 50) -> str:
        """
        Run the complete data processing pipeline with PROPER cleaning.
        
        Args:
            main_csv_path: Path to main CSV file
            extended_csv_path: Path to extended CSV file  
            ratings_json_path: Path to ratings JSON file
            output_path: Path for final output CSV
            use_tmdb_api: Whether to use TMDB API for missing data
            batch_size: Batch size for TMDB API calls
        
        Returns:
            Path to saved final dataset
        """
        try:
            logger.info("🎬 Starting Enhanced Movie Data Processing Pipeline WITH PROPER CLEANING")
            logger.info("=" * 70)
            
            # Step 1: Load and merge all data sources
            logger.info("Step 1: Loading and merging data sources...")
            self.load_and_merge_data(main_csv_path, extended_csv_path, ratings_json_path)
            
            # Step 2: Fill missing values with TMDB API (optional)
            if use_tmdb_api:
                logger.info("Step 2: Filling missing values with TMDB API...")
                self.fill_missing_with_tmdb(batch_size=batch_size)
            else:
                logger.info("Step 2: Skipping TMDB API integration (disabled)")
            
            # Step 3: Apply PROPER cleaning methods (including Rating class)
            logger.info("Step 3: Applying PROPER data cleaning methods with Rating class...")
            self.clean_data_with_proper_methods()  # FIXED: Use proper cleaning method
            
            # Step 4: Save final dataset
            logger.info("Step 4: Saving final cleaned dataset...")
            final_path = self.save_final_dataset(output_path)
            
            logger.info("✅ Pipeline completed successfully with PROPER cleaning!")
            logger.info(f"📁 Final dataset saved to: {final_path}")
            logger.info("📋 Cleaning applied:")
            logger.info("  ✅ Timestamps converted to readable format")
            logger.info("  ✅ List fields properly formatted")
            logger.info("  ✅ Countries and languages properly mapped")
            logger.info("  ✅ Rating data properly cleaned and validated")
            
            return final_path
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

# Usage example and backward compatibility
def create_enhanced_processor():
    """Factory function to create an enhanced processor instance."""
    return EnhancedMovieDataProcessor()