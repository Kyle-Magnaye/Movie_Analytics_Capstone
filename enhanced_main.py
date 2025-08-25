import logging
import os
from processors.enhanced_data_processor import EnhancedMovieDataProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_movie_processing.log'),
        logging.StreamHandler()
    ]
)

def main():
    """Main function for the enhanced data processing pipeline."""
    
    logger = logging.getLogger(__name__)
    
    try:
        print("🎬 Enhanced Movie Data Processing System with TMDB Integration")
        print("=" * 70)
        
        # File paths - adjust these to match your data files
        main_csv_path = 'dataset/movies_main_enriched.csv'
        extended_csv_path = 'dataset/movie_extended_enriched.csv'
        ratings_json_path = 'dataset/ratings.json'
        output_path = 'output/final_cleaned_movies.csv'
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        # Initialize enhanced processor
        processor = EnhancedMovieDataProcessor()
        
        # Configuration options
        USE_TMDB_API = True  # Set to False if you want to skip TMDB API calls
        BATCH_SIZE = 50      # Number of movies to process before logging progress
        
        print(f"📁 Input files:")
        print(f"  - Main CSV: {main_csv_path}")
        print(f"  - Extended CSV: {extended_csv_path}")
        print(f"  - Ratings JSON: {ratings_json_path}")
        print(f"  - Output path: {output_path}")
        print(f"🔧 Configuration:")
        print(f"  - TMDB API enabled: {USE_TMDB_API}")
        print(f"  - Batch size: {BATCH_SIZE}")
        print()
        
        # Run the complete pipeline
        final_dataset_path = processor.run_complete_pipeline(
            main_csv_path=main_csv_path,
            extended_csv_path=extended_csv_path,
            ratings_json_path=ratings_json_path,
            output_path=output_path,
            use_tmdb_api=USE_TMDB_API,
            batch_size=BATCH_SIZE
        )
        
        print("✅ Processing completed successfully!")
        print(f"📊 Final dataset saved to: {final_dataset_path}")
        
        # Optional: Show sample of final data
        print("\n📋 Sample of processed data:")
        try:
            import pandas as pd
            sample_df = pd.read_csv(final_dataset_path)
            print(f"Dataset shape: {sample_df.shape}")
            print("\nFirst 5 rows:")
            print(sample_df.head())
            
            print("\nColumn information:")
            for col in sample_df.columns:
                non_empty = len(sample_df[sample_df[col].notna() & (sample_df[col] != '')])
                print(f"  - {col}: {non_empty}/{len(sample_df)} non-empty values")
                
        except Exception as e:
            logger.warning(f"Could not display sample data: {e}")
        
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}")
        print("❌ Error: Please make sure all required data files exist:")
        print(f"  - {main_csv_path}")
        print(f"  - {extended_csv_path}")
        print(f"  - {ratings_json_path}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"❌ Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()