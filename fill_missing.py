import logging
import os
from processors.enhanced_data_processor import EnhancedMovieDataProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_enrichment.log'),
        logging.StreamHandler()
    ]
)

def main():
    """Main function for DATA ENRICHMENT ONLY - merging and TMDB API integration."""
    
    logger = logging.getLogger(__name__)
    
    try:
        print("🔄 Movie Data Enrichment System")
        print("(Merging + TMDB API Integration)")
        print("=" * 50)
        
        # File paths - adjust these to match your data files
        main_csv_path = 'dataset/movies_main_enriched.csv'
        extended_csv_path = 'dataset/movie_extended_enriched.csv'
        ratings_json_path = 'dataset/ratings.json'
        output_path = 'output/enriched_movies_raw.csv'  # Raw enriched data (not cleaned yet)
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        print(f"📁 Input files:")
        print(f"  - Main CSV: {main_csv_path}")
        print(f"  - Extended CSV: {extended_csv_path}")
        print(f"  - Ratings JSON: {ratings_json_path}")
        print(f"  - Output path: {output_path}")
        
        # Configuration for enrichment
        USE_TMDB_API = True  # Set to False if you want to skip TMDB API calls
        BATCH_SIZE = 50      # Number of movies to process before logging progress
        
        print(f"🔧 Enrichment configuration:")
        print(f"  - TMDB API enabled: {USE_TMDB_API}")
        print(f"  - Batch size: {BATCH_SIZE}")
        print()
        
        # Initialize processor for enrichment only
        processor = EnhancedMovieDataProcessor()
        
        print("🚀 Starting enrichment process...")
        
        # Step 1: Load and merge all data sources
        logger.info("Step 1: Loading and merging data sources...")
        merged_df = processor.load_and_merge_data(main_csv_path, extended_csv_path, ratings_json_path)
        print(f"✅ Data merged: {len(merged_df)} rows, {len(merged_df.columns)} columns")
        
        # Step 2: Fill missing values with TMDB API (enrichment step)
        if USE_TMDB_API:
            logger.info("Step 2: Enriching with TMDB API data...")
            print("🌐 Fetching missing data from TMDB API...")
            enriched_df = processor.fill_missing_with_tmdb(batch_size=BATCH_SIZE)
            print("✅ TMDB enrichment completed")
        else:
            logger.info("Step 2: Skipping TMDB API integration (disabled)")
            print("⏭️ Skipping TMDB API enrichment")
            enriched_df = merged_df
        
        # Step 3: Save enriched raw data (NO CLEANING YET)
        logger.info("Step 3: Saving enriched raw data...")
        print("💾 Saving enriched raw data...")
        
        # Create output directory if needed
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the raw enriched data
        enriched_df.to_csv(output_path, index=False)
        
        print("✅ Data enrichment completed successfully!")
        print(f"📊 Enriched dataset saved to: {output_path}")
        print(f"📋 Dataset shape: {enriched_df.shape}")
        
        # Show enrichment summary
        print("\n📈 Enrichment Summary:")
        print(f"  - Total movies: {len(enriched_df)}")
        print(f"  - Total columns: {len(enriched_df.columns)}")
        
        # Check data completeness after enrichment
        key_columns = ['title', 'release_date', 'genres', 'production_companies', 
                      'production_countries', 'spoken_languages', 'budget', 'revenue']
        
        print("📊 Data completeness after enrichment:")
        for col in key_columns:
            if col in enriched_df.columns:
                non_empty = len(enriched_df[enriched_df[col].notna() & (enriched_df[col] != '') & (enriched_df[col] != 0)])
                print(f"  - {col}: {non_empty}/{len(enriched_df)} ({non_empty/len(enriched_df)*100:.1f}%)")
        
        print(f"\n🎯 Next step: Run the cleaning pipeline on '{output_path}'")
        print("   Use: python clean_single_file_main.py")
        
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}")
        print("❌ Error: Please make sure all required data files exist:")
        print(f"  - {main_csv_path}")
        print(f"  - {extended_csv_path}")
        print(f"  - {ratings_json_path}")
        
    except Exception as e:
        logger.error(f"Error in enrichment process: {e}")
        print(f"❌ Enrichment failed: {e}")
        raise

if __name__ == "__main__":
    main()