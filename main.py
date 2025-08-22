import logging
import json
from processors.data_processor import MovieDataProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movie_processing.log'),
        logging.StreamHandler()
    ]
)

def main():
    """Main function demonstrating the complete data processing pipeline."""
    
    logger = logging.getLogger(__name__)
    processor = MovieDataProcessor()
    
    try:
        print("🎬 Movie Data Processing System")
        print("=" * 50)
        

        # Step 1: Load data
        print("📁 Loading CSV data...")
        movies_df = processor.read_csv_data('dataset/movies_main_enriched.csv', 'dataset/movie_extended_enriched.csv')
        
        print("📊 Loading JSON ratings...")
        ratings_df = processor.read_json_data('dataset/ratings.json')
        
        # Step 2: Process data
        print("🔄 Processing movies with ISO mapping...")
        processed_movies = processor.process_movies()
        
        print("⭐ Processing ratings...")
        processed_ratings = processor.process_ratings()
        
        # Step 3: Create merged dataset (NEW!)
        print("🔗 Creating merged dataset...")
        merged_df = processor.create_merged_dataset()
        
        # Step 4: Save all datasets
        print("💾 Saving datasets...")
        saved_files = processor.save_datasets('./output')
        
        print("✅ Files saved:")
        for dataset_type, file_path in saved_files.items():
            print(f"  - {dataset_type}: {file_path}")
        
        # Step 5: Display sample output
        print("📋 Merged Dataset Sample:")
        print(merged_df.head())
        print(f"\nDataset Info:")
        print(f"- Total rows: {len(merged_df)}")
        print(f"- Total columns: {len(merged_df.columns)}")
        print(f"- Movies with ratings: {len(merged_df[merged_df['total_ratings'] > 0])}")
        
        print("System initialized successfully!")
        print("\n📋 Usage Instructions:")
        print("1. Install requirements: pip install pandas pycountry langcodes")
        print("2. Create a MovieDataProcessor instance")
        print("3. Load your data files:")
        print("   - processor.read_csv_data('movie_main.csv', 'movies_extended.csv')")
        print("   - processor.read_json_data('ratings.json')")
        print("4. Process the data:")
        print("   - processor.process_movies()")
        print("   - processor.process_ratings()")
        print("5. Create merged dataset: processor.create_merged_dataset()")
        print("6. Save everything: processor.save_datasets('./output')")
        print("7. Analyze: processor.analyze_data()")
        
        print("\n📊 Expected Output Files:")
        print("  - cleaned_movies.csv (movies only)")
        print("  - cleaned_ratings.csv (ratings only)")  
        print("  - merged_movies_ratings.csv (everything combined) ⭐")
        
        print("\n🔧 Merged Dataset Structure:")
        print("Columns: id, title, release_date, genres, production_companies,")
        print("         production_countries, spoken_languages, budget, revenue,")
        print("         avg_rating, total_ratings, std_dev, last_rated")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()