import pandas as pd
import os
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('etl_pipeline')

class ETLPipeline:
    def __init__(self, source_path, destination_path):
        """
        Initialize the ETL pipeline with source and destination paths.
        
        Args:
            source_path (str): Path to the source data file
            destination_path (str): Path where transformed data will be saved
        """
        self.source_path = source_path
        self.destination_path = destination_path
        logger.info(f"ETL Pipeline initialized: {source_path} -> {destination_path}")
        
    def extract(self):
        """
        Extract data from the source file.
        Supports CSV, JSON, and Excel files.
        
        Returns:
            pandas.DataFrame: Extracted data
        """
        logger.info(f"Extracting data from {self.source_path}")
        
        file_extension = os.path.splitext(self.source_path)[1].lower()
        
        try:
            if file_extension == '.csv':
                data = pd.read_csv(self.source_path)
            elif file_extension == '.json':
                data = pd.read_json(self.source_path)
            elif file_extension in ['.xlsx', '.xls']:
                data = pd.read_excel(self.source_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
                
            logger.info(f"Successfully extracted {len(data)} records")
            return data
        
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            raise
    
    def transform(self, data):
        """
        Transform the extracted data.
        
        Args:
            data (pandas.DataFrame): Data to transform
            
        Returns:
            pandas.DataFrame: Transformed data
        """
        logger.info("Transforming data")
        
        try:
            # Example transformations (customize based on your needs)
            
            # 1. Drop any rows with missing values
            data = data.dropna()
            
            # 2. Convert column names to lowercase
            data.columns = [col.lower() for col in data.columns]
            
            # 3. Add a timestamp column
            data['etl_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 4. Add an ID column if it doesn't exist
            if 'id' not in data.columns:
                data['id'] = range(1, len(data) + 1)
                
            logger.info(f"Transformation complete. {len(data)} records after transformation")
            return data
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def load(self, data):
        """
        Load the transformed data to the destination.
        
        Args:
            data (pandas.DataFrame): Data to load
            
        Returns:
            bool: True if successful
        """
        logger.info(f"Loading data to {self.destination_path}")
        
        try:
            file_extension = os.path.splitext(self.destination_path)[1].lower()
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.destination_path)), exist_ok=True)
            
            if file_extension == '.csv':
                data.to_csv(self.destination_path, index=False)
            elif file_extension == '.json':
                data.to_json(self.destination_path, orient='records')
            elif file_extension in ['.xlsx', '.xls']:
                data.to_excel(self.destination_path, index=False)
            else:
                raise ValueError(f"Unsupported destination format: {file_extension}")
                
            logger.info(f"Successfully loaded {len(data)} records to {self.destination_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error during loading: {str(e)}")
            raise
            
    def run(self):
        """Run the complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline")
            
            # Extract
            raw_data = self.extract()
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            self.load(transformed_data)
            
            logger.info("ETL pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # Sample data files (replace with your actual paths)
    sample_source = "data/source/my_data.csv"
    sample_destination = "data/destination/processed_data.csv"
    
    # Create sample data if it doesn't exist
    # if not os.path.exists(sample_source):
    #     os.makedirs(os.path.dirname(os.path.abspath(sample_source)), exist_ok=True)
        
    #     # Create sample data
    #     sample_df = pd.DataFrame({
    #         'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown'],
    #         'age': [28, 34, 42, 31],
    #         'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com'],
    #         'active': [True, True, False, True]
    #     })
        
    #     sample_df.to_csv(sample_source, index=False)
    #     print(f"Created sample data at {sample_source}")
    
    # Run the ETL pipeline
    pipeline = ETLPipeline(sample_source, sample_destination)
    result = pipeline.run()
    
    if result:
        print("ETL process completed successfully.")
    else:
        print("ETL process failed. Check logs for details.")