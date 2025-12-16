import pandas as pd
from loguru import logger
from pathlib import Path
import os
import io

from complaint_priority.utils.common import read_yaml, create_directories
from complaint_priority.entity.data_entities import RawDataSchema, ValidationDataSchema


CONFIG_PATH = Path("config/params.yaml")

def load_data(file_path : Path) -> pd.DataFrame:
    """
    Loada data from the specified path , handling .zip compression
    
    Args: 
        file_path : The path to the data file (expected to be .csv.zip).
        
    Returns:
        A pandas Data Frame
    """
    logger.info(f"Loading data fromm {file_path}...")
    try:
        df = pd.read_csv(file_path, low_memory=False) 
        """
        low_memory = False 
        by default it is True , process the file in smaller chunks to conserve memeory,
        we are loading the entire dataset into memory , to check the data types
        """
        logger.info(f"Data loaded successfully. Initial shape : {df.shape}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found at : {file_path}")
    except Exception as e:
        logger.error(f"Error loading data from : {file_path} : {e}")
        raise
    

def validate_and_clean_data(df:pd.DataFrame) -> pd.DataFrame:
    """
    Validates the Dataframe against the RawDataSchema, performs basic cleaning,
    and returns a DataFrame ready for the Silver layer.
    
    Args:
        df : The raw DataFrame loaded from the Bronze layer.
    
    Returns:
        A cleaned and validated DataFrame.
    """
    logger.info("Starting data validation and cleaning process...")
    
    """
    Steps : 
        1. Rename columns to match Pydantic field aliases for easier validation
        2. Drop records where the core narrative is missing (Critical loss)
        3. Validation using Pydantic (Row-by-Row validation)
    """
    column_mapping = {field.alias or name : name for name, field in RawDataSchema.__fields__.items()}
    df.rename(columns={alias : name for alias,name in column_mapping.items()}, inplace=True)
    
    initial_count = len(df)
    df.dropna(subset=['Consumer_complaint_narrative'], inplace=True)
    dropped_count = initial_count - len(df)
    logger.warning(
        f"Dropped {dropped_count} rows ({dropped_count/initial_count:.2%}) due to missing 'Consumer_complaint_narrative'."
    )
    
    validated_records = []
    invalid_count = 0
    
    """
    We iterate over the DataFrame rows to apply the Pydantic schema validation 
    
    Slow process but very precise for structured validatoin.
    """
    for index,row in df.iterrows():
        try:
            validated_data = RawDataSchema(**row.to_dict())
            record = validated_data.dict()
            
            clean_record = {
                'complaint_id': record['Complaint_ID'],
                'date_received': record['Date_received'],
                'product': record['Product'],
                'sub_product': record['Sub_product'],
                'issue': record['Issue'],
                'narrative': record['Consumer_complaint_narrative'],
                'company_response': record['Company_response_to_consumer'],
                'timely_response': record['Timely_response'],
                'consumer_disputed': record['Consumer_disputed'],
                'company': record['Company'],
                'state': record['State'],
                'submitted_via': record['Submitted_via'],
            }
            validated_records.append(clean_record)
            
        except Exception as e:
            # log the error and drop the row
            invalid_count += 1
            # logger.debug(f"Row {index} failed validatoin : {e}")  # Debugging
            
    final_df = pd.DataFrame(validated_records)
    
    logger.info(f"Total invalid rows dropped during Pydantic validation: {invalid_count}")
    logger.success(f"Validation complete. Final cleaned data shape: {final_df.shape}")
    
    ValidationDataSchema.parse_obj(final_df.iloc[0].to_dict())
    return final_df

def run_data_ingestion():
    """
    Takes the data from bronze layer and cleans it and stores it in silver layer
    """
    logger.info("Starting Bronze to Silver data ingestion pipeline.")
    try:
        config = read_yaml(CONFIG_PATH)
        raw_dir = config['data_paths']['raw_data_dir']
        raw_filename = config['data_paths']['raw_data_filename']
        raw_data_path = Path(raw_dir)/raw_filename
        
        silver_dir = config['data_paths']['raw_data_dir'].replace('bronze','silver')
        silver_filename = 'complaints_validated.parquet'
        silver_data_path = Path(silver_dir) / silver_filename
        
        df_raw = load_data(raw_data_path)
        df_silver = validate_and_clean_data(df_raw)
        create_directories([silver_dir])
        df_silver.to_parquet(silver_data_path, index = False)
        logger.success(f"Validated data saved successfully to Silver layer : {silver_data_path}")
    except Exception as e:
        logger.error(f"Data Ingestion Pipeline Failed: {e}")
        raise e
    
if __name__ == "__main__":
    run_data_ingestion()
    
    
    
            