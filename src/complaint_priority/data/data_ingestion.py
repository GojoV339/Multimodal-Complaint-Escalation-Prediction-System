# src/complaint_priority/data/data_ingestion.py

import pandas as pd
from loguru import logger
from pathlib import Path
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import os
import sys

# Import common utilities and Pydantic schemas
from complaint_priority.utils.common import read_yaml, create_directories
from complaint_priority.entity.data_entities import RawDataSchema, ValidatedDataSchema

CONFIG_PATH = Path("config/params.yaml")

def validate_chunk(df_chunk: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """
    Validates a single chunk of data against the RawDataSchema, performs essential 
    cleaning (narrative drop), and returns a cleaned DataFrame suitable for the Silver layer.

    This function is central to the memory-efficient processing pipeline.

    Args:
        df_chunk: The raw DataFrame chunk loaded from the Bronze layer.
        column_map: A dictionary mapping original column names to the clean 
                    Pydantic field names for internal consistency.

    Returns:
        A cleaned and validated DataFrame chunk.
    """
    
    # 1. Drop records where the core narrative is missing (Pre-Pydantic cleanup)
    # This is a critical data quality filter as the narrative is the multimodal input.
    initial_count = len(df_chunk)
    df_chunk.dropna(subset=['Consumer complaint narrative'], inplace=True)
    
    if len(df_chunk) < initial_count:
        logger.warning(
            f"Dropped {initial_count - len(df_chunk)} rows due to missing 'Consumer complaint narrative' in chunk."
        )

    # 2. Rename columns to match Pydantic field names
    # This aligns the DataFrame column headers with the internal model structure.
    df_chunk.rename(columns=column_map, inplace=True)
    
    # 3. Validation using Pydantic (Row-by-Row validation)
    validated_records = []
    invalid_count = 0
    
    for _, row in df_chunk.iterrows():
        try:
            # Pydantic validation: ensures types, checks for required fields, 
            # and runs custom validators (e.g., Timely_response check).
            validated_data = RawDataSchema(**row.to_dict())
            
            # Map the validated data to the clean Silver schema (ValidatedDataSchema) names
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
            # Increment the counter for invalid records and optionally log the error detail.
            invalid_count += 1
            # logger.debug(f"Row failed validation: {e}") 
            
    final_df = pd.DataFrame(validated_records)
    
    if invalid_count > 0:
        logger.warning(f"Dropped {invalid_count} rows due to Pydantic validation errors in chunk.")
    
    return final_df

def run_data_ingestion():
    """
    Main function to run the data ingestion pipeline (Bronze -> Silver).

    This pipeline reads the large raw dataset in memory-efficient chunks, 
    validates each chunk against the defined Pydantic schema, and incrementally 
    saves the cleaned data to the Silver layer as a single Parquet file using 
    the robust PyArrow.ParquetWriter.
    
    Raises:
        Exception: If any step of the pipeline fails.
    """
    logger.info("Starting Bronze to Silver data ingestion pipeline with chunking.")
    
    # Initialize writer outside try block so it can be accessed in finally block
    writer = None 
    total_rows_written = 0
    
    try:
        # 1. Read configuration
        config = read_yaml(CONFIG_PATH)
        paths = config['data_paths']
        data_schema = config['data_schema']
        
        raw_data_path = Path(paths['raw_data_dir']) / paths['raw_data_filename']
        silver_dir = paths['silver_data_dir']
        silver_data_path = Path(silver_dir) / paths['silver_data_filename']
        
        # Extract dtypes and column names for the Pandas Reader for memory optimization
        dtype_map = data_schema['usecols_and_dtypes']
        columns_to_load = list(dtype_map.keys())
        chunk_size = config['data_source']['chunk_size']

        # Determine column mapping for Pydantic validation (Original name -> Pythonic name)
        pydantic_column_map = {field.alias or name: name for name, field in RawDataSchema.__fields__.items()}
        
        # 2. Initialize output settings
        create_directories([silver_dir])
        
        # 3. Use pandas.read_csv with chunksize to iterate over the massive file
        logger.info(f"Reading data in chunks of {chunk_size} rows...")
        
        reader = pd.read_csv(
            raw_data_path, 
            compression='zip', 
            usecols=columns_to_load, 
            dtype=dtype_map,
            chunksize=chunk_size,
            iterator=True 
        )
        
        # Iterate through the chunks returned by the reader
        for i, df_chunk in enumerate(reader):
            logger.info(f"Processing chunk {i+1}...")
            
            # 4. Validate and clean the chunk
            df_validated = validate_chunk(df_chunk, pydantic_column_map)
            
            # 5. Save the validated chunk using PyArrow.ParquetWriter
            
            # Convert Pandas DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df_validated, preserve_index=False)
            
            if i == 0:
                # First chunk: Initialize the Parquet Writer and set the schema
                writer = pq.ParquetWriter(silver_data_path, table.schema)
                logger.info(f"Initialized PyArrow Parquet Writer at {silver_data_path}")
            
            # Write the current chunk (table)
            writer.write_table(table)
            
            total_rows_written += len(df_validated)
            logger.info(f"Chunk {i+1} processed. Valid rows written: {len(df_validated)}. Total rows: {total_rows_written}")

        logger.success(f"Ingestion complete. Total validated rows saved to Silver layer: {total_rows_written}")
        
    except Exception as e:
        logger.error(f"Data Ingestion Pipeline Failed: {e}")
        # Cleanup: If the pipeline fails, remove the partial Parquet file to avoid corrupted data.
        if 'total_rows_written' in locals() and total_rows_written == 0 and Path(silver_data_path).exists():
            os.remove(silver_data_path)
            logger.warning(f"Cleaned up failed partial file: {silver_data_path}")
        raise e
    
    finally:
        # 6. CRITICAL: Close the Parquet Writer to finalize the file structure
        if writer:
            writer.close()
            logger.info("Closed ParquetWriter connection, file is finalized.")

if __name__ == '__main__':
    run_data_ingestion()