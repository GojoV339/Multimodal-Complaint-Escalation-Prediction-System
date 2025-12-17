# src/complaint_priority/data/data_ingestion.py

import pandas as pd
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm
from complaint_priority.utils.common import read_yaml, create_directories

CONFIG_PATH = Path("config/params.yaml")

def run_data_ingestion():
    """
    Reads the Params.yaml file and gets the required dataset paths 
    column mapped because python doesn't allow column names with spaces 
    and using simple checks we have reduced the dataset size to only 2 lakhs 
    and saved it as a parquet file.
    """
    logger.info("Starting High-Quality Search & Ingestion...")
    config = read_yaml(CONFIG_PATH)
    
    
    target_rows = config['data_source']['target_rows']
    chunk_size = config['data_source']['chunk_size']
    raw_path = Path(config['data_paths']['raw_data_dir']) / config['data_paths']['raw_data_filename']
    silver_path = Path(config['data_paths']['silver_data_dir']) / config['data_paths']['silver_data_filename']
    create_directories([config['data_paths']['silver_data_dir']])


    column_mapping = {
        'Complaint ID': 'complaint_id',
        'Date received': 'date_received',
        'Product': 'product',
        'Sub-product': 'sub_product',
        'Issue': 'issue',
        'Consumer complaint narrative': 'narrative',
        'Company response to consumer': 'company_response',
        'Timely response?': 'timely_response',
        'Consumer disputed?': 'consumer_disputed',
        'Company': 'company',
        'State': 'state',
        'Submitted via': 'submitted_via'
    }

    total_saved = 0
    writer = None
    pbar = tqdm(total=target_rows, desc="Searching for Narratives")


    reader = pd.read_csv(
        raw_path, 
        compression='zip', 
        usecols=list(column_mapping.keys()), 
        chunksize=chunk_size,
        low_memory=False
    )

    for df_chunk in reader:
        if total_saved >= target_rows:
            break

        valid_chunk = df_chunk[df_chunk['Consumer complaint narrative'].notna()].copy()
        valid_chunk = valid_chunk[valid_chunk['Consumer complaint narrative'].str.len() > 50]

        if not valid_chunk.empty:
            valid_chunk = valid_chunk.fillna("N/A")

            valid_chunk.rename(columns=column_mapping, inplace=True)

            rows_to_add = min(len(valid_chunk), target_rows - total_saved)
            df_to_save = valid_chunk.head(rows_to_add)

            table = pa.Table.from_pandas(df_to_save, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(silver_path, table.schema)
            
            writer.write_table(table)
            
            total_saved += rows_to_add
            pbar.update(rows_to_add)

    pbar.close()
    if writer:
        writer.close()
    
    if total_saved < target_rows:
        logger.warning(f"Only found {total_saved} rows with narratives in the entire file.")
    else:
        logger.success(f"Successfully saved {total_saved} rows to Silver Layer.")

if __name__ == "__main__":
    run_data_ingestion()