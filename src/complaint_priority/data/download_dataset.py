import requests
import os
from loguru import logger
from pathlib import Path
from tqdm import tqdm
from complaint_priority.utils.common import read_yaml, create_directories


CONFIG_PATH = Path("config/params.yaml")

def download_data():
    """
    Downloads the raw dataset from the url and 
    saves it to the bronze folder
    """
    logger.info("Starting data download process...")
    
    try:
        # 1. Reading configuration
        config = read_yaml(CONFIG_PATH)
        download_url = config['data_source']['download_url']
        raw_dir = config['data_paths']['raw_data_dir']
        raw_filename = config['data_paths']['raw_data_filename']
        
        output_path = Path(raw_dir)/raw_filename
        
        # create target directories 
        create_directories([raw_dir])
        
        if output_path.exists():
            logger.info(f"File Already Exists {output_path}. Skipping download")
            return
        
        logger.info(f"Attempting to download data from : {download_url}")
        
        response = requests.get(download_url, stream = True)
        response.raise_for_status() # Raising exception for bad status like (4xx or 5xx)
        
        total_size = int(response.headers.get('content-length',0))
        block_size = 1024
        
        with open(output_path, 'wb') as file:
            with tqdm(total=total_size, unit='iB', unit_scale=True, desc = "Downloading") as bar:
                for data in response.iter_content(block_size):
                    bar.update(len(data))
                    file.write(data)
                    
                    
        logger.success(f"Successfully downloaded and saved raw data to: {output_path}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network or Download Error: {e}") 
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred during data download: {e}")
        raise e
    

if __name__ == "__main__":
    download_data() # for local testing        