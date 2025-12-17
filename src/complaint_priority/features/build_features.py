import pandas as pd
from loguru import logger
from pathlib import Path
from complaint_priority.utils.common import read_yaml, create_directories
import os

CONFIG_PATH = Path("config/params.yaml")

def apply_labeling_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes DataFrame as input and return out a DataFrame.
    
    Applies the logic : escalation_risk = 1 if(disputed or not timely)
    
    Dispured:
        Indicates whether the consumer disagreed with the company's response 
        to the computer 
        
    Source column : "Consumer disputed?"
    values : 
        - Yes -> Consumer was dissatisfied with the resolution (high escalation signal)
        - No -> Consumer accepted the company response
        
    A disputed complaint often require senior support, complaince review,or regulatory attention,
    making it a strong indictor of escalation risk.
    
    Not Timely:
        Indicates whether the company failed to respond to the complaint
        within the required service-level agreement (SLA) timeframe.

    Source column: "Timely response?"
    Values:
         "No"  → Response was delayed (not timely)
         "Yes" → Response was within SLA

    Delayed responses increase customer dissatisfaction and regulatory risk,
    and are therefore treated as a key escalation signal.
    
    """
    
    logger.info("Applying labeling rules to create target variable `escalation_risk`...")
    
    """
    1 : if Consumer disputed is Yes or Timely response is No 
    0 : otherwise
    """
    df['escalation_risk'] = (
        (df['consumer_disputed'] == 'Yes') |
        (df['timely_response'] == 'No')
    ).astype(int)
    
    balance = df['escalation_risk'].value_counts(normalize=True) * 100
    logger.info(f"Label Distribution: \n{balance}")
    
    return df

def preprocess_narratives(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes input a DataFrame and returns out a cleaned DataFrame
    
    Light Cleaning of the text data for the NLP Component.
    lower cases the narrative and removes the extra whitespace.
    """
    logger.info("Performing basic text processing on narratives...")
    
    df['narrative'] = df['narrative'].astype(str).str.lower()
    df['narrative'] = df['narrative'].str.replace(r'\s+',' ',regex=True).str.strip()
    
    return df

def build_gold_layer():
    log_file = "logs/build_features.log"
    os.makedirs("logs", exist_ok=True)
    logger.add(log_file, rotation="10 MB", retention="5 days", level="INFO")
    
    config = read_yaml(CONFIG_PATH)
    
    silver_path = Path(config['data_paths']['silver_data_dir']) / config['data_paths']['silver_data_filename']
    gold_dir = Path("data/gold")
    gold_path = gold_dir / "complaints_labeled.parquet"
    
    
    create_directories([gold_dir])
    
    logger.info(f"Loading Silver data from {silver_path}...")
    df = pd.read_parquet(silver_path)
    
    df = apply_labeling_rules(df)
    df = preprocess_narratives(df)
    logger.info(f"Saving gold dataset to {gold_path}...")
    df.to_parquet(gold_path, index = False)
    
    logger.success(f"Gold layer built successfully with {len(df)} labeled records.")
    
    
if __name__=="__main__":
    build_gold_layer()
    