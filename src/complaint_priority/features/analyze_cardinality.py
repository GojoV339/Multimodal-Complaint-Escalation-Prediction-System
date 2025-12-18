import pandas as pd
from loguru import logger 
from pathlib import Path

def analyze_tabular_values():
    """
    reads data from the silver folder and iterates over 
    the columns and find the number of unique values , so 
    we can use this to choose the encoding methods.
    """
    
    silver_path = Path("data/silver/complaints_validated.parquet")
    
    tabular_cols = [
        'product', 'sub_product', 'issue',
        'company', 'state', 'submitted_via',
        'timely_response', 'consumer_disputed'
    ]
    
    logger.info(f"Analyzing column cardinality in {silver_path}...")
    
    unique_trackers = {col : set() for col in tabular_cols}
    
    df = pd.read_parquet(silver_path, columns=tabular_cols)
    
    print("\n" + "="*60)
    print(f"{'COLUMN':<20} | {'UNIQUE COUNT':<15} | {'SAMPLE VALUES'}")
    print("-" * 60)
    
    for col in tabular_cols:
        unique_vals = df[col].unique().tolist()
        count = len(unique_vals)
        
        sample = str(sorted([str(v) for v in unique_vals])[:3]) + "..."
        print(f"{col:<20} | {count:<15} | {sample}")
    
    print("="*60 + "\n")
    
if __name__ == "__main__":
    analyze_tabular_values()