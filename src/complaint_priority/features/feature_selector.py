import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from loguru import logger
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from category_encoders import TargetEncoder
from sklearn.preprocessing import OneHotEncoder


def process_features_lazy():
    """
    Builds a multimodal feature matrix using lazy (chunked) processing.
    
    This function reads validated complaint data from the silver layer and 
    constructs a unified feature representation consisting of:
        - Dense text embeddings from complaint narratives
        - Target-encoded categorical features for high-cardinality columns
        - One-hot encoded features for low/medium-cardinality columns 
        
    The feature matrix is written incrementally to disk using a NumPy
    memory-mapped file to minimize RAM usage and support large datasets.
    
    Steps performed:
    1) Load Silver-layer data
    2) Fit Categorical encoders using a temporary escalation-based label
    3) Encode text using a pre-trained sentence transformer
    4) Process data in chunks and fuse all features
    5) Persist the final feature matrix and fitted encoders 
    
    Artifacts produced :
        - data/gold/feature_matrix.dat
        - models/artifacts /target_encoder.joblib
        - models/artifacts/onehot_encoder.joblib
        
    This function is intended to be run as a standalone preprocessing step 
    before model training and evaluation.
    """
    
    silver_path = Path("data/silver/complaints_validated.parquet")
    output_dir = Path("models/artifacts")
    output_dir.mkdir(parents = True, exist_ok = True)
    
    model = SentenceTransformer('all-MiniLM-L6-v2')
    chunk_size = 100
    
    logger.info("Fitting Tabular Encoders...")
    df_full = pd.read_parquet(silver_path)
    
    df_full['temp_label'] = ((df_full['consumer_disputed'] == 'Yes') | 
                             (df_full['timely_response'] == 'No')).astype(int)
    
    te = TargetEncoder(cols = ['company','issue','sub_product'])
    te.fit(df_full[['company','issue','sub_product']], df_full['temp_label'])
    
    ohe = OneHotEncoder(sparse_output = False, handle_unknown = 'ignore')
    ohe.fit(df_full[['product','state']])
    
    num_rows = len(df_full)
    # We create a memory-mapped file for the final features to keep RAM low
    # Total features: 384 (text) + 3 (target encoded) + ~83 (one-hot) = ~470
    total_feature_dim = 384 + 3 + len(ohe.get_feature_names_out())
    final_matrix = np.memmap('data/gold/feature_matrix.dat', dtype='float32', 
                             mode='w+', shape=(num_rows, total_feature_dim))
    
    logger.info(f"Starting chunked processing for {num_rows} rows...")
    
    for start_idx in tqdm(range(0, num_rows, chunk_size)):
        end_idx = min(start_idx + chunk_size, num_rows)
        chunk = df_full.iloc[start_idx : end_idx].copy()
        
        # Transform Tabular
        tab_te = te.transform(chunk[['company', 'issue', 'sub_product']]).values
        tab_ohe = ohe.transform(chunk[['product', 'state']])
        
        # Transform Text (Sentence Transformer)
        text_embeds = model.encode(chunk['narrative'].tolist(), convert_to_numpy=True)
        
        # Fuse & Store in Memmap
        combined = np.hstack([tab_te, tab_ohe, text_embeds])
        final_matrix[start_idx:end_idx, :] = combined
        
    final_matrix.flush()
    joblib.dump(te, output_dir / "target_encoder.joblib")
    joblib.dump(ohe, output_dir / "onehot_encoder.joblib")
    
    logger.success(f"Feature matrix saved to disk (Memmap). Shape: {final_matrix.shape}")

if __name__ == "__main__":
    process_features_lazy()