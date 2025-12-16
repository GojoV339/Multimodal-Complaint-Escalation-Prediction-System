# src/complaint_priority/entity/data_entities.py

from pydantic import BaseModel, validator, Field
from typing import Optional
from datetime import datetime
import pandas as pd

# --- Constants for Categorical Fields ---
# Define expected values for validation consistency
TIMELY_RESPONSE_CHOICES = ["Yes", "No"] 
CONSUMER_DISPUTED_CHOICES = ["Yes", "No", "N/A"]

# ---------------------------------------------------------------------
# 1. RAW DATA SCHEMA (Data read from Bronze)
# ---------------------------------------------------------------------

class RawDataSchema(BaseModel):
    """
    Defines the expected schema for the raw complaint data read from the Bronze layer.
    Uses Field aliases to map CSV column names (with spaces) to Pythonic names.
    """
    
    # Core fields for the ML task
    Complaint_ID: int = Field(alias="Complaint ID")
    Date_received: datetime = Field(alias="Date received") # Pydantic will attempt to convert str to datetime
    Product: str
    Sub_product: Optional[str] = Field(default=None, alias="Sub-product")
    Issue: str
    Consumer_complaint_narrative: str = Field(alias="Consumer complaint narrative") # Assumed required after dropna()
    
    # Fields critical for later prioritization/labeling
    Company_response_to_consumer: str = Field(alias="Company response to consumer")
    Timely_response: str = Field(alias="Timely response?")
    Consumer_disputed: Optional[str] = Field(default=None, alias="Consumer disputed?")
    
    # Metadata fields
    Company: str
    State: Optional[str] = Field(default=None)
    Submitted_via: str = Field(alias="Submitted via")
    
    # Configuration to handle column names with spaces from the CSV
    class Config:
        allow_population_by_field_name = True
        
    # --- Custom Validators for Integrity/Consistency Checks ---

    @validator("Timely_response")
    def check_timely_response_value(cls, value):
        """Ensures the timely response field is one of the expected categorical values."""
        if value not in TIMELY_RESPONSE_CHOICES:
            raise ValueError(f"Timely response must be one of {TIMELY_RESPONSE_CHOICES}, but got '{value}'")
        return value

    @validator("Consumer_disputed")
    def check_consumer_disputed_value(cls, value):
        """Ensures the consumer disputed field is one of the expected categorical values."""
        if value is not None and value not in CONSUMER_DISPUTED_CHOICES:
            raise ValueError(f"Consumer disputed must be one of {CONSUMER_DISPUTED_CHOICES}, but got '{value}'")
        return value


# ---------------------------------------------------------------------
# 2. VALIDATED DATA SCHEMA (Data saved to Silver)
# ---------------------------------------------------------------------

class ValidatedDataSchema(BaseModel):
    """
    Defines the final, cleaned, and validated schema for data saved to the Silver layer.
    Note: Column names are standardized (no spaces) here.
    """
    
    complaint_id: int
    date_received: datetime
    product: str
    sub_product: Optional[str]
    issue: str
    narrative: str
    
    company_response: str
    timely_response: str
    consumer_disputed: Optional[str]
    
    company: str
    state: Optional[str]
    submitted_via: str

# ---------------------------------------------------------------------
# 3. FEATURED DATA SCHEMA (Data saved to Gold) - Placeholder for next stage
# ---------------------------------------------------------------------

class FeaturedDataSchema(ValidatedDataSchema):
    """
    Extends the validated schema for data ready for the ML model (Gold layer).
    """
    # Target Variable (0 or 1)
    escalation_risk: int
    
    # Feature Placeholder (e.g., length of narrative, number of keywords, TF-IDF, embeddings)
    # This will be filled in the 'build_features' step.
    narrative_length: int
    
    # Placeholder for NLP embeddings (e.g., array of floats)
    # We will represent this as a string or list for now, but in practice, 
    # it would be handled as a Pandas series of vectors or a separate matrix.
    # embedding_vector: list[float] # Placeholder for the final structured vector
    
    # Validator to ensure escalation_risk is binary
    @validator("escalation_risk")
    def check_escalation_risk_value(cls, value):
        if value not in [0, 1]:
            raise ValueError("escalation_risk must be binary (0 or 1).")
        return value