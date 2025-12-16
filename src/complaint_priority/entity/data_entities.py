from pydantic import BaseModel, validator, Field
from typing import Optional
from datetime import datetime

# Constraints for Categorical Fields 
TIMELY_RESPONSE_CHOICES = ["Yes","No"]

class RawDataSchema(BaseModel):
    """
    Defines the expected schema for the raw complaint data 
    reads from the bronze layer and enforces constraints on the raw data
    """
    
    
    # Core feilds for the ML task
    Complaint_ID : int = Field(alias = "Complaint ID")
    Date_received : datetime = Field(alias = "Date received")
    Product : str
    Sub_product: Optional[str] = None
    Issue: str
    Consumer_complaint_narrative : Optional[str] = Field(alias = "Consumer complaint narrative")
    
    
    # Fields critical for priorititzation/labeling
    comapany_response_to_consumer : str = Field(alias = "Company response to consumer")
    Timely_response: str = Field(alias = "Timely response?")
    consumer_dispute : Optional[str] = Field(alias = "Consumer disputed?")
    
    # Metadata Fields
    company : str
    state : Optional[str] = None
    Submitted_via : str = Field(alias = "Submitted via")
    
    class Config:
        """
        Configuration to handle column names with spaces from the CSV
        """
        allow_population_by_field_name = True
        
    @validator("Timely_response")
    def check_timely_response_value(cls,value):
        if value not in TIMELY_RESPONSE_CHOICES:
            raise ValueError(f"Timely response must be one of {TIMELY_RESPONSE_CHOICES}, but got '{value}'")    
        return value
    
    
class ValidationDataSchema(BaseModel):
    """
    Defines the schema for clean and validated data 
    Saved to the Silver Layer
    """
    
    complaint_id : int
    date_received : datetime
    product : str
    sub_product : Optional[str]
    issue : str
    narrative : str 
    
    company_response : str
    timely_response : str
    consumer_disputed : Optional[str]
    
    company : str
    state : Optional[str]
    submitted_via : str