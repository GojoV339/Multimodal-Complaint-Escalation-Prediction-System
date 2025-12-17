from pydantic import BaseModel, Field, validator
from typing import Optional



from pydantic import BaseModel, Field, validator
from typing import Optional

class RawDataSchema(BaseModel):
    Complaint_ID: int = Field(alias="Complaint ID")
    Date_received: str = Field(alias="Date received")
    Product: str 
    Sub_product: Optional[str] = Field(alias="Sub-product") 
    Issue: str
    Consumer_complaint_narrative: str = Field(alias="Consumer complaint narrative")
    Company_response_to_consumer: str = Field(alias="Company response to consumer")
    Timely_response: str = Field(alias="Timely response?")
    Consumer_disputed: Optional[str] = Field(alias="Consumer disputed?") 
    Company: str
    State: Optional[str] 
    Submitted_via: str = Field(alias="Submitted via")

    @validator("Consumer_complaint_narrative")
    def check_narrative_length(cls, v):
        if len(str(v)) < 50: 
            raise ValueError("Narrative too short")
        return v
class ValidatedDataSchema(BaseModel):
    complaint_id: int
    date_received: str
    product: str
    sub_product: str
    issue: str
    narrative: str
    company_response: str
    timely_response: str
    consumer_disputed: str
    company: str
    state: str
    submitted_via: str
    
    
