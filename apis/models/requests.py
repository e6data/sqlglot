from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List


class TranspileOptions(BaseModel):
    """Options for transpilation"""
    pretty_print: bool = Field(default=True, description="Format output with proper indentation")
    table_alias_qualification: bool = Field(default=False, description="Enable table alias qualification")
    use_two_phase_qualification_scheme: bool = Field(default=False, description="Use two-phase qualification")
    skip_e6_transpilation: bool = Field(default=False, description="Skip E6 transpilation, only transform catalog.schema")


class TranspileRequest(BaseModel):
    """Request for single query transpilation"""
    query: str = Field(..., description="SQL query to transpile", min_length=1)
    source_dialect: str = Field(..., description="Source SQL dialect", alias="from_sql")
    target_dialect: str = Field(default="e6", description="Target SQL dialect", alias="to_sql")
    query_id: Optional[str] = Field(default="NO_ID_MENTIONED", description="Optional query identifier")
    options: Optional[TranspileOptions] = Field(default_factory=TranspileOptions)

    class Config:
        populate_by_name = True


class AnalyzeRequest(BaseModel):
    """Request for single query analysis"""
    query: str = Field(..., description="SQL query to analyze", min_length=1)
    source_dialect: str = Field(..., description="Source SQL dialect", alias="from_sql")
    target_dialect: str = Field(default="e6", description="Target SQL dialect", alias="to_sql")
    query_id: Optional[str] = Field(default="NO_ID_MENTIONED", description="Optional query identifier")
    options: Optional[TranspileOptions] = Field(default_factory=TranspileOptions)

    class Config:
        populate_by_name = True


class BatchQueryItem(BaseModel):
    """Single query item in a batch request"""
    id: str = Field(..., description="Unique identifier for this query in the batch")
    query: str = Field(..., description="SQL query", min_length=1)


class BatchTranspileRequest(BaseModel):
    """Request for batch transpilation"""
    queries: List[BatchQueryItem] = Field(..., description="List of queries to transpile", min_items=1)
    source_dialect: str = Field(..., description="Source SQL dialect", alias="from_sql")
    target_dialect: str = Field(default="e6", description="Target SQL dialect", alias="to_sql")
    options: Optional[TranspileOptions] = Field(default_factory=TranspileOptions)
    stop_on_error: bool = Field(default=False, description="Stop processing on first error")

    class Config:
        populate_by_name = True


class BatchAnalyzeRequest(BaseModel):
    """Request for batch analysis"""
    queries: List[BatchQueryItem] = Field(..., description="List of queries to analyze", min_items=1)
    source_dialect: str = Field(..., description="Source SQL dialect", alias="from_sql")
    target_dialect: str = Field(default="e6", description="Target SQL dialect", alias="to_sql")
    options: Optional[TranspileOptions] = Field(default_factory=TranspileOptions)
    stop_on_error: bool = Field(default=False, description="Stop processing on first error")

    class Config:
        populate_by_name = True
