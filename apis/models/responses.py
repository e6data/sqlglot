from pydantic import BaseModel, Field
from typing import Optional, List, Set, Dict, Any
from enum import Enum


class QueryStatus(str, Enum):
    """Status of query processing"""
    SUCCESS = "success"
    ERROR = "error"


class TranspileResponse(BaseModel):
    """Response for single query transpilation"""
    transpiled_query: str = Field(..., description="Transpiled SQL query")
    source_dialect: str = Field(..., description="Source SQL dialect")
    target_dialect: str = Field(..., description="Target SQL dialect")
    query_id: Optional[str] = Field(None, description="Query identifier if provided")


class FunctionAnalysis(BaseModel):
    """Analysis of functions in query"""
    supported: List[str] = Field(default_factory=list, description="Functions supported in target dialect")
    unsupported: List[str] = Field(default_factory=list, description="Functions not supported in target dialect")


class QueryMetadata(BaseModel):
    """Metadata extracted from query"""
    tables: List[str] = Field(default_factory=list, description="Tables referenced in query")
    joins: List[List[Any]] = Field(default_factory=list, description="Join information as nested structure")
    ctes: List[str] = Field(default_factory=list, description="Common Table Expressions")
    subqueries: List[str] = Field(default_factory=list, description="Subqueries found")
    udfs: List[str] = Field(default_factory=list, description="User-defined functions")
    schemas: List[str] = Field(default_factory=list, description="Schemas/databases referenced in query")


class TimingInfo(BaseModel):
    """Timing information for different phases"""
    total_ms: float = Field(..., description="Total time in milliseconds")
    parsing_ms: Optional[float] = Field(None, description="Time to parse source query")
    function_analysis_ms: Optional[float] = Field(None, description="Time to analyze functions")
    metadata_extraction_ms: Optional[float] = Field(None, description="Time to extract metadata")
    transpilation_ms: Optional[float] = Field(None, description="Time to transpile query")
    post_analysis_ms: Optional[float] = Field(None, description="Time to analyze transpiled query")


class AnalyzeResponse(BaseModel):
    """Response for single query analysis"""
    transpiled_query: str = Field(..., description="Transpiled SQL query")
    source_dialect: str = Field(..., description="Source SQL dialect")
    target_dialect: str = Field(..., description="Target SQL dialect")
    query_id: Optional[str] = Field(None, description="Query identifier if provided")
    executable: bool = Field(..., description="Whether query is executable on target dialect")
    functions: FunctionAnalysis = Field(..., description="Function compatibility analysis")
    metadata: QueryMetadata = Field(..., description="Query structure metadata")
    source_ast: Optional[Dict[str, Any]] = Field(None, description="SQLGlot AST of source query")
    transpiled_ast: Optional[Dict[str, Any]] = Field(None, description="SQLGlot AST of transpiled query")
    timing: Optional[TimingInfo] = Field(None, description="Timing information for different phases")


class ErrorDetail(BaseModel):
    """Error details"""
    message: str = Field(..., description="Error message")
    code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")


class BatchResultItem(BaseModel):
    """Single result item in batch response"""
    id: str = Field(..., description="Query identifier from request")
    status: QueryStatus = Field(..., description="Processing status")
    transpiled_query: Optional[str] = Field(None, description="Transpiled query if successful")
    error: Optional[ErrorDetail] = Field(None, description="Error details if failed")


class BatchAnalysisResultItem(BaseModel):
    """Single analysis result item in batch response"""
    id: str = Field(..., description="Query identifier from request")
    status: QueryStatus = Field(..., description="Processing status")
    transpiled_query: Optional[str] = Field(None, description="Transpiled query if successful")
    executable: Optional[bool] = Field(None, description="Whether query is executable")
    functions: Optional[FunctionAnalysis] = Field(None, description="Function analysis")
    metadata: Optional[QueryMetadata] = Field(None, description="Query metadata")
    error: Optional[ErrorDetail] = Field(None, description="Error details if failed")


class BatchSummary(BaseModel):
    """Summary of batch processing"""
    total: int = Field(..., description="Total queries processed")
    succeeded: int = Field(..., description="Number of successful transpilations")
    failed: int = Field(..., description="Number of failed transpilations")
    duration_ms: float = Field(..., description="Total processing time in milliseconds")


class BatchTranspileResponse(BaseModel):
    """Response for batch transpilation"""
    results: List[BatchResultItem] = Field(..., description="Individual query results")
    summary: BatchSummary = Field(..., description="Batch processing summary")


class BatchAnalyzeResponse(BaseModel):
    """Response for batch analysis"""
    results: List[BatchAnalysisResultItem] = Field(..., description="Individual analysis results")
    summary: BatchSummary = Field(..., description="Batch processing summary")


class DialectInfo(BaseModel):
    """Information about a SQL dialect"""
    name: str = Field(..., description="Dialect name")
    supported_functions_count: int = Field(..., description="Number of supported functions")
    aliases: List[str] = Field(default_factory=list, description="Alternative names for this dialect")


class DialectsResponse(BaseModel):
    """Response listing all supported dialects"""
    dialects: List[DialectInfo] = Field(..., description="List of supported SQL dialects")


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(default="healthy", description="Service health status")
    version: str = Field(default="1.0.0", description="API version")
