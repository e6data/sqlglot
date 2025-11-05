from pydantic import BaseModel, Field
from typing import Optional, List, Set, Dict, Any
from enum import Enum


class QueryStatus(str, Enum):
    """Status of query processing"""
    SUCCESS = "success"
    ERROR = "error"


class ConfigFieldInfo(BaseModel):
    """Information about a single configuration field"""
    name: str = Field(..., description="Configuration field name")
    value: Any = Field(..., description="Current value of the configuration")
    description: str = Field(..., description="Description of what this field does")
    type: str = Field(..., description="Type of the field (e.g., 'int', 'str', 'bool')")


class ConfigResponse(BaseModel):
    """Response containing deployment configuration"""
    server: List[ConfigFieldInfo] = Field(..., description="Server configuration settings")
    api: List[ConfigFieldInfo] = Field(..., description="API configuration settings")
    transpilation_defaults: List[ConfigFieldInfo] = Field(..., description="Default transpilation settings")


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

    # Preprocessing
    normalization_ms: Optional[float] = Field(None, description="Time for Unicode normalization and comment stripping")
    config_loading_ms: Optional[float] = Field(None, description="Time to load supported functions for dialects")

    # Phase 1: Source Parsing
    parsing_ms: Optional[float] = Field(None, description="Time to parse source query to AST")

    # Phase 2: Function Analysis (detailed)
    function_extraction_ms: Optional[float] = Field(None, description="Time to extract functions via regex")
    function_categorization_ms: Optional[float] = Field(None, description="Time to categorize functions as supported/unsupported")
    udf_extraction_ms: Optional[float] = Field(None, description="Time to identify user-defined functions")
    unsupported_detection_ms: Optional[float] = Field(None, description="Time to detect unsupported features in AST")
    function_analysis_ms: Optional[float] = Field(None, description="Total function analysis time (legacy field)")

    # Phase 3: Metadata Extraction (detailed)
    table_extraction_ms: Optional[float] = Field(None, description="Time to extract table names")
    join_extraction_ms: Optional[float] = Field(None, description="Time to extract join information")
    cte_extraction_ms: Optional[float] = Field(None, description="Time to extract CTEs, VALUES, and subqueries")
    schema_extraction_ms: Optional[float] = Field(None, description="Time to extract schemas from table names")
    metadata_extraction_ms: Optional[float] = Field(None, description="Total metadata extraction time (legacy field)")

    # Phase 4: Transpilation (detailed)
    ast_preprocessing_ms: Optional[float] = Field(None, description="Time for VALUES wrapping and CTE case fixing")
    transpilation_parsing_ms: Optional[float] = Field(None, description="Time to re-parse after preprocessing")
    identifier_qualification_ms: Optional[float] = Field(None, description="Time to qualify identifiers with quotes")
    sql_generation_ms: Optional[float] = Field(None, description="Time to generate target SQL from AST")
    post_processing_ms: Optional[float] = Field(None, description="Time for STRUCT replacement and comment re-insertion")
    transpilation_ms: Optional[float] = Field(None, description="Total transpilation time (legacy field)")

    # Phase 5: Post-Transpilation Analysis (detailed)
    transpiled_parsing_ms: Optional[float] = Field(None, description="Time to parse transpiled query AST")
    transpiled_function_extraction_ms: Optional[float] = Field(None, description="Time to extract functions from transpiled query")
    transpiled_function_analysis_ms: Optional[float] = Field(None, description="Time to analyze transpiled query functions")
    post_analysis_ms: Optional[float] = Field(None, description="Total post-transpilation analysis time (legacy field)")

    # Final Steps
    ast_serialization_ms: Optional[float] = Field(None, description="Time to serialize ASTs via .dump()")


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


class BatchAnalysisResultItem(BaseModel):
    """Single analysis result item in batch response"""
    id: str = Field(..., description="Query identifier from request")
    status: QueryStatus = Field(..., description="Processing status")
    transpiled_query: Optional[str] = Field(None, description="Transpiled query if successful")
    executable: Optional[bool] = Field(None, description="Whether query is executable")
    functions: Optional[FunctionAnalysis] = Field(None, description="Function analysis")
    metadata: Optional[QueryMetadata] = Field(None, description="Query metadata")
    error: Optional[ErrorDetail] = Field(None, description="Error details if failed")


class ExecutionSummary(BaseModel):
    """Summary of execution statistics"""
    total_queries: int = Field(..., description="Total number of queries")
    succeeded: int = Field(..., description="Number of successful analyses")
    failed: int = Field(..., description="Number of failed analyses")
    executable_queries: int = Field(..., description="Number of queries executable on target dialect")
    non_executable_queries: int = Field(..., description="Number of queries with unsupported features")
    success_rate_percentage: float = Field(..., description="Success rate as percentage")


class FunctionSummary(BaseModel):
    """Summary of function analysis across all queries"""
    unique_supported_count: int = Field(..., description="Count of unique supported functions")
    unique_unsupported_count: int = Field(..., description="Count of unique unsupported functions")
    total_udfs: int = Field(..., description="Total unique user-defined functions")


class ComplexitySummary(BaseModel):
    """Summary of query complexity metrics"""
    total_unique_tables: int = Field(..., description="Total unique tables referenced")
    total_unique_schemas: int = Field(..., description="Total unique schemas referenced")
    avg_tables_per_query: float = Field(..., description="Average tables per query")
    avg_functions_per_query: float = Field(..., description="Average functions per query")
    total_joins: int = Field(..., description="Total joins across all queries")
    total_ctes: int = Field(..., description="Total CTEs across all queries")
    total_subqueries: int = Field(..., description="Total subqueries across all queries")


class TimingSummary(BaseModel):
    """Summary of timing statistics"""
    total_duration_ms: float = Field(..., description="Total batch processing time in milliseconds")
    avg_query_duration_ms: float = Field(..., description="Average time per query")
    min_query_duration_ms: float = Field(..., description="Fastest query time")
    max_query_duration_ms: float = Field(..., description="Slowest query time")
    avg_parsing_ms: Optional[float] = Field(None, description="Average parsing time")
    avg_transpilation_ms: Optional[float] = Field(None, description="Average transpilation time")
    avg_function_analysis_ms: Optional[float] = Field(None, description="Average function analysis time")


class DialectSummary(BaseModel):
    """Summary of dialects used"""
    source_dialect: str = Field(..., description="Source SQL dialect")
    target_dialect: str = Field(..., description="Target SQL dialect")


class BatchSummary(BaseModel):
    """Comprehensive summary of batch processing"""
    execution: ExecutionSummary = Field(..., description="Execution statistics")
    functions: FunctionSummary = Field(..., description="Function analysis summary")
    complexity: ComplexitySummary = Field(..., description="Query complexity metrics")
    timing: TimingSummary = Field(..., description="Timing statistics")
    dialects: DialectSummary = Field(..., description="Dialect information")


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
