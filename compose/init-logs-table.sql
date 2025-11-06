-- Create logs table for transpiler application logs
CREATE TABLE IF NOT EXISTS transpiler_logs (
    ts TIMESTAMP TIME INDEX,
    "level" STRING,
    "message" STRING,
    "service" STRING,
    container_name STRING,
    query_id STRING,
    source_dialect STRING,
    target_dialect STRING,
    duration_ms DOUBLE,
    duration_s DOUBLE,
    executable STRING,
    "error" STRING,
    phase STRING,
    "endpoint" STRING,
    PRIMARY KEY ("service")
);
