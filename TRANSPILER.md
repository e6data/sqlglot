# E6 SQL Transpiler API

SQL transpiler API for converting queries from various dialects (Databricks, Snowflake, BigQuery, etc.) to E6 dialect.

## Setup

```bash
# One-time setup
task setup

# Start API
task dev:backend
```

API runs on `http://localhost:8100`

## API Documentation

- **Swagger UI**: http://localhost:8100/docs
- **ReDoc**: http://localhost:8100/redoc
- **OpenAPI JSON**: http://localhost:8100/openapi.json

## Quick Test

```bash
curl -X POST 'http://localhost:8100/api/v1/inline/transpile' \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM users", "from_sql": "databricks"}'
```
