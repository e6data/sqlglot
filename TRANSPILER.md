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

## API Endpoints

### Inline Mode (Single Query)

**Transpile:**
```bash
POST /api/v1/inline/transpile
{
  "query": "SELECT * FROM users WHERE id > 100",
  "from_sql": "databricks",
  "to_sql": "e6"
}
```

**Analyze:**
```bash
POST /api/v1/inline/analyze
{
  "query": "SELECT * FROM users",
  "from_sql": "snowflake"
}
```

### Batch Mode (Multiple Queries)

**Transpile:**
```bash
POST /api/v1/batch/transpile
{
  "queries": [
    {"id": "q1", "query": "SELECT * FROM users"},
    {"id": "q2", "query": "SELECT * FROM orders"}
  ],
  "from_sql": "databricks",
  "to_sql": "e6"
}
```

**Analyze:**
```bash
POST /api/v1/batch/analyze
{
  "queries": [...],
  "from_sql": "databricks"
}
```

### Meta

- `GET /api/v1/health` - Health check
- `GET /api/v1/dialects` - List supported dialects

## Documentation

Interactive API docs: `http://localhost:8100/docs`

## Legacy Endpoints

Old endpoints still work:
- `POST /convert-query` (form-encoded)
- `POST /statistics` (form-encoded)
