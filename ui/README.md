# SQLGlot E6 Converter UI

A minimal, production-ready web interface for converting SQL queries to E6 dialect using SQLGlot.

## Features

- 🔄 Convert SQL from multiple dialects (Snowflake, Databricks, Postgres, BigQuery, etc.) to E6
- ⚙️ Feature flags for advanced conversion options
- 📋 Copy and download converted queries
- 🎨 E6-branded UI with custom color scheme

## Setup

### Install dependencies

```bash
npm install
```

### Run development server

```bash
npm run dev
```

Visit [http://localhost:3000](http://localhost:3000)

### Build for production

```bash
npm run build
npm start
```

### Docker

```bash
docker build -t sqlglot-ui .
docker run -p 3000:3000 -e NEXT_PUBLIC_API_URL=http://localhost:8100 sqlglot-ui
```

## Environment Variables

- `NEXT_PUBLIC_API_URL` - URL of the SQLGlot FastAPI backend (default: http://localhost:8100)

## Usage

1. Select source dialect from dropdown
2. Paste SQL query in left editor
3. Configure feature flags if needed (gear icon)
4. Click "Convert to E6"
5. Copy or download the converted query

## Feature Flags

- **Enable Table Alias Qualification** - Add table aliases to column references
- **Pretty Print** - Format output with indentation (default: true)
- **Two Phase Qualification Scheme** - Use two-phase qualification
- **Skip E6 Transpilation** - Only transform catalog.schema references

## Tech Stack

- Next.js 15 + React 19
- TypeScript
- Tailwind CSS 4
- CodeMirror (SQL editor)
- Radix UI primitives
