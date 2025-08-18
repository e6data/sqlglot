# Final Distributed Processing System

This directory contains the implementation of the hash-based modulo batch processing system for SQL transpilation as described in the architecture document.

## Components

### Core Modules
- `batch_processor.py` - Hash-based modulo batch distribution and processing
- `session_manager.py` - Batch session tracking with Redis
- `celery_worker.py` - Celery worker for processing modulo batches
- `api_endpoints.py` - API endpoint handlers for distributed processing
- `config.py` - Configuration settings

### Test and Setup
- `test_system.py` - Test script for the complete system
- `requirements.txt` - Python dependencies
- `README.md` - This file

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Redis:**
   ```bash
   redis-server
   ```

3. **Start Celery worker:**
   ```bash
   cd final_distributed_processing
   python celery_worker.py
   ```

4. **Start API server:**
   ```bash
   cd ..
   python converter_api.py
   ```

5. **Test the system:**
   ```bash
   cd final_distributed_processing
   python test_system.py
   ```

## API Endpoints

### Main Processing Endpoint
- `POST /process-parquet-directory` - Process entire directories with hash-based batching

### Progress Tracking
- `GET /status/{session_id}` - Get session progress
- `GET /batch/{batch_id}/status` - Get specific batch status
- `POST /retry/{batch_id}` - Retry failed batch

## Architecture Implementation

This implementation follows the architecture document:
1. **Single API entry point** for directory processing
2. **Hash-based batch distribution** using modulo operations
3. **Pre-scanning** to determine optimal batch count
4. **Distributed processing** with Celery workers
5. **Redis metadata storage** (no batch data stored)
6. **Iceberg storage** for results