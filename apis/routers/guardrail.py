from fastapi import APIRouter, Form, HTTPException
from typing import Optional
from apis.utils.helpers import process_guardrail, transpile_query
import os
from guardrail.main import StorageServiceClient

router = APIRouter()

# Environment variables for Guardrail service
ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "cops-beta1-storage-storage-blue")
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", "9006")

# Initialize the storage service client if guardrail is enabled
storage_service_client = None
if ENABLE_GUARDRAIL.lower() == "true":
    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)


@router.post("/guard")
async def guard(
        query: str = Form(...),
        schema: str = Form(...),
        catalog: str = Form(...),
):
    """Validate SQL queries against guardrails."""
    try:
        if storage_service_client is None:
            raise HTTPException(status_code=500, detail="Storage Service Not Initialized.")

        violations = process_guardrail(query, schema, catalog, storage_service_client)
        return {"action": "deny" if violations else "allow", "violations": violations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transguard")
async def transguard(
        query: str = Form(...),
        schema: str = Form(...),
        catalog: str = Form(...),
        from_sql: str = Form(...),
        to_sql: Optional[str] = Form("E6"),
):
    """
    Transpile SQL queries from one dialect to another, then validate them against guardrails.
    """
    try:
        if storage_service_client is None:
            raise HTTPException(status_code=500, detail="Storage Service Not Initialized.")

        # Transpile the query from one SQL dialect to another
        transpiled_query = transpile_query(query, from_sql, to_sql)

        # Validate the transpiled query against guardrails
        violations = process_guardrail(transpiled_query, schema, catalog, storage_service_client)
        return {
            "action": "deny" if violations else "allow",
            "violations": violations,
            "transpiled_query": transpiled_query,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
