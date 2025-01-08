from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict
import uvicorn

app = FastAPI()


class RuleRequest(BaseModel):
    sql_string: str
    catalog_name: str
    schema_name: str
    rule_json: Dict
    requestid: str


def get_table_info(sql_string: str) -> str:
    # Placeholder for actual table info retrieval logic
    if "warn" in sql_string:
        return "warn"
    elif "ok" in sql_string:
        return "ok"
    else:
        return "no"


@app.post("/validate_rule")
async def validate_rule(request: RuleRequest):
    status = get_table_info(request.sql_string)
    return {"status": status}


# if __name__ == "__main__":
# uvicorn.run(app, host="0.0.0.0", port=8000)
