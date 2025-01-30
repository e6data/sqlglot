from fastapi import FastAPI
from apis.routers.convert import router as conversion_router
from apis.routers.guardrail import router as guardrail_router
from apis.routers.statistics import router as statistics_router

# Initialize FastAPI app
app = FastAPI()

# Include routers
app.include_router(conversion_router, prefix="/conversion", tags=["Conversion"])
app.include_router(guardrail_router, prefix="/guardrail", tags=["Guardrail"])
app.include_router(statistics_router, prefix="/statistics", tags=["Statistics"])


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "OK"}
