import uvicorn

if __name__ == "__main__":
    # Run the FastAPI app
    uvicorn.run("apis.app:app", host="0.0.0.0", port=8100, proxy_headers=True, workers=5)
