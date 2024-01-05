import uvicorn

uvicorn.run(
    "app:app",
    log_level="info"
)