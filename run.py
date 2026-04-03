"""Startup script for Databricks App."""
import os
import sys

# Set environment variables
os.environ["DATABRICKS_HTTP_PATH"] = "/sql/1.0/warehouses/148ccb90800933a1"

# Add current directory to path
sys.path.insert(0, "/app/python/source_code")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server.app_minimal:app",
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
