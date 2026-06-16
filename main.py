import os
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST", "dbc-fb1af771-8c33.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG          = "wbe_test_catalog"
SCHEMA           = "car"
TABLE            = "location_log"

SQL = f"""
SELECT latitude, longitude, recorded_at
FROM {CATALOG}.{SCHEMA}.{TABLE}
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
ORDER BY recorded_at DESC
LIMIT 1
"""


@app.get("/api/volvo-location")
async def volvo_location():
    if not DATABRICKS_TOKEN:
        raise HTTPException(status_code=500, detail="DATABRICKS_TOKEN not set")

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }
    base_url = f"https://{DATABRICKS_HOST}"

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Get a warehouse
        wh_res = await client.get(f"{base_url}/api/2.0/sql/warehouses", headers=headers)
        wh_data = wh_res.json()
        warehouses = wh_data.get("warehouses", [])
        wh = next((w for w in warehouses if w.get("state") == "RUNNING"), None) or (warehouses[0] if warehouses else None)
        if not wh:
            raise HTTPException(status_code=500, detail="No SQL warehouse found — start one in Databricks")

        # 2. Submit SQL statement
        submit_res = await client.post(
            f"{base_url}/api/2.0/sql/statements",
            headers=headers,
            json={
                "statement": SQL,
                "warehouse_id": wh["id"],
                "wait_timeout": "10s",
                "on_wait_timeout": "CONTINUE",
            },
        )
        result = submit_res.json()

        # 3. Poll until done
        attempts = 0
        while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if attempts > 20:
                raise HTTPException(status_code=504, detail="Query timeout")
            import asyncio; await asyncio.sleep(0.5)
            poll_res = await client.get(
                f"{base_url}/api/2.0/sql/statements/{result['statement_id']}",
                headers=headers,
            )
            result = poll_res.json()
            attempts += 1

        if result.get("status", {}).get("state") != "SUCCEEDED":
            err = result.get("status", {}).get("error", {}).get("message", "Query failed")
            raise HTTPException(status_code=500, detail=err)

        rows = result.get("result", {}).get("data_array", [])
        if not rows:
            raise HTTPException(status_code=404, detail="No location data found")

        lat, lon, ts = rows[0]
        return {"latitude": float(lat), "longitude": float(lon), "recorded_at": ts}


# Serve static files (the dashboard HTML)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
