import os
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

app = FastAPI()

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST", "dbc-fb1af771-8c33.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG          = "wbe_test_catalog"
SCHEMA           = "car"

# Cache warehouse id across requests
_warehouse_id = None


def get_headers():
    if not DATABRICKS_TOKEN:
        raise HTTPException(status_code=500, detail="DATABRICKS_TOKEN not set")
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


async def get_warehouse(client: httpx.AsyncClient) -> str:
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id
    res = await client.get(
        f"https://{DATABRICKS_HOST}/api/2.0/sql/warehouses",
        headers=get_headers()
    )
    warehouses = res.json().get("warehouses", [])
    wh = next((w for w in warehouses if w.get("state") == "RUNNING"), None) or (warehouses[0] if warehouses else None)
    if not wh:
        raise HTTPException(status_code=500, detail="No SQL warehouse found — start one in Databricks")
    _warehouse_id = wh["id"]
    return _warehouse_id


async def run_sql(client: httpx.AsyncClient, sql: str) -> list:
    """Submit a SQL statement and wait for results. Returns data_array rows."""
    wh_id = await get_warehouse(client)
    submit_res = await client.post(
        f"https://{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=get_headers(),
        json={
            "statement": sql,
            "warehouse_id": wh_id,
            "wait_timeout": "10s",
            "on_wait_timeout": "CONTINUE",
        },
    )
    result = submit_res.json()

    attempts = 0
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if attempts > 20:
            raise HTTPException(status_code=504, detail="Query timeout")
        await asyncio.sleep(0.5)
        poll_res = await client.get(
            f"https://{DATABRICKS_HOST}/api/2.0/sql/statements/{result['statement_id']}",
            headers=get_headers(),
        )
        result = poll_res.json()
        attempts += 1

    if result.get("status", {}).get("state") != "SUCCEEDED":
        err = result.get("status", {}).get("error", {}).get("message", "Query failed")
        raise HTTPException(status_code=500, detail=err)

    return result.get("result", {}).get("data_array", [])


# ── GET: latest Volvo location ────────────────────────────────────────────────

@app.get("/api/volvo-location")
async def volvo_location():
    sql = f"""
        SELECT latitude, longitude, recorded_at
        FROM {CATALOG}.{SCHEMA}.location_log
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY recorded_at DESC
        LIMIT 1
    """
    async with httpx.AsyncClient(timeout=30) as client:
        rows = await run_sql(client, sql)

    if not rows:
        raise HTTPException(status_code=404, detail="No location data found")

    lat, lon, ts = rows[0]
    return {"latitude": float(lat), "longitude": float(lon), "recorded_at": ts}


# ── POST: save iPhone location ────────────────────────────────────────────────

class IPhoneLocation(BaseModel):
    latitude: float
    longitude: float
    accuracy: float | None = None
    recorded_at: str | None = None  # ISO string; defaults to now if omitted


@app.post("/api/iphone-location")
async def save_iphone_location(loc: IPhoneLocation):
    ts = loc.recorded_at or datetime.now(timezone.utc).isoformat()

    sql = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.iphone_location_log
          (latitude, longitude, accuracy, recorded_at)
        VALUES
          ({loc.latitude}, {loc.longitude}, {loc.accuracy or 'NULL'}, '{ts}')
    """
    async with httpx.AsyncClient(timeout=30) as client:
        await run_sql(client, sql)

    return {"status": "ok", "recorded_at": ts}


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Static files (dashboard) ──────────────────────────────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")
