import os
import json
import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# ====================================================
# 1. Load Neon DATABASE_URL
# ====================================================
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL missing from environment!")

# Force asyncpg dialect
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+asyncpg://")

# Create async engine
engine = create_async_engine(
    DB_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
)

# ====================================================
# 2. Helpers
# ====================================================

def clean_json(value):
    """Convert Pandas + NumPy + Timestamps into JSON-safe types"""
    if isinstance(value, (datetime,)):
        return value.isoformat()

    try:
        import pandas as pd
        import numpy as np
        if isinstance(value, (pd.Timestamp, np.datetime64)):
            return str(value)
        if isinstance(value, (np.int64, np.float64)):
            return float(value)
    except:
        pass

    return value


def row_to_json(row_dict):
    """Convert entire dict to JSON-safe"""
    return {k: clean_json(v) for k, v in row_dict.items()}

# ====================================================
# 3. Startup: create tables
# ====================================================

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS runs (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT NOW(),
    event TEXT
);

CREATE TABLE IF NOT EXISTS quiver_raw (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT NOW(),
    ticker TEXT,
    transaction TEXT,
    traded TIMESTAMP,
    raw_json JSONB
);

CREATE TABLE IF NOT EXISTS buys (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT NOW(),
    symbol TEXT,
    qty INT,
    price NUMERIC
);

CREATE TABLE IF NOT EXISTS sells (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT NOW(),
    symbol TEXT,
    qty INT,
    price NUMERIC,
    reason TEXT
);
"""

async def init_db():
    async with engine.begin() as conn:
        await conn.execute(text(CREATE_TABLES))

# ====================================================
# 4. Insert Functions
# ====================================================

async def log_run(event: str):
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO runs (event) VALUES (:event)"),
                {"event": event}
            )
    except Exception as e:
        print("DB log_run error:", e)


async def log_quiver_raw(df):
    """Insert all politician trades"""
    try:
        async with engine.begin() as conn:
            for _, row in df.iterrows():
                data = row_to_json(row.to_dict())

                await conn.execute(
                    text("""
                        INSERT INTO quiver_raw (ticker, transaction, traded, raw_json)
                        VALUES (:ticker, :transaction, :traded, :raw_json)
                    """),
                    {
                        "ticker": data.get("Ticker"),
                        "transaction": data.get("Transaction"),
                        "traded": data.get("TransactionDate"),
                        "raw_json": json.dumps(data)
                    }
                )
    except Exception as e:
        print("DB log_quiver_raw error:", e)


async def log_buy(symbol, qty, price):
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO buys (symbol, qty, price)
                    VALUES (:symbol, :qty, :price)
                """),
                {"symbol": symbol, "qty": qty, "price": price}
            )
    except Exception as e:
        print("DB log_buy error:", e)


async def log_sell(symbol, qty, price, reason):
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO sells (symbol, qty, price, reason)
                    VALUES (:symbol, :qty, :price, :reason)
                """),
                {"symbol": symbol, "qty": qty, "price": price, "reason": reason}
            )
    except Exception as e:
        print("DB log_sell error:", e)

# ====================================================
# 5. Optional: Fetch Functions
# ====================================================

async def fetch_last_runs(limit=20):
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT * FROM runs ORDER BY id DESC LIMIT :lim"),
                {"lim": limit}
            )
            return result.fetchall()
    except Exception as e:
        print("DB fetch_last_runs error:", e)
        return []

# ====================================================
# 6. Module initialization
# ====================================================

asyncio.get_event_loop().run_until_complete(init_db())
