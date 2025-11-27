import os
import asyncio
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
    AsyncSession,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import backoff


# ============================================================
# DATABASE URL (from Streamlit secrets or real env var)
# ============================================================

def load_db_url():
    # Streamlit secrets if available
    try:
        import streamlit as st
        return st.secrets["database"]["url"]
    except:
        return os.getenv("DATABASE_URL")

DATABASE_URL = load_db_url()

# Convert to asyncpg
ASYNC_DB_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")


# ============================================================
# CREATE ENGINE + SESSION FACTORY
# ============================================================

engine: AsyncEngine = create_async_engine(
    ASYNC_DB_URL,
    echo=False,
    future=True,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
)

AsyncSessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


# ============================================================
# RETRY HANDLER (auto reconnect on Neon network blips)
# ============================================================
def backoff_hdlr(details):
    print(f"[DB RETRY] Error: {details['exception']} — retrying...")


retry_db = backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=5,
    on_backoff=backoff_hdlr,
)


# ============================================================
# CORE HELPER
# ============================================================
@retry_db
async def exec(query: str, params: Optional[dict] = None):
    """Run a query and return result rows as list of dicts."""
    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(text(query), params or {})
            await session.commit()

            # Convert to list of dicts
            if result.returns_rows:
                rows = [dict(row) for row in result.mappings().all()]
                return rows
            return None

        except Exception as e:
            await session.rollback()
            print("❌ DB ERROR:", e)
            raise


# ============================================================
# INSERTORS
# ============================================================

async def log_politician_trade(
    politician: str,
    ticker: str,
    transaction_type: str,
    amount_min: Optional[float],
    amount_max: Optional[float],
    date: datetime,
):
    return await exec(
        """
        INSERT INTO politician_trades
        (politician, ticker, transaction_type, amount_min, amount_max, transaction_date)
        VALUES (:p, :t, :ty, :min, :max, :d)
        """,
        {
            "p": politician,
            "t": ticker,
            "ty": transaction_type,
            "min": amount_min,
            "max": amount_max,
            "d": date,
        },
    )


async def log_buy_score(
    ticker: str,
    score: float,
    reason: str,
    politician_count: int,
    last_trade_date: Optional[datetime],
):
    return await exec(
        """
        INSERT INTO buy_scores (ticker, score, reason, politician_count, last_trade_date)
        VALUES (:t, :s, :r, :c, :d)
        """,
        {
            "t": ticker,
            "s": score,
            "r": reason,
            "c": politician_count,
            "d": last_trade_date,
        },
    )


async def log_bot_trade(
    ticker: str,
    side: str,
    qty: float,
    price: float,
    order_id: str,
    portfolio_value: Optional[float],
    score: Optional[float],
    signal_count: Optional[int],
):
    return await exec(
        """
        INSERT INTO bot_trades
        (ticker, side, qty, price, order_id, portfolio_value, score, politician_signal)
        VALUES (:t, :s, :q, :p, :oid, :pv, :sc, :sig)
        ON CONFLICT (order_id) DO NOTHING;
        """,
        {
            "t": ticker,
            "s": side,
            "q": qty,
            "p": price,
            "oid": order_id,
            "pv": portfolio_value,
            "sc": score,
            "sig": signal_count,
        },
    )


async def log_portfolio_value(value: float):
    return await exec(
        """
        INSERT INTO portfolio_history (total_value)
        VALUES (:v)
        """,
        {"v": value},
    )


async def log_position(
    ticker: str,
    qty: float,
    avg: float,
    price: Optional[float],
):
    return await exec(
        """
        INSERT INTO positions (ticker, qty, avg_entry_price, current_price)
        VALUES (:t, :q, :a, :p)
        """,
        {"t": ticker, "q": qty, "a": avg, "p": price},
    )


async def log_bot_event(level: str, message: str, context: Optional[dict] = None):
    return await exec(
        """
        INSERT INTO bot_logs (level, message, context)
        VALUES (:l, :m, :c)
        """,
        {"l": level, "m": message, "c": json.dumps(context or {})},
    )


async def log_bot_run_start() -> int:
    rows = await exec(
        """
        INSERT INTO bot_runs (run_started)
        VALUES (NOW())
        RETURNING id;
        """
    )
    return rows[0]["id"]


async def log_bot_run_end(run_id: int, trades: int, errors: int):
    return await exec(
        """
        UPDATE bot_runs
        SET run_finished = NOW(),
            trades_made = :t,
            errors = :e
        WHERE id = :id;
        """,
        {"id": run_id, "t": trades, "e": errors},
    )


# ============================================================
# FETCH HELPERS
# ============================================================

async def get_latest_scores(limit=50):
    return await exec(
        """
        SELECT * FROM buy_scores
        ORDER BY created_at DESC
        LIMIT :n
        """,
        {"n": limit},
    )


async def get_recent_trades(limit=50):
    return await exec(
        """
        SELECT * FROM bot_trades
        ORDER BY timestamp DESC
        LIMIT :n
        """,
        {"n": limit},
    )


async def get_positions_for_ticker(ticker: str):
    return await exec(
        """
        SELECT * FROM positions
        WHERE ticker = :t
        ORDER BY timestamp DESC
        """,
        {"t": ticker},
    )


# ============================================================
# TEST FUNCTION
# ============================================================
if __name__ == "__main__":
    async def test():
        print("▶ Testing DB connection...")
        rows = await exec("SELECT NOW() AS server_time;")
        print("Success:", rows)

    asyncio.run(test())
