import os
import json
import traceback
import asyncio
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Load Streamlit-style secrets or os env
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("database_url")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in environment!")

# Convert postgres:// → postgresql+asyncpg://
ASYNC_DB_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

engine = create_async_engine(
    ASYNC_DB_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# ============================================================
# REUSABLE EXECUTOR
# ============================================================
async def execute(query, params=None):
    try:
        async with engine.begin() as conn:
            await conn.execute(text(query), params or {})
    except Exception as e:
        print("DB Error:", e)
        print(query)
        print(params)
        print(traceback.format_exc())
        # Never raise — logging only
        return False
    return True


# ============================================================
# RAW QUIVER DATA LOGGING
# ============================================================
async def log_quiver_raw(df):
    if df.empty:
        return

    query = """
        INSERT INTO quiver_raw (
            transaction_date, ticker, company, transaction, trade_size_usd,
            name, party, district, chamber, excess_return, raw_json
        )
        VALUES (
            :transaction_date, :ticker, :company, :transaction, :trade_size_usd,
            :name, :party, :district, :chamber, :excess_return, :raw_json
        );
    """

    for _, row in df.iterrows():
        params = {
            "transaction_date": row.get("TransactionDate"),
            "ticker": row.get("Ticker"),
            "company": row.get("Company"),
            "transaction": row.get("Transaction"),
            "trade_size_usd": row.get("Trade_Size_USD"),
            "name": row.get("Name"),
            "party": row.get("Party"),
            "district": row.get("District"),
            "chamber": row.get("Chamber"),
            "excess_return": row.get("excess_return"),
            "raw_json": json.dumps(row.to_dict())
        }
        await execute(query, params)


# ============================================================
# SCORED TRADES LOGGING
# ============================================================
async def log_scored_trades(df):
    if df.empty:
        return

    query = """
        INSERT INTO scored_trades (
            transaction_date, ticker, company, transaction, trade_size_usd,
            name, party, district, chamber, excess_return, score, raw_json
        )
        VALUES (
            :transaction_date, :ticker, :company, :transaction, :trade_size_usd,
            :name, :party, :district, :chamber, :excess_return, :score, :raw_json
        );
    """

    for _, row in df.iterrows():
        params = {
            "transaction_date": row.get("TransactionDate"),
            "ticker": row.get("Ticker"),
            "company": row.get("Company"),
            "transaction": row.get("Transaction"),
            "trade_size_usd": row.get("Trade_Size_USD"),
            "name": row.get("Name"),
            "party": row.get("Party"),
            "district": row.get("District"),
            "chamber": row.get("Chamber"),
            "excess_return": row.get("excess_return"),
            "score": row.get("score"),
            "raw_json": json.dumps(row.to_dict())
        }
        await execute(query, params)


# ============================================================
# BUY LOGGING
# ============================================================
async def log_buys(buys):
    if not buys:
        return

    query = """
        INSERT INTO buys (ticker, qty, price, total_cost, raw_json)
        VALUES (:ticker, :qty, :price, :total_cost, :raw_json);
    """

    for sym, qty, price in buys:
        params = {
            "ticker": sym,
            "qty": qty,
            "price": price,
            "total_cost": qty * price,
            "raw_json": json.dumps({"symbol": sym, "qty": qty, "price": price})
        }
        await execute(query, params)


# ============================================================
# SELL LOGGING
# ============================================================
async def log_sells(sells):
    if not sells:
        return

    query = """
        INSERT INTO sells (
            ticker, qty, price, cost_basis, highest_price,
            drop_pct, pl_pct, reason, raw_json
        )
        VALUES (
            :ticker, :qty, :price, :cost_basis, :highest_price,
            :drop_pct, :pl_pct, :reason, :raw_json
        );
    """

    for s in sells:
        params = {
            "ticker": s["symbol"],
            "qty": s["qty"],
            "price": s["price"],
            "cost_basis": s["cost"],
            "highest_price": s["highest"],
            "drop_pct": s["drop_pct"],
            "pl_pct": s["pl_pct"],
            "reason": "Trailing Stop",
            "raw_json": json.dumps(s)
        }
        await execute(query, params)


# ============================================================
# POSITION SNAPSHOT
# ============================================================
async def log_positions(positions):
    if not positions:
        return

    query = """
        INSERT INTO positions (
            ticker, qty, avg_cost, current_price,
            market_value, unrealized_pl
        )
        VALUES (
            :ticker, :qty, :avg_cost, :current_price,
            :market_value, :unrealized_pl
        );
    """

    for p in positions:
        params = {
            "ticker": p.symbol,
            "qty": float(p.qty),
            "avg_cost": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "market_value": float(p.market_value),
            "unrealized_pl": float(p.unrealized_pl)
        }
        await execute(query, params)


# ============================================================
# RUN START/END
# ============================================================
async def log_run_event(event):
    await execute("INSERT INTO runs (event) VALUES (:event)", {"event": event})


# ============================================================
# ERROR LOGGING
# ============================================================
async def log_error(step, error):
    await execute(
        """INSERT INTO errors (step, message, traceback)
           VALUES (:step, :message, :traceback)""",
        {
            "step": step,
            "message": str(error),
            "traceback": traceback.format_exc(),
        }
    )
