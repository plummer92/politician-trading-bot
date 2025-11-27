import streamlit as st
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
from alpaca.trading.client import TradingClient
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# Load Secrets
# -----------------------------
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Google Sheets setup
creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_APPLICATION_CREDENTIALS,
    ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# Alpaca client
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

# ===============================================
# Helper: Load Buy Log
# ===============================================
def load_buys():
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.sheet1
    data = ws.get_all_records()
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

# ===============================================
# Helper: Load Sales Log
# ===============================================
def load_sales():
    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws = sh.worksheet("Sales_Log")
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()

# ===============================================
# Helper: Fetch portfolio from Alpaca
# ===============================================
def load_portfolio():
    positions = trading_client.get_all_positions()
    rows = []
    for p in positions:
        rows.append({
            "symbol": p.symbol,
            "qty": float(p.qty),
            "avg_entry": float(p.avg_entry_price),
            "current": float(p.current_price),
        })
    return pd.DataFrame(rows)

# ===============================================
# Dashboard UI
# ===============================================
st.set_page_config(page_title="Politician Trading Dashboard", layout="wide")
st.title("📊 Politician Trading Bot — Portfolio Dashboard")

# Load Data
buys = load_buys()
sales = load_sales()
portfolio = load_portfolio()

# -----------------------------
# PORTFOLIO SUMMARY
# -----------------------------
st.header("💼 Portfolio Summary")

if portfolio.empty:
    st.info("No open positions.")
else:
    portfolio["unrealized_pl"] = (portfolio["current"] - portfolio["avg_entry"]) * portfolio["qty"]
    portfolio["pl_pct"] = (portfolio["current"] - portfolio["avg_entry"]) / portfolio["avg_entry"]

    total_value = (portfolio["current"] * portfolio["qty"]).sum()
    total_profit = portfolio["unrealized_pl"].sum()

    col1, col2 = st.columns(2)
    col1.metric("Total Portfolio Value", f"${total_value:,.2f}")
    col2.metric("Unrealized P/L", f"${total_profit:,.2f}")

    st.dataframe(portfolio)

# -----------------------------
# PIE CHART: POSITION DISTRIBUTION
# -----------------------------
if not portfolio.empty:
    st.subheader("📌 Position Distribution")

    fig, ax = plt.subplots()
    ax.pie(
        portfolio["qty"],
        labels=portfolio["symbol"],
        autopct="%1.1f%%"
    )
    st.pyplot(fig)

# -----------------------------
# EQUITY CURVE (Based on log)
# -----------------------------
st.header("📈 Equity Curve")

if buys.empty:
    st.info("No buy history yet.")
else:
    buys["TransactionDate"] = pd.to_datetime(buys["TransactionDate"], errors="coerce")
    buys["Trade_Size_USD"] = pd.to_numeric(buys["Trade_Size_USD"], errors="coerce").fillna(0)

    eq = buys.sort_values("TransactionDate").copy()
    eq["cumulative_spend"] = eq["Trade_Size_USD"].cumsum()

    fig, ax = plt.subplots()
    ax.plot(eq["TransactionDate"], eq["cumulative_spend"], label="Cumulative Buy Volume")
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Invested ($)")
    ax.legend()
    st.pyplot(fig)

# -----------------------------
# SCORE HEATMAP
# -----------------------------
st.header("🔥 Score Heatmap")

if not buys.empty:
    st.write("Higher score = stronger politician conviction")

    score_df = buys[["Ticker", "score"]]

    # Pivot into heatmap format
    heatmap = score_df.pivot_table(
        index="Ticker", values="score", aggfunc="mean"
    )

    st.dataframe(heatmap.style.background_gradient(cmap="RdYlGn"))

# -----------------------------
# POLITICIAN ACTIVITY
# -----------------------------
st.header("🏛 Most Active Politician Trades")

if not buys.empty:
    activity = buys["Ticker"].value_counts().head(10)
    st.bar_chart(activity)

# -----------------------------
# SALES HISTORY
# -----------------------------
st.header("📉 Closed Trades (Sales Log)")

if sales.empty:
    st.info("No trailing-stop sales yet.")
else:
    st.dataframe(sales)

st.success("Dashboard loaded successfully!")
