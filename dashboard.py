import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import datetime
import altair as alt

st.set_page_config(page_title="Politician Trading Dashboard", layout="wide")

# ============================================================
# SECRETS (NEW FORMAT)
# ============================================================
QUIVER_KEY = st.secrets["QUIVER_KEY"]
ALPACA_KEY = st.secrets["ALPACA_KEY"]
ALPACA_SECRET = st.secrets["ALPACA_SECRET"]
GOOGLE_SHEET_NAME = st.secrets["GOOGLE_SHEET_NAME"]

# NEW Google Credentials dict
GCP_JSON = dict(st.secrets["google_credentials"])

# ============================================================
# GOOGLE SHEETS CONNECTION
# ============================================================
def connect_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(GCP_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME)

try:
    sh = connect_sheets()

    # If your sheets don't exist yet, create them safely
    try:
        trades_ws = sh.worksheet("Trades")
    except:
        trades_ws = sh.add_worksheet("Trades", 1000, 20)

    try:
        buys_ws = sh.worksheet("Buys")
    except:
        buys_ws = sh.add_worksheet("Buys", 1000, 20)

except Exception as e:
    st.error(f"❌ Failed to connect to Google Sheets: {e}")
    st.stop()

# Load data safely
try:
    trades_df = pd.DataFrame(trades_ws.get_all_records())
except:
    trades_df = pd.DataFrame()

try:
    buys_df = pd.DataFrame(buys_ws.get_all_records())
except:
    buys_df = pd.DataFrame()

# ============================================================
# ALPACA CLIENTS
# ============================================================
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)
market_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.header("📊 Dashboard Filters")
show_equity = st.sidebar.checkbox("Show Equity Curve", True)
show_positions = st.sidebar.checkbox("Show Current Positions", True)
show_heatmap = st.sidebar.checkbox("Show Score Heatmap", True)
show_politicians = st.sidebar.checkbox("Show Politician Activity", True)

# ============================================================
# TITLE
# ============================================================
st.title("🇺🇸 Politician Trading Bot — Portfolio Dashboard")
st.markdown("Live performance overview of your automated trader.")

# ============================================================
# CURRENT PORTFOLIO
# ============================================================
if show_positions:
    st.header("📈 Current Portfolio")

    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        st.error(f"❌ Error fetching positions: {e}")
        positions = []

    if positions:
        rows = []
        total_value = 0

        for p in positions:
            mv = float(p.market_value)
            total_value += mv
            rows.append({
                "Symbol": p.symbol,
                "Qty": float(p.qty),
                "Cost Basis": float(p.avg_entry_price),
                "Current Price": float(p.current_price),
                "Market Value": mv,
                "Unrealized P/L": float(p.unrealized_pl)
            })

        pos_df = pd.DataFrame(rows)
        st.dataframe(pos_df, use_container_width=True)
        st.metric("Total Portfolio Value", f"${total_value:,.2f}")

    else:
        st.info("No open positions.")

# ============================================================
# EQUITY CURVE
# ============================================================
if show_equity:
    st.header("📉 Equity Curve")

    if not trades_df.empty and "PortfolioValue" in trades_df.columns:
        trades_df["Timestamp"] = pd.to_datetime(trades_df["Timestamp"])

        chart = (
            alt.Chart(trades_df)
            .mark_line()
            .encode(
                x="Timestamp:T",
                y="PortfolioValue:Q"
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No equity curve data logged yet.")

# ============================================================
# SCORE HEATMAP
# ============================================================
if show_heatmap:
    st.header("🔥 Buy Score Heatmap")

    if not buys_df.empty and "score" in buys_df.columns and "Ticker" in buys_df.columns:
        buys_df["score"] = pd.to_numeric(buys_df["score"], errors="coerce")

        heat = (
            alt.Chart(buys_df)
            .mark_rect()
            .encode(
                x="Ticker:N",
                y="score:Q",
                color="score:Q"
            )
        )
        st.altair_chart(heat, use_container_width=True)
    else:
        st.info("No scored buys to show.")

# ============================================================
# POLITICIAN ACTIVITY
# ============================================================
if show_politicians:
    st.header("🏛 Politician Trading Volume")

    if not buys_df.empty and "Name" in buys_df.columns:
        counts = buys_df["Name"].value_counts().reset_index()
        counts.columns = ["Politician", "Trades"]

        bar = (
            alt.Chart(counts)
            .mark_bar()
            .encode(
                x="Politician:N",
                y="Trades:Q",
                color="Trades:Q"
            )
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("No politician data logged yet.")
