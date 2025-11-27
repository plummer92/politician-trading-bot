import streamlit as st
import pandas as pd
import asyncio
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Import Neon async DB
import db


st.set_page_config(
    page_title="Politician Trading Bot Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Politician Trading Bot Dashboard")
st.caption("Real-time analytics powered by Neon PostgreSQL + Alpaca")


# ======================================================
# ASYNC FETCH HELPERS
# ======================================================
@st.cache_data(show_spinner=False)
def sync_fetch(coro):
    return asyncio.run(coro)


# ======================================================
# LOAD DB DATA
# ======================================================
with st.spinner("Loading database…"):
    raw_trades = sync_fetch(db.fetch_raw_quiver())
    scored_trades = sync_fetch(db.fetch_scored_trades())
    buy_log = sync_fetch(db.fetch_buy_log())
    sell_log = sync_fetch(db.fetch_sell_log())
    run_events = sync_fetch(db.fetch_run_events())


# Convert timestamps for charts
def prepare(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])
        df["date"] = df["ts"].dt.date
    return df


raw_trades = prepare(raw_trades)
scored_trades = prepare(scored_trades)
buy_log = prepare(buy_log)
sell_log = prepare(sell_log)
run_events = prepare(run_events)


# ======================================================
# TABS
# ======================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview",
    "🟢 Buys & 🔻 Sells",
    "⭐ Scoreboard",
    "🏛 Politician Activity",
    "📜 Run Logs",
    "💼 Positions (Alpaca)"
])


# ======================================================
# 📊 TAB 1 — OVERVIEW
# ======================================================
with tab1:
    st.header("📊 Daily Overview")

    c1, c2, c3 = st.columns(3)

    c1.metric("Total Raw Trades", len(raw_trades))
    c2.metric("Total Scored Trades", len(scored_trades))
    c3.metric("Total Buys Executed", len(buy_log))

    if not buy_log.empty:
        fig = px.histogram(buy_log, x="date", nbins=20,
                           title="Daily Buy Activity")
        st.plotly_chart(fig, use_container_width=True)

    if not scored_trades.empty:
        fig = px.box(scored_trades, y="score", title="Score Distribution")
        st.plotly_chart(fig, use_container_width=True)


# ======================================================
# 🟢 TAB 2 — Buys & Sells
# ======================================================
with tab2:
    st.header("🟢 Buy Log & 🔻 Sell Log")

    st.subheader("🟢 Buys")
    st.dataframe(buy_log, use_container_width=True)

    st.subheader("🔻 Sells")
    st.dataframe(sell_log, use_container_width=True)

    if not sell_log.empty:
        fig = px.histogram(sell_log, x="drop_pct",
                           title="Sell Drop Percent Histogram")
        st.plotly_chart(fig, use_container_width=True)


# ======================================================
# ⭐ TAB 3 — SCOREBOARD
# ======================================================
with tab3:
    st.header("⭐ Top Scored Trades")

    if not scored_trades.empty:
        top_scores = scored_trades.sort_values("score", ascending=False).head(50)
        st.dataframe(top_scores, use_container_width=True)

        fig = px.bar(top_scores, x="Ticker", y="score",
                     color="score", title="Top 50 Scored Trades")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No scored trades yet.")


# ======================================================
# 🏛 TAB 4 — POLITICIAN ACTIVITY
# ======================================================
with tab4:
    st.header("🏛 Politician Heatmap")

    if not raw_trades.empty:
        count = raw_trades.groupby(["Name", "Transaction"]).size().reset_index(name="count")

        fig = px.treemap(
            count,
            path=["Transaction", "Name"],
            values="count",
            title="Politician Trading Activity"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No raw Quiver trades yet.")


# ======================================================
# 📜 TAB 5 — Run Logs
# ======================================================
with tab5:
    st.header("📜 Bot Run History")

    st.dataframe(run_events, use_container_width=True)

    if not run_events.empty:
        fig = px.line(run_events, x="ts", y="event_type",
                      title="Bot Run Event Timeline")
        st.plotly_chart(fig, use_container_width=True)


# ======================================================
# 💼 TAB 6 — Alpaca Positions
# ======================================================
with tab6:
    st.header("💼 Alpaca Positions (Live)")

    from alpaca.trading.client import TradingClient
    ALPACA_KEY = st.secrets["ALPACA_KEY"]
    ALPACA_SECRET = st.secrets["ALPACA_SECRET"]

    trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

    positions = trading_client.get_all_positions()

    if not positions:
        st.info("No positions currently held.")
    else:
        rows = []
        for p in positions:
            rows.append({
                "Symbol": p.symbol,
                "Qty": p.qty,
                "Entry Price": p.avg_entry_price,
                "Current Price": p.current_price,
                "Unrealized P/L": p.unrealized_pl,
                "P/L %": p.unrealized_plpc,
            })

        df_pos = pd.DataFrame(rows)
        st.dataframe(df_pos, use_container_width=True)

        fig = px.bar(
            df_pos,
            x="Symbol",
            y="Unrealized P/L",
            color="P/L %",
            title="Current P/L by Position"
        )
        st.plotly_chart(fig, use_container_width=True)
