import os
import json
import requests
import pandas as pd
import numpy as np
import datetime as dt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# ====================================================
# 1. Load environment variables from GitHub Secrets
# ====================================================
QUIVER_KEY = os.getenv("QUIVER_KEY")
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not QUIVER_KEY:
    raise ValueError("QUIVER_KEY secret missing")

# ====================================================
# 2. Authenticate Google Sheets via service account file
# ====================================================
creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_APPLICATION_CREDENTIALS,
    ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

try:
    sheet = gc.open(GOOGLE_SHEET_NAME).sheet1
except Exception as e:
    raise RuntimeError(f"Could not open Google Sheet: {e}")

# ====================================================
# 3. Fetch Bulk Congress Trading Data
# ====================================================
def fetch_congress_trades():
    url = "https://api.quiverquant.com/beta/bulk/congresstrading"
    headers = {"accept": "application/json", "Authorization": f"Token {QUIVER_KEY}"}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    print("Columns returned:", df.columns.tolist())

    # The correct date field in YOUR dataset is "Traded"
    if "Traded" not in df.columns:
        raise KeyError("Expected column 'Traded' not found in Quiver data")

    df["TransactionDate"] = pd.to_datetime(df["Traded"], errors="coerce")

    # Drop invalid dates
    df = df.dropna(subset=["TransactionDate"])

    # Filter trades from the last 30 days
    cutoff = dt.datetime.utcnow() - dt.timedelta(days=30)
    df = df[df["TransactionDate"] >= cutoff]

    return df




# ====================================================
# 4. Score trades (simple but smart model)
# ====================================================
def score_trades(df):
    df = df.copy()

    # Initialize score
    df["score"] = 0

    # 1. Trade size score (Trade_Size_USD)
    if "Trade_Size_USD" in df.columns:
        df["score"] += df["Trade_Size_USD"].apply(
            lambda x: 3 if x >= 100000 else (2 if x >= 25000 else 1)
        )

    # 2. Transaction type (BUY / SELL)
    if "Transaction" in df.columns:
        df["score"] += df["Transaction"].apply(
            lambda x: 2 if str(x).upper() == "BUY" else (-2 if str(x).upper() == "SELL" else 0)
        )

    # 3. excess_return scoring
    if "excess_return" in df.columns:
        df["score"] += df["excess_return"].apply(
            lambda x: 2 if x > 0.05 else (1 if x > 0 else 0)
        )

    # 4. Generic activity bonus per politician
    df["score"] += 1

    # Sort high → low
    df = df.sort_values(by="score", ascending=False)

    return df



# ====================================================
# 5. Log trades to Google Sheets
# ====================================================
def log_to_sheet(df):
    rows = df[
        ["TransactionDate", "Representative", "Ticker", "Transaction", "Range", "score"]
    ].astype(str).values.tolist()
    sheet.append_rows(rows)


# ====================================================
# 6. Alpaca trading
# ====================================================
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

def place_trade(ticker, side, notional=50):
    order_data = MarketOrderRequest(
        symbol=ticker,
        notional=notional,
        side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
    )
    try:
        trading_client.submit_order(order_data=order_data)
        print(f"Placed {side} order: {ticker}")
    except Exception as e:
        print(f"Order error for {ticker}: {e}")


# ====================================================
# 7. Strategy: Buy top 5 trades with score >= 10
# ====================================================
def generate_signals(df, min_score=10, limit=5):
    signals = []
    for _, row in df.head(limit).iterrows():
        if row["score"] >= min_score:
            signals.append({
                "ticker": row["Ticker"],
                "side": "buy",
                "score": row["score"]
            })
    return signals


# ====================================================
# 8. Run the full bot
# ====================================================
def run_bot():
    print("Fetching trades...")
    df = fetch_congress_trades()

    print("Scoring trades...")
    df_scored = score_trades(df)

    print("Logging to Google Sheets...")
    log_to_sheet(df_scored)

    print("Generating signals...")
    signals = generate_signals(df_scored)

    print(f"Signals generated: {signals}")
    for s in signals:
        place_trade(s["ticker"], s["side"], notional=50)

    print("Bot run complete.")


# ====================================================
# Execute
# ====================================================
if __name__ == "__main__":
    try:
        run_bot()
    except Exception as e:
        print("Bot failed:", e)
        raise
