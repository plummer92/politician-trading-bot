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
# 1. Load Secrets
# ====================================================
QUIVER_KEY = os.getenv("QUIVER_KEY")
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not QUIVER_KEY:
    raise ValueError("QUIVER_KEY secret missing")

# ====================================================
# 2. Google Sheets Authentication
# ====================================================
creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_APPLICATION_CREDENTIALS,
    ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# ====================================================
# 3. Trailing Stop Setup
# ====================================================
TRAILING_FILE = "trailing_sl.json"
TRAIL_PERCENT = 0.08         # 8% trailing stop
TOP_LOSERS_TO_SELL = 3       # Sell 3 worst losers

def load_trailing_data():
    if not os.path.exists(TRAILING_FILE):
        return {}
    try:
        with open(TRAILING_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_trailing_data(data):
    with open(TRAILING_FILE, "w") as f:
        json.dump(data, f, indent=4)

# ====================================================
# 4. Fetch Quiver Data
# ====================================================
def fetch_congress_trades():
    url = "https://api.quiverquant.com/beta/bulk/congresstrading"
    headers = {"accept": "application/json", "Authorization": f"Token {QUIVER_KEY}"}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    df = pd.DataFrame(r.json())
    print("Columns returned:", df.columns.tolist())

    if "Traded" not in df.columns:
        raise KeyError("Expected 'Traded' column missing.")

    df["TransactionDate"] = pd.to_datetime(df["Traded"], errors="coerce")
    df = df.dropna(subset=["TransactionDate"])

    cutoff = dt.datetime.utcnow() - dt.timedelta(days=30)
    df = df[df["TransactionDate"] >= cutoff]

    return df

# ====================================================
# 5. Score Trades
# ====================================================
def score_trades(df):
    df = df.copy()
    df["score"] = 0

    # trade size
    if "Trade_Size_USD" in df.columns:
        size = pd.to_numeric(df["Trade_Size_USD"], errors="coerce").fillna(0)
        df["score"] += np.where(size >= 100000, 3,
                        np.where(size >= 25000, 2, 1))

    # buy stronger than sell
    df["score"] += df["Transaction"].apply(
        lambda x: 2 if str(x).upper() == "BUY"
        else (-2 if str(x).upper() == "SELL" else 0)
    )

    # excess return
    if "excess_return" in df.columns:
        er = pd.to_numeric(df["excess_return"], errors="coerce").fillna(0)
        df["score"] += np.where(er > 0.05, 2,
                         np.where(er > 0, 1, 0))

    # activity bonus
    df["score"] += 1

    return df.sort_values(by="score", ascending=False)

# ====================================================
# 6. Log Buys to Sheet
# ====================================================
def log_to_sheet(df):
    print("Logging buys...")

    cols = [
        "TransactionDate","Ticker","Company","Transaction","Trade_Size_USD",
        "Name","Party","District","Chamber","excess_return","score"
    ]
    cols = [c for c in cols if c in df.columns]
    rows = df[cols].astype(str).values.tolist()

    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.sheet1

    if ws.acell("A1").value in (None, ""):
        ws.append_row(cols)

    ws.append_rows(rows)
    print("Buy logging complete.")

# ====================================================
# 7. Alpaca Buy Engine
# ====================================================
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

def get_price(symbol):
    """Safe Alpaca quote fetch that works in GitHub Actions."""
    try:
        quote = trading_client.get_latest_quote(symbol)
        return quote.ask_price or quote.bid_price
    except:
        return None

def execute_trades(df):
    print("Executing buys...")

    df_trade = df[df["score"] >= 6]  # threshold = 6

    if df_trade.empty:
        print("No trades >= 6.")
        return

    existing_positions = {pos.symbol for pos in trading_client.get_all_positions()}
    BUDGET = 50

    for _, row in df_trade.iterrows():
        symbol = row["Ticker"]

        if not symbol or symbol in existing_positions:
            continue

        price = get_price(symbol)
        if not price or price <= 0:
            print(f"Skipping {symbol}: bad price")
            continue

        qty = max(1, int(BUDGET // price))

        try:
            trading_client.submit_order(
                MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
            )
            print(f"Bought {qty} {symbol}")

        except Exception as e:
            print(f"Buy error for {symbol}: {e}")

# ====================================================
# 8. Log Sales
# ====================================================
def log_sale_to_sheet(row):
    sh = gc.open(GOOGLE_SHEET_NAME)

    try:
        ws = sh.worksheet("Sales_Log")
    except:
        ws = sh.add_worksheet("Sales_Log", rows=1000, cols=10)
        ws.append_row(["Timestamp","Ticker","Qty","SellPrice","CostBasis","HighestPrice","DropPct","PLPct","Reason"])

    ws.append_row([
        dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        row["symbol"],
        row["qty"],
        row["current"],
        row["cost"],
        row["highest"],
        f"{row['drop_pct']*100:.2f}%",
        f"{row['pl_pct']*100:.2f}%",
        "Trailing Stop"
    ])

# ====================================================
# 9. Trailing Stop Engine (Top 3 losers)
# ====================================================
def trailing_stop_and_sell():
    print("\n=== Trailing Stop Evaluation ===")

    trailing = load_trailing_data()
    positions = trading_client.get_all_positions()

    if not positions:
        print("No open positions.")
        return

    eval_rows = []

    for pos in positions:
        symbol = pos.symbol
        qty = float(pos.qty)
        cost = float(pos.avg_entry_price)
        current = float(pos.current_price)

        highest = trailing.get(symbol, {}).get("highest", current)
        if current > highest:
            highest = current

        trailing[symbol] = {"highest": highest}

        drop_pct = (highest - current) / highest
        pl_pct = (current - cost) / cost if cost > 0 else 0

        eval_rows.append({
            "symbol": symbol,
            "qty": qty,
            "current": current,
            "cost": cost,
            "highest": highest,
            "drop_pct": drop_pct,
            "pl_pct": pl_pct
        })

    save_trailing_data(trailing)

    violators = [r for r in eval_rows if r["drop_pct"] >= TRAIL_PERCENT]

    if not violators:
        print("No trailing stops triggered.")
        return

    violators_sorted = sorted(violators, key=lambda x: x["pl_pct"])
    to_sell = violators_sorted[:TOP_LOSERS_TO_SELL]

    for row in to_sell:
        try:
            trading_client.submit_order(
                MarketOrderRequest(
                    symbol=row["symbol"],
                    qty=int(row["qty"]),
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
            )
            print(f"🔻 SOLD {row['symbol']} — drop {row['drop_pct']*100:.2f}%")
            log_sale_to_sheet(row)

        except Exception as e:
            print(f"Sell error for {row['symbol']}: {e}")

# ====================================================
# 10. Main Bot Flow
# ====================================================
def run_bot():
    print("Fetching trades...")
    df = fetch_congress_trades()

    print("Scoring...")
    df_scored = score_trades(df)

    print("Logging buys...")
    log_to_sheet(df_scored)

    print("Buying...")
    execute_trades(df_scored)

    print("Trailing stops...")
    trailing_stop_and_sell()

    print("Bot complete.")

# ====================================================
# Execute
# ====================================================
if __name__ == "__main__":
    run_bot()
