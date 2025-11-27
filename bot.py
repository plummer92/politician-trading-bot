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
# 2. Authenticate Google Sheets
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

    if "Traded" not in df.columns:
        raise KeyError("Expected column 'Traded' not found in Quiver data")

    df["TransactionDate"] = pd.to_datetime(df["Traded"], errors="coerce")
    df = df.dropna(subset=["TransactionDate"])

    cutoff = dt.datetime.utcnow() - dt.timedelta(days=30)
    df = df[df["TransactionDate"] >= cutoff]

    return df

# ====================================================
# 4. Score Trades
# ====================================================
def score_trades(df):
    df = df.copy()
    df["score"] = 0

    # Trade size score
    if "Trade_Size_USD" in df.columns:
        size = pd.to_numeric(df["Trade_Size_USD"], errors="coerce").fillna(0)
        df["score"] += np.where(size >= 100000, 3,
                        np.where(size >= 25000, 2, 1))

    # Transaction type
    df["score"] += df["Transaction"].apply(
        lambda x: 2 if str(x).upper() == "BUY"
        else (-2 if str(x).upper() == "SELL" else 0)
    )

    # Excess return bonus
    if "excess_return" in df.columns:
        er = pd.to_numeric(df["excess_return"], errors="coerce").fillna(0)
        df["score"] += np.where(er > 0.05, 2,
                         np.where(er > 0, 1, 0))

    # Politician activity bonus
    df["score"] += 1

    return df.sort_values(by="score", ascending=False)

# ====================================================
# 5. Log trades to Google Sheets
# ====================================================
def log_to_sheet(df):
    print("Logging to Google Sheets...")

    cols = [
        "TransactionDate","Ticker","Company","Transaction","Trade_Size_USD",
        "Name","Party","District","Chamber","excess_return","score"
    ]
    cols = [c for c in cols if c in df.columns]

    rows = df[cols].astype(str).values.tolist()

    worksheet = gc.open(GOOGLE_SHEET_NAME).sheet1

    # Write header if empty
    if worksheet.acell("A1").value in (None, ""):
        worksheet.append_row(cols)

    # Batch append
    worksheet.append_rows(rows)

    print("Google Sheets logging complete.")

# ====================================================
# 6. Alpaca – New Trading Engine
# ====================================================
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

def execute_trades(df):
    print("Executing Alpaca paper trades...")

    df_trade = df[df["score"] >= 6]   # ⭐ NEW THRESHOLD
   # ⭐ NEW RULE

    if df_trade.empty:
        print("No trades above score threshold (>=5).")
        return

    # Avoid double-buying the same stock
    existing_positions = {pos.symbol for pos in trading_client.get_all_positions()}

    BUDGET = 50  # dollars per trade

    for _, row in df_trade.iterrows():
        symbol = row["Ticker"]

        if not symbol or not isinstance(symbol, str):
            continue

        if symbol in existing_positions:
            print(f"Skipping {symbol}: already owned.")
            continue

        try:
            latest = trading_client.get_latest_trade(symbol)
            price = latest.price

            if not price or price <= 0:
                print(f"Skipping {symbol}: invalid price")
                continue

            qty = max(1, int(BUDGET // price))
            if qty <= 0:
                print(f"Skipping {symbol}: insufficient funds for {symbol}")
                continue

            order_data = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY
            )

            trading_client.submit_order(order_data)
            print(f"Bought {qty} shares of {symbol} (score={row['score']})")

        except Exception as e:
            print(f"Trade error for {symbol}: {e}")

    print("Trade execution completed.")
# ====================================================
# Sell Logic (Stop-Loss & Take-Profit)
# ====================================================
def sell_logic(stop_loss=-0.05, take_profit=0.10):
    print("Checking positions for sell conditions...")

    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        print("Failed to fetch Alpaca positions:", e)
        return

    if not positions:
        print("No positions to evaluate.")
        return

    sells = []  # track sales for logging

    for pos in positions:
        try:
            symbol = pos.symbol
            qty = float(pos.qty)
            cost = float(pos.avg_entry_price)

            latest = trading_client.get_latest_trade(symbol)
            current_price = latest.price

            if not current_price or current_price <= 0:
                print(f"Skipping {symbol}: unable to read price.")
                continue

            pl_pct = (current_price - cost) / cost

            print(f"{symbol}: cost={cost}, price={current_price}, P/L={pl_pct:.2%}")

            should_sell = False
            reason = ""

            if pl_pct <= stop_loss:
                should_sell = True
                reason = f"STOP-LOSS triggered ({pl_pct:.2%})"
            elif pl_pct >= take_profit:
                should_sell = True
                reason = f"TAKE-PROFIT triggered ({pl_pct:.2%})"

            if should_sell:
                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=int(qty),
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )

                try:
                    trading_client.submit_order(order)
                    print(f"Sold {int(qty)} shares of {symbol} — {reason}")

                    sells.append({
                        "Ticker": symbol,
                        "Quantity": int(qty),
                        "SellReason": reason,
                        "SellPrice": current_price,
                        "Timestamp": dt.datetime.utcnow().isoformat()
                    })

                except Exception as e:
                    print(f"Sell error for {symbol}: {e}")

        except Exception as e:
            print(f"Error evaluating {pos.symbol}: {e}")

    if sells:
        log_sales_to_sheet(sells)

# ====================================================
# Log Sells to Google Sheets
# ====================================================
def log_sales_to_sheet(sells):
    print("Logging sales to Google Sheets...")

    worksheet = gc.open(GOOGLE_SHEET_NAME).sheet1

    # Ensure SELL LOG header exists
    header = ["Timestamp", "Ticker", "Quantity", "SellPrice", "SellReason"]
    existing_header = worksheet.row_values(1)

    if existing_header != header:
        worksheet.clear()
        worksheet.append_row(header)

    for s in sells:
        row = [
            s["Timestamp"],
            s["Ticker"],
            str(s["Quantity"]),
            str(s["SellPrice"]),
            s["SellReason"],
        ]
        worksheet.append_row(row)

    print("Sales logged.")




# ====================================================
# 7. Main Bot Flow
# ====================================================
def run_bot():
    print("Fetching trades...")
    df = fetch_congress_trades()

    print("Scoring trades...")
    df_scored = score_trades(df)

    print("Logging to Google Sheets...")
    log_to_sheet(df_scored)

    print("Executing trades...")
    execute_trades(df_scored)

    print("Evaluating sell conditions...")
    sell_logic(stop_loss=-0.05, take_profit=0.10)


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
