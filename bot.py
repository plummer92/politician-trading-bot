import os
import json
import smtplib
import requests
import pandas as pd
import numpy as np
import datetime as dt
import gspread
import asyncio

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")

# ====================================================
# 2. Neon Async Logging
# ====================================================
import db  # uses async Neon engine


# ====================================================
# 3. Google Sheets
# ====================================================
creds = ServiceAccountCredentials.from_json_keyfile_name(
    GOOGLE_APPLICATION_CREDENTIALS,
    ["https://spreadsheets.google.com/feeds",
     "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)


# ====================================================
# 4. Trailing Stop System
# ====================================================
TRAILING_FILE = "trailing_sl.json"
TRAIL_PERCENT = 0.08          # 8% trailing stop loss
TOP_LOSERS_TO_SELL = 3        # max sells/day


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
# 5. Fetch Quiver Congress Trades
# ====================================================
def fetch_congress_trades():
    url = "https://api.quiverquant.com/beta/bulk/congresstrading"
    headers = {"Authorization": f"Token {QUIVER_KEY}"}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    df["TransactionDate"] = pd.to_datetime(df["Traded"], errors="coerce")
    df = df.dropna(subset=["TransactionDate"])

    cutoff = dt.datetime.utcnow() - dt.timedelta(days=30)
    return df[df["TransactionDate"] >= cutoff]


# ====================================================
# 6. Trade Scoring Engine
# ====================================================
def score_trades(df):
    df = df.copy()
    df["score"] = 0

    # Trade size
    if "Trade_Size_USD" in df.columns:
        size = pd.to_numeric(df["Trade_Size_USD"], errors="ignore")
        df["score"] += np.where(size >= 100000, 3,
                        np.where(size >= 25000, 2, 1))

    # Buy > Sell
    df["score"] += df["Transaction"].apply(
        lambda t: 2 if str(t).upper().startswith("BUY") else
                  (-2 if str(t).upper().startswith("SELL") else 0)
    )

    # Excess return bonus
    if "excess_return" in df.columns:
        er = pd.to_numeric(df["excess_return"], errors="coerce").fillna(0)
        df["score"] += np.where(er > 0.05, 2,
                         np.where(er > 0, 1, 0))

    # Politician bonus
    df["score"] += 1

    return df.sort_values("score", ascending=False)


# ====================================================
# 7. Log Buys to Google Sheet
# ====================================================
def log_buys_to_sheet(df):
    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws = sh.sheet1
    except Exception as e:
        print("Google sheet error:", e)
        return

    cols = [
        "TransactionDate","Ticker","Company","Transaction","Trade_Size_USD",
        "Name","Party","District","Chamber","excess_return","score"
    ]
    cols = [c for c in cols if c in df.columns]

    if ws.acell("A1").value in (None, ""):
        ws.append_row(cols)

    ws.append_rows(df[cols].astype(str).values.tolist())
    print("Logged buys to sheet")


# ====================================================
# 8. Alpaca Buy Engine
# ====================================================
trading_client = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)


def get_price(symbol):
    try:
        q = trading_client.get_latest_quote(symbol)
        return q.ask_price or q.bid_price
    except:
        return None


def execute_buys(df):
    df_trade = df[df["score"] >= 6]
    if df_trade.empty:
        return []

    existing = {p.symbol for p in trading_client.get_all_positions()}
    bought = []
    BUDGET = 50

    for _, row in df_trade.iterrows():
        sym = row["Ticker"]

        if not sym or sym in existing:
            continue

        price = get_price(sym)
        if not price:
            continue

        qty = max(1, int(BUDGET // price))

        try:
            trading_client.submit_order(
                MarketOrderRequest(
                    symbol=sym,
                    qty=qty,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
            )
            bought.append((sym, qty, price))
        except Exception as e:
            print(f"Buy error {sym}:", e)

    return bought


# ====================================================
# 9. Trailing Stop Sell Engine
# ====================================================
def trailing_stop_and_sell():
    trailing = load_trailing_data()
    positions = trading_client.get_all_positions()

    if not positions:
        return []

    evaluations = []

    for pos in positions:
        sym = pos.symbol
        qty = float(pos.qty)
        cost = float(pos.avg_entry_price)
        current = float(pos.current_price)

        highest = trailing.get(sym, {}).get("highest", current)
        if current > highest:
            highest = current

        trailing[sym] = {"highest": highest}

        drop_pct = (highest - current) / highest if highest > 0 else 0
        pl_pct = (current - cost) / cost if cost > 0 else 0

        evaluations.append({
            "symbol": sym,
            "qty": qty,
            "price": current,
            "cost": cost,
            "highest": highest,
            "drop_pct": drop_pct,
            "pl_pct": pl_pct,
        })

    save_trailing_data(trailing)

    violators = [e for e in evaluations if e["drop_pct"] >= TRAIL_PERCENT]
    violators.sort(key=lambda x: x["pl_pct"])

    sells = violators[:TOP_LOSERS_TO_SELL]
    executed = []

    for row in sells:
        try:
            trading_client.submit_order(
                MarketOrderRequest(
                    symbol=row["symbol"],
                    qty=int(row["qty"]),
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
            )
            executed.append(row)
        except Exception as e:
            print(f"Sell error {row['symbol']}:", e)

    return executed


# ====================================================
# 🔔 10. Email Report
# ====================================================
def send_email_report(buys, sells):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "📈 Daily Politician Trading Bot Report"
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECIPIENT

        html = "<h3>Daily Politician Trading Bot Report</h3>"

        if buys:
            html += "<h4>Buys:</h4><ul>"
            for sym, qty, price in buys:
                html += f"<li>Bought {qty} {sym} @ ${price:.2f}</li>"
            html += "</ul>"
        else:
            html += "<p>No buys today.</p>"

        if sells:
            html += "<h4>Sells:</h4><ul>"
            for s in sells:
                html += f"<li>Sold {s['symbol']} — drop {s['drop_pct']*100:.2f}%</li>"
            html += "</ul>"
        else:
            html += "<p>No sells today.</p>"

        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())

        print("Email sent")

    except Exception as e:
        print("Email error:", e)


# ====================================================
# 11. Main Run
# ====================================================
def run_bot():
    df = fetch_congress_trades()
    df_scored = score_trades(df)

    log_buys_to_sheet(df_scored)

    buys = execute_buys(df_scored)
    sells = trailing_stop_and_sell()

    send_email_report(buys, sells)

    # Async Neon Logging
    async def async_logging():
        await db.log_run("start")
        await db.log_quiver_raw(df)
        await db.log_scored(df_scored)
        await db.log_buy(buys)
        await db.log_sell(sells)
        await db.log_run("end")

    asyncio.run(async_logging())

    print("Bot complete.")


if __name__ == "__main__":
    run_bot()
