#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import os
import csv

OUT_PATH = r'C:\Users\Aditya\PyCharmMiscProject\nifty_50_tracker_yf.csv'
TICKER = "^NSEI"  # Yahoo ticker for Nifty 50 (commonly ^NSEI). Verify if needed.

def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def append_row(row, out_path=OUT_PATH):
    ensure_dir(out_path)
    write_header = not os.path.exists(out_path)
    with open(out_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["date","open","high","low","close","change","percent_change","source_timestamp"])
        writer.writerow(row)

def main():
    # fetch latest day (interval 1d)
    ticker = yf.Ticker(TICKER)
    hist = ticker.history(period="2d", interval="1d")  # last 2 days to compute change if needed
    if hist.empty:
        print("No data returned")
        return
    # take last available row
    last = hist.iloc[-1]
    # print(hist)
    # date
    date = last.name.strftime("%Y-%m-%d")
    open_p = round(float(last["Open"]), 2)
    high_p = round(float(last["High"]), 2)
    low_p = round(float(last["Low"]), 2)
    close_p = round(float(last["Close"]), 2)
    # compute change and percent_change using previous close if available
    if len(hist) >= 2:
        prev_close = float(hist.iloc[-2]["Close"])
        change = close_p - prev_close
        pct = round(((change / prev_close) * 100) , 2) if prev_close != 0 else 0.0
    else:
        change = 0.0
        pct = 0.0
    ts = datetime.now(timezone.utc).isoformat()
    row = [date, open_p, high_p, low_p, close_p, round(change, 4), round(pct, 4), ts]
    append_row(row)
    print("Appended:", row)

if __name__ == "__main__":
    main()
