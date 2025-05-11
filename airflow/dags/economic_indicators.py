import pandas as pd
import requests
import psycopg2
from datetime import datetime
from bs4 import BeautifulSoup

def update_economic_indicators():
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # --- 1. Get USD/IDR exchange rate ---
    try:
        fx_url = "https://api.exchangerate.host/latest?base=USD&symbols=IDR"
        fx_response = requests.get(fx_url).json()
        usd_idr = fx_response['rates']['IDR']
    except Exception as e:
        print(f"Error fetching USD/IDR: {e}")
        usd_idr = None

    # --- 2. Get BI rate (from BI page) ---
    try:
        bi_url = "https://www.bi.go.id/id/moneter/bi-rate/Default.aspx"
        response = requests.get(bi_url)
        soup = BeautifulSoup(response.text, "html.parser")
        rate_element = soup.find("div", {"class": "table-responsive"}).find("td")
        bi_rate = float(rate_element.text.replace(",", "."))
    except Exception as e:
        print(f"Error fetching BI rate: {e}")
        bi_rate = None

    # --- 3. Get inflation (from BPS monthly PDF/API/manual scrape) ---
    try:
        # Placeholder (should be replaced with real scrape or static value)
        inflation = 2.85
    except:
        inflation = None

    # --- 4. Get IHSG close from Yahoo Finance API ---
    try:
        import yfinance as yf
        jci = yf.Ticker("^JKSE")
        hist = jci.history(period="2d")
        jci_close = hist['Close'].iloc[-1]
    except Exception as e:
        print(f"Error fetching JCI: {e}")
        jci_close = None

    today = datetime.today().date()

    # --- Insert to DB ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS economic_indicators (
            date DATE PRIMARY KEY,
            exchange_rate_usd_idr FLOAT,
            interest_rate FLOAT,
            inflation_rate FLOAT,
            jci_index_close FLOAT
        );
    """)

    cur.execute("""
        INSERT INTO economic_indicators (date, exchange_rate_usd_idr, interest_rate, inflation_rate, jci_index_close)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            exchange_rate_usd_idr = EXCLUDED.exchange_rate_usd_idr,
            interest_rate = EXCLUDED.interest_rate,
            inflation_rate = EXCLUDED.inflation_rate,
            jci_index_close = EXCLUDED.jci_index_close
    """, (today, usd_idr, bi_rate, inflation, jci_close))

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Updated economic indicators for {today}")
