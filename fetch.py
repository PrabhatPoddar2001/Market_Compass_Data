"""
Market Compass - daily data pull.

Runs on GitHub Actions every weekday morning and writes data.json, which the
Claude morning brief reads instead of searching the web for every number.

Sources (in this order of preference, as the brief's rules require):
  - NSE closing files (nsearchives.nseindia.com) for Indian indices, India VIX,
    P/E and the four holdings. Yahoo is used only as a labelled fallback.
  - US Treasury daily par yield curve for 3M / 5Y / 10Y / 30Y yields.
  - Yahoo Finance (via yfinance) for global indices, FX, commodities and the
    AI-company market caps.

Every value carries its own date, and anything that fails is listed under
"errors" instead of being guessed.
"""

import csv
import io
import json
import statistics
from datetime import date, datetime, timedelta, timezone

import requests
import pandas as pd
import yfinance as yf

IST = timezone(timedelta(hours=5, minutes=30))
TODAY = datetime.now(IST).date()
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

out = {"generated_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
       "errors": []}


def err(where, e):
    out["errors"].append(f"{where}: {type(e).__name__}: {str(e)[:160]}")


def r2(x, n=2):
    return None if x is None else round(float(x), n)


# --------------------------------------------------------------------------
# NSE
# --------------------------------------------------------------------------
NSE_INDICES = ["Nifty 50", "Nifty Bank", "NIFTY Midcap 100", "NIFTY Midcap 150",
               "NIFTY Smallcap 100", "NIFTY Smallcap 250", "India VIX",
               "Nifty IT", "Nifty Media", "Nifty Pharma", "Nifty Private Bank",
               "Nifty Healthcare Index", "Nifty Auto", "Nifty PSU Bank",
               "Nifty Metal", "Nifty Financial Services", "Nifty Consumer Durables",
               "Nifty Infrastructure", "Nifty FMCG", "Nifty Oil & Gas",
               "Nifty Energy", "Nifty Realty"]
HOLDINGS = ["INDHOTEL", "EXIDEIND", "BOSCHLTD", "BAJAJ-AUTO"]

nse = requests.Session()
nse.headers.update(UA)


def nse_get(url):
    r = nse.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def ind_close(d):
    """NSE all-indices closing file for date d, as {name_lower: row}."""
    txt = nse_get(f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv")
    rows = {}
    for row in csv.DictReader(io.StringIO(txt)):
        rows[row["Index Name"].strip().lower()] = row
    return rows


def bhav(d):
    """NSE equity bhavcopy for date d, as {symbol: close} (EQ series only)."""
    txt = nse_get(f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv")
    res = {}
    for row in csv.DictReader(io.StringIO(txt)):
        row = {k.strip(): (v or "").strip() for k, v in row.items()}
        if row.get("SERIES") == "EQ":
            res[row["SYMBOL"]] = float(row["CLOSE_PRICE"])
    return res


def last_on_or_before(fn, d, max_back=7):
    """Call fn on d, d-1, ... until a file exists. Returns (date, data)."""
    for i in range(max_back + 1):
        day = d - timedelta(days=i)
        if day.weekday() >= 5:
            continue
        try:
            return day, fn(day)
        except Exception:
            continue
    raise RuntimeError(f"no NSE file on or before {d}")


def months_back(d, m):
    y, mo = d.year, d.month - m
    while mo <= 0:
        mo += 12
        y -= 1
    for day in (d.day, 30, 29, 28):
        try:
            return date(y, mo, day)
        except ValueError:
            continue


def f(v):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


india = {}
try:
    # last completed session = most recent file strictly before today (IST)
    end_d, end_rows = last_on_or_before(ind_close, TODAY - timedelta(days=1))
    m3_d, m3_rows = last_on_or_before(ind_close, months_back(end_d, 3))
    m6_d, m6_rows = last_on_or_before(ind_close, months_back(end_d, 6))
    india["session"] = end_d.isoformat()
    india["window_3m_start"] = m3_d.isoformat()
    india["window_6m_start"] = m6_d.isoformat()
    idx = {}
    for name in NSE_INDICES:
        k = name.lower()
        row = end_rows.get(k)
        if not row:
            out["errors"].append(f"NSE file has no row for {name}")
            continue
        c = f(row["Closing Index Value"])
        c3 = f(m3_rows.get(k, {}).get("Closing Index Value"))
        c6 = f(m6_rows.get(k, {}).get("Closing Index Value"))
        idx[name] = {
            "close": c, "pts_chg": f(row["Points Change"]), "pct_chg": f(row["Change(%)"]),
            "high": f(row["High Index Value"]), "low": f(row["Low Index Value"]),
            "pe": f(row.get("P/E")), "pb": f(row.get("P/B")),
            "close_3m_ago": c3, "close_6m_ago": c6,
            "ret_3m_pct": r2((c / c3 - 1) * 100) if c and c3 else None,
            "ret_prev_3m_pct": r2((c3 / c6 - 1) * 100) if c3 and c6 else None,
        }
    india["indices"] = idx
    india["source"] = "NSE ind_close_all files"
except Exception as e:
    err("NSE index files", e)

try:
    end_d = date.fromisoformat(india.get("session", (TODAY - timedelta(days=1)).isoformat()))
    _, b0 = last_on_or_before(bhav, end_d)
    d3, b3 = last_on_or_before(bhav, months_back(end_d, 3))
    d6, b6 = last_on_or_before(bhav, months_back(end_d, 6))
    hold = {}
    for s in HOLDINGS:
        c, c3, c6 = b0.get(s), b3.get(s), b6.get(s)
        hold[s] = {"close": c, "close_3m_ago": c3, "close_6m_ago": c6,
                   "ret_3m_pct": r2((c / c3 - 1) * 100) if c and c3 else None,
                   "ret_6m_pct": r2((c / c6 - 1) * 100) if c and c6 else None}
    india["holdings"] = hold
    india["holdings_source"] = f"NSE bhavcopy {end_d}, {d3}, {d6}"
except Exception as e:
    err("NSE bhavcopy", e)

# 52-week high/low (NSE allIndices API needs a cookie from the home page)
try:
    nse.get("https://www.nseindia.com", timeout=30)
    j = nse.get("https://www.nseindia.com/api/allIndices", timeout=30).json()
    want = {"NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
            "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250", "INDIA VIX"}
    india["year_high_low"] = {d["index"]: {"yearHigh": d.get("yearHigh"), "yearLow": d.get("yearLow"),
                                          "last": d.get("last")}
                              for d in j.get("data", []) if d.get("index") in want}
except Exception as e:
    err("NSE allIndices (52-week range)", e)

out["india"] = india

# --------------------------------------------------------------------------
# Yahoo Finance helpers
# --------------------------------------------------------------------------


def hist(ticker, period="1y"):
    h = yf.Ticker(ticker).history(period=period, auto_adjust=False)
    h = h.dropna(subset=["Close"])
    if ticker.endswith("=X"):
        # Yahoo stamps each daily FX bar one day late: the bar dated Tuesday holds
        # Monday's close, and Friday's close sits in the bar dated Saturday or
        # Monday. Move every FX bar to the previous weekday and keep the latest.
        def prev_weekday(ts):
            d = ts - pd.Timedelta(days=1)
            while d.weekday() >= 5:
                d -= pd.Timedelta(days=1)
            return d
        h.index = pd.DatetimeIndex([prev_weekday(t) for t in h.index])
        h = h[~h.index.duplicated(keep="last")]
    h = h[[d.weekday() < 5 for d in h.index]]  # drop weekend FX ticks
    if h.empty:
        raise RuntimeError("no data")
    return h


def last_two(ticker):
    h = hist(ticker, "10d")
    last, prev = h.iloc[-1], h.iloc[-2]
    return {"close": r2(last["Close"], 4), "date": h.index[-1].date().isoformat(),
            "prev_close": r2(prev["Close"], 4),
            "pct_chg": r2((last["Close"] / prev["Close"] - 1) * 100)}


def with_52w(ticker):
    h = hist(ticker, "1y")
    d = {"close": r2(h["Close"].iloc[-1], 4), "date": h.index[-1].date().isoformat(),
         "prev_close": r2(h["Close"].iloc[-2], 4),
         "pct_chg": r2((h["Close"].iloc[-1] / h["Close"].iloc[-2] - 1) * 100),
         "low_52w_close": r2(h["Close"].min(), 4), "high_52w_close": r2(h["Close"].max(), 4)}
    return d


def grab(group, items, fn):
    res = {}
    for label, t in items.items():
        try:
            res[label] = {"ticker": t, **fn(t)}
        except Exception as e:
            err(f"{group} {label} ({t})", e)
    return res


out["global_indices"] = grab("index", {
    "S&P 500": "^GSPC", "Nikkei 225": "^N225", "KOSPI": "^KS11", "TAIEX": "^TWII",
    "Shanghai Composite": "000001.SS", "DAX": "^GDAXI", "Sensex": "^BSESN"}, last_two)

out["fx"] = grab("fx", {
    "USD/INR": "INR=X", "EUR/INR": "EURINR=X", "GBP/INR": "GBPINR=X",
    "JPY/INR": "JPYINR=X", "CNY/INR": "CNYINR=X", "USD/JPY": "JPY=X",
    "USD/KRW": "KRW=X", "USD/TWD": "TWD=X", "USD/CNY": "CNY=X", "EUR/USD": "EURUSD=X"}, with_52w)
if "CNY/INR" not in out["fx"] and "USD/INR" in out["fx"] and "USD/CNY" in out["fx"]:
    u, c = out["fx"]["USD/INR"], out["fx"]["USD/CNY"]
    out["fx"]["CNY/INR"] = {"close": r2(u["close"] / c["close"], 4), "date": u["date"],
                            "note": "derived as USD/INR / USD/CNY (Yahoo has no direct CNYINR series)"}
if "JPY/INR" in out["fx"]:
    out["fx"]["JPY/INR"]["note"] = "per 1 yen; multiply by 100 for the brief's per-100 tile"

# Fallback only if NSE could not be reached (GitHub's servers are in the US and
# NSE sometimes blocks them). Clearly labelled so the brief marks it as such.
if not india.get("indices") or not india.get("holdings"):
    fb = {}
    if not india.get("indices"):
        fb["indices"] = grab("India fallback", {
            "Nifty 50": "^NSEI", "Nifty Bank": "^NSEBANK", "India VIX": "^INDIAVIX"}, last_two)
    if not india.get("holdings"):
        fb["holdings"] = grab("holdings fallback", {s: s + ".NS" for s in HOLDINGS}, last_two)
    india["yahoo_fallback"] = fb
    india["yahoo_fallback_note"] = ("NSE unreachable from GitHub Actions; Yahoo values shown. "
                                    "Midcap/Smallcap, sectors and P/E need NSE.")

out["commodities"] = grab("commodity", {
    "Brent": "BZ=F", "WTI": "CL=F", "Gold": "GC=F", "Silver": "SI=F",
    "Copper": "HG=F", "Henry Hub gas": "NG=F", "Soybean oil": "ZL=F"}, last_two)

# India VIX 52-week box-plot stats (Yahoo history; today's close comes from NSE above)
try:
    v = hist("^INDIAVIX", "1y")["Close"].tolist()
    q = statistics.quantiles(v, n=4, method="inclusive")
    out["india_vix_52w"] = {"n": len(v), "min": r2(min(v)), "q1": r2(q[0]), "median": r2(q[1]),
                            "q3": r2(q[2]), "max": r2(max(v)), "source": "Yahoo ^INDIAVIX daily closes"}
except Exception as e:
    err("India VIX history", e)

# --------------------------------------------------------------------------
# US Treasury par yield curve
# --------------------------------------------------------------------------
try:
    url = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
           f"daily-treasury-rates.csv/{TODAY.year}/all?type=daily_treasury_yield_curve"
           f"&field_tdr_date_value={TODAY.year}&page&_format=csv")
    rows = list(csv.DictReader(io.StringIO(requests.get(url, headers=UA, timeout=30).text)))
    rows.sort(key=lambda r: datetime.strptime(r["Date"], "%m/%d/%Y"), reverse=True)
    pick = lambda r: {"date": datetime.strptime(r["Date"], "%m/%d/%Y").date().isoformat(),
                      "3M": f(r.get("3 Mo")), "5Y": f(r.get("5 Yr")),
                      "10Y": f(r.get("10 Yr")), "30Y": f(r.get("30 Yr"))}
    out["treasury_yields"] = {"latest": pick(rows[0]), "previous": pick(rows[1]),
                              "source": "US Treasury daily par yield curve"}
except Exception as e:
    err("US Treasury yield curve", e)
    try:  # fallback: Yahoo yield indices
        out["treasury_yields"] = {"yahoo_fallback": grab("yield", {
            "3M": "^IRX", "5Y": "^FVX", "10Y": "^TNX", "30Y": "^TYX"}, last_two)}
    except Exception as e2:
        err("Yahoo yields", e2)

# --------------------------------------------------------------------------
# AI complex market caps (USD billions)
# --------------------------------------------------------------------------
AI = {"Nvidia": "NVDA", "Broadcom": "AVGO", "AMD": "AMD", "Arm": "ARM", "Marvell": "MRVL",
      "Qualcomm": "QCOM", "TSMC": "TSM", "Samsung Electronics": "005930.KS", "Intel": "INTC",
      "Micron": "MU", "SK Hynix": "000660.KS", "ASML": "ASML", "Lam Research": "LRCX",
      "Applied Materials": "AMAT", "KLA": "KLAC", "Cadence": "CDNS", "Synopsys": "SNPS",
      "Arista Networks": "ANET", "Vertiv": "VRT", "Alphabet": "GOOGL", "Microsoft": "MSFT",
      "Amazon": "AMZN", "Meta": "META", "Oracle": "ORCL", "Alibaba": "BABA", "CoreWeave": "CRWV"}
krw = (out["fx"].get("USD/KRW") or {}).get("close")
ai = {}
for name, t in AI.items():
    try:
        tk = yf.Ticker(t)
        px = last_two(t)
        cap = tk.fast_info.get("marketCap")
        if cap and t.endswith(".KS"):
            cap = cap / krw if krw else None  # KRW -> USD
        ai[name] = {"ticker": t, **px, "market_cap_usd_bn": r2(cap / 1e9, 1) if cap else None}
    except Exception as e:
        err(f"AI {name} ({t})", e)
out["ai_market_caps"] = ai

# ---------- Daily history for the S&P 500 / Sensex rupee-dollar chart ----------
# Rolling 12 months plus a margin: one row per weekday when either market traded;
# a market that was shut carries its last close forward.
try:
    s = hist("^GSPC", "13mo")["Close"]
    x = hist("^BSESN", "13mo")["Close"]
    r = hist("INR=X", "13mo")["Close"]
    def by_day(ser):
        d = {}
        for k, v in ser.items():
            d[k.date()] = float(v)
        return d
    s, x, r = by_day(s), by_day(x), by_day(r)
    days = sorted(set(s) | set(x))
    rows, ls, lx, lr = [], None, None, None
    for d in days:
        ls, lx, lr = s.get(d, ls), x.get(d, lx), r.get(d, lr)
        if None not in (ls, lx, lr):
            rows.append([d.isoformat(), r2(ls), r2(lx), r2(lr, 4)])
    out["spinr_history"] = {"columns": ["date", "sp500", "sensex", "usdinr"],
                            "source": "Yahoo Finance daily closes (^GSPC, ^BSESN, INR=X)",
                            "rows": rows}
except Exception as e:
    err("spinr_history", e)

with open("data.json", "w") as fh:
    json.dump(out, fh, indent=1, default=str)
print(f"wrote data.json with {len(out['errors'])} error(s)")
for e in out["errors"]:
    print(" -", e)
