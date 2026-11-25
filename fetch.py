"""
Market Compass - daily data pull (v2).

Runs on GitHub Actions every weekday morning and writes data.json, which the
Claude morning brief reads instead of searching the web for every number.

What v2 changes compared with v1
  * Never loses data: if a source fails, the last good value from the previous
    data.json is kept and marked "stale" with its own date, instead of the
    whole section disappearing.
  * Retries with back-off on every network call, and tells a holiday (file not
    found) apart from a block or timeout, so a blocked NSE day is never
    silently reported as "the last session".
  * Much faster: one batched Yahoo download for every ticker (instead of ~70
    one-by-one calls), and NSE files fetched in parallel.
  * A "health" block at the top of data.json says, per section, ok / partial /
    stale / failed and the as-of date, so the brief can trust it at a glance.
  * Sanity checks flag implausible moves under "warnings" (values are kept).
  * New: FII/DII daily cash flows (NSE), Treasury month-end history,
    US Treasury fallback across year boundaries, CNY/INR always derived.
  * Atomic write: data.json is replaced only by a complete, valid file.
  * `python fetch.py --check` exits 1 if a critical section is stale, so the
    workflow goes red and GitHub e-mails you.

Output keys used by v1 are all kept with the same shape.
"""

import csv
import io
import json
import os
import random
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
OUT_FILE = os.environ.get("MC_OUT", "data.json")
IST = timezone(timedelta(hours=5, minutes=30))
NOW_IST = datetime.now(IST)
TODAY = NOW_IST.date()

# Before 19:00 IST the NSE files for today are not final, so the newest session
# we look for is yesterday. Run it in the evening and it picks up today's.
NSE_SEARCH_FROM = TODAY if NOW_IST.hour >= 19 else TODAY - timedelta(days=1)

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

NSE_INDICES = ["Nifty 50", "Nifty Bank", "NIFTY Midcap 100", "NIFTY Midcap 150",
               "NIFTY Smallcap 100", "NIFTY Smallcap 250", "India VIX",
               "Nifty IT", "Nifty Media", "Nifty Pharma", "Nifty Private Bank",
               "Nifty Healthcare Index", "Nifty Auto", "Nifty PSU Bank",
               "Nifty Metal", "Nifty Financial Services", "Nifty Consumer Durables",
               "Nifty Infrastructure", "Nifty FMCG", "Nifty Oil & Gas",
               "Nifty Energy", "Nifty Realty"]
HOLDINGS = ["INDHOTEL", "EXIDEIND", "BOSCHLTD", "BAJAJ-AUTO"]
YEAR_HL_WANT = {"NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
                "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250", "INDIA VIX"}

GLOBAL_INDICES = {"S&P 500": "^GSPC", "Nikkei 225": "^N225", "KOSPI": "^KS11",
                  "TAIEX": "^TWII", "Shanghai Composite": "000001.SS",
                  "DAX": "^GDAXI", "Sensex": "^BSESN"}
FX = {"USD/INR": "INR=X", "EUR/INR": "EURINR=X", "GBP/INR": "GBPINR=X",
      "JPY/INR": "JPYINR=X", "USD/JPY": "JPY=X", "USD/KRW": "KRW=X",
      "USD/TWD": "TWD=X", "USD/CNY": "CNY=X", "EUR/USD": "EURUSD=X"}
COMMODITIES = {"Brent": "BZ=F", "WTI": "CL=F", "Gold": "GC=F", "Silver": "SI=F",
               "Copper": "HG=F", "Henry Hub gas": "NG=F", "Soybean oil": "ZL=F"}
AI = {"Nvidia": "NVDA", "Broadcom": "AVGO", "AMD": "AMD", "Arm": "ARM", "Marvell": "MRVL",
      "Qualcomm": "QCOM", "TSMC": "TSM", "Samsung Electronics": "005930.KS", "Intel": "INTC",
      "Micron": "MU", "SK Hynix": "000660.KS", "ASML": "ASML", "Lam Research": "LRCX",
      "Applied Materials": "AMAT", "KLA": "KLAC", "Cadence": "CDNS", "Synopsys": "SNPS",
      "Arista Networks": "ANET", "Vertiv": "VRT", "Alphabet": "GOOGL", "Microsoft": "MSFT",
      "Amazon": "AMZN", "Meta": "META", "Oracle": "ORCL", "Alibaba": "BABA", "CoreWeave": "CRWV"}
YIELDS_YAHOO = {"3M": "^IRX", "5Y": "^FVX", "10Y": "^TNX", "30Y": "^TYX"}
INDIA_FALLBACK = {"Nifty 50": "^NSEI", "Nifty Bank": "^NSEBANK", "India VIX": "^INDIAVIX"}

# Sections that must be fresh for `--check` to pass.
CRITICAL = ["india", "global_indices", "fx", "spinr_history"]

# Daily moves above these are flagged under "warnings" (value is still kept).
MOVE_LIMIT = {"index": 12, "fx": 6, "commodity": 15, "stock": 35}

# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
out = {"generated_at_ist": NOW_IST.strftime("%Y-%m-%d %H:%M"),
       "script_version": 2, "errors": [], "warnings": []}
T0 = time.time()


def err(where, e):
    msg = f"{where}: {type(e).__name__}: {str(e)[:200]}"
    out["errors"].append(msg)
    print("  ERROR", msg, flush=True)


def warn(msg):
    out["warnings"].append(msg)
    print("  WARN ", msg, flush=True)


def r2(x, n=2):
    try:
        if x is None or pd.isna(x):
            return None
        return round(float(x), n)
    except Exception:
        return None


def num(v):
    try:
        s = str(v).replace(",", "").strip()
        if s in ("", "-", "NA", "N/A", "nan"):
            return None
        return float(s)
    except Exception:
        return None


def pct(a, b):
    return r2((a / b - 1) * 100) if a and b else None


def log(msg):
    print(f"[{time.time() - T0:5.1f}s] {msg}", flush=True)


class NotFound(Exception):
    """The server answered 404: the file does not exist (holiday / not yet out)."""


def http_get(url, session=None, tries=4, timeout=(10, 40), backoff=2.0):
    """GET with retries and jittered exponential back-off.
    404 raises NotFound at once (no point retrying). Everything else retries."""
    s = session or requests
    last = None
    for i in range(tries):
        try:
            r = s.get(url, headers=UA, timeout=timeout)
            if r.status_code == 404:
                raise NotFound(url)
            r.raise_for_status()
            if not r.content:
                raise RuntimeError("empty response")
            return r
        except NotFound:
            raise
        except Exception as e:  # timeouts, 403/429/5xx, resets
            last = e
            if i < tries - 1:
                time.sleep(backoff * (2 ** i) + random.uniform(0, 1))
    raise last


def load_previous():
    try:
        with open(OUT_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


PREV = load_previous()


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


# --------------------------------------------------------------------------
# NSE
# --------------------------------------------------------------------------
nse = requests.Session()
nse.headers.update(UA)

NSE_HOSTS = ["https://nsearchives.nseindia.com", "https://archives.nseindia.com"]


def nse_archive(path):
    """Try the main archive host, then the old mirror. 404 on all = NotFound."""
    nf, last = 0, None
    for host in NSE_HOSTS:
        try:
            return http_get(host + path, session=nse, tries=3).text
        except NotFound:
            nf += 1
        except Exception as e:
            last = e
    if nf == len(NSE_HOSTS):
        raise NotFound(path)
    raise last


def ind_close(d):
    txt = nse_archive(f"/content/indices/ind_close_all_{d:%d%m%Y}.csv")
    rows = {}
    for row in csv.DictReader(io.StringIO(txt)):
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        if row.get("Index Name"):
            rows[row["Index Name"].lower()] = row
    if len(rows) < 20:
        raise RuntimeError(f"ind_close file for {d} looks truncated ({len(rows)} rows)")
    return rows


def bhav(d):
    txt = nse_archive(f"/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv")
    res = {}
    for row in csv.DictReader(io.StringIO(txt)):
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        if row.get("SERIES") == "EQ":
            c = num(row.get("CLOSE_PRICE"))
            if c:
                res[row["SYMBOL"]] = c
    if len(res) < 500:
        raise RuntimeError(f"bhavcopy for {d} looks truncated ({len(res)} rows)")
    return res


def latest_file(fn, d, max_back=10, strict=True):
    """Newest weekday <= d whose file exists. A day counts as a holiday only if
    NSE answered 404. If a newer day failed for another reason (block, timeout)
    and strict=True, raise instead of quietly returning an older session."""
    blocked = []
    for i in range(max_back + 1):
        day = d - timedelta(days=i)
        if day.weekday() >= 5:
            continue
        try:
            return day, fn(day)
        except NotFound:
            continue
        except Exception as e:
            blocked.append(f"{day}: {type(e).__name__}")
            if strict:
                raise RuntimeError(f"NSE unreachable for {day} ({e}); refusing to fall back "
                                   f"to an older session") from e
    raise RuntimeError(f"no NSE file on or before {d}; blocked: {blocked}")


def fetch_nse_core():
    india = {}
    end_d, end_rows = latest_file(ind_close, NSE_SEARCH_FROM)
    log(f"NSE last session {end_d}")
    t3, t6 = months_back(end_d, 3), months_back(end_d, 6)
    with ThreadPoolExecutor(5) as ex:
        f_m3 = ex.submit(latest_file, ind_close, t3, 10, False)
        f_m6 = ex.submit(latest_file, ind_close, t6, 10, False)
        f_b0 = ex.submit(latest_file, bhav, end_d, 0)
        f_b3 = ex.submit(latest_file, bhav, t3, 10, False)
        f_b6 = ex.submit(latest_file, bhav, t6, 10, False)
        m3_d, m3_rows = f_m3.result()
        m6_d, m6_rows = f_m6.result()

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
            c = num(row.get("Closing Index Value"))
            c3 = num(m3_rows.get(k, {}).get("Closing Index Value"))
            c6 = num(m6_rows.get(k, {}).get("Closing Index Value"))
            idx[name] = {
                "close": c, "pts_chg": num(row.get("Points Change")),
                "pct_chg": num(row.get("Change(%)")),
                "high": num(row.get("High Index Value")), "low": num(row.get("Low Index Value")),
                "pe": num(row.get("P/E")), "pb": num(row.get("P/B")),
                "close_3m_ago": c3, "close_6m_ago": c6,
                "ret_3m_pct": pct(c, c3), "ret_prev_3m_pct": pct(c3, c6),
            }
            if idx[name]["pct_chg"] is not None and abs(idx[name]["pct_chg"]) > MOVE_LIMIT["index"] \
                    and name != "India VIX":
                warn(f"NSE {name} moved {idx[name]['pct_chg']}% on {end_d} - check")
        india["indices"] = idx
        india["source"] = "NSE ind_close_all files"

        try:
            _, b0 = f_b0.result()
            d3, b3 = f_b3.result()
            d6, b6 = f_b6.result()
            hold = {}
            for s in HOLDINGS:
                c, c3, c6 = b0.get(s), b3.get(s), b6.get(s)
                if c is None:
                    out["errors"].append(f"bhavcopy {end_d} has no EQ row for {s}")
                hold[s] = {"close": c, "close_3m_ago": c3, "close_6m_ago": c6,
                           "ret_3m_pct": pct(c, c3), "ret_6m_pct": pct(c, c6)}
            india["holdings"] = hold
            india["holdings_source"] = f"NSE bhavcopy {end_d}, {d3}, {d6}"
        except Exception as e:
            err("NSE bhavcopy", e)
    return india


def nse_api_session():
    """NSE's JSON API needs cookies from the home page first."""
    s = requests.Session()
    s.headers.update({**UA, "Referer": "https://www.nseindia.com/"})
    for u in ("https://www.nseindia.com", "https://www.nseindia.com/market-data/live-market-indices"):
        try:
            s.get(u, timeout=(10, 20))
        except Exception:
            pass
    return s


def nse_api(s, path):
    return http_get("https://www.nseindia.com" + path, session=s, tries=3).json()


def fetch_nse_api():
    """52-week range and FII/DII flows. Both best-effort."""
    res = {}
    s = nse_api_session()
    try:
        j = nse_api(s, "/api/allIndices")
        res["year_high_low"] = {d["index"]: {"yearHigh": d.get("yearHigh"),
                                             "yearLow": d.get("yearLow"), "last": d.get("last")}
                                for d in j.get("data", []) if d.get("index") in YEAR_HL_WANT}
        if not res["year_high_low"]:
            raise RuntimeError("allIndices returned no matching rows")
    except Exception as e:
        err("NSE allIndices (52-week range)", e)
    try:
        j = nse_api(s, "/api/fiidiiTradeReact")
        rows = j if isinstance(j, list) else j.get("data", [])
        flows = {}
        for r in rows:
            cat = str(r.get("category", "")).upper()
            key = "FII" if ("FII" in cat or "FPI" in cat) else ("DII" if "DII" in cat else None)
            if not key:
                continue
            d = r.get("date")
            try:
                d = datetime.strptime(d, "%d-%b-%Y").date().isoformat()
            except Exception:
                pass
            flows[key] = {"date": d, "buy_cr": num(r.get("buyValue")),
                          "sell_cr": num(r.get("sellValue")), "net_cr": num(r.get("netValue"))}
        if not flows:
            raise RuntimeError("fiidiiTradeReact returned no FII/DII rows")
        flows["source"] = "NSE fiidiiTradeReact (provisional, NSE+BSE+MSEI cash market, Rs crore)"
        res["fii_dii"] = flows
    except Exception as e:
        err("NSE FII/DII flows", e)
    return res


# --------------------------------------------------------------------------
# Yahoo Finance: one batched download, then per-ticker retries for gaps
# --------------------------------------------------------------------------
def _prev_weekday(ts):
    d = ts - pd.Timedelta(days=1)
    while d.weekday() >= 5:
        d -= pd.Timedelta(days=1)
    return d


def clean(ticker, h):
    """Same conventions as v1: drop empty bars, shift FX bars to the previous
    weekday (Yahoo stamps them a day late), drop weekend ticks."""
    if h is None or len(h) == 0 or "Close" not in h:
        raise RuntimeError("no data")
    h = h.dropna(subset=["Close"]).copy()
    if getattr(h.index, "tz", None) is not None:
        h.index = h.index.tz_localize(None)
    h.index = pd.DatetimeIndex(h.index).normalize()
    if ticker.endswith("=X"):
        h.index = pd.DatetimeIndex([_prev_weekday(t) for t in h.index])
        h = h[~h.index.duplicated(keep="last")]
    h = h[[d.weekday() < 5 for d in h.index]]
    h = h[~h.index.duplicated(keep="last")].sort_index()
    if h.empty:
        raise RuntimeError("no data")
    return h


def yahoo_batch(tickers, period="13mo"):
    """Returns {ticker: DataFrame}. Missing tickers are retried one by one."""
    data = {}
    for attempt in range(3):
        todo = [t for t in tickers if t not in data]
        if not todo:
            break
        try:
            raw = yf.download(todo, period=period, interval="1d", group_by="ticker",
                              auto_adjust=False, threads=True, progress=False, timeout=30)
            for t in todo:
                try:
                    sub = raw[t] if isinstance(raw.columns, pd.MultiIndex) else raw
                    data[t] = clean(t, sub)
                except Exception:
                    pass
        except Exception as e:
            log(f"batch download attempt {attempt + 1} failed: {e}")
        if len(data) < len(tickers):
            time.sleep(3 * (attempt + 1))
    # one-by-one for anything still missing - unless Yahoo returned nothing at
    # all (it is down or blocking us; per-ticker retries would only burn minutes)
    missing = [t for t in tickers if t not in data]
    if not data:
        log("Yahoo returned no data at all; skipping per-ticker retries")
        return data
    for t in missing:
        for i in range(3):
            try:
                data[t] = clean(t, yf.Ticker(t).history(period=period, auto_adjust=False))
                break
            except Exception:
                time.sleep(2 * (i + 1))
    return data


def last_two(h):
    last, prev = h.iloc[-1], h.iloc[-2]
    return {"close": r2(last["Close"], 4), "date": h.index[-1].date().isoformat(),
            "prev_close": r2(prev["Close"], 4), "pct_chg": pct(last["Close"], prev["Close"])}


def with_52w(h):
    d = last_two(h)
    cut = h.index[-1] - pd.Timedelta(days=365)
    y = h[h.index > cut]["Close"]
    d["low_52w_close"], d["high_52w_close"] = r2(y.min(), 4), r2(y.max(), 4)
    return d


def build(group, items, Y, fn, kind):
    res = {}
    for label, t in items.items():
        try:
            if t not in Y:
                raise RuntimeError("no data from Yahoo after retries")
            res[label] = {"ticker": t, **fn(Y[t])}
            pc = res[label].get("pct_chg")
            if pc is not None and abs(pc) > MOVE_LIMIT[kind]:
                warn(f"{group} {label} moved {pc}% - check")
            age = (TODAY - date.fromisoformat(res[label]["date"])).days
            if age > 6:
                warn(f"{group} {label} last bar is {age} days old ({res[label]['date']})")
        except Exception as e:
            err(f"{group} {label} ({t})", e)
    return res


def shares_outstanding(tickers):
    """Shares per ticker via fast_info, in parallel. Missing ones are None."""
    def one(t):
        for i in range(2):
            try:
                fi = yf.Ticker(t).fast_info
                # marketCap / lastPrice first: identical basis to v1's market caps
                cap, px = fi.get("marketCap"), fi.get("lastPrice")
                if cap and px:
                    return t, float(cap) / float(px), None
                sh = fi.get("shares")
                if sh:
                    return t, float(sh), None
            except Exception:
                time.sleep(1 + i)
        return t, None, None
    with ThreadPoolExecutor(8) as ex:
        return {t: sh for t, sh, _ in ex.map(one, tickers)}


# --------------------------------------------------------------------------
# US Treasury par yield curve
# --------------------------------------------------------------------------
def treasury_year(y):
    url = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
           f"daily-treasury-rates.csv/{y}/all?type=daily_treasury_yield_curve"
           f"&field_tdr_date_value={y}&page&_format=csv")
    rows = list(csv.DictReader(io.StringIO(http_get(url).text)))
    res = []
    for r in rows:
        try:
            res.append({"date": datetime.strptime(r["Date"], "%m/%d/%Y").date().isoformat(),
                        "3M": num(r.get("3 Mo")), "5Y": num(r.get("5 Yr")),
                        "10Y": num(r.get("10 Yr")), "30Y": num(r.get("30 Yr"))})
        except Exception:
            continue
    return res


def fetch_treasury():
    years = [TODAY.year - 1, TODAY.year] if TODAY.year > 2025 else [2025]
    if 2025 not in years:
        years = [2025] + years  # the brief's chart starts in Jan 2025
    rows = []
    with ThreadPoolExecutor(len(years)) as ex:
        for part in ex.map(treasury_year, sorted(set(years))):
            rows += part
    rows = sorted({r["date"]: r for r in rows if r["10Y"] is not None}.values(),
                  key=lambda r: r["date"])
    if len(rows) < 2:
        raise RuntimeError("fewer than 2 Treasury rows")
    month_end = {}
    for r in rows:
        month_end[r["date"][:7]] = r  # last row of each month wins
    return {"latest": rows[-1], "previous": rows[-2],
            "month_ends": [month_end[k] for k in sorted(month_end) if k >= "2025-01"],
            "source": "US Treasury daily par yield curve"}


# --------------------------------------------------------------------------
# Carry-forward of last good values
# --------------------------------------------------------------------------
def carry_items(section, required=None):
    """Per-item merge for dict sections (fx, commodities, ...): items missing
    today are copied from the previous file and marked stale."""
    new = out.get(section) or {}
    old = PREV.get(section) or {}
    for k, v in old.items():
        if k not in new and isinstance(v, dict) and (required is None or k in required):
            new[k] = {**v, "stale": True}
    out[section] = new


def carry_section(section, reason):
    if PREV.get(section):
        v = PREV[section]
        if isinstance(v, dict):
            v = {**v, "stale": True, "stale_reason": reason,
                 "carried_from_run": PREV.get("generated_at_ist")}
        out[section] = v
        warn(f"{section}: kept previous values ({reason})")


# --------------------------------------------------------------------------
# Health summary
# --------------------------------------------------------------------------
def summarize():
    h = {}

    def items_status(sec):
        v = out.get(sec) or {}
        items = {k: x for k, x in v.items() if isinstance(x, dict)}
        if not items:
            return {"status": "failed"}
        stale = [k for k, x in items.items() if x.get("stale")]
        dates = sorted({x.get("date") for x in items.values() if x.get("date")})
        st = "ok" if not stale else ("stale" if len(stale) == len(items) else "partial")
        r = {"status": st, "items": len(items), "stale_items": stale}
        if dates:
            r["as_of"] = dates[-1]
        return r

    for sec in ("global_indices", "fx", "commodities", "ai_market_caps"):
        h[sec] = items_status(sec)

    ind = out.get("india") or {}
    if not ind.get("indices"):
        h["india"] = {"status": "failed"}
    else:
        st = "stale" if ind.get("stale") else "ok"
        if st == "ok" and (not ind.get("holdings") or not ind.get("year_high_low")):
            st = "partial"
        h["india"] = {"status": st, "as_of": ind.get("session"),
                      "has_holdings": bool(ind.get("holdings")),
                      "has_52w": bool(ind.get("year_high_low")),
                      "has_fii_dii": bool(ind.get("fii_dii"))}
    for sec, key in (("treasury_yields", "latest"), ("spinr_history", "rows"),
                     ("india_vix_52w", "n")):
        v = out.get(sec)
        if not v:
            h[sec] = {"status": "failed"}
            continue
        r = {"status": "stale" if v.get("stale") else "ok"}
        if sec == "treasury_yields" and v.get("latest"):
            r["as_of"] = v["latest"].get("date")
        if sec == "spinr_history" and v.get("rows"):
            r["as_of"] = v["rows"][-1][0]
        h[sec] = r
    h["critical_ok"] = all(h.get(s, {}).get("status") in ("ok", "partial") for s in CRITICAL)
    h["seconds"] = round(time.time() - T0, 1)
    return h


def write_atomic(obj, path):
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".data.", suffix=".json", dir=d)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=1, default=str)
    with open(tmp, encoding="utf-8") as fh:
        json.load(fh)  # validate before replacing the real file
    os.replace(tmp, path)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    log(f"start; NSE search from {NSE_SEARCH_FROM}; previous run {PREV.get('generated_at_ist')}")

    # Everything independent runs at the same time.
    yahoo_tickers = sorted(set(GLOBAL_INDICES.values()) | set(FX.values()) |
                           set(COMMODITIES.values()) | set(AI.values()) |
                           set(INDIA_FALLBACK.values()) | set(YIELDS_YAHOO.values()) |
                           {s + ".NS" for s in HOLDINGS})
    with ThreadPoolExecutor(5) as ex:
        f_nse = ex.submit(fetch_nse_core)
        f_api = ex.submit(fetch_nse_api)
        f_tsy = ex.submit(fetch_treasury)
        f_yh = ex.submit(yahoo_batch, yahoo_tickers)
        f_sh = ex.submit(shares_outstanding, [t for t in AI.values()])

        Y = f_yh.result()
        log(f"Yahoo: {len(Y)}/{len(yahoo_tickers)} tickers")

        # ---- India (NSE) ----
        india = {}
        try:
            india = f_nse.result()
        except Exception as e:
            err("NSE index files", e)
        try:
            india.update(f_api.result())
        except Exception as e:
            err("NSE API", e)
        # 52-week ranges: any index NSE left out this time keeps its last value
        old_hl = (PREV.get("india") or {}).get("year_high_low") or {}
        if india.get("year_high_low"):
            for k, v in old_hl.items():
                if k not in india["year_high_low"] and isinstance(v, dict):
                    india["year_high_low"][k] = {**v, "stale": True}
        if india.get("indices"):
            # keep yesterday's 52w / flows if only those failed
            for k in ("year_high_low", "fii_dii", "holdings"):
                if not india.get(k) and (PREV.get("india") or {}).get(k):
                    india[k] = {**PREV["india"][k], "stale": True}
                    if k == "holdings":
                        india["holdings_source"] = (PREV["india"].get("holdings_source", "") +
                                                    " (stale: carried from previous run)")
        out["india"] = india
        if not india.get("indices"):
            fresh_api = {k: india[k] for k in ("year_high_low", "fii_dii") if india.get(k)}
            carry_section("india", "NSE unreachable this run")
            out.setdefault("india", {}).update(fresh_api)
            # Labelled Yahoo fallback, like v1, so there is at least a fresh Nifty.
            fb = {"indices": build("India fallback", INDIA_FALLBACK, Y, last_two, "index"),
                  "holdings": build("holdings fallback", {s: s + ".NS" for s in HOLDINGS},
                                    Y, last_two, "stock")}
            out.setdefault("india", {})
            out["india"]["yahoo_fallback"] = fb
            out["india"]["yahoo_fallback_note"] = (
                "NSE unreachable from GitHub Actions; Yahoo values shown. "
                "Midcap/Smallcap, sectors and P/E are from the previous run.")

        # ---- Yahoo sections ----
        out["global_indices"] = build("index", GLOBAL_INDICES, Y, last_two, "index")
        out["fx"] = build("fx", FX, Y, with_52w, "fx")
        u, c = out["fx"].get("USD/INR"), out["fx"].get("USD/CNY")
        if u and c and u.get("close") and c.get("close"):
            hu, hc = Y.get("INR=X"), Y.get("CNY=X")
            cny = {"close": r2(u["close"] / c["close"], 4), "date": u["date"],
                   "note": "derived as USD/INR / USD/CNY (Yahoo has no direct CNYINR series)"}
            if u.get("prev_close") and c.get("prev_close"):
                cny["prev_close"] = r2(u["prev_close"] / c["prev_close"], 4)
                cny["pct_chg"] = pct(cny["close"], cny["prev_close"])
            try:
                ratio = (hu["Close"] / hc["Close"]).dropna()
                ratio = ratio[ratio.index > ratio.index[-1] - pd.Timedelta(days=365)]
                cny["low_52w_close"], cny["high_52w_close"] = r2(ratio.min(), 4), r2(ratio.max(), 4)
            except Exception:
                pass
            out["fx"]["CNY/INR"] = cny
        if "JPY/INR" in out["fx"]:
            out["fx"]["JPY/INR"]["note"] = "per 1 yen; multiply by 100 for the brief's per-100 tile"
        out["commodities"] = build("commodity", COMMODITIES, Y, last_two, "commodity")

        # ---- India VIX 52-week stats ----
        try:
            h = Y["^INDIAVIX"]
            v = h[h.index > h.index[-1] - pd.Timedelta(days=365)]["Close"].tolist()
            q = statistics.quantiles(v, n=4, method="inclusive")
            out["india_vix_52w"] = {"n": len(v), "min": r2(min(v)), "q1": r2(q[0]),
                                    "median": r2(q[1]), "q3": r2(q[2]), "max": r2(max(v)),
                                    "source": "Yahoo ^INDIAVIX daily closes"}
        except Exception as e:
            err("India VIX history", e)

        # ---- Treasury ----
        try:
            out["treasury_yields"] = f_tsy.result()
        except Exception as e:
            err("US Treasury yield curve", e)
            fb = build("yield", YIELDS_YAHOO, Y, last_two, "index")
            if fb:
                out["treasury_yields"] = {"yahoo_fallback": fb,
                                          "note": "Yahoo CBOE yield indices; ^TNX etc. are "
                                                  "already in percent"}

        # ---- AI market caps ----
        shares = {}
        try:
            shares = f_sh.result()
        except Exception as e:
            err("shares outstanding", e)
        krw = (out["fx"].get("USD/KRW") or {}).get("close")
        prev_ai = PREV.get("ai_market_caps") or {}
        ai = {}
        for name, t in AI.items():
            try:
                if t not in Y:
                    raise RuntimeError("no price data")
                px = last_two(Y[t])
                cap, how = None, None
                sh = shares.get(t)
                if sh:
                    cap = sh * px["close"]
                    if t.endswith(".KS"):
                        cap = cap / krw if krw else None
                    how = "shares x close"
                if cap is None:
                    p = prev_ai.get(name) or {}
                    if p.get("market_cap_usd_bn") and p.get("close"):
                        cap = p["market_cap_usd_bn"] * 1e9 * px["close"] / p["close"]
                        how = "previous cap rolled forward by price change"
                ai[name] = {"ticker": t, **px, "market_cap_usd_bn": r2(cap / 1e9, 1) if cap else None,
                            "cap_method": how}
                if px["pct_chg"] is not None and abs(px["pct_chg"]) > MOVE_LIMIT["stock"]:
                    warn(f"AI {name} moved {px['pct_chg']}% - check")
            except Exception as e:
                err(f"AI {name} ({t})", e)
        out["ai_market_caps"] = ai

        # ---- SPINR history (same rules as v1) ----
        try:
            def by_day(t):
                return {k.date(): float(v) for k, v in Y[t]["Close"].items()}
            s, x, r = by_day("^GSPC"), by_day("^BSESN"), by_day("INR=X")
            rows, ls, lx, lr = [], None, None, None
            for d in sorted(set(s) | set(x)):
                ls, lx, lr = s.get(d, ls), x.get(d, lx), r.get(d, lr)
                if None not in (ls, lx, lr):
                    rows.append([d.isoformat(), r2(ls), r2(lx), r2(lr, 4)])
            if len(rows) < 200:
                raise RuntimeError(f"only {len(rows)} rows")
            out["spinr_history"] = {"columns": ["date", "sp500", "sensex", "usdinr"],
                                    "source": "Yahoo Finance daily closes (^GSPC, ^BSESN, INR=X)",
                                    "rows": rows}
        except Exception as e:
            err("spinr_history", e)

    # ---- Fill any gaps from the previous file ----
    for sec in ("global_indices", "fx", "commodities", "ai_market_caps"):
        carry_items(sec)
    for sec in ("treasury_yields", "spinr_history", "india_vix_52w"):
        if not out.get(sec):
            carry_section(sec, "source failed this run")

    out["health"] = summarize()
    # put health first so it is the first thing anyone reading the file sees
    ordered = {"generated_at_ist": out["generated_at_ist"], "health": out["health"]}
    ordered.update({k: v for k, v in out.items() if k not in ordered})

    fresh_anything = any(out["health"].get(s, {}).get("status") in ("ok", "partial")
                         for s in ("india", "global_indices", "fx", "commodities",
                                   "ai_market_caps", "treasury_yields"))
    if not fresh_anything and PREV:
        print("Nothing fresh at all this run - leaving the previous data.json untouched.")
        return 2
    write_atomic(ordered, OUT_FILE)
    log(f"wrote {OUT_FILE}: {len(out['errors'])} error(s), {len(out['warnings'])} warning(s), "
        f"critical_ok={out['health']['critical_ok']}")
    return 0


def check():
    """Exit 1 if data.json is missing, unreadable or its critical sections are stale."""
    try:
        with open(OUT_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
    except Exception as e:
        print(f"data.json unreadable: {e}")
        return 1
    h = d.get("health") or {}
    print(json.dumps(h, indent=1))
    for e in d.get("errors", []):
        print(" error:", e)
    for w in d.get("warnings", []):
        print(" warning:", w)
    return 0 if h.get("critical_ok") else 1


def already_fresh():
    """True if data.json was written today (IST) and its critical sections were OK.
    Used by the backup cron so it does not overwrite a good morning file."""
    h = PREV.get("health") or {}
    return bool(h.get("critical_ok")) and str(PREV.get("generated_at_ist", "")).startswith(
        TODAY.isoformat())


if __name__ == "__main__":
    if "--check" in sys.argv:
        sys.exit(check())
    if "--skip-if-fresh" in sys.argv and already_fresh():
        print(f"data.json already fresh from {PREV.get('generated_at_ist')} - nothing to do.")
        sys.exit(0)
    sys.exit(main())
