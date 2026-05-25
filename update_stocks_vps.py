#!/usr/bin/env python3
"""
Update IDX stock database — runs on VPS via cron.
Downloads 5-year OHLCV data from Yahoo Finance with retry logic.
"""
import yfinance as yf
import pandas as pd
import sqlite3
import json
import os
import time
from datetime import datetime, timedelta

DB_PATH = os.path.expanduser("~/.hermes/databases/stocks.db")
SUMMARY_PATH = os.path.expanduser("~/projects/stock-updater/updated_stocks.json")
RETRY_DELAYS = [2, 5, 15]  # Seconds between retries

# Full ticker list (957 IDX stocks)
IDX_TICKERS = [
    "AADI","AALI","ABBA","ABDA","ABMM","ACES","ACRO","ACST","ADCP","ADES",
    "ADHI","ADMF","ADMG","ADRO","ADS","AEON","AGAR","AGII","AGRO","AGRS",
    "AIMS","AISA","AKRA","AKSI","ALDO","ALKA","ALMI","ALTO","AMAG","AMIN",
    "AMOR","AMRT","ANJT","ANTM","APEX","APIC","APLN","APOL","ARGO","ARII",
    "ARNA","ARTA","ARTI","ARTO","ASBI","ASDM","ASGR","ASII","ASJT","ASLC",
    "ASMI","ASPI","ASRI","ASSA","ATAP","AUTO","BABY","BACA","BAEK","BAIK",
    "BALI","BAPA","BAPI","BATA","BAYU","BBCA","BBHI","BBKP","BBLD","BBMD",
    "BBNI","BBRI","BBRM","BBTN","BBYB","BCAP","BCIP","BDEB","BDMN","BDPC",
    "BEBS","BEEF","BEKS","BELI","BEST","BFIN","BGTG","BHAT","BHTN","BIKA",
    "BIMA","BIRD","BISI","BJBR","BJTM","BKRW","BLTA","BLTZ","BMAS","BMRI",
    "BMSR","BMTR","BNBA","BNGA","BNII","BNLI","BOGA","BOLA","BOSS","BPFI",
    "BPII","BPTR","BRAM","BREN","BRIS","BRMS","BRNA","BRPT","BSBK","BSSR",
    "BSWD","BTEL","BTEK","BTON","BTPN","BTPS","BUAH","BUDI","BULL","BUMI",
    "BUVA","BVML","BWPT","CAMP","CANI","CARE","CASH","CBPE","CBUT","CCSI",
    "CENT","CGLB","CGSP","CINT","CITA","CITY","CKRA","CLAY","CLEO","CLPI",
    "CMNP","CMRY","CMSA","CNMA","CNTB","COCO","COLS","COMM","COMP","COWL",
    "CPIN","CPRO","CRAB","CRAY","CRSN","CSAP","CSMI","CTBN","CTRA","CTTH",
    "DADA","DART","DASS","DAYA","DCCU","DCII","DECO","DECK","DEGI","DENG",
    "DFAM","DGIK","DGIS","DILD","DIVA","DKFT","DLTA","DMAS","DMMX","DMND",
    "DNET","DOID","DPNS","DPUM","DSNG","DSON","DSTX","DSUC","DTAM","DVLA",
    "DWIN","DYAN","EBMT","ECII","EDGE","EKAD","ELSA","ELTY","EMBR","EMDE",
    "EMTK","ENAK","ENRG","ENVY","EPAC","EPCM","ERAA","ESIP","ESSA","ESTA",
    "ETWA","EURO","EWIN","EXCL","FAPA","FAST","FASW","FILM","FIRE","FISH",
    "FMII","FOOD","FORU","FORZ","FPNI","FREN","FROZ","FSLD","FTII","GAMA",
    "GATA","GCMA","GDST","GDYR","GEMA","GEMS","GEOB","GGRM","GIDS","GIGI",
    "GIHH","GILL","GJTL","GLVA","GMDB","GMFI","GOLD","GOLL","GOOD","GOOG",
    "GPRA","GPSO","GSBA","GSMF","GTBO","GTSI","GULA","GUNR","GWSA","HADE",
    "HALO","HATO","HDIT","HDFA","HEAL","HERO","HEXA","HITS","HKMU","HMNY",
    "HMSP","HOKI","HOTL","HRME","HRTA","HRUM","HSFI","HSIK","ICON","ICBP",
    "IDCH","IDPR","IFII","IFLS","IGAR","IIKP","IKAI","IKBI","IKMG","IMAS",
    "IMJS","IMP","IMPC","INAF","INAI","INCF","INCI","INCO","INDF","INDS",
    "INDX","INET","INPC","INPP","INRU","INTD","INTP","IPCC","IPCM","IPOL",
    "IPPE","IRRA","ISAT","ISFI","ITIC","ITMA","ITMG","IVST","JAST","JAWA",
    "JBBS","JECC","JEMB","JFAS","JIHD","JKON","JKSW","JMAS","JNKA","JPFA",
    "JPGS","JRPT","JSKY","JSMR","JSPT","JSTD","JTPS","KAEF","KARW","KASO",
    "KBLI","KBLM","KDSI","KEEN","KELC","KIAS","KIOS","KJEN","KKGI","KLAS",
    "KLBF","KMDS","KMTR","KOBX","KOKA","KONI","KOMP","KOTA","KPAS","KPIG",
    "KREN","KRYA","KSBD","KSEL","KSET","KSNI","KUAS","LADI","LAPD","LATI",
    "LAYU","LCGP","LCKM","LDH","LEAD","LEEK","LEGH","LIFE","LIGA","LINC",
    "LION","LIVE","LMAS","LMPI","LMSH","LPCK","LPGI","LPLI","LPPS","LRNA",
    "LSIP","LTLS","LUCY","MABA","MACA","MADI","MAHA","MAIN","MAMI","MAND",
    "MAPB","MAPE","MAPI","MARK","MASA","MASB","MAYA","MBAP","MBAY","MBBR",
    "MBSS","MDIA","MDKI","MDMN","MDRN","MEGA","MERK","META","MFIN","MFMI",
    "MGNA","MICE","MIDI","MIKA","MINA","MIRA","MITI","MKNT","MKS","MLBI",
    "MLIA","MLPL","MLPT","MLPW","MMIX","MNCN","MOLI","MOLN","MORA","MPAX",
    "MPOW","MPRO","MRAT","MREI","MRIX","MSIE","MTDL","MTEL","MTLA","MTPS",
    "MTSM","MTWI","MUFN","MUTU","MYOR","MYRX","MYSQ","NAAM","NAPL","NATO",
    "NELY","NFCX","NICL","NIKL","NIPS","NISP","NOBU","NRCA","NUSA","OASA",
    "OCTI","OKAS","OLIV","OMED","OMRE","ONIX","ONLY","OPMS","ORIY","OT",
    "PALM","PAMG","PANI","PANS","PAN","PAPA","PARD","PAS","PBD","PBS",
    "PDES","PDPP","PDZ","PEVE","PGAS","PGEO","PGLI","PGUN","PIAA","PID",
    "PINA","PINS","PIPP","PIS","PITA","PKPK","PKP","PLAN","PLAS",
    "PLIN","PNBN","PNBS","PNIN","PNLF","PNSE","POLL","POLY","POOL","POWR",
    "PPGL","PPRE","PPRO","PPSI","PRAS","PRDA","PRIM","PRMA","PRTS","PSAB",
    "PSDN","PSGO","PSKT","PSMI","PTIS","PTPP","PTRO","PUDP","PURA","PURE",
    "PWON","PYFA","RACE","RALS","RANC","RATU","RAJA","RBMS","RDTX","REAL",
    "RELI","RENE","REPC","REZN","RICY","RIGS","RIMO","RISE","RKBY","RKYU",
    "RODA","ROTI","RSCH","RUIS","RUM","SABA","SAME","SAMF","SAPX","SATU",
    "SAVE","SCBD","SCMA","SCOO","SCPI","SDMU","SDPC","SDRA","SEAN","SECP",
    "SEMA","SFAN","SGER","SGRO","SHID","SHIP","SIAP","SICO","SIDO","SILO",
    "SKBM","SKLT","SKYB","SLIS","SMAR","SMDR","SMGR","SMKL","SMMS","SMSM",
    "SMSS","SMTG","SNLK","SNPC","SOFA","SONA","SPMA","SPPI","SPTO","SQBI",
    "SRAJ","SRIL","SRSN","SSIA","SSMS","SSTM","STAA","STAR","STTP","SUGI",
    "SULI","SUPR","SURF","SURY","SUTO","SUZI","SVGG","TALF","TAMA","TAMU",
    "TAPG","TARA","TARP","TBIG","TBLA","TBS","TBTK","TCID","TDRA","TEBE",
    "TECH","TEGUH","TELE","TELK","TEMA","TEMB","TGAH","TGKA","TIBE","TIFA",
    "TINS","TIRA","TIRO","TIRT","TISN","TITA","TKIM","TKMI","TLA","TLDN",
    "TLEV","TLKM","TMAS","TMPI","TMS","TMT","TOBA","TOOL","TOPS","TOTL",
    "TOWR","TPIA","TPMA","TPRI","TRAM","TRAY","TRIO","TRIS","TRJA","TRST",
    "TRUB","TRUE","TRUK","TSAM","TSPC","TUGU","TUPM","TURI","TUV","TWS",
    "TYAN","UANG","UCID","UDIJ","ULBI","ULTJ","UNAI","UNFI","UNIC","UNIT",
    "UNIQ","UNVR","USFI","UTAMA","UTLY","UWES","VALU","VATS","VICI","VINS",
    "VIVA","VOKS","VRNA","WAPO","WEGE","WEHA","WIKA","WINE","WINR","WINS",
    "WIRD","WMUU","WOOD","WOWS","WSBP","WSKT","WSML","WTON","YELO","YOUR",
    "YULE","ZBRA","ZEUS","ZINC","ZONE","ZRKP"
]


def fetch_with_retry(symbols, period="1mo"):
    """Fetch data with retry on rate limit."""
    for delay in RETRY_DELAYS:
        try:
            data = yf.download(symbols, period=period, interval="1d", progress=False, auto_adjust=False)
            if data is not None and not data.empty:
                return data
        except Exception as e:
            err = str(e)
            if "Rate limited" in err or "429" in err:
                print(f"  ⏳ Rate limited, waiting {delay}s...")
                time.sleep(delay)
                continue
    return None


def main():
    start_time = time.time()
    print(f"🚀 Stock Update — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Determine tickers to update
    tickers = IDX_TICKERS
    earliest_date = None
    existing_tickers = set()

    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            earliest_date = conn.execute("SELECT MIN(date) FROM stock_ohlcv").fetchone()[0]
            last_date = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
            existing = set(r[0] for r in conn.execute("SELECT DISTINCT symbol FROM stock_ohlcv").fetchall())
            if existing:
                tickers = sorted(existing)
                existing_tickers = existing
            count = conn.execute("SELECT COUNT(*) FROM stock_ohlcv").fetchone()[0]
            conn.close()
            print(f"📅 DB: {earliest_date} → {last_date}, {len(tickers)} stocks, {count:,} rows")
        except Exception as e:
            print(f"  Using full ticker list ({e})")

    print(f"📊 {len(tickers)} stocks to update\n")

    # Determine date range
    today = datetime.now()
    # Monday=0, Friday=4 (weekday), Sat=5, Sun=6
    # If weekend, we want last Friday's data
    if today.weekday() >= 5:
        days_to_monday = today.weekday() - 4
        last_friday = today - timedelta(days=days_to_monday)
        print(f"  Weekend detected, fetching up to {last_friday.strftime('%a %Y-%m-%d')}")

    # Fetch in parallel batches
    rows = []
    failed = []
    batch_size = 50
    total = len(tickers)
    batches = (total + batch_size - 1) // batch_size

    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        symbols = [f"{t}.JK" for t in batch]
        batch_num = i // batch_size + 1

        data = fetch_with_retry(symbols)

        if data is None:
            failed.extend(batch)
            continue

        # Parse data (handle MultiIndex vs single)
        for t, sym in zip(batch, symbols):
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    close = data['Close'][sym]
                    open_ = data['Open'][sym]
                    high = data['High'][sym]
                    low = data['Low'][sym]
                    vol = data['Volume'][sym]
                else:
                    close = data['Close']
                    open_ = data['Open']
                    high = data['High']
                    low = data['Low']
                    vol = data['Volume']

                for date_idx in close.dropna().index:
                    date_str = date_idx.strftime('%Y-%m-%d')
                    c = float(close[date_idx])
                    o = float(open_[date_idx]) if pd.notna(open_.get(date_idx)) else c
                    h = float(high[date_idx]) if pd.notna(high.get(date_idx)) else c
                    l = float(low[date_idx]) if pd.notna(low.get(date_idx)) else c
                    v = int(vol[date_idx]) if pd.notna(vol.get(date_idx)) else 0
                    rows.append((t, date_str, o, h, l, c, v))
            except:
                failed.append(t)

        done = len(set(r[0] for r in rows))
        if batch_num % 3 == 0 or batch_num == batches:
            elapsed = time.time() - start_time
            print(f"  [{batch_num}/{batches}] {done} done, {len(failed)} failed ({elapsed:.0f}s)")

    # Save to DB
    print(f"\n💾 Saving to DB...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS stock_ohlcv (
        symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, date)
    )""")

    for row in rows:
        try:
            cur.execute("INSERT OR REPLACE INTO stock_ohlcv VALUES (?,?,?,?,?,?,?)", row)
        except:
            pass
    conn.commit()

    # Stats
    latest = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
    stocks = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv WHERE date=?", (latest,)).fetchone()[0]
    total_stocks = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv").fetchone()[0]
    total_rows = conn.execute("SELECT COUNT(*) FROM stock_ohlcv").fetchone()[0]
    conn.close()

    # Vacuum to reclaim space
    conn2 = sqlite3.connect(DB_PATH)
    conn2.execute("VACUUM")
    conn2.close()

    db_size = os.path.getsize(DB_PATH) / 1024 / 1024
    elapsed = time.time() - start_time
    print(f"📅 Latest: {latest}")
    print(f"📊 {stocks}/{total_stocks} stocks with {latest} data, {total_rows:,} total rows")
    print(f"💾 {db_size:.0f} MB — ⏱️  {elapsed:.0f}s")

    # Save summary
    result = {
        "timestamp": datetime.now().isoformat(),
        "latest_date": latest,
        "stocks_updated": len(set(r[0] for r in rows)),
        "total_stocks": total_stocks,
        "total_rows": total_rows,
        "db_size_mb": round(db_size, 1),
        "duration_s": round(elapsed),
        "failed": failed[:20],
        "failed_count": len(failed),
    }

    with open(SUMMARY_PATH, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ Done!")


if __name__ == "__main__":
    main()
