#!/usr/bin/env python3
"""
Update IDX stock database — runs on GitHub Actions.
Uses Yahoo Finance v8 chart API directly (no yfinance dependency).
"""
import json
import os
import sqlite3
import time
import urllib.request
import ssl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = "stocks.db"
OUTPUT_JSON = "updated_stocks.json"
MAX_WORKERS = 50  # Parallel requests

# 957 IDX tickers
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

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def fetch_chart(ticker):
    """Fetch 1 month of OHLCV data for a single ticker via v8 chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.JK?range=1mo&interval=1d"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    ctx = ssl.create_default_context()

    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        data = json.loads(resp.read().decode())

        result = data.get("chart", {}).get("result")
        if not result:
            return None

        timestamps = result[0].get("timestamp", [])
        quotes = result[0].get("indicators", {}).get("quote", [{}])[0]

        rows = []
        for i, ts in enumerate(timestamps):
            close = quotes.get("close", [None])[i]
            if close is None:
                continue
            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            o = quotes.get("open", [None])[i] or close
            h = quotes.get("high", [None])[i] or close
            l = quotes.get("low", [None])[i] or close
            v = quotes.get("volume", [None])[i] or 0
            rows.append((date_str, o, h, l, close, int(v)))

        return rows if rows else None
    except Exception as e:
        err = str(e)
        if "429" in err:
            return "RATE_LIMITED"
        return None


def main():
    start = time.time()
    print(f"🚀 Stock Update — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Running on: {'GitHub Actions' if os.environ.get('GITHUB_ACTIONS') else 'Local'}")

    # Determine tickers
    tickers = IDX_TICKERS
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            existing = set(r[0] for r in conn.execute("SELECT DISTINCT symbol FROM stock_ohlcv").fetchall())
            if existing:
                tickers = sorted(existing)
            conn.close()
        except:
            pass

    total = len(tickers)
    print(f"📊 {total} stocks to fetch\n", flush=True)

    # Fetch all tickers in parallel
    all_rows = []
    failed = []
    rate_limited = False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        fut_map = {executor.submit(fetch_chart, t): t for t in tickers}

        done = 0
        for f in as_completed(fut_map):
            t = fut_map[f]
            done += 1
            try:
                result = f.result()
                if result == "RATE_LIMITED":
                    rate_limited = True
                    failed.append(t)
                elif result:
                    for r in result:
                        all_rows.append((t, *r))
                else:
                    failed.append(t)
            except Exception:
                failed.append(t)

            if done % 100 == 0 or done == total:
                elapsed = time.time() - start
                print(f"  [{done}/{total}] {len(set(r[0] for r in all_rows))} ok, {len(failed)} fail ({elapsed:.0f}s)", flush=True)

    # Save to DB
    print(f"\n💾 Saving DB...", flush=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS stock_ohlcv (
        symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, date)
    )""")

    for r in all_rows:
        try:
            conn.execute("INSERT OR REPLACE INTO stock_ohlcv VALUES (?,?,?,?,?,?,?)", r)
        except:
            pass
    conn.commit()

    latest = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
    stocks_with_data = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv WHERE date=?", (latest,)).fetchone()[0]
    total_stocks = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv").fetchone()[0]
    total_rows = conn.execute("SELECT COUNT(*) FROM stock_ohlcv").fetchone()[0]
    conn.close()

    elapsed = time.time() - start
    db_size = os.path.getsize(DB_PATH) / 1024 / 1024 if os.path.exists(DB_PATH) else 0

    print(f"\n📅 Latest: {latest}")
    print(f"📊 {stocks_with_data}/{total_stocks} stocks, {total_rows:,} rows")
    print(f"💾 {db_size:.0f} MB — ⏱️  {elapsed:.0f}s")
    print(f"❌ Failed: {len(failed)}", flush=True)

    result = {
        "timestamp": datetime.now().isoformat(),
        "latest_date": latest,
        "stocks_updated": len(set(r[0] for r in all_rows)),
        "total_stocks": total_stocks,
        "total_rows": total_rows,
        "db_size_mb": round(db_size, 1),
        "duration_s": round(elapsed),
        "failed": failed[:20],
        "failed_count": len(failed),
        "rate_limited": rate_limited,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ Done!", flush=True)


if __name__ == "__main__":
    main()
