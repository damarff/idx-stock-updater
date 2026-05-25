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
    "AADI", "AALI", "ABBA", "ABDA", "ABMM", "ACES", "ACRO", "ACST", "ADCP", "ADES",
    "ADHI", "ADMF", "ADMG", "ADMR", "ADRO", "AEGS", "AGAR", "AGII", "AGRO", "AGRS",
    "AHAP", "AIMS", "AISA", "AKKU", "AKPI", "AKRA", "AKSI", "ALDO", "ALII", "ALKA",
    "ALMI", "ALTO", "AMAG", "AMAN", "AMAR", "AMFG", "AMIN", "AMMN", "AMMS", "AMOR",
    "AMRT", "ANDI", "ANJT", "ANTM", "APEX", "APIC", "APII", "APLI", "APLN", "ARCI",
    "AREA", "ARGO", "ARII", "ARKA", "ARKO", "ARMY", "ARNA", "ARTA", "ARTI", "ARTO",
    "ASBI", "ASDM", "ASGR", "ASHA", "ASII", "ASJT", "ASLC", "ASLI", "ASMI", "ASPI",
    "ASPR", "ASRI", "ASRM", "ASSA", "ATAP", "ATIC", "ATLA", "AUTO", "AVIA", "AWAN",
    "AXIO", "AYAM", "AYLS", "BABP", "BABY", "BACA", "BAIK", "BAJA", "BALI", "BANK",
    "BAPA", "BAPI", "BATA", "BATR", "BAUT", "BAYU", "BBCA", "BBHI", "BBKP", "BBLD",
    "BBMD", "BBNI", "BBRI", "BBRM", "BBSI", "BBSS", "BBTN", "BBYB", "BCAP", "BCIC",
    "BCIP", "BDKR", "BDMN", "BEBS", "BEEF", "BEER", "BEKS", "BELI", "BELL", "BESS",
    "BEST", "BFIN", "BGTG", "BHAT", "BHIT", "BIKA", "BIKE", "BIMA", "BINA", "BINO",
    "BIPI", "BIPP", "BIRD", "BISI", "BJBR", "BJTM", "BKDP", "BKSL", "BKSW", "BLES",
    "BLOG", "BLTA", "BLTZ", "BLUE", "BMAS", "BMBL", "BMHS", "BMRI", "BMSR", "BMTR",
    "BNBA", "BNBR", "BNGA", "BNII", "BNLI", "BOAT", "BOBA", "BOGA", "BOLA", "BOLT",
    "BOSS", "BPFI", "BPII", "BPTR", "BRAM", "BREN", "BRIS", "BRMS", "BRNA", "BRPT",
    "BRRC", "BSBK", "BSDE", "BSIM", "BSML", "BSSR", "BSWD", "BTEK", "BTEL", "BTON",
    "BTPN", "BTPS", "BUAH", "BUDI", "BUKA", "BUKK", "BULL", "BUMI", "BUVA", "BVIC",
    "BWPT", "BYAN", "CAKK", "CAMP", "CANI", "CARE", "CARS", "CASA", "CASH", "CASS",
    "CBDK", "CBMF", "CBPE", "CBRE", "CBUT", "CCSI", "CDIA", "CEKA", "CENT", "CFIN",
    "CGAS", "CHEK", "CHEM", "CHIP", "CINT", "CITA", "CITY", "CLAY", "CLEO", "CLPI",
    "CMNP", "CMNT", "CMPP", "CMRY", "CNKO", "CNMA", "CNTX", "COAL", "COCO", "COIN",
    "COWL", "CPIN", "CPRI", "CPRO", "CRAB", "CRSN", "CSAP", "CSIS", "CSMI", "CSRA",
    "CTBN", "CTRA", "CTTH", "CUAN", "CYBR", "DAAZ", "DADA", "DART", "DATA", "DAYA",
    "DCII", "DEAL", "DEFI", "DEPO", "DEWA", "DEWI", "DFAM", "DGIK", "DGNS", "DGWG",
    "DIGI", "DILD", "DIVA", "DKFT", "DKHH", "DLTA", "DMAS", "DMMX", "DMND", "DNAR",
    "DNET", "DOID", "DOOH", "DOSS", "DPNS", "DPUM", "DRMA", "DSFI", "DSNG", "DSSA",
    "DUCK", "DUTI", "DVLA", "DWGL", "DYAN", "EAST", "ECII", "EDGE", "EKAD", "ELIT",
    "ELPI", "ELSA", "ELTY", "EMAS", "EMDE", "EMTK", "ENAK", "ENRG", "ENVY", "ENZO",
    "EPAC", "EPMT", "ERAA", "ERAL", "ERTX", "ESIP", "ESSA", "ESTA", "ESTI", "ETWA",
    "EURO", "EXCL", "FAPA", "FAST", "FASW", "FILM", "FIMP", "FIRE", "FISH", "FITT",
    "FLMC", "FMII", "FOLK", "FOOD", "FORE", "FORU", "FPNI", "FUJI", "FUTR", "FWCT",
    "GAMA", "GDST", "GDYR", "GEMA", "GEMS", "GGRM", "GGRP", "GHON", "GIAA", "GJTL",
    "GLOB", "GLVA", "GMFI", "GMTD", "GOLD", "GOLF", "GOLL", "GOOD", "GOTO", "GPRA",
    "GPSO", "GRIA", "GRPH", "GRPM", "GSMF", "GTBO", "GTRA", "GTSI", "GULA", "GUNA",
    "GWSA", "GZCO", "HADE", "HAIS", "HAJJ", "HALO", "HATM", "HBAT", "HDFA", "HDIT",
    "HEAL", "HELI", "HERO", "HEXA", "HGII", "HILL", "HITS", "HKMU", "HMSP", "HOKI",
    "HOME", "HOMI", "HOPE", "HOTL", "HRME", "HRTA", "HRUM", "HUMI", "HYGN", "IATA",
    "IBFN", "IBOS", "IBST", "ICBP", "ICON", "IDEA", "IDPR", "IFII", "IFSH", "IGAR",
    "IIKP", "IKAI", "IKAN", "IKBI", "IKPM", "IMAS", "IMJS", "IMPC", "INAF", "INAI",
    "INCF", "INCI", "INCO", "INDF", "INDO", "INDR", "INDS", "INDX", "INDY", "INET",
    "INKP", "INOV", "INPC", "INPP", "INPS", "INRU", "INTA", "INTD", "INTP", "IOTF",
    "IPAC", "IPCC", "IPCM", "IPOL", "IPPE", "IPTV", "IRRA", "IRSX", "ISAP", "ISAT",
    "ISEA", "ISSP", "ITIC", "ITMA", "ITMG", "JARR", "JAST", "JATI", "JAWA", "JAYA",
    "JECC", "JGLE", "JIHD", "JKON", "JMAS", "JPFA", "JRPT", "JSKY", "JSMR", "JSPT",
    "JTPE", "KAEF", "KAQI", "KARW", "KAYU", "KBAG", "KBLI", "KBLM", "KBLV", "KBRI",
    "KDSI", "KDTN", "KEEN", "KEJU", "KETR", "KIAS", "KICI", "KIJA", "KING", "KINO",
    "KIOS", "KJEN", "KKES", "KKGI", "KLAS", "KLBF", "KLIN", "KMDS", "KMTR", "KOBX",
    "KOCI", "KOIN", "KOKA", "KONI", "KOPI", "KOTA", "KPIG", "KRAS", "KREN", "KRYA",
    "KSIX", "KUAS", "LABA", "LABS", "LAJU", "LAND", "LAPD", "LCGP", "LCKM", "LEAD",
    "LFLO", "LIFE", "LINK", "LION", "LIVE", "LMAS", "LMAX", "LMPI", "LMSH", "LOPI",
    "LPCK", "LPGI", "LPIN", "LPKR", "LPLI", "LPPF", "LPPS", "LRNA", "LSIP", "LTLS",
    "LUCK", "LUCY", "MABA", "MAGP", "MAHA", "MAIN", "MANG", "MAPA", "MAPB", "MAPI",
    "MARI", "MARK", "MASB", "MAXI", "MAYA", "MBAP", "MBMA", "MBSS", "MBTO", "MCAS",
    "MCOL", "MCOR", "MDIA", "MDIY", "MDKA", "MDKI", "MDLA", "MDLN", "MDRN", "MEDC",
    "MEDS", "MEGA", "MEJA", "MENN", "MERI", "MERK", "META", "MFMI", "MGLV", "MGNA",
    "MGRO", "MHKI", "MICE", "MIDI", "MIKA", "MINA", "MINE", "MIRA", "MITI", "MKAP",
    "MKNT", "MKPI", "MKTR", "MLBI", "MLIA", "MLPL", "MLPT", "MMIX", "MMLP", "MNCN",
    "MOLI", "MORA", "MPIX", "MPMX", "MPOW", "MPPA", "MPRO", "MPXL", "MRAT", "MREI",
    "MSIE", "MSIN", "MSJA", "MSKY", "MSTI", "MTDL", "MTEL", "MTFN", "MTLA", "MTMH",
    "MTPS", "MTRA", "MTSM", "MTWI", "MUTU", "MYOH", "MYOR", "MYTX", "NAIK", "NANO",
    "NASA", "NASI", "NATO", "NAYZ", "NCKL", "NELY", "NEST", "NETV", "NFCX", "NICE",
    "NICK", "NICL", "NIKL", "NINE", "NIRO", "NISP", "NOBU", "NPGF", "NRCA", "NSSS",
    "NTBK", "NUSA", "NZIA", "OASA", "OBAT", "OBMD", "OCAP", "OILS", "OKAS", "OLIV",
    "OMED", "OMRE", "OPMS", "PACK", "PADA", "PADI", "PALM", "PAMG", "PANI", "PANR",
    "PANS", "PART", "PBID", "PBRX", "PBSA", "PCAR", "PDES", "PDPP", "PEGE", "PEHA",
    "PEVE", "PGAS", "PGEO", "PGJO", "PGLI", "PGUN", "PICO", "PIPA", "PJAA", "PJHB",
    "PKPK", "PLAN", "PLAS", "PLIN", "PMJS", "PMMP", "PMUI", "PNBN", "PNBS", "PNGO",
    "PNIN", "PNLF", "PNSE", "POLA", "POLI", "POLL", "POLU", "POLY", "POOL", "PORT",
    "POSA", "POWR", "PPGL", "PPRE", "PPRI", "PPRO", "PRAY", "PRDA", "PRIM", "PSAB",
    "PSAT", "PSDN", "PSGO", "PSKT", "PSSI", "PTBA", "PTDU", "PTIS", "PTMP", "PTMR",
    "PTPP", "PTPS", "PTPW", "PTRO", "PTSN", "PTSP", "PUDP", "PURA", "PURE", "PURI",
    "PWON", "PYFA", "PZZA", "RAAM", "RAFI", "RAJA", "RALS", "RANC", "RATU", "RBMS",
    "RCCC", "RDTX", "REAL", "RELF", "RELI", "RGAS", "RICY", "RIGS", "RIMO", "RISE",
    "RLCO", "RMKE", "RMKO", "ROCK", "RODA", "RONY", "ROTI", "RSCH", "RSGK", "RUIS",
    "RUNS", "SAFE", "SAGE", "SAME", "SAMF", "SAPX", "SATU", "SBAT", "SBMA", "SCCO",
    "SCMA", "SCNP", "SCPI", "SDMU", "SDPC", "SDRA", "SEMA", "SFAN", "SGER", "SGRO",
    "SHID", "SHIP", "SICO", "SIDO", "SILO", "SIMA", "SIMP", "SINI", "SIPD", "SKBM",
    "SKLT", "SKRN", "SKYB", "SLIS", "SMAR", "SMBR", "SMCB", "SMDM", "SMDR", "SMGA",
    "SMGR", "SMIL", "SMKL", "SMKM", "SMLE", "SMMA", "SMMT", "SMRA", "SMRU", "SMSM",
    "SNLK", "SOCI", "SOFA", "SOHO", "SOLA", "SONA", "SOSS", "SOTS", "SOUL", "SPMA",
    "SPRE", "SPTO", "SQMI", "SRAJ", "SRIL", "SRSN", "SRTG", "SSIA", "SSMS", "SSTM",
    "STAA", "STAR", "STRK", "STTP", "SUGI", "SULI", "SUNI", "SUPA", "SUPR", "SURE",
    "SURI", "SWAT", "SWID", "TALF", "TAMA", "TAMU", "TAPG", "TARA", "TAXI", "TAYS",
    "TBIG", "TBLA", "TBMS", "TCID", "TCPI", "TDPM", "TEBE", "TECH", "TELE", "TFAS",
    "TFCO", "TGKA", "TGRA", "TGUK", "TIFA", "TINS", "TIRA", "TIRT", "TKIM", "TLDN",
    "TLKM", "TMAS", "TMPO", "TNCA", "TOBA", "TOOL", "TOPS", "TOSK", "TOTL", "TOTO",
    "TOWR", "TOYS", "TPIA", "TPMA", "TRAM", "TRGU", "TRIL", "TRIM", "TRIN", "TRIO",
    "TRIS", "TRJA", "TRON", "TRST", "TRUE", "TRUK", "TRUS", "TSPC", "TUGU", "TYRE",
    "UANG", "UCID", "UDNG", "UFOE", "ULTJ", "UNIC", "UNIQ", "UNIT", "UNSP", "UNTD",
    "UNTR", "UNVR", "URBN", "UVCR", "VAST", "VERN", "VICI", "VICO", "VINS", "VISI",
    "VIVA", "VKTR", "VOKS", "VRNA", "VTNY", "WAPO", "WBSA", "WEGE", "WEHA", "WGSH",
    "WICO", "WIDI", "WIFI", "WIIM", "WIKA", "WINE", "WINR", "WINS", "WIRG", "WMPP",
    "WMUU", "WOMF", "WOOD", "WOWS", "WSBP", "WSKT", "WTON", "YELO", "YOII", "YPAS",
    "YULE", "YUPI", "ZATA", "ZBRA", "ZINC", "ZONE", "ZYRX",
]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def fetch_chart(ticker):
    """Fetch 1 month of OHLCV data for a single ticker via v8 chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.JK?range=5y&interval=1d"
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

    # Determine tickers — always use full IDX list
    tickers = IDX_TICKERS

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
