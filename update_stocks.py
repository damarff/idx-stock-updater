#!/usr/bin/env python3
"""
Update IDX stock database — runs on GitHub Actions.
- Downloads current DB from VPS
- Fetches latest data from Yahoo Finance
- Uploads updated DB back via webhook
"""
import yfinance as yf
import pandas as pd
import sqlite3, json, sys, os, time, requests
from datetime import datetime, timedelta

DB_PATH = "stocks.db"
OUTPUT_JSON = "updated_stocks.json"

# Get tickers from the existing DB or use a hardcoded list as fallback
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
    "PIL","PINA","PIN","PIPP","PIS","PITA","PKPK","PKP","PLAN","PLAS",
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

def main():
    print(f"🚀 Stock Update — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    is_gh = os.environ.get('GITHUB_ACTIONS')
    print(f"Running on: {'GitHub Actions' if is_gh else 'Local'}")
    
    # Step 1: Download current DB if webhook URL is set
    webhook_base = os.environ.get('WEBHOOK_URL', '').replace('/ingest', '')
    if webhook_base and is_gh:
        print(f"⬇️  Downloading current DB from VPS...")
        try:
            r = requests.get(f"{webhook_base}/db", timeout=30)
            if r.status_code == 200:
                with open(DB_PATH, 'wb') as f:
                    f.write(r.content)
                print(f"  ✅ DB downloaded ({len(r.content)/1024/1024:.1f} MB)")
        except Exception as e:
            print(f"  ⚠️  Could not download DB: {e}")
    
    # Step 2: Get tickers to update
    tickers = IDX_TICKERS
    last_date = None
    
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            last_date = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
            existing = set(r[0] for r in conn.execute("SELECT DISTINCT symbol FROM stock_ohlcv").fetchall())
            # Use tickers from DB if available
            if existing:
                tickers = sorted(existing)
            conn.close()
            print(f"📅 Existing DB: {last_date}, {len(tickers)} stocks")
        except:
            print(f"  Using default ticker list")
    
    print(f"📊 {len(tickers)} stocks to update")
    
    # Step 3: Fetch from Yahoo Finance
    rows = []
    failed = []    
    batch_size = 50
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    today_str = datetime.now().strftime('%Y-%m-%d')

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        symbols = [f"{t}.JK" for t in batch]

        try:
            data = yf.download(symbols, period="1mo", interval="1d", progress=False, auto_adjust=False, group_by='ticker')

            if data is None or data.empty:
                failed.extend(batch)
                continue

            for t, sym in zip(batch, symbols):
                try:
                    if sym not in data.columns.levels[1] if hasattr(data.columns, 'levels') else sym not in data.columns:
                        failed.append(t)
                        continue
                    close = data['Close'][sym] if hasattr(data.columns, 'levels') else data[sym]['Close']
                    open_ = data['Open'][sym] if hasattr(data.columns, 'levels') else data[sym]['Open']
                    high = data['High'][sym] if hasattr(data.columns, 'levels') else data[sym]['High']
                    low = data['Low'][sym] if hasattr(data.columns, 'levels') else data[sym]['Low']
                    vol = data['Volume'][sym] if hasattr(data.columns, 'levels') else data[sym]['Volume']

                    for date_idx in close.dropna().index:
                        date_str = date_idx.strftime('%Y-%m-%d')
                        c = float(close[date_idx])
                        o = float(open_[date_idx]) if pd.notna(open_.get(date_idx)) else c
                        h = float(high[date_idx]) if pd.notna(high.get(date_idx)) else c
                        l = float(low[date_idx]) if pd.notna(low.get(date_idx)) else c
                        v = int(vol[date_idx]) if pd.notna(vol.get(date_idx)) else 0
                        rows.append((t, date_str, o, h, l, c, v))
                except Exception:
                    failed.append(t)
        except Exception:
            failed.extend(batch)

        if (i // batch_size + 1) % 5 == 0:
            print(f"  [{i//batch_size + 1}/{total_batches}] {len(set(r[0] for r in rows))} done, {len(failed)} failed")
    
    stocks_updated = len(set(r[0] for r in rows))
    print(f"\n✅ {stocks_updated} stocks updated, ❌ {len(failed)} failed")
    
    # Step 4: Save to DB
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
    
    latest = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
    total_stocks = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv").fetchone()[0]
    total_rows = conn.execute("SELECT COUNT(*) FROM stock_ohlcv").fetchone()[0]
    conn.close()
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "latest_date": latest,
        "stocks_updated": stocks_updated,
        "total_stocks": total_stocks,
        "total_rows": total_rows,
        "failed": failed[:30],
        "failed_count": len(failed),
    }
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(result, f)
    
    print(f"\n📅 Latest: {latest}")
    print(f"📊 {total_stocks} stocks, {total_rows} rows")
    print(f"💾 Saved {DB_PATH} + {OUTPUT_JSON}")
    
    # Step 5: Upload via webhook
    api_key = os.environ.get('API_KEY', '')
    if webhook_base and api_key:
        print(f"📤 Uploading to webhook...")
        try:
            with open(DB_PATH, 'rb') as f:
                r = requests.post(
                    f"{webhook_base}/ingest",
                    files={'db': f},
                    data={'result': json.dumps(result)},
                    headers={'Authorization': f'Bearer {api_key}'},
                    timeout=120
                )
            print(f"  ✅ Webhook: {r.status_code} — {r.json().get('status', '')}")
        except Exception as e:
            print(f"  ❌ Webhook failed: {e}")

if __name__ == "__main__":
    main()
