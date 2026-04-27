import requests
import pandas as pd
import streamlit as st
import numpy as np
import pytz
import time
import pickle
import threading
import yfinance as yf
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════
DISPLAY_TOP    = 50
CACHE_TTL_15M  = 300
CACHE_TTL_D    = 3600

# Timezone
jakarta_tz = pytz.timezone("Asia/Jakarta")

# Cache directory
CACHE_DIR = Path("/tmp/cyrus_cache") if Path("/tmp").exists() else Path.home() / ".cyrus_cache"
CACHE_DIR.mkdir(exist_ok=True)

# Telegram
try:
    TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
    CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
except:
    TOKEN = ""; CHAT_ID = ""

# ════════════════════════════════════════
#  DISK CACHE — thread-safe
# ════════════════════════════════════════
CACHE_TTL = 300  # 5 menit default
_mem      = {}
_mem_lock = threading.Lock()

def _ck(ticker, tf): return f"cf_{ticker}_{tf}"

def _disk_get(key):
    fp = CACHE_DIR / f"{key}.pkl"
    try:
        if fp.exists():
            d = pickle.loads(fp.read_bytes())
            if time.time() - d["ts"] < CACHE_TTL: return d["df"]
    except: pass
    return None

def _disk_set(key, df):
    try: (CACHE_DIR / f"{key}.pkl").write_bytes(pickle.dumps({"ts": time.time(), "df": df}))
    except: pass

def cache_get(ticker, tf):
    key = _ck(ticker, tf)
    with _mem_lock:
        if key in _mem:
            ts, df = _mem[key]
            if time.time() - ts < CACHE_TTL: return df
    df = _disk_get(key)
    if df is not None:
        with _mem_lock: _mem[key] = (time.time(), df)
    return df

def cache_set(ticker, tf, df):
    key = _ck(ticker, tf)
    with _mem_lock: _mem[key] = (time.time(), df)
    _disk_set(key, df)

# Results persistence
RESULTS_FILE = CACHE_DIR / "last_results.pkl"
RESULTS_TTL  = 600  # 10 menit

def save_results(mode, results, scan_ts):
    try:
        RESULTS_FILE.write_bytes(pickle.dumps(
            {"mode": mode, "results": results, "ts": scan_ts}))
    except: pass

def load_results():
    try:
        if RESULTS_FILE.exists():
            d = pickle.loads(RESULTS_FILE.read_bytes())
            if time.time() - d["ts"] < RESULTS_TTL: return d
    except: pass
    return None

def _fetch_yf(ticker, interval="15m", force_fresh=False):
    """
    Fetch via yFinance — reliable, no API key needed.
    interval: '15m' untuk intraday, '1d' untuk daily
    """
    tf_yf = interval if interval in ["15m","30m","1h","1d"] else "15m"
    cache_tf = "daily" if tf_yf == "1d" else tf_yf
    
    if not force_fresh:
        cached = cache_get(ticker, cache_tf)
        if cached is not None: return cached
    
    try:
        t = ticker.replace(".JK","").upper()
        sym_yf = f"{t}.JK"
        
        if tf_yf == "1d":
            df = yf.download(sym_yf, period="60d", interval="1d",
                             progress=False, auto_adjust=True)
        else:
            df = yf.download(sym_yf, period="5d", interval="15m",
                             progress=False, auto_adjust=True)
        
        if df is None or len(df) < 5: return None
        
        # Flatten MultiIndex kalau ada
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        
        df = df.rename(columns={
            "Open":"Open","High":"High","Low":"Low",
            "Close":"Close","Volume":"Volume"
        })
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Convert to WIB
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert("Asia/Jakarta")
        else:
            df.index = df.index.tz_convert("Asia/Jakarta")
        
        if len(df) < 5: return None
        
        cache_set(ticker, cache_tf, df)
        return df
    except: return None

def _fetch_raw(ticker, interval="15m", force_fresh=False):
    """Alias — semua pakai yFinance sekarang."""
    tf_map = {"15m":"15m","30m":"30m","1h":"1h","daily":"1d","1d":"1d","d":"1d"}
    tf_yf  = tf_map.get(str(interval).lower(), "15m")
    return _fetch_yf(ticker, tf_yf, force_fresh)

# ════════════════════════════════════════
#  MARKET REGIME DETECTOR — Auto Threshold
#  Pakai IHSG (^JKSE) sebagai benchmark
#  Sama persis dengan Theta Turbo
# ════════════════════════════════════════
@st.cache_data(ttl=300)
def get_ihsg_regime():
    """
    Deteksi regime IHSG:
    GREEN  = BULL  → all signals valid
    YELLOW = SIDE  → selektif, RVOL harus > 2x
    RED    = BEAR  → skip entry, hanya monitor
    """
    try:
        df = yf.download("^JKSE", period="60d", interval="1d",
                          progress=False, auto_adjust=True)
        if df is None or len(df) < 20: return "UNKNOWN", 0.0, 0.0
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        
        c     = df["Close"].dropna()
        last  = float(c.iloc[-1])
        e20   = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        e55   = float(c.ewm(span=55, adjust=False).mean().iloc[-1])
        chg1  = (last - float(c.iloc[-2])) / max(float(c.iloc[-2]), 1) * 100
        chg5  = (last - float(c.iloc[-6])) / max(float(c.iloc[-6]), 1) * 100 if len(c)>=7 else 0
        
        if last > e20 > e55 and chg5 > 0:
            regime = "GREEN"
        elif last < e20 < e55 or chg5 < -3:
            regime = "RED"
        else:
            regime = "YELLOW"
        
        return regime, round(last, 0), round(chg1, 2)
    except:
        return "UNKNOWN", 0.0, 0.0

def get_auto_threshold(regime):
    """
    Auto-adjust filter threshold berdasarkan regime IHSG.
    GREEN  = threshold normal
    YELLOW = lebih ketat (RVOL min naik)
    RED    = paling ketat (hanya BANDAR/SUPER)
    """
    if regime == "GREEN":
        return {"min_rvol": 1.2, "min_score": 15, "min_prob": 55,
                "label": "🟢 NORMAL — Semua sinyal valid"}
    elif regime == "YELLOW":
        return {"min_rvol": 1.5, "min_score": 20, "min_prob": 60,
                "label": "🟡 SIDEWAYS — RVOL ≥ 1.5x, selektif"}
    elif regime == "RED":
        return {"min_rvol": 2.0, "min_score": 25, "min_prob": 65,
                "label": "🔴 BEAR — Hanya BANDAR/SUPER, ekstra ketat"}
    else:
        return {"min_rvol": 1.2, "min_score": 15, "min_prob": 55,
                "label": "⚪ UNKNOWN — Data IHSG tidak tersedia"}

# ════════════════════════════════════════
#  FULL IDX 778 STOCKS — Cyrus Fund
# ════════════════════════════════════════
_RAW = [
    "AADI", "AALI", "ABBA", "ABDA", "ABMM", "ACES", "ACRO", "ACST", "ADCP", "ADES", 
    "ADHI", "ADMF", "ADMG", "ADMR", "ADRO", "AEGS", "AGAR", "AGII", "AGRO", "AGRS", 
    "AHAP", "AIMS", "AISA", "AKKU", "AKPI", "AKRA", "AKSI", "ALDO", "ALII", "ALKA", 
    "ALMI", "ALTO", "AMAG", "AMAN", "AMAR", "AMFG", "AMIN", "AMMN", "AMMS", "AMOR", 
    "AMRT", "ANDI", "ANJT", "ANTM", "APEX", "APIC", "APII", "APLI", "APLN", "ARCI", 
    "AREA", "ARGO", "ARII", "ARKA", "ARKO", "ARMY", "ARNA", "ARTA", "ARTI", "ARTO", 
    "ASBI", "ASDM", "ASGR", "ASHA", "ASII", "ASJT", "ASLI", "ASLC", "ASMI", "ASPI", 
    "ASPR", "ASRI", "ASRM", "ASSA", "ATAP", "ATIC", "ATLA", "AUTO", "AVIA", "AWAN", 
    "AXIO", "AYAM", "AYLS", "BABA", "BABP", "BABY", "BACA", "BAIK", "BAJA", "BALI", 
    "BANK", "BAPA", "BAPI", "BATA", "BATR", "BAUT", "BAYU", "BBCA", "BBHI", "BBKP", 
    "BBLD", "BBMD", "BBNI", "BBRI", "BBRM", "BBSI", "BBSS", "BBTN", "BBYB", "BCAP", 
    "BCIC", "BCIP", "BDKR", "BDMN", "BEBS", "BEEF", "BEER", "BEKS", "BELI", "BELL", 
    "BESS", "BEST", "BFIN", "BGTG", "BHAT", "BHIT", "BIAS", "BIKA", "BIKE", "BIMA", 
    "BINA", "BINO", "BIPI", "BIPP", "BIRD", "BISI", "BIWA", "BJBR", "BJTM", "BKDP", 
    "BKSL", "BKSW", "BLES", "BLOG", "BLTA", "BLTZ", "BLUE", "BMAS", "BMBL", "BMHS", 
    "BMRI", "BMSR", "BMTR", "BNBA", "BNBR", "BNGA", "BNII", "BNLI", "BOAT", "BOBA", 
    "BOGA", "BOLA", "BOLT", "BOSS", "BPFI", "BPII", "BPTR", "BRAM", "BREN", "BRIS", 
    "BRMS", "BRNA", "BRPT", "BRRC", "BSBK", "BSDE", "BSIM", "BSML", "BSSR", "BSWD", 
    "BTEK", "BTEL", "BTON", "BTPN", "BTPS", "BUAH", "BUDI", "BUKA", "BUKK", "BULL", 
    "BUMI", "BUVA", "BVIC", "BWPT", "BYAN", "CAKK", "CAMP", "CANI", "CARE", "CARS", 
    "CASA", "CASH", "CASS", "CBDK", "CBPE", "CBRE", "CBUT", "CBMF", "CCSI", "CDIA", 
    "CEKA", "CENT", "CFIN", "CGAS", "CHEK", "CHEM", "CHIP", "CINT", "CITA", "CITY", 
    "CLAY", "CLEO", "CLPI", "CMNP", "CMNT", "CMPP", "CMRY", "CNKO", "CNMA", "CNTX", 
    "COAL", "COCO", "COIN", "COWL", "CPIN", "CPRI", "CPRO", "CRAB", "CRSN", "CSAP", 
    "CSIS", "CSMI", "CSRA", "CTBN", "CTRA", "CTTH", "CUAN", "CYBR", "DAAZ", "DADA", 
    "DART", "DATA", "DAYA", "DCII", "DEAL", "DEFI", "DEPO", "DEWA", "DEWI", "DFAM", 
    "DGNS", "DGWG", "DGIK", "DIGI", "DILD", "DIVA", "DKFT", "DKHH", "DLTA", "DMAS", 
    "DMMX", "DMND", "DNAR", "DNET", "DOID", "DOOH", "DOSS", "DPNS", "DPUM", "DRMA", 
    "DSFI", "DSNG", "DSSA", "DUCK", "DUTI", "DVLA", "DWGL", "DYAN", "EAST", "ECII", 
    "EDGE", "EKAD", "ELIT", "ELPI", "ELSA", "ELTY", "EMAS", "EMDE", "EMTK", "ENAK", 
    "ENRG", "ENVY", "ENZO", "EPAC", "EPMT", "ERAL", "ERAA", "ERTX", "ESIP", "ESSA", 
    "ESTA", "ESTI", "ETWA", "EURO", "EXCL", "FAPA", "FAST", "FASW", "FILM", "FIMP", 
    "FIRE", "FISH", "FITT", "FLMC", "FOLK", "FOOD", "FORE", "FORU", "FPNI", "FUJI", 
    "FUTR", "FWCT", "GAMA", "GDST", "GDYR", "GEMA", "GEMS", "GGRP", "GGRM", "GHON", 
    "GIAA", "GJTL", "GLOB", "GLVA", "GMFI", "GMTD", "GOLF", "GOLD", "GOLL", "GOOD", 
    "GOTO", "GPRA", "GPSO", "GRIA", "GRPH", "GRPM", "GRII", "GSMF", "GTBO", "GTRA", 
    "GTSI", "GULA", "GUNA", "GWSA", "GZCO", "HADE", "HAIS", "HAJJ", "HALO", "HATM", 
    "HBAT", "HDFA", "HDIT", "HEAL", "HELI", "HERO", "HEXA", "HGII", "HILL", "HITS", 
    "HKMU", "HMSP", "HOKI", "HOME", "HOMI", "HOPE", "HOTL", "HRME", "HRTA", "HRUM", 
    "HUMI", "HYGN", "IATA", "IBFN", "IBOS", "IBST", "ICBP", "ICON", "IDEA", "IDPR", 
    "IFII", "IFSH", "IGAR", "IIKP", "IKAI", "IKAN", "IKBI", "IKPM", "IMAS", "IMJS", 
    "IMPC", "INAF", "INAI", "INCF", "INCI", "INCO", "INDF", "INDO", "INDR", "INDS", 
    "INDX", "INDY", "INET", "INKP", "INOV", "INPC", "INPP", "INPS", "INRU", "INTA", 
    "INTD", "INTP", "IOTF", "IPAC", "IPCC", "IPCM", "IPOL", "IPPE", "IPTV", "IRRA", 
    "IRSX", "ISAP", "ISAT", "ISEA", "ISSP", "ITIC", "ITMA", "ITMG", "JAAS", "JARR", 
    "JAST", "JATI", "JAVA", "JAYA", "JECC", "JGLE", "JIHD", "JKON", "JMAS", "JPFA", 
    "JRPT", "JSKY", "JSMR", "JSPT", "JTPE", "KAEF", "KAQI", "KARW", "KARY", "KAST", 
    "KAYU", "KBAG", "KBLI", "KBLM", "KBLV", "KBRI", "KDSI", "KDTN", "KEEN", "KEJU", 
    "KETR", "KIAS", "KICI", "KIJA", "KING", "KINO", "KIOS", "KJEN", "KKES", "KKGI", 
    "KLAS", "KLBF", "KLIN", "KMDS", "KMTR", "KOBX", "KOCI", "KOIN", "KOKA", "KONI", 
    "KOPI", "KOTA", "KPIG", "KRAH", "KRAS", "KREN", "KSIX", "KUAS", "LABA", "LABS", 
    "LAJU", "LAND", "LAPD", "LCGP", "LCKM", "LEAD", "LFLO", "LIFE", "LINK", "LION", 
    "LIVE", "LMAS", "LMPI", "LMSH", "LOPI", "LPCK", "LPGI", "LPIN", "LPKR", "LPLI", 
    "LPPF", "LPPS", "LRNA", "LSIP", "LTLS", "LUCK", "LUCY", "MAAS", "MABA", "MADA", 
    "MAGP", "MAHA", "MAIN", "MANG", "MAPA", "MAPB", "MAPI", "MARI", "MARK", "MASA", 
    "MASB", "MAYA", "MBAP", "MBMA", "MBSS", "MBTO", "MCAS", "MCOL", "MCOR", "MDIA", 
    "MDKA", "MDKI", "MDLA", "MDLN", "MDRN", "MEDC", "MEDS", "MEGA", "MEJA", "MENN", 
    "MERI", "MERK", "META", "MFMI", "MGNA", "MGRO", "MHKI", "MICE", "MIDI", "MIKA", 
    "MINA", "MINE", "MIRA", "MITI", "MKAP", "MKPI", "MKTR", "MLBI", "MLIA", "MLPL", 
    "MLPT", "MMLP", "MMIX", "MNCN", "MOLI", "MORA", "MPOW", "MPMX", "MPPA", "MPRO", 
    "MPXL", "MRAT", "MREI", "MSIE", "MSIN", "MSJA", "MSKY", "MSTI", "MTDL", "MTEL", 
    "MTFN", "MTLA", "MTMH", "MTPS", "MTRA", "MTRN", "MTSM", "MTWI", "MUTU", "MYOH", 
    "MYOR", "MYTX", "NAIK", "NANO", "NASA", "NASI", "NATO", "NAYZ", "NCKL", "NELY", 
    "NEST", "NETV", "NICE", "NICK", "NICL", "NIKL", "NINE", "NIRO", "NISP", "NOBU", 
    "NPGF", "NRCA", "NSSS", "NTBK", "NUSA", "NZIA", "OASA", "OBAT", "OBMD", "OCAP", 
    "OILS", "OKAS", "OLIV", "OMED", "OMRE", "OPMS", "PACK", "PADA", "PADI", "PALM", 
    "PAMG", "PANI", "PANR", "PANS", "PART", "PBID", "PBSA", "PBRX", "PCAR", "PDES", 
    "PDPP", "PEGE", "PEHA", "PELI", "PENT", "PERW", "PEVE", "PGAS", "PGEO", "PGJO", 
    "PGLI", "PGUN", "PICO", "PIPA", "PJAA", "PJHB", "PKPK", "PLAN", "PLAS", "PLIN", 
    "PMJS", "PMMP", "PMUI", "PNBN", "PNBS", "PNGO", "PNIN", "PNLF", "PNSE", "POLA", 
    "POLI", "POLL", "POLU", "POLY", "POOL", "PORT", "POSA", "POWR", "PPGL", "PPRI", 
    "PPRE", "PPRO", "PRAY", "PRDA", "PRIM", "PSAB", "PSAT", "PSDN", "PSGO", "PSKT", 
    "PSSI", "PTBA", "PTDU", "PTIS", "PTMP", "PTMR", "PTPP", "PTPS", "PTPW", "PTRO", 
    "PTSN", "PTSP", "PUDP", "PURA", "PURE", "PURI", "PWON", "PYFA", "PZZA", "RAAM", 
    "RAFI", "RAJA", "RALS", "RANC", "RATU", "RBMS", "RCCC", "RDTX", "REAL", "RELF", 
    "RELI", "REPP", "RGAS", "RICY", "RIGS", "RIMO", "RISE", "RLCO", "RMBA", "RMKE", 
    "RMKO", "RMLP", "ROCK", "RODA", "ROLI", "RONY", "ROTI", "RSCH", "RSGK", "RUIS", 
    "RUNS", "SAFE", "SAGE", "SAGI", "SAME", "SAMF", "SAMR", "SAMP", "SANO", "SAPX", 
    "SATU", "SBAT", "SBMA", "SCCO", "SCMA", "SCNP", "SCPI", "SDMU", "SDPC", "SDRA", 
    "SEMA", "SFAN", "SGER", "SGGH", "SGJL", "SGRO", "SHID", "SHIP", "SICO", "SIDO", 
    "SIER", "SILO", "SIMA", "SIMP", "SINI", "SIPD", "SKBM", "SKLT", "SKRN", "SKYB", 
    "SLIS", "SMAR", "SMDM", "SMDR", "SMGA", "SMGR", "SMKM", "SMKL", "SMLE", "SMMA", 
    "SMMT", "SMRA", "SMRU", "SMSM", "SNLK", "SOCI", "SOFA", "SOHO", "SOLA", "SONA", 
    "SOSS", "SOTS", "SOUL", "SPMA", "SPRE", "SPTO", "SQMI", "SRAJ", "SREI", "SRIL", 
    "SRSN", "SRTG", "SSIA", "SSMS", "SSTM", "STAA", "STAR", "STRK", "STTP", "SUGI", 
    "SULI", "SUNI", "SUPA", "SUPR", "SURE", "SWAT", "SWID", "SYAI", "TALF", "TAMA", 
    "TAMU", "TAPG", "TARA", "TAXI", "TAYS", "TBIG", "TBLA", "TBMS", "TCID", "TCPI", 
    "TDPM", "TEBE", "TECH", "TELE", "TFAS", "TFCO", "TGKA", "TGRA", "TGUK", "TIFA", 
    "TINS", "TIRA", "TIRT", "TKIM", "TLDN", "TLKM", "TMAS", "TMPO", "TNCA", "TOBA", 
    "TOOL", "TOPS", "TOSK", "TOTL", "TOTO", "TOWR", "TOYS", "TPAI", "TPIA", "TPMA", 
    "TRAM", "TRGU", "TRIL", "TRIM", "TRIN", "TRIO", "TRIS", "TRJA", "TRON", "TRST", 
    "TRUE", "TRUK", "TRUS", "TSPC", "TUGU", "TULT", "TYRE", "UANG", "UCID", "UDNG", 
    "UFOE", "ULTJ", "UNIC", "UNIQ", "UNIT", "UNSP", "UNTR", "UNVR", "URBN", "UVCR", 
    "VAST", "VATE", "VCOK", "VERN", "VICI", "VICO", "VINS", "VISA", "VISI", "VIVA", 
    "VKTR", "VOKS", "VOSS", "VRNA", "VTNY", "WAPO", "WBSA", "WEGE", "WEHA", "WGSH", 
    "WICO", "WIDI", "WIFI", "WIIM", "WIKA", "WINE", "WINR", "WINS", "WIRG", "WITA", 
    "WMPP", "WMUU", "WOMF", "WONS", "WOOD", "WOWS", "WPOW", "WSBP", "WSKT", "WTON", 
    "YELO", "YOII", "YPAS", "YULE", "YUPI", "ZATA", "ZBRA", "ZENI", "ZINC", "ZONE", 
    "ZYRX"
]
_seen = set(); ALL_STOCKS = list(dict.fromkeys(_RAW))
# Backward compat aliases
STOCKS_30 = ALL_STOCKS
STOCKS_60 = ALL_STOCKS

# ════════════════════════════════════════
#  DATASECTORS FETCH — THREAD-SAFE
#  Fix utama: gak pakai @st.cache_data di thread!
#  Fix delay: cache-bust + no-cache header
# ════════════════════════════════════════

def add_indicators(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df = df.copy(); c = df["Close"]

    df["E9"]   = c.ewm(span=9,   adjust=False).mean()
    df["E21"]  = c.ewm(span=21,  adjust=False).mean()
    df["E50"]  = c.ewm(span=50,  adjust=False).mean()
    df["E200"] = c.ewm(span=200, adjust=False).mean()

    d  = c.diff()
    g  = d.clip(lower=0).ewm(span=14, adjust=False).mean()
    l  = (-d.clip(upper=0)).ewm(span=14, adjust=False).mean()
    rsi_raw = (100 - 100 / (1 + g / l.replace(0, np.nan))).fillna(50)
    df["RSI"]     = rsi_raw
    df["RSI_EMA"] = rsi_raw.ewm(span=14, adjust=False).mean()

    d5 = c.diff()
    g5 = d5.clip(lower=0).ewm(span=5, adjust=False).mean()
    l5 = (-d5.clip(upper=0)).ewm(span=5, adjust=False).mean()
    df["RSI5"] = (100 - 100 / (1 + g5 / l5.replace(0, np.nan))).fillna(50)

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd_line   = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df["MACD"]       = macd_line
    df["MACD_Sig"]   = signal_line
    df["MACD_H"]     = (macd_line - signal_line).fillna(0)
    df["MACD_Cross"] = (macd_line > signal_line) & (macd_line.shift(1) <= signal_line.shift(1))

    lo10  = df["Low"].rolling(10).min()
    hi10  = df["High"].rolling(10).max()
    raw_k = (100 * (c - lo10) / (hi10 - lo10).replace(0, np.nan)).fillna(50)
    stoch_k = raw_k.ewm(span=5, adjust=False).mean()
    stoch_d = stoch_k.ewm(span=5, adjust=False).mean()
    df["STOCH_K"]         = stoch_k
    df["STOCH_D"]         = stoch_d
    df["STOCH_CROSS_UP"]  = (stoch_k > stoch_d) & (stoch_k.shift(1) <= stoch_d.shift(1))
    df["STOCH_CROSS_DOWN"]= (stoch_k < stoch_d) & (stoch_k.shift(1) >= stoch_d.shift(1))

    df["RVOL"] = (df["Volume"] / df["Volume"].rolling(20).mean().replace(0, np.nan)).fillna(1)

    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - c.shift()).abs(),
        (df["Low"]  - c.shift()).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()

    body_top = df[["Close","Open"]].max(axis=1)
    body_bot = df[["Close","Open"]].min(axis=1)
    hl = (df["High"] - df["Low"]).replace(0, np.nan)
    df["LW"]   = ((body_bot - df["Low"])  / hl * 100).fillna(0)
    df["UW"]   = ((df["High"] - body_top) / hl * 100).fillna(0)
    df["Body"] = (body_top - body_bot) / hl * 100

    try:
        tp = (df["High"] + df["Low"] + df["Close"]) / 3
        df["VWAP"] = (tp * df["Volume"]).cumsum() / df["Volume"].cumsum()
    except:
        df["VWAP"] = df["Close"]

    df["PctChange"] = c.pct_change() * 100

    if "FBuy" in df.columns and "FSell" in df.columns:
        df["FNet"]   = df["FBuy"] - df["FSell"]
        df["FCum"]   = df["FNet"].cumsum()
        df["FNet3"]  = df["FNet"].rolling(3).sum()
        df["FNet8"]  = df["FNet"].rolling(8).sum()
        total_f      = df["FBuy"] + df["FSell"]
        df["FRatio"] = (df["FBuy"] / total_f.replace(0, np.nan)).fillna(0.5)
        df["FAkum"]  = df["FNet"] > 0
        df["FAkum3"] = df["FNet3"] > 0

    if "Value" in df.columns:
        df["Value"]  = pd.to_numeric(df["Value"],  errors='coerce').fillna(0)
        df["AvgVal"] = df["Value"].rolling(20).mean()
        df["RVal"]   = df["Value"] / df["AvgVal"].replace(0, np.nan)
    if "Frequency" in df.columns:
        df["Frequency"] = pd.to_numeric(df["Frequency"], errors='coerce').fillna(0)
        df["AvgFreq"]   = df["Frequency"].rolling(20).mean()
        df["RFreq"]     = df["Frequency"] / df["AvgFreq"].replace(0, np.nan)

    return df

# ════════════════════════════════════════
#  SCORING
# ════════════════════════════════════════
def get_sinyal(df, mode="Intraday"):
    if len(df) < 3: return "WAIT ❌", 0, [], False
    r = df.iloc[-1]; p = df.iloc[-2]; p2 = df.iloc[-3] if len(df) >= 3 else p

    cl    = sf(r.get("Close",0))
    e9    = sf(r.get("E9")); e21 = sf(r.get("E21")); e50 = sf(r.get("E50"))
    rsi   = sf(r.get("RSI",50)); rsi_ema = sf(r.get("RSI_EMA",50))
    rsi_ema_p = sf(p.get("RSI_EMA",50))
    sk    = sf(r.get("STOCH_K",50)); sd = sf(r.get("STOCH_D",50))
    sk_p  = sf(p.get("STOCH_K",50)); sd_p = sf(p.get("STOCH_D",50))
    mh    = sf(r.get("MACD_H",0)); mh_p = sf(p.get("MACD_H",0))
    macd  = sf(r.get("MACD",0));   sig  = sf(r.get("MACD_Sig",0))
    macd_p = sf(p.get("MACD",0)); sig_p = sf(p.get("MACD_Sig",0))
    rv    = sf(r.get("RVOL",1))
    lw    = sf(r.get("LW",0))
    uw    = sf(r.get("UW",0))
    vwap  = sf(r.get("VWAP",cl))

    score = 0; flags = []

    ema_bull = e9 > e21 > e50
    ema_gc   = e9 > e21
    ema_bear = e9 < e21 < e50
    p_e9 = sf(p.get("E9")); p_e21 = sf(p.get("E21"))
    gc_now = (e9 > e21) and (p_e9 <= p_e21)

    if ema_bull:   score += 15; flags.append("EMA▲")
    elif ema_gc:   score += 8;  flags.append("EMA GC")
    elif ema_bear: score -= 12

    stoch_os_strict = sk < 20
    stoch_os_broad  = sk < 50 and ema_bear
    stoch_ob        = sk > 80
    stoch_cross_up  = sk > sd and sk_p <= sd_p
    stoch_cross_down= sk < sd and sk_p >= sd_p

    if stoch_os_strict:
        score += 12; flags.append(f"STOCH OS {sk:.0f}")
        if stoch_cross_up: score += 8; flags.append("STOCH ↑ Cross")
    elif stoch_os_broad:
        score += 6; flags.append(f"STOCH {sk:.0f}")
        if stoch_cross_up: score += 5
    elif stoch_ob:
        score -= 10; flags.append(f"STOCH OB {sk:.0f}")
    elif stoch_cross_up and sk < 60:
        score += 6; flags.append("STOCH ↑")

    rsi_os     = rsi_ema < 40
    rsi_os_str = rsi_ema < 30
    rsi_ob     = rsi_ema > 65
    rsi_cross_up  = rsi_ema > rsi_ema_p and rsi_ema_p < 40
    rsi_cross_down= rsi_ema < rsi_ema_p and rsi_ema_p > 65

    if rsi_os_str:
        score += 12; flags.append(f"RSI EMA {rsi_ema:.0f} OS")
        if rsi_cross_up: score += 8; flags.append("RSI ↑ Pivot")
    elif rsi_os:
        score += 7; flags.append(f"RSI EMA {rsi_ema:.0f}")
        if rsi_cross_up: score += 5; flags.append("RSI ↑")
    elif 45 < rsi_ema < 65:
        score += 5; flags.append(f"RSI {rsi_ema:.0f}")
    elif rsi_ob:
        score -= 8; flags.append(f"RSI OB {rsi_ema:.0f}")

    macd_cross_up   = macd > sig and macd_p <= sig_p
    macd_cross_down = macd < sig and macd_p >= sig_p
    macd_expanding  = mh > 0 and mh > mh_p
    macd_weakening  = mh < 0 and mh < mh_p

    if macd_cross_up:     score += 10; flags.append("MACD ↑ Cross")
    elif macd_expanding:  score += 7;  flags.append("MACD Expand")
    elif mh > 0:          score += 3
    elif macd_cross_down: score -= 10; flags.append("MACD ↓ Cross")
    elif macd_weakening:  score -= 5

    if rv > 3:     score += 15; flags.append(f"RVOL {rv:.1f}x 🔥")
    elif rv > 2:   score += 10; flags.append(f"RVOL {rv:.1f}x")
    elif rv > 1.5: score += 5;  flags.append(f"RVOL {rv:.1f}x")
    elif rv < 0.5: score -= 5

    if lw > 60:   score += 10; flags.append(f"LWick {lw:.0f}%")
    elif lw > 40: score += 6;  flags.append(f"LWick {lw:.0f}%")
    elif lw > 25: score += 3

    uw_sell = uw > 50 and sf(r.get("Body",50)) < 30
    if uw_sell: flags.append(f"UWick JUAL {uw:.0f}%")

    if cl > vwap:   score += 5;  flags.append("VWAP▲")
    elif cl < vwap: score -= 3

    entry_kuat = (
        (stoch_os_strict or stoch_os_broad) and
        (rsi_os or rsi_cross_up) and
        (macd_cross_up or macd_expanding) and
        rv >= 1.2
    )
    entry_moderate = (
        sum([stoch_os_strict or stoch_cross_up,
             rsi_os or rsi_cross_up,
             macd_expanding or macd_cross_up]) >= 2 and rv >= 1.0
    )
    is_haka    = (ema_bull and rv > 1.5 and macd_expanding and rsi_ema > 50 and sk > sd and cl > vwap)
    is_super   = (entry_kuat and rv > 2 and score >= 35)
    is_rebound = (entry_kuat and (stoch_os_strict or rsi_os_str))
    is_sell    = (uw_sell and (stoch_ob or rsi_ob) and rv > 1.0)

    fnet3  = sf(r.get("FNet3",0))
    fnet8  = sf(r.get("FNet8",0))
    fratio = sf(r.get("FRatio",0.5))

    if fnet3 > 0 and fnet8 > 0:
        score += 10; flags.append("🔵 Asing Akum")
        if fratio > 0.7: score += 5; flags.append("Asing Dominan")
    elif fnet3 < 0 and fnet8 < 0:
        score -= 8; flags.append("🔴 Asing Jual")

    smart_money = (fnet3 > 0 and rv > 1.5 and rsi_ema < 55)
    if smart_money: score += 8; flags.append("💎 Smart Money")

    is_bandar = (fnet3 > 0 and fnet8 > 0 and fratio > 0.6 and rv > 1.2 and ema_gc)

    if is_sell:    return "JUAL ⬇️",    score, flags, gc_now
    if is_bandar:  return "BANDAR 🔵",  score, flags, gc_now
    if is_haka:    return "HAKA 🔨",    score, flags, gc_now
    if is_super:   return "SUPER 🔥",   score, flags, gc_now
    if is_rebound: return "REBOUND 🏀", score, flags, gc_now
    if entry_moderate and score >= 20: return "AKUM 📦", score, flags, gc_now
    if score >= 15: return "ON TRACK ✅", score, flags, gc_now
    return "WAIT ❌", score, flags, gc_now

def get_aksi(score, gc_now, sinyal):
    if sinyal in ["HAKA 🔨","SUPER 🔥"] and score >= 35: return "AT ENTRY 🎯"
    elif sinyal == "REBOUND 🏀": return "WATCH REB 🏀"
    elif gc_now:    return "GC NOW ⚡"
    elif score >= 25: return "AT ENTRY 🎯"
    elif score >= 15: return "WAIT GC ⏳"
    else:             return "WAIT ❌"

def get_rsi_sig(rsi):
    if rsi >= 60:  return "UP","#00ff88"
    elif rsi < 35: return "DEAD","#ff3d5a"
    elif rsi < 45: return "DOWN","#ff7b00"
    else:          return "NEUTRAL","#4a5568"

def get_trend(df):
    if df is None or len(df) < 2: return "NETRAL","#4a5568"
    r = df.iloc[-1]
    e9 = sf(r.get("E9")); e21 = sf(r.get("E21")); e50 = sf(r.get("E50",0))
    if e9 > e21 > e50: return "BULL 🔥","#00ff88"
    if e9 < e21 < e50: return "BEAR ❄️","#ff3d5a"
    return "NETRAL","#4a5568"

def get_fase(df):
    if df is None or len(df) < 5: return "AKUM","#00e5ff"
    vn = df["Volume"].iloc[-3:].mean()
    va = df["Volume"].iloc[-20:-3].mean() if len(df) >= 20 else vn
    cn = sf(df["Close"].iloc[-1]); cp = sf(df["Close"].iloc[-5])
    vr = vn / max(va, 1); pr = cn / max(cp, 1)
    if vr > 1.5 and pr > 1.02: return "BIG AKUM 🔥","#ff7b00"
    if vr > 1.2 and pr > 1.0:  return "AKUM 📦","#00e5ff"
    if vr > 1.3 and pr < 0.99: return "DIST ⚠️","#ff3d5a"
    return "NETRAL","#4a5568"

# ════════════════════════════════════════
#  BUILD RESULT
# ════════════════════════════════════════
def build_result(ticker, df_main, df_daily, mode):
    try:
        df   = add_indicators(df_main)
        # df_daily: raw untuk gain/val, add_indicators hanya untuk trend/fase
        df_d = df_daily
        df_d_ind = add_indicators(df_daily) if df_daily is not None and len(df_daily) >= 10 else None

        sinyal, score, flags, gc_now = get_sinyal(df, mode)
        aksi = get_aksi(score, gc_now, sinyal)
        trend, trend_col = get_trend(df_d_ind if df_d_ind is not None else df)
        fase,  fase_col  = get_fase(df_d_ind  if df_d_ind is not None else df)

        r  = df.iloc[-1]; r1 = df.iloc[-2] if len(df) > 1 else r
        cl = sf(r.get("Close",0))
        if cl == 0: return None

        vol = sf(r.get("Volume",0))
        atr = sf(r.get("ATR", cl * 0.02))
        rv  = sf(r.get("RVOL",1))
        rsi = sf(r.get("RSI",50))
        rsi5= sf(r.get("RSI5",50))
        e9  = sf(r.get("E9",cl))
        lw  = sf(r.get("LW",0))

        # GAIN & VAL — persis Theta Turbo (float direct, no sf wrapping)
        if df_d is not None and len(df_d) >= 2:
            try:
                c1       = float(df_d.iloc[-1]["Close"])
                c0       = float(df_d.iloc[-2]["Close"])
                gain     = (c1 - c0) / max(c0, 1) * 100
                daily_vol= float(df_d.iloc[-1]["Volume"])
                vb       = c1 * daily_vol / 1e9
            except:
                gain = 0.0; vb = cl * vol / 1e9
        else:
            gain = 0.0; vb = cl * vol / 1e9

        val_str = f"{vb:.1f}B" if vb >= 1 else f"{round(vb*1000,0):.0f}M"

        if mode == "BSJP":
            tp = cl + 3.0 * atr; sl = cl - 1.5 * atr
        elif mode == "Swing":
            tp = cl + 4.0 * atr; sl = cl - 2.0 * atr
        else:
            tp = cl + 4.0 * atr; sl = cl - 2.0 * atr

        profit = (tp - cl) / cl * 100

        if "WAIT" in aksi:
            entry_str = "WAIT GC"; entry_val = 0
        else:
            entry_val = int(min(cl, e9 * 1.002)); entry_str = str(entry_val)

        rsi_sig, rsi_col = get_rsi_sig(rsi)
        rvol_str = f"{rv*100:.0f}%" if rv < 10 else f"{rv:.1f}x"
        prob = max(5, min(95, score + 50))

        # FIX 3: ASING — hapus threshold FTotal, langsung dari FNet3/FNet8
        # FTotal threshold bikin semua saham kecil/mid jadi "no data"
        fnet3  = sf(r.get("FNet3", 0))
        fnet8  = sf(r.get("FNet8", 0))
        fratio = sf(r.get("FRatio", 0.5))
        rfreq  = sf(r.get("RFreq", 1))
        fbuy   = sf(r.get("FBuy", 0))
        fsell  = sf(r.get("FSell", 0))

        # Cek ada data asing atau tidak (FBuy + FSell > 0)
        has_asing = (fbuy + fsell) > 0
        if not has_asing:
            fdir = "—"; fc = "#4a5568"
        elif fnet3 > 0 and fnet8 > 0:
            fdir = "🔵 BELI"; fc = "#4da6ff"
        elif fnet3 < 0 and fnet8 < 0:
            fdir = "🔴 JUAL"; fc = "#ff3d5a"
        else:
            fdir = "⚪ MIX"; fc = "#888888"

        return {
            "T": ticker, "Prob": prob, "FDir": fdir, "FC": fc,
            "FNet3": int(fnet3), "FNet8": int(fnet8),
            "FRatio": round(fratio,2), "RFreq": round(rfreq,1),
            "Gain": round(gain,1), "Wick": round(lw,1),
            "Aksi": aksi, "Sinyal": sinyal,
            "RVOL_raw": round(rv,2), "RVOL_str": rvol_str,
            "Entry_str": entry_str, "Entry_val": entry_val,
            "Now": int(cl), "TP": int(tp), "SL": int(sl),
            "Profit": round(profit,1), "Upside": round(profit,1),
            "RSI_Sig": rsi_sig, "RSI_Col": rsi_col, "RSI5": round(rsi5,1),
            "Val": val_str, "Fase": fase, "Fase_col": fase_col,
            "Trend": trend, "Trend_col": trend_col,
            "Score": score, "GC": gc_now,
            "Flags": " · ".join(flags[:3]), "ATR": round(atr,0),
        }
    except Exception as _e:
        return None  # silently skip bad tickers

# ════════════════════════════════════════
#  SCAN ENGINE — PARALLEL + LIVE PREVIEW
#  Fix: fetch_ds dipanggil langsung (no @st.cache_data wrapper)
#  Preview: tampilkan hasil sementara setiap batch
# ════════════════════════════════════════
def do_scan(stocks, mode, pb, status_ph, preview_ph=None, force_fresh=False):
    """
    10 threads parallel — thread-safe (disk cache, bukan @st.cache_data).
    DS rate: 1000 req/min. 10 threads × 1.5s = ~6.7 req/s → aman.
    778 tickers ÷ 10 ≈ 2 menit vs 20 menit sequential.
    """
    n  = len(stocks)
    tf = "daily" if mode == "Swing" else "15m"
    raw_main = {}; raw_ctx = {}

    # Cache check dulu — skip yang sudah fresh
    need_main = []
    for t in stocks:
        if not force_fresh:
            cached = cache_get(t, tf)
            if cached is not None:
                raw_main[t] = cached; continue
        need_main.append(t)

    n_cached = len(raw_main)
    status_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#ff7b00">'
        f'📊 {n_cached} cache · {len(need_main)} fetch yFinance [{tf}]...</div>',
        unsafe_allow_html=True)
    pb.progress(0.05)

    def _fm(t): return t, _fetch_raw(t, tf, True)
    done = [0]
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fm, t): t for t in need_main}
        for f in as_completed(futs):
            done[0] += 1
            if done[0] % 20 == 0:
                pb.progress(0.05 + (done[0] / max(len(need_main), 1)) * 0.38)
            try:
                t, df = f.result(timeout=15)
                if df is not None and len(df) >= 20: raw_main[t] = df
            except: pass

    if True:  # Fetch daily untuk SEMUA mode — Gain & Val harus akurat
        need_ctx = [t for t in stocks]
        status_ph.markdown(
            '<div style="font-family:Space Mono,monospace;font-size:11px;color:#00e5ff">'
            '📅 Daily context untuk Gain & Val (10 threads)...</div>',
            unsafe_allow_html=True)
        def _fc(t): return t, _fetch_raw(t, "daily", force_fresh)
        done2 = [0]
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(_fc, t): t for t in need_ctx}
            for f in as_completed(futs):
                done2[0] += 1
                if done2[0] % 50 == 0:
                    pb.progress(0.43 + (done2[0] / max(n, 1)) * 0.35)
                try:
                    t, df = f.result(timeout=15)
                    if df is not None and len(df) >= 2:
                        raw_ctx[t] = df
                except: pass

    pb.progress(0.85)
    status_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#00ff88">'
        f'⚙️ Processing {len(raw_main)}/{n}...</div>', unsafe_allow_html=True)

    results = []
    for ticker in stocks:
        df_main = raw_main.get(ticker)
        df_ctx  = raw_ctx.get(ticker) if mode != "Swing" else None
        if df_main is None or len(df_main) < 20: continue
        r = build_result(ticker, df_main, df_ctx, mode)
        if r: results.append(r)

    pb.progress(1.0); status_ph.empty()
    results.sort(key=lambda x: x["Prob"], reverse=True)

    # Auto Threshold filter berdasarkan regime IHSG
    threshold = st.session_state.get("threshold", get_auto_threshold("UNKNOWN"))
    min_prob  = threshold["min_prob"]
    min_score = threshold["min_score"]
    regime    = st.session_state.get("regime", "UNKNOWN")

    # Filter — tapi BANDAR/SUPER selalu lolos
    filtered = []
    for r in results:
        sig = r.get("Sinyal","")
        is_premium = any(k in sig for k in ["BANDAR","SUPER","HAKA"])
        if is_premium or (r["Prob"] >= min_prob and r["Score"] >= min_score):
            filtered.append(r)

    # RED regime: tambahkan warning kalau hasil terlalu sedikit
    if regime == "RED" and len(filtered) < 5:
        # Relax sedikit di RED — show top 5 minimal
        filtered = results[:5]

    return filtered[:DISPLAY_TOP]

# ════════════════════════════════════════
#  PAGE CONFIG
# ════════════════════════════════════════
st.set_page_config(
    layout="wide", page_title="Cyrus Fund Scanner",
    page_icon="🎯", initial_sidebar_state="collapsed")

_CSS = '''<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@700&display=swap');
html,body,[data-testid="stAppViewContainer"]{background:#060a0e!important;color:#c9d1d9!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
button[data-testid="baseButton-primary"]{background:#ff7b00!important;color:#000!important;font-family:'Space Mono',monospace!important;font-weight:700!important;}
.mc{background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:8px 14px;flex:1;min-width:80px;border-top:3px solid #4a5568;}
.ml{font-size:9px;color:#4a5568;letter-spacing:1px;text-transform:uppercase}
.mv{font-family:'Space Mono',monospace;font-size:20px;font-weight:700;color:#e6edf3}
/* Age badge */
.cache-fresh{color:#00ff88;font-size:9px;font-family:Space Mono,monospace;}
.cache-stale{color:#ff7b00;font-size:9px;font-family:Space Mono,monospace;}
</style>'''
st.markdown(_CSS, unsafe_allow_html=True)

# ════════════════════════════════════════
#  SESSION STATE INIT + DISK PERSISTENCE
#  Fix: browser reload (JS timer) reset session state.
#  Solusi: simpan hasil scan ke disk, load otomatis saat startup.
# ════════════════════════════════════════
RESULTS_FILE = CACHE_DIR / "last_results.pkl"
RESULTS_TTL  = 600  # 10 menit — hasil masih relevan

def save_results(mode, results, scan_ts):
    """Simpan hasil scan ke disk."""
    try:
        data = {"mode": mode, "results": results, "ts": scan_ts,
                "keys": {f"res_{mode.lower()}": results}}
        RESULTS_FILE.write_bytes(pickle.dumps(data))
    except: pass

def load_results():
    """Load hasil scan dari disk kalau masih fresh."""
    try:
        if RESULTS_FILE.exists():
            data = pickle.loads(RESULTS_FILE.read_bytes())
            if time.time() - data["ts"] < RESULTS_TTL:
                return data
    except: pass
    return None

_defaults = {
    "res_momentum":[], "res_intraday":[], "res_bsjp":[], "res_swing":[], "wl_res":[],
    "last_scan": None, "scan_mode": "", "wl_tickers": [],
    "regime": "UNKNOWN", "threshold": get_auto_threshold("UNKNOWN"),
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Auto-restore dari disk setelah browser refresh
if not any([st.session_state.res_momentum, st.session_state.res_intraday,
            st.session_state.res_bsjp, st.session_state.res_swing]):
    _saved = load_results()
    if _saved:
        mode_key = f"res_{_saved['mode'].lower()}"
        if mode_key in st.session_state:
            st.session_state[mode_key] = _saved["results"]
        st.session_state.last_scan = _saved["ts"]
        st.session_state.scan_mode = _saved["mode"]

# ════════════════════════════════════════
#  UI HELPERS
# ════════════════════════════════════════
def _ab(a):
    a = str(a)
    if "AT ENTRY" in a:   c, bg = "#00ff88","#1a472a"
    elif "GC NOW" in a:   c, bg = "#00e5ff","#0d2233"
    elif "WATCH" in a:    c, bg = "#ffb700","#2a2000"
    else:                 c, bg = "#ff3d5a","#2a0d0d"
    return f'<span style="background:{bg};color:{c};padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;font-family:Space Mono,monospace">{a}</span>'

def _sb(s):
    s = str(s)
    M = {
        "BANDAR":("#4da6ff","#0a1525"), "HAKA":("#00ff88","#0a2010"),
        "SUPER":("#bf5fff","#150a25"),  "REBOUND":("#ffb700","#251800"),
        "JUAL":("#ff3d5a","#250a0d"),   "AKUM":("#00e5ff","#0a1515"),
        "ON TRACK":("#00ff88","#0a1a0a"),
    }
    for k, (c, bg) in M.items():
        if k in s:
            return f'<span style="background:{bg};color:{c};padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700;border:1px solid {c}44">{s}</span>'
    return f'<span style="background:#111;color:#4a5568;padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700">{s}</span>'

def show_met(res):
    if not res: return
    try:
        bd   = sum(1 for x in res if "BANDAR"  in x.get("Sinyal",""))
        hk   = sum(1 for x in res if "HAKA"    in x.get("Sinyal",""))
        sp   = sum(1 for x in res if "SUPER"   in x.get("Sinyal",""))
        rb   = sum(1 for x in res if "REBOUND" in x.get("Sinyal",""))
        beli = sum(1 for x in res if "AT ENTRY" in x.get("Aksi",""))
        ab   = sum(1 for x in res if "BELI" in x.get("FDir",""))
        aj   = sum(1 for x in res if "JUAL" in x.get("FDir",""))
        ap   = round(sum(x.get("Prob",50) for x in res) / len(res))
        pc   = "#00ff88" if ap >= 65 else "#ffb700" if ap >= 55 else "#ff3d5a"
        top  = res[0].get("T","—")
    except: return
    html = '<div style="display:flex;gap:8px;margin:10px 0;flex-wrap:wrap">'
    for lbl,val,col in [
        ("BANDAR 🔵",bd,"#4da6ff"),("HAKA 🔨",hk,"#00ff88"),
        ("SUPER 🔥",sp,"#bf5fff"), ("REBOUND",rb,"#ffb700"),
        ("BELI",beli,"#00ff88"),   ("AVG PROB",str(ap)+"%",pc),
        ("TOP PICK",top,"#ff7b00"),("ASING BUY",ab,"#4da6ff"),
        ("ASING SELL",aj,"#ff3d5a")
    ]:
        fs = "16px" if lbl == "TOP PICK" else "20px"
        html += f'<div class="mc" style="border-top-color:{col}"><div class="ml">{lbl}</div><div class="mv" style="color:{col};font-size:{fs}">{val}</div></div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

TH = ['EMITEN','GAIN','WICK','AKSI','SINYAL','RVOL','ENTRY','NOW','TP','SL','PROFIT','RSI','RSI5M','VAL','FASE','TREND','ASING']
TC = {'TP':'#00ff88','SL':'#ff3d5a','PROFIT':'#00e5ff','ASING':'#4da6ff'}

def show_tbl(res):
    if not res: return
    rows = ""
    for r in res:
        try:
            gc = "#00ff88" if r.get("Gain",0) > 0 else "#ff3d5a"
            wc = "#00ff88" if r.get("Wick",0) > 30 else "#4a5568"
            rc = r.get("RSI_Col","#4a5568")
            fd = r.get("FDir","—")
            fc = r.get("FC","#4a5568")
            rows += "<tr style='font-family:Space Mono,monospace;font-size:10px'>"
            rows += f"<td style='padding:5px 8px;font-weight:700;color:#e6edf3;border-bottom:1px solid #1c2533;white-space:nowrap'>{r.get('T','?')}</td>"
            rows += f"<td style='padding:5px 6px;color:{gc};font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r.get('Gain',0):+.1f}%</td>"
            rows += f"<td style='padding:5px 6px;color:{wc};border-bottom:1px solid #1c2533;text-align:center'>{int(r.get('Wick',0))}%</td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'>{_ab(r.get('Aksi','WAIT'))}</td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'>{_sb(r.get('Sinyal','—'))}</td>"
            rows += f"<td style='padding:5px 6px;color:#ff7b00;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r.get('RVOL_str','—')}</td>"
            rows += f"<td style='padding:5px 6px;color:#4a5568;border-bottom:1px solid #1c2533;text-align:center'>{r.get('Entry_str','—')}</td>"
            rows += f"<td style='padding:5px 6px;color:#e6edf3;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r.get('Now',0):,}</td>"
            rows += f"<td style='padding:5px 6px;background:#0d2b0d;color:#00ff88;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r.get('TP',0):,}</td>"
            rows += f"<td style='padding:5px 6px;background:#2b0d0d;color:#ff3d5a;border-bottom:1px solid #1c2533;text-align:center'>{r.get('SL',0):,}</td>"
            rows += f"<td style='padding:5px 6px;color:#00ff88;border-bottom:1px solid #1c2533;text-align:center'>{r.get('Profit',0):.1f}%</td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{rc};font-weight:700'>{r.get('RSI_Sig','—')}</span></td>"
            rows += f"<td style='padding:5px 6px;color:{rc};border-bottom:1px solid #1c2533;text-align:center'>{r.get('RSI5',0):.0f}</td>"
            rows += f"<td style='padding:5px 6px;color:#4a5568;font-size:9px;border-bottom:1px solid #1c2533;text-align:center'>{r.get('Val','—')}</td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{r.get('Fase_col','#4a5568')};font-size:10px'>{r.get('Fase','')}</span></td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{r.get('Trend_col','#4a5568')};font-weight:700;font-size:10px'>{r.get('Trend','')}</span></td>"
            rows += f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center;font-size:10px'><span style='color:{fc}'>{fd}</span></td>"
            rows += "</tr>"
        except: continue
    hdrs = "".join(
        f"<th style='padding:7px 6px;color:{TC.get(h,'#4a5568')};font-family:Space Mono,monospace;font-size:9px;"
        f"letter-spacing:1px;border-bottom:2px solid #1c2533;{'text-align:left' if h=='EMITEN' else ''}'>{h}</th>"
        for h in TH
    )
    st.markdown(
        f"<div style='overflow-x:auto;border-radius:8px;border:1px solid #1c2533;max-height:72vh;overflow-y:auto'>"
        f"<table style='width:100%;border-collapse:collapse'>"
        f"<thead><tr style='background:#080c10;position:sticky;top:0;z-index:10'>{hdrs}</tr></thead>"
        f"<tbody style='background:#0d1117'>{rows}</tbody></table>"
        f"<div style='padding:5px 12px;background:#080c10;font-family:Space Mono,monospace;font-size:9px;"
        f"color:#4a5568;border-top:1px solid #1c2533'>SL=2xATR · TP=4xATR · yFinance 📊</div>"
        f"</div>", unsafe_allow_html=True)

def show_cards(res):
    for idx in range(0, min(9, len(res)), 3):
        cols = st.columns(3)
        for ci, r in enumerate(res[idx:idx+3]):
            pc = "#00ff88" if r["Prob"] >= 75 else "#ffb700" if r["Prob"] >= 60 else "#ff7b00"
            gc = "#00ff88" if r["Gain"] > 0 else "#ff3d5a"
            fd = r.get("FDir","—"); fc = r.get("FC","#4a5568")
            bc = "#4da6ff44" if "BANDAR" in r["Sinyal"] else "#1c2533"
            with cols[ci]:
                st.markdown(
                    f"<div style='background:#0d1117;border:1px solid {bc};border-radius:10px;padding:12px;margin-bottom:8px'>"
                    f"<div style='display:flex;justify-content:space-between'>"
                    f"<div><div style='font-family:Space Mono,monospace;font-size:16px;font-weight:700;color:#e6edf3'>{r['T']}</div>"
                    f"<div style='font-size:10px;color:{gc}'>{r['Now']:,} ({r['Gain']:+.1f}%)</div></div>"
                    f"<div style='text-align:right'><div style='font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{pc}'>{r['Prob']}%</div>"
                    f"<div style='font-size:9px;color:#4a5568'>PROB</div></div></div>"
                    f"<div style='margin:6px 0'>{_sb(r['Sinyal'])} {_ab(r['Aksi'])}</div>"
                    f"<div style='height:3px;background:#1c2533;border-radius:2px;overflow:hidden'>"
                    f"<div style='width:{r['Prob']}%;height:100%;background:{pc}'></div></div>"
                    f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:2px;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:6px'>"
                    f"<div>RVOL<br><span style='color:#ff7b00'>{r['RVOL_str']}</span></div>"
                    f"<div>TP<br><span style='color:#00ff88'>{r['TP']:,}</span></div>"
                    f"<div>SL<br><span style='color:#ff3d5a'>{r['SL']:,}</span></div>"
                    f"<div>ASING<br><span style='color:{fc}'>{fd}</span></div></div></div>",
                    unsafe_allow_html=True)

def empty_state(emoji, label, sub=""):
    st.markdown(
        f"<div style='text-align:center;padding:60px;color:#4a5568;font-family:Space Mono,monospace'>"
        f"<div style='font-size:36px;margin-bottom:12px'>{emoji}</div>"
        f"<div style='font-size:12px;letter-spacing:2px'>KLIK {label}</div>"
        f"<div style='font-size:10px;margin-top:8px;color:#2d3748'>{sub}</div></div>",
        unsafe_allow_html=True)

def cache_info_badge(stocks, tf):
    """Tampilkan info cache umur rata-rata → user tau data seberapa fresh."""
    ages = []
    for t in stocks[:5]:  # sample 5 saham
        a = cache_age(t, tf)
        if a is not None: ages.append(a)
    if not ages:
        return '<span class="cache-stale">📡 No cache</span>'
    avg = sum(ages) / len(ages)
    m, s = int(avg // 60), int(avg % 60)
    cls = "cache-fresh" if avg < 180 else "cache-stale"
    icon = "✅" if avg < 180 else "⚠️"
    return f'<span class="{cls}">{icon} Cache: {m}m {s}s lalu</span>'

# ════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════
now_jkt = datetime.now(jakarta_tz)
is_open = 9 <= now_jkt.hour < 16
ds_ok   = True  # yFinance — always available

hc = "#2dd4bf" if ds_ok else "#ffb700"
oc = "#00ff88" if is_open else "#ffb700"
ob = "0,255,136" if is_open else "255,183,0"

st.markdown(
    f"<div style='display:flex;align-items:center;padding:12px 0 10px;border-bottom:1px solid #1c2533;margin-bottom:14px'>"
    f"<div><div style='font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:#ff7b00'>"
    f"🎯 CYRUS FUND SCANNER <span style='font-size:11px;color:{hc}'>{'📊 yFinance'}</span></div>"
    f"<div style='font-size:10px;color:#4a5568;letter-spacing:2px'>FULL IDX {len(ALL_STOCKS)} SAHAM · TOP {DISPLAY_TOP} RESULTS · BANDAR·HAKA·BSJP·SWING</div></div>"
    f"<div style='margin-left:auto;font-family:Space Mono,monospace;font-size:10px;padding:4px 12px;"
    f"border-radius:20px;background:rgba({ob},.08);border:1px solid rgba({ob},.3);color:{oc}'>"
    f"{'🟢 OPEN' if is_open else '🟡 CLOSED'} {now_jkt.strftime('%H:%M:%S')} WIB</div></div>",
    unsafe_allow_html=True)

if st.session_state.last_scan:
    e  = now_jkt.timestamp() - st.session_state.last_scan
    r  = max(0, 480 - e); m = int(r // 60); s = int(r % 60)
    lt = datetime.fromtimestamp(st.session_state.last_scan, jakarta_tz).strftime("%H:%M:%S")
    elapsed_m = int(e // 60); elapsed_s = int(e % 60)
    st.caption(f"⏱️ Scan {elapsed_m}m {elapsed_s}s lalu · Refresh dalam: {m:02d}:{s:02d} · Mode: {st.session_state.scan_mode} · {lt} WIB")

# ════════════════════════════════════════
#  MARKET REGIME BANNER — Auto Threshold
# ════════════════════════════════════════
_regime, _ihsg_last, _ihsg_chg = get_ihsg_regime()
_threshold = get_auto_threshold(_regime)

_regime_color = {"GREEN":"#00ff88","RED":"#ff3d5a","YELLOW":"#ffb700","UNKNOWN":"#4a5568"}
_regime_bg    = {"GREEN":"#0a2010","RED":"#250a0d","YELLOW":"#201000","UNKNOWN":"#0d1117"}
_rc = _regime_color.get(_regime, "#4a5568")
_rb = _regime_bg.get(_regime, "#0d1117")
_chg_c = "#00ff88" if _ihsg_chg >= 0 else "#ff3d5a"

st.markdown(
    f"<div style='padding:8px 16px;border-radius:8px;background:{_rb};"
    f"border:1px solid {_rc}44;margin-bottom:10px;"
    f"display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px'>"
    f"<div style='font-family:Space Mono,monospace;font-size:11px;color:{_rc};font-weight:700'>"
    f"IHSG REGIME: {_regime} · {_threshold['label']}</div>"
    f"<div style='font-family:Space Mono,monospace;font-size:11px;color:#4a5568'>"
    f"IHSG <span style='color:#e6edf3'>{_ihsg_last:,.0f}</span> "
    f"<span style='color:{_chg_c}'>{_ihsg_chg:+.2f}%</span> · "
    f"Min RVOL <span style='color:#ff7b00'>{_threshold['min_rvol']}x</span> · "
    f"Min Score <span style='color:#ffb700'>{_threshold['min_score']}</span></div>"
    f"</div>",
    unsafe_allow_html=True)

# Simpan regime ke session state
if "regime" not in st.session_state:
    st.session_state.regime = _regime
st.session_state.regime = _regime
st.session_state.threshold = _threshold

# ════════════════════════════════════════
#  TELEGRAM HELPER
# ════════════════════════════════════════
def send_tele(results, mode):
    if not TOKEN or not CHAT_ID or not results: return False
    now_=datetime.now(jakarta_tz); sep="━"*24
    hdr=(f"🎯 *CYRUS FUND SCANNER*\n"
         f"📊 Mode: *{mode}* · {now_.strftime('%H:%M:%S')} WIB\n{sep}\n")
    body=""
    for r in results[:5]:
        sig=r.get("Sinyal","—")
        em="🔥" if any(k in sig for k in ["BANDAR","HAKA","SUPER"]) else "⚡"
        body+=(f"\n{em} *{r['T']}* `{sig}`\n"
               f"   `{r['Now']:,}` | Prob `{r['Prob']}%` | RVOL `{r['RVOL_str']}`\n"
               f"   TP `{r['TP']:,}` | SL `{r['SL']:,}` | +{r['Profit']:.1f}%\n"
               f"   _{r.get('Flags','')[:50]}_\n")
    footer=f"\n{sep}\nTop {DISPLAY_TOP} dari {len(ALL_STOCKS)} saham IDX\n⚠️ _Bukan saran investasi!_"
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
        return True
    except: return False

# ════════════════════════════════════════
#  TABS — DIRECT INLINE (no wrapper function!)
#  Key: setiap button unique key, scan langsung di tempat
# ════════════════════════════════════════
tab_mom, tab_int, tab_bsjp, tab_swing, tab_wl = st.tabs(
    ["🚀 Momentum","⚡ Intraday","🌙 BSJP","📈 Swing","👁️ Scanner Mandiri"])

# ── MOMENTUM ──
with tab_mom:
    _c1,_c2,_c3=st.columns([4,1,1])
    with _c1: _btn_m=st.button("🚀 SCAN MOMENTUM",type="primary",use_container_width=True,key="CF_mom_btn")
    with _c2: _force_m=st.toggle("🔄 Fresh",value=False,key="CF_mom_fresh")
    with _c3: _tele_m=st.toggle("📡 Tele",value=True,key="CF_mom_tele")
    st.caption(f"📊 {len(ALL_STOCKS)} saham IDX · 15M · Top {DISPLAY_TOP} rotating")
    if _btn_m:
        _pb=st.progress(0); _msg=st.empty()
        st.session_state.res_momentum=do_scan(ALL_STOCKS,"Momentum",_pb,_msg,force_fresh=_force_m)
        st.session_state.last_scan=now_jkt.timestamp(); st.session_state.scan_mode="Momentum"
        save_results("Momentum",st.session_state.res_momentum,st.session_state.last_scan)
        _pb.empty()
        if _tele_m and st.session_state.res_momentum:
            if send_tele(st.session_state.res_momentum,"Momentum"): st.toast("📡 Terkirim!",icon="✅")
    if st.session_state.res_momentum:
        show_met(st.session_state.res_momentum)
        show_tbl(st.session_state.res_momentum)
    else:
        empty_state("🚀","SCAN MOMENTUM",f"Full {len(ALL_STOCKS)} saham IDX · Top {DISPLAY_TOP} rotating")

# ── INTRADAY ──
with tab_int:
    _c1,_c2,_c3=st.columns([4,1,1])
    with _c1: _btn_i=st.button("⚡ SCAN INTRADAY",type="primary",use_container_width=True,key="CF_int_btn")
    with _c2: _force_i=st.toggle("🔄 Fresh",value=False,key="CF_int_fresh")
    with _c3: _tele_i=st.toggle("📡 Tele",value=True,key="CF_int_tele")
    st.caption(f"📊 {len(ALL_STOCKS)} saham IDX · 15M · RSI OS Bounce · Top {DISPLAY_TOP}")
    if _btn_i:
        _pb=st.progress(0); _msg=st.empty()
        st.session_state.res_intraday=do_scan(ALL_STOCKS,"Intraday",_pb,_msg,force_fresh=_force_i)
        st.session_state.last_scan=now_jkt.timestamp(); st.session_state.scan_mode="Intraday"
        save_results("Intraday",st.session_state.res_intraday,st.session_state.last_scan)
        _pb.empty()
        if _tele_i and st.session_state.res_intraday:
            if send_tele(st.session_state.res_intraday,"Intraday"): st.toast("📡 Terkirim!",icon="✅")
    if st.session_state.res_intraday:
        show_met(st.session_state.res_intraday)
        show_tbl(st.session_state.res_intraday)
    else:
        empty_state("⚡","SCAN INTRADAY",f"Full {len(ALL_STOCKS)} saham · Bottom fishing RSI OS")

# ── BSJP ──
with tab_bsjp:
    _jam_ok=now_jkt.hour>=14
    _bcol="#00ff88" if _jam_ok else "#ffb700"
    _btxt="🟢 JAM ENTRY BSJP! Beli 14:30–15:45 WIB." if _jam_ok else f"⏳ Tunggu jam 14:00 WIB — sekarang {now_jkt.strftime('%H:%M')} WIB"
    st.markdown(f"<div style='font-family:Space Mono,monospace;font-size:10px;padding:6px 12px;border-radius:6px;margin-bottom:6px;background:{'#0d2010' if _jam_ok else '#201000'};color:{_bcol};border:1px solid {_bcol}44'>{_btxt}</div>",unsafe_allow_html=True)
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:9px;color:#bf5fff;padding:6px 12px;background:#0d0a1a;border-radius:6px;margin-bottom:8px;border:1px solid #bf5fff33'>🌙 BSJP: EMA uptrend + Close dekat High + RVOL surge + RSI 45-70 (BUKAN OS) · Intraday: RSI/Stoch OS bounce</div>",unsafe_allow_html=True)
    _c1,_c2,_c3=st.columns([4,1,1])
    with _c1: _btn_b=st.button("🌙 SCAN BSJP",type="primary",use_container_width=True,key="CF_bsjp_btn")
    with _c2: _force_b=st.toggle("🔄 Fresh",value=False,key="CF_bsjp_fresh")
    with _c3: _tele_b=st.toggle("📡 Tele",value=True,key="CF_bsjp_tele")
    st.caption(f"📊 {len(ALL_STOCKS)} saham · 15M · Entry 14:30–15:45 · Logic berbeda dari Intraday!")
    if _btn_b:
        _pb=st.progress(0); _msg=st.empty()
        st.session_state.res_bsjp=do_scan(ALL_STOCKS,"BSJP",_pb,_msg,force_fresh=_force_b)
        st.session_state.last_scan=now_jkt.timestamp(); st.session_state.scan_mode="BSJP"
        save_results("BSJP",st.session_state.res_bsjp,st.session_state.last_scan)
        _pb.empty()
        if _tele_b and st.session_state.res_bsjp:
            if send_tele(st.session_state.res_bsjp,"BSJP"): st.toast("📡 Terkirim!",icon="✅")
    if st.session_state.res_bsjp:
        show_met(st.session_state.res_bsjp)
        show_tbl(st.session_state.res_bsjp)
    else:
        empty_state("🌙","SCAN BSJP",f"Full {len(ALL_STOCKS)} saham · CLOSE% tinggi = gap up potential!")

# ── SWING ──
with tab_swing:
    st.info("📈 Swing pakai data Daily (D1) — hold 3–10 hari · akurat, no whipsaw")
    _c1,_c2,_c3=st.columns([4,1,1])
    with _c1: _btn_s=st.button("📈 SCAN SWING",type="primary",use_container_width=True,key="CF_swing_btn")
    with _c2: _force_s=st.toggle("🔄 Fresh",value=False,key="CF_swing_fresh")
    with _c3: _tele_s=st.toggle("📡 Tele",value=True,key="CF_swing_tele")
    st.caption(f"📊 {len(ALL_STOCKS)} saham IDX · Daily D1 · Top {DISPLAY_TOP}")
    if _btn_s:
        _pb=st.progress(0); _msg=st.empty()
        st.session_state.res_swing=do_scan(ALL_STOCKS,"Swing",_pb,_msg,force_fresh=_force_s)
        st.session_state.last_scan=now_jkt.timestamp(); st.session_state.scan_mode="Swing"
        save_results("Swing",st.session_state.res_swing,st.session_state.last_scan)
        _pb.empty()
        if _tele_s and st.session_state.res_swing:
            if send_tele(st.session_state.res_swing,"Swing"): st.toast("📡 Terkirim!",icon="✅")
    if st.session_state.res_swing:
        show_met(st.session_state.res_swing)
        show_tbl(st.session_state.res_swing)
    else:
        empty_state("📈","SCAN SWING",f"Full {len(ALL_STOCKS)} saham · Data D1")

# ── SCANNER MANDIRI ──
with tab_wl:
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:10px;color:#4a5568;padding:8px 12px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;margin-bottom:10px'>Analisa ticker pilihan · yFinance 📊 · No API key needed</div>",unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1:
        wtxt=st.text_area("T",height=100,label_visibility="collapsed",placeholder="BBCA\nARCI, ASSA, GOTO",key="CF_wl_txt")
    with wc2:
        wmode=st.radio("Mode",["Intraday","BSJP","Swing","Momentum"],key="CF_wl_mode")
        wview=st.radio("V",["📋 Tabel","🃏 Cards"],key="CF_wl_view",label_visibility="collapsed")
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wforce=st.toggle("🔄 Fresh",value=False,key="CF_wl_fresh")
        wtele=st.toggle("📡 Tele",value=True,key="CF_wl_tele")
        btn_wl=st.button("🔍 Analisa",type="primary",use_container_width=True,key="CF_wl_btn")
        btn_tele_m=st.button("📡 Kirim Manual",use_container_width=True,key="CF_wl_telebtn")
    if btn_wl and wtxt.strip():
        raw=list(dict.fromkeys([t.strip().upper() for ln in wtxt.split("\n") for t in ln.split(",") if t.strip()]))
        if raw:
            _pb=st.progress(0); _msg=st.empty()
            st.session_state.wl_res=do_scan(raw,wmode,_pb,_msg,force_fresh=wforce)
            _pb.empty()
            if wtele and st.session_state.wl_res:
                if send_tele(st.session_state.wl_res,wmode): st.toast("📡 Terkirim!",icon="✅")
    if btn_tele_m and st.session_state.wl_res:
        if send_tele(st.session_state.wl_res,wmode): st.toast("📡 Terkirim!",icon="✅")
        else: st.error("Gagal — cek TOKEN/CHAT_ID di secrets.toml")
    if st.session_state.wl_res:
        show_met(st.session_state.wl_res)
        if "Tabel" in wview: show_tbl(st.session_state.wl_res)
        else: show_cards(st.session_state.wl_res)
    else:
        st.markdown("<div style='text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace'><div style='font-size:28px;margin-bottom:8px'>👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>",unsafe_allow_html=True)

# ════════════════════════════════════════
#  AUTO-REFRESH — PORT EXACT DARI THETA
#  Aturan: timer HANYA di-set kalau elapsed < 480 (belum waktunya)
#  Kalau elapsed >= 480 = waktunya scan ulang, bukan reload lagi
#  Ini yang fix kedap-kedip!
# ════════════════════════════════════════
import streamlit.components.v1 as _cf_comp

_has_results = any([st.session_state.res_momentum, st.session_state.res_intraday,
                    st.session_state.res_bsjp, st.session_state.res_swing])

if is_open and _has_results and st.session_state.last_scan:
    _el_ar  = int(now_jkt.timestamp() - st.session_state.last_scan)
    _rem_ar = 480 - _el_ar

    # KUNCI: hanya inject timer kalau BELUM waktunya refresh
    # Kalau elapsed >= 480 → jangan set timer → page tetap idle
    # sampai user klik scan atau next render
    if _el_ar < 480:
        _rem_ms = max(10000, _rem_ar * 1000)  # min 10 detik
        _cf_comp.html(
            f"""<script>
            if (window._cyrus_ar) clearTimeout(window._cyrus_ar);
            window._cyrus_ar = setTimeout(function() {{
                window.parent.location.reload();
            }}, {_rem_ms});
            </script>""",
            height=0)

_m_ar = int(max(0, 480 - (now_jkt.timestamp() - st.session_state.last_scan)) // 60) if st.session_state.last_scan else 8
_s_ar = int(max(0, 480 - (now_jkt.timestamp() - st.session_state.last_scan)) % 60)  if st.session_state.last_scan else 0

st.markdown(
    f"<div style='margin-top:24px;padding-top:12px;border-top:1px solid #1c2533;"
    f"font-family:Space Mono,monospace;font-size:9px;color:#4a5568;text-align:center'>"
    f"🎯 Cyrus Fund Scanner · Full IDX {len(ALL_STOCKS)} saham · Top {DISPLAY_TOP} rotating · "
    f"yFinance 📊 · Next refresh: {_m_ar:02d}:{_s_ar:02d}</div>",
    unsafe_allow_html=True)
