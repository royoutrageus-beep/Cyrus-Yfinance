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
try:
    TOKEN   = st.secrets.get("TELEGRAM_TOKEN", "")
    CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")
except:
    TOKEN = ""; CHAT_ID = ""

jakarta_tz  = pytz.timezone("Asia/Jakarta")
DISPLAY_TOP = 50

# ════════════════════════════════════════
#  DISK CACHE — thread-safe
# ════════════════════════════════════════
CACHE_DIR = Path("/tmp/cyrus_yf") if Path("/tmp").exists() else Path.home() / ".cyrus_yf"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL  = 300
_mem       = {}
_mem_lock  = threading.Lock()

def _ck(ticker, tf): return f"{ticker}_{tf}"

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

def cache_age(ticker, tf):
    key = _ck(ticker, tf)
    with _mem_lock:
        if key in _mem: return time.time() - _mem[key][0]
    fp = CACHE_DIR / f"{key}.pkl"
    try:
        if fp.exists():
            d = pickle.loads(fp.read_bytes())
            return time.time() - d["ts"]
    except: pass
    return None

# Results persistence
RESULTS_FILE = CACHE_DIR / "last_results.pkl"
RESULTS_TTL  = 600

def save_results(mode, results, ts):
    try: RESULTS_FILE.write_bytes(pickle.dumps({"mode":mode,"results":results,"ts":ts}))
    except: pass

def load_results():
    try:
        if RESULTS_FILE.exists():
            d = pickle.loads(RESULTS_FILE.read_bytes())
            if time.time() - d["ts"] < RESULTS_TTL: return d
    except: pass
    return None

# ════════════════════════════════════════
#  STOCK LIST — 312 saham aktif IDX
# ════════════════════════════════════════
_RAW = [
    "AALI","ACES","ACST","ADES","ADHI","ADMF","ADMG","ADMR","ADRO","AGII","AGRO","AGRS",
    "AKPI","AKRA","AKSI","ALDO","ALKA","ALMI","AMAG","AMAR","AMFG","AMIN","AMMS","AMOR",
    "AMRT","ANDI","ANJT","ANTM","APLN","ARCI","ARNA","ARTO","ASDM","ASGR","ASII","ASRI",
    "ASRM","ASSA","AUTO","AVIA","AWAN","AXIO","BACA","BBCA","BBHI","BBKP","BBLD","BBMD",
    "BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP","BCIC","BCIP","BDMN","BEST",
    "BFIN","BIRD","BISI","BJBR","BJTM","BLTZ","BLUE","BMBL","BMRI","BMTR","BNGA","BNII",
    "BNLI","BRAM","BRIS","BRNA","BRPT","BSDE","BSSR","BTON","BTPS","BUDI","BULL","BUMI",
    "BUKA","BYAN","CAMP","CASH","CASS","CBRE","CEKA","CINT","CITA","CITY","CLEO","CMRY",
    "COCO","CPIN","CPRO","CSAP","CSIS","CTBN","CTRA","CUAN","DART","DCII","DGNS","DIGI",
    "DILD","DLTA","DNET","DOID","DPNS","DSSA","DUTI","DVLA","EKAD","ELPI","ELSA","EMAS",
    "EMTK","EPMT","ERAA","ESSA","EXCL","FAST","FASW","FISH","GDST","GEMA","GEMS","GGRM",
    "GGRP","GIAA","GJTL","GOLD","GOOD","GOTO","GPRA","HEAL","HERO","HEXA","HITS","HMSP",
    "HOKI","HRTA","HRUM","ICBP","IMAS","IMPC","INAF","INAI","INCO","INDF","INET","INFO",
    "INPP","INTA","INTP","IPCC","IPCM","ISAT","ISSP","ITMG","JECC","JIHD","JKON","JPFA",
    "JRPT","JSMR","KAEF","KBLI","KBLM","KDSI","KEJU","KIJA","KING","KINO","KKGI","KLBF",
    "LPCK","LPGI","LPIN","LPKR","LPPF","LSIP","LTLS","LUCK","MAIN","MAPI","MARI","MARK",
    "MASA","MAYA","MBAP","MBMA","MBSS","MBTO","MDKA","MDLN","MEDC","MEGA","MIDI","MIKA",
    "MKPI","MLBI","MLIA","MLPT","MNCN","MYOR","MTDL","MTEL","MTLA","MYOH","NELY","NFCX",
    "NOBU","NRCA","PANI","PANR","PANS","PEHA","PGAS","PGEO","PGUN","PICO","PJAA","PLIN",
    "PNLF","POLU","PORT","POWR","PRDA","PRIM","PSSI","PTBA","PTRO","PWON","RAJA","RALS",
    "RICY","RIGS","RISE","RODA","ROTI","SAFE","SAME","SCCO","SCMA","SDRA","SGRO","SHIP",
    "SILO","SIMP","SKBM","SMAR","SMCB","SMDR","SMGR","SMMA","SMRA","SMSM","SOHO","SPMA",
    "SPTO","SRIL","SRTG","SSIA","SSMS","STAA","STTP","SUNU","SUPR","AMMN","TBIG","TBLA",
    "TCID","TCPI","TECH","TELE","TGKA","TINS","TKIM","TLKM","TMAS","TOBA","TOWR","TRGU",
    "TRIM","TRIS","TRST","TRUE","TRUK","TSPC","TUGU","UNIC","UNIT","UNTR","UNVR","VOKS",
    "WEGE","WEHA","WICO","WIFI","WIKA","WINE","WINS","WITA","WOOD","WSKT","WTON","ZINC",
]
ALL_STOCKS = list(dict.fromkeys(_RAW))
STOCKS_30  = ALL_STOCKS  # backward compat
STOCKS_60  = ALL_STOCKS

# ════════════════════════════════════════
#  YFINANCE FETCH — BATCH (pattern solid)
# ════════════════════════════════════════
def _fetch_one_yf(ticker, interval="15m", force_fresh=False):
    """
    Fetch 1 ticker via yFinance — simple, no batch complexity.
    Ini yang pasti work di semua versi yFinance.
    """
    tf_map = {"daily":"1d","15m":"15m","1d":"1d"}
    yf_tf  = tf_map.get(interval, "15m")
    period = "60d" if yf_tf=="1d" else "5d"

    if not force_fresh:
        cached = cache_get(ticker, interval)
        if cached is not None: return cached
    try:
        sym = f"{ticker}.JK"
        df  = yf.download(sym, period=period, interval=yf_tf,
                          progress=False, auto_adjust=True)
        if df is None or len(df) < 5: return None

        # Flatten MultiIndex kalau ada (yfinance kadang wrap single ticker juga)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        df.columns = [str(c).strip() for c in df.columns]
        # Normalize column names
        col_map = {c: c.title() for c in df.columns}
        df = df.rename(columns=col_map)

        needed = [c for c in ["Open","High","Low","Close","Volume"] if c in df.columns]
        if len(needed) < 4: return None

        df = df[needed].dropna()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert("Asia/Jakarta")
        else:
            df.index = df.index.tz_convert("Asia/Jakarta")

        if len(df) < 5: return None
        cache_set(ticker, interval, df)
        return df
    except:
        return None

def fetch_batch_yf(tickers, interval="15m", force_fresh=False):
    """
    Batch download yFinance — pattern yang solid.
    25 ticker per batch, parallel per batch.
    """
    BATCH  = 25
    yf_tf  = "1d" if interval in ["daily","1d","d"] else "15m"
    period = "60d" if yf_tf == "1d" else "5d"
    result = {}

    need = []
    for t in tickers:
        if not force_fresh:
            cached = cache_get(t, interval)
            if cached is not None:
                result[t] = cached; continue
        need.append(t)

    for i in range(0, len(need), BATCH):
        batch = need[i:i+BATCH]
        syms  = [t+".JK" for t in batch]
        try:
            raw = yf.download(
                " ".join(syms), period=period, interval=yf_tf,
                group_by="ticker", progress=False, threads=True, auto_adjust=True)
            if raw is None or len(raw) == 0: continue
            parsed = _parse_yf_batch(raw, syms)
            for t, df in parsed.items():
                cache_set(t, interval, df)
                result[t] = df
        except: pass
        time.sleep(0.1)  # kecil delay antar batch

    return result

def _fetch_raw(ticker, interval="15m", force_fresh=False):
    """Single ticker fetch — dipakai untuk do_scan internal."""
    r = fetch_batch_yf([ticker], interval, force_fresh)
    return r.get(ticker)

# ════════════════════════════════════════
#  MARKET REGIME — IHSG ^JKSE
# ════════════════════════════════════════
@st.cache_data(ttl=300)
def get_ihsg_regime():
    try:
        df = yf.download("^JKSE", period="60d", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < 20: return "UNKNOWN", 0.0, 0.0
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        c     = df["Close"].dropna()
        last  = float(c.iloc[-1])
        e20   = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        e55   = float(c.ewm(span=55, adjust=False).mean().iloc[-1])
        chg1  = (last - float(c.iloc[-2])) / max(float(c.iloc[-2]), 1) * 100
        chg5  = (last - float(c.iloc[-6])) / max(float(c.iloc[-6]), 1) * 100 if len(c) >= 7 else 0
        if last > e20 > e55 and chg5 > 0:    regime = "GREEN"
        elif last < e20 < e55 or chg5 < -3:  regime = "RED"
        else:                                  regime = "YELLOW"
        return regime, round(last, 0), round(chg1, 2)
    except: return "UNKNOWN", 0.0, 0.0

def get_auto_threshold(regime):
    return {
        "GREEN":   {"min_rvol":1.2, "min_score":15, "min_prob":55,
                    "label":"Semua sinyal valid"},
        "YELLOW":  {"min_rvol":1.5, "min_score":20, "min_prob":60,
                    "label":"RVOL ≥ 1.5x, selektif"},
        "RED":     {"min_rvol":2.0, "min_score":25, "min_prob":65,
                    "label":"Hanya BANDAR/SUPER"},
        "UNKNOWN": {"min_rvol":1.2, "min_score":15, "min_prob":55,
                    "label":"Data IHSG tidak tersedia"},
    }.get(regime, {"min_rvol":1.2,"min_score":15,"min_prob":55,"label":"—"})

# ════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════
def sf(v, d=0.):
    try:
        x = float(v); return d if (np.isnan(x) or np.isinf(x)) else x
    except: return d

def add_indicators(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
    df = df.copy(); c = df["Close"]
    df["E9"]   = c.ewm(span=9,   adjust=False).mean()
    df["E21"]  = c.ewm(span=21,  adjust=False).mean()
    df["E50"]  = c.ewm(span=50,  adjust=False).mean()
    df["E200"] = c.ewm(span=200, adjust=False).mean()
    d = c.diff()
    g = d.clip(lower=0).ewm(span=14, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=14, adjust=False).mean()
    rsi_raw = (100 - 100 / (1 + g / l.replace(0, np.nan))).fillna(50)
    df["RSI"]     = rsi_raw
    df["RSI_EMA"] = rsi_raw.ewm(span=14, adjust=False).mean()
    d5 = c.diff()
    g5 = d5.clip(lower=0).ewm(span=5, adjust=False).mean()
    l5 = (-d5.clip(upper=0)).ewm(span=5, adjust=False).mean()
    df["RSI5"] = (100 - 100 / (1 + g5 / l5.replace(0, np.nan))).fillna(50)
    ema12 = c.ewm(span=12, adjust=False).mean(); ema26 = c.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26; signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df["MACD"] = macd_line; df["MACD_Sig"] = signal_line
    df["MACD_H"] = (macd_line - signal_line).fillna(0)
    lo10 = df["Low"].rolling(10).min(); hi10 = df["High"].rolling(10).max()
    raw_k = (100 * (c - lo10) / (hi10 - lo10).replace(0, np.nan)).fillna(50)
    stoch_k = raw_k.ewm(span=5, adjust=False).mean()
    df["STOCH_K"] = stoch_k; df["STOCH_D"] = stoch_k.ewm(span=5, adjust=False).mean()
    df["RVOL"] = (df["Volume"] / df["Volume"].rolling(20).mean().replace(0, np.nan)).fillna(1)
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-c.shift()).abs(), (df["Low"]-c.shift()).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    bt = df[["Close","Open"]].max(axis=1); bb = df[["Close","Open"]].min(axis=1)
    hl = (df["High"]-df["Low"]).replace(0, np.nan)
    df["LW"]   = ((bb-df["Low"])/hl*100).fillna(0)
    df["UW"]   = ((df["High"]-bt)/hl*100).fillna(0)
    df["Body"] = (bt-bb)/hl*100
    try:
        tp = (df["High"]+df["Low"]+df["Close"])/3
        df["VWAP"] = (tp*df["Volume"]).cumsum()/df["Volume"].cumsum()
    except: df["VWAP"] = df["Close"]
    df["PctChange"] = c.pct_change()*100
    return df

# ════════════════════════════════════════
#  SCORING — ORIGINAL WORKING VERSION
# ════════════════════════════════════════
def get_sinyal(df, mode="Intraday"):
    if len(df) < 3: return "WAIT ❌", 0, [], False
    r = df.iloc[-1]; p = df.iloc[-2]; p2 = df.iloc[-3] if len(df) >= 3 else p
    cl=sf(r.get("Close",0)); e9=sf(r.get("E9")); e21=sf(r.get("E21")); e50=sf(r.get("E50"))
    rsi_ema=sf(r.get("RSI_EMA",50)); rsi_ema_p=sf(p.get("RSI_EMA",50))
    sk=sf(r.get("STOCH_K",50)); sd=sf(r.get("STOCH_D",50))
    sk_p=sf(p.get("STOCH_K",50)); sd_p=sf(p.get("STOCH_D",50))
    mh=sf(r.get("MACD_H",0)); mh_p=sf(p.get("MACD_H",0))
    macd=sf(r.get("MACD",0)); sig=sf(r.get("MACD_Sig",0))
    macd_p=sf(p.get("MACD",0)); sig_p=sf(p.get("MACD_Sig",0))
    rv=sf(r.get("RVOL",1)); lw=sf(r.get("LW",0)); uw=sf(r.get("UW",0))
    vwap=sf(r.get("VWAP",cl))
    score=0; flags=[]
    ema_bull=e9>e21>e50; ema_gc=e9>e21; ema_bear=e9<e21<e50
    p_e9=sf(p.get("E9")); p_e21=sf(p.get("E21"))
    gc_now=(e9>e21)and(p_e9<=p_e21)
    if ema_bull:   score+=15; flags.append("EMA▲")
    elif ema_gc:   score+=8;  flags.append("EMA GC")
    elif ema_bear: score-=12
    stoch_os=sk<20; stoch_ob=sk>80
    stoch_cu=sk>sd and sk_p<=sd_p
    if stoch_os:
        score+=12; flags.append(f"STOCH OS {sk:.0f}")
        if stoch_cu: score+=8; flags.append("STOCH ↑")
    elif stoch_ob: score-=10
    elif stoch_cu and sk<60: score+=6; flags.append("STOCH ↑")
    rsi_os=rsi_ema<40; rsi_os_str=rsi_ema<30; rsi_ob=rsi_ema>65
    rsi_cu=rsi_ema>rsi_ema_p and rsi_ema_p<40
    if rsi_os_str:
        score+=12; flags.append(f"RSI {rsi_ema:.0f} OS")
        if rsi_cu: score+=8; flags.append("RSI ↑")
    elif rsi_os:
        score+=7; flags.append(f"RSI {rsi_ema:.0f}")
        if rsi_cu: score+=5
    elif 45<rsi_ema<65: score+=5
    elif rsi_ob: score-=8
    macd_cu=macd>sig and macd_p<=sig_p; macd_cd=macd<sig and macd_p>=sig_p
    macd_exp=mh>0 and mh>mh_p; macd_wk=mh<0 and mh<mh_p
    if macd_cu:   score+=10; flags.append("MACD ↑")
    elif macd_exp:score+=7;  flags.append("MACD Exp")
    elif mh>0:    score+=3
    elif macd_cd: score-=10
    elif macd_wk: score-=5
    if rv>3:     score+=15; flags.append(f"RVOL {rv:.1f}x 🔥")
    elif rv>2:   score+=10; flags.append(f"RVOL {rv:.1f}x")
    elif rv>1.5: score+=5;  flags.append(f"RVOL {rv:.1f}x")
    elif rv<0.5: score-=5
    if lw>60:   score+=10; flags.append(f"LWick {lw:.0f}%")
    elif lw>40: score+=6;  flags.append(f"LWick {lw:.0f}%")
    elif lw>25: score+=3
    uw_sell=uw>50 and sf(r.get("Body",50))<30
    if uw_sell: flags.append(f"UWick {uw:.0f}%")
    if cl>vwap:   score+=5; flags.append("VWAP▲")
    elif cl<vwap: score-=3
    entry_kuat=((stoch_os)and(rsi_os or rsi_cu)and(macd_cu or macd_exp)and rv>=1.2)
    entry_mod=(sum([stoch_os or stoch_cu,rsi_os or rsi_cu,macd_exp or macd_cu])>=2 and rv>=1.0)
    is_haka=(ema_bull and rv>1.5 and macd_exp and rsi_ema>50 and sk>sd and cl>vwap)
    is_super=(entry_kuat and rv>2 and score>=35)
    is_rebound=(entry_kuat and(stoch_os or rsi_os_str))
    is_sell=(uw_sell and(stoch_ob or rsi_ob)and rv>1.0)
    if is_sell:    return "JUAL ⬇️",    score, flags, gc_now
    if is_haka:    return "HAKA 🔨",    score, flags, gc_now
    if is_super:   return "SUPER 🔥",   score, flags, gc_now
    if is_rebound: return "REBOUND 🏀", score, flags, gc_now
    if entry_mod and score>=20: return "AKUM 📦", score, flags, gc_now
    if score>=15: return "ON TRACK ✅", score, flags, gc_now
    return "WAIT ❌", score, flags, gc_now

def get_aksi(score, gc_now, sinyal):
    if sinyal in ["HAKA 🔨","SUPER 🔥"] and score>=35: return "AT ENTRY 🎯"
    elif sinyal=="REBOUND 🏀": return "WATCH REB 🏀"
    elif gc_now:    return "GC NOW ⚡"
    elif score>=25: return "AT ENTRY 🎯"
    elif score>=15: return "WAIT GC ⏳"
    else:           return "WAIT ❌"

def get_rsi_sig(rsi):
    if rsi>=60:  return "UP","#00ff88"
    elif rsi<35: return "DEAD","#ff3d5a"
    elif rsi<45: return "DOWN","#ff7b00"
    else:        return "NEUTRAL","#4a5568"

def get_trend(df):
    if df is None or len(df)<2: return "NETRAL","#4a5568"
    r=df.iloc[-1]; e9=sf(r.get("E9")); e21=sf(r.get("E21")); e50=sf(r.get("E50",0))
    if e9>e21>e50: return "BULL 🔥","#00ff88"
    if e9<e21<e50: return "BEAR ❄️","#ff3d5a"
    return "NETRAL","#4a5568"

def get_fase(df):
    if df is None or len(df)<5: return "AKUM","#00e5ff"
    vn=df["Volume"].iloc[-3:].mean(); va=df["Volume"].iloc[-20:-3].mean() if len(df)>=20 else vn
    cn=sf(df["Close"].iloc[-1]); cp=sf(df["Close"].iloc[-5])
    vr=vn/max(va,1); pr=cn/max(cp,1)
    if vr>1.5 and pr>1.02: return "BIG AKUM 🔥","#ff7b00"
    if vr>1.2 and pr>1.0:  return "AKUM 📦","#00e5ff"
    if vr>1.3 and pr<0.99: return "DIST ⚠️","#ff3d5a"
    return "NETRAL","#4a5568"

# ════════════════════════════════════════
#  BUILD RESULT — ORIGINAL WORKING
# ════════════════════════════════════════
def build_result(ticker, df_main, df_daily, mode):
    try:
        df   = add_indicators(df_main)
        df_d = add_indicators(df_daily) if df_daily is not None and len(df_daily)>=10 else None
        sinyal,score,flags,gc_now = get_sinyal(df, mode)
        aksi  = get_aksi(score, gc_now, sinyal)
        trend, trend_col = get_trend(df_d if df_d is not None else df)
        fase,  fase_col  = get_fase(df_d  if df_d is not None else df)
        r=df.iloc[-1]; r1=df.iloc[-2] if len(df)>1 else r
        cl=sf(r.get("Close",0))
        if cl==0: return None
        vol=sf(r.get("Volume",0)); atr=sf(r.get("ATR",cl*0.02))
        rv=sf(r.get("RVOL",1)); rsi=sf(r.get("RSI",50)); rsi5=sf(r.get("RSI5",50))
        e9=sf(r.get("E9",cl)); lw=sf(r.get("LW",0))
        # GAIN + VAL dari daily D1 (akurat)
        if df_daily is not None and len(df_daily)>=2:
            try:
                c1=float(df_daily.iloc[-1]["Close"]); c0=float(df_daily.iloc[-2]["Close"])
                gain=(c1-c0)/max(c0,1)*100
                vb=c1*float(df_daily.iloc[-1]["Volume"])/1e9
            except:
                gain=(cl-sf(r1.get("Close",cl)))/max(sf(r1.get("Close",cl)),1)*100
                vb=cl*vol/1e9
        else:
            gain=(cl-sf(r1.get("Close",cl)))/max(sf(r1.get("Close",cl)),1)*100
            vb=cl*vol/1e9
        if mode=="BSJP": tp=cl+3.0*atr; sl=cl-1.5*atr
        else:            tp=cl+4.0*atr; sl=cl-2.0*atr
        profit=(tp-cl)/cl*100
        if "WAIT" in aksi: entry_str="WAIT GC"; entry_val=0
        else: entry_val=int(min(cl,e9*1.002)); entry_str=str(entry_val)
        val_str=f"{vb:.1f}B" if vb>=1 else f"{round(vb*1000,0):.0f}M"
        rsi_sig,rsi_col=get_rsi_sig(rsi)
        rvol_str=f"{rv*100:.0f}%" if rv<10 else f"{rv:.1f}x"
        prob=max(5,min(95,score+50))
        return {
            "T":ticker,"Prob":prob,"Gain":round(gain,1),"Wick":round(lw,1),
            "Aksi":aksi,"Sinyal":sinyal,"RVOL_raw":round(rv,2),"RVOL_str":rvol_str,
            "Entry_str":entry_str,"Entry_val":entry_val,
            "Now":int(cl),"TP":int(tp),"SL":int(sl),"Profit":round(profit,1),
            "RSI_Sig":rsi_sig,"RSI_Col":rsi_col,"RSI5":round(rsi5,1),
            "Val":val_str,"Fase":fase,"Fase_col":fase_col,"Trend":trend,"Trend_col":trend_col,
            "Score":score,"GC":gc_now,"Flags":" · ".join(flags[:3]),"ATR":round(atr,0),
            "FDir":"—","FC":"#4a5568",
        }
    except: return None

# ════════════════════════════════════════
#  SCAN ENGINE — yFinance batch
# ════════════════════════════════════════
def do_scan(stocks, mode, pb, status_ph, preview_ph=None, force_fresh=False, skip_filter=False):
    """
    Per-ticker yFinance + ThreadPoolExecutor 15 threads.
    Simple & reliable — no batch complexity.
    """
    n  = len(stocks)
    tf = "daily" if mode=="Swing" else "15m"
    raw_main = {}; raw_ctx = {}

    # Cache check dulu
    need = []
    for t in stocks:
        if not force_fresh:
            cached = cache_get(t, tf)
            if cached is not None: raw_main[t] = cached; continue
        need.append(t)

    status_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#ff7b00">'
        f'⬇️ {len(raw_main)} cache · {len(need)} fetch yFinance [{tf}]...</div>',
        unsafe_allow_html=True)
    pb.progress(0.05)

    # Parallel fetch 15 threads
    done = [0]
    def _fm(t):
        return t, _fetch_one_yf(t, tf, True)

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(_fm, t): t for t in need}
        for f in as_completed(futs):
            done[0] += 1
            pb.progress(0.05 + (done[0]/max(len(need),1))*0.38)
            if done[0] % 15 == 0:
                status_ph.markdown(
                    f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#ff7b00">'
                    f'⬇️ {done[0]}/{len(need)} · OK: {len(raw_main)+done[0]}...</div>',
                    unsafe_allow_html=True)
            try:
                t, df = f.result(timeout=20)
                if df is not None and len(df) >= 20:
                    raw_main[t] = df
            except: pass

    # Daily context untuk gain & val
    need_d = [t for t in raw_main if force_fresh or cache_get(t,"daily") is None]
    for t in raw_main:
        if t not in need_d:
            c = cache_get(t, "daily")
            if c is not None: raw_ctx[t] = c

    status_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#00e5ff">'
        f'📅 Daily context {len(need_d)} saham...</div>', unsafe_allow_html=True)
    pb.progress(0.50)

    done2 = [0]
    def _fd(t):
        return t, _fetch_one_yf(t, "daily", True)

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(_fd, t): t for t in need_d}
        for f in as_completed(futs):
            done2[0] += 1
            pb.progress(0.50 + (done2[0]/max(len(need_d),1))*0.30)
            try:
                t, df = f.result(timeout=20)
                if df is not None and len(df) >= 2:
                    raw_ctx[t] = df
            except: pass

    # Process
    pb.progress(0.85)
    status_ph.markdown(
        f'<div style="font-family:Space Mono,monospace;font-size:11px;color:#00ff88">'
        f'⚙️ Processing {len(raw_main)}/{n}...</div>', unsafe_allow_html=True)

    results = []
    for t in stocks:
        df_main = raw_main.get(t)
        df_ctx  = raw_ctx.get(t)
        if df_main is None or len(df_main) < 20: continue
        r = build_result(t, df_main, df_ctx, mode)
        if r: results.append(r)

    pb.progress(1.0); status_ph.empty()
    results.sort(key=lambda x: x["Prob"], reverse=True)

    if skip_filter: return results[:DISPLAY_TOP]

    thr      = st.session_state.get("threshold", get_auto_threshold("UNKNOWN"))
    min_prob = thr.get("min_prob", 55)
    min_score= thr.get("min_score", 15)
    min_rvol = thr.get("min_rvol", 1.2)
    regime   = st.session_state.get("regime", "UNKNOWN")
    filtered = [r for r in results if
                any(k in r.get("Sinyal","") for k in ["HAKA","SUPER","REBOUND"]) or
                (r["Prob"]>=min_prob and r["Score"]>=min_score and r["RVOL_raw"]>=min_rvol)]
    if regime=="RED" and len(filtered)<5: filtered=results[:5]
    return filtered[:DISPLAY_TOP]


# ════════════════════════════════════════
#  PAGE CONFIG + CSS
# ════════════════════════════════════════
st.set_page_config(layout="wide",page_title="Cyrus Fund Scanner",page_icon="🎯",initial_sidebar_state="collapsed")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@700&display=swap');
html,body,[data-testid="stAppViewContainer"]{background:#060a0e!important;color:#c9d1d9!important;font-family:'Syne',sans-serif;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stSidebar"]{display:none!important;}
button[data-testid="baseButton-primary"]{background:#ff7b00!important;color:#000!important;font-family:'Space Mono',monospace!important;font-weight:700!important;}
.mc{background:#0d1117;border:1px solid #1c2533;border-radius:8px;padding:8px 14px;flex:1;min-width:80px;border-top:3px solid #4a5568;}
.ml{font-size:9px;color:#4a5568;letter-spacing:1px;text-transform:uppercase}
.mv{font-family:'Space Mono',monospace;font-size:20px;font-weight:700;color:#e6edf3}
</style>""",unsafe_allow_html=True)

# ════════════════════════════════════════
#  SESSION STATE
# ════════════════════════════════════════
for k,v in {"res_momentum":[],"res_intraday":[],"res_bsjp":[],"res_swing":[],"wl_res":[],
            "last_scan":None,"scan_mode":"","regime":"UNKNOWN",
            "threshold":get_auto_threshold("UNKNOWN")}.items():
    if k not in st.session_state: st.session_state[k]=v

# Auto-restore dari disk
if not any([st.session_state.res_momentum,st.session_state.res_intraday,
            st.session_state.res_bsjp,st.session_state.res_swing]):
    _saved=load_results()
    if _saved:
        mk=f"res_{_saved['mode'].lower()}"
        if mk in st.session_state: st.session_state[mk]=_saved["results"]
        st.session_state.last_scan=_saved["ts"]; st.session_state.scan_mode=_saved["mode"]

# ════════════════════════════════════════
#  UI HELPERS — ORIGINAL WORKING
# ════════════════════════════════════════
def _ab(a):
    a=str(a)
    if "AT ENTRY" in a:  c,bg="#00ff88","#1a472a"
    elif "GC NOW" in a:  c,bg="#00e5ff","#0d2233"
    elif "WATCH" in a:   c,bg="#ffb700","#2a2000"
    else:                c,bg="#ff3d5a","#2a0d0d"
    return f'<span style="background:{bg};color:{c};padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;font-family:Space Mono,monospace">{a}</span>'

def _sb(s):
    s=str(s)
    M={"HAKA":("#00ff88","#0a2010"),"SUPER":("#bf5fff","#150a25"),
       "REBOUND":("#ffb700","#251800"),"JUAL":("#ff3d5a","#250a0d"),
       "AKUM":("#00e5ff","#0a1515"),"ON TRACK":("#00ff88","#0a1a0a")}
    for k,(c,bg) in M.items():
        if k in s: return f'<span style="background:{bg};color:{c};padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700;border:1px solid {c}44">{s}</span>'
    return f'<span style="background:#111;color:#4a5568;padding:2px 10px;border-radius:4px;font-size:9px;font-weight:700">{s}</span>'

def show_met(res):
    if not res: return
    try:
        bd=sum(1 for x in res if "HAKA" in x.get("Sinyal",""))
        sp=sum(1 for x in res if "SUPER" in x.get("Sinyal",""))
        rb=sum(1 for x in res if "REBOUND" in x.get("Sinyal",""))
        beli=sum(1 for x in res if "AT ENTRY" in x.get("Aksi",""))
        ap=round(sum(x["Prob"] for x in res)/len(res))
        pc="#00ff88" if ap>=65 else "#ffb700" if ap>=55 else "#ff3d5a"
        top=res[0]["T"]
    except: return
    html='<div style="display:flex;gap:8px;margin:10px 0;flex-wrap:wrap">'
    for lbl,val,col in [("HAKA 🔨",bd,"#00ff88"),("SUPER 🔥",sp,"#bf5fff"),
                        ("REBOUND",rb,"#ffb700"),("AT ENTRY",beli,"#00ff88"),
                        ("AVG PROB",str(ap)+"%",pc),("TOP PICK",top,"#ff7b00")]:
        fs="16px" if lbl=="TOP PICK" else "20px"
        html+=f'<div class="mc" style="border-top-color:{col}"><div class="ml">{lbl}</div><div class="mv" style="color:{col};font-size:{fs}">{val}</div></div>'
    html+='</div>'
    st.markdown(html,unsafe_allow_html=True)

TH=['EMITEN','GAIN','WICK','AKSI','SINYAL','RVOL','ENTRY','NOW','TP','SL','PROFIT','RSI','RSI5M','VAL','FASE','TREND']

def show_tbl(res):
    if not res: return
    rows=""
    for r in res:
        try:
            gc="#00ff88" if r["Gain"]>0 else "#ff3d5a"
            wc="#00ff88" if r["Wick"]>30 else "#4a5568"
            rc=r["RSI_Col"]
            rows+="<tr style='font-family:Space Mono,monospace;font-size:10px'>"
            rows+=f"<td style='padding:5px 8px;font-weight:700;color:#e6edf3;border-bottom:1px solid #1c2533;white-space:nowrap'>{r['T']}</td>"
            rows+=f"<td style='padding:5px 6px;color:{gc};font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r['Gain']:+.1f}%</td>"
            rows+=f"<td style='padding:5px 6px;color:{wc};border-bottom:1px solid #1c2533;text-align:center'>{int(r['Wick'])}%</td>"
            rows+=f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'>{_ab(r['Aksi'])}</td>"
            rows+=f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'>{_sb(r['Sinyal'])}</td>"
            rows+=f"<td style='padding:5px 6px;color:#ff7b00;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r['RVOL_str']}</td>"
            rows+=f"<td style='padding:5px 6px;color:#4a5568;border-bottom:1px solid #1c2533;text-align:center'>{r['Entry_str']}</td>"
            rows+=f"<td style='padding:5px 6px;color:#e6edf3;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r['Now']:,}</td>"
            rows+=f"<td style='padding:5px 6px;background:#0d2b0d;color:#00ff88;font-weight:700;border-bottom:1px solid #1c2533;text-align:center'>{r['TP']:,}</td>"
            rows+=f"<td style='padding:5px 6px;background:#2b0d0d;color:#ff3d5a;border-bottom:1px solid #1c2533;text-align:center'>{r['SL']:,}</td>"
            rows+=f"<td style='padding:5px 6px;color:#00ff88;border-bottom:1px solid #1c2533;text-align:center'>{r['Profit']:.1f}%</td>"
            rows+=f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{rc};font-weight:700'>{r['RSI_Sig']}</span></td>"
            rows+=f"<td style='padding:5px 6px;color:{rc};border-bottom:1px solid #1c2533;text-align:center'>{r['RSI5']:.0f}</td>"
            rows+=f"<td style='padding:5px 6px;color:#4a5568;font-size:9px;border-bottom:1px solid #1c2533;text-align:center'>{r['Val']}</td>"
            rows+=f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{r.get('Fase_col','#4a5568')};font-size:10px'>{r.get('Fase','')}</span></td>"
            rows+=f"<td style='padding:5px 6px;border-bottom:1px solid #1c2533;text-align:center'><span style='color:{r.get('Trend_col','#4a5568')};font-weight:700;font-size:10px'>{r.get('Trend','')}</span></td>"
            rows+="</tr>"
        except: continue
    hdrs="".join(f"<th style='padding:7px 6px;color:#4a5568;font-family:Space Mono,monospace;font-size:9px;letter-spacing:1px;border-bottom:2px solid #1c2533'>{h}</th>" for h in TH)
    st.markdown(
        f"<div style='overflow-x:auto;border-radius:8px;border:1px solid #1c2533;max-height:72vh;overflow-y:auto'>"
        f"<table style='width:100%;border-collapse:collapse'>"
        f"<thead><tr style='background:#080c10;position:sticky;top:0;z-index:10'>{hdrs}</tr></thead>"
        f"<tbody style='background:#0d1117'>{rows}</tbody></table>"
        f"<div style='padding:5px 12px;background:#080c10;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;border-top:1px solid #1c2533'>"
        f"Top {DISPLAY_TOP} · {len(ALL_STOCKS)} saham IDX · yFinance 📊</div></div>",
        unsafe_allow_html=True)

def show_cards(res):
    for idx in range(0,min(9,len(res)),3):
        cols=st.columns(3)
        for ci,r in enumerate(res[idx:idx+3]):
            pc="#00ff88" if r["Prob"]>=75 else "#ffb700" if r["Prob"]>=60 else "#ff7b00"
            gc="#00ff88" if r["Gain"]>0 else "#ff3d5a"
            with cols[ci]:
                st.markdown(
                    f"<div style='background:#0d1117;border:1px solid #1c2533;border-radius:10px;padding:12px;margin-bottom:8px'>"
                    f"<div style='display:flex;justify-content:space-between'>"
                    f"<div><div style='font-family:Space Mono,monospace;font-size:16px;font-weight:700;color:#e6edf3'>{r['T']}</div>"
                    f"<div style='font-size:10px;color:{gc}'>{r['Now']:,} ({r['Gain']:+.1f}%)</div></div>"
                    f"<div style='text-align:right'><div style='font-family:Space Mono,monospace;font-size:22px;font-weight:700;color:{pc}'>{r['Prob']}%</div>"
                    f"<div style='font-size:9px;color:#4a5568'>PROB</div></div></div>"
                    f"<div style='margin:6px 0'>{_sb(r['Sinyal'])} {_ab(r['Aksi'])}</div>"
                    f"<div style='height:3px;background:#1c2533;border-radius:2px;overflow:hidden'>"
                    f"<div style='width:{r['Prob']}%;height:100%;background:{pc}'></div></div>"
                    f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:2px;font-family:Space Mono,monospace;font-size:9px;color:#4a5568;margin-top:6px'>"
                    f"<div>RVOL<br><span style='color:#ff7b00'>{r['RVOL_str']}</span></div>"
                    f"<div>TP<br><span style='color:#00ff88'>{r['TP']:,}</span></div>"
                    f"<div>SL<br><span style='color:#ff3d5a'>{r['SL']:,}</span></div></div></div>",
                    unsafe_allow_html=True)

def empty_state(emoji,label,sub=""):
    st.markdown(
        f"<div style='text-align:center;padding:60px;color:#4a5568;font-family:Space Mono,monospace'>"
        f"<div style='font-size:36px;margin-bottom:12px'>{emoji}</div>"
        f"<div style='font-size:12px;letter-spacing:2px'>KLIK {label}</div>"
        f"<div style='font-size:10px;margin-top:8px;color:#2d3748'>{sub}</div></div>",
        unsafe_allow_html=True)

def cache_info_badge(stocks,tf):
    ages=[a for t in stocks[:5] if (a:=cache_age(t,tf)) is not None]
    if not ages: return '<span style="font-size:9px;color:#4a5568">📡 No cache</span>'
    avg=sum(ages)/len(ages); m,s=int(avg//60),int(avg%60)
    c="color:#00ff88" if avg<180 else "color:#ff7b00"
    return f'<span style="font-size:9px;{c}">{"✅" if avg<180 else "⚠️"} Cache: {m}m {s}s</span>'

# ════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════
now_jkt=datetime.now(jakarta_tz); is_open=9<=now_jkt.hour<16
oc="#00ff88" if is_open else "#ffb700"; ob="0,255,136" if is_open else "255,183,0"
st.markdown(
    f"<div style='display:flex;align-items:center;padding:12px 0 10px;border-bottom:1px solid #1c2533;margin-bottom:10px'>"
    f"<div><div style='font-family:Space Mono,monospace;font-size:20px;font-weight:700;color:#ff7b00'>"
    f"🎯 CYRUS FUND SCANNER <span style='font-size:11px;color:#2dd4bf'>📊 yFinance</span></div>"
    f"<div style='font-size:10px;color:#4a5568;letter-spacing:2px'>FULL IDX {len(ALL_STOCKS)} SAHAM · TOP {DISPLAY_TOP} RESULTS · HAKA·SUPER·REBOUND</div></div>"
    f"<div style='margin-left:auto;font-family:Space Mono,monospace;font-size:10px;padding:4px 12px;"
    f"border-radius:20px;background:rgba({ob},.08);border:1px solid rgba({ob},.3);color:{oc}'>"
    f"{'🟢 OPEN' if is_open else '🟡 CLOSED'} {now_jkt.strftime('%H:%M:%S')} WIB</div></div>",
    unsafe_allow_html=True)

if st.session_state.last_scan:
    e=now_jkt.timestamp()-st.session_state.last_scan; rr=max(0,480-e)
    m=int(rr//60); s=int(rr%60); lt=datetime.fromtimestamp(st.session_state.last_scan,jakarta_tz).strftime("%H:%M:%S")
    st.caption(f"⏱️ Scan {int(e//60)}m {int(e%60)}s lalu · Refresh dalam: {m:02d}:{s:02d} · Mode: {st.session_state.scan_mode} · {lt} WIB")

# ════════════════════════════════════════
#  MARKET REGIME + SCANNER SETTINGS
# ════════════════════════════════════════
_regime,_ihsg,_chg=get_ihsg_regime()
_auto_thr=get_auto_threshold(_regime)
_rc={"GREEN":"#00ff88","RED":"#ff3d5a","YELLOW":"#ffb700","UNKNOWN":"#4a5568"}.get(_regime,"#4a5568")
_rb={"GREEN":"#0a2010","RED":"#250a0d","YELLOW":"#201000","UNKNOWN":"#0d1117"}.get(_regime,"#0d1117")
_cc="#00ff88" if _chg>=0 else "#ff3d5a"
st.markdown(
    f"<div style='padding:7px 14px;border-radius:7px;background:{_rb};border:1px solid {_rc}33;"
    f"display:flex;align-items:center;justify-content:space-between;margin-bottom:6px'>"
    f"<div style='font-family:Space Mono,monospace;font-size:11px;color:{_rc};font-weight:700'>"
    f"● MARKET {_regime} — {_auto_thr['label']}</div>"
    f"<div style='font-family:Space Mono,monospace;font-size:10px;color:#4a5568'>"
    f"IHSG <span style='color:#e6edf3'>{_ihsg:,.0f}</span> <span style='color:{_cc}'>{_chg:+.2f}%</span></div>"
    f"</div>",unsafe_allow_html=True)

with st.expander("⚙️ Scanner Settings", expanded=False):
    _sc1,_sc2,_sc3=st.columns(3)
    with _sc1:
        st.markdown('<div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px;margin-bottom:4px">MARKET REGIME</div>',unsafe_allow_html=True)
        _auto_reg=st.toggle("🤖 Auto-Detect Regime",value=True,key="CF_auto_reg")
        if _auto_reg:
            _active_regime=_regime
            st.markdown(f'<div style="font-family:Space Mono,monospace;font-size:10px;padding:5px 9px;background:rgba(0,0,0,.3);border-radius:4px;color:{_rc}">Auto: {_regime} · IHSG {_ihsg:,.0f}</div>',unsafe_allow_html=True)
        else:
            _active_regime=st.radio("Regime",["GREEN","YELLOW","RED"],label_visibility="collapsed",key="CF_man_reg",horizontal=True)
    with _sc2:
        st.markdown('<div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px;margin-bottom:4px">FILTER THRESHOLD</div>',unsafe_allow_html=True)
        _auto_t=st.toggle("🤖 Auto-Threshold",value=True,key="CF_auto_thr")
        _act_thr=get_auto_threshold(_active_regime)
        if _auto_t:
            _min_rvol=_act_thr["min_rvol"]; _min_score=_act_thr["min_score"]; _min_prob=_act_thr["min_prob"]
            st.caption(f"Auto: RVOL≥{_min_rvol}x · Score≥{_min_score} · Prob≥{_min_prob}%")
        else:
            _min_rvol=st.slider("Min RVOL",1.0,5.0,_act_thr["min_rvol"],0.1,key="CF_rvol")
            _min_score=st.slider("Min Score",0,30,_act_thr["min_score"],1,key="CF_score")
            _min_prob=st.slider("Min Prob%",40,85,_act_thr["min_prob"],5,key="CF_prob")
        _min_turn=st.number_input("Min Turnover (M Rp)",value=200,step=100,key="CF_turn")*1_000_000
    with _sc3:
        st.markdown('<div style="font-family:Space Mono,monospace;font-size:9px;color:#4a5568;letter-spacing:1px;margin-bottom:4px">TAMPILAN</div>',unsafe_allow_html=True)
        _quick=st.toggle("⚡ Quick (150 saham)",value=False,key="CF_quick")
        _force_g=st.toggle("🔄 Fresh Data",value=False,key="CF_fresh_g")
        st.caption(f"🎯 Regime: {_active_regime} · {150 if _quick else len(ALL_STOCKS)} saham")
        st.caption(f"RVOL≥{_min_rvol}x · Score≥{_min_score} · Prob≥{_min_prob}%")

# Simpan ke session state
st.session_state.regime=_active_regime
st.session_state.threshold={"min_rvol":_min_rvol,"min_score":_min_score,"min_prob":_min_prob}
_scan_stocks=ALL_STOCKS[:150] if _quick else ALL_STOCKS

# Telegram helper
def send_tele(results,mode):
    if not TOKEN or not CHAT_ID or not results: return False
    now_=datetime.now(jakarta_tz); sep="━"*22
    hdr=f"🎯 *CYRUS FUND SCANNER*\n📊 *{mode}* · Regime:{_active_regime} · {now_.strftime('%H:%M')} WIB\n{sep}\n"
    body=""
    for r in results[:5]:
        sig=r.get("Sinyal","—"); em="🔥" if any(k in sig for k in ["HAKA","SUPER"]) else "⚡"
        body+=f"\n{em} *{r['T']}* `{sig}`\n   `{r['Now']:,}` | Prob `{r['Prob']}%` | RVOL `{r['RVOL_str']}`\n   TP `{r['TP']:,}` | SL `{r['SL']:,}` | +{r['Profit']:.1f}%\n"
    footer=f"\n{sep}\nTop {DISPLAY_TOP} dari {len(ALL_STOCKS)} saham\n⚠️ _Bukan saran investasi!_"
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id":CHAT_ID,"text":hdr+body+footer,"parse_mode":"Markdown"},timeout=10)
        return True
    except: return False

# ════════════════════════════════════════
#  TABS — ORIGINAL PATTERN YANG WORKS
# ════════════════════════════════════════
tab_mom,tab_int,tab_bsjp,tab_swing,tab_wl=st.tabs(
    ["🚀 Momentum","⚡ Intraday","🌙 BSJP","📈 Swing","👁️ Scanner Mandiri"])

def render_tab(label_btn,key_prefix,mode,tf_label,extra_info=""):
    c1,c2,c3,c4=st.columns([3,1,1,1])
    with c1: btn=st.button(label_btn,type="primary",use_container_width=True,key=f"btn_{key_prefix}")
    with c2: view=st.radio("V",["📋 Tabel","🃏 Cards"],key=f"view_{key_prefix}",label_visibility="collapsed")
    with c3: tele=st.toggle("📡 Tele",value=True,key=f"tele_{key_prefix}")
    with c4: force=st.toggle("🔄 Fresh",value=False,key=f"fresh_{key_prefix}")
    ci=cache_info_badge(_scan_stocks,"daily" if mode=="Swing" else "15m")
    st.markdown(f'<div style="font-size:10px;color:#4a5568;margin-bottom:6px">📊 {len(_scan_stocks)} saham · {tf_label} · {ci}{" · "+extra_info if extra_info else ""}</div>',unsafe_allow_html=True)
    if btn:
        pb=st.progress(0); msg=st.empty()
        res=do_scan(_scan_stocks,mode,pb,msg,force_fresh=force or _force_g)
        pb.empty(); msg.empty()
        st.session_state.last_scan=now_jkt.timestamp()
        st.session_state.scan_mode=mode
        save_results(mode,res,st.session_state.last_scan)
        if tele and res: send_tele(res,mode); st.toast("📡 Terkirim!",icon="✅")
        return res,view
    return None,view

with tab_mom:
    res,view=render_tab("🚀 SCAN MOMENTUM","momentum","Momentum","15M")
    if res is not None: st.session_state.res_momentum=res
    if st.session_state.res_momentum:
        show_met(st.session_state.res_momentum)
        show_tbl(st.session_state.res_momentum) if "Tabel" in view else show_cards(st.session_state.res_momentum)
    else: empty_state("🚀","SCAN MOMENTUM",f"Full {len(_scan_stocks)} saham IDX · Top {DISPLAY_TOP}")

with tab_int:
    res,view=render_tab("⚡ SCAN INTRADAY","intraday","Intraday","15M")
    if res is not None: st.session_state.res_intraday=res
    if st.session_state.res_intraday:
        show_met(st.session_state.res_intraday)
        show_tbl(st.session_state.res_intraday) if "Tabel" in view else show_cards(st.session_state.res_intraday)
    else: empty_state("⚡","SCAN INTRADAY",f"Full {len(_scan_stocks)} saham · RSI OS Bounce")

with tab_bsjp:
    _jok=now_jkt.hour>=14
    st.markdown(f"<div style='font-family:Space Mono,monospace;font-size:10px;padding:6px 12px;border-radius:6px;margin-bottom:8px;background:{'#0d2010' if _jok else '#201000'};color:{'#00ff88' if _jok else '#ffb700'};border:1px solid {'#00ff8844' if _jok else '#ffb70044'}'>{'🟢 JAM ENTRY BSJP! Beli 14:30–15:45 WIB.' if _jok else f'⏳ Tunggu jam 14:00 WIB — sekarang {now_jkt.strftime(chr(37)+chr(72)+chr(58)+chr(37)+chr(77))} WIB'}</div>",unsafe_allow_html=True)
    res,view=render_tab("🌙 SCAN BSJP","bsjp","BSJP","15M","Entry 14:30–15:45")
    if res is not None: st.session_state.res_bsjp=res
    if st.session_state.res_bsjp:
        show_met(st.session_state.res_bsjp)
        show_tbl(st.session_state.res_bsjp) if "Tabel" in view else show_cards(st.session_state.res_bsjp)
    else: empty_state("🌙","SCAN BSJP",f"Close dekat High · gap up potential")

with tab_swing:
    st.info("📈 Swing pakai data Daily (D1) — hold 3–10 hari")
    res,view=render_tab("📈 SCAN SWING","swing","Swing","Daily D1")
    if res is not None: st.session_state.res_swing=res
    if st.session_state.res_swing:
        show_met(st.session_state.res_swing)
        show_tbl(st.session_state.res_swing) if "Tabel" in view else show_cards(st.session_state.res_swing)
    else: empty_state("📈","SCAN SWING",f"Data D1 · {len(_scan_stocks)} saham")

with tab_wl:
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:10px;color:#4a5568;padding:8px 12px;background:#0d1117;border-radius:6px;border-left:3px solid #ff7b00;margin-bottom:10px'>Input ticker sendiri · bypass threshold filter · tampilkan semua</div>",unsafe_allow_html=True)
    wc1,wc2,wc3=st.columns([3,1,1])
    with wc1: wtxt=st.text_area("T",height=100,label_visibility="collapsed",placeholder="BBCA\nARCI, ASSA, GOTO",key="wtxt_input")
    with wc2:
        wmode=st.radio("Mode",["Momentum","Intraday","BSJP","Swing"],key="wmode_sel")
        wview=st.radio("V",["📋 Tabel","🃏 Cards"],key="wview_sel",label_visibility="collapsed")
    with wc3:
        st.markdown("<br>",unsafe_allow_html=True)
        wforce=st.toggle("🔄 Fresh",value=False,key="wl_fresh")
        wtele=st.toggle("📡 Tele",value=True,key="wl_tele")
        btn_wl=st.button("🔍 Analisa",type="primary",use_container_width=True,key="run_mandiri")
        btn_tele_m=st.button("📡 Kirim",use_container_width=True,key="tele_mandiri")
    if btn_wl and wtxt.strip():
        raw=list(dict.fromkeys([t.strip().upper() for ln in wtxt.split("\n") for t in ln.split(",") if t.strip()]))
        if raw:
            pb=st.progress(0); msg=st.empty()
            # skip_filter=True → bypass threshold, tampilkan semua ticker user
            st.session_state.wl_res=do_scan(raw,wmode,pb,msg,force_fresh=wforce,skip_filter=True)
            pb.empty(); msg.empty()
    if btn_tele_m and st.session_state.wl_res:
        if send_tele(st.session_state.wl_res,wmode): st.toast("📡 Terkirim!",icon="✅")
        else: st.error("Cek TOKEN/CHAT_ID di secrets.toml")
    if st.session_state.wl_res:
        show_met(st.session_state.wl_res)
        show_tbl(st.session_state.wl_res) if "Tabel" in wview else show_cards(st.session_state.wl_res)
    else:
        st.markdown("<div style='text-align:center;padding:48px;color:#4a5568;font-family:Space Mono,monospace'><div style='font-size:28px;margin-bottom:8px'>👁️</div><div>MASUKKAN TICKER DI ATAS</div></div>",unsafe_allow_html=True)

# ════════════════════════════════════════
#  AUTO-REFRESH — JS Timer (no rerun loop!)
# ════════════════════════════════════════
import streamlit.components.v1 as _cf
_has=any([st.session_state.res_momentum,st.session_state.res_intraday,
          st.session_state.res_bsjp,st.session_state.res_swing])
if is_open and _has and st.session_state.last_scan:
    _el=int(now_jkt.timestamp()-st.session_state.last_scan)
    if _el < 480:
        _ms=max(10000,(480-_el)*1000)
        _cf.html(f"""<script>
        if(window._cyrus_ar)clearTimeout(window._cyrus_ar);
        window._cyrus_ar=setTimeout(function(){{window.parent.location.reload();}},{_ms});
        </script>""",height=0)

_mar=int(max(0,480-(now_jkt.timestamp()-st.session_state.last_scan))//60) if st.session_state.last_scan else 8
_sar=int(max(0,480-(now_jkt.timestamp()-st.session_state.last_scan))%60) if st.session_state.last_scan else 0
st.markdown(
    f"<div style='margin-top:20px;padding-top:10px;border-top:1px solid #1c2533;"
    f"font-family:Space Mono,monospace;font-size:9px;color:#4a5568;text-align:center'>"
    f"🎯 Cyrus Fund Scanner · {len(ALL_STOCKS)} saham IDX · Top {DISPLAY_TOP} · yFinance 📊 · Next: {_mar:02d}:{_sar:02d}</div>",
    unsafe_allow_html=True)
