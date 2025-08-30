# -*- coding: utf-8 -*-
import os, json
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TIMEZONE = timezone(timedelta(hours=7))

# ===== Helpers =====
def _now(): return datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

def _debug_on(): 
    qp = st.query_params
    return (qp.get("debug", ["0"])[0] in ("1","true","yes"))

def _secrets_keys():
    try:
        return list(st.secrets.keys())
    except Exception:
        return []

def _load_credentials():
    try:
        from google.oauth2.service_account import Credentials
    except Exception:
        st.error("ต้องติดตั้ง gspread และ google-auth (ดู requirements.txt)"); st.stop()

    # 1) table: [gcp_service_account] หรือ [service_account]
    for k in ("gcp_service_account", "service_account"):
        if k in st.secrets and isinstance(st.secrets[k], dict):
            info = dict(st.secrets[k])
            scope = ["https://www.googleapis.com/auth/spreadsheets",
                     "https://www.googleapis.com/auth/drive"]
            return Credentials.from_service_account_info(info, scopes=scope)

    # 2) top-level keys (วางคีย์บนสุด)
    required = {"type","project_id","private_key_id","private_key","client_email","client_id"}
    if required.issubset(set(_secrets_keys())):
        info = {k: st.secrets[k] for k in required}
        info["auth_uri"]  = st.secrets.get("auth_uri","https://accounts.google.com/o/oauth2/auth")
        info["token_uri"] = st.secrets.get("token_uri","https://oauth2.googleapis.com/token")
        info["auth_provider_x509_cert_url"] = st.secrets.get("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs")
        info["client_x509_cert_url"] = st.secrets.get("client_x509_cert_url","")
        scope = ["https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive"]
        return Credentials.from_service_account_info(info, scopes=scope)

    # 3) JSON string
    s = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if s:
        try:
            info = json.loads(s)
            scope = ["https://www.googleapis.com/auth/spreadsheets",
                     "https://www.googleapis.com/auth/drive"]
            return Credentials.from_service_account_info(info, scopes=scope)
        except Exception:
            st.error("GOOGLE_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง"); st.stop()

    # 4) file path
    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        scope = ["https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive"]
        return Credentials.from_service_account_file(p, scopes=scope)

    st.error("ไม่พบ Service Account ใน Secrets")
    if _debug_on(): st.sidebar.write("Secrets keys:", _secrets_keys())
    st.stop()

def _open_spreadsheet(client):
    SHEET_ID  = st.secrets.get("SHEET_ID","").strip() or os.environ.get("SHEET_ID","").strip()
    SHEET_URL = st.secrets.get("SHEET_URL","").strip() or os.environ.get("SHEET_URL","").strip()
    if SHEET_ID: 
        return client.open_by_key(SHEET_ID)
    if SHEET_URL:
        return client.open_by_url(SHEET_URL)

    # AutoURL: ให้ผู้ใช้วางลิงก์บนหน้าแรก
    st.info("ยังไม่ตั้งค่า SHEET_ID/SHEET_URL — วางลิงก์ Google Sheet ด้านล่างเพื่อเชื่อมต่อ")
    url = st.text_input("วาง URL ของ Google Sheet (เริ่มด้วย https://docs.google.com/spreadsheets/...)",
                        value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีตจาก URL", type="primary"):
        if not url.strip():
            st.warning("กรุณาวาง URL"); st.stop()
        st.session_state["input_sheet_url"] = url.strip()
        try:
            return client.open_by_url(url.strip())
        except Exception as e:
            st.error(f"เปิดชีตไม่สำเร็จ: {e}"); st.stop()
    st.stop()

def _ensure(ws, headers):
    first = ws.row_values(1) or []
    if not first: ws.update("A1", [headers])
    else:
        missing = [h for h in headers if h not in first]
        if missing: ws.update("A1", [first + missing])

def _to_df(ws):
    vals = ws.get_all_values()
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()

def _find_col(df, names:set):
    for c in df.columns:
        if c.strip() in names or c.strip().lower() in {x.lower() for x in names}: return c
    return None

# ===== App =====
def main():
    import gspread
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    creds = _load_credentials()
    client = gspread.authorize(creds)
    ss = _open_spreadsheet(client)

    # เตรียมชีต
    ws_users = ss.worksheet("Users")     if "Users"     in [w.title for w in ss.worksheets()] else ss.add_worksheet("Users", 1000, 26)
    ws_items = ss.worksheet("Items")     if "Items"     in [w.title for w in ss.worksheets()] else ss.add_worksheet("Items", 1000, 26)
    ws_reqs  = ss.worksheet("Requests")  if "Requests"  in [w.title for w in ss.worksheets()]  else ss.add_worksheet("Requests", 1000, 26)
    ws_noti  = ss.worksheet("Notifications") if "Notifications" in [w.title for w in ss.worksheets()] else ss.add_worksheet("Notifications", 1000, 26)
    ws_conf  = ss.worksheet("Settings")  if "Settings"  in [w.title for w in ss.worksheets()]  else ss.add_worksheet("Settings", 1000, 26)

    _ensure(ws_users, ["username","password","role","BranchCode"])
    _ensure(ws_items, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    _ensure(ws_reqs,  ["ReqNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty",
                       "Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"])
    _ensure(ws_noti,  ["NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"])
    _ensure(ws_conf,  ["key","value"])

    # Login (ขั้นต่ำ)
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    u = st.sidebar.text_input("ชื่อผู้ใช้")
    p = st.sidebar.text_input("รหัสผ่าน", type="password")
    if st.sidebar.button("ล็อกอิน", use_container_width=True):
        df = _to_df(ws_users)
        cu = _find_col(df, {"username","user","บัญชีผู้ใช้"})
        cp = _find_col(df, {"password","รหัสผ่าน"})
        cb = _find_col(df, {"branch","BranchCode","สาขา","branchcode"})
        if df.empty or not all([cu,cp,cb]): st.error("Users sheet ไม่ครบคอลัมน์"); st.stop()
        row = df[df[cu]==u].head(1)
        if row.empty or str(row.iloc[0][cp]) != p: st.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง"); st.stop()
        st.session_state["auth"]=True; st.session_state["user"]={"username":u,"branch":str(row.iloc[0][cb])}
        st.success(f"ยินดีต้อนรับ {u}")

    if not st.session_state.get("auth"): st.stop()

    # หน้า “คลัง” ให้เช็คเห็นข้อมูลได้
    st.header("📦 คลังสำหรับสาขา")
    items = _to_df(ws_items)
    if items.empty:
        st.info("ยังไม่มีข้อมูลอุปกรณ์ใน Items"); return
    c_code = _find_col(items, {"รหัส","ItemCode","Code"})
    c_name = _find_col(items, {"ชื่อ","Name","รายการ"})
    c_qty  = _find_col(items, {"คงเหลือ","Qty","จำนวน"})
    c_ready= _find_col(items, {"พร้อมให้เบิก","พร้อมให้เบิก(Y/N)","Ready"})
    df = items[[c_code,c_name,c_qty] + ([c_ready] if c_ready else [])].copy()
    df.rename(columns={c_code:"รหัส",c_name:"ชื่อ",c_qty:"คงเหลือ"}, inplace=True)
    if c_ready: df.rename(columns={c_ready:"พร้อมให้เบิก"}, inplace=True)
    st.dataframe(df, use_container_width=True)

    if _debug_on():
        st.sidebar.write("Secrets keys:", _secrets_keys())

if __name__ == "__main__":
    main()
