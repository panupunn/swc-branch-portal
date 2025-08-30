# -*- coding: utf-8 -*-
import os, json
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

BUILD_TAG = "diag-2025-08-30-A"  # ใช้ตรวจว่าไฟล์ใหม่นี้รันอยู่จริง
APP_TITLE = f"WishCo Branch Portal — เบิกอุปกรณ์ ({BUILD_TAG})"
TIMEZONE = timezone(timedelta(hours=7))

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

def show_secrets_status():
    try:
        keys = list(st.secrets.keys())
    except Exception:
        keys = []
    st.sidebar.subheader("Secrets status")
    st.sidebar.write("keys:", keys)
    if "gcp_service_account" in st.secrets:
        d = dict(st.secrets["gcp_service_account"])
        # ซ่อนรายละเอียด key แต่แสดงว่าเป็นหลายบรรทัด
        prv = d.get("private_key","")
        preview = prv.splitlines()[:2] + ["..."] + prv.splitlines()[-2:] if prv else []
        d_preview = {k: ("<hidden>" if "key" in k and k!="private_key" else v) for k,v in d.items() if k!="private_key"}
        st.sidebar.write("gcp_service_account:", d_preview)
        st.sidebar.code("\n".join(preview) if preview else "(no private_key)", language="text")

def load_credentials():
    from google.oauth2.service_account import Credentials
    scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], dict):
        st.sidebar.success("using [gcp_service_account]")
        return Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scope)
    req = {"type","project_id","private_key_id","private_key","client_email","client_id"}
    if req.issubset(set(st.secrets.keys())):
        st.sidebar.success("using top-level keys")
        info = {k: st.secrets[k] for k in req}
        info.setdefault("auth_uri","https://accounts.google.com/o/oauth2/auth")
        info.setdefault("token_uri","https://oauth2.googleapis.com/token")
        info.setdefault("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs")
        info.setdefault("client_x509_cert_url","")
        return Credentials.from_service_account_info(info, scopes=scope)
    s = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if s:
        st.sidebar.success("using GOOGLE_SERVICE_ACCOUNT_JSON")
        return Credentials.from_service_account_info(json.loads(s), scopes=scope)
    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        st.sidebar.success(f"using file: {p}")
        return Credentials.from_service_account_file(p, scopes=scope)
    st.error("ไม่พบ Service Account ใน Secrets"); st.stop()

def open_sheet(client):
    SHEET_ID  = st.secrets.get("SHEET_ID","").strip() or os.environ.get("SHEET_ID","").strip()
    SHEET_URL = st.secrets.get("SHEET_URL","").strip() or os.environ.get("SHEET_URL","").strip()
    if SHEET_ID:  return client.open_by_key(SHEET_ID)
    if SHEET_URL: return client.open_by_url(SHEET_URL)
    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์เพื่อเชื่อมต่อ")
    url = st.text_input("URL ของ Google Sheet", value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีตจาก URL", type="primary"):
        st.session_state["input_sheet_url"] = url.strip()
        import gspread
        try:
            return client.open_by_url(url.strip())
        except Exception as e:
            st.error(f"เปิดชีตไม่สำเร็จ: {e}"); st.stop()
    st.stop()

def ensure_headers(ws, headers):
    first = ws.row_values(1) or []
    if not first: ws.update("A1", [headers])
    else:
        missing = [h for h in headers if h not in first]
        if missing: ws.update("A1", [first + missing])

def ws_to_df(ws):
    vals = ws.get_all_values()
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()

def find_col(df, names:set):
    for c in df.columns:
        if c.strip() in names or c.strip().lower() in {x.lower() for x in names}: return c
    return None

def main():
    import gspread
    show_secrets_status()
    creds = load_credentials()
    client = gspread.authorize(creds)
    ss = open_sheet(client)

    titles = [w.title for w in ss.worksheets()]
    ws_users = ss.worksheet("Users") if "Users" in titles else ss.add_worksheet("Users", 1000, 26)
    ws_items = ss.worksheet("Items") if "Items" in titles else ss.add_worksheet("Items", 1000, 26)
    ensure_headers(ws_users, ["username","password","role","BranchCode"])
    ensure_headers(ws_items, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])

    st.header("📦 คลังสำหรับสาขา")
    items = ws_to_df(ws_items)
    if items.empty:
        st.info("ยังไม่มีข้อมูลใน Items"); return
    c_code = find_col(items, {"รหัส","ItemCode","Code"})
    c_name = find_col(items, {"ชื่อ","Name","รายการ"})
    c_qty  = find_col(items, {"คงเหลือ","Qty","จำนวน"})
    c_ready= find_col(items, {"พร้อมให้เบิก","พร้อมให้เบิก(Y/N)","Ready"})
    df = items[[c_code,c_name,c_qty] + ([c_ready] if c_ready else [])].copy()
    df.rename(columns={c_code:"รหัส",c_name:"ชื่อ",c_qty:"คงเหลือ"}, inplace=True)
    if c_ready: df.rename(columns={c_ready:"พร้อมให้เบิก"}, inplace=True)
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
