# -*- coding: utf-8 -*-
import os, json, re
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

BUILD_TAG = "diag-2025-08-30-C"
APP_TITLE = f"WishCo Branch Portal — เบิกอุปกรณ์ ({BUILD_TAG})"
TIMEZONE = timezone(timedelta(hours=7))

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

def pk_diagnostics(pk: str):
    info = {}
    info["is_str"] = isinstance(pk, str)
    info["len"] = len(pk) if isinstance(pk, str) else None
    info["contains_literal_backslash_n"] = "\\n" in pk if isinstance(pk, str) else False
    info["contains_real_newlines"] = ("\n" in pk) if isinstance(pk, str) else False
    info["starts_with_header"] = pk.startswith("-----BEGIN PRIVATE KEY-----") if isinstance(pk, str) else False
    info["ends_with_footer"] = pk.rstrip().endswith("-----END PRIVATE KEY-----") if isinstance(pk, str) else False
    if isinstance(pk, str):
        lines = pk.splitlines()
        info["line_count"] = len(lines)
        # find lines that have non base64 chars (ignore header/footer/empty)
        bad = []
        for i,ln in enumerate(lines):
            if ln.startswith("-----BEGIN") or ln.startswith("-----END") or ln.strip()=="" or ":" in ln:
                continue
            # Allow base64 + '='
            if not re.fullmatch(r"[A-Za-z0-9+/=]+", ln):
                bad.append((i+1, ln[:60]+"..." if len(ln)>60 else ln))
        info["bad_lines"] = bad
        # check mod 4 length for base64 lines
        mod4 = []
        for i,ln in enumerate(lines):
            if ln.startswith("-----BEGIN") or ln.startswith("-----END") or ln.strip()=="":
                continue
            mod4.append((i+1, len(ln)%4))
        info["mod4_summary"] = {m: sum(1 for _,mm in mod4 if mm==m) for m in (0,1,2,3)}
    return info

def normalize_private_key(pk: str):
    if not isinstance(pk, str): return pk, "not_str"
    original = pk
    # If it looks like JSON-escaped (has \n but no real newline), unescape
    if "\\n" in pk and "\n" not in pk:
        pk = pk.replace("\\n", "\n")
    # Strip leading/trailing spaces from each line
    lines = [ln.strip() for ln in pk.splitlines()]
    # Remove accidental surrounding quotes/backticks
    if lines and lines[0].startswith('"') and lines[-1].endswith('"'):
        lines[0] = lines[0].lstrip('"')
        lines[-1] = lines[-1].rstrip('"')
    pk = "\n".join(lines)
    if not pk.endswith("\n"): pk += "\n"
    changed = "changed" if pk != original else "unchanged"
    return pk, changed

def show_secrets_status():
    keys = list(st.secrets.keys()) if hasattr(st, "secrets") else []
    st.sidebar.subheader("Secrets status")
    st.sidebar.write("keys:", keys)
    if "gcp_service_account" in st.secrets:
        d = dict(st.secrets["gcp_service_account"])
        prv = d.get("private_key", "")
        d_preview = {k: ("<hidden>" if "key" in k and k!="private_key" else v) for k,v in d.items() if k!="private_key"}
        st.sidebar.write("gcp_service_account:", d_preview)
        di = pk_diagnostics(prv)
        st.sidebar.write("private_key diagnostics:", di)
        if isinstance(prv, str) and prv:
            preview = prv.splitlines()[:2] + ["..."] + prv.splitlines()[-2:]
            st.sidebar.code("\n".join(preview), language="text")

def load_credentials():
    from google.oauth2.service_account import Credentials
    scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

    if "gcp_service_account" in st.secrets:
        raw = dict(st.secrets["gcp_service_account"])
        pk = raw.get("private_key","")
        try:
            return Credentials.from_service_account_info(raw, scopes=scope)
        except Exception as e:
            # try normalization
            pk2, status = normalize_private_key(pk)
            raw2 = dict(raw); raw2["private_key"] = pk2
            try:
                creds = Credentials.from_service_account_info(raw2, scopes=scope)
                st.success(f"Normalized private_key ({status}) แล้วใช้งานได้ ✅")
                return creds
            except Exception as e2:
                st.error(f"สร้าง Credentials ไม่ได้ (เดิม: {e}) / (หลัง normalize: {e2})")
                st.stop()

    # fallbacks
    req = {"type","project_id","private_key_id","private_key","client_email","client_id"}
    if req.issubset(set(st.secrets.keys())):
        info = {k: st.secrets[k] for k in req}
        info.setdefault("auth_uri","https://accounts.google.com/o/oauth2/auth")
        info.setdefault("token_uri","https://oauth2.googleapis.com/token")
        info.setdefault("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs")
        info.setdefault("client_x509_cert_url","")
        try:
            return Credentials.from_service_account_info(info, scopes=scope)
        except Exception as e:
            st.error(f"top-level keys พบแต่สร้าง Credentials ไม่ได้: {e}"); st.stop()

    s = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if s:
        try:
            return Credentials.from_service_account_info(json.loads(s), scopes=scope)
        except Exception as e:
            st.error(f"GOOGLE_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง: {e}"); st.stop()

    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        try:
            return Credentials.from_service_account_file(p, scopes=scope)
        except Exception as e:
            st.error(f"GOOGLE_APPLICATION_CREDENTIALS ใช้ไม่ได้: {e}"); st.stop()

    st.error("ไม่พบ Service Account ใน Secrets"); st.stop()

def open_sheet(client):
    import gspread
    SHEET_ID  = st.secrets.get("SHEET_ID","").strip() or os.environ.get("SHEET_ID","").strip()
    SHEET_URL = st.secrets.get("SHEET_URL","").strip() or os.environ.get("SHEET_URL","").strip()
    if SHEET_ID:  return client.open_by_key(SHEET_ID)
    if SHEET_URL: return client.open_by_url(SHEET_URL)
    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์เพื่อเชื่อมต่อ")
    url = st.text_input("URL ของ Google Sheet", value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีตจาก URL", type="primary"):
        st.session_state["input_sheet_url"] = url.strip()
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
