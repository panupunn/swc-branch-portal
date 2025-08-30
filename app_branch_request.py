# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — Phase 1 (Production, Updated)

- โหลด Service Account จาก Secrets ได้หลายรูปแบบ
- รองรับ SHEET_ID / SHEET_URL (หรือวาง URL ครั้งแรกบนหน้า)
- ล็อกอินสาขา (ทนต่อช่องว่าง/พิมพ์ใหญ่เล็ก)
- แสดงสต็อก + ฟอร์ม "เบิกอุปกรณ์" → บันทึกลง Requests + แจ้งเตือนใน Notifications
"""

import os, json, time
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TZ = timezone(timedelta(hours=7))

# ---------- small helpers ----------
def do_rerun():
    """Streamlit >=1.32 ใช้ st.rerun(); เก่าๆ ยังมี st.experimental_rerun()"""
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()  # type: ignore[attr-defined]
        except Exception:
            pass

def now_str():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def ensure_headers(ws, headers):
    first = ws.row_values(1) or []
    if not first:
        ws.update("A1", [headers])
        return headers
    missing = [h for h in headers if h not in first]
    if missing:
        ws.update("A1", [first + missing])
        first += missing
    return first

def ws_to_df(ws):
    vals = ws.get_all_values()
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()

def find_col(df, names:set):
    lowset = {x.lower() for x in names}
    for c in list(df.columns):
        if c.strip() in names or c.strip().lower() in lowset:
            return c
    return None

# ---------- Credentials loader ----------
def load_credentials():
    from google.oauth2.service_account import Credentials
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        return Credentials.from_service_account_info(info, scopes=scope)

    top_keys = {"type","project_id","private_key_id","private_key","client_email","client_id"}
    if top_keys.issubset(set(st.secrets.keys())):
        info = {k: st.secrets[k] for k in top_keys}
        info.setdefault("auth_uri","https://accounts.google.com/o/oauth2/auth")
        info.setdefault("token_uri","https://oauth2.googleapis.com/token")
        info.setdefault("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs")
        info.setdefault("client_x509_cert_url","")
        return Credentials.from_service_account_info(info, scopes=scope)

    raw = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip() or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if raw:
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            info = json.loads(raw.replace("\n","").replace("\r",""))
        return Credentials.from_service_account_info(info, scopes=scope)

    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        return Credentials.from_service_account_file(p, scopes=scope)

    st.error("ไม่พบ Service Account ใน Secrets"); st.stop()

def open_spreadsheet(client):
    SHEET_ID  = st.secrets.get("SHEET_ID","").strip() or os.environ.get("SHEET_ID","").strip()
    SHEET_URL = st.secrets.get("SHEET_URL","").strip() or os.environ.get("SHEET_URL","").strip()
    if SHEET_ID:
        return client.open_by_key(SHEET_ID)
    if SHEET_URL:
        return client.open_by_url(SHEET_URL)

    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์ Google Sheet เพื่อเชื่อมต่อครั้งแรก")
    url = st.text_input("URL ของ Google Sheet (https://docs.google.com/spreadsheets/…)",
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

# ---------- App ----------
def main():
    import gspread
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    creds = load_credentials()
    client = gspread.authorize(creds)
    ss = open_spreadsheet(client)

    titles = [w.title for w in ss.worksheets()]
    ws_users = ss.worksheet("Users") if "Users" in titles else ss.add_worksheet("Users", 1000, 26)
    ws_items = ss.worksheet("Items") if "Items" in titles else ss.add_worksheet("Items", 2000, 26)
    ws_reqs  = ss.worksheet("Requests") if "Requests" in titles else ss.add_worksheet("Requests", 2000, 26)
    ws_noti  = ss.worksheet("Notifications") if "Notifications" in titles else ss.add_worksheet("Notifications", 2000, 26)
    ws_conf  = ss.worksheet("Settings") if "Settings" in titles else ss.add_worksheet("Settings", 1000, 26)

    ensure_headers(ws_users, ["username","password","role","BranchCode"])
    ensure_headers(ws_items, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    ensure_headers(ws_reqs,  ["ReqNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty","Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"])
    ensure_headers(ws_noti,  ["NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"])
    ensure_headers(ws_conf,  ["key","value"])

    # ----- Login panel (robust) -----
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
        st.session_state["user"] = {}

    if not st.session_state["auth"]:
        u = st.sidebar.text_input("ชื่อผู้ใช้")
        p = st.sidebar.text_input("รหัสผ่าน", type="password")
        if st.sidebar.button("ล็อกอิน", use_container_width=True):
            dfu = ws_to_df(ws_users)
            if dfu.empty:
                st.sidebar.error("ไม่มีผู้ใช้ในชีต Users"); st.stop()

            cu = find_col(dfu, {"username","user","บัญชีผู้ใช้","ชื่อผู้ใช้"})
            cp = find_col(dfu, {"password","รหัสผ่าน"})
            cb = find_col(dfu, {"BranchCode","สาขา","branch"})
            if not (cu and cp and cb):
                st.sidebar.error("Users sheet ไม่ครบคอลัมน์ (ต้องมี username/password/BranchCode)"); st.stop()

            for c in (cu, cp, cb):
                dfu[c] = dfu[c].astype(str).str.strip()

            u_norm = (u or "").strip().casefold()
            p_norm = (p or "").strip()
            row = dfu[dfu[cu].str.casefold() == u_norm].head(1)

            if row.empty or str(row.iloc[0][cp]).strip() != p_norm:
                st.sidebar.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
            else:
                st.session_state["auth"] = True
                st.session_state["user"] = {"username": (u or "").strip(),
                                            "branch": str(row.iloc[0][cb]).strip()}
                st.sidebar.success(f"ยินดีต้อนรับ {st.session_state['user']['username']}")
                time.sleep(0.5)
                do_rerun()
        st.stop()

    # Logout
    if st.sidebar.button("ออกจากระบบ"):
        st.session_state["auth"] = False
        st.session_state["user"] = {}
        do_rerun()

    branch_code = st.session_state["user"]["branch"]
    username = st.session_state["user"]["username"]

    # ----- Inventory -----
    st.header("📦 คลังสำหรับสาขา")
    dfi = ws_to_df(ws_items)
    if dfi.empty:
        st.info("ยังไม่มีข้อมูลใน Items"); st.stop()

    c_code = find_col(dfi, {"รหัส","ItemCode","Code"})
    c_name = find_col(dfi, {"ชื่อ","Name","รายการ"})
    c_qty  = find_col(dfi, {"คงเหลือ","Qty","จำนวน"})
    c_ready= find_col(dfi, {"พร้อมให้เบิก","พร้อมให้เบิก(Y/N)","Ready"})

    view_df = dfi[[c_code,c_name,c_qty] + ([c_ready] if c_ready else [])].copy()
    view_df.rename(columns={c_code:"รหัส",c_name:"ชื่อ",c_qty:"คงเหลือ"}, inplace=True)
    if c_ready: view_df.rename(columns={c_ready:"พร้อมให้เบิก"}, inplace=True)
    st.dataframe(view_df, use_container_width=True, height=420)

    # ----- Request form -----
    st.subheader("📝 เบิกอุปกรณ์")
    ready_df = dfi.copy()
    if c_ready:
        ready_df = ready_df[ready_df[c_ready].astype(str).str.upper().str.strip().isin(["Y","YES","TRUE","1"])]
    if ready_df.empty:
        st.warning("ยังไม่มีอุปกรณ์ที่พร้อมให้เบิก"); st.stop()

    ready_df["_label"] = (
        ready_df[c_code].astype(str)
        + " — " + ready_df[c_name].astype(str)
        + " (คงเหลือ: " + ready_df[c_qty].astype(str) + ")"
    )
    choice = st.selectbox("เลือกอุปกรณ์", ready_df["_label"].tolist())
    qty_req = st.number_input("จำนวนที่ต้องการ", min_value=1, step=1, value=1)
    note = st.text_input("หมายเหตุ (ถ้ามี)", value="")

    if st.button("ยืนยันเบิกอุปกรณ์", type="primary"):
        row = ready_df[ready_df["_label"] == choice].iloc[0]
        item_code = str(row[c_code])
        item_name = str(row[c_name])

        req_no = f"REQ-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
        ts = now_str()

        # Requests
        headers = ws_reqs.row_values(1)
        new_rec = {
            "ReqNo": req_no,
            "CreatedAt": ts,
            "Branch": branch_code,
            "Requester": username,
            "ItemCode": item_code,
            "ItemName": item_name,
            "Qty": str(int(qty_req)),
            "Status": "pending",
            "Approver": "",
            "LastUpdate": ts,
            "Note": note,
            "NotifiedMain(Y/N)": "N",
            "NotifiedBranch(Y/N)": "N",
        }
        ws_reqs.append_row([new_rec.get(h,"") for h in headers], value_input_option="USER_ENTERED")

        # Notifications
        n_headers = ws_noti.row_values(1)
        noti = {
            "NotiID": f"NOTI-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}",
            "CreatedAt": ts,
            "TargetApp": "main_app",
            "TargetBranch": branch_code,
            "Type": "REQUEST_CREATED",
            "RefID": req_no,
            "Message": f"{branch_code} เบิกอุปกรณ์ {item_code} x {int(qty_req)} โดย {username}",
            "ReadFlag": "N",
            "ReadAt": "",
        }
        ws_noti.append_row([noti.get(h,"") for h in n_headers], value_input_option="USER_ENTERED")

        st.success(f"สร้างคำขอ {req_no} สำเร็จ! (รอการดำเนินการ)")
        time.sleep(1.2)
        do_rerun()

    # ----- My requests (preview) -----
    with st.expander("คำขอของฉัน (ล่าสุด)"):
        dfr = ws_to_df(ws_reqs)
        if not dfr.empty:
            c_branch= find_col(dfr, {"Branch"})
            c_user  = find_col(dfr, {"Requester"})
            c_created = find_col(dfr, {"CreatedAt"})
            sub = dfr[(dfr[c_branch]==branch_code) & (dfr[c_user]==username)].copy()
            if not sub.empty:
                if c_created:
                    sub = sub.sort_values(c_created, ascending=False).head(20)
                st.dataframe(sub, use_container_width=True, height=300)
            else:
                st.write("ยังไม่มีคำขอล่าสุด")
        else:
            st.write("ยังไม่มีคำขอในระบบ")

if __name__ == "__main__":
    main()
