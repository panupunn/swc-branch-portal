# -*- coding: utf-8 -*-
"""
Branch Portal (Streamlit + Google Sheets) — Phase 1 (patched for Streamlit Cloud secrets)
"""
import os
import io
import json
import time
import random
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

# ---- Auto-load Streamlit secrets into environment (for Streamlit Cloud) ----
try:
    for _k, _v in st.secrets.items():
        if isinstance(_v, (dict, list)):
            os.environ.setdefault(_k, json.dumps(_v, ensure_ascii=False))
        else:
            os.environ.setdefault(_k, str(_v))
except Exception:
    pass
# ---------------------------------------------------------------------------

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception as e:
    st.warning("ไม่พบไลบรารี gspread / google-auth (โปรด pip install gspread google-auth)")
    gspread = None
    Credentials = None

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TIMEZONE = timezone(timedelta(hours=7))

SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SHEET_URL = os.environ.get("SHEET_URL", "").strip()
SERVICE_ACCOUNT_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

SHEET_USERS = os.environ.get("SHEET_USERS", "Users")
SHEET_ITEMS = os.environ.get("SHEET_ITEMS", "Items")
SHEET_REQUESTS = os.environ.get("SHEET_REQUESTS", "Requests")
SHEET_NOTI = os.environ.get("SHEET_NOTIFICATIONS", "Notifications")
SHEET_SETTINGS = os.environ.get("SHEET_SETTINGS", "Settings")

VISIBLE_AVAILABLE_ONLY = "AVAILABLE_ONLY"
VISIBLE_ALL_WITH_FLAG = "ALL_WITH_FLAG"

STATUS_PENDING = "PENDING"
STATUS_ISSUED  = "ISSUED"
STATUS_RECEIVED = "RECEIVED"

NOTI_REQ_CREATED = "REQ_CREATED"
NOTI_ITEM_ISSUED = "ITEM_ISSUED"

def _now_str():
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

def _load_credentials():
    info = None
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
        except Exception as e:
            st.error("GOOGLE_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง")
            return None
    if info:
        scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        return Credentials.from_service_account_info(info, scopes=scope)

    path_candidates = []
    if SERVICE_ACCOUNT_PATH:
        path_candidates.append(SERVICE_ACCOUNT_PATH)
    path_candidates.append("service_account.json")
    for p in path_candidates:
        if os.path.exists(p):
            scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            return Credentials.from_service_account_file(p, scopes=scope)
    st.error("ไม่พบ Service Account (ตั้งค่า GOOGLE_SERVICE_ACCOUNT_JSON หรือ GOOGLE_APPLICATION_CREDENTIALS)")
    return None

def _open_spreadsheet(client):
    if SHEET_ID:
        return client.open_by_key(SHEET_ID)
    if SHEET_URL:
        return client.open_by_url(SHEET_URL)
    st.error("โปรดตั้งค่า SHEET_ID หรือ SHEET_URL ใน environment")
    return None

def _ensure_worksheet(ss, name, headers):
    try:
        ws = ss.worksheet(name)
    except Exception:
        ws = ss.add_worksheet(title=name, rows=1000, cols=50)
        ws.append_row(headers)
        return ws
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if not first:
        ws.update("A1", [headers])
    else:
        missing = [h for h in headers if h not in first]
        if missing:
            new = first + missing
            ws.update("A1", [new])
    return ws

def _worksheet_to_df(ws):
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def _append_rows(ws, rows):
    if not rows:
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def _update_cell(ws, row, col, value):
    ws.update_cell(row, col, value)

def _find_col(df, candidates: set):
    for c in df.columns:
        if c.strip() in candidates:
            return c
        if c.strip().lower() in {x.lower() for x in candidates}:
            return c
    return None

ITEM_CODE_HEADERS = {"รหัส", "รหัสวัสดุ", "ItemCode", "Code", "รหัสอุปกรณ์"}
ITEM_NAME_HEADERS = {"ชื่อ", "รายการ", "ItemName", "Name", "ชื่ออุปกรณ์"}
ITEM_QTY_HEADERS  = {"คงเหลือ", "จำนวนคงเหลือ", "Stock", "Qty", "จำนวน"}
ITEM_READY_HEADERS = {"พร้อมให้เบิก", "พร้อมให้เบิก(Y/N)", "Available", "Ready", "พร้อม"}

USER_NAME_HEADERS = {"username", "user", "บัญชีผู้ใช้", "ผู้ใช้"}
USER_PASS_HEADERS = {"password", "รหัสผ่าน"}
USER_ROLE_HEADERS = {"role", "บทบาท"}
USER_BRANCH_HEADERS = {"branch", "สาขา", "branchcode", "BranchCode"}

@st.cache_data(ttl=15, show_spinner=False)
def load_settings_df():
    ws = st.session_state["ws_settings"]
    return _worksheet_to_df(ws)

@st.cache_data(ttl=15, show_spinner=False)
def load_items_df():
    ws = st.session_state["ws_items"]
    return _worksheet_to_df(ws)

@st.cache_data(ttl=15, show_spinner=False)
def load_users_df():
    ws = st.session_state["ws_users"]
    return _worksheet_to_df(ws)

@st.cache_data(ttl=10, show_spinner=False)
def load_requests_df():
    ws = st.session_state["ws_requests"]
    return _worksheet_to_df(ws)

@st.cache_data(ttl=10, show_spinner=False)
def load_notifications_df():
    ws = st.session_state["ws_noti"]
    return _worksheet_to_df(ws)

def verify_password(plain: str, hashed: str) -> bool:
    if not hashed:
        return False
    try:
        import bcrypt  # type: ignore
        if hashed.startswith("$2"):
            return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        pass
    return plain == hashed

def do_login():
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    username = st.sidebar.text_input("ชื่อผู้ใช้", key="login_user")
    password = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_pass")
    btn = st.sidebar.button("ล็อกอิน", use_container_width=True)
    if btn:
        users = load_users_df()
        col_user = _find_col(users, USER_NAME_HEADERS)
        col_pass = _find_col(users, USER_PASS_HEADERS)
        col_role = _find_col(users, USER_ROLE_HEADERS)
        col_branch = _find_col(users, USER_BRANCH_HEADERS)

        if not all([col_user, col_pass, col_branch]):
            st.error("Users sheet ไม่ครบคอลัมน์ที่จำเป็น (username/password/branch)")
            return

        row = users[users[col_user] == username].head(1)
        if row.empty:
            st.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
            return

        hashed = str(row.iloc[0][col_pass])
        if not verify_password(password, hashed):
            st.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
            return

        role = str(row.iloc[0][col_role]) if col_role else ""
        if role and role.lower() not in {"branch", "user", "staff", "สาขา"}:
            st.error("บัญชีนี้ไม่ได้กำหนดบทบาทสำหรับสาขา")
            return

        branch = str(row.iloc[0][col_branch])
        st.session_state["auth"] = True
        st.session_state["user"] = {"username": username, "role": role or "branch", "branch": branch}
        st.success(f"ยินดีต้อนรับ {username} (สาขา {branch})")

def ensure_session():
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
    if "user" not in st.session_state:
        st.session_state["user"] = None

def page_notifications():
    st.header("🔔 การแจ้งเตือน")
    noti = load_notifications_df().copy()
    if noti.empty:
        st.info("ยังไม่มีการแจ้งเตือน")
        return
    branch = st.session_state["user"]["branch"]
    for c in ["TargetApp", "TargetBranch", "ReadFlag"]:
        if c not in noti.columns:
            st.warning("Notifications sheet ยังไม่มีคอลัมน์สำคัญครบ")
            return
    view = noti[(noti["TargetApp"] == "branch") & ((noti["TargetBranch"] == branch) | (noti["TargetBranch"] == ""))].copy()
    if view.empty:
        st.info("ยังไม่มีการแจ้งเตือนสำหรับสาขานี้")
        return
    if "CreatedAt" in view.columns:
        try:
            view["__ts"] = pd.to_datetime(view["CreatedAt"], errors="coerce")
            view = view.sort_values("__ts", ascending=False)
        except Exception:
            pass
    st.dataframe(view[[c for c in view.columns if c != "__ts"]], use_container_width=True)
    mark = st.button("ทำเครื่องหมายว่าอ่านแล้วทั้งหมด")
    if mark:
        ws = st.session_state["ws_noti"]
        header = ws.row_values(1)
        id_idx = header.index("NotiID") + 1 if "NotiID" in header else None
        read_idx = header.index("ReadFlag") + 1 if "ReadFlag" in header else None
        readat_idx = header.index("ReadAt") + 1 if "ReadAt" in header else None
        if id_idx and read_idx:
            # iterate dataframe rows to mark as read
            for _, r in view.iterrows():
                # find row index in sheet by NotiID (simple linear scan)
                allvals = ws.get_all_values()
                hdr = allvals[0]
                rows = allvals[1:]
                nid_pos = hdr.index("NotiID") if "NotiID" in hdr else None
                if nid_pos is None: break
                for i, row in enumerate(rows, start=2):
                    if len(row) > nid_pos and row[nid_pos] == r.get("NotiID",""):
                        ws.update_cell(i, read_idx, "Y")
                        if readat_idx:
                            ws.update_cell(i, readat_idx, datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"))
                        break
        st.cache_data.clear()
        st.success("อัปเดตการอ่านเรียบร้อยแล้ว")
        st.rerun()

def page_stock():
    st.header("📦 คลังสำหรับสาขา")
    items = load_items_df().copy()
    if items.empty:
        st.info("ยังไม่มีข้อมูลอุปกรณ์ใน Items")
        return
    mode = get_branch_visible_mode()
    c_code = _find_col(items, ITEM_CODE_HEADERS)
    c_name = _find_col(items, ITEM_NAME_HEADERS)
    c_qty = _find_col(items, ITEM_QTY_HEADERS)
    c_ready = _find_col(items, ITEM_READY_HEADERS)
    if not all([c_code, c_name, c_qty]):
        st.error("Items sheet ต้องมีคอลัมน์ รหัส/ชื่อ/คงเหลือ อย่างน้อย")
        return
    df = items[[c_code, c_name, c_qty] + ([c_ready] if c_ready else [])].copy()
    df.rename(columns={c_code: "รหัส", c_name: "ชื่อ", c_qty: "คงเหลือ"}, inplace=True)
    if c_ready:
        df.rename(columns={c_ready: "พร้อมให้เบิก"}, inplace=True)
    else:
        df["พร้อมให้เบิก"] = ""
    if mode == VISIBLE_AVAILABLE_ONLY:
        df = df[(df["พร้อมให้เบิก"].str.upper() == "Y") | (df["พร้อมให้เบิก"].str.upper() == "YES")]
    else:
        df["สถานะ"] = df["พร้อมให้เบิก"].apply(lambda x: "✅ พร้อม" if str(x).upper() in {"Y","YES","TRUE","1"} else "🚫 ไม่พร้อม")
    st.dataframe(df, use_container_width=True)

def page_create_request():
    st.header("🧾 สร้างคำขอเบิกอุปกรณ์")
    items = load_items_df().copy()
    if items.empty:
        st.info("ยังไม่มีข้อมูลอุปกรณ์ใน Items")
        return
    c_code = _find_col(items, ITEM_CODE_HEADERS)
    c_name = _find_col(items, ITEM_NAME_HEADERS)
    c_qty = _find_col(items, ITEM_QTY_HEADERS)
    c_ready = _find_col(items, ITEM_READY_HEADERS)
    if not all([c_code, c_name, c_qty]):
        st.error("Items sheet ต้องมีคอลัมน์ รหัส/ชื่อ/คงเหลือ อย่างน้อย")
        return
    df_view = items[[c_code, c_name, c_qty] + ([c_ready] if c_ready else [])].copy()
    df_view.columns = ["code","name","qty"] + (["ready"] if c_ready else [])
    def build_label(r):
        try:
            q = int(float(r["qty"]))
        except Exception:
            q = r["qty"]
        return f"{r['code']} | {r['name']} (คงเหลือ {q})"
    df_view["label"] = df_view.apply(build_label, axis=1)
    options = df_view["label"].tolist()
    st.write("เพิ่มรายการที่ต้องการเบิก (เลือกอุปกรณ์ + ระบุจำนวน)")
    init_rows = [{"อุปกรณ์": "", "จำนวน": 1, "หมายเหตุ": ""} for _ in range(5)]
    edited = st.data_editor(pd.DataFrame(init_rows), num_rows="dynamic", use_container_width=True,
                            column_config={
                                "อุปกรณ์": st.column_config.SelectboxColumn(options=options, required=False),
                                "จำนวน": st.column_config.NumberColumn(min_value=1, step=1),
                                "หมายเหตุ": st.column_config.TextColumn(),
                            }, key="request_editor")
    requester = st.text_input("ผู้ขอเบิก (ชื่อผู้ติดต่อ)")
    note_header = st.text_area("หมายเหตุ (สำหรับคำขอนี้ โดยรวม)", "")
    if st.button("ส่งคำขอ", use_container_width=True, type="primary"):
        rows = edited.dropna(how="all")
        rows = rows[rows["อุปกรณ์"].astype(str).str.strip() != ""]
        if rows.empty:
            st.warning("กรุณาเพิ่มอย่างน้อย 1 รายการ")
            return
        branch = st.session_state["user"]["branch"]
        req_no = make_req_no(branch)
        now = _now_str()
        to_append = []
        for _, r in rows.iterrows():
            label = r["อุปกรณ์"]
            qty = int(r.get("จำนวน", 1))
            item = df_view[df_view["label"] == label].head(1)
            if item.empty:
                continue
            code = str(item.iloc[0]["code"]); name = str(item.iloc[0]["name"])
            row = [req_no, now, branch, requester, code, name, qty,
                   STATUS_PENDING, "", now, str(r.get("หมายเหตุ","")), "N", "N"]
            to_append.append(row)
        if not to_append:
            st.warning("ไม่พบรายการที่ถูกต้อง")
            return
        ws_req = st.session_state["ws_requests"]; ws_req.append_rows(to_append, value_input_option="USER_ENTERED")
        ws_n = st.session_state["ws_noti"]
        ws_n.append_row([make_noti_id(), now, "main", "", NOTI_REQ_CREATED, req_no,
                         f"มีคำขอเบิกใหม่จากสาขา {branch}: {req_no}", "N", ""], value_input_option="USER_ENTERED")
        st.cache_data.clear(); st.success(f"ส่งคำขอเรียบร้อยแล้ว (เลขที่ {req_no})"); st.rerun()

def page_my_requests():
    st.header("📮 คำขอของฉัน")
    req = load_requests_df().copy()
    if req.empty:
        st.info("ยังไม่มีคำขอ"); return
    branch = st.session_state["user"]["branch"]
    if "Branch" not in req.columns:
        st.warning("Requests sheet ไม่มีคอลัมน์ Branch"); return
    view = req[req["Branch"] == branch].copy()
    if view.empty:
        st.info("ยังไม่มีคำขอสำหรับสาขานี้"); return
    if "CreatedAt" in view.columns:
        try:
            view["__ts"] = pd.to_datetime(view["CreatedAt"], errors="coerce")
            view = view.sort_values(["ReqNo","__ts"], ascending=[False, False])
        except Exception:
            pass
    st.dataframe(view[[c for c in view.columns if c != "__ts"]], use_container_width=True)
    st.write("---"); st.subheader("ยืนยันการได้รับอุปกรณ์ (สำหรับคำขอที่สถานะ 'เบิกอุปกรณ์แล้ว')")
    reqno = st.text_input("ระบุเลขที่คำขอ (ReqNo) ที่ต้องการยืนยันได้รับ")
    if st.button("ยืนยันได้รับแล้ว"):
        if not reqno.strip():
            st.warning("กรุณาระบุเลขที่คำขอ"); return
        ws = st.session_state["ws_requests"]
        df = load_requests_df(); hits = df[df["ReqNo"] == reqno]
        if hits.empty:
            st.error("ไม่พบเลขที่คำขอ"); return
        header = ws.row_values(1); colidx = {h: i+1 for i,h in enumerate(header)}
        for idx in hits.index:
            sheet_row = idx + 2
            if "Status" in colidx: ws.update_cell(sheet_row, colidx["Status"], STATUS_RECEIVED)
            if "LastUpdate" in colidx: ws.update_cell(sheet_row, colidx["LastUpdate"], _now_str())
            if "NotifiedBranch(Y/N)" in colidx: ws.update_cell(sheet_row, colidx["NotifiedBranch(Y/N)"], "Y")
        st.cache_data.clear(); st.success("อัปเดตเรียบร้อยแล้ว"); st.rerun()

def get_branch_visible_mode():
    df = load_settings_df()
    if df.empty: return "AVAILABLE_ONLY"
    key_col = "key" if "key" in df.columns else ("Key" if "Key" in df.columns else None)
    val_col = "value" if "value" in df.columns else ("Value" if "Value" in df.columns else None)
    if not key_col or not val_col: return "AVAILABLE_ONLY"
    row = df[df[key_col] == "branch_visible_mode"]
    if row.empty: return "AVAILABLE_ONLY"
    mode = str(row.iloc[0][val_col]).strip().upper()
    if mode not in {"AVAILABLE_ONLY","ALL_WITH_FLAG"}: return "AVAILABLE_ONLY"
    return mode

def make_req_no(branch: str):
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S"); rnd = random.randint(100, 999)
    return f"{branch}-{ts}-{rnd}"

def make_noti_id():
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S"); rnd = random.randint(1000, 9999)
    return f"NOTI-{ts}-{rnd}"

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    if gspread is None or Credentials is None: st.stop()
    if "auth" not in st.session_state: st.session_state["auth"] = False
    if "user" not in st.session_state: st.session_state["user"] = None

    creds = _load_credentials()
    if creds is None: st.stop()
    client = gspread.authorize(creds)
    ss = _open_spreadsheet(client)
    if ss is None: st.stop()

    ws_users = _ensure_worksheet(ss, SHEET_USERS, ["username","password","role","BranchCode"])
    ws_items = _ensure_worksheet(ss, SHEET_ITEMS, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    ws_requests = _ensure_worksheet(ss, SHEET_REQUESTS, [
        "ReqNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty",
        "Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"
    ])
    ws_noti = _ensure_worksheet(ss, SHEET_NOTI, [
        "NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"
    ])
    ws_settings = _ensure_worksheet(ss, SHEET_SETTINGS, ["key","value"])

    st.session_state["ws_users"] = ws_users
    st.session_state["ws_items"] = ws_items
    st.session_state["ws_requests"] = ws_requests
    st.session_state["ws_noti"] = ws_noti
    st.session_state["ws_settings"] = ws_settings

    if not st.session_state["auth"]:
        do_login(); st.stop()
    else:
        u = st.session_state["user"]
        with st.sidebar.expander("บัญชีของฉัน", expanded=True):
            st.write(f"ผู้ใช้: **{u['username']}**")
            st.write(f"บทบาท: **{u['role'] or 'branch'}**")
            st.write(f"สาขา: **{u['branch']}**")
            if st.button("ออกจากระบบ", use_container_width=True):
                st.session_state["auth"] = False; st.session_state["user"] = None
                st.cache_data.clear(); st.rerun()

    tab = st.sidebar.radio("เมนู", ["🔔 การแจ้งเตือน", "🧾 สร้างคำขอ", "📮 คำขอของฉัน", "📦 คลังสำหรับสาขา"], index=0)
    if tab.startswith("🔔"): page_notifications()
    elif tab.startswith("🧾"): page_create_request()
    elif tab.startswith("📮"): page_my_requests()
    else: page_stock()

if __name__ == "__main__":
    main()
