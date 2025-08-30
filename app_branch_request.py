# -*- coding: utf-8 -*-
"""
Branch Portal (Streamlit + Google Sheets) — Phase 1
- ใช้ชีตเดียวกับแอปเดิม (อ่าน Items/Users) + เพิ่มชีต Requests/Notifications/Settings (ถ้ายังไม่มี)
- ฟีเจอร์: ล็อกอิน (Role=branch), ดูสต็อกที่อนุญาต, สร้างคำขอเบิก, ติดตามคำขอ, ศูนย์แจ้งเตือน
- การแจ้งเตือน: เขียน Notifications ไป TargetApp="main" เมื่อมีคำขอใหม่
- ตั้งค่าโหมดการมองเห็น: อ่านจาก Settings.branch_visible_mode (AVAILABLE_ONLY | ALL_WITH_FLAG)
- ปลอดภัยต่อแอปเดิม (ไม่แตะโค้ดเดิม/สคีมาเดิมนอกจาก "เติม" ชีต/คอลัมน์ใหม่)
"""
import os
import io
import json
import time
import random
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

# การเชื่อมต่อ Google Sheets
# - ใช้ gspread + google-auth
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception as e:
    st.warning("ไม่พบไลบรารี gspread / google-auth (โปรด pip install gspread google-auth)")
    gspread = None
    Credentials = None

# -------------------------- ค่าคงที่ / ตั้งค่า --------------------------
APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TIMEZONE = timezone(timedelta(hours=7))  # Asia/Bangkok

# ---- ใส่บล็อกนี้ก่อน "# อ่านค่า ENV" ----
try:
    for k, v in st.secrets.items():
        if isinstance(v, (dict, list)):
            os.environ.setdefault(k, json.dumps(v))
        else:
            os.environ.setdefault(k, str(v))
except Exception:
    pass
# -------------------------------------------

# อ่านค่า ENV
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SHEET_URL = os.environ.get("SHEET_URL", "").strip()
SERVICE_ACCOUNT_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# ชื่อชีตหลัก (ของเดิม + ของใหม่)
SHEET_USERS = os.environ.get("SHEET_USERS", "Users")
SHEET_ITEMS = os.environ.get("SHEET_ITEMS", "Items")
SHEET_REQUESTS = os.environ.get("SHEET_REQUESTS", "Requests")
SHEET_NOTI = os.environ.get("SHEET_NOTIFICATIONS", "Notifications")
SHEET_SETTINGS = os.environ.get("SHEET_SETTINGS", "Settings")

# ค่า setting: โหมดการมองเห็นสต็อกของฝั่งสาขา
VISIBLE_AVAILABLE_ONLY = "AVAILABLE_ONLY"
VISIBLE_ALL_WITH_FLAG = "ALL_WITH_FLAG"

# สถานะคำขอ
STATUS_PENDING = "PENDING"     # ส่งคำขอแล้ว รอแอปเดิมอนุมัติ/เบิกอุปกรณ์
STATUS_ISSUED  = "ISSUED"      # แอปเดิมอนุมัติและเบิกอุปกรณ์แล้ว (ข้อความที่ผู้ใช้เห็น: 'เบิกอุปกรณ์แล้ว')
STATUS_RECEIVED = "RECEIVED"   # สาขายืนยันได้รับของแล้ว

# ชนิดการแจ้งเตือน
NOTI_REQ_CREATED = "REQ_CREATED"
NOTI_ITEM_ISSUED = "ITEM_ISSUED"  # ฝั่ง main เบิกอุปกรณ์แล้ว

# -------------------------- Utilities --------------------------
def _now_str():
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

def _load_credentials():
    """
    โหลด service account credentials ได้ 3 วิธี:
    1) GOOGLE_SERVICE_ACCOUNT_JSON (JSON string)
    2) GOOGLE_APPLICATION_CREDENTIALS (path)
    3) ./service_account.json (ในโฟลเดอร์เดียวกัน)
    """
    info = None
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
        except Exception as e:
            st.error("GOOGLE_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง")
            return None

    if info:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        return Credentials.from_service_account_info(info, scopes=scope)

    # path mode
    path_candidates = []
    if SERVICE_ACCOUNT_PATH:
        path_candidates.append(SERVICE_ACCOUNT_PATH)
    path_candidates.append("service_account.json")

    for p in path_candidates:
        if os.path.exists(p):
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
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

    # ตรวจว่าแถวแรกมีหัวครบไหม ถ้าไม่ครบให้เติมหัวท้าย
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if not first:
        ws.update("A1", [headers])
    else:
        # เติมหัวที่ขาด (เรียงต่อท้าย)
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
    """rows: List[List[str]]"""
    if not rows:
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def _update_cell(ws, row, col, value):
    ws.update_cell(row, col, value)

def _update_row_by_id(df, ws, id_col, id_val, updates: dict):
    # หา index ของคอลัมน์
    header = ws.row_values(1)
    col_idx = {h: i+1 for i, h in enumerate(header)}  # 1-based
    # หาแถว
    if id_col not in df.columns:
        return False
    hits = df.index[df[id_col] == id_val].tolist()
    if not hits:
        return False
    # เลือกแถวแรกที่พบ (ควร unique)
    idx = hits[0]
    # แถวในชีต = idx + 2 (เพราะมี header)
    sheet_row = idx + 2
    # อัปเดตแต่ละคอลัมน์
    for k, v in updates.items():
        if k in col_idx:
            _update_cell(ws, sheet_row, col_idx[k], v)
    return True

# -------------------------- Header mapping --------------------------
ITEM_CODE_HEADERS = {"รหัส", "รหัสวัสดุ", "ItemCode", "Code", "รหัสอุปกรณ์"}
ITEM_NAME_HEADERS = {"ชื่อ", "รายการ", "ItemName", "Name", "ชื่ออุปกรณ์"}
ITEM_QTY_HEADERS  = {"คงเหลือ", "จำนวนคงเหลือ", "Stock", "Qty", "จำนวน"}
ITEM_READY_HEADERS = {"พร้อมให้เบิก", "พร้อมให้เบิก(Y/N)", "Available", "Ready", "พร้อม"}

USER_NAME_HEADERS = {"username", "user", "บัญชีผู้ใช้", "ผู้ใช้"}
USER_PASS_HEADERS = {"password", "รหัสผ่าน"}
USER_ROLE_HEADERS = {"role", "บทบาท"}
USER_BRANCH_HEADERS = {"branch", "สาขา", "branchcode", "BranchCode"}

def _find_col(df: pd.DataFrame, candidates: set):
    for c in df.columns:
        if c.strip() in candidates:
            return c
        if c.strip().lower() in {x.lower() for x in candidates}:
            return c
    return None

# -------------------------- Cache layers --------------------------
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

# -------------------------- Auth --------------------------
def verify_password(plain: str, hashed: str) -> bool:
    """
    รองรับทั้ง bcrypt และ plain-text (เพื่อให้ใช้กับข้อมูลเดิมได้)
    """
    if not hashed:
        return False
    try:
        import bcrypt  # type: ignore
        if hashed.startswith("$2"):  # รูปแบบ bcrypt
            return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        pass
    # fallback: เทียบตรง ๆ (ไม่แนะนำในโปรดักชัน)
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
        st.session_state["user"] = {
            "username": username,
            "role": role or "branch",
            "branch": branch,
        }
        st.success(f"ยินดีต้อนรับ {username} (สาขา {branch})")

def ensure_session():
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
    if "user" not in st.session_state:
        st.session_state["user"] = None

# -------------------------- App Pages --------------------------
def page_notifications():
    st.header("🔔 การแจ้งเตือน")
    noti = load_notifications_df().copy()
    if noti.empty:
        st.info("ยังไม่มีการแจ้งเตือน")
        return

    # ฟิลเตอร์เฉพาะของสาขานี้
    branch = st.session_state["user"]["branch"]
    cols = list(noti.columns)
    # ensure columns
    for c in ["TargetApp", "TargetBranch", "ReadFlag"]:
        if c not in cols:
            st.warning("Notifications sheet ยังไม่มีคอลัมน์สำคัญครบ")
            return

    view = noti[(noti["TargetApp"] == "branch") & ((noti["TargetBranch"] == branch) | (noti["TargetBranch"] == ""))].copy()
    if view.empty:
        st.info("ยังไม่มีการแจ้งเตือนสำหรับสาขานี้")
        return

    # จัดเรียงล่าสุดก่อน
    if "CreatedAt" in view.columns:
        try:
            view["__ts"] = pd.to_datetime(view["CreatedAt"], errors="coerce")
            view = view.sort_values("__ts", ascending=False)
        except Exception:
            pass

    # แสดง
    st.dataframe(view[[c for c in view.columns if c != "__ts"]], use_container_width=True)

    # ทำเครื่องหมายว่าอ่านทั้งหมด
    mark = st.button("ทำเครื่องหมายว่าอ่านแล้วทั้งหมด")
    if mark:
        ws = st.session_state["ws_noti"]
        for idx, r in view.iterrows():
            if r.get("ReadFlag", "N") != "Y":
                # อัปเดต ReadFlag = Y, ReadAt = now
                _update_row_by_id(noti, ws, "NotiID", r.get("NotiID", ""), {
                    "ReadFlag": "Y",
                    "ReadAt": _now_str(),
                })
        st.cache_data.clear()
        st.success("อัปเดตการอ่านเรียบร้อยแล้ว")
        st.rerun()

def page_stock():
    st.header("📦 คลังสำหรับสาขา")
    items = load_items_df().copy()
    if items.empty:
        st.info("ยังไม่มีข้อมูลอุปกรณ์ใน Items")
        return

    # อ่านโหมดการมองเห็นจาก Settings
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

    # กรองตามโหมด
    if mode == VISIBLE_AVAILABLE_ONLY:
        df = df[(df["พร้อมให้เบิก"].str.upper() == "Y") | (df["พร้อมให้เบิก"].str.upper() == "YES")]
    else:
        # แสดงทั้งหมด แต่ทำแสดงไอคอน/สีในคอลัมน์ใหม่
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

    # สร้าง label สำหรับเลือก
    df_view = items[[c_code, c_name, c_qty] + ([c_ready] if c_ready else [])].copy()
    df_view.columns = ["code","name","qty"] + (["ready"] if c_ready else [])
    def build_label(r):
        qty = r["qty"]
        try:
            qty = int(float(qty))
        except Exception:
            pass
        return f"{r['code']} | {r['name']} (คงเหลือ {qty})"
    df_view["label"] = df_view.apply(build_label, axis=1)
    options = df_view["label"].tolist()

    st.write("เพิ่มรายการที่ต้องการเบิก (เลือกอุปกรณ์ + ระบุจำนวน)")
    # เริ่มต้นด้วยตารางว่าง 5 แถว
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

        # สร้าง ReqNo
        branch = st.session_state["user"]["branch"]
        req_no = make_req_no(branch)

        # เตรียมบรรทัดลง Requests
        now = _now_str()
        to_append = []
        for _, r in rows.iterrows():
            label = r["อุปกรณ์"]
            qty = int(r.get("จำนวน", 1))
            item = df_view[df_view["label"] == label].head(1)
            if item.empty:
                continue
            code = str(item.iloc[0]["code"])
            name = str(item.iloc[0]["name"])
            row = [
                req_no, now, branch, requester, code, name, qty,
                STATUS_PENDING, "", now, str(r.get("หมายเหตุ","")),
                "N", "N"  # NotifiedMain, NotifiedBranch
            ]
            to_append.append(row)

        if not to_append:
            st.warning("ไม่พบรายการที่ถูกต้อง")
            return

        # เขียนลง Requests + สร้าง Notification สำหรับ main
        ws_req = st.session_state["ws_requests"]
        _append_rows(ws_req, to_append)

        ws_noti = st.session_state["ws_noti"]
        noti_row = [
            make_noti_id(), now, "main", "", NOTI_REQ_CREATED, req_no,
            f"มีคำขอเบิกใหม่จากสาขา {branch}: {req_no}", "N", ""
        ]
        _append_rows(ws_noti, [noti_row])

        # เคลียร์แคช
        st.cache_data.clear()
        st.success(f"ส่งคำขอเรียบร้อยแล้ว (เลขที่ {req_no})")
        st.rerun()

def page_my_requests():
    st.header("📮 คำขอของฉัน")
    req = load_requests_df().copy()
    if req.empty:
        st.info("ยังไม่มีคำขอ")
        return

    branch = st.session_state["user"]["branch"]
    if "Branch" not in req.columns:
        st.warning("Requests sheet ไม่มีคอลัมน์ Branch")
        return

    view = req[req["Branch"] == branch].copy()
    if view.empty:
        st.info("ยังไม่มีคำขอสำหรับสาขานี้")
        return

    # จัดเรียงล่าสุด
    if "CreatedAt" in view.columns:
        try:
            view["__ts"] = pd.to_datetime(view["CreatedAt"], errors="coerce")
            view = view.sort_values(["ReqNo","__ts"], ascending=[False, False])
        except Exception:
            pass

    st.dataframe(view[[c for c in view.columns if c != "__ts"]], use_container_width=True)

    # ยืนยันได้รับของ (สำหรับคำขอที่ ISSUED)
    st.write("---")
    st.subheader("ยืนยันการได้รับอุปกรณ์ (สำหรับคำขอที่สถานะ 'เบิกอุปกรณ์แล้ว')")
    reqno = st.text_input("ระบุเลขที่คำขอ (ReqNo) ที่ต้องการยืนยันได้รับ")
    if st.button("ยืนยันได้รับแล้ว"):
        if not reqno.strip():
            st.warning("กรุณาระบุเลขที่คำขอ")
            return
        ws = st.session_state["ws_requests"]
        df = load_requests_df()
        hits = df[df["ReqNo"] == reqno]
        if hits.empty:
            st.error("ไม่พบเลขที่คำขอ")
            return

        # อัปเดตทุกแถวของ ReqNo เดียวกัน → Status = RECEIVED, LastUpdate, NotifiedBranch = Y
        ws_header = ws.row_values(1)
        colidx = {h: i+1 for i,h in enumerate(ws_header)}
        updates = []
        for idx in hits.index:
            sheet_row = idx + 2
            if "Status" in colidx:
                ws.update_cell(sheet_row, colidx["Status"], STATUS_RECEIVED)
            if "LastUpdate" in colidx:
                ws.update_cell(sheet_row, colidx["LastUpdate"], _now_str())
            if "NotifiedBranch(Y/N)" in colidx:
                ws.update_cell(sheet_row, colidx["NotifiedBranch(Y/N)"], "Y")

        st.cache_data.clear()
        st.success("อัปเดตเรียบร้อยแล้ว")
        st.rerun()

# -------------------------- Helpers --------------------------
def get_branch_visible_mode():
    df = load_settings_df()
    if df.empty:
        return VISIBLE_AVAILABLE_ONLY
    key_col = "key" if "key" in df.columns else ("Key" if "Key" in df.columns else None)
    val_col = "value" if "value" in df.columns else ("Value" if "Value" in df.columns else None)
    if not key_col or not val_col:
        return VISIBLE_AVAILABLE_ONLY
    row = df[df[key_col] == "branch_visible_mode"]
    if row.empty:
        return VISIBLE_AVAILABLE_ONLY
    mode = str(row.iloc[0][val_col]).strip().upper()
    if mode not in {VISIBLE_AVAILABLE_ONLY, VISIBLE_ALL_WITH_FLAG}:
        return VISIBLE_AVAILABLE_ONLY
    return mode

def make_req_no(branch: str):
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S")
    rand = random.randint(100, 999)
    # ทำให้สั้นและจำง่าย: BRANCH-YYYYMMDD-HHMMSS-XYZ
    return f"{branch}-{ts}-{rand}"

def make_noti_id():
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S")
    rand = random.randint(1000, 9999)
    return f"NOTI-{ts}-{rand}"

# -------------------------- App Entry --------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    ensure_session()
    # เตรียม client + spreadsheet + worksheets
    if gspread is None or Credentials is None:
        st.stop()

    creds = _load_credentials()
    if creds is None:
        st.stop()

    client = gspread.authorize(creds)
    ss = _open_spreadsheet(client)
    if ss is None:
        st.stop()

    # ตรวจ/สร้างชีตของใหม่ (เติมอย่างเดียว)
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

    # เก็บไว้ใน session
    st.session_state["ws_users"] = ws_users
    st.session_state["ws_items"] = ws_items
    st.session_state["ws_requests"] = ws_requests
    st.session_state["ws_noti"] = ws_noti
    st.session_state["ws_settings"] = ws_settings

    # Sidebar: Login / Profile
    if not st.session_state["auth"]:
        do_login()
        st.stop()
    else:
        u = st.session_state["user"]
        with st.sidebar.expander("บัญชีของฉัน", expanded=True):
            st.write(f"ผู้ใช้: **{u['username']}**")
            st.write(f"บทบาท: **{u['role'] or 'branch'}**")
            st.write(f"สาขา: **{u['branch']}**")
            if st.button("ออกจากระบบ", use_container_width=True):
                st.session_state["auth"] = False
                st.session_state["user"] = None
                st.cache_data.clear()
                st.rerun()

    # เมนูหลัก
    tab = st.sidebar.radio("เมนู", ["🔔 การแจ้งเตือน", "🧾 สร้างคำขอ", "📮 คำขอของฉัน", "📦 คลังสำหรับสาขา"], index=0)
    if tab.startswith("🔔"):
        page_notifications()
    elif tab.startswith("🧾"):
        page_create_request()
    elif tab.startswith("📮"):
        page_my_requests()
    else:
        page_stock()


if __name__ == "__main__":
    main()
