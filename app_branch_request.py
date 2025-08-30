# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — Phase 1 (On-demand + 429-safe, Patched)

คุณสมบัติสำคัญ:
- Login ด้วยชีต Users
- แสดงรายการอุปกรณ์พร้อมให้เบิก (Items) ในตารางเดียว (เลือก + ระบุจำนวน)
- สร้าง Order (OrderNo) และบันทึกลงชีต Requests
- แจ้งเตือนผ่านชีต Notifications (ให้แอปหลักมอนิเตอร์)
- ประวัติคำสั่งเบิก (History) โหลดเมื่อผู้ใช้กดแท็บเท่านั้น
- ลดจำนวนการเรียก API ผ่าน cache + exponential backoff + on-demand loading
"""

import os, json, time, re, random
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st
import gspread
from gspread.exceptions import WorksheetNotFound, APIError

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TZ = timezone(timedelta(hours=7))


# ====================== Utilities ======================
def do_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass


def now_str():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def ensure_headers(ws, headers):
    """ถ้าชีตว่าง ใส่ header ให้, ถ้ามีแล้วและขาดอันไหน เติมท้ายให้"""
    first = ws.row_values(1) or []
    if not first:
        ws.update("A1", [headers])
        return headers
    missing = [h for h in headers if h not in first]
    if missing:
        ws.update("A1", [first + missing])
        first += missing
    return first


def _norm(s: str) -> str:
    s = str(s or "")
    s = s.strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9A-Za-zก-๙]+", "", s)
    return s.lower()


def find_col_fuzzy(df, keywords) -> str | None:
    """จับคู่ชื่อคอลัมน์แบบยืดหยุ่น"""
    if df is None or df.empty:
        return None
    headers = list(df.columns)
    norm = {h: _norm(h) for h in headers}
    kset = {_norm(k) for k in keywords}
    # ตรงตัวก่อน
    for h in headers:
        if norm[h] in kset:
            return h
    # partial match
    for h in headers:
        for k in kset:
            if k and (k in norm[h]):
                return h
    return None


# ====================== Credentials & Spreadsheet ======================
def load_credentials():
    """โหลด Service Account ได้ 4 รูปแบบ: st.secrets[gcp_service_account]/top-level/JSON string/env file"""
    from google.oauth2.service_account import Credentials
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # 1) st.secrets[gcp_service_account]
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        return Credentials.from_service_account_info(info, scopes=scope)

    # 2) top-level secrets
    top = {"type", "project_id", "private_key_id", "private_key", "client_email", "client_id"}
    if top.issubset(set(st.secrets.keys())):
        info = {k: st.secrets[k] for k in top}
        info.setdefault("auth_uri", "https://accounts.google.com/o/oauth2/auth")
        info.setdefault("token_uri", "https://oauth2.googleapis.com/token")
        info.setdefault("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs")
        info.setdefault("client_x509_cert_url", "")
        return Credentials.from_service_account_info(info, scopes=scope)

    # 3) GOOGLE_SERVICE_ACCOUNT_JSON (string)
    raw = (
        st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    )
    if raw:
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            # กันกรณีวางแล้วมี \n แปลก ๆ
            info = json.loads(raw.replace("\n", "\\n"))
        return Credentials.from_service_account_info(info, scopes=scope)

    # 4) GOOGLE_APPLICATION_CREDENTIALS (file path)
    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if p and os.path.exists(p):
        return Credentials.from_service_account_file(p, scopes=scope)

    st.error("ไม่พบ Service Account ใน Secrets/Environment")
    st.stop()


def _extract_sheet_id(id_or_url: str) -> str | None:
    s = (id_or_url or "").strip()
    if not s:
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9\-_]{20,}", s):
        return s
    return None


def open_spreadsheet(client):
    """อ่าน SHEET_ID/SHEET_URL จาก secrets/env; ถ้าไม่ตั้งค่า เปิดช่องให้วาง"""
    raw = (
        st.secrets.get("SHEET_ID", "").strip()
        or st.secrets.get("SHEET_URL", "").strip()
        or os.environ.get("SHEET_ID", "").strip()
        or os.environ.get("SHEET_URL", "").strip()
    )

    def _try_open(sid: str):
        try:
            return client.open_by_key(sid)
        except Exception as e:
            sa = getattr(client.auth, "service_account_email", None)
            with st.expander("รายละเอียดการเชื่อมต่อ / วิธีแก้ (คลิกเพื่อดู)", expanded=True):
                st.error(f"เปิดสเปรดชีตไม่สำเร็จ (ID: {sid})")
                if sa:
                    st.write("Service Account:", f"`{sa}`")
                st.exception(e)
            st.stop()

    sid = _extract_sheet_id(raw) if raw else None
    if sid:
        return _try_open(sid)

    # ยังไม่ตั้งค่า -> ให้ผู้ใช้วาง URL/ID
    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์หรือ Spreadsheet ID")
    inp = st.text_input("URL หรือ Spreadsheet ID", value=st.session_state.get("input_sheet_url", ""))
    if st.button("เชื่อมต่อชีต", type="primary"):
        sid2 = _extract_sheet_id(inp)
        if not sid2:
            st.warning("รูปแบบไม่ถูกต้อง"); st.stop()
        st.session_state["input_sheet_url"] = inp.strip()
        return _try_open(sid2)
    st.stop()


# ====================== 429 helpers ======================
def _is_429(e: Exception) -> bool:
    msg = str(e)
    if "429" in msg and "Quota exceeded" in msg:
        return True
    try:
        code = getattr(getattr(e, "response", None), "status_code", None)
        return code == 429
    except Exception:
        return False


def with_retry(func, *args, announce=False, **kwargs):
    """
    เรียก gspread function พร้อม Exponential backoff เมื่อเจอ 429
    - announce=True จะขึ้นข้อความแจ้งเตือน 'ครั้งแรก' เท่านั้น (ไม่สแปม)
    """
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if _is_429(e) and attempt < 5:
                wait = (2 ** attempt) + random.uniform(0, 0.5)
                if announce and not st.session_state.get("_quota_msg_shown"):
                    st.session_state["_quota_msg_shown"] = True
                    st.info(f"โควต้าอ่าน Google Sheets เกินชั่วคราว กำลังรอ {wait:.1f}s แล้วลองใหม่…")
                time.sleep(wait)
                attempt += 1
                continue
            raise


# ====================== Cached connectors/readers ======================
@st.cache_resource(show_spinner=False)
def get_client_and_ss():
    creds = load_credentials()
    client = gspread.authorize(creds)
    ss = open_spreadsheet(client)
    return client, ss


@st.cache_data(ttl=300, show_spinner=False)
def get_worksheets_map() -> dict:
    """อ่าน metadata แค่ครั้งเดียว แล้วแคช 5 นาที: {title: sheetId}"""
    _, ss = get_client_and_ss()
    lst = with_retry(ss.worksheets)   # คืนค่ามาเลย (อย่าเรียกซ้ำ)
    return {w.title: w.id for w in lst}


def get_or_create_ws(ss, title: str, rows: int = 1000, cols: int = 26):
    """เปิดแผ่นงานด้วย sheetId ถ้ามี; ถ้าไม่มีให้สร้าง แล้วเคลียร์ mapping"""
    try:
        mp = get_worksheets_map()
        if title in mp:
            return with_retry(ss.get_worksheet_by_id, mp[title])
        ws = with_retry(ss.add_worksheet, title, rows, cols)
        st.cache_data.clear()  # refresh mapping cache
        return ws
    except APIError as e:
        if _is_429(e):
            with st.expander("รายละเอียดการเชื่อมต่อ / ข้อผิดพลาด (คลิกเพื่อดู)", expanded=True):
                st.error(f"เปิดแผ่นงาน '{title}' ไม่สำเร็จ (โควต้าอ่านเกินชั่วคราว)")
                st.exception(e)
            st.stop()
        raise


@st.cache_data(ttl=90, show_spinner=False)
def read_sheet_as_df(sheet_name: str) -> pd.DataFrame:
    """อ่านชีตเป็น DataFrame (cache 90s)"""
    _, ss = get_client_and_ss()
    ws = get_or_create_ws(ss, sheet_name, 1000, 26)
    vals = with_retry(ws.get_all_values, announce=True)
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()


@st.cache_data(ttl=90, show_spinner=False)
def read_requests_df() -> pd.DataFrame:
    """อ่านชีต Requests (on-demand)"""
    _, ss = get_client_and_ss()
    ws = get_or_create_ws(ss, "Requests", 2000, 26)
    vals = with_retry(ws.get_all_values, announce=True)
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()


# ====================== App ======================
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    client, ss = get_client_and_ss()
    try:
        st.caption(f"Service Account: `{client.auth.service_account_email}`")
    except Exception:
        pass

    # ---------- Login ----------
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
        st.session_state["user"] = {}

    if not st.session_state["auth"]:
        u = st.sidebar.text_input("ชื่อผู้ใช้")
        p = st.sidebar.text_input("รหัสผ่าน", type="password")
        if st.sidebar.button("ล็อกอิน", use_container_width=True):
            dfu = read_sheet_as_df("Users")
            if dfu.empty:
                st.sidebar.error("ไม่มีผู้ใช้ในชีต Users"); st.stop()

            cu = find_col_fuzzy(dfu, {"username", "user", "บัญชีผู้ใช้", "ชื่อผู้ใช้"})
            cp = find_col_fuzzy(dfu, {"password", "รหัสผ่าน"})
            cb = find_col_fuzzy(dfu, {"BranchCode", "สาขา", "branch"})
            if not (cu and cp and cb):
                st.sidebar.error("Users sheet ไม่ครบคอลัมน์"); st.stop()

            for c in (cu, cp, cb):
                dfu[c] = dfu[c].astype(str).str.strip()

            row = dfu[dfu[cu].str.casefold() == (u or "").strip().casefold()].head(1)
            if row.empty or str(row.iloc[0][cp]).strip() != (p or "").strip():
                st.sidebar.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
            else:
                st.session_state["auth"] = True
                st.session_state["user"] = {
                    "username": (u or "").strip(),
                    "branch": str(row.iloc[0][cb]).strip(),
                }
                st.sidebar.success(f"ยินดีต้อนรับ {st.session_state['user']['username']}")
                time.sleep(0.5); do_rerun()
        st.stop()

    if st.sidebar.button("ออกจากระบบ"):
        st.session_state["auth"] = False
        st.session_state["user"] = {}
        do_rerun()

    branch_code = st.session_state["user"]["branch"]
    username = st.session_state["user"]["username"]

    # ---------- Tabs ----------
    tab_req, tab_hist = st.tabs(["🧺 เบิกอุปกรณ์", "🧾 ประวัติคำสั่งเบิก"])

    # ===== Tab: เบิกอุปกรณ์ =====
    with tab_req:
        st.header("📦 รายการอุปกรณ์ที่พร้อมให้เบิก", anchor=False)

        # โหลดเฉพาะ Items
        dfi = read_sheet_as_df("Items")
        if dfi.empty:
            st.info("ยังไม่มีข้อมูลใน Items"); st.stop()

        # จับคอลัมน์สำคัญ
        c_code = find_col_fuzzy(dfi, {"รหัส", "itemcode", "code", "sku", "part", "partno", "partnumber"})
        if not c_code:
            st.error("Items: หา 'รหัส' ไม่พบ")
            st.stop()

        # ชื่ออุปกรณ์ (เลือกคอลัมน์ที่มีข้อมูลมากที่สุด)
        name_candidates = []
        for keys in [
            {"ชื่ออุปกรณ์", "ชื่อสินค้า", "itemname", "productname"},
            {"ชื่อ", "name", "รายการ", "description", "desc"},
        ]:
            c = find_col_fuzzy(dfi, keys)
            if c:
                name_candidates.append(c)
        if name_candidates:
            c_name = max(name_candidates, key=lambda c: dfi[c].astype(str).str.strip().ne("").sum())
        else:
            others = [c for c in dfi.columns if c != c_code]
            c_name = others[0] if others else None

        name_display = dfi[c_name].astype(str).str.strip() if c_name else pd.Series([""] * len(dfi))

        c_qty = find_col_fuzzy(dfi, {"คงเหลือ", "qty", "จำนวน", "stock", "balance", "remaining", "remain", "จำนวนคงเหลือ"})
        c_ready = find_col_fuzzy(
            dfi,
            {
                "พร้อมให้เบิก",
                "พร้อมให้เบิก(y/n)",
                "ready",
                "available",
                "ให้เบิก",
                "allow",
                "เปิดให้เบิก",
                "ใช้งาน",
                "สถานะ",
                "active",
                "enabled",
                "availableflag",
            },
        )

        if c_ready:
            ready_mask = dfi[c_ready].astype(str).str.upper().str.strip().isin(["Y", "YES", "TRUE", "1"])
        elif c_qty:
            ready_mask = pd.to_numeric(dfi[c_qty], errors="coerce").fillna(0) > 0
        else:
            ready_mask = pd.Series([True] * len(dfi))

        ready_df = dfi[ready_mask].copy()
        name_ready = name_display[ready_mask].copy()

        if ready_df.empty:
            st.warning("ยังไม่มีอุปกรณ์ที่พร้อมให้เบิก")
            st.stop()

        base_df = pd.DataFrame(
            {
                "รหัส": ready_df[c_code].astype(str).values,
                "ชื่อ": name_ready.replace("", "(ไม่มีชื่อ)").values,
                "เลือก": [False] * len(ready_df),
                "จำนวนที่ต้องการ": [0] * len(ready_df),
            }
        )

        if "order_table" not in st.session_state or st.session_state.get("order_table_shape") != base_df.shape:
            st.session_state["order_table"] = base_df.copy()
            st.session_state["order_table_shape"] = base_df.shape

        edited = st.data_editor(
            st.session_state["order_table"],
            num_rows="fixed",
            key="order_editor",
            use_container_width=True,
            column_config={
                "เลือก": st.column_config.CheckboxColumn("เลือก"),
                "จำนวนที่ต้องการ": st.column_config.NumberColumn("จำนวนที่ต้องการ", min_value=1, step=1),
            },
            hide_index=True,
        )
        st.session_state["order_table"] = edited

        col1, col2 = st.columns([1, 1])
        submit = col1.button("✅ เบิกอุปกรณ์", type="primary", use_container_width=True)
        clear = col2.button("🧹 ล้างข้อมูล", use_container_width=True)

        if clear:
            st.session_state.pop("order_table", None)
            st.session_state.pop("order_table_shape", None)
            st.success("ล้างการเลือกแล้ว")
            time.sleep(0.3); do_rerun()

        # ----- กด "เบิกอุปกรณ์" -----
        if submit:
            sel = edited[
                (edited["เลือก"] == True)
                & (pd.to_numeric(edited["จำนวนที่ต้องการ"], errors="coerce").fillna(0) > 0)
            ].copy()

            if sel.empty:
                st.warning("กรุณาเลือกอุปกรณ์อย่างน้อย 1 รายการ และระบุจำนวน")
                st.stop()

            # เปิดเฉพาะตอนจะเขียน เพื่อลด request
            ws_reqs = get_or_create_ws(ss, "Requests", 2000, 26)
            ws_noti = get_or_create_ws(ss, "Notifications", 2000, 26)

            ensure_headers(
                ws_reqs,
                [
                    "ReqNo",
                    "OrderNo",
                    "CreatedAt",
                    "Branch",
                    "Requester",
                    "ItemCode",
                    "ItemName",
                    "Qty",
                    "Status",
                    "Approver",
                    "LastUpdate",
                    "Note",
                    "NotifiedMain(Y/N)",
                    "NotifiedBranch(Y/N)",
                ],
            )
            ensure_headers(
                ws_noti,
                ["NotiID", "CreatedAt", "TargetApp", "TargetBranch", "Type", "RefID", "Message", "ReadFlag", "ReadAt"],
            )

            order_no = f"ORD-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
            ts = now_str()

            for _, r in sel.iterrows():
                req_no = f"REQ-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
                row = [
                    req_no,
                    order_no,
                    ts,
                    branch_code,
                    username,
                    r["รหัส"],
                    r["ชื่อ"],
                    str(int(r["จำนวนที่ต้องการ"])),
                    "pending",
                    "",
                    ts,
                    "",
                    "N",
                    "N",
                ]
                with_retry(ws_reqs.append_row, row, value_input_option="USER_ENTERED", announce=True)

            n_headers = with_retry(ws_noti.row_values, 1, announce=True)
            noti = {
                "NotiID": f"NOTI-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}",
                "CreatedAt": ts,
                "TargetApp": "main_app",
                "TargetBranch": branch_code,
                "Type": "ORDER_CREATED",
                "RefID": order_no,
                "Message": f"{branch_code} สร้างคำสั่งเบิก {order_no} จำนวน {len(sel)} รายการ โดย {username}",
                "ReadFlag": "N",
                "ReadAt": "",
            }
            with_retry(
                ws_noti.append_row,
                [noti.get(h, "") for h in n_headers],
                value_input_option="USER_ENTERED",
                announce=True,
            )

            # refresh cache & UI
            st.cache_data.clear()
            with st.success(f"สร้างคำสั่งเบิกสำเร็จ: **{order_no}**"):
                st.write("สรุปรายการในออร์เดอร์:")
                st.dataframe(
                    sel[["รหัส", "ชื่อ", "จำนวนที่ต้องการ"]].rename(columns={"จำนวนที่ต้องการ": "Qty"}),
                    use_container_width=True,
                )

            st.session_state.pop("order_table", None)
            st.session_state.pop("order_table_shape", None)

    # ===== Tab: ประวัติคำสั่งเบิก =====
    with tab_hist:
        st.header("🧾 ประวัติคำสั่งเบิก", anchor=False)
        dfr = read_requests_df()  # โหลดเมื่อผู้ใช้กดแท็บเท่านั้น
        if not dfr.empty:
            c_branch = find_col_fuzzy(dfr, {"Branch"})
            c_user = find_col_fuzzy(dfr, {"Requester"})
            c_order = find_col_fuzzy(dfr, {"OrderNo"})
            c_code2 = find_col_fuzzy(dfr, {"ItemCode", "รหัส"})
            c_name2 = find_col_fuzzy(dfr, {"ItemName", "ชื่อ"})
            c_qty2 = find_col_fuzzy(dfr, {"Qty", "จำนวน"})
            c_status = find_col_fuzzy(dfr, {"Status", "สถานะ"})
            c_created = find_col_fuzzy(dfr, {"CreatedAt"})

            if c_order and c_branch and c_user:
                my = dfr[(dfr[c_branch] == branch_code) & (dfr[c_user] == username)].copy()
                if not my.empty:
                    orders = my[c_order].dropna().unique().tolist()
                    orders = sorted(orders, reverse=True)
                    ord_sel = st.selectbox("เลือกออร์เดอร์", orders)
                    sub = my[my[c_order] == ord_sel].copy()
                    if c_created:
                        sub = sub.sort_values(c_created)
                    show_cols = [c_code2, c_name2, c_qty2, c_status]
                    show_cols = [c for c in show_cols if c]
                    st.dataframe(
                        sub[show_cols].rename(
                            columns={c_code2: "รหัส", c_name2: "ชื่อ", c_qty2: "Qty", c_status: "สถานะ"}
                        ),
                        use_container_width=True,
                        height=260,
                    )
                else:
                    st.info("ยังไม่มีคำสั่งเบิกของคุณ")
            else:
                st.info("Requests sheet ยังไม่มีคอลัมน์ OrderNo/Branch/Requester ครบถ้วน")
        else:
            st.info("ยังไม่มีข้อมูลคำสั่งเบิก")


if __name__ == "__main__":
    main()
