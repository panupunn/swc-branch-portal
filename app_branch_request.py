# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — Phase 1 (Single-table, Cached, Patched)

- ใช้ @st.cache_resource แคช client + spreadsheet
- ใช้ @st.cache_data (TTL 90s) แคชการอ่านชีต; เคลียร์หลังเขียน
- เลิกเรียก ss.worksheets() (แพง/ชนโควตา) → เปิดเฉพาะแผ่นที่ต้องใช้ด้วย try/except
- ตารางเดียว: ติ๊กเลือก + ใส่จำนวน + ปุ่ม “เบิกอุปกรณ์ / ล้างข้อมูล”
- สร้าง OrderNo เดียวสำหรับหลายบรรทัด, บันทึก Requests และ Notifications
"""

import os, json, time, re
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st
import gspread
from gspread.exceptions import WorksheetNotFound

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TZ = timezone(timedelta(hours=7))

# ----------------- Utils -----------------
def do_rerun():
    try: st.rerun()
    except Exception:
        try: st.experimental_rerun()
        except Exception: pass

def now_str():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def ensure_headers(ws, headers):
    first = ws.row_values(1) or []
    if not first:
        ws.update("A1", [headers]); return headers
    missing = [h for h in headers if h not in first]
    if missing:
        ws.update("A1", [first + missing]); first += missing
    return first

def _norm(s: str) -> str:
    s = str(s or "")
    s = s.strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9A-Za-zก-๙]+", "", s)
    return s.lower()

def find_col_fuzzy(df, keywords) -> str | None:
    if df is None or df.empty:
        return None
    headers = list(df.columns)
    norm = {h: _norm(h) for h in headers}
    kset = {_norm(k) for k in keywords}
    for h in headers:
        if norm[h] in kset:
            return h
    for h in headers:
        for k in kset:
            if k and (k in norm[h]):
                return h
    return None

# ----------------- Credentials -----------------
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
        try: info = json.loads(raw)
        except json.JSONDecodeError:
            info = json.loads(raw.replace("\n","").replace("\r",""))
        return Credentials.from_service_account_info(info, scopes=scope)

    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        return Credentials.from_service_account_file(p, scopes=scope)

    st.error("ไม่พบ Service Account ใน Secrets"); st.stop()

def _extract_sheet_id(id_or_url: str) -> str | None:
    s = (id_or_url or "").strip()
    if not s: return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", s)
    if m: return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9\-_]{20,}", s): return s
    return None

def open_spreadsheet(client):
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
                st.write("1) แชร์ไฟล์ให้ Service Account (สิทธิ Editor)")
                if sa: st.write("Service Account:", f"`{sa}`")
                st.write("2) ตรวจ SHEET_URL / SHEET_ID ว่าถูกต้อง")
                st.write("รายละเอียดข้อผิดพลาด (developer):")
                st.exception(e)
            st.stop()

    sid = _extract_sheet_id(raw) if raw else None
    if sid: return _try_open(sid)

    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์หรือ Spreadsheet ID")
    inp = st.text_input("URL หรือ Spreadsheet ID", value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีต", type="primary"):
        sid2 = _extract_sheet_id(inp)
        if not sid2:
            st.warning("รูปแบบไม่ถูกต้อง"); st.stop()
        st.session_state["input_sheet_url"] = inp.strip()
        return _try_open(sid2)
    st.stop()

# ----------------- Cached connectors & readers -----------------
@st.cache_resource(show_spinner=False)
def get_client_and_ss():
    creds = load_credentials()
    client = gspread.authorize(creds)
    ss = open_spreadsheet(client)
    return client, ss

@st.cache_data(ttl=90, show_spinner=False)
def read_sheet_as_df(sheet_name: str) -> pd.DataFrame:
    """อ่านชีตเป็น DataFrame (cache 90s) — ไม่รับ ss เพื่อหลีกเลี่ยง UnhashableParamError"""
    _, ss = get_client_and_ss()
    ws = ss.worksheet(sheet_name)
    vals = ws.get_all_values()
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()

# ----------------- App -----------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    client, ss = get_client_and_ss()
    try:
        st.caption(f"Service Account: `{client.auth.service_account_email}`")
    except Exception:
        pass

    # ---- เปิดเฉพาะชีตที่ต้องใช้ (ไม่ list ทั้งไฟล์) ----
    def get_or_create_ws(_ss, title: str, rows: int = 1000, cols: int = 26):
        try:
            return _ss.worksheet(title)
        except WorksheetNotFound:
            return _ss.add_worksheet(title, rows, cols)
        except Exception as e:
            with st.expander("รายละเอียดการเชื่อมต่อ / ข้อผิดพลาด (คลิกเพื่อดู)", expanded=True):
                st.error(f"เปิดแผ่นงาน '{title}' ไม่สำเร็จ")
                st.exception(e)
            st.stop()

    ws_users = get_or_create_ws(ss, "Users",         1000, 26)
    ws_items = get_or_create_ws(ss, "Items",         2000, 26)
    ws_reqs  = get_or_create_ws(ss, "Requests",      2000, 26)
    ws_noti  = get_or_create_ws(ss, "Notifications", 2000, 26)
    ws_conf  = get_or_create_ws(ss, "Settings",      1000, 26)

    ensure_headers(ws_users, ["username","password","role","BranchCode"])
    ensure_headers(ws_items, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    ensure_headers(ws_reqs,  ["ReqNo","OrderNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty","Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"])
    ensure_headers(ws_noti,  ["NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"])
    ensure_headers(ws_conf,  ["key","value"])

    # -------- Login --------
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False; st.session_state["user"] = {}
    if not st.session_state["auth"]:
        u = st.sidebar.text_input("ชื่อผู้ใช้")
        p = st.sidebar.text_input("รหัสผ่าน", type="password")
        if st.sidebar.button("ล็อกอิน", use_container_width=True):
            dfu = read_sheet_as_df("Users")
            if dfu.empty: st.sidebar.error("ไม่มีผู้ใช้ในชีต Users"); st.stop()
            cu = find_col_fuzzy(dfu, {"username","user","บัญชีผู้ใช้","ชื่อผู้ใช้"})
            cp = find_col_fuzzy(dfu, {"password","รหัสผ่าน"})
            cb = find_col_fuzzy(dfu, {"BranchCode","สาขา","branch"})
            if not (cu and cp and cb): st.sidebar.error("Users sheet ไม่ครบคอลัมน์"); st.stop()
            for c in (cu, cp, cb): dfu[c] = dfu[c].astype(str).str.strip()
            row = dfu[dfu[cu].str.casefold() == (u or "").strip().casefold()].head(1)
            if row.empty or str(row.iloc[0][cp]).strip() != (p or "").strip():
                st.sidebar.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
            else:
                st.session_state["auth"] = True
                st.session_state["user"] = {"username": (u or "").strip(),
                                            "branch": str(row.iloc[0][cb]).strip()}
                st.sidebar.success(f"ยินดีต้อนรับ {st.session_state['user']['username']}")
                time.sleep(0.5); do_rerun()
        st.stop()

    if st.sidebar.button("ออกจากระบบ"):
        st.session_state["auth"] = False; st.session_state["user"] = {}; do_rerun()

    branch_code = st.session_state["user"]["branch"]
    username    = st.session_state["user"]["username"]

    # -------- Inventory (single table) --------
    st.header("📦 รายการอุปกรณ์ที่พร้อมให้เบิก")

    dfi = read_sheet_as_df("Items")
    if dfi.empty:
        st.info("ยังไม่มีข้อมูลใน Items"); st.stop()

    c_code = find_col_fuzzy(dfi, {"รหัส","itemcode","code","sku","part","partno","partnumber"})
    if not c_code:
        st.error("Items: หา 'รหัส' ไม่พบ"); st.stop()

    # เลือกคอลัมน์ชื่อแบบฉลาด
    name_candidates = []
    for keys in [
        {"ชื่ออุปกรณ์","ชื่อสินค้า","itemname","productname"},
        {"ชื่อ","name","รายการ","description","desc"},
    ]:
        c = find_col_fuzzy(dfi, keys)
        if c: name_candidates.append(c)
    if name_candidates:
        c_name = max(name_candidates, key=lambda c: dfi[c].astype(str).str.strip().ne("").sum())
    else:
        others = [c for c in dfi.columns if c != c_code]
        c_name = others[0] if others else None

    name_display = dfi[c_name].astype(str).str.strip() if c_name else pd.Series([""]*len(dfi))

    # (ทางเลือก) เติมชื่อจากชีต Catalog (ถ้ามี) — ไม่ list ชีตทั้งหมด
    for cat in ("Catalog", "Catalogs", "ItemMaster", "Master"):
        try:
            dfm = read_sheet_as_df(cat)
        except Exception:
            dfm = pd.DataFrame()
        if dfm.empty: 
            continue
        m_code = find_col_fuzzy(dfm, {"รหัส","itemcode","code","sku","part","partno","partnumber"})
        m_name = find_col_fuzzy(dfm, {"ชื่อ","ชื่ออุปกรณ์","ชื่อสินค้า","name","รายการ","description","desc"})
        if m_code and m_name:
            mp = {str(r[m_code]).strip(): str(r[m_name]).strip()
                  for _, r in dfm.iterrows() if str(r[m_code]).strip()}
            for idx, row in dfi.iterrows():
                if not name_display.iloc[idx]:
                    code = str(row[c_code]).strip()
                    if code in mp:
                        name_display.iloc[idx] = mp[code]
            if not name_display.eq("").any():
                break

    c_qty   = find_col_fuzzy(dfi, {"คงเหลือ","qty","จำนวน","stock","balance","remaining","remain","จำนวนคงเหลือ"})
    c_ready = find_col_fuzzy(dfi, {
        "พร้อมให้เบิก","พร้อมให้เบิก(y/n)","ready","available",
        "ให้เบิก","allow","เปิดให้เบิก",
        "ใช้งาน","สถานะ","active","enabled","availableflag"
    })

    if c_ready:
        ready_mask = dfi[c_ready].astype(str).str.upper().str.strip().isin(["Y","YES","TRUE","1"])
    elif c_qty:
        ready_mask = pd.to_numeric(dfi[c_qty], errors="coerce").fillna(0) > 0
    else:
        ready_mask = pd.Series([True]*len(dfi))

    ready_df = dfi[ready_mask].copy()
    name_ready = name_display[ready_mask].copy()

    if ready_df.empty:
        st.warning("ยังไม่มีอุปกรณ์ที่พร้อมให้เบิก"); st.stop()

    base_df = pd.DataFrame({
        "รหัส": ready_df[c_code].astype(str).values,
        "ชื่อ":  name_ready.replace("", "(ไม่มีชื่อ)").values,
        "เลือก": [False] * len(ready_df),
        "จำนวนที่ต้องการ": [0] * len(ready_df),
    })

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

    col1, col2 = st.columns([1,1])
    submit = col1.button("✅ เบิกอุปกรณ์", type="primary", use_container_width=True)
    clear  = col2.button("🧹 ล้างข้อมูล", use_container_width=True)

    if clear:
        st.session_state.pop("order_table", None)
        st.session_state.pop("order_table_shape", None)
        st.success("ล้างการเลือกแล้ว")
        time.sleep(0.3); do_rerun()

    # -------- Submit --------
    if submit:
        sel = edited[(edited["เลือก"] == True) & (pd.to_numeric(edited["จำนวนที่ต้องการ"], errors="coerce").fillna(0) > 0)].copy()
        if sel.empty:
            st.warning("กรุณาเลือกอุปกรณ์อย่างน้อย 1 รายการ และระบุจำนวน"); st.stop()

        order_no = f"ORD-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
        ts = now_str()

        for _, r in sel.iterrows():
            req_no = f"REQ-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
            row = [
                req_no, order_no, ts, branch_code, username,
                r["รหัส"], r["ชื่อ"], str(int(r["จำนวนที่ต้องการ"])),
                "pending", "", ts, "", "N", "N",
            ]
            ws_reqs.append_row(row, value_input_option="USER_ENTERED")

        n_headers = ws_noti.row_values(1)
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
        ws_noti.append_row([noti.get(h,"") for h in n_headers], value_input_option="USER_ENTERED")

        # เคลียร์ cache อ่านชีต → หน้า History จะเห็นออเดอร์ใหม่ทันที
        st.cache_data.clear()

        with st.success(f"สร้างคำสั่งเบิกสำเร็จ: **{order_no}**"):
            st.write("สรุปรายการในออร์เดอร์:")
            st.dataframe(sel[["รหัส","ชื่อ","จำนวนที่ต้องการ"]].rename(columns={"จำนวนที่ต้องการ":"Qty"}),
                         use_container_width=True)

        st.session_state.pop("order_table", None)
        st.session_state.pop("order_table_shape", None)

    # -------- History --------
    st.markdown("### 🧾 ประวัติคำสั่งเบิก (ตามออร์เดอร์)")
    dfr = read_sheet_as_df("Requests")
    if not dfr.empty:
        c_branch = find_col_fuzzy(dfr, {"Branch"})
        c_user   = find_col_fuzzy(dfr, {"Requester"})
        c_order  = find_col_fuzzy(dfr, {"OrderNo"})
        c_code2  = find_col_fuzzy(dfr, {"ItemCode","รหัส"})
        c_name2  = find_col_fuzzy(dfr, {"ItemName","ชื่อ"})
        c_qty2   = find_col_fuzzy(dfr, {"Qty","จำนวน"})
        c_status = find_col_fuzzy(dfr, {"Status","สถานะ"})
        c_created= find_col_fuzzy(dfr, {"CreatedAt"})
        if c_order and c_branch and c_user:
            my = dfr[(dfr[c_branch]==branch_code) & (dfr[c_user]==username)].copy()
            if not my.empty:
                orders = my[c_order].dropna().unique().tolist()
                orders = sorted(orders, reverse=True)
                ord_sel = st.selectbox("เลือกออร์เดอร์", orders)
                sub = my[my[c_order]==ord_sel].copy()
                if c_created: sub = sub.sort_values(c_created)
                show_cols = [c_code2, c_name2, c_qty2, c_status]
                show_cols = [c for c in show_cols if c]
                st.dataframe(sub[show_cols].rename(columns={
                    c_code2:"รหัส", c_name2:"ชื่อ", c_qty2:"Qty", c_status:"สถานะ"
                }), use_container_width=True, height=260)
            else:
                st.info("ยังไม่มีคำสั่งเบิกของคุณ")
        else:
            st.info("Requests sheet ยังไม่มีคอลัมน์ OrderNo (ระบบจะสร้างให้อัตโนมัติเมื่อมีคำสั่งเบิกรอบแรก)")
    else:
        st.info("ยังไม่มีข้อมูลคำสั่งเบิก")

if __name__ == "__main__":
    main()
