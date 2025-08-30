# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — Phase 1 (Single-table request workflow)

สิ่งที่ปรับตามคำขอ:
1) มี “ตารางเดียว” สำหรับรายการที่พร้อมให้เบิก → ให้ติ๊ก checkbox ต่อรายการ และใส่ “จำนวนที่ต้องการ”
2) ปุ่ม “เบิกอุปกรณ์” และปุ่ม “ล้างข้อมูล”
3) เมื่อบันทึกแล้ว สร้างเลขออร์เดอร์ (OrderNo) เดียวรวมหลายบรรทัด
   - บันทึกลงชีต Requests (เพิ่มคอลัมน์ OrderNo อัตโนมัติถ้ายังไม่มี)
   - สร้าง Notification ไปยังฝั่งหลัก
   - มีส่วน “ประวัติออร์เดอร์” ให้เลือกดูย้อนหลัง (เลือก OrderNo แล้วสรุปให้)

หมายเหตุ:
- ไม่แสดง “คงเหลือ/พร้อมให้เบิก” ในตาราง แต่จะใช้เงื่อนไขภายในในการกรองรายการที่ “พร้อมให้เบิก”
- ถ้าไม่มีคอลัมน์พร้อมให้เบิก → ใช้เงื่อนไขคงเหลือ>0; ถ้าก็ไม่มี → ให้เบิกได้ทุกชิ้น
- ถ้าชื่ออุปกรณ์ใน Items ว่าง จะพยายามดึงจากแคตตาล็อก (สแกนทุกแผ่นที่มีคู่คอลัมน์รหัส/ชื่อ)
"""

import os, json, time, re
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TZ = timezone(timedelta(hours=7))

# ---------- helpers ----------
def do_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()  # สำหรับเวอร์ชัน streamlit เก่า
        except Exception:
            pass

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

def ws_to_df(ws):
    vals = ws.get_all_values()
    return pd.DataFrame(vals[1:], columns=vals[0]) if vals else pd.DataFrame()

def _norm(s: str) -> str:
    s = str(s or "")
    s = s.strip()
    s = re.sub(r"\s+", "", s)               # remove spaces
    s = re.sub(r"[^0-9A-Za-zก-๙]+", "", s)  # keep letters/digits/thai
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

# ---------- Credentials ----------
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

def _extract_sheet_id(id_or_url: str) -> str | None:
    s = (id_or_url or "").strip()
    if not s: return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", s)
    if m: return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9\-_]{20,}", s): return s
    return None

def open_spreadsheet(client, creds):
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
            sa = getattr(creds, "service_account_email", None)
            with st.expander("รายละเอียดการเชื่อมต่อ / วิธีแก้ (คลิกเพื่อดู)"):
                st.error(f"เปิดสเปรดชีตไม่สำเร็จ (ID: {sid})")
                st.write("1) แชร์ไฟล์ให้ Service Account (สิทธิ Editor)")
                if sa: st.write("Service Account:", f"`{sa}`")
                st.write("2) ตรวจ SHEET_URL / SHEET_ID ว่าถูกต้อง")
                st.exception(e)
            st.stop()

    sid = _extract_sheet_id(raw) if raw else None
    if sid:
        return _try_open(sid)

    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์หรือ Spreadsheet ID")
    inp = st.text_input("URL หรือ Spreadsheet ID", value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีต", type="primary"):
        sid2 = _extract_sheet_id(inp)
        if not sid2:
            st.warning("รูปแบบไม่ถูกต้อง"); st.stop()
        st.session_state["input_sheet_url"] = inp.strip()
        return _try_open(sid2)
    st.stop()

# ---------- App ----------
def main():
    import gspread
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    creds = load_credentials()
    client = gspread.authorize(creds)
    try:
        st.caption(f"Service Account: `{creds.service_account_email}`")
    except Exception:
        pass

    ss = open_spreadsheet(client, creds)

    titles = [w.title for w in ss.worksheets()]
    ws_users = ss.worksheet("Users") if "Users" in titles else ss.add_worksheet("Users", 1000, 26)
    ws_items = ss.worksheet("Items") if "Items" in titles else ss.add_worksheet("Items", 2000, 26)
    ws_reqs  = ss.worksheet("Requests") if "Requests" in titles else ss.add_worksheet("Requests", 2000, 26)
    ws_noti  = ss.worksheet("Notifications") if "Notifications" in titles else ss.add_worksheet("Notifications", 2000, 26)
    ws_conf  = ss.worksheet("Settings") if "Settings" in titles else ss.add_worksheet("Settings", 1000, 26)

    # เพิ่ม OrderNo ถ้ายังไม่มี
    ensure_headers(ws_users, ["username","password","role","BranchCode"])
    ensure_headers(ws_items, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    ensure_headers(ws_reqs,  ["ReqNo","OrderNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty","Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"])
    ensure_headers(ws_noti,  ["NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"])
    ensure_headers(ws_conf,  ["key","value"])

    # ----- Login -----
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False; st.session_state["user"] = {}
    if not st.session_state["auth"]:
        u = st.sidebar.text_input("ชื่อผู้ใช้")
        p = st.sidebar.text_input("รหัสผ่าน", type="password")
        if st.sidebar.button("ล็อกอิน", use_container_width=True):
            dfu = ws_to_df(ws_users)
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

    # ----- READ INVENTORY -----
    st.header("📦 รายการอุปกรณ์ที่พร้อมให้เบิก")
    dfi = ws_to_df(ws_items)
    if dfi.empty: st.info("ยังไม่มีข้อมูลใน Items"); st.stop()

    c_code  = find_col_fuzzy(dfi, {"รหัส","itemcode","code","sku","part","partno","partnumber"})
    c_name  = find_col_fuzzy(dfi, {"ชื่อ","ชื่ออุปกรณ์","ชื่อสินค้า","name","รายการ","รายละเอียด","description","desc","itemname","product"})
    c_qty   = find_col_fuzzy(dfi, {"คงเหลือ","qty","จำนวน","stock","balance","remaining","remain","จำนวนคงเหลือ"})
    c_ready = find_col_fuzzy(dfi, {"พร้อมให้เบิก","พร้อมให้เบิก(y/n)","ready","available","ให้เบิก","allow","เปิดให้เบิก"})
    if not c_code:
        st.error("Items: หา 'รหัส' ไม่พบ"); st.stop()
    if not c_name:  # เดาคอลัมน์ชื่อถ้าไม่เจอ
        others = [c for c in dfi.columns if c != c_code]
        c_name = others[0] if others else None

    # สร้างชื่อแสดงผล (ถ้าว่างจะลองดึงจากแคตตาล็อก)
    name_display = dfi[c_name].astype(str).str.strip() if c_name else pd.Series([""]*len(dfi))
    if name_display.eq("").any():
        # สแกนทุกแผ่นหา (รหัส,ชื่อ)
        system_tabs = {"Users","Items","Requests","Notifications","Settings"}
        for w in ss.worksheets():
            if w.title in system_tabs: 
                continue
            dfm = ws_to_df(w)
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
                        if code in mp: name_display.iloc[idx] = mp[code]
                if not name_display.eq("").any():
                    break

    # filter พร้อมให้เบิก (ไม่แสดง stock)
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

    # ---------- SINGLE TABLE SELECT (checkbox + qty) ----------
    # เตรียม DataFrame สำหรับ editor
    base_df = pd.DataFrame({
        "รหัส": ready_df[c_code].astype(str).values,
        "ชื่อ":  name_ready.replace("", "(ไม่มีชื่อ)").values,
        "เลือก": [False] * len(ready_df),
        "จำนวนที่ต้องการ": [0] * len(ready_df),
    })

    # ใช้ค่าจาก session เพื่อคงการแก้ไขระหว่าง rerun
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
    # อัพเดต session ให้ตรงกับ editor
    st.session_state["order_table"] = edited

    col1, col2 = st.columns([1,1])
    submit = col1.button("✅ เบิกอุปกรณ์", type="primary", use_container_width=True)
    clear  = col2.button("🧹 ล้างข้อมูล", use_container_width=True)

    if clear:
        st.session_state.pop("order_table", None)
        st.session_state.pop("order_table_shape", None)
        st.success("ล้างการเลือกแล้ว")
        time.sleep(0.3); do_rerun()

    # ----- SUBMIT -----
    if submit:
        sel = edited[(edited["เลือก"] == True) & (pd.to_numeric(edited["จำนวนที่ต้องการ"], errors="coerce").fillna(0) > 0)].copy()
        if sel.empty:
            st.warning("กรุณาเลือกอุปกรณ์อย่างน้อย 1 รายการ และระบุจำนวน"); st.stop()

        # สร้าง OrderNo เดียวรวมหลายบรรทัด
        order_no = f"ORD-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
        ts = now_str()

        # เขียนลง Requests (เพิ่มหัว OrderNo ไว้แล้วด้านบน)
        req_headers = ws_reqs.row_values(1)
        rows = []
        for _, r in sel.iterrows():
            rows.append([
                "",                                # ReqNo จะใส่ภายหลังต่อบรรทัด
                order_no,                           # OrderNo
                ts,                                 # CreatedAt
                branch_code,                        # Branch
                username,                           # Requester
                r["รหัส"],                          # ItemCode
                r["ชื่อ"],                           # ItemName
                str(int(r["จำนวนที่ต้องการ"])),       # Qty
                "pending",                          # Status
                "",                                 # Approver
                ts,                                 # LastUpdate
                "",                                 # Note
                "N",                                # NotifiedMain(Y/N)
                "N",                                # NotifiedBranch(Y/N)
            ])

        # เติม ReqNo ให้แต่ละแถวแล้ว append
        for line in rows:
            req_no = f"REQ-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
            line[0] = req_no
            ws_reqs.append_row(line, value_input_option="USER_ENTERED")

        # แจ้งเตือนให้ฝั่งหลัก (รวมทั้งออร์เดอร์)
        n_headers = ws_noti.row_values(1)
        noti = {
            "NotiID": f"NOTI-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}",
            "CreatedAt": ts,
            "TargetApp": "main_app",
            "TargetBranch": branch_code,
            "Type": "ORDER_CREATED",
            "RefID": order_no,
            "Message": f"{branch_code} สร้างคำสั่งเบิก {order_no} จำนวน {len(rows)} รายการ โดย {username}",
            "ReadFlag": "N",
            "ReadAt": "",
        }
        ws_noti.append_row([noti.get(h,"") for h in n_headers], value_input_option="USER_ENTERED")

        # สรุปผลออร์เดอร์ทันที
        with st.success(f"สร้างคำสั่งเบิกสำเร็จ: **{order_no}**"):
            st.write("สรุปรายการในออร์เดอร์:")
            st.dataframe(sel[["รหัส","ชื่อ","จำนวนที่ต้องการ"]].rename(columns={"จำนวนที่ต้องการ":"Qty"}), use_container_width=True)

        # เคลียร์ตารางเลือก
        st.session_state.pop("order_table", None)
        st.session_state.pop("order_table_shape", None)

    # ----- HISTORY (เลือกดูตาม OrderNo) -----
    st.markdown("### 🧾 ประวัติคำสั่งเบิก (ตามออร์เดอร์)")
    dfr = ws_to_df(ws_reqs)
    if not dfr.empty:
        c_branch = find_col_fuzzy(dfr, {"Branch"})
        c_user   = find_col_fuzzy(dfr, {"Requester"})
        c_order  = find_col_fuzzy(dfr, {"OrderNo"})
        c_code   = find_col_fuzzy(dfr, {"ItemCode","รหัส"})
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
                show_cols = [c_code, c_name2, c_qty2, c_status]
                show_cols = [c for c in show_cols if c]
                st.dataframe(sub[show_cols].rename(columns={
                    c_code:"รหัส", c_name2:"ชื่อ", c_qty2:"Qty", c_status:"สถานะ"
                }), use_container_width=True, height=260)
            else:
                st.info("ยังไม่มีคำสั่งเบิกของคุณ")
        else:
            st.info("Requests sheet ยังไม่มีคอลัมน์ OrderNo (ระบบจะสร้างให้อัตโนมัติเมื่อมีคำสั่งเบิกรอบแรก)")
    else:
        st.info("ยังไม่มีข้อมูลคำสั่งเบิก")

if __name__ == "__main__":
    main()
