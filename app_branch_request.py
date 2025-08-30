# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — Phase 1 (Production, Fuzzy headers + Auto Catalog)

- โชว์เฉพาะ “รหัส” และ “ชื่ออุปกรณ์”
- หา 'ชื่อ' แบบ fuzzy + เดาอัตโนมัติถ้าไม่เจอ
- ถ้า 'ชื่อ' ใน Items ว่าง → ดึงชื่อจากแคตตาล็อก (สแกนทุกแผ่นที่มีคู่คอลัมน์รหัส/ชื่อ)
- เงื่อนไขพร้อมให้เบิก: ถ้ามีคอลัมน์ ready ใช้เลย, ถ้าไม่มีใช้ qty>0, ถ้ายังไม่มีให้เบิกได้ทั้งหมด
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
            st.experimental_rerun()  # for old streamlit
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
    """เลือกคอลัมน์ตามคีย์เวิร์ด (exact/contains), ไม่สนเว้นวรรค/พิมพ์เล็กใหญ่"""
    if df is None or df.empty:
        return None
    headers = list(df.columns)
    norm = {h: _norm(h) for h in headers}
    kset = {_norm(k) for k in keywords}

    # exact
    for h in headers:
        if norm[h] in kset:
            return h
    # contains
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

def open_spreadsheet(client):
    SHEET_ID  = st.secrets.get("SHEET_ID","").strip() or os.environ.get("SHEET_ID","").strip()
    SHEET_URL = st.secrets.get("SHEET_URL","").strip() or os.environ.get("SHEET_URL","").strip()
    if SHEET_ID:  return client.open_by_key(SHEET_ID)
    if SHEET_URL: return client.open_by_url(SHEET_URL)

    st.info("ยังไม่ตั้งค่า SHEET_ID / SHEET_URL — วางลิงก์ Google Sheet ครั้งแรก")
    url = st.text_input("URL ของ Google Sheet (https://docs.google.com/spreadsheets/…)",
                        value=st.session_state.get("input_sheet_url",""))
    if st.button("เชื่อมต่อชีตจาก URL", type="primary"):
        if not url.strip(): st.warning("กรุณาวาง URL"); st.stop()
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

    # ----- Inventory (show only code + name) -----
    st.header("📦 คลังสำหรับสาขา")
    dfi = ws_to_df(ws_items)
    if dfi.empty: st.info("ยังไม่มีข้อมูลใน Items"); st.stop()

    # pick columns (fuzzy)
    c_code  = find_col_fuzzy(dfi, {"รหัส","itemcode","code","sku","part","partno","partnumber"})
    c_name  = find_col_fuzzy(dfi, {"ชื่อ","ชื่ออุปกรณ์","ชื่อสินค้า","name","รายการ","รายละเอียด","description","desc","itemname","product"})
    c_qty   = find_col_fuzzy(dfi, {"คงเหลือ","qty","จำนวน","stock","balance","remaining","remain","จำนวนคงเหลือ"})
    c_ready = find_col_fuzzy(dfi, {"พร้อมให้เบิก","พร้อมให้เบิก(y/n)","ready","available","ให้เบิก","allow","เปิดให้เบิก"})

    if not c_code:
        st.error("Items: หา 'รหัส' ไม่พบ (เช่น รหัส/Code/ItemCode/SKU/PartNo)"); st.stop()

    # ถ้าไม่เจอ 'ชื่อ' ให้เดาคอลัมน์ที่ไม่ใช่รหัสเป็นชื่อ (คอลัมน์ลำดับถัดไป)
    if not c_name:
        others = [c for c in dfi.columns if c != c_code]
        c_name = others[0] if others else None

    # สร้างชุดชื่อแสดงผล
    name_display = dfi[c_name].astype(str).str.strip() if c_name else pd.Series([""]*len(dfi))

    # ดึงชื่อจาก "แคตตาล็อก" อัตโนมัติ ถ้าช่องชื่อว่าง
    # สแกนทุกแผ่นที่ไม่ใช่ระบบ หาแผ่นที่มี (รหัส,ชื่อ)
    if name_display.eq("").any():
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
                # เติมเฉพาะที่ว่าง
                for idx, row in dfi.iterrows():
                    if not name_display.iloc[idx]:
                        code = str(row[c_code]).strip()
                        if code in mp:
                            name_display.iloc[idx] = mp[code]
                # ถ้าเติมครบแล้วก็พอ
                if not name_display.eq("").any():
                    break

    # ตารางแสดงผล: เฉพาะ “รหัส” + “ชื่อ”
    view_df = pd.DataFrame({"รหัส": dfi[c_code].astype(str), "ชื่อ": name_display})
    st.dataframe(view_df, use_container_width=True, height=420)

    # ----- Request form -----
    st.subheader("📝 เบิกอุปกรณ์")
    # เงื่อนไขพร้อมให้เบิก
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

    ready_df["_label"] = ready_df[c_code].astype(str) + " — " + name_ready.replace("", "(ไม่มีชื่อ)")
    choice = st.selectbox("เลือกอุปกรณ์", ready_df["_label"].tolist())
    qty_req = st.number_input("จำนวนที่ต้องการ", min_value=1, step=1, value=1)
    note = st.text_input("หมายเหตุ (ถ้ามี)", value="")

    if st.button("ยืนยันเบิกอุปกรณ์", type="primary"):
        row = ready_df[ready_df["_label"] == choice].iloc[0]
        shown_name = str(name_ready.loc[row.name]) if row.name in name_ready.index else ""
        item_code = str(row[c_code])
        item_name = shown_name or ""

        req_no = f"REQ-{branch_code}-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}"
        ts = now_str()

        headers = ws_reqs.row_values(1)
        new_rec = {
            "ReqNo": req_no, "CreatedAt": ts, "Branch": branch_code,
            "Requester": username, "ItemCode": item_code, "ItemName": item_name,
            "Qty": str(int(qty_req)), "Status": "pending", "Approver": "",
            "LastUpdate": ts, "Note": note,
            "NotifiedMain(Y/N)": "N", "NotifiedBranch(Y/N)": "N",
        }
        ws_reqs.append_row([new_rec.get(h,"") for h in headers], value_input_option="USER_ENTERED")

        n_headers = ws_noti.row_values(1)
        noti = {
            "NotiID": f"NOTI-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}",
            "CreatedAt": ts, "TargetApp": "main_app", "TargetBranch": branch_code,
            "Type": "REQUEST_CREATED", "RefID": req_no,
            "Message": f"{branch_code} เบิกอุปกรณ์ {item_code} x {int(qty_req)} โดย {username}",
            "ReadFlag": "N", "ReadAt": "",
        }
        ws_noti.append_row([noti.get(h,"") for h in n_headers], value_input_option="USER_ENTERED")

        st.success(f"สร้างคำขอ {req_no} สำเร็จ! (รอการดำเนินการ)")
        time.sleep(1.2); do_rerun()

    # ----- My requests -----
    with st.expander("คำขอของฉัน (ล่าสุด)"):
        dfr = ws_to_df(ws_reqs)
        if not dfr.empty:
            c_branch= find_col_fuzzy(dfr, {"Branch"})
            c_user  = find_col_fuzzy(dfr, {"Requester"})
            c_created = find_col_fuzzy(dfr, {"CreatedAt"})
            sub = dfr[(dfr[c_branch]==branch_code) & (dfr[c_user]==username)].copy()
            if not sub.empty:
                if c_created: sub = sub.sort_values(c_created, ascending=False).head(20)
                st.dataframe(sub, use_container_width=True, height=300)
            else:
                st.write("ยังไม่มีคำขอล่าสุด")
        else:
            st.write("ยังไม่มีคำขอในระบบ")

if __name__ == "__main__":
    main()
