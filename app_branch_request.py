# -*- coding: utf-8 -*-
"""
Branch Portal (Streamlit + Google Sheets) — CloudSafe Variant
- รองรับ 3 รูปแบบ secrets:
  1) st.secrets["gcp_service_account"] เป็น dict (แนะนำที่สุดบน Streamlit Cloud)
  2) st.secrets["service_account"] เป็น dict
  3) GOOGLE_SERVICE_ACCOUNT_JSON เป็นสตริง JSON
"""
import os, json
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st

# auto-load st.secrets into env strings for backward compatibility
try:
    for _k, _v in st.secrets.items():
        if isinstance(_v, (dict, list)):
            os.environ.setdefault(_k, json.dumps(_v, ensure_ascii=False))
        else:
            os.environ.setdefault(_k, str(_v))
except Exception:
    pass

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception as e:
    st.error("ต้องติดตั้ง gspread และ google-auth")
    st.stop()

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"
TIMEZONE = timezone(timedelta(hours=7))
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SHEET_URL = os.environ.get("SHEET_URL", "").strip()
SHEET_USERS = os.environ.get("SHEET_USERS", "Users")
SHEET_ITEMS = os.environ.get("SHEET_ITEMS", "Items")
SHEET_REQUESTS = os.environ.get("SHEET_REQUESTS", "Requests")
SHEET_NOTI = os.environ.get("SHEET_NOTIFICATIONS", "Notifications")
SHEET_SETTINGS = os.environ.get("SHEET_SETTINGS", "Settings")

VISIBLE_AVAILABLE_ONLY = "AVAILABLE_ONLY"
VISIBLE_ALL_WITH_FLAG = "ALL_WITH_FLAG"
STATUS_PENDING, STATUS_ISSUED, STATUS_RECEIVED = "PENDING","ISSUED","RECEIVED"
NOTI_REQ_CREATED, NOTI_ITEM_ISSUED = "REQ_CREATED","ITEM_ISSUED"

def _now_str(): return datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

def _load_credentials():
    # 1) Preferred: secrets table (dict) — avoids JSON string errors
    for k in ("gcp_service_account", "service_account", "GOOGLE_SERVICE_ACCOUNT_JSON"):
        if k in st.secrets:
            val = st.secrets[k]
            if isinstance(val, dict):
                scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
                return Credentials.from_service_account_info(dict(val), scopes=scope)
            # if it's a string, try to json.loads
            if isinstance(val, str) and val.strip():
                try:
                    info = json.loads(val)
                    scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
                    return Credentials.from_service_account_info(info, scopes=scope)
                except Exception:
                    st.error("ค่า GOOGLE_SERVICE_ACCOUNT_JSON ใน Secrets ไม่ใช่ JSON ที่ถูกต้อง (ลองใช้รูปแบบ [gcp_service_account] แทน)")
                    return None
    # 2) Fallback: env var JSON
    s = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if s:
        try:
            info = json.loads(s)
            scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            return Credentials.from_service_account_info(info, scopes=scope)
        except Exception:
            st.error("GOOGLE_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง")
            return None
    # 3) Fallback: file path
    p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if p and os.path.exists(p):
        scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        return Credentials.from_service_account_file(p, scopes=scope)
    st.error("ไม่พบ Service Account (ตั้งค่า secrets แบบ [gcp_service_account] แนะนำ)")
    return None

def _open_spreadsheet(client):
    if SHEET_ID: return client.open_by_key(SHEET_ID)
    if SHEET_URL: return client.open_by_url(SHEET_URL)
    st.error("โปรดตั้งค่า SHEET_ID หรือ SHEET_URL")
    return None

def _ensure_worksheet(ss, name, headers):
    try:
        ws = ss.worksheet(name)
    except Exception:
        ws = ss.add_worksheet(title=name, rows=1000, cols=50)
        ws.append_row(headers); return ws
    first = ws.row_values(1) or []
    missing = [h for h in headers if h not in first]
    if not first: ws.update("A1", [headers])
    elif missing: ws.update("A1", [first + missing])
    return ws

def _worksheet_to_df(ws):
    vals = ws.get_all_values()
    if not vals: return pd.DataFrame()
    return pd.DataFrame(vals[1:], columns=vals[0])

def _find_col(df, candidates:set):
    for c in df.columns:
        if c.strip() in candidates or c.strip().lower() in {x.lower() for x in candidates}:
            return c
    return None

@st.cache_data(ttl=15) 
def load_settings_df(): return _worksheet_to_df(st.session_state["ws_settings"])
@st.cache_data(ttl=15) 
def load_items_df(): return _worksheet_to_df(st.session_state["ws_items"])
@st.cache_data(ttl=15) 
def load_users_df(): return _worksheet_to_df(st.session_state["ws_users"])
@st.cache_data(ttl=10) 
def load_requests_df(): return _worksheet_to_df(st.session_state["ws_requests"])
@st.cache_data(ttl=10) 
def load_notifications_df(): return _worksheet_to_df(st.session_state["ws_noti"])

def verify_password(plain, hashed):
    if not hashed: return False
    try:
        import bcrypt
        if hashed.startswith("$2"): return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception: pass
    return plain == hashed

def do_login():
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    u = st.sidebar.text_input("ชื่อผู้ใช้")
    p = st.sidebar.text_input("รหัสผ่าน", type="password")
    if st.sidebar.button("ล็อกอิน", use_container_width=True):
        users = load_users_df()
        cu = _find_col(users, {"username","user","บัญชีผู้ใช้"})
        cp = _find_col(users, {"password","รหัสผ่าน"})
        cr = _find_col(users, {"role","บทบาท"})
        cb = _find_col(users, {"branch","สาขา","branchcode","BranchCode"})
        if not all([cu,cp,cb]): st.error("Users sheet ไม่ครบคอลัมน์ที่จำเป็น"); return
        row = users[users[cu]==u].head(1)
        if row.empty: st.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง"); return
        if not verify_password(p, str(row.iloc[0][cp])): st.error("ไม่พบผู้ใช้หรือรหัสผ่านไม่ถูกต้อง"); return
        role = str(row.iloc[0][cr]) if cr else "branch"
        if role and role.lower() not in {"branch","user","staff","สาขา"}:
            st.error("บัญชีนี้ไม่ได้กำหนดบทบาทสำหรับสาขา"); return
        st.session_state["auth"]=True
        st.session_state["user"]={"username":u,"role":role or "branch","branch":str(row.iloc[0][cb])}
        st.success(f"ยินดีต้อนรับ {u}")

def get_branch_visible_mode():
    df = load_settings_df()
    if df.empty: return VISIBLE_AVAILABLE_ONLY
    key_col = "key" if "key" in df.columns else ("Key" if "Key" in df.columns else None)
    val_col = "value" if "value" in df.columns else ("Value" if "Value" in df.columns else None)
    if not key_col or not val_col: return VISIBLE_AVAILABLE_ONLY
    hit = df[df[key_col]=="branch_visible_mode"]
    mode = str(hit.iloc[0][val_col]).strip().upper() if not hit.empty else VISIBLE_AVAILABLE_ONLY
    return mode if mode in {VISIBLE_AVAILABLE_ONLY, VISIBLE_ALL_WITH_FLAG} else VISIBLE_AVAILABLE_ONLY

def page_stock():
    st.header("📦 คลังสำหรับสาขา")
    items = load_items_df()
    if items.empty: st.info("ยังไม่มีข้อมูล"); return
    c_code = _find_col(items, {"รหัส","รหัสวัสดุ","ItemCode","Code","รหัสอุปกรณ์"})
    c_name = _find_col(items, {"ชื่อ","รายการ","ItemName","Name","ชื่ออุปกรณ์"})
    c_qty  = _find_col(items, {"คงเหลือ","จำนวนคงเหลือ","Stock","Qty","จำนวน"})
    c_ready= _find_col(items, {"พร้อมให้เบิก","พร้อมให้เบิก(Y/N)","Available","Ready","พร้อม"})
    if not all([c_code,c_name,c_qty]):
        st.error("Items sheet ต้องมี รหัส/ชื่อ/คงเหลือ"); return
    df = items[[c_code,c_name,c_qty] + ([c_ready] if c_ready else [])].copy()
    df.rename(columns={c_code:"รหัส",c_name:"ชื่อ",c_qty:"คงเหลือ"}, inplace=True)
    if c_ready: df.rename(columns={c_ready:"พร้อมให้เบิก"}, inplace=True)
    else: df["พร้อมให้เบิก"] = ""
    mode = get_branch_visible_mode()
    if mode==VISIBLE_AVAILABLE_ONLY:
        df = df[(df["พร้อมให้เบิก"].str.upper()=="Y") | (df["พร้อมให้เบิก"].str.upper()=="YES")]
    else:
        df["สถานะ"] = df["พร้อมให้เบิก"].apply(lambda x: "✅ พร้อม" if str(x).upper() in {"Y","YES","TRUE","1"} else "🚫 ไม่พร้อม")
    st.dataframe(df, use_container_width=True)

def page_create_request():
    st.header("🧾 สร้างคำขอเบิกอุปกรณ์")
    items = load_items_df()
    if items.empty: st.info("ยังไม่มีข้อมูล"); return
    c_code = _find_col(items, {"รหัส","รหัสวัสดุ","ItemCode","Code","รหัสอุปกรณ์"})
    c_name = _find_col(items, {"ชื่อ","รายการ","ItemName","Name","ชื่ออุปกรณ์"})
    c_qty  = _find_col(items, {"คงเหลือ","จำนวนคงเหลือ","Stock","Qty","จำนวน"})
    df = items[[c_code,c_name,c_qty]].copy(); df.columns=["code","name","qty"]
    def label(r):
        try: q=int(float(r["qty"]))
        except Exception: q=r["qty"]
        return f"{r['code']} | {r['name']} (คงเหลือ {q})"
    df["label"]=df.apply(label, axis=1)
    st.write("เพิ่มรายการที่ต้องการเบิก (เลือกอุปกรณ์ + ระบุจำนวน)")
    data = pd.DataFrame([{"อุปกรณ์":"","จำนวน":1,"หมายเหตุ":""} for _ in range(5)])
    edited = st.data_editor(data, num_rows="dynamic", use_container_width=True,
                            column_config={
                                "อุปกรณ์": st.column_config.SelectboxColumn(options=df["label"].tolist(), required=False),
                                "จำนวน": st.column_config.NumberColumn(min_value=1, step=1),
                                "หมายเหตุ": st.column_config.TextColumn(),
                            })
    requester = st.text_input("ผู้ขอเบิก (ชื่อผู้ติดต่อ)")
    if st.button("ส่งคำขอ", type="primary", use_container_width=True):
        rows = edited.dropna(how="all"); rows = rows[rows["อุปกรณ์"].astype(str).str.strip()!=""]
        if rows.empty: st.warning("กรุณาเพิ่มอย่างน้อย 1 รายการ"); return
        req_no = make_req_no(st.session_state["user"]["branch"]); now=_now_str()
        to_req=[]
        for _,r in rows.iterrows():
            item = df[df["label"]==r["อุปกรณ์"]].head(1)
            if item.empty: continue
            to_req.append([req_no,now,st.session_state["user"]["branch"],requester,
                           str(item.iloc[0]["code"]), str(item.iloc[0]["name"]), int(r["จำนวน"]),
                           STATUS_PENDING,"",now, str(r.get("หมายเหตุ","")), "N","N"])
        st.session_state["ws_requests"].append_rows(to_req, value_input_option="USER_ENTERED")
        st.session_state["ws_noti"].append_row([make_noti_id(), now, "main","", NOTI_REQ_CREATED, req_no,
                                                f"มีคำขอเบิกใหม่จากสาขา {st.session_state['user']['branch']}: {req_no}", "N",""],
                                               value_input_option="USER_ENTERED")
        st.cache_data.clear(); st.success(f"ส่งคำขอเรียบร้อยแล้ว (เลขที่ {req_no})"); st.experimental_rerun()

def page_my_requests():
    st.header("📮 คำขอของฉัน")
    req = load_requests_df()
    if req.empty: st.info("ยังไม่มีคำขอ"); return
    if "Branch" not in req.columns: st.warning("Requests sheet ไม่มีคอลัมน์ Branch"); return
    view = req[req["Branch"]==st.session_state["user"]["branch"]].copy()
    st.dataframe(view, use_container_width=True)

def make_req_no(branch):
    from random import randint
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S"); return f"{branch}-{ts}-{randint(100,999)}"
def make_noti_id():
    from random import randint
    ts = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S"); return f"NOTI-{ts}-{randint(1000,9999)}"

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    creds = _load_credentials()
    if creds is None: st.stop()
    client = gspread.authorize(creds)
    ss = _open_spreadsheet(client)
    if ss is None: st.stop()
    ws_users = _ensure_worksheet(ss, SHEET_USERS, ["username","password","role","BranchCode"])
    ws_items = _ensure_worksheet(ss, SHEET_ITEMS, ["รหัส","ชื่อ","คงเหลือ","พร้อมให้เบิก(Y/N)"])
    ws_requests = _ensure_worksheet(ss, SHEET_REQUESTS, [
        "ReqNo","CreatedAt","Branch","Requester","ItemCode","ItemName","Qty","Status","Approver","LastUpdate","Note","NotifiedMain(Y/N)","NotifiedBranch(Y/N)"
    ])
    ws_noti = _ensure_worksheet(ss, SHEET_NOTI, ["NotiID","CreatedAt","TargetApp","TargetBranch","Type","RefID","Message","ReadFlag","ReadAt"])
    ws_settings = _ensure_worksheet(ss, SHEET_SETTINGS, ["key","value"])
    st.session_state["ws_users"]=ws_users; st.session_state["ws_items"]=ws_items
    st.session_state["ws_requests"]=ws_requests; st.session_state["ws_noti"]=ws_noti; st.session_state["ws_settings"]=ws_settings

    if not st.session_state.get("auth"): do_login(); st.stop()
    u = st.session_state["user"]
    with st.sidebar.expander("บัญชีของฉัน", expanded=True):
        st.write(f"ผู้ใช้: **{u['username']}**"); st.write(f"บทบาท: **{u['role'] or 'branch'}**"); st.write(f"สาขา: **{u['branch']}**")
        if st.button("ออกจากระบบ", use_container_width=True):
            st.session_state["auth"]=False; st.session_state["user"]=None; st.cache_data.clear(); st.experimental_rerun()

    tab = st.sidebar.radio("เมนู", ["🔔 การแจ้งเตือน","🧾 สร้างคำขอ","📮 คำขอของฉัน","📦 คลังสำหรับสาขา"], index=0)
    if tab.startswith("🔔"): st.write("ยังไม่มีแจ้งเตือนใหม่ในเฟสนี้");  # viewer only (ฝั่ง main จะเขียนกลับในเฟส 2)
    elif tab.startswith("🧾"): page_create_request()
    elif tab.startswith("📮"): page_my_requests()
    else: page_stock()

if __name__ == "__main__":
    main()
