# app_branch_request.py
# -*- coding: utf-8 -*-

import os, json, base64, random, string, datetime
from io import BytesIO

import streamlit as st
import pandas as pd
from PIL import Image

# ถ้าคุณใช้กูเกิลชีต ให้แน่ใจว่าติดตั้ง gspread แล้ว (Streamlit Cloud มีมาให้)
import gspread
from google.oauth2.service_account import Credentials
import requests

APP_TITLE = "WishCo Branch Portal — เบิกอุปกรณ์"


# =====================================================================================
# 1) ------------------------------  MOBILE CSS  ---------------------------------------
# =====================================================================================
def inject_mobile_css():
    st.markdown("""
    <style>
      .block-container {padding-top:.6rem; padding-bottom:6rem;}

      .wishco-logo-box {
        display:flex; justify-content:center; align-items:center;
        background:rgba(255,255,255,0.85);
        border-radius:16px; padding:14px 18px;
        box-shadow:0 8px 20px rgba(0,0,0,0.08);
        margin:.3rem auto 0.2rem auto;
        max-width:720px;
      }
      .wishco-title { text-align:center; font-size:1.35rem; font-weight:800; color:#0F2D52; }

      .btn-primary, .btn-secondary {
        display:inline-block; width:100%;
        padding:.9rem 1rem; border-radius:14px;
        font-weight:800; text-align:center; border:none;
      }
      .btn-primary { background:#ef233c; color:#fff; }
      .btn-secondary { background:#e9eef5; color:#0f172a; }

      .card {
        padding:.8rem 1rem; border:1px solid #e9ecef; border-radius:14px;
        margin-bottom:.6rem; background:#fff;
        box-shadow:0 2px 10px rgba(0,0,0,.03);
      }
      .row-top {display:flex; align-items:center; justify-content:space-between; gap:.7rem;}
      .code {font-weight:800; color:#0f2d52;}
      .name {font-size:.92rem; color:#334155;}
      .badge {background:#f1f5f9; border-radius:10px; padding:.15rem .6rem; font-size:.75rem;}

      .qty-box {display:flex; align-items:center; gap:.4rem;}
      .qty-btn {
        background:#f1f5f9; border:none; width:40px; height:40px; border-radius:12px;
        font-size:1.2rem; font-weight:900; color:#0f172a;
      }
      .qty-input input {text-align:center;}

      .action-bar {display:flex; gap:.6rem;}
      @media (max-width: 640px) {
        .action-bar { flex-direction:column; }
        .wishco-title { font-size:1.15rem !important; }
      }
    </style>
    """, unsafe_allow_html=True)


# =====================================================================================
# 2) ---------------------------  LOGO (Auto / URL / Base64 / Upload)  -----------------
# =====================================================================================
def _glob_case_insensitive(paths):
    out = []
    for p in paths:
        if os.path.exists(p):
            out.append(p)
            continue
        d = os.path.dirname(p) or "."
        name = os.path.basename(p).lower()
        try:
            for fname in os.listdir(d):
                if fname.lower() == name:
                    out.append(os.path.join(d, fname))
                    break
        except FileNotFoundError:
            pass
    return out

def _load_logo_image():
    # 1) จาก uploader ใน session
    if "_logo_bytes" in st.session_state and st.session_state["_logo_bytes"]:
        try:
            return Image.open(BytesIO(st.session_state["_logo_bytes"]))
        except Exception:
            pass

    # 2) จาก Secrets
    logo_url = st.secrets.get("LOGO_URL", "").strip() if "LOGO_URL" in st.secrets else ""
    logo_b64 = st.secrets.get("LOGO_BASE64", "").strip() if "LOGO_BASE64" in st.secrets else ""

    if logo_url:
        try:
            r = requests.get(logo_url, timeout=10)
            r.raise_for_status()
            return Image.open(BytesIO(r.content))
        except Exception as e:
            st.caption(f"LOGO_URL โหลดไม่ได้: {e}")

    if logo_b64:
        try:
            raw = base64.b64decode(logo_b64)
            return Image.open(BytesIO(raw))
        except Exception as e:
            st.caption(f"LOGO_BASE64 ผิดรูปแบบ: {e}")

    # 3) หาใน repo เอง
    names = ["logoW1", "wishco_logo", "wishco", "logo"]
    exts  = [".png", ".jpg", ".jpeg", ".webp"]
    dirs  = ["", "assets", "static", "images", "img", "public"]

    candidates = []
    for d in dirs:
        for n in names:
            for e in exts:
                candidates.append(os.path.join(d, n + e))

    real_paths = _glob_case_insensitive(candidates)
    for p in real_paths:
        try:
            return Image.open(p)
        except Exception:
            continue

    return None

def render_header_with_logo():
    with st.sidebar.expander("อัปโหลดโลโก้ (ใช้ชั่วคราวต่อ Session)"):
        upl = st.file_uploader("เลือกรูป (png/jpg/webp)", type=["png","jpg","jpeg","webp"])
        if upl is not None:
            st.session_state["_logo_bytes"] = upl.read()
            st.success("อัปโหลดเสร็จแล้ว")

        st.caption("แนะนำ: วางไฟล์ไว้ใน repo เช่น assets/logoW1.jpg")

    logo_img = _load_logo_image()

    col_left, col_mid, col_right = st.columns([1,3,1])
    with col_mid:
        st.markdown('<div class="wishco-logo-box">', unsafe_allow_html=True)
        if logo_img is not None:
            st.image(logo_img, use_container_width=False, width=520)
        else:
            st.write("**WishCo Wholesale**")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="wishco-title">{APP_TITLE}</div>', unsafe_allow_html=True)


# =====================================================================================
# 3) -------------------------  GOOGLE SHEETS CONNECTION  ------------------------------
# =====================================================================================
def _load_sa_from_secrets():
    """รองรับทั้ง gcp_service_account (dict) และ GOOGLE_SERVICE_ACCOUNT_JSON (string JSON)"""
    if "gcp_service_account" in st.secrets:
        return dict(st.secrets["gcp_service_account"])
    if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
        if isinstance(raw, str) and raw.strip():
            return json.loads(raw)
    return None

def get_gspread_client():
    sa = _load_sa_from_secrets()
    if not sa:
        raise RuntimeError("ไม่พบ Service Account ใน Secrets (gcp_service_account หรือ GOOGLE_SERVICE_ACCOUNT_JSON)")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    sheet_url = st.secrets.get("SHEET_URL", "").strip()
    sheet_id  = st.secrets.get("SHEET_ID", "").strip()

    if sheet_url:
        return gc.open_by_url(sheet_url)
    if sheet_id:
        return gc.open_by_key(sheet_id)

    # ถ้าไม่ได้ตั้งค่า ให้ผู้ใช้วาง URL ในหน้าเว็บ
    st.warning("ยังไม่ได้ตั้งค่า SHEET_URL / SHEET_ID (Secrets)")
    url = st.text_input("URL ของ Google Sheet", placeholder="วางลิงก์ชีตที่นี่แล้วกด Enter")
    if url:
        return gc.open_by_url(url)
    raise RuntimeError("ไม่มีข้อมูลการเชื่อมต่อชีต")

def _norm_colname(s):
    s = str(s).strip().lower()
    s = s.replace(" ", "").replace("_","")
    return s

def read_ready_items(ss):
    """
    อ่านตารางสต็อกจากชีตใดก็ได้ในไฟล์ แล้วหาคอลัมน์โดยอิงชื่อแบบยืดหยุ่น
    ต้องการอย่างน้อย: ‘รหัส’, ‘ชื่อ’/‘ชื่ออุปกรณ์’, ‘คงเหลือ’, ‘ใช้งาน’
    ตัดรายการที่คงเหลือ > 0 และใช้งาน == Y
    คืน DataFrame: ['รหัส','ชื่อ']
    """
    # เลือกชีตแรกที่มีหัวตาราง
    ws = None
    for w in ss.worksheets():
        try:
            values = w.get_all_values()
            if not values:
                continue
            header = values[0]
            if len(header) >= 2:
                ws = w
                break
        except Exception:
            continue

    if ws is None:
        raise RuntimeError("ไม่พบชีตที่มีข้อมูล")

    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])

    # mapping หัวคอลัมน์
    cols = {_norm_colname(c): c for c in df.columns}

    code_col = None
    for cname in ["รหัส","code","itemcode","sku","productcode"]:
        if _norm_colname(cname) in cols: code_col = cols[_norm_colname(cname)]; break
        if cname in df.columns: code_col = cname; break
    if code_col is None:
        # เดา: คอลัมน์แรก
        code_col = df.columns[0]

    name_col = None
    for cname in ["ชื่อ","ชื่ออุปกรณ์","name","itemname","productname"]:
        if _norm_colname(cname) in cols: name_col = cols[_norm_colname(cname)]; break
        if cname in df.columns: name_col = cname; break
    if name_col is None:
        # เดา: คอลัมน์ที่ 2
        name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

    remain_col = None
    for cname in ["คงเหลือ","balance","remain","stock","quantity"]:
        if _norm_colname(cname) in cols: remain_col = cols[_norm_colname(cname)]; break

    active_col = None
    for cname in ["ใช้งาน","active","enabled","use","status"]:
        if _norm_colname(cname) in cols: active_col = cols[_norm_colname(cname)]; break

    # แปลงชนิดข้อมูล
    if remain_col:
        def to_int(x):
            try:
                return int(str(x).strip().replace(",",""))
            except:
                return 0
        df["_remain"] = df[remain_col].map(to_int)
    else:
        df["_remain"] = 1  # ถ้าไม่มีคอลัมน์คงเหลือ ให้ถือว่าพร้อมเบิกทุกชิ้น

    if active_col:
        df["_active"] = df[active_col].map(lambda x: str(x).strip().lower() in ["y","yes","1","true"])
    else:
        df["_active"] = True

    ready = df[(df["_active"]) & (df["_remain"] > 0)].copy()
    out = ready[[code_col, name_col]].rename(columns={code_col:"รหัส", name_col:"ชื่อ"}).dropna()
    out["รหัส"] = out["รหัส"].astype(str).str.strip()
    out["ชื่อ"]  = out["ชื่อ"].astype(str).str.strip()
    out = out.drop_duplicates(subset=["รหัส"]).reset_index(drop=True)
    return out


# =====================================================================================
# 4) --------------------------  UI: CARD LIST + +/- Qty  ------------------------------
# =====================================================================================
def render_mobile_list(df_ready: pd.DataFrame):
    selected = []

    for _, row in df_ready.iterrows():
        code = str(row.get("รหัส", ""))
        name = str(row.get("ชื่อ", ""))

        qty_key = f"qty_{code}"
        sel_key = f"sel_{code}"

        st.session_state.setdefault(qty_key, 0)
        st.session_state.setdefault(sel_key, False)

        st.markdown('<div class="card">', unsafe_allow_html=True)

        c_top1, c_top2 = st.columns([7,3])
        with c_top1:
            st.markdown(
                f'<div class="row-top"><div class="code">{code}</div>'
                f'<div class="badge">อุปกรณ์</div></div>', unsafe_allow_html=True
            )
            st.markdown(f'<div class="name">{name}</div>', unsafe_allow_html=True)
        with c_top2:
            st.checkbox("เลือก", key=sel_key)

        st.write("")
        c_qty1, c_qty2 = st.columns([6,6])
        with c_qty1:
            q1, q2, q3 = st.columns([1,2,1])

            if q1.button("−", key=f"minus_{code}"):
                st.session_state[qty_key] = max(0, int(st.session_state[qty_key]) - 1)

            q2.number_input("จำนวน", min_value=0, step=1, key=qty_key,
                            label_visibility="collapsed")

            if q3.button("+", key=f"plus_{code}"):
                st.session_state[qty_key] = int(st.session_state[qty_key]) + 1

        st.markdown("</div>", unsafe_allow_html=True)

        qty = int(st.session_state[qty_key])
        if st.session_state[sel_key] and qty > 0:
            selected.append({"รหัส": code, "ชื่อ": name, "จำนวน": qty})

    return selected


# =====================================================================================
# 5) -------------------------  WRITE ORDERS TO SHEET  --------------------------------
# =====================================================================================
def write_order_lines(ss, order_no, ts, branch_user, items):
    """
    บันทึกลงชีตชื่อ 'Orders' (สร้างให้ถ้าไม่มี)
    ฟอร์แมตแถวต่อรายการ: [timestamp, order_no, user, code, name, qty]
    """
    try:
        try:
            ws = ss.worksheet("Orders")
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title="Orders", rows=1000, cols=10)
            ws.append_row(["timestamp", "order_no", "user", "code", "name", "qty"])

        rows = [[ts, order_no, branch_user, it["รหัส"], it["ชื่อ"], int(it["จำนวน"])] for it in items]
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        return True, ""
    except Exception as e:
        return False, str(e)


# =====================================================================================
# 6) ---------------------------------  MAIN  ------------------------------------------
# =====================================================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📦", layout="wide")
    inject_mobile_css()

    # Header + Logo
    try:
        render_header_with_logo()
    except Exception:
        st.markdown(f"### {APP_TITLE}")

    # -------------------- Sidebar: Login (แบบง่าย) --------------------
    with st.sidebar:
        st.markdown("### เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
        user = st.text_input("ชื่อผู้ใช้", placeholder="เช่น branch01", key="branch_user")
        pwd  = st.text_input("รหัสผ่าน", type="password", key="branch_pass")
        st.caption("สำหรับสาธิต ระบบไม่ตรวจสอบรหัสจริง (ใช้เพื่อระบุชื่อผู้ทำรายการ)")
        if st.button("ล็อกอิน"):
            st.session_state["_login_ok"] = True
        if st.button("ออกจากระบบ"):
            st.session_state.clear()
            st.experimental_rerun()

    login_ok = st.session_state.get("_login_ok", True)  # demo: อนุญาตเข้าได้เลย
    branch_user = st.session_state.get("branch_user", "branch01") or "branch01"

    # -------------------- Connect Google Sheet --------------------
    df_ready = None
    error_connect = None
    try:
        gc = get_gspread_client()
        ss = open_spreadsheet(gc)
        df_ready = read_ready_items(ss)
    except Exception as e:
        error_connect = str(e)

    # ถ้าดึงข้อมูลไม่ได้ ให้มีโหมด demo
    if df_ready is None or df_ready.empty:
        if error_connect:
            with st.expander("รายละเอียดการเชื่อมต่อ/ข้อผิดพลาด (คลิกเพื่อดู)"):
                st.error(error_connect)
        # โหมดสาธิต
        df_ready = pd.DataFrame([
            {"รหัส":"INK-001","ชื่อ":"หมึกสีแดง"},
            {"รหัส":"TON-001","ชื่อ":"TONER BROTHER TN1000"},
            {"รหัส":"DRM-001","ชื่อ":"DRUM BROTHER DR-1000"},
        ])

    # -------------------- UI --------------------
    st.subheader("รายการอุปกรณ์ที่พร้อมให้เบิก", anchor=False)
    selected_items = render_mobile_list(df_ready)

    st.write("")
    c1, c2 = st.columns([6,6])
    with c1:
        if st.button("ล้างข้อมูล", type="secondary", use_container_width=True):
            for code in df_ready["รหัส"].astype(str):
                st.session_state.pop(f"sel_{code}", None)
                st.session_state.pop(f"qty_{code}", None)
            st.success("ล้างรายการเรียบร้อย")
    with c2:
        if st.button("เบิกอุปกรณ์", type="primary", use_container_width=True):
            if not selected_items:
                st.warning("กรุณาเลือกอุปกรณ์และระบุจำนวนก่อน")
            else:
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                order_no = "ORD-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") \
                           + "-" + "".join(random.choices(string.ascii_uppercase+string.digits, k=4))

                saved_ok, err = (True, "")
                # ถ้ามีการเชื่อมต่อชีตสำเร็จ ให้บันทึกจริง
                if 'ss' in locals():
                    saved_ok, err = write_order_lines(ss, order_no, ts, branch_user, selected_items)

                if saved_ok:
                    st.success("ทำรายการเบิกสำเร็จ")
                    st.markdown(f"**Order No.:** `{order_no}`")
                    st.markdown(f"**วันที่-เวลา:** {ts}")
                    st.markdown(f"**ผู้ทำรายการ:** `{branch_user}`")
                    st.markdown("**รายการที่เบิก**")
                    for it in selected_items:
                        st.markdown(f"- `{it['รหัส']}` — {it['ชื่อ']} × **{it['จำนวน']}**")
                else:
                    st.error("บันทึกไม่สำเร็จ")
                    st.code(err)

    # ใส่ช่องหมายเหตุ/ประกาศด้านล่าง (ถ้าต้องการ)
    st.write("")
    st.info("หากไม่พบข้อมูลจากชีต ระบบจะแสดงรายการสาธิต — ตรวจสอบ Secrets: `gcp_service_account` + `SHEET_URL`/`SHEET_ID`")


# =====================================================================================
# 7) ---------------------------------  RUN  -------------------------------------------
# =====================================================================================
if __name__ == "__main__":
    main()
