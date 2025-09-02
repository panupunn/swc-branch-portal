
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Summary table editable with in-cell +/- (AgGrid), Requests history slider, clear-all.

Version: v2025-09-02g

Requirements (add to requirements.txt):
- streamlit-aggrid>=0.3.4
"""

from __future__ import annotations
import os, json, time
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd

# Optional dependency
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode  # type: ignore
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

try:
    import gspread  # type: ignore
except Exception:
    gspread = None

try:
    import bcrypt  # type: ignore
except Exception:
    bcrypt = None


# ----------------------------- Settings -------------------------------------
DEDUCT_STOCK_ON_REQUEST = False   # change to True if you want to decrease stock immediately
WRITE_TRANSACTIONS     = True     # keep Transactions history as well


# ----------------------------- Helpers --------------------------------------
def _ensure_session_defaults():
    if "auth" not in st.session_state: st.session_state["auth"] = False
    if "user" not in st.session_state: st.session_state["user"] = {}
    if "sel_map" not in st.session_state: st.session_state["sel_map"] = {}  # {code: bool}
    if "qty_map" not in st.session_state: st.session_state["qty_map"] = {}  # {code: int}
    if "last_order_id" not in st.session_state: st.session_state["last_order_id"] = ""


def _safe_rerun():
    try: st.rerun()
    except Exception:
        try: st.experimental_rerun()
        except Exception: pass


def _get_sa_dict_from_secrets():
    try: s = st.secrets
    except Exception: return None
    if "GOOGLE_SERVICE_ACCOUNT_JSON" in s:
        raw = s["GOOGLE_SERVICE_ACCOUNT_JSON"]
        if isinstance(raw, str):
            try: return json.loads(raw)
            except Exception: pass
        if isinstance(raw, dict): return dict(raw)
    if "gcp_service_account" in s and isinstance(s["gcp_service_account"], dict):
        return dict(s["gcp_service_account"])
    flat = ["type","project_id","private_key_id","private_key","client_email","client_id","auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"]
    if all(k in s for k in flat):
        return {k:s[k] for k in flat}
    return None


def _get_sheet_loc_from_secrets():
    out = {}
    try: s = st.secrets
    except Exception: s = {}
    for k in ["SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"]:
        v = None
        if isinstance(s, dict) and k in s: v = s[k]
        if not v: v = os.environ.get(k)
        if v:
            if "URL" in k.upper(): out["sheet_url"] = str(v)
            else: out["sheet_id"] = str(v)
    return out


def _open_spreadsheet():
    if gspread is None: raise RuntimeError("gspread not available.")
    sa = _get_sa_dict_from_secrets()
    if not sa: raise RuntimeError("Service Account not found.")
    gc = gspread.service_account_from_dict(sa)
    loc = _get_sheet_loc_from_secrets()
    if "sheet_id" in loc: return gc.open_by_key(loc["sheet_id"])
    if "sheet_url" in loc: return gc.open_by_url(loc["sheet_url"])
    raise RuntimeError("Missing SHEET_ID or SHEET_URL.")


# Columns normalization
CANON = {
    "username": ["username","user","บัญชีผู้ใช้","ชื่อผู้ใช้","ชื่อเข้าใช้"],
    "password": ["password","รหัสผ่าน"],
    "passwordhash": ["passwordhash","hash","bcrypt"],
    "active": ["active","enabled","สถานะ"],
    "displayname": ["displayname","name","ชื่อ"],
    "role": ["role","สิทธิ์"],
    "branchcode": ["branchcode","branch","สาขา","รหัสสาขา","code"],

    "itemcode": ["itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์"],
    "itemname": ["itemname","name","สินค้า","ชื่อสินค้า","ชื่ออุปกรณ์","รายการ"],
    "stock": ["stock","คงเหลือ","จำนวนคงคลัง"],
    "unit": ["unit","หน่วย","หน่วยนับ"],

    "requestid": ["requestid","reqid","orderid","เลขที่ออเดอร์"],
    "txid": ["txid","orderid"],
    "txtime": ["txtime","time","datetime","timestamp"],
    "qty": ["qty","จำนวน"],
}
def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    lowers = {str(c).strip().lower(): c for c in df.columns}
    for canon, alts in CANON.items():
        for a in alts+[canon]:
            if a.lower() in lowers: mapping[lowers[a.lower()]] = canon
    return df.rename(columns=mapping).copy()


# Sheets readers/writers
def _read_users_df(ss) -> pd.DataFrame:
    try: ws = ss.worksheet("Users")
    except Exception:
        ws = ss.add_worksheet(title="Users", rows=100, cols=10)
        ws.update("A1:F1", [["Username","DisplayName","Role","PasswordHash","Active","BranchCode"]])
    vals = ws.get_all_values()
    if not vals:
        header = ["Username","DisplayName","Role","PasswordHash","Active","BranchCode"]
        ws.update("A1:F1", [header]); vals=[header]
    df = pd.DataFrame(vals[1:], columns=vals[0])
    return _normalize(df)


ITEMS_HEADER = ["ItemCode","ItemName","Stock","Unit","Category","Active"]
def _read_items_df(ss) -> pd.DataFrame:
    try: ws = ss.worksheet("Items")
    except Exception:
        ws = ss.add_worksheet(title="Items", rows=1000, cols=10)
        ws.update("A1:F1", [ITEMS_HEADER])
    vals = ws.get_all_values()
    if not vals:
        ws.update("A1:F1", [ITEMS_HEADER]); vals=[ITEMS_HEADER]
    df = pd.DataFrame(vals[1:], columns=vals[0])
    df = _normalize(df)
    if "stock" in df.columns:
        def _to_num(x):
            try: return float(str(x).replace(",","").strip()) if str(x).strip()!="" else 0.0
            except Exception: return 0.0
        df["stock"] = df["stock"].map(_to_num)
    return df


REQ_HEADER = ["RequestTime","RequestID","Username","BranchCode","ItemCode","ItemName","Qty","Status","Note"]
def _ensure_req_sheet(ss):
    try:
        ws = ss.worksheet("Requests")
        got = ws.get_all_values()
        if not got: ws.update("A1:I1", [REQ_HEADER])
    except Exception:
        ws = ss.add_worksheet(title="Requests", rows=1000, cols=15)
        ws.update("A1:I1", [REQ_HEADER])
    return ss.worksheet("Requests")


TX_HEADER = ["TxTime","TxID","Username","BranchCode","ItemCode","ItemName","Qty","Type","Note"]
def _ensure_tx_sheet(ss):
    try:
        ws = ss.worksheet("Transactions")
        got = ws.get_all_values()
        if not got: ws.update("A1:I1", [TX_HEADER])
    except Exception:
        ws = ss.add_worksheet(title="Transactions", rows=1000, cols=15)
        ws.update("A1:I1", [TX_HEADER])
    return ss.worksheet("Transactions")


def _append_req(ss, rows: List[List[Any]]):
    ws = _ensure_req_sheet(ss)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def _append_tx(ss, rows: List[List[Any]]):
    ws = _ensure_tx_sheet(ss)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


# Business rules
def _is_active(val)->bool:
    if val is None: return True
    s = str(val).strip().lower()
    return s not in ("n","no","0","false","inactive","disabled")


def _verify_pw(row, raw)->bool:
    ph = str(row.get("passwordhash") or "").strip()
    pw = str(row.get("password") or "").strip()
    if ph and bcrypt:
        try: return bcrypt.checkpw((raw or "").encode("utf-8"), ph.encode("utf-8"))
        except Exception: pass
    if pw: return (raw or "") == pw
    return False


def _derive_branch_code(ss, row)->str:
    bc = str(row.get("branchcode") or "").strip()
    return bc or "SWC000"


def _generate_order_id_from_requests(ss, username: str) -> str:
    uname = (username or "").strip().upper()
    ymd = time.strftime("%y%m%d")
    prefix = f"{uname}{ymd}-"
    ws = _ensure_req_sheet(ss)
    vals = ws.get_all_values()
    max_run = 0
    if vals and len(vals) > 1:
        for r in vals[1:]:
            rid = r[1] if len(r)>1 else ""
            if isinstance(rid, str) and rid.startswith(prefix) and len(rid) >= len(prefix)+2:
                suf = rid[len(prefix):len(prefix)+2]
                if suf.isdigit(): max_run = max(max_run, int(suf))
    return f"{prefix}{min(max_run+1,99):02d}"


# ----------------------------- UI Pages -------------------------------------
def page_health():
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.header("🩺 Health Check — การเชื่อมต่อและโครงสร้างสเปรดชีต")
    found = []
    try:
        s = st.secrets
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in s: found.append("GOOGLE_SERVICE_ACCOUNT_JSON")
        if "gcp_service_account" in s: found.append("gcp_service_account")
        for k in ("SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"):
            if k in s: found.append(k)
    except Exception: pass
    for k in ("SHEET_ID","SPREADSHEET_ID","SHEET_URL","SPREADSHEET_URL"):
        if os.environ.get(k): found.append(k+" (env)")
    if found: st.info("พบคีย์ใน secrets.toml: " + ", ".join(found))
    else: st.warning("ไม่พบคีย์ใน secrets.toml/ENV")
    try:
        ss = _open_spreadsheet()
        st.success(f"เชื่อมต่อได้: {ss.title}")
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่สำเร็จ: {e}")


def page_login():
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    uname = st.sidebar.text_input("ชื่อผู้ใช้", key="login_username")
    pw = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_password")
    if st.sidebar.button("ล็อกอิน", use_container_width=True):
        try:
            ss = _open_spreadsheet(); df = _read_users_df(ss)
        except Exception as e:
            st.error(f"เชื่อมต่อสเปรดชีตไม่ได้หรืออ่านแท็บ Users ไม่ได้: {e}"); return
        cols = [c.lower() for c in df.columns]
        if "username" not in cols or not (("password" in cols) or ("passwordhash" in cols)):
            st.error("Users sheet ไม่ครบคอลัมน์ (ต้องมี Username และ Password หรือ PasswordHash)"); return
        dfn = df.copy(); dfn["un_norm"] = dfn["username"].astype(str).str.strip().str.lower()
        row = dfn[dfn["un_norm"] == (uname or "").strip().lower()].head(1)
        if row.empty: st.error("ไม่พบบัญชีผู้ใช้"); return
        r = row.iloc[0]
        if "active" in dfn.columns and not _is_active(r.get("active")): st.error("บัญชีนี้ถูกปิดการใช้งาน"); return
        if not _verify_pw(r, pw): st.error("รหัสผ่านไม่ถูกต้อง"); return
        st.session_state["user"] = {
            "username": str(r.get("username") or "").strip(),
            "displayname": str(r.get("displayname") or ""),
            "role": str(r.get("role") or ""),
            "branch_code": _derive_branch_code(ss, r),
        }
        st.session_state["auth"] = True
        st.success("ล็อกอินสำเร็จ — กำลังกำหนดค่าสภาพแวดล้อม...")
        _safe_rerun()
    else:
        st.title("WishCo Branch Portal — เบิกอุปกรณ์")


def page_issue():
    # Predefine locals to avoid UnboundLocalError
    user = st.session_state.get("user", {})
    ss = None
    items = pd.DataFrame()

    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.caption(f"ผู้ใช้: **{user.get('username','')}** | สาขา: **{user.get('branch_code','')}**")

    # Load Items
    try:
        ss = _open_spreadsheet(); items = _read_items_df(ss)
    except Exception as e:
        st.error(f"เชื่อมต่อ/อ่าน Items ไม่สำเร็จ: {e}"); return

    # Active only
    if "active" in items.columns: items = items[items["active"].apply(_is_active)]

    # Search
    q = st.text_input("ค้นหาชื่อ/รหัสอุปกรณ์", placeholder="พิมพ์คำค้น เช่น 'สาย HDMI' หรือ 'HDMI'")
    if q:
        s = q.strip().lower()
        items = items[ items["itemname"].astype(str).str.lower().str.contains(s) | items["itemcode"].astype(str).str.lower().str.contains(s) ]

    # Build table for editor (hide stock in UI)
    codes = items["itemcode"].astype(str).tolist()
    sel_defaults = [bool(st.session_state["sel_map"].get(c, False)) for c in codes]
    qty_defaults = [int(st.session_state["qty_map"].get(c, 0)) for c in codes]

    table = pd.DataFrame({
        "เลือก": sel_defaults,
        "รหัส": codes,
        "รายการ": items["itemname"].astype(str).tolist(),
        "จำนวนที่เบิก": qty_defaults,
        "หน่วย": items.get("unit", pd.Series([""]*len(codes))).astype(str).tolist(),
    })

    edited = st.data_editor(
        table,
        hide_index=True,
        use_container_width=True,
        column_config={
            "เลือก": st.column_config.CheckboxColumn("เลือก"),
            "รหัส": st.column_config.TextColumn("รหัส", disabled=True),
            "รายการ": st.column_config.TextColumn("รายการ", disabled=True),
            "จำนวนที่เบิก": st.column_config.NumberColumn("จำนวนที่เบิก", min_value=0, step=1, format="%d"),
            "หน่วย": st.column_config.TextColumn("หน่วย", disabled=True),
        },
        key="issue_table",
    )

    # Clear-all selections (inline)
    if st.button("ล้างที่เลือกทั้งหมด", use_container_width=True):
        st.session_state["sel_map"].clear(); st.session_state["qty_map"].clear(); _safe_rerun()

    # Auto-qty=1 for newly ticked rows
    changed = False
    for i, row in edited.iterrows():
        code = row["รหัส"]
        selected = bool(row["เลือก"])
        qty = int(row["จำนวนที่เบิก"] or 0)
        prev_sel = st.session_state["sel_map"].get(code, False)
        if selected and (not prev_sel) and qty <= 0:
            edited.at[i, "จำนวนที่เบิก"] = 1
            qty = 1
            changed = True
        # persist
        st.session_state["sel_map"][code] = selected
        st.session_state["qty_map"][code] = qty

    if changed:
        _safe_rerun()

    # Summary
    chosen = edited[(edited["เลือก"] == True) & (edited["จำนวนที่เบิก"] > 0)].copy()
    if not chosen.empty:

        st.subheader("สรุปรายการที่จะเบิก")

        sum_df = chosen[["รหัส","รายการ","จำนวนที่เบิก","หน่วย"]].copy()
        sum_df["จำนวนที่เบิก"] = pd.to_numeric(sum_df["จำนวนที่เบิก"], errors="coerce").fillna(0).astype(int)

        used_aggrid = False
        if HAS_AGGRID:
            try:
                from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode  # type: ignore
                gb = GridOptionsBuilder.from_dataframe(sum_df, editable=True)
                js_plus_minus = JsCode("""
                class QtyRenderer {
                  init(params){
                    this.params = params;
                    this.eGui = document.createElement('div');
                    const val = Number(params.value || 0);
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'flex-end';
                    this.eGui.style.gap = '6px';
                    const btnMinus = document.createElement('button');
                    btnMinus.innerText = '−';
                    btnMinus.style.padding = '2px 8px';
                    btnMinus.style.border = '1px solid #ccc';
                    btnMinus.style.borderRadius = '6px';
                    btnMinus.addEventListener('click', ()=>{
                      const cur = Number(this.params.value || 0);
                      const next = Math.max(0, cur - 1);
                      this.params.setValue(next);
                    });
                    const span = document.createElement('span');
                    span.innerText = val.toString();
                    span.style.minWidth = '20px';
                    span.style.textAlign = 'center';
                    const btnPlus = document.createElement('button');
                    btnPlus.innerText = '+';
                    btnPlus.style.padding = '2px 8px';
                    btnPlus.style.border = '1px solid #ccc';
                    btnPlus.style.borderRadius = '6px';
                    btnPlus.addEventListener('click', ()=>{
                      const cur = Number(this.params.value || 0);
                      const next = cur + 1;
                      this.params.setValue(next);
                    });
                    this.eGui.appendChild(btnMinus);
                    this.eGui.appendChild(span);
                    this.eGui.appendChild(btnPlus);
                    this.span = span;
                  }
                  getGui(){ return this.eGui; }
                  refresh(params){
                    this.span.innerText = String(params.value || 0);
                    return true;
                  }
                }
                """)
                gb.configure_column("รหัส", editable=False, width=120)
                gb.configure_column("รายการ", editable=False)
                gb.configure_column("หน่วย", editable=False, width=100)
                gb.configure_column("จำนวนที่เบิก", editable=True, width=160,
                                    cellRenderer=js_plus_minus, valueParser=JsCode("function(x){return Number(x.newValue)||0;}"))
                gb.configure_grid_options(domLayout='autoHeight', suppressClickEdit=False)
                grid = AgGrid(sum_df, gridOptions=gb.build(), update_mode=GridUpdateMode.VALUE_CHANGED,
                              allow_unsafe_jscode=True, fit_columns_on_grid_load=True)
                updated = pd.DataFrame(grid.get("data", sum_df))
                for _, r in updated.iterrows():
                    st.session_state["qty_map"][str(r["รหัส"])] = int(r["จำนวนที่เบิก"] or 0)
                used_aggrid = True
            except Exception:
                used_aggrid = False
        if not used_aggrid:
            # fallback to Streamlit editor with numeric spinner
            sum_df2 = st.data_editor(
                sum_df, hide_index=True, use_container_width=True,
                column_config={
                    "รหัส": st.column_config.TextColumn("รหัส", disabled=True),
                    "รายการ": st.column_config.TextColumn("รายการ", disabled=True),
                    "จำนวนที่เบิก": st.column_config.NumberColumn("จำนวนที่เบิก", min_value=0, step=1, format="%d"),
                    "หน่วย": st.column_config.TextColumn("หน่วย", disabled=True),
                },
                key="summary_editor_fallback2",
            )
            for _, r in sum_df2.iterrows():
                st.session_state["qty_map"][str(r["รหัส"])] = int(r["จำนวนที่เบิก"] or 0)

    else:
        st.info("ยังไม่เลือกรายการ")

    # Confirm
    if (not chosen.empty) and st.button("ยืนยันการเบิก", type="primary", use_container_width=True):
        # Validate vs stock silently
        full_items = _read_items_df(ss)
        insufficient = []
        # Use session qty_map to ensure latest from summary grid
        current_map = st.session_state.get("qty_map", {})
        codes = [str(c) for c in chosen["รหัส"].tolist()]
        pairs = [(c, int(current_map.get(c, 0))) for c in codes if int(current_map.get(c, 0)) > 0]
        if not pairs:
            st.error("ไม่มีจำนวนที่เบิก"); return
        for code, qty in pairs:
            have = float(full_items[full_items["itemcode"].astype(str)==code].head(1).get("stock", pd.Series([0])).iloc[0] or 0)
            if qty > have:
                name = str(full_items[full_items["itemcode"].astype(str)==code].head(1).get("itemname", pd.Series([""])).iloc[0])
                insufficient.append((code, name, have, qty))
        if insufficient:
            msg = "สต็อกไม่พอ: " + ", ".join([f"{c} ({have} < {need})" for c, _, have, need in insufficient])
            st.error(msg)
            return

        order_id = _generate_order_id_from_requests(ss, user.get("username",""))
        now = time.strftime("%Y-%m-%d %H:%M:%S")

        # Build rows
        req_rows, tx_rows = [], []
        for code, qty in pairs:
            item_row = full_items[full_items["itemcode"].astype(str)==code].head(1).iloc[0]
            req_rows.append([ now, order_id, user.get("username",""), user.get("branch_code",""),
                              item_row.get("itemcode"), item_row.get("itemname"), qty, "Pending", "" ])
            if WRITE_TRANSACTIONS:
                tx_rows.append([ now, order_id, user.get("username",""), user.get("branch_code",""),
                                 item_row.get("itemcode"), item_row.get("itemname"), qty, "REQ", "" ])
        try:
            _append_req(ss, req_rows)
            if WRITE_TRANSACTIONS and tx_rows: _append_tx(ss, tx_rows)
            if DEDUCT_STOCK_ON_REQUEST:
                ws = ss.worksheet("Items")
                values = ws.get_all_values()
                header = values[0] if values else ["ItemCode","ItemName","Stock","Unit","Category","Active"]
                code_idx = 0
                for i,h in enumerate(header):
                    if str(h).strip().lower() in ("itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์"): code_idx = i; break
                stock_idx = None
                for i,h in enumerate(header):
                    if str(h).strip().lower() in ("stock","คงเหลือ","จำนวนคงคลัง"): stock_idx = i; break
                if stock_idx is None:
                    stock_idx = len(header); header.append("Stock"); ws.update_cell(1, stock_idx+1, "Stock"); values = ws.get_all_values()
                code_to_row = {}
                for rn in range(2, len(values)+1):
                    row_vals = values[rn-1]
                    c = str(row_vals[code_idx]).strip() if len(row_vals)>code_idx else ""
                    if c: code_to_row[c] = rn
                batch = []
                for code, qty in pairs:
                    have = float(full_items[full_items["itemcode"].astype(str)==code].iloc[0].get("stock") or 0)
                    new_stock = have - qty
                    rn = code_to_row.get(code)
                    if rn: batch.append({"range": f"{chr(ord('A')+stock_idx)}{rn}", "values": [[new_stock]]})
                if batch: ws.batch_update([{"range": b["range"], "values": b["values"]} for b in batch])

            st.session_state["last_order_id"] = order_id
            st.success(f"ส่งคำขอเบิกเรียบร้อย เลขที่ออเดอร์: **{order_id}** | รายการ: {len(req_rows)}")
            st.info("คำขอถูกบันทึกลงชีต 'Requests' เรียบร้อยแล้ว (สถานะ: Pending)")
            st.session_state["sel_map"].clear(); st.session_state["qty_map"].clear()
        except Exception as e:
            st.error(f"ไม่สามารถบันทึกคำขอได้: {e}")



    # คำขอที่ถูกบันทึกลงชีต 'Requests' พร้อมไอคอนสถานะ และปุ่มยกเลิกเมื่อ Pending (robust cols)
    st.subheader("คำขอที่ส่งไป (ล่าสุด)")
    num_orders = st.slider("จำนวนออเดอร์ล่าสุดที่ต้องการดู", min_value=1, max_value=50, value=5, step=1)
    try:
        ws = _ensure_req_sheet(ss)
        vals = ws.get_all_values()
        if vals and len(vals) > 1:
            df_req = pd.DataFrame(vals[1:], columns=vals[0])
            df_req = _normalize(df_req)

            # resolve column names (case-insensitive fallback)
            def col(df, *names):
                for n in names:
                    if n in df.columns: return n
                # try case variants
                lowers = {c.lower(): c for c in df.columns}
                for n in names:
                    if n.lower() in lowers: return lowers[n.lower()]
                return None

            c_user = col(df_req, "username", "Username")
            c_id   = col(df_req, "requestid", "RequestID", "orderid")
            c_qty  = col(df_req, "qty", "Qty", "จำนวน")
            c_name = col(df_req, "itemname", "ItemName", "รายการ")
            c_stat = col(df_req, "status", "Status")
            c_time = col(df_req, "requesttime", "RequestTime", "time", "datetime")

            if c_user: df_req = df_req[df_req[c_user].astype(str).str.lower() == str(user.get("username","")).lower()]

            if c_qty:
                df_req["qty_num"] = pd.to_numeric(df_req[c_qty], errors="coerce").fillna(0).astype(float)
            else:
                df_req["qty_num"] = 0.0

            if c_name is None: c_name = c_id  # worst case fallback to show something
            if c_stat is None: df_req["__status__"] = "Pending"; c_stat="__status__"
            if c_time is None: df_req["__time__"] = ""; c_time="__time__"

            if c_id and not df_req.empty:
                df_req["pair"] = df_req[c_name].astype(str) + " (" + df_req["qty_num"].astype(int).astype(str) + ")"

                def agg_status(g):
                    sset = set(str(x).strip().lower() for x in g[c_stat].astype(str))
                    if "canceled" in sset or "cancelled" in sset: return "Canceled"
                    if "approved" in sset or "อนุมัติ" in sset: return "Approved"
                    return "Pending"

                grp = (df_req
                       .groupby([c_id], as_index=False)
                       .agg(รายการ=("pair", lambda s: ", ".join(list(s))),
                            จำนวนรวม=("qty_num", "sum"),
                            เวลา=(c_time, "max"),
                            สถานะ=(c_stat, agg_status))
                      ).sort_values("เวลา", ascending=False).head(num_orders)

                def status_icon(s):
                    ss = str(s).strip().lower()
                    return "🟡" if ss=="pending" else ("🟢" if ss=="approved" else "🔴")
                grp["ไอคอน"] = grp["สถานะ"].map(status_icon)

                show_df = grp.rename(columns={c_id:"เลขที่ออเดอร์"})[["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]].copy()
                show_df["จำนวนรวม"] = show_df["จำนวนรวม"].astype(int)
                st.dataframe(show_df, use_container_width=True, hide_index=True)

                # Cancel select for Pending
                pending_ids = show_df[show_df["สถานะ"]=="Pending"]["เลขที่ออเดอร์"].tolist()
                if pending_ids:
                    sel_id = st.selectbox("เลือกเลขที่ออเดอร์ (Pending) เพื่อยกเลิก", pending_ids, key="cancel_reqid")
                    if st.button("ยกเลิกออเดอร์นี้", type="secondary"):
                        header = vals[0]
                        lowers = {h.lower(): idx for idx,h in enumerate(header)}
                        idx_id = lowers.get("requestid", lowers.get("orderid"))
                        idx_user = lowers.get("username")
                        idx_status = lowers.get("status")
                        idx_note = lowers.get("note")
                        me = str(user.get("username","")).lower()
                        changes = []
                        from time import strftime
                        now = strftime("%Y-%m-%d %H:%M:%S")
                        for rnum in range(2, len(vals)+1):
                            row = vals[rnum-1]
                            rid = row[idx_id] if idx_id is not None and idx_id < len(row) else ""
                            uname = row[idx_user] if idx_user is not None and idx_user < len(row) else ""
                            stv = row[idx_status] if idx_status is not None and idx_status < len(row) else ""
                            if str(rid)==str(sel_id) and str(uname).lower()==me and str(stv).strip().lower()=="pending":
                                if idx_status is not None:
                                    changes.append({"range": f"{chr(ord('A')+idx_status)}{rnum}", "values": [["Canceled"]]})
                                if idx_note is not None:
                                    changes.append({"range": f"{chr(ord('A')+idx_note)}{rnum}", "values": [[f"Canceled by user at {now}"]]})
                        if changes:
                            ws.batch_update(changes)
                            st.success(f"ยกเลิกออเดอร์ {sel_id} สำเร็จ")
                            _safe_rerun()
                        else:
                            st.info("ออเดอร์นี้ไม่อยู่ในสถานะ Pending แล้ว")
                else:
                    st.caption("ไม่มีออเดอร์สถานะ Pending")
            else:
                st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), use_container_width=True, hide_index=True)
        else:
            st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), use_container_width=True, hide_index=True)
    except Exception as e:
        st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), use_container_width=True, hide_index=True)
def main():
    st.set_page_config(page_title="WishCo Branch Portal", layout="wide", page_icon="🧰")
    _ensure_session_defaults()
    if st.session_state.get("auth", False):
        menu = st.sidebar.radio("เมนู", options=["เบิกอุปกรณ์","Health Check","ออกจากระบบ"], index=0)
        if menu == "Health Check":
            page_health()
        elif menu == "ออกจากระบบ":
            st.session_state["auth"] = False; st.session_state["user"] = {}; st.session_state["sel_map"] = {}; st.session_state["qty_map"] = {}; st.session_state["last_order_id"] = ""
            st.success("ออกจากระบบแล้ว"); _safe_rerun()
        else:
            # Quick actions
                        page_issue()
    else:
        menu = st.sidebar.radio("เมนู", options=["เข้าสู่ระบบ","Health Check"], index=0)
        if menu == "Health Check": page_health()
        else: page_login()


if __name__ == "__main__":
    main()
