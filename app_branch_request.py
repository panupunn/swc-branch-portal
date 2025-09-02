
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Table UI + Requests sheet integration + persistent OrderID

Version: v2025-09-02f

Changes from v4:
- After confirm, write rows to a new sheet **Requests** so that the main app ("คำขอเบิก (จากสาขา)") can see them.
- OrderID format: USERNAME+YYMMDD-XX (daily running per user). Running number derived from **Requests** (not Transactions).
- Show success message with OrderID **without immediate rerun**, so it doesn't disappear.
- Add "ประวัติคำขอ (20 ออเดอร์ล่าสุด)" table sourced from **Requests** grouped by RequestID.
- Keep Transactions writing optional for internal history; enabled by default.
- By default, DO NOT deduct stock when submitting requests (set `DEDUCT_STOCK_ON_REQUEST=False`).

Other existing features:
- Login fixes; flexible Users headers; bcrypt-first verify; compact Health Check.
- Table UI with checkbox + auto qty=1 on first tick.
"""
from __future__ import annotations
import os, json, time
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd

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
    user = st.session_state.get("user", {})
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

    # Build table for editor (hide stock)
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
            "จำนวนที่เบิก": st.column_config.NumberColumn("จำนวนที่เบิก", min_value=0, step=1),
            "หน่วย": st.column_config.TextColumn("หน่วย", disabled=True),
        },
        key="issue_table",
    )

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
        st.dataframe(chosen[["รหัส","รายการ","จำนวนที่เบิก","หน่วย"]], hide_index=True, use_container_width=True)
    else:
        st.info("ยังไม่เลือกรายการ")

    # Confirm
    if (not chosen.empty) and st.button("ยืนยันการเบิก", type="primary", use_container_width=True):
        # Validate vs stock silently
        full_items = _read_items_df(ss)
        insufficient = []
        for _, r in chosen.iterrows():
            code = str(r["รหัส"]); qty = int(r["จำนวนที่เบิก"])
            have = float(full_items[full_items["itemcode"].astype(str)==code].head(1).get("stock", pd.Series([0])).iloc[0] or 0)
            if qty > have:
                name = str(full_items[full_items["itemcode"].astype(str)==code].head(1).get("itemname", pd.Series([""])).iloc[0])
                insufficient.append((code, name, have, qty))
        if insufficient:
            st.error("สต็อกไม่พอ: " + ", ".join([f"{c} ({have} < {need})" for c,_,have,need in insufficient])); return

        order_id = _generate_order_id_from_requests(ss, user.get("username",""))
        now = time.strftime("%Y-%m-%d %H:%M:%S")

        # Build request rows
        req_rows = []
        for _, r in chosen.iterrows():
            code = str(r["รหัส"]); qty = int(r["จำนวนที่เบิก"])
            item_row = full_items[full_items["itemcode"].astype(str)==code].head(1).iloc[0]
            req_rows.append([ now, order_id, user.get("username",""), user.get("branch_code",""),
                              item_row.get("itemcode"), item_row.get("itemname"), qty, "Pending", "" ])

        # Build transaction rows (optional)
        tx_rows = []
        if WRITE_TRANSACTIONS:
            for _, r in chosen.iterrows():
                code = str(r["รหัส"]); qty = int(r["จำนวนที่เบิก"])
                item_row = full_items[full_items["itemcode"].astype(str)==code].head(1).iloc[0]
                tx_rows.append([ now, order_id, user.get("username",""), user.get("branch_code",""),
                                 item_row.get("itemcode"), item_row.get("itemname"), qty, "REQ", "" ])

        try:
            _append_req(ss, req_rows)
            if WRITE_TRANSACTIONS and tx_rows:
                _append_tx(ss, tx_rows)

            if DEDUCT_STOCK_ON_REQUEST:
                # Update stock immediately (optional)
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
                for _, r in chosen.iterrows():
                    code = str(r["รหัส"]); qty = int(r["จำนวนที่เบิก"])
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

    # Recent order history (Requests)
    st.subheader("ประวัติคำขอ (20 ออเดอร์ล่าสุด)")
    try:
        ws = _ensure_req_sheet(ss)
        vals = ws.get_all_values()
        if vals and len(vals) > 1:
            df_req = pd.DataFrame(vals[1:], columns=vals[0])
            df_req = _normalize(df_req)
            # keep only current user
            df_req = df_req[df_req["username"].astype(str).str.lower() == str(user.get("username","")).lower()]
            if not df_req.empty and "requestid" in df_req.columns:
                grp = (df_req
                       .groupby(["requestid"], as_index=False)
                       .agg({"qty": lambda s: sum([float(x) if str(x).replace('.','',1).isdigit() else 0 for x in s]),
                             "requesttime": "max"})
                       .sort_values("requesttime", ascending=False)
                       .head(20))
                grp = grp.rename(columns={"requestid":"เลขที่ออเดอร์","qty":"จำนวนรวม","requesttime":"เวลา"})
                st.dataframe(grp, use_container_width=True, hide_index=True)
            else:
                st.info("ยังไม่มีคำขอของผู้ใช้นี้")
        else:
            st.info("ยังไม่มีคำขอ")
    except Exception as e:
        st.error(f"อ่านประวัติคำขอไม่สำเร็จ: {e}")


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
            page_issue()
    else:
        menu = st.sidebar.radio("เมนู", options=["เข้าสู่ระบบ","Health Check"], index=0)
        if menu == "Health Check": page_health()
        else: page_login()


if __name__ == "__main__":
    main()
