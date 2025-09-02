
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Full flow with selection & +/- controls + OrderID (username+YYMMDD-XX)

Version: v2025-09-02d

- Login fixes (indentation, bcrypt-first, flexible Users header)
- Health Check (compact)
- Issue page:
  * Hide stock column in UI
  * Select rows with checkbox, per-row +/- controls for quantity
  * Summary of selected items
  * OrderID format: <USERNAME><YYMMDD>-<XX> (01-99 per user per day, auto-run)
  * Append to Transactions (history) and update Items stock
  * Show recent history by order for the current user
"""
from __future__ import annotations
import os, json, time, re, uuid
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


# ----------------------------- Streamlit helpers -----------------------------
def _ensure_session_defaults():
    if "auth" not in st.session_state: st.session_state["auth"] = False
    if "user" not in st.session_state: st.session_state["user"] = {}
    if "qty_map" not in st.session_state: st.session_state["qty_map"] = {}  # {ItemCode: qty}
    if "sel_map" not in st.session_state: st.session_state["sel_map"] = {}  # {ItemCode: bool}


def _safe_rerun():
    try: st.rerun()
    except Exception:
        try: st.experimental_rerun()
        except Exception: pass


# ----------------------------- Secrets / Sheets ------------------------------
def _get_sa_dict_from_secrets() -> Optional[Dict[str, Any]]:
    try:
        s = st.secrets
    except Exception:
        return None

    if "GOOGLE_SERVICE_ACCOUNT_JSON" in s:
        raw = s["GOOGLE_SERVICE_ACCOUNT_JSON"]
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                pass
        if isinstance(raw, dict):
            return dict(raw)

    if "gcp_service_account" in s and isinstance(s["gcp_service_account"], dict):
        return dict(s["gcp_service_account"])

    flat = [
        "type","project_id","private_key_id","private_key","client_email",
        "client_id","auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"
    ]
    try:
        if all(k in s for k in flat):
            return {k: s[k] for k in flat}
    except Exception:
        pass
    return None


def _get_sheet_loc_from_secrets() -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        s = st.secrets
    except Exception:
        s = {}
    for k in ["SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"]:
        v = None
        if isinstance(s, dict) and k in s: v = s[k]
        if not v: v = os.environ.get(k)
        if v:
            if "URL" in k.upper(): out["sheet_url"] = str(v)
            else: out["sheet_id"] = str(v)
    return out


def _open_spreadsheet():
    if gspread is None:
        raise RuntimeError("gspread not available. Add it to requirements.")
    sa = _get_sa_dict_from_secrets()
    if not sa:
        raise RuntimeError("Service Account not found in secrets.")
    gc = gspread.service_account_from_dict(sa)
    loc = _get_sheet_loc_from_secrets()
    if "sheet_id" in loc: return gc.open_by_key(loc["sheet_id"])
    if "sheet_url" in loc: return gc.open_by_url(loc["sheet_url"])
    raise RuntimeError("Missing SHEET_ID or SHEET_URL in secrets.")


def _list_found_secret_keys() -> List[str]:
    found = []
    try:
        s = st.secrets
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in s: found.append("GOOGLE_SERVICE_ACCOUNT_JSON")
        if "gcp_service_account" in s: found.append("gcp_service_account")
        for k in ("SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"):
            if k in s: found.append(k)
    except Exception:
        pass
    for k in ("SHEET_ID","SPREADSHEET_ID","SHEET_URL","SPREADSHEET_URL"):
        if os.environ.get(k): found.append(k+" (env)")
    return found


# ----------------------------- Data helpers ---------------------------------
CANONICAL_COLS = {
    "username": ["username","user","บัญชีผู้ใช้","ชื่อผู้ใช้","ชื่อเข้าใช้"],
    "branchcode": ["branchcode","branch","สาขา","รหัสสาขา","code"],
    "password": ["password","รหัสผ่าน"],
    "passwordhash": ["passwordhash","hash","bcrypt","passhash"],
    "displayname": ["displayname","name","ชื่อ","ชื่อแสดง"],
    "active": ["active","enabled","สถานะ","isactive"],
    "role": ["role","ตำแหน่ง","สิทธิ์"],

    "itemcode": ["itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์"],
    "itemname": ["itemname","name","สินค้า","ชื่อสินค้า","ชื่ออุปกรณ์","รายการ"],
    "stock": ["stock","คงเหลือ","จำนวนคงคลัง"],
    "unit": ["unit","หน่วย","หน่วยนับ"],
    "category": ["category","หมวดหมู่","ประเภท"],
}
REQUIRED_ANY_OF = ["password","passwordhash"]
REQUIRED_ALWAYS = ["username"]


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    lowers = {str(c).strip().lower(): c for c in df.columns}
    for canon, alts in CANONICAL_COLS.items():
        for a in alts+[canon]:
            if a.lower() in lowers:
                mapping[lowers[a.lower()]] = canon
    return df.rename(columns=mapping).copy()


# ----- Users -----
def _read_users_df(ss) -> pd.DataFrame:
    try: ws = ss.worksheet("Users")
    except Exception:
        ws = ss.add_worksheet(title="Users", rows=100, cols=10)
        ws.update("A1:F1", [["Username","DisplayName","Role","PasswordHash","Active","BranchCode"]])
    values = ws.get_all_values()
    if not values:
        header = ["Username","DisplayName","Role","PasswordHash","Active","BranchCode"]
        ws.update("A1:F1", [header])
        values = [header]
    df = pd.DataFrame(values[1:], columns=values[0])
    return _normalize_cols(df)


# ----- Items -----
ITEMS_HEADER = ["ItemCode","ItemName","Stock","Unit","Category","Active"]
def _read_items_df(ss) -> pd.DataFrame:
    try: ws = ss.worksheet("Items")
    except Exception:
        ws = ss.add_worksheet(title="Items", rows=1000, cols=10)
        ws.update("A1:F1", [ITEMS_HEADER])
    values = ws.get_all_values()
    if not values:
        ws.update("A1:F1", [ITEMS_HEADER]); values = [ITEMS_HEADER]
    df = pd.DataFrame(values[1:], columns=values[0])
    df = _normalize_cols(df)
    # Standardize numeric stock
    if "stock" in df.columns:
        def _to_num(x):
            try:
                return float(str(x).replace(",","").strip()) if str(x).strip()!="" else 0.0
            except Exception:
                return 0.0
        df["stock"] = df["stock"].map(_to_num)
    return df


# ----- Transactions -----
# We'll keep legacy columns and use TxID as OrderID for compatibility.
TX_LEGACY_HEADER = ["TxTime","TxID","Username","BranchCode","ItemCode","ItemName","Qty","Type","Note"]
def _ensure_transactions_sheet(ss):
    try:
        ws = ss.worksheet("Transactions")
        header = ws.get_all_values()[0] if ws.get_all_values() else []
        if not header:
            ws.update("A1:I1", [TX_LEGACY_HEADER])
    except Exception:
        ws = ss.add_worksheet(title="Transactions", rows=1000, cols=15)
        ws.update("A1:I1", [TX_LEGACY_HEADER])
    return ss.worksheet("Transactions")


def _append_transactions(ss, rows: List[List[Any]]):
    ws = _ensure_transactions_sheet(ss)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


# ----------------------------- Business rules -------------------------------
def _is_active(val) -> bool:
    if val is None: return True
    s = str(val).strip().lower()
    return s not in ("n","no","0","false","inactive","disabled")


def _verify_password(row: pd.Series, raw: str) -> bool:
    ph = str(row.get("passwordhash") or "").strip()
    pw = str(row.get("password") or "").strip()
    if ph and bcrypt:
        try:
            return bcrypt.checkpw(raw.encode("utf-8"), ph.encode("utf-8"))
        except Exception:
            pass
    if pw:
        return raw == pw
    return False


def _derive_branch_code(ss, row: pd.Series) -> str:
    bc = str(row.get("branchcode") or "").strip()
    if bc: return bc
    # Try first Branch in "Branches"
    try:
        ws = ss.worksheet("Branches")
        vals = ws.get_all_values()
        if vals and len(vals) > 1:
            header = [h.strip().lower() for h in vals[0]] if vals[0] else []
            idx = 0
            for i,h in enumerate(header):
                if h in ("code","branchcode","รหัสสาขา","branch_code","สาขา"):
                    idx = i; break
            guess = str(vals[1][idx]).strip() if len(vals[1])>idx else ""
            if guess: return guess
    except Exception:
        pass
    return "SWC000"


def _generate_order_id(ss, username: str) -> str:
    """username + YYMMDD-XX (per user per day)"""
    uname = (username or "").strip().upper()
    ymd = time.strftime("%y%m%d")
    prefix = f"{uname}{ymd}-"
    ws = _ensure_transactions_sheet(ss)
    vals = ws.get_all_values()
    max_run = 0
    if vals and len(vals) > 1:
        # Find in TxID column (index 1)
        for r in vals[1:]:
            txid = r[1] if len(r)>1 else ""
            if isinstance(txid, str) and txid.startswith(prefix) and len(txid) >= len(prefix)+2:
                suf = txid[len(prefix):len(prefix)+2]
                if suf.isdigit():
                    max_run = max(max_run, int(suf))
    next_run = min(max_run + 1, 99)
    return f"{prefix}{next_run:02d}"


# ----------------------------- UI: Health Check -----------------------------
def page_health_check():
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.header("🩺 Health Check — การเชื่อมต่อและโครงสร้างสเปรดชีต")
    found = _list_found_secret_keys()
    if found:
        st.info("พบคีย์ใน secrets.toml: " + ", ".join(found))
    else:
        st.warning("ไม่พบคีย์ใน secrets.toml/ENV")
    try:
        ss = _open_spreadsheet()
        st.success(f"เชื่อมต่อได้: {ss.title}")
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่สำเร็จ: {e}")


# ----------------------------- UI: Login ------------------------------------
def page_login():
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    uname = st.sidebar.text_input("ชื่อผู้ใช้", key="login_username")
    pw = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_password")
    clicked = st.sidebar.button("ล็อกอิน", use_container_width=True)

    if not clicked:
        st.title("WishCo Branch Portal — เบิกอุปกรณ์")
        return

    # Connect & read user
    try:
        ss = _open_spreadsheet(); df = _read_users_df(ss)
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่ได้หรืออ่านแท็บ Users ไม่ได้: {e}"); return

    cols_lower = [c.lower() for c in df.columns]
    missing_always = [c for c in REQUIRED_ALWAYS if c not in cols_lower]
    has_any_pwd = any(x in cols_lower for x in REQUIRED_ANY_OF)
    if missing_always or not has_any_pwd:
        st.error("Users sheet ไม่ครบคอลัมน์ (ต้องมี Username และ Password หรือ PasswordHash)"); return

    u = (uname or "").strip().lower()
    if not u:
        st.error("กรุณากรอกชื่อผู้ใช้"); return

    dfn = df.copy()
    dfn["username_norm"] = dfn["username"].astype(str).str.strip().str.lower()
    row = dfn[dfn["username_norm"] == u].head(1)
    if row.empty:
        st.error("ไม่พบบัญชีผู้ใช้"); return
    r = row.iloc[0]

    if "active" in dfn.columns and not _is_active(r.get("active")):
        st.error("บัญชีนี้ถูกปิดการใช้งาน"); return

    if not _verify_password(r, pw or ""):
        if r.get("passwordhash") and not bcrypt:
            st.error("ระบบใช้รหัสผ่านแบบแฮช แต่เซิร์ฟเวอร์ยังไม่มีไลบรารี bcrypt")
        else:
            st.error("รหัสผ่านไม่ถูกต้อง")
        return

    st.session_state["user"] = {
        "username": str(r.get("username") or "").strip(),
        "displayname": str(r.get("displayname") or ""),
        "role": str(r.get("role") or ""),
        "branch_code": _derive_branch_code(_open_spreadsheet(), r),
    }
    st.session_state["auth"] = True
    st.session_state["qty_map"] = {}
    st.session_state["sel_map"] = {}
    st.success("ล็อกอินสำเร็จ — กำลังกำหนดค่าสภาพแวดล้อม...")
    _safe_rerun()


# ----------------------------- UI: Issue Items ------------------------------
def _row_controls(code: str, name: str, default_qty: float = 0.0):
    sel_key = f"sel_{code}"
    qty_key = f"qty_{code}"

    if sel_key not in st.session_state:
        st.session_state[sel_key] = st.session_state["sel_map"].get(code, False)
    if qty_key not in st.session_state:
        st.session_state[qty_key] = st.session_state["qty_map"].get(code, default_qty)

    c1, c2, c3, c4, c5 = st.columns([0.08, 0.18, 0.56, 0.09, 0.09])
    with c1:
        sel = st.checkbox("", key=sel_key)
    with c2:
        st.write(f"**{code}**")
    with c3:
        st.write(name)
    with c4:
        if st.button("－", key=f"minus_{code}"):
            st.session_state[qty_key] = max(0, int(st.session_state[qty_key]) - 1)
    with c5:
        if st.button("＋", key=f"plus_{code}"):
            st.session_state[qty_key] = int(st.session_state[qty_key]) + 1

    # show qty right aligned under the row (small)
    st.caption(f"จำนวน: {int(st.session_state[qty_key])}")

    # Sync maps
    st.session_state["sel_map"][code] = st.session_state[sel_key]
    st.session_state["qty_map"][code] = int(st.session_state[qty_key])


def page_issue():
    user = st.session_state.get("user", {})
    if not user:
        st.warning("กรุณาเข้าสู่ระบบก่อน")
        return

    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.caption(f"ผู้ใช้: **{user.get('username','')}** | สาขา: **{user.get('branch_code','')}**")

    # Connect + load items
    try:
        ss = _open_spreadsheet()
        df_items = _read_items_df(ss)
    except Exception as e:
        st.error(f"เชื่อมต่อ/อ่าน Items ไม่สำเร็จ: {e}")
        return

    # Filter only active
    if "active" in df_items.columns:
        df_show = df_items[df_items["active"].apply(_is_active)].copy()
    else:
        df_show = df_items.copy()

    # Search
    q = st.text_input("ค้นหาชื่อ/รหัสอุปกรณ์", placeholder="พิมพ์คำค้น เช่น 'สาย HDMI' หรือ 'HDMI'")
    if q:
        s = q.strip().lower()
        df_show = df_show[
            df_show["itemname"].astype(str).str.lower().str.contains(s) |
            df_show["itemcode"].astype(str).str.lower().str.contains(s)
        ]

    st.markdown("##### เลือกรายการที่จะเบิก")
    header_cols = st.columns([0.08, 0.18, 0.56, 0.09, 0.09])
    header_cols[0].markdown("**เลือก**")
    header_cols[1].markdown("**รหัส**")
    header_cols[2].markdown("**รายการ**")
    header_cols[3].markdown("**ลด**")
    header_cols[4].markdown("**เพิ่ม**")

    # List rows with controls (no stock shown)
    for _, r in df_show.iterrows():
        code = str(r.get("itemcode"))
        name = str(r.get("itemname"))
        _row_controls(code, name)

    st.divider()

    # Build summary from selected items
    qty_map = st.session_state.get("qty_map", {})
    sel_map = st.session_state.get("sel_map", {})
    chosen = [(c, int(qty_map.get(c, 0))) for c, sel in sel_map.items() if sel and int(qty_map.get(c, 0)) > 0]
    if chosen:
        sub_df = df_items[df_items["itemcode"].astype(str).isin([c for c, _ in chosen])][["itemcode","itemname","unit","stock"]].copy()
        sub_df["จำนวนที่เบิก"] = sub_df["itemcode"].map(dict(chosen)).astype(int)
        show_df = sub_df[["itemcode","itemname","จำนวนที่เบิก","unit"]]
        st.subheader("สรุปรายการที่จะเบิก")
        st.dataframe(show_df.rename(columns={"itemcode":"รหัส","itemname":"รายการ","unit":"หน่วย"}),
                     use_container_width=True, hide_index=True)
    else:
        st.info("ยังไม่เลือกรายการ")

    # Commit
    if chosen and st.button("ยืนยันการเบิก", type="primary", use_container_width=True):
        # Validate vs stock (even though we hide from UI)
        insufficient = []
        for code, qty in chosen:
            r = df_items[df_items["itemcode"].astype(str) == str(code)].head(1)
            if r.empty:
                insufficient.append((code, "ไม่พบใน Items", 0, qty))
            else:
                have = float(r.iloc[0].get("stock") or 0)
                if qty > have:
                    insufficient.append((code, str(r.iloc[0].get("itemname")), have, qty))
        if insufficient:
            st.error("สต็อกไม่พอ: " + ", ".join([f"{c} ({have} < {need})" for c,_,have,need in insufficient]))
            return

        # Build rows
        order_id = _generate_order_id(ss, user.get("username",""))
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for code, qty in chosen:
            r = df_items[df_items["itemcode"].astype(str) == str(code)].iloc[0]
            rows.append([
                now, order_id, user.get("username",""), user.get("branch_code",""),
                r.get("itemcode"), r.get("itemname"), int(qty), "OUT", ""
            ])
        try:
            _append_transactions(ss, rows)
            # Update stock in Items
            ws = ss.worksheet("Items")
            values = ws.get_all_values()
            header = values[0] if values else ITEMS_HEADER
            # locate code and stock column
            code_idx = 0
            for i,h in enumerate(header):
                if str(h).strip().lower() in ("itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์"):
                    code_idx = i; break
            stock_idx = None
            for i,h in enumerate(header):
                if str(h).strip().lower() in ("stock","คงเหลือ","จำนวนคงคลัง"):
                    stock_idx = i; break
            if stock_idx is None:
                stock_idx = len(header)
                header.append("Stock")
                ws.update_cell(1, stock_idx+1, "Stock")
                values = ws.get_all_values()
            code_to_rownum = {}
            for rn in range(2, len(values)+1):
                row_vals = values[rn-1] if rn-1 < len(values) else []
                c = str(row_vals[code_idx]).strip() if len(row_vals)>code_idx else ""
                if c:
                    code_to_rownum[c] = rn
            batch_updates = []
            for code, qty in chosen:
                rn = code_to_rownum.get(str(code))
                if rn:
                    # find original stock from df_items
                    have = float(df_items[df_items["itemcode"].astype(str)==str(code)].iloc[0].get("stock") or 0)
                    new_stock = have - float(qty)
                    batch_updates.append({"range": f"{chr(ord('A')+stock_idx)}{rn}", "values": [[new_stock]]})
            if batch_updates:
                ws.batch_update([{"range": u["range"], "values": u["values"]} for u in batch_updates])

            st.success(f"บันทึกการเบิกเรียบร้อย เลขที่ออเดอร์: **{order_id}** | รายการ: {len(rows)}")
            # Clear selections
            for code,_ in chosen:
                st.session_state[f"sel_{code}"] = False
                st.session_state[f"qty_{code}"] = 0
            st.session_state["sel_map"] = {}
            st.session_state["qty_map"] = {}
        except Exception as e:
            st.error(f"ไม่สามารถบันทึกการเบิกได้: {e}")

    # Recent history
    with st.expander("ประวัติการเบิกล่าสุด (20 ออเดอร์ล่าสุด)"):
        try:
            ws = _ensure_transactions_sheet(ss)
            vals = ws.get_all_values()
            if vals and len(vals) > 1:
                df_tx = pd.DataFrame(vals[1:], columns=vals[0])
                df_tx = _normalize_cols(df_tx.rename(columns={"txid":"txid"}))
                # Keep only current user
                df_tx = df_tx[df_tx["username"].astype(str).str.lower() == str(user.get("username","")).lower()]
                # group by TxID/OrderID
                if "txid" in df_tx.columns:
                    grp = df_tx.groupby("txid").agg({"qty":"sum","txtime":"max"}).reset_index().sort_values("txtime", ascending=False).head(20)
                    grp = grp.rename(columns={"txid":"เลขที่ออเดอร์","qty":"จำนวนรวม","txtime":"เวลา"})
                    st.dataframe(grp, use_container_width=True, hide_index=True)
                else:
                    st.info("ไม่มีข้อมูล TxID ใน Transactions")
            else:
                st.info("ยังไม่มีประวัติ")
        except Exception as e:
            st.error(f"อ่านประวัติไม่สำเร็จ: {e}")


# ----------------------------- Main / Routing -------------------------------
def main():
    st.set_page_config(page_title="WishCo Branch Portal", layout="wide", page_icon="🧰")
    _ensure_session_defaults()

    if st.session_state.get("auth", False):
        menu = st.sidebar.radio("เมนู", options=["เบิกอุปกรณ์","Health Check","ออกจากระบบ"], index=0)
        if menu == "Health Check":
            page_health_check()
        elif menu == "ออกจากระบบ":
            st.session_state["auth"] = False
            st.session_state["user"] = {}
            st.session_state["qty_map"] = {}
            st.session_state["sel_map"] = {}
            st.success("ออกจากระบบแล้ว")
            _safe_rerun()
        else:
            page_issue()
    else:
        menu = st.sidebar.radio("เมนู", options=["เข้าสู่ระบบ","Health Check"], index=0)
        if menu == "Health Check":
            page_health_check()
        else:
            page_login()


if __name__ == "__main__":
    main()
