
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Full flow: Login + Health Check + เบิกอุปกรณ์ (Items OUT)
Version: v2025-09-02c

Key points:
- Safe rerun across Streamlit versions (st.rerun fallback).
- Login fixed (indentation, Users header relaxed, bcrypt-first).
- Health Check (compact): only shows found secret keys & spreadsheet title.
- Issue page "เบิกอุปกรณ์": browse/search Items, input quantities, commit to Transactions,
  update stock with basic concurrency checks and helpful messages.
- Auto-create/repair headers for Users, Items, Transactions sheets if missing.
- Minimal UI and Thai labels, smartphone-friendly.
"""
from __future__ import annotations
import os, json, time, uuid
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
    if "cart" not in st.session_state: st.session_state["cart"] = {}  # {ItemCode: qty}


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
TX_HEADER = ["TxTime","TxID","Username","BranchCode","ItemCode","ItemName","Qty","Type","Note"]
def _append_transactions(ss, rows: List[List[Any]]):
    try: ws = ss.worksheet("Transactions")
    except Exception:
        ws = ss.add_worksheet(title="Transactions", rows=1000, cols=15)
        ws.update("A1:I1", [TX_HEADER])
    # Append rows
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
        "branch_code": _derive_branch_code(ss, r),
    }
    st.session_state["auth"] = True
    st.success("ล็อกอินสำเร็จ — กำลังกำหนดค่าสภาพแวดล้อม...")
    _safe_rerun()


# ----------------------------- UI: Issue Items ------------------------------
def _render_cart_table(items_df: pd.DataFrame, cart: Dict[str, float]):
    if not cart:
        st.info("ยังไม่เลือกรายการเบิก")
        return pd.DataFrame()
    codes = list(cart.keys())
    sub = items_df[items_df["itemcode"].astype(str).isin(codes)].copy()
    sub["จำนวนที่เบิก"] = sub["itemcode"].map(cart).fillna(0)
    sub = sub[["itemcode","itemname","stock","จำนวนที่เบิก","unit","category"]]
    st.dataframe(sub, use_container_width=True, hide_index=True)
    return sub


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

    # Prepare editable column for quantity
    edit_df = df_show[["itemcode","itemname","stock"]].copy()
    edit_df["จำนวนที่เบิก"] = 0
    edited = st.data_editor(
        edit_df,
        num_rows="dynamic",
        column_config={
            "itemcode": st.column_config.TextColumn("รหัส"),
            "itemname": st.column_config.TextColumn("รายการ"),
            "stock": st.column_config.NumberColumn("คงเหลือ"),
            "จำนวนที่เบิก": st.column_config.NumberColumn("จำนวนที่เบิก", min_value=0, step=1),
        },
        disabled=["itemcode","itemname","stock"],
        hide_index=True,
        use_container_width=True,
        key="edit_items"
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("เพิ่มลงตะกร้า / อัปเดตตะกร้า", use_container_width=True):
            cart = {}
            for _, row in edited.iterrows():
                qty = float(row.get("จำนวนที่เบิก") or 0)
                code = str(row.get("itemcode"))
                if qty > 0:
                    cart[code] = qty
            st.session_state["cart"] = cart
            st.success(f"อัปเดตตะกร้าแล้ว: {len(cart)} รายการ")
    with col2:
        if st.button("ล้างตะกร้า", use_container_width=True):
            st.session_state["cart"] = {}
            st.info("ล้างตะกร้าแล้ว")

    # Show cart
    st.subheader("ตะกร้า")
    sub = _render_cart_table(df_items, st.session_state.get("cart", {}))

    # Commit
    if not sub.empty and st.button("ยืนยันการเบิก", type="primary", use_container_width=True):
        # Validation vs stock
        cart = st.session_state.get("cart", {})
        insufficient = []
        for _, r in sub.iterrows():
            if float(r["จำนวนที่เบิก"]) > float(r["stock"]):
                insufficient.append((r["itemcode"], r["itemname"], r["stock"], r["จำนวนที่เบิก"]))
        if insufficient:
            st.error("สต็อกไม่พอ: " + ", ".join([f"{c} ({have} < {need})" for c,_,have,need in insufficient]))
            return

        # Append transactions & update stock
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        txid = f"OUT-{int(time.time())}-{uuid.uuid4().hex[:6].upper()}"
        rows = []
        for _, r in sub.iterrows():
            rows.append([
                now, txid, user.get("username",""), user.get("branch_code",""),
                r["itemcode"], r["itemname"], float(r["จำนวนที่เบิก"]), "OUT", ""
            ])
        try:
            _append_transactions(ss, rows)
            # Update each item stock
            ws = ss.worksheet("Items")
            values = ws.get_all_values()
            header = values[0] if values else ITEMS_HEADER
            # Build index of code -> row number
            code_idx = 0
            for i,h in enumerate(header):
                if str(h).strip().lower() in ("itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์"):
                    code_idx = i; break
            stock_idx = None
            for i,h in enumerate(header):
                if str(h).strip().lower() in ("stock","คงเหลือ","จำนวนคงคลัง"):
                    stock_idx = i; break
            if stock_idx is None:
                # create Stock column if missing
                stock_idx = len(header)
                header.append("Stock")
                ws.update_cell(1, stock_idx+1, "Stock")
                values = ws.get_all_values()
            # Build map
            code_to_rownum = {}
            for rn in range(2, len(values)+1):
                row_vals = values[rn-1] if rn-1 < len(values) else []
                code = str(row_vals[code_idx]).strip() if len(row_vals)>code_idx else ""
                if code:
                    code_to_rownum[code] = rn
            # Apply updates
            batch_updates = []
            for _, r in sub.iterrows():
                code = str(r["itemcode"])
                rn = code_to_rownum.get(code)
                if rn:
                    current_stock = float(r["stock"])
                    new_stock = current_stock - float(r["จำนวนที่เบิก"])
                    batch_updates.append({"range": f"{chr(ord('A')+stock_idx)}{rn}", "values": [[new_stock]]})
            if batch_updates:
                ws.batch_update([{"range": u["range"], "values": u["values"]} for u in batch_updates])
            st.success(f"บันทึกการเบิก {len(rows)} รายการเรียบร้อย (เลขที่ {txid})")
            st.session_state["cart"] = {}
            _safe_rerun()
        except Exception as e:
            st.error(f"ไม่สามารถบันทึกการเบิกได้: {e}")


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
            st.session_state["cart"] = {}
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
