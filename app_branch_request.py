
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Patched login + compact Health Check (v2025-09-02b)

- Fixes AttributeError: st.experimental_rerun -> use st.rerun() with fallback.
- Keeps previous fixes: indentation cleanup, relaxed Users header, bcrypt-first verification,
  compact Health Check (only shows keys & spreadsheet title).
"""
from __future__ import annotations
import os, json
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
    if all(k in s for k in flat):
        return {k: s[k] for k in flat}
    return None


def _get_sheet_loc_from_secrets() -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        s = st.secrets
    except Exception:
        s = {}
    for k in ["SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"]:
        v = s.get(k) if isinstance(s, dict) else (st.secrets.get(k) if k in st.secrets else None)
        if not v:
            v = os.environ.get(k)
        if v:
            if "URL" in k.upper():
                out["sheet_url"] = str(v)
            else:
                out["sheet_id"] = str(v)
    return out


def _open_spreadsheet():
    if gspread is None:
        raise RuntimeError("gspread not available. Add it to requirements.")
    sa = _get_sa_dict_from_secrets()
    if not sa:
        raise RuntimeError("Service Account not found in secrets.")
    gc = gspread.service_account_from_dict(sa)
    loc = _get_sheet_loc_from_secrets()
    if "sheet_id" in loc:
        return gc.open_by_key(loc["sheet_id"])
    if "sheet_url" in loc:
        return gc.open_by_url(loc["sheet_url"])
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
        if os.environ.get(k):
            found.append(k+" (env)")
    return found


CANONICAL_COLS = {
    "username": ["username","user","บัญชีผู้ใช้","ชื่อผู้ใช้","ชื่อเข้าใช้"],
    "branchcode": ["branchcode","branch","สาขา","รหัสสาขา","code"],
    "password": ["password","รหัสผ่าน"],
    "passwordhash": ["passwordhash","hash","bcrypt","passhash"],
    "displayname": ["displayname","name","ชื่อ","ชื่อแสดง"],
    "active": ["active","enabled","สถานะ","isactive"],
    "role": ["role","ตำแหน่ง","สิทธิ์"],
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


def _read_users_df(ss) -> pd.DataFrame:
    try:
        ws = ss.worksheet("Users")
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


def _ensure_session_defaults():
    if "auth" not in st.session_state: st.session_state["auth"] = False
    if "user" not in st.session_state: st.session_state["user"] = {}

def _safe_rerun():
    # Streamlit ≥ 1.29 has st.rerun(); experimental_rerun removed in some builds.
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass


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


def page_login():
    _ensure_session_defaults()
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    uname = st.sidebar.text_input("ชื่อผู้ใช้", key="login_username")
    pw = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_password")
    clicked = st.sidebar.button("ล็อกอิน", use_container_width=True)

    if not clicked:
        st.title("WishCo Branch Portal — เบิกอุปกรณ์")
        return

    try:
        ss = _open_spreadsheet()
        df = _read_users_df(ss)
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่ได้หรืออ่านแท็บ Users ไม่ได้: {e}")
        return

    cols_lower = [c.lower() for c in df.columns]
    missing_always = [c for c in REQUIRED_ALWAYS if c not in cols_lower]
    has_any_pwd = any(x in cols_lower for x in REQUIRED_ANY_OF)
    if missing_always or not has_any_pwd:
        st.error("Users sheet ไม่ครบคอลัมน์ (ต้องมี Username และ Password หรือ PasswordHash)")
        return

    u = (uname or "").strip().lower()
    if not u:
        st.error("กรุณากรอกชื่อผู้ใช้")
        return

    dfn = df.copy()
    dfn["username_norm"] = dfn["username"].astype(str).str.strip().str.lower()
    row = dfn[dfn["username_norm"] == u].head(1)
    if row.empty:
        st.error("ไม่พบบัญชีผู้ใช้")
        return

    r = row.iloc[0]
    if "active" in dfn.columns and not _is_active(r.get("active")):
        st.error("บัญชีนี้ถูกปิดการใช้งาน")
        return

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


def page_portal():
    _ensure_session_defaults()
    if not st.session_state["auth"]:
        st.warning("กรุณาเข้าสู่ระบบก่อน")
        return
    user = st.session_state.get("user", {})
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.write(f"ผู้ใช้: **{user.get('username','')}** | สาขา: **{user.get('branch_code','')}**")
    st.info("เข้าสู่หน้าเบิกอุปกรณ์ (ใส่เนื้อหาจริงต่อจากนี้)")


def main():
    st.set_page_config(page_title="WishCo Branch Portal", layout="wide", page_icon="🧰")
    _ensure_session_defaults()
    menu = st.sidebar.radio("เมนู", options=["เข้าสู่ระบบ","Health Check"], index=0)
    if menu == "Health Check":
        page_health_check()
    else:
        if st.session_state.get("auth", False):
            page_portal()
        else:
            page_login()


if __name__ == "__main__":
    main()
