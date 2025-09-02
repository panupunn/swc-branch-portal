
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์
Patched login + compact Health Check (v2025-09-02)

- Fixes recurring IndentationError around login flow (uses 4-space indentation consistently).
- Relaxes Users sheet requirements: only Username + (Password OR PasswordHash) are required.
  BranchCode is optional (derived/fallback when missing).
- Adds compact Health Check page that shows only:
    • keys found in secrets.toml
    • Spreadsheet title if connection is successful
- Safer password verification: bcrypt first when available, else plaintext.
- Does not touch other app areas to avoid side effects.
"""
from __future__ import annotations
import os, json, re, time
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd

# Optional dependencies: gspread, bcrypt
try:
    import gspread  # type: ignore
except Exception as e:  # pragma: no cover
    gspread = None

try:
    import bcrypt  # type: ignore
except Exception:
    bcrypt = None


# ------------------------- Utilities -------------------------
def _get_sa_dict_from_secrets() -> Optional[Dict[str, Any]]:
    """Return service account credentials dict from Streamlit secrets if present."""
    try:
        s = st.secrets
    except Exception:
        return None

    # Common placements
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

    # Flat keys (fallback)
    flat_keys = [
        "type", "project_id", "private_key_id", "private_key", "client_email",
        "client_id", "auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"
    ]
    if all(k in s for k in flat_keys):
        return {k: s[k] for k in flat_keys}

    return None


def _get_sheet_loc_from_secrets() -> Dict[str, str]:
    """Return dict with sheet_id or sheet_url if present in Streamlit secrets/env."""
    out: Dict[str, str] = {}
    try:
        s = st.secrets
    except Exception:
        s = {}

    candidates = ["SHEET_ID", "sheet_id", "SPREADSHEET_ID", "SHEET_URL", "sheet_url", "SPREADSHEET_URL"]
    for k in candidates:
        v = None
        if k in s:
            v = s[k]
        elif k in os.environ:
            v = os.environ.get(k)

        if v:
            if "URL" in k.upper():
                out["sheet_url"] = str(v)
            else:
                out["sheet_id"] = str(v)

    return out


def _open_spreadsheet() -> Any:
    """Open Google Spreadsheet using credentials + id/url from secrets/env. Returns gspread Spreadsheet."""
    if gspread is None:
        raise RuntimeError("gspread library is not available in this environment. Please add it to requirements.")

    sa = _get_sa_dict_from_secrets()
    if not sa:
        raise RuntimeError("Service Account credentials not found in secrets. Please set GOOGLE_SERVICE_ACCOUNT_JSON or gcp_service_account.")

    try:
        gc = gspread.service_account_from_dict(sa)
    except Exception as e:
        raise RuntimeError(f"Failed to build gspread client from service account: {e}")

    loc = _get_sheet_loc_from_secrets()
    if "sheet_id" in loc:
        try:
            ss = gc.open_by_key(loc["sheet_id"])
            return ss
        except Exception as e:
            raise RuntimeError(f"Cannot open spreadsheet by SHEET_ID: {e}")

    if "sheet_url" in loc:
        try:
            ss = gc.open_by_url(loc["sheet_url"])
            return ss
        except Exception as e:
            raise RuntimeError(f"Cannot open spreadsheet by SHEET_URL: {e}")

    raise RuntimeError("Missing SHEET_ID or SHEET_URL in secrets.toml/env.")


def _list_found_secret_keys() -> List[str]:
    found = []
    try:
        s = st.secrets
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in s:
            found.append("GOOGLE_SERVICE_ACCOUNT_JSON")
        if "gcp_service_account" in s:
            found.append("gcp_service_account")
        for k in ("SHEET_ID", "sheet_id", "SPREADSHEET_ID", "SHEET_URL", "sheet_url", "SPREADSHEET_URL"):
            if k in s:
                found.append(k)
    except Exception:
        pass
    # ENV fallback display (do not echo private keys)
    for k in ("SHEET_ID", "SPREADSHEET_ID", "SHEET_URL", "SPREADSHEET_URL"):
        if os.environ.get(k):
            found.append(k + " (env)")
    return found


# ------------------------- Data helpers -------------------------
CANONICAL_COLS = {
    "username": ["username", "user", "บัญชีผู้ใช้", "ชื่อผู้ใช้", "ชื่อเข้าใช้"],
    "branchcode": ["branchcode", "branch", "สาขา", "รหัสสาขา", "code"],
    "password": ["password", "รหัสผ่าน"],
    "passwordhash": ["passwordhash", "hash", "bcrypt", "passhash"],
    "displayname": ["displayname", "name", "ชื่อ", "ชื่อแสดง"],
    "active": ["active", "enabled", "สถานะ", "isactive"],
    "role": ["role", "ตำแหน่ง", "สิทธิ์"],
}

REQUIRED_ANY_OF = ["password", "passwordhash"]
REQUIRED_ALWAYS = ["username"]


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for canon, alts in CANONICAL_COLS.items():
        for col in df.columns:
            k = str(col).strip()
            if k.lower() in [a.lower() for a in alts]:
                mapping[col] = canon
    # also map exact canonical names
    for canon in CANONICAL_COLS:
        for col in df.columns:
            if str(col).strip().lower() == canon:
                mapping[col] = canon

    nd = df.rename(columns=mapping).copy()
    return nd


def _read_users_df(ss) -> pd.DataFrame:
    try:
        ws = ss.worksheet("Users")
    except Exception:
        # create if missing with minimal header
        ws = ss.add_worksheet(title="Users", rows=100, cols=10)
        ws.update("A1:F1", [["Username", "DisplayName", "Role", "PasswordHash", "Active", "BranchCode"]])

    values = ws.get_all_values()
    if not values:
        header = ["Username", "DisplayName", "Role", "PasswordHash", "Active", "BranchCode"]
        ws.update("A1:F1", [header])
        values = [header]

    # Build DF
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    df = _normalize_cols(df)
    return df


def _is_active(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    if s in ("n", "no", "0", "false", "inactive", "disabled"):
        return False
    return True


def _verify_password(row: pd.Series, raw_password: str) -> bool:
    ph = str(row.get("passwordhash") or "").strip()
    pw = str(row.get("password") or "").strip()

    if ph and bcrypt:
        try:
            return bcrypt.checkpw(raw_password.encode("utf-8"), ph.encode("utf-8"))
        except Exception:
            # fall back to plaintext below
            pass

    if pw:
        return raw_password == pw
    # if we have only hash but bcrypt missing, reject with message outside
    return False


def _derive_branch_code(ss, row: pd.Series) -> str:
    bc = str(row.get("branchcode") or "").strip()
    if bc:
        return bc

    # Try to read first branch from Branches sheet
    try:
        ws = ss.worksheet("Branches")
        vals = ws.get_all_values()
        if vals and len(vals) > 1:
            # heuristics: first column contains code
            if vals[0]:
                # find a header that looks like code
                code_idx = 0
                for i, h in enumerate(vals[0]):
                    if str(h).strip().lower() in ("code", "branchcode", "รหัสสาขา", "branch_code", "สาขา"):
                        code_idx = i
                        break
            else:
                code_idx = 0
            first_data = vals[1]
            if first_data and len(first_data) > code_idx:
                guess = str(first_data[code_idx]).strip()
                if guess:
                    return guess
    except Exception:
        pass
    return "SWC000"


# ------------------------- UI Pages -------------------------
def page_health_check():
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.header("🩺 Health Check — การเชื่อมต่อและโครงสร้างสเปรดชีต")

    # Row: found keys
    found_keys = _list_found_secret_keys()
    if found_keys:
        st.info("พบคีย์ใน secrets.toml: " + ", ".join(found_keys))
    else:
        st.warning("ไม่พบคีย์ใน secrets.toml/ENV (โปรดตั้งค่า GOOGLE_SERVICE_ACCOUNT_JSON และ SHEET_ID/SHEET_URL)")

    # Row: spreadsheet connection
    try:
        ss = _open_spreadsheet()
        # Only show the title line (compact per user's request)
        try:
            title = ss.title  # gspread Spreadsheet.title
        except Exception:
            title = "(unknown)"
        st.success(f"เชื่อมต่อได้: {title}")
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่สำเร็จ: {e}")


def _ensure_session_defaults():
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
    if "user" not in st.session_state:
        st.session_state["user"] = {}


def page_login():
    _ensure_session_defaults()

    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    username_input = st.sidebar.text_input("ชื่อผู้ใช้", key="login_username")
    password_input = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_password")
    login_clicked = st.sidebar.button("ล็อกอิน", use_container_width=True)

    # Early return if not clicked
    if not login_clicked:
        if not st.session_state["auth"]:
            st.session_state["auth"] = False
        st.title("WishCo Branch Portal — เบิกอุปกรณ์")
        return

    # On click: perform login
    try:
        ss = _open_spreadsheet()
        df = _read_users_df(ss)
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่ได้หรืออ่านแท็บ Users ไม่ได้: {e}")
        return

    # Validate header presence
    cols_lower = [c.lower() for c in df.columns]
    missing_always = [c for c in REQUIRED_ALWAYS if c not in cols_lower]
    has_any_pwd = any(x in cols_lower for x in REQUIRED_ANY_OF)
    if missing_always or not has_any_pwd:
        st.error("Users sheet ไม่ครบคอลัมน์ (ต้องมี Username และ Password หรือ PasswordHash)")
        return

    # Find user (case-insensitive)
    uname = str(username_input or "").strip().lower()
    if not uname:
        st.error("กรุณากรอกชื่อผู้ใช้")
        return

    dfn = df.copy()
    dfn["username_norm"] = dfn["username"].astype(str).str.strip().str.lower()
    row = dfn[dfn["username_norm"] == uname].head(1)
    if row.empty:
        st.error("ไม่พบบัญชีผู้ใช้")
        return

    r = row.iloc[0]

    # Active check (if present)
    if "active" in dfn.columns and not _is_active(r.get("active")):
        st.error("บัญชีนี้ถูกปิดการใช้งาน")
        return

    # Verify password
    raw_pw = str(password_input or "")
    ok = _verify_password(r, raw_pw)
    if not ok and r.get("passwordhash") and not bcrypt:
        st.error("ระบบใช้รหัสผ่านแบบแฮช แต่เซิร์ฟเวอร์ยังไม่มีไลบรารี bcrypt")
        return
    if not ok:
        st.error("รหัสผ่านไม่ถูกต้อง")
        return

    # Build user session
    user_info = {
        "username": str(r.get("username") or "").strip(),
        "displayname": str(r.get("displayname") or ""),
        "role": str(r.get("role") or ""),
        "branch_code": _derive_branch_code(ss, r),
    }
    st.session_state["user"] = user_info
    st.session_state["auth"] = True
    st.success("ล็อกอินสำเร็จ — กำลังเข้าสู่ระบบ...")
    st.experimental_rerun()


def page_portal():
    """Main portal shown after login. Keep simple to avoid indentation mistakes."""
    _ensure_session_defaults()
    if not st.session_state["auth"]:
        st.warning("กรุณาเข้าสู่ระบบก่อน")
        return
    user = st.session_state.get("user", {})
    username = user.get("username", "")
    branch = user.get("branch_code", "")
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.write(f"ผู้ใช้: **{username}** | สาขา: **{branch}**")
    st.info("เข้าสู่หน้าเบิกอุปกรณ์ (เนื้อหาจริงของพอร์ตัลสามารถใส่ต่อจากนี้ได้)")


# ------------------------- App entry -------------------------
def main():
    st.set_page_config(page_title="WishCo Branch Portal", layout="wide", page_icon="🧰")
    _ensure_session_defaults()

    menu = st.sidebar.radio("เมนู", options=["เข้าสู่ระบบ", "Health Check"], index=1 if st.session_state.get("force_health", False) else 0)
    if menu == "Health Check":
        page_health_check()
    else:
        if st.session_state.get("auth"):
            page_portal()
        else:
            page_login()


if __name__ == "__main__":
    main()
