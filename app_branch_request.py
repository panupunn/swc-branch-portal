
# -*- coding: utf-8 -*-
"""
WishCo Branch Portal — เบิกอุปกรณ์ (v11)
- ตารางเลือกอุปกรณ์ + ปุ่มล้างใต้ตาราง
- บันทึกคำขอไปชีต 'Requests' (Pending) + เลขออเดอร์ USER+YYMMDD-XX
- ตาราง "คำขอที่ส่งไป (ล่าสุด)" แสดงไอคอนสถานะ 🟡/🟢/🔴
- ยกเลิกออเดอร์ได้เมื่อสถานะเป็น Pending (เลือกจาก dropdown)
"""

from __future__ import annotations
import os, json, time
from typing import Any, Dict, List

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


# ----------------------------- Helpers & Config ------------------------------
REQ_HEADER = ["RequestTime","RequestID","Username","BranchCode",
              "ItemCode","ItemName","Qty","Status","Note"]
TX_HEADER  = ["TxTime","TxID","Username","BranchCode",
              "ItemCode","ItemName","Qty","Type","Note"]

CANON = {
    "username":    ["username","user","บัญชีผู้ใช้","ชื่อผู้ใช้","ชื่อเข้าใช้","Username","User"],
    "password":    ["password","รหัสผ่าน","Password"],
    "passwordhash":["passwordhash","hash","bcrypt","PasswordHash"],
    "active":      ["active","enabled","สถานะ","Active"],
    "displayname": ["displayname","name","ชื่อ","DisplayName","Name"],
    "role":        ["role","สิทธิ์","Role"],
    "branchcode":  ["branchcode","branch","สาขา","รหัสสาขา","code","BranchCode","Branch"],

    "itemcode":    ["itemcode","code","รหัส","รหัสสินค้า","รหัสอุปกรณ์","ItemCode","Code"],
    "itemname":    ["itemname","name","สินค้า","ชื่อสินค้า","ชื่ออุปกรณ์","รายการ","ItemName","Name"],
    "stock":       ["stock","คงเหลือ","จำนวนคงคลัง","Stock"],
    "unit":        ["unit","หน่วย","หน่วยนับ","Unit"],

    "requestid":   ["requestid","orderid","เลขที่ออเดอร์","RequestID","OrderID"],
    "requesttime": ["requesttime","time","datetime","timestamp","RequestTime","Time"],
    "status":      ["status","สถานะ","Status"],
    "qty":         ["qty","จำนวน","Qty"],
    "note":        ["note","หมายเหตุ","Note"],
}

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to canonical keys when possible."""
    lowers = {str(c).strip().lower(): c for c in df.columns}
    mapping = {}
    for canon, alts in CANON.items():
        for name in alts + [canon]:
            key = name.lower()
            if key in lowers:
                mapping[lowers[key]] = canon
                break
    return df.rename(columns=mapping).copy()

def _ensure_session():
    for k, v in [("auth", False), ("user", {}),
                 ("sel_map", {}), ("qty_map", {}), ("last_order_id", "")]:
        if k not in st.session_state: st.session_state[k] = v

def _safe_rerun():
    try: st.rerun()
    except Exception:
        try: st.experimental_rerun()
        except Exception: pass

def _get_sa_dict_from_secrets():
    try: s = st.secrets
    except Exception: s = {}
    if isinstance(s, dict):
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in s:
            raw = s["GOOGLE_SERVICE_ACCOUNT_JSON"]
            if isinstance(raw, str):
                try: return json.loads(raw)
                except Exception: pass
            if isinstance(raw, dict): return dict(raw)
        if "gcp_service_account" in s and isinstance(s["gcp_service_account"], dict):
            return dict(s["gcp_service_account"])
        fields = ["type","project_id","private_key_id","private_key","client_email","client_id",
                  "auth_uri","token_uri","auth_provider_x509_cert_url","client_x509_cert_url"]
        if all(k in s for k in fields):
            return {k:s[k] for k in fields}
    return None

def _sheet_loc():
    out = {}
    try: s = st.secrets
    except Exception: s = {}
    for k in ("SHEET_ID","sheet_id","SPREADSHEET_ID","SHEET_URL","sheet_url","SPREADSHEET_URL"):
        v = None
        if isinstance(s, dict) and k in s: v = s[k]
        if not v: v = os.environ.get(k)
        if v:
            if "URL" in k.upper(): out["sheet_url"] = str(v)
            else: out["sheet_id"] = str(v)
    return out

def _open_spreadsheet():
    if gspread is None: raise RuntimeError("gspread not available")
    sa = _get_sa_dict_from_secrets()
    if not sa: raise RuntimeError("Service Account not found in secrets")
    gc = gspread.service_account_from_dict(sa)
    loc = _sheet_loc()
    if "sheet_id" in loc: return gc.open_by_key(loc["sheet_id"])
    if "sheet_url" in loc: return gc.open_by_url(loc["sheet_url"])
    raise RuntimeError("Missing SHEET_ID or SHEET_URL in secrets/env")

def _ensure_sheet(ss, title: str, header: List[str]) -> Any:
    try:
        ws = ss.worksheet(title)
        got = ws.get_all_values()
        if not got:
            ws.update(f"A1:{chr(ord('A')+len(header)-1)}1", [header])
        return ws
    except Exception:
        ws = ss.add_worksheet(title=title, rows=1000, cols=max(10, len(header)))
        ws.update(f"A1:{chr(ord('A')+len(header)-1)}1", [header])
        return ws

def _read_users_df(ss) -> pd.DataFrame:
    ws = _ensure_sheet(ss, "Users", ["Username","DisplayName","Role","PasswordHash","Active","BranchCode"])
    vals = ws.get_all_values()
    vals = vals if vals else [["Username","DisplayName","Role","PasswordHash","Active","BranchCode"]]
    df = pd.DataFrame(vals[1:], columns=vals[0])
    return _normalize(df)

def _read_items_df(ss) -> pd.DataFrame:
    ws = _ensure_sheet(ss, "Items", ["ItemCode","ItemName","Stock","Unit","Category","Active"])
    vals = ws.get_all_values()
    vals = vals if vals else [["ItemCode","ItemName","Stock","Unit","Category","Active"]]
    df = pd.DataFrame(vals[1:], columns=vals[0])
    df = _normalize(df)
    if "stock" in df.columns:
        def to_num(x):
            try:
                s = str(x).replace(",","").strip()
                return float(s) if s else 0.0
            except Exception: return 0.0
        df["stock"] = df["stock"].map(to_num)
    return df

def _requests_ws(ss):    return _ensure_sheet(ss, "Requests", REQ_HEADER)
def _transactions_ws(ss):return _ensure_sheet(ss, "Transactions", TX_HEADER)

def _append_rows(ws, rows: List[List[Any]]):
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def _is_active(val)->bool:
    s = str(val).strip().lower()
    return s not in ("n","no","0","false","inactive","disabled")

def _verify_pw(row, raw)->bool:
    ph = str(row.get("passwordhash") or "").strip()
    pw = str(row.get("password") or "").strip()
    if ph and bcrypt:
        try:
            return bcrypt.checkpw((raw or "").encode("utf-8"), ph.encode("utf-8"))
        except Exception:
            pass
    if pw:
        return (raw or "") == pw
    return False

def _branch_code(row)->str:
    bc = str(row.get("branchcode") or "").strip()
    return bc or "SWC000"

def _generate_order_id(ss, username: str) -> str:
    uname = (username or "").strip().upper()
    ymd = time.strftime("%y%m%d")
    prefix = f"{uname}{ymd}-"
    ws = _requests_ws(ss)
    vals = ws.get_all_values()
    mx = 0
    if vals and len(vals) > 1:
        for r in vals[1:]:
            rid = r[1] if len(r)>1 else ""
            if isinstance(rid, str) and rid.startswith(prefix) and len(rid) >= len(prefix)+2:
                suf = rid[len(prefix):len(prefix)+2]
                if suf.isdigit():
                    mx = max(mx, int(suf))
    return f"{prefix}{min(mx+1, 99):02d}"


# ----------------------------- Pages ----------------------------------------
def page_health():
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.header("🩺 Health Check")
    keys = []
    try:
        s = st.secrets
        for k in ("GOOGLE_SERVICE_ACCOUNT_JSON","gcp_service_account",
                  "SHEET_ID","SPREADSHEET_ID","SHEET_URL","SPREADSHEET_URL"):
            if k in s: keys.append(k)
    except Exception: pass
    for k in ("SHEET_ID","SPREADSHEET_ID","SHEET_URL","SPREADSHEET_URL"):
        if os.environ.get(k): keys.append(k+" (env)")
    if keys: st.info("พบคีย์เชื่อมต่อ: " + ", ".join(keys))
    try:
        ss = _open_spreadsheet()
        st.success(f"เชื่อมต่อสเปรดชีตได้: {ss.title}")
    except Exception as e:
        st.error(f"เชื่อมต่อไม่ได้: {e}")


def page_login():
    st.sidebar.subheader("เข้าสู่ระบบสำหรับสาขา/หน่วยงาน")
    u = st.sidebar.text_input("ชื่อผู้ใช้", key="login_username")
    p = st.sidebar.text_input("รหัสผ่าน", type="password", key="login_password")
    if st.sidebar.button("ล็อกอิน", use_container_width=True):
        try:
            ss = _open_spreadsheet()
            df = _read_users_df(ss)
        except Exception as e:
            st.error(f"เชื่อมต่อ/อ่าน Users ไม่สำเร็จ: {e}"); return
        df["un_norm"] = df["username"].astype(str).str.strip().str.lower()
        row = df[df["un_norm"] == (u or "").strip().lower()].head(1)
        if row.empty: st.error("ไม่พบบัญชีผู้ใช้"); return
        r = row.iloc[0]
        if "active" in df.columns and not _is_active(r.get("active")):
            st.error("บัญชีนี้ถูกปิดการใช้งาน"); return
        if not _verify_pw(r, p):
            st.error("รหัสผ่านไม่ถูกต้อง"); return
        st.session_state["user"] = {
            "username": str(r.get("username") or ""),
            "displayname": str(r.get("displayname") or ""),
            "branch_code": _branch_code(r),
        }
        st.session_state["auth"] = True
        st.success("ล็อกอินสำเร็จ"); _safe_rerun()
    else:
        st.title("WishCo Branch Portal — เบิกอุปกรณ์")


def _items_editor(ss):
    items = _read_items_df(ss)
    if "active" in items.columns:
        items = items[items["active"].apply(_is_active)]
    q = st.text_input("ค้นหาชื่อ/รหัสอุปกรณ์", placeholder="พิมพ์คำค้น เช่น 'สาย HDMI' หรือ 'HDMI'")
    if q:
        s = q.strip().lower()
        items = items[ items["itemname"].astype(str).str.lower().str.contains(s) | 
                       items["itemcode"].astype(str).str.lower().str.contains(s) ]

    codes = items["itemcode"].astype(str).tolist()
    sel = [bool(st.session_state["sel_map"].get(c, False)) for c in codes]
    qty = [int(st.session_state["qty_map"].get(c, 0)) for c in codes]

    table = pd.DataFrame({
        "เลือก": sel,
        "รหัส": codes,
        "รายการ": items["itemname"].astype(str).tolist(),
        "จำนวนที่เบิก": qty,
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

    # inline clear button (no sidebar version)
    if st.button("ล้างที่เลือกทั้งหมด", use_container_width=True):
        st.session_state["sel_map"].clear()
        st.session_state["qty_map"].clear()
        _safe_rerun()

    # auto qty=1 when checked first time
    changed = False
    for i, r in edited.iterrows():
        code = r["รหัส"]
        selected = bool(r["เลือก"])
        qty_val = int(r["จำนวนที่เบิก"] or 0)
        if selected and not st.session_state["sel_map"].get(code, False) and qty_val <= 0:
            edited.at[i, "จำนวนที่เบิก"] = 1
            qty_val = 1
            changed = True
        st.session_state["sel_map"][code] = selected
        st.session_state["qty_map"][code] = qty_val
    if changed: _safe_rerun()

    return edited, items


def _requests_table(ss, user):
    """Render latest requests with icons + cancel pending by dropdown."""
    st.subheader("คำขอที่ส่งไป (ล่าสุด)")
    num = st.slider("จำนวนออเดอร์ล่าสุดที่ต้องการดู", 1, 50, 5, 1)

    try:
        ws = _requests_ws(ss)
        vals = ws.get_all_values()
        if not vals or len(vals) <= 1:
            st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), 
                         use_container_width=True, hide_index=True)
            return

        df = pd.DataFrame(vals[1:], columns=vals[0])
        df = _normalize(df)

        # columns (robust fallback)
        def pick(df, *cands):
            for c in cands:
                if c in df.columns: return c
            lowers = {c.lower(): c for c in df.columns}
            for c in cands:
                if c.lower() in lowers: return lowers[c.lower()]
            return None

        c_user = pick(df, "username","Username")
        c_id   = pick(df, "requestid","RequestID","orderid")
        c_qty  = pick(df, "qty","Qty","จำนวน")
        c_name = pick(df, "itemname","ItemName","รายการ")
        c_stat = pick(df, "status","Status","สถานะ")
        c_time = pick(df, "requesttime","RequestTime","time","datetime")

        me = str(user.get("username","")).lower()
        if c_user:
            df = df[df[c_user].astype(str).str.lower() == me]

        if c_qty: df["qty_num"] = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).astype(float)
        else:     df["qty_num"] = 0.0
        if not c_name: c_name = c_id

        if not c_stat:
            df["__status__"] = "Pending"; c_stat="__status__"
        if not c_time:
            df["__time__"] = ""; c_time="__time__"

        if not c_id or df.empty:
            st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), 
                         use_container_width=True, hide_index=True)
            return

        df["pair"] = df[c_name].astype(str) + " (" + df["qty_num"].astype(int).astype(str) + ")"
        def agg_status(g):
            sset = set(str(x).strip().lower() for x in g[c_stat].astype(str))
            if "canceled" in sset or "cancelled" in sset: return "Canceled"
            if "approved" in sset or "อนุมัติ" in sset: return "Approved"
            return "Pending"

        grp = (df.groupby([c_id], as_index=False)
                 .agg(รายการ=("pair", lambda s: ", ".join(list(s))),
                      จำนวนรวม=("qty_num","sum"),
                      เวลา=(c_time,"max"),
                      สถานะ=(c_stat, agg_status))
               ).sort_values("เวลา", ascending=False).head(num)

        def icon(s):
            ss = str(s).strip().lower()
            return "🟡" if ss=="pending" else ("🟢" if ss=="approved" else "🔴")
        grp["ไอคอน"] = grp["สถานะ"].map(icon)

        show = grp.rename(columns={c_id:"เลขที่ออเดอร์"})[["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]].copy()
        show["จำนวนรวม"] = show["จำนวนรวม"].astype(int)
        st.dataframe(show, use_container_width=True, hide_index=True)

        # cancel pending
        pending_ids = show[show["สถานะ"]=="Pending"]["เลขที่ออเดอร์"].tolist()
        if pending_ids:
            sel = st.selectbox("เลือกเลขที่ออเดอร์ (Pending) เพื่อยกเลิก", pending_ids, key="cancel_reqid")
            if st.button("ยกเลิกออเดอร์นี้", type="secondary"):
                header = vals[0]
                lowers = {str(h).strip().lower(): i for i,h in enumerate(header)}
                idx_id    = lowers.get("requestid", lowers.get("orderid"))
                idx_user  = lowers.get("username")
                idx_stat  = lowers.get("status")
                idx_note  = lowers.get("note")
                changes = []
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                for rnum in range(2, len(vals)+1):
                    row = vals[rnum-1]
                    rid = row[idx_id]   if idx_id  is not None and idx_id  < len(row) else ""
                    un  = row[idx_user] if idx_user is not None and idx_user < len(row) else ""
                    stv = row[idx_stat] if idx_stat is not None and idx_stat < len(row) else ""
                    if str(rid)==str(sel) and str(un).lower()==me and str(stv).strip().lower()=="pending":
                        if idx_stat is not None:
                            changes.append({"range": f"{chr(ord('A')+idx_stat)}{rnum}", "values": [["Canceled"]]})
                        if idx_note is not None:
                            changes.append({"range": f"{chr(ord('A')+idx_note)}{rnum}", "values": [[f"Canceled by user at {now}"]]})
                if changes:
                    ws.batch_update(changes)
                    st.success(f"ยกเลิกออเดอร์ {sel} สำเร็จ")
                    _safe_rerun()
                else:
                    st.info("ออเดอร์นี้ไม่อยู่ในสถานะ Pending แล้ว")
        else:
            st.caption("ไม่มีออเดอร์สถานะ Pending")

    except Exception as e:
        st.dataframe(pd.DataFrame(columns=["ไอคอน","เลขที่ออเดอร์","รายการ","จำนวนรวม","สถานะ","เวลา"]), 
                     use_container_width=True, hide_index=True)


def page_issue():
    user = st.session_state.get("user", {})
    st.title("WishCo Branch Portal — เบิกอุปกรณ์")
    st.caption(f"ผู้ใช้: **{user.get('username','')}** | สาขา: **{user.get('branch_code','')}**")

    try:
        ss = _open_spreadsheet()
    except Exception as e:
        st.error(f"เชื่อมต่อสเปรดชีตไม่ได้: {e}"); return

    edited, items = _items_editor(ss)

    # summary + confirm
    chosen = edited[(edited["เลือก"]==True) & (edited["จำนวนที่เบิก"]>0)].copy()
    if not chosen.empty:
        st.subheader("สรุปรายการที่จะเบิก")
        sum_df = chosen[["รหัส","รายการ","จำนวนที่เบิก","หน่วย"]].copy()
        sum_df["จำนวนที่เบิก"] = pd.to_numeric(sum_df["จำนวนที่เบิก"], errors="coerce").fillna(0).astype(int)

        # in-cell spinner editor
        sum_df2 = st.data_editor(
            sum_df, hide_index=True, use_container_width=True,
            column_config={
                "รหัส": st.column_config.TextColumn("รหัส", disabled=True),
                "รายการ": st.column_config.TextColumn("รายการ", disabled=True),
                "จำนวนที่เบิก": st.column_config.NumberColumn("จำนวนที่เบิก", min_value=0, step=1, format="%d"),
                "หน่วย": st.column_config.TextColumn("หน่วย", disabled=True),
            },
            key="summary_editor_v11",
        )
        for _, r in sum_df2.iterrows():
            st.session_state["qty_map"][str(r["รหัส"])] = int(r["จำนวนที่เบิก"] or 0)

        if st.button("ยืนยันการเบิก", type="primary", use_container_width=True):
            # validate stock
            full_items = _read_items_df(ss)
            insufficient = []
            pairs = []
            for code in sum_df2["รหัส"].tolist():
                q = int(st.session_state["qty_map"].get(code, 0))
                if q > 0:
                    pairs.append((code, q))
                    have = float(full_items[full_items["itemcode"].astype(str)==code].head(1).get("stock", pd.Series([0])).iloc[0] or 0)
                    if q > have:
                        name = str(full_items[full_items["itemcode"].astype(str)==code].head(1).get("itemname", pd.Series([""])).iloc[0])
                        insufficient.append((code, name, have, q))
            if insufficient:
                msg = "สต็อกไม่พอ: " + ", ".join([f"{c} ({have} < {need})" for c,_,have,need in insufficient])
                st.error(msg); return

            order_id = _generate_order_id(ss, user.get("username",""))
            now = time.strftime("%Y-%m-%d %H:%M:%S")

            req_rows = []
            for code, q in pairs:
                row = full_items[full_items["itemcode"].astype(str)==code].head(1).iloc[0]
                req_rows.append([ now, order_id, user.get("username",""), user.get("branch_code",""),
                                  row.get("itemcode"), row.get("itemname"), q, "Pending", "" ])
            try:
                _append_rows(_requests_ws(ss), req_rows)
                st.session_state["last_order_id"] = order_id
                st.success(f"ส่งคำขอเบิกเรียบร้อย เลขที่ออเดอร์: {order_id} | รายการ: {len(req_rows)}")
                st.info("คำขอถูกบันทึกลงชีต 'Requests' เรียบร้อยแล้ว (สถานะ: Pending)")
                st.session_state["sel_map"].clear(); st.session_state["qty_map"].clear()
            except Exception as e:
                st.error(f"บันทึกคำขอไม่สำเร็จ: {e}")
    else:
        st.info("ยังไม่เลือกรายการ")

    # history table with icons + cancel
    _requests_table(ss, user)


def main():
    st.set_page_config(page_title="WishCo Branch Portal", layout="wide", page_icon="🧰")
    _ensure_session()
    if st.session_state.get("auth", False):
        menu = st.sidebar.radio("เมนู", ["เบิกอุปกรณ์","Health Check","ออกจากระบบ"], index=0)
        if menu == "Health Check":
            page_health()
        elif menu == "ออกจากระบบ":
            st.session_state["auth"]=False; st.session_state["user"]={}; st.session_state["sel_map"].clear(); st.session_state["qty_map"].clear()
            st.success("ออกจากระบบแล้ว"); _safe_rerun()
        else:
            page_issue()
    else:
        menu = st.sidebar.radio("เมนู", ["เข้าสู่ระบบ","Health Check"], index=0)
        if menu == "Health Check": page_health()
        else: page_login()


if __name__ == "__main__":
    main()
