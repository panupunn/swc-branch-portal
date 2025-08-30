# secrets_probe.py  (ใช้ชั่วคราวเพื่อดีบั๊กเท่านั้น)
import os, json, streamlit as st
from pprint import pformat

st.set_page_config(page_title="Secrets Probe", layout="wide")
st.title("Secrets Probe")

# แสดงคีย์ที่ระบบเห็น
try:
    keys = list(st.secrets.keys())
except Exception as e:
    keys = []
st.write("**st.secrets keys:**", keys)

# แสดงว่ามีคีย์สำคัญหรือไม่
checks = {
    "has[gcp_service_account]": "gcp_service_account" in st.secrets,
    "has[service_account]": "service_account" in st.secrets,
    "has[GOOGLE_SERVICE_ACCOUNT_JSON]": bool(st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()),
}
# top-level keys
required = ["type","project_id","private_key_id","private_key","client_email","client_id"]
checks["has[top_level_required_keys]"] = all(k in st.secrets for k in required)
st.write("**checks:**", checks)

# ถ้ามี table ให้ดูตัวอย่างค่า
if "gcp_service_account" in st.secrets:
    d = dict(st.secrets["gcp_service_account"])
    d_short = {k: (v[:40]+"..." if isinstance(v, str) and len(v)>60 else v) for k,v in d.items()}
    st.write("**gcp_service_account (preview):**", d_short)

# ถ้ามี JSON string ทดลอง parse ให้เลย
raw_json = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
if raw_json:
    try:
        info = json.loads(raw_json)
        st.success("GOOGLE_SERVICE_ACCOUNT_JSON: JSON ถูกต้อง ✅")
        st.write({k: ("<hidden>" if "key" in k else v) for k,v in info.items() if k!="private_key"})
        st.code((info.get("private_key","")[:80] + "..."), language="text")
    except Exception as e:
        st.error(f"GOOGLE_SERVICE_ACCOUNT_JSON: JSON พัง ❌ : {e}")

# สรุป
st.info("ถ้า keys ว่างสนิท → คุณกำลังแก้ Secrets ‘คนละแอป’ หรือ TOML พังจน parse ไม่ได้")
