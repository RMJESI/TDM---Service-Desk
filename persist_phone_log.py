# persist_phone_log.py
import os, base64, json, requests, pandas as pd

try:
    import streamlit as st
    _SECRETS = dict(st.secrets)
except Exception:
    _SECRETS = {}

GH_TOKEN   = _SECRETS.get("GITHUB_TOKEN", "")
GH_OWNER   = _SECRETS.get("GH_OWNER", "RMJESI")
GH_REPO    = _SECRETS.get("GH_REPO", "BearPath")
PHONE_PATH = _SECRETS.get("PHONE_LOG_PATH", "data/phone_log.csv")

_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
}

PHONE_COLS = [
    "date","company_property","address",
    "caller_name","caller_phone","caller_email",
    "problem","needed","done"
]

def _contents_url(path: str) -> str:
    return f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{path}"

def load_phone_log() -> pd.DataFrame:
    """Load phone log CSV from GitHub; return empty DF with columns if not found."""
    r = requests.get(_contents_url(PHONE_PATH), headers=_HEADERS)
    if r.status_code == 200:
        content_b64 = r.json().get("content","")
        csv_bytes = base64.b64decode(content_b64)
        df = pd.read_csv(pd.io.common.BytesIO(csv_bytes), dtype=str)
        # normalize columns
        for c in PHONE_COLS:
            if c not in df.columns: df[c] = ""
        df = df[PHONE_COLS].fillna("")
        return df
    if r.status_code == 404:
        return pd.DataFrame(columns=PHONE_COLS)
    r.raise_for_status()

def _get_existing_sha() -> str | None:
    r = requests.get(_contents_url(PHONE_PATH), headers=_HEADERS)
    if r.status_code == 200:
        return r.json().get("sha")
    return None

def save_phone_log(df: pd.DataFrame, message: str = "Update phone log"):
    """Overwrite CSV in GitHub with the entire DataFrame (bulk save)."""
    if df is None:
        df = pd.DataFrame(columns=PHONE_COLS)
    # normalize/order
    for c in PHONE_COLS:
        if c not in df.columns: df[c] = ""
    df = df[PHONE_COLS].fillna("")
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    payload = {
        "message": message,
        "content": base64.b64encode(csv_bytes).decode("utf-8"),
        "branch": "main",
    }
    sha = _get_existing_sha()
    if sha:
        payload["sha"] = sha
    put_r = requests.put(_contents_url(PHONE_PATH), headers=_HEADERS, data=json.dumps(payload))
    if put_r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub save failed: {put_r.status_code} {put_r.text}")

def append_phone_log(row: dict, message: str = "Add phone log entry"):
    """Append a single row by loading -> concat -> save."""
    df = load_phone_log()
    df = pd.concat([df, pd.DataFrame([{k: str(row.get(k,"")) for k in PHONE_COLS}] )], ignore_index=True)
    save_phone_log(df, message=message)
