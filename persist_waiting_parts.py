# persist_waiting_parts.py
# GitHub-backed persistence + weekly sync helper for “Waiting for Parts”
# FIXED TO:
#   • Stop auto-saving on weekly sync
#   • Delete rows ONLY if missing from upload AND Status="Ready"
#   • Preserve Status/Notes/PO/Property on blank upload
#   • Use Task ID as identity
#   • Maintain First Seen / Last Seen / Weeks On List / This Week

import os, base64, json, io, requests
from datetime import datetime, timezone
import pandas as pd

# ------------------------------------------------------------
# Optional Streamlit secrets
# ------------------------------------------------------------
try:
    import streamlit as st
    _SECRETS = dict(st.secrets)
except Exception:
    _SECRETS = {}

# ------------------------------------------------------------
# GitHub configuration
# ------------------------------------------------------------
GH_TOKEN  = _SECRETS.get("GITHUB_TOKEN", os.getenv("GITHUB_TOKEN", ""))
GH_OWNER  = _SECRETS.get("GH_OWNER",   os.getenv("GH_OWNER",   "RMJESI"))
GH_REPO   = _SECRETS.get("GH_REPO",    os.getenv("GH_REPO",    "BearPath"))
GH_BRANCH = _SECRETS.get("GH_BRANCH",  os.getenv("GH_BRANCH",  "main"))
WFP_PATH  = _SECRETS.get("WFP_PATH",   os.getenv("WFP_PATH",   "data/waiting_for_parts.csv"))

_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}" if GH_TOKEN else "",
    "Accept": "application/vnd.github+json",
}

# ------------------------------------------------------------
# Columns used in WFP table
# ------------------------------------------------------------
BASE_COLS = ["Task ID", "Property", "PO", "Status", "Notes", "Last Updated"]
TRACKING_COLS = ["First Seen", "Last Seen", "Weeks On List", "This Week"]
ALL_COLS = BASE_COLS + TRACKING_COLS

DATE_FMT = "%Y-%m-%d"

def _today_str():
    return datetime.now(timezone.utc).astimezone().strftime(DATE_FMT)

# ------------------------------------------------------------
# GitHub helpers
# ------------------------------------------------------------
def _contents_url(path: str) -> str:
    return f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{path}"

def _get_file_json(path: str):
    if not GH_TOKEN:
        return None
    r = requests.get(_contents_url(path), headers=_HEADERS)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    raise RuntimeError(f"GitHub GET failed {r.status_code}: {r.text}")

def _get_existing_sha(path: str) -> str | None:
    meta = _get_file_json(path)
    return (meta or {}).get("sha")

def _read_github_csv(path: str) -> pd.DataFrame | None:
    meta = _get_file_json(path)
    if not meta:
        return None
    raw = base64.b64decode(meta.get("content", ""))
    return pd.read_csv(io.BytesIO(raw))

def _write_github_csv(path: str, df: pd.DataFrame, message: str):
    """
    Direct GitHub write. Called only when user presses Save in UI.
    """
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    if not GH_TOKEN:
        # Local fallback
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(csv_bytes)
        return

    payload = {
        "message": message,
        "content": base64.b64encode(csv_bytes).decode("utf-8"),
        "branch": GH_BRANCH,
    }
    sha = _get_existing_sha(path)
    if sha:
        payload["sha"] = sha

    r = requests.put(_contents_url(path), headers=_HEADERS, data=json.dumps(payload))
    if r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT failed {r.status_code}: {r.text}")

# ------------------------------------------------------------
# Public load/save API
# ------------------------------------------------------------
def load_wfp() -> pd.DataFrame:
    """
    Loads WFP table and guarantees all columns exist.
    """
    df = None
    try:
        df = _read_github_csv(WFP_PATH)
    except Exception:
        pass

    if df is None:
        if os.path.exists(WFP_PATH):
            df = pd.read_csv(WFP_PATH)
        else:
            df = pd.DataFrame(columns=ALL_COLS)

    return _ensure_cols(df)

def save_wfp(df: pd.DataFrame, message: str = "Update waiting_for_parts.csv"):
    """
    Explicit save to GitHub. ONLY triggered by UI "Save" button.
    """
    df = _ensure_cols(df)
    _write_github_csv(WFP_PATH, df, message)

# ------------------------------------------------------------
# Column normalization helpers
# ------------------------------------------------------------
def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Add missing columns
    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = ""

    # Order
    df = df[ALL_COLS]

    # Normalize
    for c in ["Task ID","Property","PO","Status","Notes"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    df["Weeks On List"] = pd.to_numeric(df["Weeks On List"], errors="coerce").fillna(0).astype(int)
    df["This Week"] = df["This Week"].astype(str).str.lower().isin(["1","true","yes","y","t"])

    for c in ["First Seen","Last Seen","Last Updated"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    return df

def _normalize_incoming(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes uploaded CSV. Incoming only has:
      - Task ID
      - Company Name → Property
    """
    df = df.copy()
    rename = {}

    for col in df.columns:
        key = col.lower().strip()
        if key in ("task id","taskid"):
            rename[col] = "Task ID"
        elif key in ("company","company name","property"):
            rename[col] = "Property"

    df = df.rename(columns=rename)

    # Ensure the two essential columns exist
    if "Task ID" not in df.columns:
        df["Task ID"] = ""
    if "Property" not in df.columns:
        df["Property"] = ""

    df = df[["Task ID", "Property"]]
    df["Task ID"] = df["Task ID"].astype(str).str.strip()
    df["Property"] = df["Property"].astype(str).str.strip()

    return df

# ------------------------------------------------------------
# WEEKLY SYNC — SAFE VERSION (NO AUTO-SAVE)
# ------------------------------------------------------------
def weekly_sync(upload_df, as_of=None) -> pd.DataFrame:
    """
    Performs the weekly sync merge WITHOUT saving.

    Rules:
      ✓ Add new Task IDs.
      ✓ Preserve existing Status/Notes/PO/Property unless explicitly provided.
      ✓ Update First Seen / Last Seen / Weeks On List / This Week.
      ✓ DELETE rows ONLY if:
            - Task ID is missing from upload AND
            - Status = "Ready"
      ✓ NO GitHub save inside this function.
    """
    import math

    # ---------------------
    # Parse incoming
    # ---------------------
    if isinstance(upload_df, pd.DataFrame):
        incoming = upload_df.copy()
    else:
        incoming = pd.read_csv(upload_df, dtype=str).fillna("")

    incoming = _normalize_incoming(incoming)
    incoming_task_ids = set(incoming["Task ID"])

    # ---------------------
    # Dates
    # ---------------------
    as_of_str = as_of or _today_str()
    as_of_date = pd.to_datetime(as_of_str).date()

    # ---------------------
    # Load current table
    # ---------------------
    current = load_wfp().copy()

    # Ensure identity exists
    current["Task ID"] = current["Task ID"].astype(str).str.strip()

    # ---------------------
    # Identify deletions:
    # Only delete rows that:
    #   - Do NOT appear in incoming upload
    #   - AND have Status == "Ready"
    # ---------------------
    mask_delete = (~current["Task ID"].isin(incoming_task_ids)) & (current["Status"] == "Ready")
    to_delete_ids = set(current.loc[mask_delete, "Task ID"])

    # Keep all others
    kept = current.loc[~current["Task ID"].isin(to_delete_ids)].copy()

    # ---------------------
    # Merge incoming
    # ---------------------
    base_for_merge = kept.add_suffix("_old").copy()

    merged = incoming.merge(
        base_for_merge,
        left_on="Task ID",
        right_on="Task ID_old",
        how="left"
    )

    # Drop Task ID_old helper
    merged.drop(columns=["Task ID_old"], inplace=True, errors="ignore")

    # ---------------------
    # Tracking fields
    # ---------------------
    # First Seen: keep old if exists, else as_of
    merged["First Seen"] = merged["First Seen_old"].apply(lambda x: x if str(x).strip() else as_of_str)

    # Last Seen: always today
    merged["Last Seen"] = as_of_str

    # ---------------------
    # Preserve existing base fields if incoming is blank
    # ---------------------
    def _coalesce(new_val, old_val):
        new_val = "" if pd.isna(new_val) else str(new_val).strip()
        old_val = "" if pd.isna(old_val) else str(old_val).strip()
        return new_val if new_val else old_val

    for col in ["Property", "PO", "Status", "Notes", "Last Updated"]:
        new_vals = merged.get(col, "")
        old_vals = merged.get(f"{col}_old", "")
        merged[col] = [_coalesce(n, o) for n, o in zip(new_vals, old_vals)]

    # If Last Updated is still empty, set to as_of
    merged["Last Updated"] = merged["Last Updated"].apply(
        lambda v: v if v else as_of_str
    )

    # ---------------------
    # Weeks On List
    # ---------------------
    def _parse_date(d):
        try:
            return pd.to_datetime(d).date()
        except:
            return None

    weeks = []
    for fs in merged["First Seen"]:
        dt = _parse_date(fs)
        if dt is None:
            weeks.append(1)
        else:
            days = max(0, (as_of_date - dt).days)
            weeks.append((days // 7) + 1)

    merged["Weeks On List"] = weeks

    # ---------------------
    # This Week
    # ---------------------
    merged["This Week"] = True

    # ---------------------
    # Final ordering + cleanup
    # ---------------------
    out = merged[ALL_COLS].copy()
    out = _ensure_cols(out)

    # DO NOT SAVE HERE — UI will save_wfp() after showing results.
    return out

