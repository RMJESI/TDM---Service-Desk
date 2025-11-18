# persist_waiting_parts.py
# GitHub-backed persistence + weekly sync helper for “Waiting for Parts”
#
# This version is neutral and branded for TDM Service Desk:
#   • Safe for internal use
#   • No personal identifiers
#   • No BearPath references
#   • GitHub repo defaults to TDM-Service-Desk
#
# Features:
#   • Delete ONLY rows missing from upload AND Status="Ready"
#   • Preserve existing Status/Notes/PO/Property on blank upload
#   • Use Task ID as identity key
#   • Maintain First Seen / Last Seen / Weeks On List / This Week
#   • Weekly sync does NOT save automatically (UI triggers save)

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
# GitHub configuration (TDM Service Desk defaults)
# ------------------------------------------------------------
GH_TOKEN  = _SECRETS.get("GITHUB_TOKEN", os.getenv("GITHUB_TOKEN", ""))
GH_OWNER  = _SECRETS.get("GH_OWNER",   os.getenv("GH_OWNER",   "DumbellMan"))
GH_REPO   = _SECRETS.get("GH_REPO",    os.getenv("GH_REPO",    "TDM-Service-Desk"))
GH_BRANCH = _SECRETS.get("GH_BRANCH",  os.getenv("GH_BRANCH",  "main"))
WFP_PATH  = _SECRETS.get("WFP_PATH",   os.getenv("WFP_PATH",   "data/waiting_for_parts.csv"))

_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}" if GH_TOKEN else "",
    "Accept": "application/vnd.github+json",
}


# ------------------------------------------------------------
# Columns used in the WFP table
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
    Direct GitHub write. Called only when a user clicks Save in the UI.
    """
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Local fallback if no GitHub token
    if not GH_TOKEN:
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
    Loads Waiting For Parts table (TDM Service Desk).
    Ensures all required columns exist.
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
    Explicit save to GitHub.
    (Called ONLY when the UI "Save" button is pressed.)
    """
    df = _ensure_cols(df)
    _write_github_csv(WFP_PATH, df, message)


# ------------------------------------------------------------
# Column normalization
# ------------------------------------------------------------
def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure missing columns exist
    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[ALL_COLS]

    # Normalize text fields
    for c in ["Task ID", "Property", "PO", "Status", "Notes"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    df["Weeks On List"] = (
        pd.to_numeric(df["Weeks On List"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df["This Week"] = (
        df["This Week"].astype(str).str.lower().isin(["1", "true", "yes", "y", "t"])
    )

    for c in ["First Seen", "Last Seen", "Last Updated"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    return df


def _normalize_incoming(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes uploaded CSV with:
      - Task ID
      - Company Name → Property
    """
    df = df.copy()
    rename = {}

    for col in df.columns:
        key = col.lower().strip()
        if key in ("task id", "taskid"):
            rename[col] = "Task ID"
        elif key in ("company name", "company", "property"):
            rename[col] = "Property"

    df = df.rename(columns=rename)

    if "Task ID" not in df.columns:
        df["Task ID"] = ""
    if "Property" not in df.columns:
        df["Property"] = ""

    df = df[["Task ID", "Property"]]
    df["Task ID"] = df["Task ID"].astype(str).str.strip()
    df["Property"] = df["Property"].astype(str).str.strip()
    return df


# ------------------------------------------------------------
# WEEKLY SYNC (no auto-save)
# ------------------------------------------------------------
def weekly_sync(upload_df, as_of=None) -> pd.DataFrame:
    """
    Weekly merge:
      ✓ Add new Task IDs
      ✓ Keep existing Status/Notes/PO/Property unless incoming has value
      ✓ Update First Seen / Last Seen / Weeks / This Week
      ✓ Delete ONLY if missing from upload AND Status="Ready"
      ✓ NO saving — UI will handle save
    """
    # Parse input
    if isinstance(upload_df, pd.DataFrame):
        incoming = upload_df.copy()
    else:
        incoming = pd.read_csv(upload_df, dtype=str).fillna("")

    incoming = _normalize_incoming(incoming)
    incoming_task_ids = set(incoming["Task ID"])

    # Set merge date
    as_of_str = as_of or _today_str()
    as_of_date = pd.to_datetime(as_of_str).date()

    # Load existing
    current = load_wfp().copy()
    current["Task ID"] = current["Task ID"].astype(str).str.strip()

    # Identify rows to delete
    mask_delete = (~current["Task ID"].isin(incoming_task_ids)) & (current["Status"] == "Ready")
    to_delete_ids = set(current.loc[mask_delete, "Task ID"])

    kept = current.loc[~current["Task ID"].isin(to_delete_ids)].copy()

    # Merge incoming
    merged = incoming.merge(
        kept.add_suffix("_old"),
        left_on="Task ID",
        right_on="Task ID_old",
        how="left"
    )
    merged.drop(columns=["Task ID_old"], inplace=True, errors="ignore")

    # Tracking fields
    merged["First Seen"] = merged["First Seen_old"].apply(
        lambda x: x if str(x).strip() else as_of_str
    )
    merged["Last Seen"] = as_of_str

    # Preserve base fields if incoming is blank
    def _coalesce(new, old):
        new = "" if pd.isna(new) else str(new).strip()
        old = "" if pd.isna(old) else str(old).strip()
        return new if new else old

    for col in ["Property", "PO", "Status", "Notes", "Last Updated"]:
        merged[col] = [
            _coalesce(n, o)
            for n, o in zip(merged.get(col, ""), merged.get(f"{col}_old", ""))
        ]

    # Ensure Last Updated
    merged["Last Updated"] = merged["Last Updated"].apply(
        lambda v: v if v else as_of_str
    )

    # Weeks on list
    def _date_or_none(v):
        try:
            return pd.to_datetime(v).date()
        except:
            return None

    weeks = []
    for fs in merged["First Seen"]:
        dt = _date_or_none(fs)
        if dt:
            days = max(0, (as_of_date - dt).days)
            weeks.append((days // 7) + 1)
        else:
            weeks.append(1)

    merged["Weeks On List"] = weeks
    merged["This Week"] = True

    out = merged[ALL_COLS].copy()
    return _ensure_cols(out)
