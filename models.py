# models.py — Shared SQLite (Streamlit-safe) + Google Sheet seed + auto-migrate
# Stable: 2025-11-03.A (Remy sheet: A–G mapped)

from __future__ import annotations

import os, sqlite3, tempfile, time
from dataclasses import dataclass
from typing import List, Optional, Tuple, Iterable

import pandas as pd

# ----- Streamlit secrets (works local & Cloud) -----
try:
    import streamlit as st
    _SECRETS = dict(st.secrets)
except Exception:
    _SECRETS = {}


def _get_secret(name: str, default: str = "") -> str:
    val = _SECRETS.get(name)
    if val is None or str(val).strip() == "":
        val = os.environ.get(name, default)
    return (val or "").strip()


# ===== CONFIG =====
CSV_URL = _get_secret(
    "BEARPATH_CSV_URL",
    # New Sheet (CSV export endpoint)
    # A: Company Name
    # B: Full Address
    # C: Latitude
    # D: Longitude
    # E: Frequency of Service (→ pm_type)
    # F: Hours to Complete (→ pm_hours)
    # G: Fixed Timing (→ pm_phase)
    "https://docs.google.com/spreadsheets/d/1ptGWoDcTPp1fh-L_7mmqxEESiWbSO0feav62NnbcYug/gviz/tq?tqx=out:csv",
)

# Default DB uses a shared in-memory URI; engine may pass a real file path.
DB_PATH = _get_secret("BEARPATH_DB_PATH", "file:bearpath_db?mode=memory&cache=shared")


# ===== Data classes =====
@dataclass
class Office:
    region: str
    name: str
    lat: Optional[float]
    lon: Optional[float]


@dataclass
class Tech:
    name: str
    region: str
    first_appt: str         # "08:30"
    latest_return: str      # "15:30"
    max_pms_per_day: int    # e.g., 2


@dataclass
class Property:
    id: int
    name: str
    customer: str
    address: str            # mapped from full_address
    city: str
    state: str
    zip: str
    lat: Optional[float]
    lon: Optional[float]
    region: str


@dataclass
class MonthJob:
    id: Optional[int]
    month: str              # "YYYY-MM"
    property_id: int
    type: str               # "PM", "Quarterly", etc.
    duration_hours: Optional[float]
    priority: Optional[int]
    fixed_date: Optional[str]          # "YYYY-MM-DD" if pinned
    phase: Optional[str]               # "early"|"mid"|"late"|None
    time_window_start: Optional[str]   # "HH:MM"|None
    time_window_end: Optional[str]     # "HH:MM"|None
    must_be_last_thursday: int         # 0/1
    notes: str
    assigned_tech: Optional[str]


# ---------- connection ----------
def connect(db_path: str = DB_PATH):
    """
    Connect to SQLite. Uses URI mode only if db_path starts with "file:".
    Adds WAL + busy_timeout and retries around one-time startup tasks to avoid
    'database table is locked' on Streamlit's concurrent init.
    """
    use_uri = isinstance(db_path, str) and db_path.startswith("file:")
    con = sqlite3.connect(db_path, uri=use_uri, check_same_thread=False)
    con.row_factory = sqlite3.Row

    # Be resilient to concurrent readers during app boot
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=15000;")  # 15s wait before 'locked' errors

    ensure_schema(con)

    # Retry the write-phase boot steps in case another worker briefly holds a lock
    def _retry(fn, tries=5, base_sleep=0.25):
        for i in range(tries):
            try:
                return fn()
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    time.sleep(base_sleep * (i + 1))
                    continue
                raise

    _retry(lambda: _migrate_schema(con))
    _retry(lambda: _seed_reference(con))
    _retry(lambda: _import_properties_from_sheet(con))

    return con


# ---------- helpers ----------
def _as_text(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _as_float(v) -> Optional[float]:
    try:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return float(v)
    except Exception:
        return None


def _norm_month(s: str) -> str:
    s = _as_text(s)
    return s[:7] if len(s) >= 7 else s


def _ensure_office(con: sqlite3.Connection, region: str, name: str, lat: Optional[float], lon: Optional[float]):
    con.execute(
        """
        INSERT INTO offices(region, name, lat, lon)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(region) DO UPDATE SET
            name=excluded.name,
            lat=COALESCE(excluded.lat, offices.lat),
            lon=COALESCE(excluded.lon, offices.lon)
        """,
        (region, name, lat, lon),
    )


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA foreign_keys=ON;")
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS offices(
            region TEXT PRIMARY KEY,
            name   TEXT NOT NULL,
            lat    REAL,
            lon    REAL
        );

        CREATE TABLE IF NOT EXISTS techs(
            name TEXT PRIMARY KEY,
            region TEXT NOT NULL,
            first_appt TEXT NOT NULL DEFAULT '08:30',
            latest_return TEXT NOT NULL DEFAULT '15:30',
            max_pms_per_day INTEGER NOT NULL DEFAULT 2
        );

        CREATE TABLE IF NOT EXISTS properties(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            customer TEXT,
            full_address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            lat REAL,
            lon REAL,
            region TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS month_jobs(
            id INTEGER PRIMARY KEY,
            month TEXT NOT NULL,              -- 'YYYY-MM'
            property_id INTEGER NOT NULL,
            type TEXT,
            duration_hours REAL,
            priority INTEGER,
            fixed_date TEXT,                  -- 'YYYY-MM-DD'
            phase TEXT,
            time_window_start TEXT,
            time_window_end TEXT,
            must_be_last_thursday INTEGER DEFAULT 0,
            notes TEXT,
            assigned_tech TEXT,
            FOREIGN KEY(property_id) REFERENCES properties(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_mj_month ON month_jobs(month);
        CREATE INDEX IF NOT EXISTS idx_mj_prop ON month_jobs(property_id);
        """
    )
    con.commit()


# ---------- migrate ----------
def _migrate_schema(con: sqlite3.Connection) -> None:
    """
    Ensure legacy DBs have all columns our code expects and **relax** any old UNIQUE indexes
    that caused 'UNIQUE constraint failed: properties.full_address'.
    Run inside a short IMMEDIATE transaction to avoid lock contention.
    """
    cur = con.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")  # take a write lock up front

        # --- properties: add any missing columns ---
        cols = {row["name"] for row in cur.execute("PRAGMA table_info(properties)").fetchall()}
        needed = [
            ("customer", "TEXT", "NULL"),
            ("full_address", "TEXT", "NULL"),
            ("city", "TEXT", "NULL"),
            ("state", "TEXT", "NULL"),
            ("zip", "TEXT", "NULL"),
            ("lat", "REAL", "NULL"),
            ("lon", "REAL", "NULL"),
            ("region", "TEXT", "'CA'"),

            # NEW optional scheduling defaults per property
            ("pm_type", "TEXT", "NULL"),          # from 'Frequency of Service' (E)
            ("pm_hours", "REAL", "NULL"),         # from 'Hours to Complete' (F)
            ("pm_priority", "INTEGER", "NULL"),   # not in sheet; stays NULL (fallback=3)
            ("pm_phase", "TEXT", "NULL"),         # from 'Fixed Timing ...' (G)
            ("pm_window_start", "TEXT", "NULL"),  # not in sheet; optional future
            ("pm_window_end", "TEXT", "NULL"),    # not in sheet; optional future
            ("pm_last_thursday", "INTEGER", "0"), # not in sheet; default 0
        ]
        for name, decl, default_sql in needed:
            if name not in cols:
                cur.execute(f"ALTER TABLE properties ADD COLUMN {name} {decl} DEFAULT {default_sql}")

        # normalize region
        cur.execute("UPDATE properties SET region = COALESCE(NULLIF(TRIM(region),''), 'CA')")

        # --- Relax unique constraints safely (drop if present, recreate non-unique) ---
        cur.execute("DROP INDEX IF EXISTS ux_properties_name")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_properties_name ON properties(name)")
        cur.execute("DROP INDEX IF EXISTS ux_properties_full_address")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_properties_full_address ON properties(full_address)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region)")

        con.commit()
    except Exception:
        con.rollback()
        raise


# ---------- seeding ----------
def _seed_reference(con: sqlite3.Connection) -> None:
    """
    Seed offices and techs idempotently (safe to run multiple times).
    """
    # Offices (insert/update; keep existing lat/lon if incoming is None)
    _ensure_office(con, "CA", "California Office", 33.8446, -118.3295)  # Torrance-ish
    _ensure_office(con, "NV", "Las Vegas Office", 36.1147, -115.1728)   # Vegas-ish

    # Techs — UPSERT each one so missing names get added even if table isn't empty
    tech_rows = [
        ("Amador",   "CA", "08:30", "15:30", 2),
        ("Juan",     "CA", "08:30", "15:30", 2),
        ("Eloy",     "CA", "08:30", "15:30", 2),
        ("Eddy",     "CA", "08:30", "15:30", 2),
        ("Steven",   "CA", "07:30", "14:00", 2),
        ("Fernando", "NV", "08:30", "15:30", 2),
        ("Tracy",    "NV", "08:30", "15:30", 2),
        ("Tevin",    "NV", "08:30", "15:30", 2),
        ("Roberto",  "NV", "08:30", "15:30", 2),
    ]
    con.executemany(
        """
        INSERT INTO techs(name, region, first_appt, latest_return, max_pms_per_day)
        VALUES(?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            region          = excluded.region,
            first_appt      = excluded.first_appt,
            latest_return   = excluded.latest_return,
            max_pms_per_day = excluded.max_pms_per_day
        """,
        tech_rows,
    )
    con.commit()

# ---------- Google Sheet import ----------
def _infer_region(state: str) -> str:
    s = _as_text(state).upper()
    if s == "NV":
        return "NV"
    return "CA"  # default bucket


def _import_properties_from_sheet(con: sqlite3.Connection) -> None:
    """
    Import/update properties from the configured Google Sheet CSV.
    Runs once per process (idempotent) — guarded by a marker table.
    """
    con.execute("CREATE TABLE IF NOT EXISTS _imports(marker TEXT PRIMARY KEY, ts TEXT)")
    done = con.execute("SELECT marker FROM _imports WHERE marker='props_google_csv'").fetchone()
    if done:
        return

    # ---- load CSV (best-effort) ----
    try:
        df = pd.read_csv(CSV_URL)
    except Exception:
        # mark as done to prevent loops if the sheet isn't reachable
        con.execute(
            "INSERT OR REPLACE INTO _imports(marker, ts) VALUES('props_google_csv', datetime('now'))"
        )
        con.commit()
        return

    # ---- tolerant column mapping ----
    def pick(df, variants: Iterable[str]) -> Optional[str]:
        for c in df.columns:
            for v in variants:
                if c.strip().lower() == v.strip().lower():
                    return c
        return None

    # Exact headers (A–G) with safe aliases
    col_company  = pick(df, ["Company Name", "Company", "Property", "Property Name"])
    col_fulladdr = pick(df, ["Full Address", "Address"])
    col_lat      = pick(df, ["Latitude", "lat"])
    col_lon      = pick(df, ["Longitude", "lon", "lng"])

    # Optional basics (not present in this sheet—left tolerant for future)
    col_city     = pick(df, ["City"])
    col_state    = pick(df, ["State", "ST"])
    col_zip      = pick(df, ["Zip", "Zip Code", "Postal Code"])

    # NEW: scheduling defaults from your sheet
    col_pm_type  = pick(df, ["Frequency of Service", "PM Type", "Type"])
    col_pm_hours = pick(df, ["Hours to Complete", "PM Hours", "Hours", "Duration"])
    col_pm_phase = pick(df, ["Fixed Timing (Early, Mid, Late, Tuesday, Thursday, etc)", "Fixed Timing", "PM Phase", "Phase"])

    def val(row, col) -> str:
        if not col:
            return ""
        v = row.get(col, "")
        return "" if pd.isna(v) else str(v)

    # ---- UPDATE-or-INSERT (no ON CONFLICT dependency) ----
    for _, row in df.iterrows():
        name = val(row, col_company).strip()
        if not name:
            continue

        city   = val(row, col_city)
        state  = val(row, col_state)
        zipc   = val(row, col_zip)
        full   = val(row, col_fulladdr)
        lat    = _as_float(row.get(col_lat, None) if col_lat else None)
        lon    = _as_float(row.get(col_lon, None) if col_lon else None)
        region = _infer_region(state)

        # Scheduling defaults from the sheet
        pm_type  = (val(row, col_pm_type) or None)
        raw_hours = val(row, col_pm_hours)
        try:
            pm_hours = float(raw_hours) if raw_hours not in ("", None) else None
        except Exception:
            # Handle things like "3 hrs", "3.0", or "3.5 hours"
            import re
            m = re.search(r"(\d+(\.\d+)?)", str(raw_hours))
            pm_hours = float(m.group(1)) if m else None
        pm_phase = (val(row, col_pm_phase) or None)

        existing = con.execute("SELECT id FROM properties WHERE name = ?", (name,)).fetchone()
        if existing:
            con.execute(
                """
                UPDATE properties SET
                    customer=?, full_address=?, city=?, state=?, zip=?,
                    lat=COALESCE(?, lat), lon=COALESCE(?, lon), region=?,
                    pm_type=COALESCE(?, pm_type),
                    pm_hours=COALESCE(?, pm_hours),
                    pm_phase=COALESCE(?, pm_phase)
                WHERE id=?
                """,
                (name, full, city, state, zipc, lat, lon, region,
                 pm_type, pm_hours, pm_phase,
                 int(existing['id'])),
            )
        else:
            con.execute(
                """
                INSERT INTO properties(
                    name, customer, full_address, city, state, zip, lat, lon, region,
                    pm_type, pm_hours, pm_phase
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (name, name, full, city, state, zipc, lat, lon, region,
                 pm_type, pm_hours, pm_phase),
            )

    con.execute(
        "INSERT OR REPLACE INTO _imports(marker, ts) VALUES('props_google_csv', datetime('now'))"
    )
    con.commit()


# ---------- lookups ----------
def fetch_office(con: sqlite3.Connection, region: str) -> Optional[Office]:
    r = con.execute("SELECT * FROM offices WHERE region=?", (region,)).fetchone()
    if not r:
        return None
    return Office(region=r["region"], name=r["name"], lat=r["lat"], lon=r["lon"])


def fetch_tech(con: sqlite3.Connection, name: str) -> Optional[Tech]:
    r = con.execute("SELECT * FROM techs WHERE name=?", (name,)).fetchone()
    if not r:
        return None
    return Tech(
        name=r["name"],
        region=r["region"],
        first_appt=r["first_appt"],
        latest_return=r["latest_return"],
        max_pms_per_day=r["max_pms_per_day"],
    )


def list_tech_names(con: sqlite3.Connection, region_filter: Optional[str] = None) -> List[str]:
    if region_filter:
        rows = con.execute("SELECT name FROM techs WHERE region=? ORDER BY name", (region_filter,)).fetchall()
    else:
        rows = con.execute("SELECT name FROM techs ORDER BY name").fetchall()
    return [r["name"] for r in rows]


import re

def _norm_name(s: str) -> str:
    s = _as_text(s).lower()
    # normalize dashes/spaces, strip most punctuation except & and alphanumerics
    s = re.sub(r"[–—−\-]+", " ", s)         # any dash-ish -> space
    s = re.sub(r"[^\w\s&]", "", s)          # drop punctuation except word chars, space, &
    s = re.sub(r"\s+", " ", s).strip()
    return s

def search_properties_by_company(
    con: sqlite3.Connection,
    query: str,
    region: Optional[str] = None,
    limit: int = 50,
    # legacy kw the UI sometimes passes:
    region_filter: Optional[str] = None,
) -> List[Property]:
    """
    Exact-first (normalized) search, then fuzzy contains.
    Accepts either `region` or `region_filter`.
    """
    _region = region if region is not None else region_filter
    norm_q = _norm_name(query)

    # Pull candidates (scoped by region if provided). We keep this simple & robust.
    if _region:
        rows = con.execute("SELECT * FROM properties WHERE region=?", (_region,)).fetchall()
    else:
        rows = con.execute("SELECT * FROM properties").fetchall()

    exact: List[sqlite3.Row] = []
    fuzzy: List[sqlite3.Row] = []

    for r in rows:
        nm = _as_text(r["name"])
        nm_norm = _norm_name(nm)
        if nm_norm == norm_q:
            exact.append(r)
        elif norm_q and (norm_q in nm_norm):
            fuzzy.append(r)

    # Prefer exacts, fall back to fuzzies, cap by limit
    hits = (exact + fuzzy)[:limit]

    out: List[Property] = []
    for r in hits:
        out.append(
            Property(
                id=int(r["id"]),
                name=_as_text(r["name"]),
                customer=_as_text(r["customer"]) or _as_text(r["name"]),
                address=_as_text(r["full_address"]),
                city=_as_text(r["city"]),
                state=_as_text(r["state"]),
                zip=_as_text(r["zip"]),
                lat=(float(r["lat"]) if r["lat"] is not None else None),
                lon=(float(r["lon"]) if r["lon"] is not None else None),
                region=_as_text(r["region"]) or "CA",
            )
        )
    return out

# Small helper for UI defaults; returns (type, hours, priority, notes)
def fetch_pm_defaults(con: sqlite3.Connection, property_id: int) -> Tuple[str, float, int, str]:
    """
    Use per-property defaults when available; sane fallbacks otherwise.
    pm_type  ← 'Frequency of Service' (E)    → default 'PM'
    pm_hours ← 'Hours to Complete' (F)       → default 1.0
    pm_priority                                default 3 (no column in sheet yet)
    """
    r = con.execute(
        "SELECT pm_type, pm_hours, pm_priority FROM properties WHERE id = ?",
        (property_id,)
    ).fetchone()
    typ = (r["pm_type"] if r and r["pm_type"] else "PM")
    hrs = (float(r["pm_hours"]) if r and r["pm_hours"] is not None else 1.0)
    prio = (int(r["pm_priority"]) if r and r["pm_priority"] is not None else 3)
    return (typ, hrs, prio, "")


# ---------- month_jobs I/O ----------
def insert_month_job(
    con: sqlite3.Connection,
    *,
    month: str,
    property_id: int,
    type: Optional[str] = None,
    duration_hours: Optional[float] = None,
    priority: Optional[int] = None,
    fixed_date: Optional[str] = None,
    phase: Optional[str] = None,
    time_window_start: Optional[str] = None,
    time_window_end: Optional[str] = None,
    must_be_last_thursday: int = 0,
    notes: str = "",
    assigned_tech: Optional[str] = None,
) -> int:
    """
    Insert one MonthJob. `month` may be 'YYYY-MM' or a full date 'YYYY-MM-DD'.
    We normalize it to 'YYYY-MM' on write.
    """
    norm = _norm_month(month)
    cur = con.execute(
        """
        INSERT INTO month_jobs(
            month, property_id, type, duration_hours, priority, fixed_date, phase,
            time_window_start, time_window_end, must_be_last_thursday, notes, assigned_tech
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            norm,
            property_id,
            type,
            duration_hours,
            priority,
            fixed_date,
            phase,
            time_window_start,
            time_window_end,
            must_be_last_thursday,
            notes,
            assigned_tech,
        ),
    )
    con.commit()
    return int(cur.lastrowid)


def list_month_jobs(
    con: sqlite3.Connection,
    month: str,
    region: str,
) -> List[Tuple[MonthJob, Property]]:
    """
    Return ONLY the jobs explicitly added for `month` (and optionally `region`),
    as a list of (MonthJob, Property) tuples expected by the engine.
    Accepts 'YYYY-MM' and any 'YYYY-MM-%' via prefix match.
    Region predicate is forgiving for empty property regions.
    """
    sql = """
        SELECT
            mj.id               AS mj_id,
            mj.month            AS mj_month,
            mj.property_id      AS mj_property_id,
            mj.type             AS mj_type,
            mj.duration_hours   AS mj_duration_hours,
            mj.priority         AS mj_priority,
            mj.fixed_date       AS mj_fixed_date,
            mj.phase            AS mj_phase,
            mj.time_window_start AS mj_tws,
            mj.time_window_end   AS mj_twe,
            mj.must_be_last_thursday AS mj_last_thu,
            mj.notes            AS mj_notes,
            mj.assigned_tech    AS mj_assigned_tech,

            p.id                AS p_id,
            p.name              AS p_name,
            p.full_address      AS p_full_address,
            p.city              AS p_city,
            p.state             AS p_state,
            p.zip               AS p_zip,
            p.lat               AS p_lat,
            p.lon               AS p_lon,
            p.region            AS p_region,
            p.customer          AS p_customer
        FROM month_jobs mj
        JOIN properties p ON p.id = mj.property_id
        WHERE (mj.month = ? OR mj.month LIKE ?)
          AND (
               (? = '' AND 1=1)
            OR p.region = ?
            OR COALESCE(p.region,'') = ''
          )
        ORDER BY
            COALESCE(mj.priority, 9999) ASC,
            p.name ASC
    """
    rows = con.execute(sql, (month, f"{_norm_month(month)}%", region or "", region or "")).fetchall()

    out: List[Tuple[MonthJob, Property]] = []
    for r in rows:
        prop = Property(
            id=int(r["p_id"]),
            name=_as_text(r["p_name"]),
            customer=_as_text(r["p_customer"]) or _as_text(r["p_name"]),
            address=_as_text(r["p_full_address"]),
            city=_as_text(r["p_city"]),
            state=_as_text(r["p_state"]),
            zip=_as_text(r["p_zip"]),
            lat=(float(r["p_lat"]) if r["p_lat"] is not None else None),
            lon=(float(r["p_lon"]) if r["p_lon"] is not None else None),
            region=_as_text(r["p_region"]) or "CA",
        )
        mj = MonthJob(
            id=int(r["mj_id"]) if r["mj_id"] is not None else None,
            month=_as_text(r["mj_month"]),
            property_id=int(r["mj_property_id"]),
            type=_as_text(r["mj_type"]) or "PM",
            duration_hours=(float(r["mj_duration_hours"]) if r["mj_duration_hours"] is not None else None),
            priority=(int(r["mj_priority"]) if r["mj_priority"] is not None else None),
            fixed_date=(_as_text(r["mj_fixed_date"]) or None),
            phase=(_as_text(r["mj_phase"]) or None),
            time_window_start=(_as_text(r["mj_tws"]) or None),
            time_window_end=(_as_text(r["mj_twe"]) or None),
            must_be_last_thursday=int(r["mj_last_thu"] or 0),
            notes=_as_text(r["mj_notes"]),
            assigned_tech=(_as_text(r["mj_assigned_tech"]) or None),
        )
        out.append((mj, prop))
    return out


# ---------- export: real .db file ----------
def export_db_to_tempfile(con: Optional[sqlite3.Connection] = None) -> str:
    """
    Create a real on-disk SQLite .db file containing the current DB contents
    and return its path. If no connection is provided, open a fresh one.
    """
    if con is None:
        con = connect()

    fd, path = tempfile.mkstemp(prefix="bearpath_", suffix=".db")
    os.close(fd)

    disk_con = sqlite3.connect(path, check_same_thread=False)
    try:
        con.backup(disk_con)
        disk_con.commit()
    finally:
        disk_con.close()

    return path





























































