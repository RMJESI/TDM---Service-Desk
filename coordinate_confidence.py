# coordinate_confidence.py
# BearPath — Coordinate Confidence Tool (Cloud-Safe / Turbo Mode)
# Includes built-in DEBUG SECTION (no extra tabs needed)

import math
import pandas as pd
import requests
import sqlite3
import time
import os
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import connect

# -----------------------------
# Cloud-safe cache path
# -----------------------------
CACHE_PATH = "/mount/tmp/geocode_cache.csv"

# Turbo-mode settings
WORKERS = 20
MAX_RETRIES = 5
BATCH_DELAY = 1.25
RADIUS_MILES = 0.75
MAX_MILES = 10.0


# -----------------------------
# Helpers
# -----------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def compute_confidence(dist):
    if dist is None:
        return 0
    if dist <= RADIUS_MILES:
        return 100
    if dist >= MAX_MILES:
        return 0
    scale = (dist - RADIUS_MILES) / (MAX_MILES - RADIUS_MILES)
    return max(0, min(100, int(round(100 * (1 - scale)))))


def geocode_once(addr):
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": addr, "format": "json", "limit": 1},
            headers={"User-Agent": "BearPath"},
            timeout=15,
        )
        data = resp.json()
        if not data:
            return None, None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None, None


def geocode_with_retry(addr):
    for attempt in range(1, MAX_RETRIES + 1):
        lat, lon = geocode_once(addr)
        if lat is not None and lon is not None:
            return lat, lon
        time.sleep(0.5 * attempt)
    return None, None


# -----------------------------
# Pastel row colors
# -----------------------------
def pastel_color(conf):
    if conf >= 100:
        return "background-color: #d9f3e7;"  # mint
    if conf >= 80:
        return "background-color: #e9f8ef;"  # light green
    if conf >= 50:
        return "background-color: #fff9d9;"  # pastel yellow
    return "background-color: #fde7e7;"      # soft red/pink


# -----------------------------
# Cache helpers
# -----------------------------
def load_cache():
    if os.path.exists(CACHE_PATH):
        return pd.read_csv(CACHE_PATH)
    return pd.DataFrame(columns=["id", "full_address", "lat", "lon"])


def append_to_cache(row_dict):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    df = pd.DataFrame([row_dict])

    if not os.path.exists(CACHE_PATH):
        df.to_csv(CACHE_PATH, index=False)
    else:
        df.to_csv(CACHE_PATH, mode="a", header=False, index=False)


# -----------------------------
# Main UI
# -----------------------------
def render_coordinate_confidence_tab(sidebar_root):

    with sidebar_root:
        st.header("Coordinate Confidence")
        run = st.button("Run Confidence Scan")
        rebuild = st.button("Rebuild Cache (slow!)")

    st.markdown("### 📍 Coordinate Confidence Scan")

    # -------------------------------------------------------------
    # REBUILD
    # -------------------------------------------------------------
    if rebuild:
        if os.path.exists(CACHE_PATH):
            os.remove(CACHE_PATH)
        st.warning("Cache cleared. Click **Run Confidence Scan** to rebuild.")
        return

    if not run:
        st.info("Click **Run Confidence Scan** in the left sidebar to begin.")
        return

    # Load properties
    con = connect()
    con.row_factory = sqlite3.Row

    props = con.execute("""
        SELECT id, name, full_address, lat AS stored_lat, lon AS stored_lon
        FROM properties
        ORDER BY name
    """).fetchall()

    st.write(f"Total properties: **{len(props)}**")

    # Load cache
    cache = load_cache()
    cache["id"] = cache["id"].astype(str)
    existing_ids = set(cache["id"])

    # Missing rows
    missing = [p for p in props if str(p["id"]) not in existing_ids]
    st.write(f"Needing geocode: **{len(missing)}**")

    # -------------------------------------------------------------
    # GEOCODE
    # -------------------------------------------------------------
    if missing:
        st.warning("Geocoding in turbo mode… may take a few minutes.")
        progress = st.progress(0)

        done = 0
        total = len(missing)

        with ThreadPoolExecutor(max_workers=WORKERS) as exe:
            futures = {exe.submit(geocode_with_retry, p["full_address"]): p for p in missing}

            for future in as_completed(futures):
                p = futures[future]
                lat, lon = future.result()

                append_to_cache({
                    "id": p["id"],
                    "full_address": p["full_address"],
                    "lat": lat,
                    "lon": lon,
                })

                done += 1
                progress.progress(done / total)

        time.sleep(BATCH_DELAY)
        st.success("Geocoding complete — computing confidence…")
        st.warning("Checkpoint A — geocode finished")

    # Reload updated cache
    cache = load_cache()
    cache["id"] = cache["id"].astype(str)

    # -------------------------------------------------------------
    # CONFIDENCE COMPUTATION
    # -------------------------------------------------------------
    rows = []
    for p in props:
        pid = str(p["id"])
        cached = cache[cache["id"] == pid]

        if cached.empty:
            glat, glon = None, None
            dist = None
            conf = 0
        else:
            glat = cached.iloc[0]["lat"]
            glon = cached.iloc[0]["lon"]

            if None in (glat, glon, p["stored_lat"], p["stored_lon"]):
                dist = None
                conf = 0
            else:
                dist = haversine(p["stored_lat"], p["stored_lon"], glat, glon)
                conf = compute_confidence(dist)

        rows.append({
            "Property": p["name"],
            "Address": p["full_address"],
            "Stored Lat": p["stored_lat"],
            "Stored Lon": p["stored_lon"],
            "Geocoded Lat": glat,
            "Geocoded Lon": glon,
            "Dist (mi)": dist,
            "Confidence": conf,
        })

    df = pd.DataFrame(rows).sort_values("Confidence")

    st.success("Scan complete!")

    # -------------------------------------------------------------
    # DISPLAY TABLE
    # -------------------------------------------------------------
    st.dataframe(
        df.style.apply(lambda r: [pastel_color(r["Confidence"])] * len(r), axis=1),
        use_container_width=True,
    )

    # -------------------------------------------------------------
    # DOWNLOAD BUTTON
    # -------------------------------------------------------------
    st.download_button(
        "📥 Download Confidence Report",
        df.to_csv(index=False),
        "coordinate_confidence_report.csv",
        "text/csv",
    )

    # -------------------------------------------------------------
    # BUILT-IN DEBUG PANEL
    # -------------------------------------------------------------
    st.warning("Checkpoint C — about to show debug expander")

    with st.expander("🐞 Debug Info (Cache Status)"):

        if not os.path.exists(CACHE_PATH):
            st.error("❌ Cache file does NOT exist.")
            return

        st.success(f"Cache file exists at: {CACHE_PATH}")

        # File info
        try:
            stat = os.stat(CACHE_PATH)
            st.json({
                "Path": CACHE_PATH,
                "Size (bytes)": stat.st_size,
                "Last Modified (epoch)": stat.st_mtime,
            })
        except Exception as e:
            st.error(f"Unable to stat file: {e}")
            return

        # Load cache content
        try:
            dfc = pd.read_csv(CACHE_PATH)
            st.write(f"Rows in cache: **{len(dfc)}**")
            st.write("Columns:", list(dfc.columns))

            st.write("### First 5 cached rows")
            st.dataframe(dfc.head(), use_container_width=True)

            st.write("### Last 5 cached rows")
            st.dataframe(dfc.tail(), use_container_width=True)

        except Exception as e:
            st.error(f"Error reading cache: {e}")

        # Compare with DB count
        total = con.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"]
        st.write("### Cache vs DB count")
        st.json({
            "Rows in cache": len(dfc),
            "Properties in DB": total,
            "Match": (len(dfc) == total),
        })
