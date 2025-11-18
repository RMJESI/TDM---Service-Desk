# ======================================================================
# scheduler.py — Clean Version (Triple Pulse Glow + Sync at Bottom)
# ======================================================================

import os
import streamlit as st
import pandas as pd
import tempfile
from datetime import datetime, date
from collections import Counter

from engine import schedule_month
from export import write_csv
from models import (
    connect, ensure_schema, search_properties_by_company,
    fetch_pm_defaults, insert_month_job, list_tech_names,
    export_db_to_tempfile,
    _import_properties_from_sheet
)

# -------------------------------------------------
# Helper reused from UI
# -------------------------------------------------
def parse_holidays(txt: str):
    out = set()
    for token in (txt or "").replace(",", "\n").splitlines():
        t = token.strip()
        if not t:
            continue
        try:
            y, m, d = map(int, t.split("-"))
            from datetime import date as _d
            out.add(_d(y, m, d))
        except Exception:
            pass
    return out

# -------------------------------------------------
# Utility for grouping notes
# -------------------------------------------------
def _format_grouped_notes(notes: list[str], max_unique: int = 12) -> str:
    if not notes:
        return ""
    ctr = Counter([n.strip() for n in notes if n.strip()])
    items = [f"{reason} (x{count})" for reason, count in ctr.most_common(max_unique)]
    extra = max(0, len(ctr) - max_unique)
    suffix = f"  •  +{extra} more…" if extra > 0 else ""
    return "Notes: " + "  •  ".join(items) + suffix

# -------------------------------------------------
# Helper to write DB to temp file
# -------------------------------------------------
def _materialize_db_to_tempfile(con) -> str:
    return export_db_to_tempfile(con)

# -------------------------------------------------
# Sidebar
# -------------------------------------------------
def sidebar_for_service_scheduler_v2(sidebar):

    # 🔧 Sidebar readability fix (selectbox text, inputs, labels, arrows)
    st.markdown("""
    <style>
    /* --------------------------------------------
       Sidebar Dropdown + Input Contrast Fix
       -------------------------------------------- */

    /* Selectbox label text ("Region", "Technician") */
    [data-testid="stSidebar"] .stSelectbox label p {
        color: #f5faf5 !important;
        font-weight: 600 !important;
    }

    /* Selected value text ("CA", "Amador") */
    [data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] * {
        color: #1a1a1a !important;   /* DARKER TEXT */
        font-weight: 600 !important; /* Optional: make text slightly bolder */
    }

    /* Input text inside text inputs */
    [data-testid="stSidebar"] input {
        color: #1a1a1a !important;   /* Dark input text */
        font-weight: 600 !important;
    }


    /* Dropdown arrow icon */
    [data-testid="stSidebar"] .stSelectbox svg {
        color: #ffffff !important;
    }

    /* Dropdown menu items (keep dark on light for readability) */
    [data-testid="stSidebar"] .stSelectbox [role="listbox"] div[role="option"] * {
        color: #1a1a1a !important;
    }
    </style>
    """, unsafe_allow_html=True)

    with sidebar:
        st.header("Run Scheduler")
        month = st.text_input("Month (YYYY-MM)", value="2025-11", key="sb_month")
        region = st.selectbox("Region", ["CA", "NV"], index=0, key="sb_region")

        _con = connect()
        tech_options = (
            list_tech_names(_con, region_filter=region)
            or list_tech_names(_con, region_filter=None)
        )
        if not tech_options:
            tech_options = ["(no techs in DB)"]

        tech_name = st.selectbox("Technician", tech_options, index=0, key="sb_tech")

        pm_cap_default = st.number_input(
            "PM cap per day", min_value=0, step=1, value=2, key="sb_cap"
        )

        st.subheader("Holidays (skip dates)")
        holidays_text = st.text_area(
            "Comma- or newline-separated", value="", height=100, key="sb_holidays"
        )

        st.subheader("Hold Open Days")
        keep_open_toggle = st.checkbox(
            "Hold first working days open", value=True, key="sb_holdopen"
        )
        n_workdays_open = st.number_input(
            "Days to hold open", min_value=1, max_value=10, value=5, key="sb_holdcount"
        )

    return (
        month, region, tech_name, pm_cap_default,
        holidays_text, keep_open_toggle, n_workdays_open
    )

# ======================================================================
# MAIN TAB
# ======================================================================

def render_scheduler_tab(sidebar_root):

    sbx = sidebar_root.container()

    (
        month, region, tech_name, pm_cap_default,
        holidays_text, keep_open_toggle, n_workdays_open
    ) = sidebar_for_service_scheduler_v2(sbx)

    # -------------------------------------------------
    # Button Styling
    # -------------------------------------------------
    st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #1f4a30 !important;
        color: white !important;
        font-size: 18px !important;
        font-weight: 600 !important;
        padding: 0.75rem 2.25rem !important;
        border-radius: 10px !important;
        border: none !important;
    }
    div.stButton > button:first-child:hover {
        background-color:#2b6b46 !important;
        transform:scale(1.03) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # -------------------------------------------------
    # RUN SCHEDULER
    # -------------------------------------------------
    if st.button("Run Scheduler", key="run_sched"):

        overrides_raw = st.text_input("Overrides (YYYY-MM-DD:CAP)", "", key="ovr_input")
        from datetime import date as _date

        ovr_map = {}
        if overrides_raw.strip():
            for token in overrides_raw.split(","):
                if ":" in token:
                    ds, cap = token.split(":")
                    y, m, d = map(int, ds.strip().split("-"))
                    ovr_map[_date(y, m, d)] = int(cap)

        holidays_set = parse_holidays(holidays_text)

        con = connect()
        db_path = _materialize_db_to_tempfile(con)

        try:
            sched = schedule_month(
                db_path=db_path,
                month=month,
                region=region,
                tech_name=tech_name,
                pm_cap_default=int(pm_cap_default),
                pm_cap_overrides=ovr_map,
                holidays=holidays_set,
                keep_first_n_workdays_open=int(n_workdays_open) if keep_open_toggle else 0,
            )
        finally:
            try: os.unlink(db_path)
            except: pass

        st.success("Schedule created.")

        for day, jobs in sorted(sched.items()):
            st.subheader(day.isoformat())
            rows = []
            notes = []

            for p in jobs:
                if p.job_id == -1:
                    if getattr(p, "reasoning", None):
                        notes.append(p.reasoning)
                else:
                    hrs = round((p.end - p.start).total_seconds()/3600, 2)
                    rows.append({
                        "Property": p.property,
                        "Type": p.type,
                        "Start": p.start.strftime("%H:%M"),
                        "End": p.end.strftime("%H:%M"),
                        "Hours": hrs,
                        "Drive(min)": round(p.drive_min_from_prev,1),
                        "Reason": p.reasoning
                    })

            if rows:
                st.dataframe(rows, use_container_width=True)
            if notes:
                st.caption("Notes: " + "; ".join(notes))

    # -------------------------------------------------
    # ADD JOBS
    # -------------------------------------------------
    with st.expander("📥 Add Jobs"):
        bulk_text = st.text_area("Paste names (one per line)", "", height=200)

        if st.button("Add All", key="add_jobs"):
            con = connect()
            ensure_schema(con)
            ok = 0
            fail = []

            lines = [ln.strip() for ln in bulk_text.splitlines() if ln.strip()]

            for name in lines:
                props = search_properties_by_company(con, name, limit=20)
                if not props:
                    fail.append((name, "No match"))
                    continue

                sel = props[0]
                d_type, d_hours, d_prio, d_notes = fetch_pm_defaults(con, sel.id)

                row = con.execute("""
                    SELECT pm_phase, pm_window_start, pm_window_end, pm_last_thursday
                    FROM properties WHERE id=?
                """, (sel.id,)).fetchone()

                phase = row["pm_phase"] if row else None
                tws = row["pm_window_start"] if row else None
                twe = row["pm_window_end"] if row else None
                last_thu = int(row["pm_last_thursday"] or 0) if row else 0

                try:
                    insert_month_job(
                        con,
                        month=month,
                        property_id=sel.id,
                        type=d_type,
                        duration_hours=d_hours,
                        priority=d_prio,
                        phase=phase,
                        time_window_start=tws,
                        time_window_end=twe,
                        must_be_last_thursday=last_thu,
                        notes=d_notes,
                        assigned_tech=tech_name,
                    )
                    ok += 1
                except Exception as e:
                    fail.append((name, str(e)))

            st.success(f"Added: {ok}")
            if fail:
                st.warning(fail)

    # ==================================================================
    # DANGER ZONE — Clear ALL Jobs (Balloons only)
    # ==================================================================
    
    st.divider()
    with st.expander("⚠️ Danger Zone — Clear ALL Jobs"):
        st.warning("Permanently deletes ALL rows in month_jobs.")
    
        if st.button("🗑️ Clear ALL Jobs", key="clear_all"):
            con = connect()
            con.execute("DELETE FROM month_jobs")
            con.commit()
    
            st.success("All jobs cleared!")
    
            # 🎈 Bring balloons back!
            try:
                st.balloons()
            except:
                pass
    
            # Give the animation time to play before reload
            import time
            time.sleep(3.0)
    
            st.rerun()


    # ==================================================================
    # ONE CLICK SYNC — BELOW Danger Zone
    # ==================================================================

    st.divider()
    with st.expander("🔁 One-Click Data Sync"):
        st.caption("Runs all data sync operations safely in the correct order.")

        if st.button("Run Full Sync", key="full_sync"):
            con = connect()

            # Unlock sheet import
            con.execute("DELETE FROM _imports WHERE marker='props_google_csv'")
            con.commit()

            # Force import Google Sheet
            try:
                _import_properties_from_sheet(con)
                st.success("Google Sheet imported.")
            except Exception as e:
                st.error(f"Import failed: {e}")

            # Reset DB cache
            st.cache_resource.clear()

            # Sync Hours
            con.execute("""
                UPDATE month_jobs
                SET duration_hours = (
                    SELECT pm_hours FROM properties
                    WHERE properties.id = month_jobs.property_id
                )
                WHERE duration_hours IN (0, 1, 1.0, 1.5)
                   OR duration_hours IS NULL;
            """)
            con.commit()

            st.success("✔ All sync tasks completed successfully!")
