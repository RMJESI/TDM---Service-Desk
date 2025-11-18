# waiting_for_parts.py
# Extracted exactly from ui.py and wrapped cleanly into a function.

import streamlit as st
import pandas as pd
from datetime import date
from persist_waiting_parts import load_wfp, save_wfp, weekly_sync

# -------------------------------------------------
# RENDER WAITING FOR PARTS TAB
# -------------------------------------------------
def render_wfp_tab(sidebar_root):

    # ---------- Scoped sidebar CSS (Waiting for Parts only) ----------
    st.markdown("""
    <style>
    
      /* --- Maintenance label --- */
      [data-testid="stSidebar"] .sb-label {
          color: #ffffff !important;
          font-weight: 700 !important;
          font-size: 1rem !important;
      }
    
      /* --- ALL buttons inside the sidebar --- */
      [data-testid="stSidebar"] .stButton > button {
          background-color: #f7faf8 !important;
          color: #163422 !important;
          border: 1px solid #9dc2a9 !important;
          font-weight: 700 !important;
          border-radius: 8px !important;
      }
    
      /* Ensure icon + text inside the button stay dark */
      [data-testid="stSidebar"] .stButton > button * {
          color: #163422 !important;
          fill: #163422 !important;
      }
    
      /* Hover state */
      [data-testid="stSidebar"] .stButton > button:hover {
          background-color: #e8f2ed !important;
          color: #163422 !important;
      }
    
    </style>
    """, unsafe_allow_html=True)



    # ---------- Sidebar controls for this page ----------
    sbx = sidebar_root.container()
    with sbx:
        # wrap all WFP sidebar content in a scoped div
        st.markdown("<div class='wfp-sidebar'>", unsafe_allow_html=True)

        st.header("Waiting for Parts")
        st.radio(
            "View mode",
            ["Grid view (bulk)", "Color grid (read-only)"],
            horizontal=False,
            key="wfp_view"
        )
        st.divider()
        st.markdown('<span class="sb-label">Maintenance</span>', unsafe_allow_html=True)
        if st.button("🧮 Recompute Weeks (today)", key="wfp_recompute_btn_sidebar"):
            def _recompute_weeks_from_first_seen(df_in: pd.DataFrame, as_of_str: str) -> pd.DataFrame:
                out = df_in.copy()
                as_of_dt = pd.to_datetime(str(as_of_str), errors="coerce")
                if pd.isna(as_of_dt):
                    as_of_dt = pd.Timestamp.today().normalize()
                else:
                    as_of_dt = as_of_dt.normalize()
                fs_parsed = pd.to_datetime(out["First Seen"].astype(str), errors="coerce")
                days = (as_of_dt - fs_parsed).dt.days.fillna(0).clip(lower=0)
                weeks = (days // 7) + 1
                out["Weeks On List"] = weeks.astype(int)
                return out

            _df_now = load_wfp().copy()
            # ensure cols (safe)
            for c in ["Task ID","Property","PO","Status","Notes","Last Updated",
                      "First Seen","Last Seen","Weeks On List","This Week"]:
                if c not in _df_now.columns:
                    _df_now[c] = ""
            fixed = _recompute_weeks_from_first_seen(_df_now, date.today().isoformat())
            save_wfp(fixed, message=f"Waiting for Parts: recompute weeks as of {date.today().isoformat()}")
            st.success("Weeks On List recomputed.")
            st.rerun()

        # close the scoped div
        st.markdown("</div>", unsafe_allow_html=True)

    # -------------------------------------------------
    # STATUS METADATA
    # -------------------------------------------------
    STATUS_META = {
        "Backordered": {"label":"Backordered", "color":"#3B82F6"},
        "Ordered Recently": {"label":"Ordered Recently", "color":"#6EE7B7"},
        "Has Tracking Info": {"label":"Has Tracking Info", "color":"#10B981"},
        "Ready": {"label":"Ready", "color":"#065F46"},
        "Upholstery": {"label":"Upholstery", "color":"#FB923C"},
        "Direct Shipped": {"label":"Direct Shipped", "color":"#F472B6"},
        "Need to Email Vendor": {"label":"Need to Email Vendor", "color":"#8B5CF6"},
        "Need to Email Team": {"label":"Need to Email Team", "color":"#FCA5A5"},
        "Other": {"label":"Other", "color":"#9CA3AF"},
        "": {"label":"(Unset)", "color":"#D1D5DB"},
    }

    columns_order = [
        "Backordered",
        "Ordered Recently",
        "Has Tracking Info",
        "Upholstery",
        "Direct Shipped",
        "Need to Email Vendor",
        "Need to Email Team",
        "Other",
        "Ready",
    ]
    STATUS_RANK = {name: i for i, name in enumerate(columns_order)}
    STATUS_RANK["Ready"] = 999

    # -------------------------------------------------
    # COUNTER STYLING
    # -------------------------------------------------
    st.markdown("""
    <style>
      .wfp-counter {
        display:flex; align-items:center; justify-content:center;
        gap:.6rem;
        border-radius: 12px;
        padding: 14px 18px;
        font-weight: 800;
        border: 2px solid rgba(0,0,0,0.06);
        box-shadow: 0 2px 6px rgba(0,0,0,0.06);
        min-height: 68px;
      }
      .wfp-counter .num { font-size: 28px; line-height: 1; }
      .wfp-counter .lbl { font-size: 17px; line-height: 1.15; opacity: .9; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("### ⚙️ Waiting for Parts")

    # -------------------------------------------------
    # WEEKLY SYNC (Miracle CSV upload)
    # -------------------------------------------------
    with st.expander("📥 Weekly sync from Miracle (Task ID, Property, PO)"):

        st.caption("Upload this week's CSV. New Task IDs are added; carry-overs keep Status/Notes and recompute **Weeks On List**.")

        if "wfp_processed_signature" not in st.session_state:
            st.session_state["wfp_processed_signature"] = None
        if "wfp_uploader_nonce" not in st.session_state:
            st.session_state["wfp_uploader_nonce"] = 0

        sync_date = st.date_input("Sync date", value=date.today(), format="YYYY-MM-DD", key="wfp_sync_date")

        uploader_key = f"wfp_seed_uploader_{st.session_state['wfp_uploader_nonce']}"
        seed_file = st.file_uploader("Upload CSV", type=["csv"], key=uploader_key)

        def _file_sig(upload):
            if upload is None:
                return None
            try:
                upload.seek(0); data = upload.read(); upload.seek(0)
                return (upload.name, len(data))
            except Exception:
                return (getattr(upload, "name", "unknown"), 0)

        sig = _file_sig(seed_file)

        process_clicked = st.button("🚀 Process upload", disabled=(seed_file is None), key="wfp_process_btn")

        if process_clicked and seed_file is not None:
            try:
                if sig is not None and sig == st.session_state["wfp_processed_signature"]:
                    st.info("This file was already processed. Upload a new CSV or change the date.")
                else:
                    seed_df = pd.read_csv(seed_file, dtype=str).fillna("")
                    merged = weekly_sync(seed_df, as_of=sync_date.isoformat())
                    save_wfp(merged, message=f"Waiting for Parts: weekly sync {sync_date.isoformat()}")

                    st.session_state["wfp_processed_signature"] = sig
                    st.session_state["wfp_uploader_nonce"] += 1
                    st.success("Weekly sync complete.")
                    st.toast("Weekly sync complete.", icon="✅")
                    st.rerun()
            except Exception as e:
                st.error(f"Weekly sync failed: {e}")

    # -------------------------------------------------
    # LOAD & NORMALIZE TABLE
    # -------------------------------------------------
    df = load_wfp().copy()
    for c in ["Task ID","Property","PO","Status","Notes","Last Updated",
              "First Seen","Last Seen","Weeks On List","This Week"]:
        if c not in df.columns:
            df[c] = ""

    df["Weeks On List"] = pd.to_numeric(df["Weeks On List"], errors="coerce").fillna(0).astype(int)
    df["This Week"] = df["This Week"].astype(str).str.lower().isin(["1","true","yes","y","t"])

    # -------------------------------------------------
    # SUMMARY COUNTERS
    # -------------------------------------------------
    act1, act2 = st.columns([2, 1])
    with act1:
        hide_ready = st.checkbox("Hide ‘Ready’ (active work only)", value=False, key="wfp_hide_ready")
    with act2:
        st.caption("Tip: Delete rows manually after ~2 weeks in ‘Ready’ status.")

    counts = {k: int((df["Status"] == k).sum()) for k in STATUS_META if k}

    counter_cols = st.columns(len(columns_order))
    for key, col in zip(columns_order, counter_cols):
        meta = STATUS_META[key]
        col.markdown(
            f"""
            <div class="wfp-counter" style="background:{meta['color']}22;border-color:{meta['color']}66;">
              <div class="num">{counts.get(key,0)}</div>
              <div class="lbl">{meta['label']}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.divider()

    # -------------------------------------------------
    # FILTERING
    # -------------------------------------------------
    f1, f2 = st.columns([2,1])
    with f1:
        q = st.text_input("Search (Task ID / Property / PO / Notes)", "")
    with f2:
        flt = st.selectbox("Filter by Status", ["All"] + columns_order)

    view = df.copy()
    if hide_ready:
        view = view[view["Status"] != "Ready"]
    if q:
        ql = q.lower()
        view = view[view.apply(lambda r: ql in " ".join(map(str, r.values)).lower(), axis=1)]
    if flt != "All":
        view = view[view["Status"] == flt]

    # -------------------------------------------------
    # SORTING ORDER
    # -------------------------------------------------
    def _sort_view(df_in: pd.DataFrame) -> pd.DataFrame:
        df2 = df_in.copy()
        df2["_rank"] = df2["Status"].map(STATUS_RANK).fillna(500).astype(int)
        df2 = df2.sort_values(by=["_rank","Property","Task ID"], ascending=[True, True, True])
        return df2.drop(columns=["_rank"], errors="ignore")

    view = _sort_view(view)

    # -------------------------------------------------
    # COLOR GRID HELPER
    # -------------------------------------------------
    def _colorize_wfp(view_df: pd.DataFrame):
        need_cols = ["Task ID","Property","PO","Status","Notes","Last Updated",
                     "First Seen","Last Seen","Weeks On List","This Week"]
        for c in need_cols:
            if c not in view_df.columns:
                view_df[c] = ""

        def _row_style(r):
            color = STATUS_META.get(r.get("Status",""), STATUS_META[""])["color"]
            bg = f"background-color: {color}22"
            bd = f"border-bottom: 1px solid {color}55"
            return [f"{bg}; {bd}" for _ in r]

        return (
            view_df[need_cols]
            .style.apply(_row_style, axis=1)
            .set_properties(**{"white-space":"pre-wrap"})
        )

    # -------------------------------------------------
    # RENDER MODE
    # -------------------------------------------------
    mode = st.session_state.get("wfp_view", "Grid view (bulk)")

    if mode == "Grid view (bulk)":
        grid = view.copy()
        grid["Status"] = grid["Status"].where(grid["Status"].isin(columns_order + [""]), "")
        grid["Last Updated"] = pd.to_datetime(grid["Last Updated"], errors="coerce").dt.date
        grid["First Seen"]   = pd.to_datetime(grid.get("First Seen"), errors="coerce").dt.date
        grid["Last Seen"]    = pd.to_datetime(grid.get("Last Seen"), errors="coerce").dt.date
        grid["Weeks On List"] = pd.to_numeric(grid.get("Weeks On List"), errors="coerce").fillna(0).astype(int)
        grid["This Week"]     = grid.get("This Week").astype(bool)

        nrows = max(8, len(grid))
        editor_height = max(500, min(1000, 120 + 38 * nrows))

        edited = st.data_editor(
            grid,
            use_container_width=True,
            num_rows="dynamic",
            height=editor_height,
            column_config={
                "Task ID": st.column_config.TextColumn("Task ID"),
                "Property": st.column_config.TextColumn("Property"),
                "PO": st.column_config.TextColumn("PO"),
                "Status": st.column_config.SelectboxColumn("Status", options=columns_order + [""] ),
                "Notes": st.column_config.TextColumn("Notes", width="medium"),
                "Weeks On List": st.column_config.NumberColumn("Weeks On List", step=1, format="%d"),
                "First Seen": st.column_config.DateColumn("First Seen", format="YYYY-MM-DD"),
                "Last Seen": st.column_config.DateColumn("Last Seen", format="YYYY-MM-DD"),
                "This Week": st.column_config.CheckboxColumn("This Week"),
                "Last Updated": st.column_config.DateColumn("Last Updated", format="YYYY-MM-DD"),
            },
            key="wfp_editor",
        )

        if st.button("💾 Save all changes to GitHub", key="wfp_bulk_save"):
            out = edited.copy()

            # Date serialization
            for dcol in ["Last Updated","First Seen","Last Seen"]:
                out[dcol] = out[dcol].apply(lambda d: d.isoformat() if pd.notna(d) and d != "" else "")

            out["Weeks On List"] = pd.to_numeric(out["Weeks On List"], errors="coerce").fillna(0).astype(int)
            out["This Week"] = out["This Week"].astype(bool)

            need_cols = ["Task ID","Property","PO","Status","Notes","Last Updated",
                         "First Seen","Last Seen","Weeks On List","This Week"]

            for c in need_cols:
                if c not in out.columns:
                    out[c] = ""

            full = df.copy().set_index("Task ID")
            out  = out.set_index("Task ID")

            full.loc[out.index, need_cols[1:]] = out[need_cols[1:]]
            full = full.reset_index()

            save_wfp(full, message="Waiting for Parts: bulk update")
            st.success("Saved changes to GitHub.")
            st.rerun()

    else:
        styled = _colorize_wfp(view.copy())
        st.dataframe(styled, use_container_width=True)
        st.caption("Tip: switch to ‘Grid view (bulk)’ to edit values.")



