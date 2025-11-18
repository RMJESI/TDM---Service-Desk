# waiting_for_parts.py — TDM Ops Console Edition

import streamlit as st
import pandas as pd
from datetime import date
from persist_waiting_parts import load_wfp, save_wfp, weekly_sync

# -------------------------------------------------
# RENDER WAITING FOR PARTS TAB
# -------------------------------------------------
def render_wfp_tab(sidebar_root):

    # ---------- Sidebar CSS (dark red / black / gray / white palette) ----------
    st.markdown("""
    <style>

      /* Sidebar label */
      [data-testid="stSidebar"] .sb-label {
          color: #ffffff !important;
          font-weight: 700 !important;
          font-size: 1rem !important;
      }

      /* Buttons inside sidebar */
      [data-testid="stSidebar"] .stButton > button {
          background-color: #ffffff !important;
          color: #2a2a2a !important;
          border: 1px solid #6b6b6b !important;
          font-weight: 700 !important;
          border-radius: 8px !important;
      }

      /* Ensure inner text/icons stay dark */
      [data-testid="stSidebar"] .stButton > button * {
          color: #2a2a2a !important;
          fill: #2a2a2a !important;
      }

      /* Hover */
      [data-testid="stSidebar"] .stButton > button:hover {
          background-color: #f0f0f0 !important;
          color: #1a1a1a !important;
      }

    </style>
    """, unsafe_allow_html=True)

    # ---------- Sidebar controls ----------
    sbx = sidebar_root.container()
    with sbx:
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

        # Recompute weeks
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

            # Ensure columns exist
            need_cols = [
                "Task ID","Property","PO","Status","Notes","Last Updated",
                "First Seen","Last Seen","Weeks On List","This Week"
            ]
            for c in need_cols:
                if c not in _df_now.columns:
                    _df_now[c] = ""

            fixed = _recompute_weeks_from_first_seen(_df_now, date.today().isoformat())
            save_wfp(fixed, message=f"WFP: recompute weeks as of {date.today().isoformat()}")
            st.success("Weeks On List recomputed.")
            st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

    # -------------------------------------------------
    # STATUS COLORS — TDM palette
    # -------------------------------------------------
    STATUS_META = {
        "Backordered":          {"color":"#8b1e3f"},   # Maroon-ish red
        "Ordered Recently":     {"color":"#b84156"},   # Lighter red
        "Has Tracking Info":    {"color":"#5f7c8a"},   # Steel blue gray
        "Ready":                {"color":"#1d1d1d"},   # Black
        "Upholstery":           {"color":"#c05621"},   # Burnt orange
        "Direct Shipped":       {"color":"#a22a7e"},   # Dark magenta
        "Need to Email Vendor": {"color":"#6c3fbb"},   # Purple
        "Need to Email Team":   {"color":"#d65d5d"},   # Muted red
        "Other":                {"color":"#999999"},   # Gray
        "":                     {"color":"#d7d7d7"},   # Unset
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
    STATUS_RANK["Ready"] = 999  # always pushed to bottom

    # -------------------------------------------------
    # COUNTERS
    # -------------------------------------------------
    st.markdown("""
    <style>
      .wfp-counter {
        display:flex; align-items:center; justify-content:center;
        gap:.6rem;
        border-radius: 12px;
        padding: 14px 18px;
        font-weight: 800;
        border: 2px solid rgba(0,0,0,0.08);
        box-shadow: 0 2px 6px rgba(0,0,0,0.06);
        min-height: 68px;
        background:#ffffff;
      }
      .wfp-counter .num { font-size: 28px; line-height: 1; color:#1d1d1d; }
      .wfp-counter .lbl { font-size: 17px; color:#4a4a4a; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("### ⚙️ Waiting for Parts")

    # -------------------------------------------------
    # WEEKLY SYNC (Upload)
    # -------------------------------------------------
    with st.expander("📥 Weekly sync (Miracle export)"):

        st.caption("Upload this week's CSV. New Task IDs added. Old rows updated safely.")

        if "wfp_processed_signature" not in st.session_state:
            st.session_state["wfp_processed_signature"] = None
        if "wfp_uploader_nonce" not in st.session_state:
            st.session_state["wfp_uploader_nonce"] = 0

        sync_date = st.date_input(
            "Sync date",
            value=date.today(),
            format="YYYY-MM-DD",
            key="wfp_sync_date"
        )

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

        process_clicked = st.button(
            "🚀 Process upload",
            disabled=(seed_file is None),
            key="wfp_process_btn"
        )

        if process_clicked and seed_file is not None:
            try:
                if sig == st.session_state["wfp_processed_signature"]:
                    st.info("This file has already been processed.")
                else:
                    seed_df = pd.read_csv(seed_file, dtype=str).fillna("")
                    merged = weekly_sync(seed_df, as_of=sync_date.isoformat())
                    save_wfp(merged, message=f"WFP weekly sync {sync_date.isoformat()}")

                    # Avoid duplicate processing
                    st.session_state["wfp_processed_signature"] = sig
                    st.session_state["wfp_uploader_nonce"] += 1

                    st.success("Weekly sync complete.")
                    st.toast("Weekly sync complete.", icon="✅")
                    st.rerun()

            except Exception as e:
                st.error(f"Weekly sync failed: {e}")

    # -------------------------------------------------
    # LOAD TABLE
    # -------------------------------------------------
    df = load_wfp().copy()

    need_cols = [
        "Task ID","Property","PO","Status","Notes","Last Updated",
        "First Seen","Last Seen","Weeks On List","This Week"
    ]
    for c in need_cols:
        if c not in df.columns:
            df[c] = ""

    df["Weeks On List"] = pd.to_numeric(df["Weeks On List"], errors="coerce").fillna(0).astype(int)
    df["This Week"] = df["This Week"].astype(str).str.lower().isin(["1","true","yes","y","t"])

    # -------------------------------------------------
    # TOP FILTERS
    # -------------------------------------------------
    act1, act2 = st.columns([2, 1])
    with act1:
        hide_ready = st.checkbox("Hide ‘Ready’ rows", value=False, key="wfp_hide_ready")
    with act2:
        st.caption("Tip: Delete Ready rows after ~2 weeks.")

    # Status counts
    counts = {k: int((df["Status"] == k).sum()) for k in STATUS_META if k}

    # Counter tiles
    counter_cols = st.columns(len(columns_order))
    for key, col in zip(columns_order, counter_cols):
        col.markdown(
            f"""
            <div class="wfp-counter">
              <div class="num">{counts.get(key,0)}</div>
              <div class="lbl">{key}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.divider()

    # -------------------------------------------------
    # SEARCH / FILTER
    # -------------------------------------------------
    f1, f2 = st.columns([2,1])
    with f1:
        q = st.text_input("Search", "")
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
    # SORT
    # -------------------------------------------------
    def _sort(df_in):
        df2 = df_in.copy()
        df2["_rank"] = df2["Status"].map(STATUS_RANK).fillna(500)
        return df2.sort_values(by=["_rank","Property","Task ID"]).drop(columns=["_rank"], errors="ignore")

    view = _sort(view)

    # -------------------------------------------------
    # COLOR GRID
    # -------------------------------------------------
    def _color_grid(vdf):
        need_cols = [
            "Task ID","Property","PO","Status","Notes","Last Updated",
            "First Seen","Last Seen","Weeks On List","This Week"
        ]
        for c in need_cols:
            if c not in vdf.columns:
                vdf[c] = ""

        def _row_style(r):
            color = STATUS_META.get(r.get("Status",""), STATUS_META[""])["color"]
            return [f"background-color:{color}22; border-bottom:1px solid {color}55;" for _ in r]

        return (
            vdf[need_cols]
            .style.apply(_row_style, axis=1)
            .set_properties(**{"white-space":"pre-wrap"})
        )

    # -------------------------------------------------
    # VIEW MODE
    # -------------------------------------------------
    if st.session_state.get("wfp_view") == "Grid view (bulk)":

        grid = view.copy()
        grid["Status"] = grid["Status"].where(grid["Status"].isin(columns_order + [""]), "")

        # Parse dates
        for col in ["Last Updated","First Seen","Last Seen"]:
            grid[col] = pd.to_datetime(grid[col], errors="coerce").dt.date

        grid["Weeks On List"] = pd.to_numeric(grid["Weeks On List"], errors="coerce").fillna(0).astype(int)
        grid["This Week"] = grid["This Week"].astype(bool)

        nrows = max(8, len(grid))
        editor_height = max(500, min(1000, 120 + 38 * nrows))

        edited = st.data_editor(
            grid,
            use_container_width=True,
            num_rows="dynamic",
            height=editor_height,
            key="wfp_editor",
            column_config={
                "Task ID": st.column_config.TextColumn("Task ID"),
                "Property": st.column_config.TextColumn("Property"),
                "PO": st.column_config.TextColumn("PO"),
                "Status": st.column_config.SelectboxColumn("Status", options=columns_order + [""]),
                "Notes": st.column_config.TextColumn("Notes", width="medium"),
                "Weeks On List": st.column_config.NumberColumn("Weeks On List", step=1),
                "First Seen": st.column_config.DateColumn("First Seen", format="YYYY-MM-DD"),
                "Last Seen": st.column_config.DateColumn("Last Seen", format="YYYY-MM-DD"),
                "This Week": st.column_config.CheckboxColumn("This Week"),
                "Last Updated": st.column_config.DateColumn("Last Updated", format="YYYY-MM-DD"),
            },
        )

        if st.button("💾 Save all changes", key="wfp_bulk_save"):
            out = edited.copy()

            # Convert dates back to strings
            for dcol in ["Last Updated","First Seen","Last Seen"]:
                out[dcol] = out[dcol].apply(lambda d: d.isoformat() if pd.notna(d) and d != "" else "")

            out["Weeks On List"] = pd.to_numeric(out["Weeks On List"], errors="coerce").fillna(0).astype(int)
            out["This Week"] = out["This Week"].astype(bool)

            # Ensure all columns exist
            for c in need_cols:
                if c not in out.columns:
                    out[c] = ""

            full = df.copy().set_index("Task ID")
            out = out.set_index("Task ID")

            # overwrite corresponding rows
            full.loc[out.index, need_cols[1:]] = out[need_cols[1:]]
            full = full.reset_index()

            save_wfp(full, message="WFP bulk update")
            st.success("Saved.")
            st.rerun()

    else:
        styled = _color_grid(view.copy())
        st.dataframe(styled, use_container_width=True)
        st.caption("Switch to Grid view to edit values.")
