# confirmations.py — TDM Service Desk Version
# Miracle Parsing + Editable Table (No Refresh) + CSV Export
# Fully rebranded for The Dumbell Man (TDM)

import streamlit as st
import pandas as pd
import io


# ---------------------------------------------------------------
# Miracle Parser (unchanged logic)
# ---------------------------------------------------------------
def parse_miracle_table(text: str) -> pd.DataFrame:
    """Parses Miracle-pasted tab-delimited data and returns normalized rows."""

    df = pd.read_csv(io.StringIO(text), sep="\t", dtype=str).fillna("")

    # Normalize header names
    rename_map = {}
    for col in df.columns:
        key = col.lower().strip()

        if key in ("taskid", "wo id", "workorder", "work order"):
            rename_map[col] = "WO ID"
        elif key in ("company name", "property name", "property"):
            rename_map[col] = "Property Name"
        elif key == "requester":
            rename_map[col] = "Requester"
        elif key in ("scheduled date", "date/time", "datetime", "date"):
            rename_map[col] = "Scheduled Date"
        elif key in ("task type", "service type"):
            rename_map[col] = "Service Type"
        elif key in ("tech name", "technician", "tech"):
            rename_map[col] = "Tech Name"
        elif key in ("customer email", "email"):
            rename_map[col] = "Customer Email"

    df = df.rename(columns=rename_map)

    # Ensure required columns exist
    required_cols = [
        "WO ID",
        "Property Name",
        "Requester",
        "Scheduled Date",
        "Service Type",
        "Tech Name",
        "Customer Email",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # Parse Scheduled Date → Date + Time
    parsed = pd.to_datetime(df["Scheduled Date"], errors="coerce")
    df["Date"] = parsed.dt.date.astype(str)
    df["Time"] = parsed.dt.strftime("%I:%M %p")

    # Final ordered DF
    cleaned = df[
        [
            "WO ID",
            "Property Name",
            "Requester",
            "Date",
            "Time",
            "Service Type",
            "Tech Name",
            "Customer Email",
        ]
    ]

    return cleaned


# ---------------------------------------------------------------
# TDM Confirmations Page
# ---------------------------------------------------------------
def render_confirmations_tab():

    # -----------------------------------------------------------
    # TDM Branding Header
    # -----------------------------------------------------------
    st.markdown(
        """
        <h1 style="color:#FFFFFF; font-weight:700; margin-bottom:0px;">
            TDM Service Desk — Service Confirmations
        </h1>
        <p style="color:#CCCCCC; margin-top:0px;">
            Process Miracle exports and generate clean CSV outputs for Power Automate.
        </p>
        """,
        unsafe_allow_html=True,
    )

    # Session
    if "parsed_confirmations" not in st.session_state:
        st.session_state["parsed_confirmations"] = None

    # ============================================================
    # Step 1 — Paste Miracle Table
    # ============================================================
    st.markdown(
        "<h3 style='color:#7A001F;'>1. Paste Miracle Table</h3>",
        unsafe_allow_html=True,
    )

    miracle_text = st.text_area(
        "Paste data including the header row:",
        placeholder=(
            "TaskID\tCompany Name\tRequester\tScheduled Date\tTask Type\tTech Name\tCustomer Email\n"
            "0037580-2\tMesa Verde\tOctavio\t11/10/2025 6:00 AM\tSVC Repair\tSub-Contractor\temail@example.com"
        ),
        height=220,
        key="miracle_input",
    )

    if st.button(
        "Validate & Parse Table",
        type="primary",
        key="parse_btn",
        help="Parse the Miracle export into a structured table.",
    ):
        if not miracle_text.strip():
            st.warning("Paste Miracle data before parsing.")
        else:
            try:
                st.session_state["parsed_confirmations"] = parse_miracle_table(miracle_text)
                st.success("Table parsed successfully.")
            except Exception as e:
                st.error(f"Error parsing table: {e}")

    parsed_df = st.session_state.get("parsed_confirmations")

    # ============================================================
    # Step 2 — Editable Parsed Table (no refresh on keystroke)
    # ============================================================
    if parsed_df is not None:
        st.markdown(
            "<h3 style='color:#7A001F;'>2. Review & Edit Rows</h3>",
            unsafe_allow_html=True,
        )

        # Wrap in form so it doesn't rerun on every keystroke
        with st.form("edit_table_form"):
            edited_df = st.data_editor(
                parsed_df,
                num_rows="dynamic",
                use_container_width=True,
                key="editable_confirmations_editor",
            )

            submitted = st.form_submit_button(
                "Save Edits",
                type="primary",
            )

        if submitted:
            st.session_state["parsed_confirmations"] = edited_df
            st.success("Edits saved.")

        st.markdown("<hr style='border-color:#3C3C3C;'>", unsafe_allow_html=True)

        # ========================================================
        # Step 3 — Export CSV
        # ========================================================
        st.markdown(
            "<h3 style='color:#7A001F;'>3. Export to CSV</h3>",
            unsafe_allow_html=True,
        )

        csv_bytes = st.session_state["parsed_confirmations"].to_csv(index=False).encode("utf-8")

        st.download_button(
            label="📥 Download CSV",
            data=csv_bytes,
            file_name="tdm_service_confirmations.csv",
            mime="text/csv",
            type="primary",
        )
