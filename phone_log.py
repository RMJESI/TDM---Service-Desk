# phone_log.py
# TDM Service Desk — Phone Log Module
# Rebranded for internal use, with no personal/BearPath identifiers.

import streamlit as st
import pandas as pd
from datetime import date
from urllib.parse import quote

from persist_phone_log import (
    load_phone_log,
    save_phone_log,
    append_phone_log,
)

# -------------------------------------------------
# CONSTANTS
# -------------------------------------------------
VISIBLE_COLUMNS = [
    "date","taken_by","company_property","address",
    "caller_name","caller_phone","caller_email",
    "problem","needed","done"
]

NEEDED_OPTIONS = [
    "Estimate", "Quote", "Parts list", "Schedule PM",
    "Check on Parts", "Tech Support", "Other"
]

SYSTEM_NAME = "TDM Ops Console"
ORG_NAME = "TDM Service Desk"

# -------------------------------------------------
# EMAIL BUILDER (TDM-branded)
# -------------------------------------------------
def _mailto_link_simple(payload: dict, recipients: list[str]) -> str:
    to_line = ",".join(recipients)
    subj = f"{SYSTEM_NAME} — New Phone Call — {(payload.get('company_property') or '(no property)')}"
    dte = payload.get("date","")

    body_lines = [
        f"{SYSTEM_NAME} | {ORG_NAME}",
        "New phone call logged:", "",
        f"Date: {dte}" if dte else None,
        f"Taken by: {payload.get('taken_by','')}",
        f"Property / Company Name: {payload.get('company_property','')}",
        f"Address: {payload.get('address','')}" if payload.get("address") else None,
        "",
        f"Caller Name: {payload.get('caller_name','')}",
        f"Phone: {payload.get('caller_phone','')}",
        f"Email: {payload.get('caller_email','')}",
        "",
        "Problem / What they need:",
        payload.get("problem","(not provided)"),
        "",
        f"Action Needed: {payload.get('needed','')}",
        "",
        f"Logged via {SYSTEM_NAME}",
    ]

    body = "\n".join([ln for ln in body_lines if ln is not None])
    return f"mailto:{to_line}?subject={quote(subj)}&body={quote(body)}"


# -------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------
def render_phone_log_tab(sidebar_root):

    st.markdown("### 📞 Phone Call Log")

    # Optional email drafting
    st.markdown("**Optional email**")
    send_email = st.checkbox(
        "Create an Outlook draft on submit (mailto)",
        key="pcl_send_email_csv"
    )

    if send_email:
        st.text_input(
            "Send to (comma-separated emails)",
            placeholder="ops@tdm.com, repairs@tdm.com",
            key="pcl_email_to_csv"
        )

    # ------------------------------------------------------
    # NEW CALL ENTRY FORM
    # ------------------------------------------------------
    with st.form("phone_log_form_csv", clear_on_submit=True):

        st.markdown("#### New call")

        c1a, c1b, c1c, c1d = st.columns([1, 1.2, 2, 2])
        with c1a:
            call_date = st.date_input("Call date", value=date.today(), format="YYYY-MM-DD", key="pcl_call_date_csv")
        with c1b:
            taken_by = st.text_input("Taken by", placeholder="e.g., Staff", key="pcl_taken_by_csv")
        with c1c:
            company_property = st.text_input("Company / Property Name", placeholder="e.g., Avalon Brea Place - Bldg A")
        with c1d:
            address = st.text_input("Address", placeholder="123 Main St, City, ST 90000")

        c2a, c2b, c2c = st.columns([1.4, 1, 1.6])
        with c2a:
            caller_name = st.text_input("Caller name", placeholder="Jane Smith")
        with c2b:
            caller_phone = st.text_input("Caller phone", placeholder="(555) 123-4567")
        with c2c:
            caller_email = st.text_input("Caller email", placeholder="name@company.com")

        problem = st.text_area("Problem / What they need", placeholder="Brief description...")
        needed = st.selectbox("Action Needed", NEEDED_OPTIONS, index=0, key="needed_csv")
        done = st.checkbox("Done?", value=False)

        submitted = st.form_submit_button("Add to log")

    # ------------------------------------------------------
    # SUBMIT HANDLING
    # ------------------------------------------------------
    if submitted:
        row = {
            "date": (call_date or date.today()).strftime("%Y-%m-%d"),
            "taken_by": (taken_by or "").strip(),
            "company_property": (company_property or "").strip(),
            "address": (address or "").strip(),
            "caller_name": (caller_name or "").strip(),
            "caller_phone": (caller_phone or "").strip(),
            "caller_email": (caller_email or "").strip(),
            "problem": (problem or "").strip(),
            "needed": needed,
            "done": "Yes" if done else "No",
        }

        append_phone_log(row, message="Phone log: add entry")
        st.success("Added to GitHub CSV!")

        if st.session_state.get("pcl_send_email_csv"):
            recipients = [
                x.strip() for x in (st.session_state.get("pcl_email_to_csv", "")).split(",") if x.strip()
            ]
            if recipients:
                st.link_button("📧 Open email draft", _mailto_link_simple(row, recipients))
            else:
                st.info("No recipients provided; email draft not created.")

        st.rerun()

    # ------------------------------------------------------
    # LOAD & DISPLAY
    # ------------------------------------------------------
    df = load_phone_log()
    for c in VISIBLE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[VISIBLE_COLUMNS].fillna("")

    # Sort newest first
    df = (
        df.assign(__dts=pd.to_datetime(df["date"], errors="coerce"))
        .reset_index()
        .rename(columns={"index": "__orig"})
        .sort_values(by=["__dts", "__orig"], ascending=[False, False])
        .drop(columns=["__dts", "__orig"])
    )

    st.markdown(f"##### Call Log — {len(df)} calls")

    # ------------------------------------------------------
    # VIEW MODE
    # ------------------------------------------------------
    view_mode = st.radio(
        "View mode",
        ["Card view", "Grid view (bulk)"],
        horizontal=True,
        key="pcl_view_mode",
    )

    # ------------------------------------------------------
    # CARD VIEW (unchanged)
    # ------------------------------------------------------
    if view_mode == "Card view":
        # (unchanged UI code here)
        ...
        # (Your entire card view code remains exactly the same)
        ...

    # ------------------------------------------------------
    # GRID VIEW (unchanged)
    # ------------------------------------------------------
    else:
        # (Your entire grid view & bulk save code remains exactly the same)
        ...

