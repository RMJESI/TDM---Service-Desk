# phone_log.py
# Extracted cleanly from ui.py with ZERO behavior changes.
# Wrapped into render_phone_log_tab() for modular imports.

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

BRAND_NAME = "BearPath"
ORG_NAME = "TDM Service Desk"

# -------------------------------------------------
# EMAIL BUILDER
# -------------------------------------------------
def _mailto_link_simple(payload: dict, recipients: list[str]) -> str:
    to_line = ",".join(recipients)
    subj = f"{BRAND_NAME} | {ORG_NAME} — New Phone Call — {(payload.get('company_property') or '(no property)')}"
    dte = payload.get("date","")

    body_lines = [
        f"{BRAND_NAME} | {ORG_NAME}",
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
        f"Logged via {BRAND_NAME} | {ORG_NAME}",
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
            placeholder="ops@company.com, bobby@company.com",
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
            taken_by = st.text_input("Taken by", placeholder="e.g., Remy", key="pcl_taken_by_csv")
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
    # LOAD + NORMALIZE DISPLAY DATA
    # ------------------------------------------------------
    df = load_phone_log()
    for c in VISIBLE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[VISIBLE_COLUMNS].fillna("")

    # sort by date desc
    df = (
        df.assign(__dts=pd.to_datetime(df["date"], errors="coerce"))
        .reset_index()
        .rename(columns={"index": "__orig"})
        .sort_values(by=["__dts", "__orig"], ascending=[False, False])
        .drop(columns=["__dts", "__orig"])
    )

    st.markdown(f"##### Call Log — {len(df)} calls")

    # ------------------------------------------------------
    # VIEW MODE SELECTION
    # ------------------------------------------------------
    view_mode = st.radio(
        "View mode",
        ["Card view", "Grid view (bulk)"],
        horizontal=True,
        key="pcl_view_mode",
    )

    # ------------------------------------------------------
    # CARD VIEW
    # ------------------------------------------------------
    if view_mode == "Card view":
        st.markdown("""
        <style>
          .pcl-card {
            position: relative;
            border-radius: 14px;
            background: rgba(255,255,255,0.85);
            -webkit-backdrop-filter: blur(14px);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(22, 52, 34, 0.12);
            padding: 10px 12px 12px 12px;
            margin: 6px 0 14px 0;
          }
          .pcl-status {
            padding: 8px 12px;
            margin: -2px -2px 10px -2px;
            border-radius: 10px;
            border: 1px solid #cfe0d4;
            font-weight: 600;
          }
          .pcl-problem p, .pcl-problem div, .pcl-problem span {
            white-space: pre-wrap !important;
            word-break: break-word !important;
          }
        </style>
        """, unsafe_allow_html=True)

        for i, row in df.reset_index(drop=True).iterrows():

            title_bits = [row.get("date",""), row.get("company_property",""), row.get("caller_name","")]
            title = " — ".join([t for t in title_bits if t]) or f"Call {i+1}"

            with st.expander(title, expanded=False):
                curr_done = (str(row.get("done","")).strip().lower() in ("yes","true","1"))

                st.markdown("<div class='pcl-card'>", unsafe_allow_html=True)

                status_bg = "#e8f5e9" if curr_done else "#ffffff"
                status_txt = "Done ✅" if curr_done else "Open ⏳"
                st.markdown(f"<div class='pcl-status' style='background:{status_bg};'>{status_txt}</div>", unsafe_allow_html=True)

                # Row control checkboxes
                top_cols = st.columns([1, 1, 1])
                with top_cols[0]:
                    new_done_box = st.checkbox("Done", value=curr_done, key=f"pcl_card_done_{i}")
                with top_cols[1]:
                    edit_mode = st.checkbox("✏️ Edit this call", value=False, key=f"pcl_card_edit_{i}")
                with top_cols[2]:
                    want_delete = st.button("🗑️ Delete", key=f"pcl_card_delete_{i}")

                # DELETE
                if want_delete:
                    df2 = df.drop(df.index[i]).reset_index(drop=True)
                    save_phone_log(df2, message=f"Phone log: delete row {i}")
                    st.success("Deleted.")
                    st.rerun()

                # VIEW MODE (not editing)
                if not edit_mode:

                    st.markdown("**Taken by:** " + (row.get("taken_by") or ""))
                    st.markdown("**Action Needed:** " + (row.get("needed") or ""))
                    st.markdown("**Address:** " + (row.get("address") or ""))
                    st.markdown("**Caller:** " + (row.get("caller_name") or ""))
                    st.markdown("**Phone / Email:** " + " / ".join(
                        [x for x in [row.get("caller_phone") or "", row.get("caller_email") or ""] if x]
                    ))

                    st.markdown("**Problem / What they need:**")
                    safe_problem = (row.get('problem') or '').replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
                    st.markdown(f"<div class='pcl-problem'>{safe_problem}</div>", unsafe_allow_html=True)

                    # Save changes if toggled done
                    if new_done_box != curr_done:
                        df2 = df.copy()
                        df2.iloc[i, df.columns.get_loc("done")] = "Yes" if new_done_box else "No"
                        save_phone_log(df2, message="Phone log: mark done (card)")
                        st.success("Saved.")
                        st.rerun()

                # EDIT MODE
                else:
                    # Parse date
                    try:
                        _curr_date = pd.to_datetime(row.get("date",""), errors="coerce").date()
                    except Exception:
                        _curr_date = date.today()

                    # Input rows
                    e1, e2, e3, e4 = st.columns([1, 1.2, 2, 2])
                    with e1:
                        new_date = st.date_input("Date", value=_curr_date, format="YYYY-MM-DD", key=f"edit_date_card_{i}")
                    with e2:
                        new_taken_by = st.text_input("Taken by", value=row.get("taken_by") or "", key=f"edit_taken_by_card_{i}")
                    with e3:
                        new_company = st.text_input("Company / Property", value=row.get("company_property") or "", key=f"edit_company_card_{i}")
                    with e4:
                        new_address = st.text_input("Address", value=row.get("address") or "", key=f"edit_address_card_{i}")

                    e5, e6, e7 = st.columns([1.4, 1, 1.6])
                    with e5:
                        new_caller = st.text_input("Caller name", value=row.get("caller_name") or "", key=f"edit_caller_card_{i}")
                    with e6:
                        new_phone = st.text_input("Caller phone", value=row.get("caller_phone") or "", key=f"edit_phone_card_{i}")
                    with e7:
                        new_email = st.text_input("Caller email", value=row.get("caller_email") or "", key=f"edit_email_card_{i}")

                    new_needed = st.selectbox(
                        "Action Needed", NEEDED_OPTIONS,
                        index=(
                            NEEDED_OPTIONS.index(row.get("needed")) 
                            if row.get("needed") in NEEDED_OPTIONS 
                            else 0
                        ),
                        key=f"edit_needed_card_{i}"
                    )

                    new_problem = st.text_area("Problem / What they need", value=row.get("problem") or "", key=f"edit_problem_card_{i}")

                    # SAVE BUTTON
                    if st.button("Save edits", key=f"save_edit_card_{i}"):

                        df2 = df.copy()
                        df2.iloc[i, df.columns.get_loc("date")] = new_date.strftime("%Y-%m-%d") if new_date else ""
                        df2.iloc[i, df.columns.get_loc("taken_by")] = new_taken_by.strip()
                        df2.iloc[i, df.columns.get_loc("company_property")] = new_company.strip()
                        df2.iloc[i, df.columns.get_loc("address")] = new_address.strip()
                        df2.iloc[i, df.columns.get_loc("caller_name")] = new_caller.strip()
                        df2.iloc[i, df.columns.get_loc("caller_phone")] = new_phone.strip()
                        df2.iloc[i, df.columns.get_loc("caller_email")] = new_email.strip()
                        df2.iloc[i, df.columns.get_loc("needed")] = new_needed
                        df2.iloc[i, df.columns.get_loc("problem")] = new_problem.strip()
                        df2.iloc[i, df.columns.get_loc("done")] = "Yes" if new_done_box else "No"

                        save_phone_log(df2, message="Phone log: edit row (card)")
                        st.success("Saved.")
                        st.rerun()

                st.markdown("</div>", unsafe_allow_html=True)

    # ------------------------------------------------------
    # GRID VIEW
    # ------------------------------------------------------
    else:

        df_display = df.copy()
        df_display["date"] = pd.to_datetime(df_display["date"], errors="coerce").dt.date

        def _to_yes_no(v):
            s = str(v).strip().lower()
            return "Yes" if s in ("yes","true","1") else "No"

        df_display["done"] = df_display["done"].map(_to_yes_no)
        df_display["needed"] = df_display["needed"].where(
            df_display["needed"].isin(NEEDED_OPTIONS),
            NEEDED_OPTIONS[0]
        )

        edited_df = st.data_editor(
            df_display,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "date": st.column_config.DateColumn("date", format="YYYY-MM-DD"),
                "taken_by": st.column_config.TextColumn("taken_by"),
                "company_property": st.column_config.TextColumn("company_property"),
                "address": st.column_config.TextColumn("address"),
                "caller_name": st.column_config.TextColumn("caller_name"),
                "caller_phone": st.column_config.TextColumn("caller_phone"),
                "caller_email": st.column_config.TextColumn("caller_email"),
                "problem": st.column_config.TextColumn("problem", width="medium"),
                "needed": st.column_config.SelectboxColumn("needed", options=NEEDED_OPTIONS),
                "done": st.column_config.SelectboxColumn("done", options=["No", "Yes"]),
            },
            key="phone_editor_csv",
        )

        if st.button("💾 Save all changes to GitHub", key="pcl_bulk_save_csv"):
            try:
                out_df = edited_df.copy()

                # Date serialization
                out_df["date"] = out_df["date"].apply(
                    lambda d: d.isoformat() if pd.notna(d) and d != "" else ""
                )

                out_df["done"] = out_df["done"].astype(str)
                out_df["needed"] = out_df["needed"].astype(str)

                # normalize all text cols
                for col in ["taken_by","company_property","address","caller_name",
                            "caller_phone","caller_email","problem"]:
                    out_df[col] = out_df[col].fillna("").astype(str)

                save_phone_log(out_df, message="Phone log: bulk update")
                st.success("Saved changes to GitHub CSV.")
                st.rerun()

            except Exception as e:
                st.error(f"Save failed: {e}")

