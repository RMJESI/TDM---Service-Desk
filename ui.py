# ui.py — TDM Ops Console (final clean)

import os
import base64
from datetime import datetime, date

import streamlit as st
import pandas as pd  # kept in case you need it later

# --------------------------------------------------------------------
# Modularized subtabs
# --------------------------------------------------------------------
from scheduler import render_scheduler_tab
from confirmations import render_confirmations_tab
from waiting_for_parts import render_wfp_tab
from phone_log import render_phone_log_tab
from coordinate_confidence import render_coordinate_confidence_tab

# DB health check
from models import connect

# --------------------------------------------------------------------
# Initial Streamlit setup
# --------------------------------------------------------------------
st.set_page_config(page_title="TDM Ops Console", layout="wide")

# --------------------------------------------------------------------
# DB connection (cached) + health indicator
# --------------------------------------------------------------------
@st.cache_resource
def get_conn_v3(_version: str = "2025-11-04.3"):
    return connect()

try:
    _con = get_conn_v3()
    _con.execute("SELECT 1")
    st.sidebar.success("✅ Connected to Database")
except Exception as e:
    st.sidebar.error(f"❌ DB load failed: {e}")

# --------------------------------------------------------------------
# Background image
# --------------------------------------------------------------------
APP_DIR = os.path.dirname(__file__)
BANNER_FILE = os.path.join(APP_DIR, "tdm_banner.jpg")   # renamed for TDM branding

def _img_to_base64(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

banner64 = _img_to_base64(BANNER_FILE)

def inject_background_layer(
    b64: str,
    pos_x_percent: float = 9.5,
    pos_y_percent: float = 9,
    scale: float = 1,
    opacity: float = 1.0,
    scroll_mode: str = "fixed",
    darken: float = 0.18
):
    """
    Injects a decorative background image behind the app.
    Clean + brand-neutral version for TDM.
    """
    if not b64:
        st.markdown("""
        <style>
        .stApp { background: linear-gradient(180deg, #f7f9fb 0%, #eef3f0 100%); }
        .block-container {
            background: rgba(255,255,255,0.78);
            padding-top: 1.25rem;
            padding-bottom: 1.5rem;
            border-radius: 18px;
        }
        </style>
        """, unsafe_allow_html=True)
        return

    st.markdown(f"""
    <style>
      .stApp {{
        background: none !important;
      }}

      .bp-bg {{
        position: {scroll_mode};
        inset: 0;
        z-index: -1;
        overflow: hidden;
      }}

      .bp-bg__img {{
        position: absolute;
        left: 50%; top: 50%;
        transform: translate({pos_x_percent - 50}%, {pos_y_percent - 50}%) scale({scale});
        min-width: 100vw; min-height: 100vh;
        opacity: {opacity};
      }}

      .bp-bg__overlay {{
        position: absolute;
        inset: 0;
        background: rgba(0,0,0,{darken});
      }}

      [data-testid="stAppViewContainer"] > .main {{
        padding-left: 0 !important;
        padding-right: 0 !important;
        width: 100vw !important;
      }}

      html, body {{
        overflow-x: hidden !important;
      }}

      .block-container {{
        background: rgba(255,255,255,0.60);
        border-radius: 18px;
        margin: 5rem auto 0 auto !important;
        max-width: min(96vw, 1800px);
        width: 100%;
        padding: 1rem 1rem 1.25rem 1rem;
      }}
    </style>

    <div class="bp-bg">
        <img class="bp-bg__img" src="data:image/jpg;base64,{b64}">
        <div class="bp-bg__overlay"></div>
    </div>
    """, unsafe_allow_html=True)

inject_background_layer(banner64)

# --------------------------------------------------------------------
# SIDEBAR & TAB STYLING (TDM green palette)
# --------------------------------------------------------------------
st.markdown("""
<style>

/* Sidebar background */
[data-testid="stSidebar"] > div:first-child {
    background: linear-gradient(180deg, #163422 0%, #1f4a30 100%);
    border-right: 1px solid rgba(0,0,0,0.08);
    margin-top: -4rem !important;
    padding-top: 0 !important;
    padding-bottom: 2rem !important;
    min-height: 125vh;
}

/* Sidebar text */
[data-testid="stSidebar"] * {
    color: #ecf2ee !important;
}

/* Inputs */
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea,
[data-testid="stSidebar"] select {
    background: rgba(255,255,255,0.98) !important;
    color: #222 !important;
}

/* Tabs (top level) */
:root {
  --main-tab-bg: rgba(255,255,255,0.88);
  --main-tab-text: #163422;
  --main-tab-border: #9dc2a9;
  --main-tab-hover-bg: #e2efe6;
  --main-tab-active-bg: #1f4a30;
  --main-tab-active-text: #ffffff;
  --main-tab-radius: 14px;
  --main-tab-font: 18px;
  --main-tab-pad-y: .55rem;
  --main-tab-pad-x: 1.10rem;
}

.stTabs [data-baseweb="tab-list"]{
  margin-top: .5rem !important;
  border-bottom: none !important;
}

.stTabs [data-baseweb="tab"] {
  border: 2px solid var(--main-tab-border) !important;
  border-radius: var(--main-tab-radius) !important;
  background: var(--main-tab-bg) !important;
  color: var(--main-tab-text) !important;
  padding: var(--main-tab-pad-y) var(--main-tab-pad-x) !important;
  font-weight: 700 !important;
  font-size: var(--main-tab-font) !important;
  transition: background .15s ease-in-out, transform .15s ease-in-out;
}

.stTabs [data-baseweb="tab"]:hover {
  background: var(--main-tab-hover-bg) !important;
  transform: translateY(-1px);
}

.stTabs [aria-selected="true"][data-baseweb="tab"] {
  background: var(--main-tab-active-bg) !important;
  color: var(--main-tab-active-text) !important;
  border-color: var(--main-tab-active-bg) !important;
}

.stTabs [data-baseweb="tab-highlight"] { display:none !important; }

</style>
""", unsafe_allow_html=True)

# --------------------------------------------------------------------
# WFP Sidebar Readability Fix (TDM version)
# --------------------------------------------------------------------
st.markdown("""
<style>

/* Sidebar section headers */
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] h4 {
    color: #f1f5f2 !important;
}

/* Label styling */
[data-testid="stSidebar"] .sb-label {
    color: #e7efe8 !important;
    font-weight: 700 !important;
}

/* Buttons inside sidebar */
[data-testid="stSidebar"] .stButton > button {
    background-color: #e8f2ec !important;
    color: #163422 !important;
    border: 1px solid #9dc2a9 !important;
    font-weight: 600 !important;
}

[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #d7e8dd !important;
}

</style>
""", unsafe_allow_html=True)

# --------------------------------------------------------------------
# MAIN NAVIGATION
# --------------------------------------------------------------------
main_tabs = st.tabs(["Service", "Logistics"])
sidebar_root = st.sidebar.container()

# ====================================================================
# SERVICE TAB — radio-based subtabs
# ====================================================================
with main_tabs[0]:

    # Subtab styling
    st.markdown("""
    <style>
    .subtabs-radio .stRadio svg { display:none !important; }
    .subtabs-radio .stRadio input[type="radio"] { display:none !important; }
    .subtabs-radio .stRadio > div { gap: .55rem !important; }
    .subtabs-radio .stRadio [role="radiogroup"] { flex-wrap: wrap; }

    .subtabs-radio .stRadio label > div[role="radio"]{
      border: 2px solid #cfe0d4 !important;
      border-radius: 10px !important;
      background: rgba(255,255,255,0.86) !important;
      color: #1f4a30 !important;
      padding: .55rem .95rem !important;
      font-weight: 700 !important;
      font-size: 16px !important;
      transition: background .15s ease-in-out, transform .15s ease-in-out;
    }
    .subtabs-radio .stRadio label > div[role="radio"]:hover {
      background: #e7f1ea !important;
      transform: translateY(-1px);
    }
    .subtabs-radio .stRadio label > div[aria-checked="true"][role="radio"]{
      background: #9dc2a9 !important;
      color: #163422 !important;
      border-color: #9dc2a9 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="subtabs-radio">', unsafe_allow_html=True)
        SERVICE_PAGES = [
            "Scheduler",
            "Confirmations",
            "Waiting for Parts",
            "Phone Call Log",
            "Coordinate Confidence"
        ]
        service_page = st.radio(
            "Service Pages",
            SERVICE_PAGES,
            horizontal=True,
            index=0,
            key="service_page_picker",
            label_visibility="collapsed",
        )
        st.markdown('</div>', unsafe_allow_html=True)

    sidebar_root.empty()

    if service_page == "Scheduler":
        render_scheduler_tab(sidebar_root)
    elif service_page == "Confirmations":
        render_confirmations_tab()
    elif service_page == "Waiting for Parts":
        render_wfp_tab(sidebar_root)
    elif service_page == "Phone Call Log":
        render_phone_log_tab(sidebar_root)
    elif service_page == "Coordinate Confidence":
        render_coordinate_confidence_tab(sidebar_root)

# ====================================================================
# LOGISTICS TAB
# ====================================================================
with main_tabs[1]:
    sidebar_root.empty()

    st.header("📆 Logistics Calendar")
    st.info("Calendar functionality coming soon.")

# ====================================================================
# FOOTER — TDM Branding
# ====================================================================
st.markdown("""
<style>
.footer-note {
    margin-top: 4rem;
    text-align: center;
    padding: .5rem;
    color: #426b53;
    font-size: 13px;
    opacity: .85;
}
</style>
<div class="footer-note">TDM Ops Console © 2025 — Internal Service Desk Platform</div>
""", unsafe_allow_html=True)

