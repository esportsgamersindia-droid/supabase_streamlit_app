# app.py
import streamlit as st
import pandas as pd
import requests
import certifi
import math
import os
import hashlib
import time
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO

# -------------------------
# Config / Env
# -------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE = "disc_dills"
LOGIN_TIMEOUT_SECONDS = 600  # 10 min inactivity timeout


# -------------------------
# Utility: Create HTTP session with retries
# -------------------------
def create_requests_session(retries=3, backoff_factor=0.5,
                            status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# -------------------------
# Secure password hashing
# -------------------------
def hash_text(text: str):
    return hashlib.sha256(text.encode()).hexdigest()


# -------------------------
# Fetch Users for Login
# -------------------------
def fetch_users():
    url = SUPABASE_URL.rstrip("/") + "/rest/v1/user_list"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json"
    }
    params = {"select": "*"}
    session = create_requests_session()

    try:
        resp = session.get(url, headers=headers, params=params, timeout=20, verify=certifi.where())
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception as e:
        st.error("Failed to load user list: " + str(e))
        return pd.DataFrame()


# -------------------------
# Logout Function
# -------------------------
def logout():
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.login_time = None


# -------------------------
# Auto Logout Check
# -------------------------
def check_timeout():
    if st.session_state.get("logged_in", False):
        now = time.time()
        if st.session_state.login_time and (now - st.session_state.login_time) > LOGIN_TIMEOUT_SECONDS:
            st.warning("⏳ Session timed out due to inactivity. Please login again.")
            logout()
            st.stop()


# -------------------------
# Fetch data from Supabase table
# -------------------------

def fetch_table_data_via_rest(table_name=TABLE, timeout=30):

    """Fetch all rows from Supabase REST endpoint using requests with certifi and retries."""

    if not SUPABASE_URL or not SUPABASE_KEY:

        raise ValueError("SUPABASE_URL or SUPABASE_KEY not set in environment (.env)")



    url = SUPABASE_URL.rstrip("/") + f"/rest/v1/{table_name}"

    headers = {

        "apikey": SUPABASE_KEY,

        "Authorization": f"Bearer {SUPABASE_KEY}",

        "Accept": "application/json"

    }

    params = {

        "select": "*"  # fetch all columns

    }



    session = create_requests_session(retries=4, backoff_factor=1.0)



    try:

        resp = session.get(url, headers=headers, params=params, timeout=timeout, verify=certifi.where())

        resp.raise_for_status()

        data = resp.json()

        df = pd.DataFrame(data)

        return df

    except requests.exceptions.SSLError as e:

        # SSL handshake errors — include actionable advice in raised exception

        raise RuntimeError(

            "SSL error while connecting to Supabase. Try switching network (use a hotspot / VPN), "

            "ensure SUPABASE_URL starts with https:// and install certifi. Original error: " + str(e)

        )

    except requests.exceptions.RequestException as e:

        raise RuntimeError("Network error when fetching Supabase data: " + str(e))





# -------------------------

# Utilities: normalize dataframe

# -------------------------

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:

        return pd.DataFrame()

    df = df.copy()



    # Ensure columns exist; coerce numeric columns

    for col in ["billAmt", "totAmt"]:

        if col in df.columns:

            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        else:

            df[col] = 0.0



    # Ensure relevant columns exist as strings

    for col in ["billNo", "serviceNo", "ero", "billMonth"]:

        if col in df.columns:

            # convert to string, but keep NaN as empty string

            df[col] = df[col].astype(str).fillna("")

        else:

            df[col] = ""



    return df



# -------------------------
# INIT SESSION STATES
# -------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "login_time" not in st.session_state:
    st.session_state.login_time = None

if "username" not in st.session_state:
    st.session_state.username = None

# Run timeout check every refresh
check_timeout()


# -------------------------
# LOGIN SCREEN
# -------------------------
if not st.session_state.logged_in:
    st.title("🔐 Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        df_users = fetch_users()

        if df_users.empty:
            st.error("User table empty or fetch failed.")
        else:
            df_users["username"] = df_users["username"].astype(str)
            df_users["password"] = df_users["password"].astype(str)


            match = df_users[
                (df_users["username"] == username) &
                (df_users["password"] == password)
            ]

            if not match.empty:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.login_time = time.time()
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password.")

    st.stop()


# -------------------------
# MAIN APP (Visible After Login)
# -------------------------
st.set_page_config(page_title="TF EBBills", layout="wide")

st.sidebar.success(f"👤 Logged in as: {st.session_state.username}")

if st.sidebar.button("Logout"):
    logout()
    st.retrun()


# --- Fetch UI ---
st.title("📦 TF EB Bills Viewer")
st.write(f"Table: **{TABLE}**")

if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
if "last_error" not in st.session_state:
    st.session_state.last_error = None

col_fetch, _ = st.columns([1, 3])
with col_fetch:
    if st.button("Fetch Data From Server"):
        try:
            st.session_state.last_error = None
            with st.spinner("Fetching data..."):
                df_raw = fetch_table_data_via_rest(TABLE, timeout=30)
            df_norm = normalize_df(df_raw)
            if df_norm.empty:
                st.session_state.data_loaded = False
                st.error("No data found.")
            else:
                st.session_state.df = df_norm
                st.session_state.data_loaded = True
                st.success(f"Fetched {len(df_norm)} records.")
        except Exception as e:
            st.error("Error fetching data: " + str(e))
            st.session_state.data_loaded = False


if st.session_state.data_loaded:
    df = st.session_state.df.copy()

    st.success(f"✔ Total Records Loaded: **{len(df)}**")

    # Filters
    st.subheader("📅 Bill Month")
    months = sorted([m for m in df["billMonth"].unique() if m and m != "nan"])
    selected_months = st.multiselect("Select bill month(s):", months, default=months)
    df = df[df["billMonth"].isin(selected_months)]

    st.subheader("🏢 ERO")
    eros = sorted(df["ero"].unique())
    selected_eros = st.multiselect("Select ERO(s):", eros, default=eros)
    df = df[df["ero"].isin(selected_eros)]

    st.subheader("🔍 Search")
    search = st.text_input("Search billNo or serviceNo:")
    if search:
        df = df[df["billNo"].str.contains(search) | df["serviceNo"].str.contains(search)]

    st.info(f"Filtered records: **{len(df)}**")

    # Pagination
    st.subheader("📄 Table")
    rows_per_page = st.selectbox("Rows per page:", [10, 20, 50, 100], index=1)
    total_pages = math.ceil(len(df) / rows_per_page)
    page = st.number_input("Page", 1, max(1, total_pages), 1)
    start = (page - 1) * rows_per_page
    st.dataframe(df.iloc[start:start+rows_per_page], use_container_width=True)

    # Totals
    st.subheader("💰 Totals")
    col1, col2, _ = st.columns(3)
    col1.metric("Total billAmt", f"{df['billAmt'].sum():,.2f}")
    col2.metric("Total totAmt", f"{df['totAmt'].sum():,.2f}")

    # Charts
    st.subheader("📊 ERO wise Total")
    ero_totals = df.groupby("ero")["totAmt"].sum()
    st.bar_chart(ero_totals)

    st.subheader("📈 Month-wise Trend")
    month_totals = df.groupby("billMonth")["totAmt"].sum()
    st.line_chart(month_totals)

    # Export
    st.subheader("⬇️ Export Data")
    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode(),
        file_name=f"{TABLE}_filtered.csv",
        mime="text/csv"
    )

    towrite = BytesIO()
    with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
    towrite.seek(0)

    st.download_button(
        "Download Excel",
        data=towrite,
        file_name=f"{TABLE}_filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Click **Fetch Data From Server** to begin.")
