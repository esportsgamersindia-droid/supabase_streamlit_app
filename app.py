# app.py
import streamlit as st
import pandas as pd
import requests
import certifi
import math
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO

# -------------------------
# Config / Env
# -------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")  # e.g. https://xyz.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # anon key or service role (be careful)
TABLE = "disc_dills"

# -------------------------
# HTTP session with retries & certifi
# -------------------------
def create_requests_session(retries=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
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
# Fetch using Supabase REST (robust)
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
# Streamlit App
# -------------------------
st.set_page_config(page_title="TF EBBills", layout="wide")

st.title("📦 TF EB Bills Viewer")
st.write(f"Table: **{TABLE}**")

# Initialize session state
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
if "last_error" not in st.session_state:
    st.session_state.last_error = None

col_fetch, col_help = st.columns([1, 3])
with col_fetch:
    if st.button("Fetch Data From Supabase"):
        try:
            st.session_state.last_error = None
            with st.spinner("Fetching data... (this may retry on transient network errors)"):
                df_raw = fetch_table_data_via_rest(TABLE, timeout=30)
            df_norm = normalize_df(df_raw)
            if df_norm.empty:
                st.session_state.df = pd.DataFrame()
                st.session_state.data_loaded = False
                st.error("No data found in the table.")
            else:
                st.session_state.df = df_norm
                st.session_state.data_loaded = True
                st.success(f"Fetched {len(df_norm)} records.")
        except Exception as e:
            st.session_state.last_error = str(e)
            st.session_state.data_loaded = False
            st.session_state.df = pd.DataFrame()
            st.error("Error fetching data: " + str(e))

with col_help:
    st.markdown(
        """
        **Notes / Troubleshooting**
        - Ensure `.env` contains `SUPABASE_URL` (https://...supabase.co) and `SUPABASE_KEY` (anon or service role).
        - If you see SSL handshake errors, try switching to a different network (mobile hotspot or VPN).
        - This app uses `certifi` (CA bundle) and retries to improve reliability.
        """
    )

# If there was a last_error show details and suggestion
if st.session_state.last_error:
    st.caption("Last fetch error: " + st.session_state.last_error)

# Main UI after data loaded
if st.session_state.data_loaded:
    df = st.session_state.df.copy()

    # 1) Total Records
    st.success(f"✔ Total Records Downloaded: **{len(df)}**")

    # 2) Multi-select billMonth
    st.subheader("📅 Select Bill Month(s)")
    bill_months = sorted([m for m in df["billMonth"].unique() if m and str(m).strip() != "nan"])
    selected_months = st.multiselect("Choose billMonth(s):", bill_months, default=bill_months if bill_months else [])

    if not selected_months:
        st.warning("Please select at least one billMonth.")
        st.stop()

    df = df[df["billMonth"].isin(selected_months)]

    # 3) Multi-select ERO
    st.subheader("🏢 Select ERO(s)")
    eros = sorted([e for e in df["ero"].unique() if e and str(e).strip() != "nan"])
    selected_eros = st.multiselect("Choose ERO(s):", eros, default=eros if eros else [])

    if not selected_eros:
        st.warning("Please select at least one ERO.")
        st.stop()

    df = df[df["ero"].isin(selected_eros)]

    # 4) Search box
    st.subheader("🔍 Search by billNo or serviceNo")
    search_value = st.text_input("Enter billNo or serviceNo (partial matches allowed):").strip()
    if search_value != "":
        mask_bill = df["billNo"].astype(str).str.contains(search_value, na=False)
        mask_service = df["serviceNo"].astype(str).str.contains(search_value, na=False)
        df = df[mask_bill | mask_service]

    st.info(f"Filtered records: **{len(df)}**")

    # 5) Pagination
    st.subheader("📄 Paginated Table")
    rows_per_page = st.selectbox("Rows per page:", options=[10, 20, 50, 100], index=1)
    total_pages = math.ceil(len(df) / rows_per_page) if len(df) > 0 else 1
    page = st.number_input("Page", min_value=1, max_value=max(1, total_pages), value=1, step=1)
    start = (page - 1) * rows_per_page
    end = start + rows_per_page
    df_page = df.iloc[start:end]
    st.dataframe(df_page.reset_index(drop=True), use_container_width=True)

    # 6) Totals
    st.subheader("💰 Totals Across Selected Filters")
    total_billAmt = float(df["billAmt"].sum()) if "billAmt" in df.columns else 0.0
    total_totAmt = float(df["totAmt"].sum()) if "totAmt" in df.columns else 0.0
    col1, col2, col3 = st.columns([1, 1, 2])
    col1.metric("Total billAmt", f"{total_billAmt:,.2f}")
    col2.metric("Total totAmt", f"{total_totAmt:,.2f}")
    col3.write(f"Showing page **{page}** of **{total_pages}** (rows {start+1} to {min(end, len(df))})")

    # 7) ERO vs Total Amount (Bar Chart)
    st.subheader("📊 ERO vs Total Amount (totAmt)")
    if "ero" in df.columns and "totAmt" in df.columns:
        df_ero_totals = df.groupby("ero", as_index=False)["totAmt"].sum().sort_values("totAmt", ascending=False)
        if not df_ero_totals.empty:
            # Use st.bar_chart or st.altair_chart for better control. We'll use the simple st.bar_chart
            df_ero_plot = df_ero_totals.set_index("ero")
            st.bar_chart(df_ero_plot["totAmt"])
        else:
            st.info("No ERO totals to display.")
    else:
        st.info("Columns 'ero' or 'totAmt' missing; cannot render ERO chart.")

    # 8) Month-wise Trend Chart
    st.subheader("📈 Month-wise Trend (totAmt)")
    if "billMonth" in df.columns and "totAmt" in df.columns:
        df_monthly = df.groupby("billMonth", as_index=False)["totAmt"].sum()
        # Try to sort billMonth intelligently if it is in YYYY-MM format; otherwise default sort
        try:
            df_monthly["__sort"] = pd.to_datetime(df_monthly["billMonth"], errors="coerce")
            df_monthly = df_monthly.sort_values("__sort").drop(columns="__sort")
        except Exception:
            df_monthly = df_monthly.sort_values("billMonth")
        if not df_monthly.empty:
            df_month_plot = df_monthly.set_index("billMonth")
            st.line_chart(df_month_plot["totAmt"])
        else:
            st.info("No month-wise totals to display.")
    else:
        st.info("Columns 'billMonth' or 'totAmt' missing; cannot render month-wise chart.")

    # 9) Exports (CSV / Excel) - export filtered dataset
    st.subheader("⬇️ Export Filtered Data")
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, file_name=f"{TABLE}_filtered.csv", mime="text/csv")

    # Excel in memory
    # Excel in memory
    towrite = BytesIO()
    with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")

    towrite.seek(0)

    st.download_button(
        "Download Excel", 
        towrite, 
        file_name=f"{TABLE}_filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # towrite = BytesIO()
    # with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
    #     df.to_excel(writer, index=False, sheet_name="data")
    #     writer.save()
    # towrite.seek(0)
    # st.download_button("Download Excel", towrite, file_name=f"{TABLE}_filtered.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

else:
    st.info("Click **Fetch Data From Supabase** to begin.")
