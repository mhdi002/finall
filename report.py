import streamlit as st
import pandas as pd
import io
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import numpy as np
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import base64
from sqlalchemy import create_engine, inspect

st.set_page_config(layout="wide", page_title="Complete Deals Reporting Dashboard", page_icon="📊")

engine = create_engine("sqlite:///C:\\Users\\mahdi\\Downloads\\report_results.db")

def update_table(df_new: pd.DataFrame, table_name: str, key_cols: list[str]) -> None:
    """
    Append only new rows in df_new to table_name, comparing on key_cols.
    
    - df_new: the DataFrame you want to save
    - table_name: the target SQL table
    - key_cols: list of columns that uniquely identify a row (e.g. ['DealID'] or ['Login','Date'])
    """
    inspector = inspect(engine)
    
    if table_name in inspector.get_table_names():
        # Read existing table
        df_existing = pd.read_sql_table(table_name, engine)
        # Identify new rows by doing an anti-join on the key columns
        merged = df_new.merge(df_existing[key_cols], on=key_cols, how='left', indicator=True)
        df_to_append = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        # Table doesn't exist yet → everything is new
        df_to_append = df_new.copy()

    if not df_to_append.empty:
        df_to_append.to_sql(table_name, engine, if_exists="append", index=False)
        st.write(f"✅ Appended {len(df_to_append):,} new rows to `{table_name}`")
    else:
        st.write(f"ℹ️ No new rows to append to `{table_name}`")

# ─── Helpers ────────────────────────────────────────────────────────────────

def round4(x):
    try: return round(float(x), 4)
    except: return 0.0

def parse_custom_datetime(s: str):
    """Parse datetime in format: dd.mm.yyyy hh:mm:ss"""
    try: 
        return pd.to_datetime(s, format="%d.%m.%Y %H:%M:%S", utc=True)
    except: 
        return pd.NaT

def sanitize_numeric_series(sr: pd.Series) -> pd.Series:
    return (
        sr.astype(str)
          .str.replace(r"[^\d\.\-]", "", regex=True)
          .replace(r"^\s*$","0", regex=True)
          .astype(float)
          .fillna(0.0)
    )

def filter_by_date_range(df: pd.DataFrame, start_date, end_date, datetime_col="Date & Time (UTC)"):
    """Filter dataframe by date range"""
    if df.empty or datetime_col not in df.columns:
        return df
    
    # Convert dates to datetime if they're strings
    if start_date and end_date:
        mask = pd.Series([True] * len(df))
        
        for idx, dt_str in enumerate(df[datetime_col]):
            if pd.isna(dt_str):
                continue
            dt = parse_custom_datetime(str(dt_str))
            if pd.isna(dt):
                continue
            
            start_dt = parse_custom_datetime(start_date) if isinstance(start_date, str) else start_date
            end_dt = parse_custom_datetime(end_date) if isinstance(end_date, str) else end_date
            
            if not (start_dt <= dt <= end_dt):
                mask.iloc[idx] = False
        
        return df[mask].copy()
    return df

# ─── Core Processing ─────────────────────────────────────────────────────────

def process_and_split(df: pd.DataFrame) -> dict[str,pd.DataFrame]:
    """1) Convert any form of USC to USD by dividing by 100, then split by Processing rule."""
    d = df.copy()
    # USC → USD conversion (case-insensitive)
    for col in d.select_dtypes(include="object"):
        d[col] = d[col].astype(str).str.replace(
            r"(?i)(\d[\d\.\-]*)\s*usc",
            lambda m: f"{round4(float(m.group(1)) / 100):.4f} USD",
            regex=True
        )
    
    if "Processing rule" not in d:
        st.error("Missing 'Processing rule' column!")
        st.stop()

    books = {"A Book": [], "B Book": [], "Multi Book": []}
    for _, row in d.iterrows():
        rule = str(row["Processing rule"]).strip()
        bucket = (
            "A Book" if rule == "Pipwise"
            else "B Book" if rule == "Retail B-book"
            else "Multi Book"
        )
        books[bucket].append(row)
    return {name: pd.DataFrame(rows, columns=d.columns) for name, rows in books.items()}

def enrich_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Add Profit Value/Unit, Date, Time; drop duplicate deals."""
    if df.empty:
        return df
    output, seen = [], set()
    for _, row in df.iterrows():
        deal = str(row.iloc[0]).strip()
        if deal in seen:
            continue
        seen.add(deal)
        raw = str(row.iloc[6] if len(row) > 6 else "")
        val = round4("".join(ch for ch in raw if ch.isdigit() or ch in ".-"))
        unit = "".join(ch for ch in raw if not (ch.isdigit() or ch in ".-")).strip().upper()
        dt_raw = str(row.iloc[7] if len(row) > 7 else "").strip()
        dt = parse_custom_datetime(dt_raw)
        date_str = dt.strftime("%Y-%m-%d") if not pd.isna(dt) else ""
        time_str = dt.strftime("%H:%M:%S") if not pd.isna(dt) else ""
        output.append(list(row) + [val, unit, date_str, time_str])
    headers = list(df.columns) + ["Profit Value", "Profit Unit", "Date", "Time"]
    return pd.DataFrame(output, columns=headers)

def aggregate_book(df: pd.DataFrame, excluded: set[str], book_type: str) -> pd.DataFrame:
    """
    Aggregate book data with proper excluded account handling based on book type.
    
    For A Book and Multi Book: Set commission, TP profit, and broker profit to 0 for excluded accounts
    For B Book: Skip excluded accounts entirely
    """
    if df.empty:
        return pd.DataFrame()
    
    required = [
        "Login", "Notional volume in USD", "Trader profit",
        "Swaps", "Commission", "TP broker profit", "Total broker profit"
    ]
    for col in required:
        if col not in df:
            st.error(f"Missing column {col}!")
            st.stop()
        if col != "Login":
            df[col] = sanitize_numeric_series(df[col])

    rows = []
    for login, group in df.groupby("Login", dropna=False):
        if pd.isna(login):
            continue
        
        login_str = str(login).strip()
        is_excluded = login_str in excluded
        
        # For B Book: Skip excluded accounts entirely (like in Apps Script)
        if book_type == "B Book" and is_excluded:
            continue
        
        # For A Book and Multi Book: Zero out specific fields for excluded accounts
        if is_excluded and book_type in ["A Book", "Multi Book"]:
            comm = 0
            tp = 0
            bk = 0
        else:
            comm = group["Commission"].sum()
            tp = group["TP broker profit"].sum()
            bk = group["Total broker profit"].sum()

        rec = {
            "Login": login_str,
            "Total Volume": group["Notional volume in USD"].sum(),
            "Trader Profit": group["Trader profit"].sum(),
            "Swaps": group["Swaps"].sum(),
            "Commission": comm,
            "TP Profit": tp,
            "Broker Profit": bk
        }
        rec["Net"] = rec["Trader Profit"] + rec["Swaps"] - rec["Commission"]
        rows.append(rec)

    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        summary = {c: round4(df_out[c].sum()) for c in df_out.columns if c != "Login"}
        summary["Login"] = "Summary"
        return pd.concat([df_out, pd.DataFrame([summary])], ignore_index=True)
    return df_out

def generate_chinese_clients(enriched_books: dict, excluded: set) -> pd.DataFrame:
    """Generate Chinese clients analysis with proper excluded account handling"""
    chinese_prefixes = ['real\\Chines', 'BBOOK\\Chines']
    chinese_summary = {}
    
    for book_name, df in enriched_books.items():
        if df.empty:
            continue
        
        required_cols = ["Login", "Group", "Notional volume in USD", "Trader profit", 
                        "Swaps", "Commission", "TP broker profit", "Total broker profit"]
        
        if not all(col in df.columns for col in required_cols):
            continue
            
        for _, row in df.iterrows():
            login = str(row["Login"]).strip()
            group = str(row["Group"]).strip()
            
            if not login:
                continue
                
            # Skip excluded accounts entirely for Chinese clients analysis
            if login in excluded:
                continue
                
            # Check if client is Chinese
            if not any(group.startswith(prefix) for prefix in chinese_prefixes):
                continue
                
            if login not in chinese_summary:
                chinese_summary[login] = {
                    "Total Volume": 0,
                    "Trader Profit": 0,
                    "Swaps": 0,
                    "Commission": 0,
                    "TP Profit": 0,
                    "Broker Profit": 0
                }
            
            chinese_summary[login]["Total Volume"] += float(row["Notional volume in USD"] or 0)
            chinese_summary[login]["Trader Profit"] += float(row["Trader profit"] or 0)
            chinese_summary[login]["Swaps"] += float(row["Swaps"] or 0)
            chinese_summary[login]["Commission"] += float(row["Commission"] or 0)
            chinese_summary[login]["TP Profit"] += float(row["TP broker profit"] or 0)
            chinese_summary[login]["Broker Profit"] += float(row["Total broker profit"] or 0)
    
    if not chinese_summary:
        return pd.DataFrame(columns=["Login", "Total Volume", "Trader Profit", "Swaps", 
                                   "Commission", "TP Profit", "Broker Profit", "Net"])
    
    # Convert to DataFrame
    rows = []
    for login, data in chinese_summary.items():
        net = data["Trader Profit"] + data["Swaps"] - data["Commission"]
        rows.append({
            "Login": login,
            "Total Volume": round4(data["Total Volume"]),
            "Trader Profit": round4(data["Trader Profit"]),
            "Swaps": round4(data["Swaps"]),
            "Commission": round4(data["Commission"]),
            "TP Profit": round4(data["TP Profit"]),
            "Broker Profit": round4(data["Broker Profit"]),
            "Net": round4(net)
        })
    
    df_chinese = pd.DataFrame(rows)
    
    # Add summary row
    if not df_chinese.empty:
        summary = {
            "Login": "Summary",
            "Total Volume": round4(df_chinese["Total Volume"].sum()),
            "Trader Profit": round4(df_chinese["Trader Profit"].sum()),
            "Swaps": round4(df_chinese["Swaps"].sum()),
            "Commission": round4(df_chinese["Commission"].sum()),
            "TP Profit": round4(df_chinese["TP Profit"].sum()),
            "Broker Profit": round4(df_chinese["Broker Profit"].sum()),
            "Net": round4(df_chinese["Net"].sum())
        }
        df_chinese = pd.concat([df_chinese, pd.DataFrame([summary])], ignore_index=True)
    
    return df_chinese

def generate_client_summary(results: dict) -> pd.DataFrame:
    """Generate consolidated client summary across all books"""
    all_clients = {}
    
    for book_name, df in results.items():
        if df.empty:
            continue
            
        # Exclude summary row
        client_data = df[df["Login"] != "Summary"].copy()
        
        for _, row in client_data.iterrows():
            login = row["Login"]
            if login not in all_clients:
                all_clients[login] = {
                    "Total Volume": 0,
                    "Trader Profit": 0,
                    "Swaps": 0,
                    "Commission": 0,
                    "TP Profit": 0,
                    "Broker Profit": 0,
                    "Net": 0
                }
            
            all_clients[login]["Total Volume"] += float(row["Total Volume"] or 0)
            all_clients[login]["Trader Profit"] += float(row["Trader Profit"] or 0)
            all_clients[login]["Swaps"] += float(row["Swaps"] or 0)
            all_clients[login]["Commission"] += float(row["Commission"] or 0)
            all_clients[login]["TP Profit"] += float(row["TP Profit"] or 0)
            all_clients[login]["Broker Profit"] += float(row["Broker Profit"] or 0)
            all_clients[login]["Net"] += float(row["Net"] or 0)
    
    if not all_clients:
        return pd.DataFrame()
    
    # Convert to DataFrame
    rows = []
    for login, data in all_clients.items():
        rows.append({
            "Login": login,
            "Total Volume": round4(data["Total Volume"]),
            "Trader Profit": round4(data["Trader Profit"]),
            "Swaps": round4(data["Swaps"]),
            "Commission": round4(data["Commission"]),
            "TP Profit": round4(data["TP Profit"]),
            "Broker Profit": round4(data["Broker Profit"]),
            "Net": round4(data["Net"])
        })
    
    df_summary = pd.DataFrame(rows)
    
    # Add summary row
    if not df_summary.empty:
        summary = {
            "Login": "Summary",
            "Total Volume": round4(df_summary["Total Volume"].sum()),
            "Trader Profit": round4(df_summary["Trader Profit"].sum()),
            "Swaps": round4(df_summary["Swaps"].sum()),
            "Commission": round4(df_summary["Commission"].sum()),
            "TP Profit": round4(df_summary["TP Profit"].sum()),
            "Broker Profit": round4(df_summary["Broker Profit"].sum()),
            "Net": round4(df_summary["Net"].sum())
        }
        df_summary = pd.concat([df_summary, pd.DataFrame([summary])], ignore_index=True)
    
    return df_summary

def calculate_vip_volume(enriched_books: dict, vip_clients: set, excluded: set) -> float:
    """Calculate total volume for VIP clients, excluding excluded accounts"""
    total_vip_volume = 0
    
    for book_name, df in enriched_books.items():
        if df.empty or "Login" not in df.columns or "Notional volume in USD" not in df.columns:
            continue
            
        for _, row in df.iterrows():
            login = str(row["Login"]).strip()
            
            # Skip excluded accounts
            if login in excluded:
                continue
                
            if login in vip_clients:
                total_vip_volume += float(row["Notional volume in USD"] or 0)
    
    return total_vip_volume

def generate_final_calculations(results: dict, chinese_df: pd.DataFrame, vip_volume: float, 
                              date_range: str = "") -> pd.DataFrame:
    """Generate comprehensive final calculations matching Apps Script logic"""
    
    # Helper function to get sum from results
    def get_sum(book_name, column):
        if book_name not in results or results[book_name].empty:
            return 0
        df = results[book_name]
        summary_row = df[df["Login"] == "Summary"]
        if summary_row.empty:
            return 0
        return float(summary_row[column].iloc[0] or 0)
    
    # A Book calculations - matches Apps Script logic
    a_book_commission = get_sum("A Book", "Commission")
    a_book_tp = get_sum("A Book", "TP Profit")
    multi_commission = get_sum("Multi Book", "Commission")
    multi_tp = get_sum("Multi Book", "TP Profit")
    a_book_total = a_book_commission + a_book_tp + multi_commission + multi_tp
    
    # B Book calculations - matches Apps Script logic
    b_book_tsm = get_sum("B Book", "Net") * -1  # (Trader + Swaps - Commission) * -1
    multi_total_broker = get_sum("Multi Book", "Broker Profit")
    multi_tp_broker = get_sum("Multi Book", "TP Profit")
    b_book_extra = multi_total_broker - multi_tp_broker
    b_book_total = b_book_tsm + b_book_extra
    
    # Volume calculations
    a_book_volume = get_sum("A Book", "Total Volume")
    b_book_volume = get_sum("B Book", "Total Volume")
    multi_volume = get_sum("Multi Book", "Total Volume")
    
    # Swap calculations
    a_book_swaps = get_sum("A Book", "Swaps")
    multi_swaps = get_sum("Multi Book", "Swaps")
    total_swaps = a_book_swaps + multi_swaps
    
    # Lot calculations (volume / 200,000)
    a_book_lot = (a_book_volume + multi_volume) / 200000
    b_book_lot = b_book_volume / 200000
    
    # Chinese volume from Chinese clients analysis
    chinese_volume = 0
    if not chinese_df.empty:
        summary_row = chinese_df[chinese_df["Login"] == "Summary"]
        if not summary_row.empty:
            chinese_volume = float(summary_row["Total Volume"].iloc[0] or 0)
    
    chinese_lot = chinese_volume / 200000
    vip_lot = vip_volume / 200000
    retail_lot = a_book_lot + b_book_lot - chinese_lot - vip_lot
    total_lot = a_book_lot + b_book_lot
    
    # Create calculations table
    calculations = []
    
    if date_range:
        calculations.append(["DATE RANGE", "", date_range])
        calculations.append(["", "", ""])
    
    calculations.extend([
        ["A BOOK SUMMARY", "", ""],
        ["Source", "Description", "Value"],
        ["A Book Result", "Sum of TP Broker Profit + Commission", round4(a_book_tp + a_book_commission)],
        ["Multi Book Result", "Sum of TP Broker Profit + Commission", round4(multi_tp + multi_commission)],
        ["Total A Book", "Sum of above two values", round4(a_book_total)],
        ["", "", ""],
        ["B BOOK SUMMARY", "", ""],
        ["Source", "Description", "Value"],
        ["B Book Result", "(-1) * Sum of (Trader + Swaps - Commission)", round4(b_book_tsm)],
        ["Multi Book Result", "Total Broker Profit - TP Broker Profit", round4(b_book_extra)],
        ["Total B Book", "Sum of above two values", round4(b_book_total)],
        ["", "", ""],
        ["EXTRA SUMMARY DATA", "", ""],
        ["A Book", "Client's Spread (TP Broker Profit)", round4(a_book_tp + multi_tp)],
        ["A Book", "Client's Commission", round4(a_book_commission + multi_commission)],
        ["Total Swap", "Sum of all Swaps", round4(total_swaps)],
        ["A Book", "Volume (Lot)", round4(a_book_lot)],
        ["B Book", "Volume (Lot)", round4(b_book_lot)],
        ["Chinese Clients", "Volume (Lot)", round4(chinese_lot)],
        ["VIP Clients", "Volume (Lot)", round4(vip_lot)],
        ["Retail Clients", "Volume (Lot)", round4(retail_lot)],
        ["Total Volume", "A Book + B Book", round4(total_lot)]
    ])
    
    return pd.DataFrame(calculations, columns=["Source", "Description", "Value"])

def create_pdf_report(all_data: dict, date_range: str = "") -> bytes:
    """Create comprehensive PDF report"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
    story = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=30,
        alignment=1,  # Center
        textColor=colors.darkblue
    )
    
    title = f"Deals Reporting Dashboard{' - ' + date_range if date_range else ''}"
    story.append(Paragraph(title, title_style))
    story.append(Spacer(1, 20))
    
    # Helper function to create tables
    def create_table(df, title):
        if df.empty:
            return []
        
        elements = []
        
        # Section title
        section_style = ParagraphStyle(
            'SectionTitle',
            parent=styles['Heading2'],
            fontSize=12,
            spaceAfter=10,
            textColor=colors.darkgreen
        )
        elements.append(Paragraph(title, section_style))
        
        # Prepare data
        data = [list(df.columns)]
        for _, row in df.iterrows():
            data.append([str(val) for val in row])
        
        # Create table
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 20))
        return elements
    
    # Add all sections
    for title, df in all_data.items():
        if not df.empty:
            story.extend(create_table(df, title))
            if title in ["A Book Result", "B Book Result"]:  # Add page break after major sections
                story.append(PageBreak())
    
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

# ─── Streamlit UI ───────────────────────────────────────────────────────────

st.title("📊 Complete Deals Reporting & Dashboard")
st.markdown("*Advanced financial reporting with comprehensive business intelligence*")

# Sidebar
st.sidebar.header("🔧 Configuration")

# File uploads
st.sidebar.subheader("📁 Upload Files")
deals_csv = st.sidebar.file_uploader("Deals CSV", type="csv", key="deals")
ex_csv = st.sidebar.file_uploader("Excluded Accounts CSV", type="csv", key="excluded")
vip_csv = st.sidebar.file_uploader("VIP Client List CSV", type="csv", key="vip")

# Date range
st.sidebar.subheader("📅 Date Range Filter")
use_date_filter = st.sidebar.checkbox("Enable Date Filtering")
start_dt = st.sidebar.text_input("Start (dd.mm.yyyy hh:mm:ss)", 
                                  placeholder="26.05.2025 00:00:00") if use_date_filter else ""
end_dt = st.sidebar.text_input("End (dd.mm.yyyy hh:mm:ss)", 
                               placeholder="26.05.2025 23:59:59") if use_date_filter else ""

# Report options
st.sidebar.subheader("📋 Report Options")
show_charts = st.sidebar.checkbox("Show Charts", value=True)
show_detailed_tables = st.sidebar.checkbox("Show Detailed Tables", value=True)
generate_pdf = st.sidebar.checkbox("Generate PDF Report")

if not deals_csv:
    st.warning("📤 Please upload your Deals CSV file to continue.")
    st.info("💡 **Instructions:**\n1. Upload your deals CSV file\n2. Optionally upload excluded accounts and VIP client lists\n3. Set date range if needed\n4. View comprehensive analysis and download reports")
    st.stop()

# ─── Main Processing ─────────────────────────────────────────────────────────

try:
    # 1) Read & Process
    with st.spinner("🔄 Processing deals data..."):
        raw = pd.read_csv(deals_csv)
        books = process_and_split(raw)
        enriched = {k: enrich_and_dedupe(v) for k, v in books.items()}
        
        # Apply date filtering if enabled
        if use_date_filter and start_dt and end_dt:
            date_range_str = f"From {start_dt} to {end_dt}"
            for k in enriched:
                enriched[k] = filter_by_date_range(enriched[k], start_dt, end_dt)
        else:
            date_range_str = ""

    # 2) Load additional data
    excluded = set()
    if ex_csv:
        excluded_df = pd.read_csv(ex_csv, header=None, names=["Login"])
        excluded = set(excluded_df["Login"].str.strip())

    vip = set()
    if vip_csv:
        vip_df = pd.read_csv(vip_csv, header=None, names=["Login"])
        vip = set(vip_df["Login"].str.strip())

    # 3) Generate all analyses with proper excluded account handling
    with st.spinner("📊 Generating comprehensive analysis..."):
        # Book aggregations with book-specific exclusion logic
        results = {}
        for book_name, book_data in enriched.items():
            results[book_name] = aggregate_book(book_data, excluded, book_name)
        
        # Chinese clients analysis
        chinese_clients = generate_chinese_clients(enriched, excluded)
        
        # Client summary
        client_summary = generate_client_summary(results)
        
        # VIP volume calculation (excluding excluded accounts)
        vip_volume = calculate_vip_volume(enriched, vip, excluded)
        
        # Final calculations
        final_calculations = generate_final_calculations(results, chinese_clients, vip_volume, date_range_str)
        
        # Save to database
        for name, df in results.items():
            if not df.empty:
                update_table(df, f"{name.replace(' ', '_')}_Results", ["Login"])
        
        if not chinese_clients.empty:
            update_table(chinese_clients, "Chinese_Clients", ["Login"])
        
        if not client_summary.empty:
            update_table(client_summary, "Client_Summary", ["Login"])
        
        if not final_calculations.empty:
            update_table(final_calculations, "Final_Calculations", ["Source"])

        st.success("✅ All results saved to SQLite database (report_results.db)")
    

    # ─── Dashboard Metrics ─────────────────────────────────────────────────────

    st.header("📈 Executive Dashboard")
    
    # Calculate key metrics
    total_volume = sum(round4(df["Total Volume"].iloc[:-1].sum()) for df in results.values() if not df.empty)
    total_broker_profit = sum(round4(df["Broker Profit"].iloc[:-1].sum()) for df in results.values() if not df.empty)
    total_net = sum(round4(df["Net"].iloc[:-1].sum()) for df in results.values() if not df.empty)
    
    # Get specific metrics from final calculations
    final_calc_data = final_calculations.set_index('Source')
    try:
        a_book_total = float(final_calc_data.loc["Total A Book", "Value"])
        b_book_total = float(final_calc_data.loc["Total B Book", "Value"])
        total_lots = float(final_calc_data.loc["Total Volume", "Value"])
    except:
        a_book_total = b_book_total = total_lots = 0

    # Main metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("💰 Total Volume (USD)", f"${total_volume:,.0f}")
    with col2:
        st.metric("📊 A Book Total", f"${a_book_total:,.0f}")
    with col3:
        st.metric("📉 B Book Total", f"${b_book_total:,.0f}")
    with col4:
        st.metric("🎯 Total Lots", f"{total_lots:,.2f}")

    # Secondary metrics
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        chinese_volume = chinese_clients["Total Volume"].iloc[-1] if not chinese_clients.empty else 0
        st.metric("🏮 Chinese Volume", f"${chinese_volume:,.0f}")
    with col6:
        st.metric("⭐ VIP Volume", f"${vip_volume:,.0f}")
    with col7:
        retail_lots = final_calc_data.loc["Retail Clients", "Value"] if "Retail Clients" in final_calc_data.index else 0
        st.metric("🏪 Retail Lots", f"{float(retail_lots):,.2f}")
    with col8:
        total_swaps = final_calc_data.loc["Total Swap", "Value"] if "Total Swap" in final_calc_data.index else 0
        st.metric("🔄 Total Swaps", f"${float(total_swaps):,.0f}")

    if date_range_str:
        st.info(f"📅 **Filtered Data:** {date_range_str}")

    # ─── Charts Section ─────────────────────────────────────────────────────────

    if show_charts:
        st.markdown("---")
        st.header("📊 Visual Analytics")
        
        # Volume and profit data for charts
        volumes = {k: round4(df["Total Volume"].iloc[:-1].sum()) for k, df in results.items() if not df.empty}
        profits = {k: round4(df["Broker Profit"].iloc[:-1].sum()) for k, df in results.items() if not df.empty}
        
        # Row 1: Volume and Profit charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            if volumes:
                fig_vol = px.bar(
                    x=list(volumes.keys()), 
                    y=list(volumes.values()),
                    title="📦 Volume by Book",
                    labels={'y': 'Volume (USD)', 'x': 'Book Type'},
                    color=list(volumes.keys()),
                    color_discrete_sequence=['#4e79a7', '#f28e2b', '#e15759']
                )
                fig_vol.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_vol, use_container_width=True)
        
        with chart_col2:
            if profits:
                fig_profit = px.bar(
                    x=list(profits.keys()), 
                    y=list(profits.values()),
                    title="💰 Broker Profit by Book",
                    labels={'y': 'Profit (USD)', 'x': 'Book Type'},
                    color=list(profits.keys()),
                    color_discrete_sequence=['#76b7b2', '#59a14f', '#edc949']
                )
                fig_profit.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_profit, use_container_width=True)
        
        # Row 2: Distribution and trend charts
        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            if volumes:
                fig_pie = px.pie(
                    values=list(volumes.values()),
                    names=list(volumes.keys()),
                    title="📈 Volume Distribution",
                    color_discrete_sequence=['#4e79a7', '#f28e2b', '#e15759']
                )
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with chart_col4:
            # Client type analysis chart
            client_types = ['A Book', 'B Book', 'Chinese', 'VIP', 'Retail']
            client_volumes = [
                volumes.get('A Book', 0),
                volumes.get('B Book', 0),
                chinese_volume,
                vip_volume,
                float(retail_lots) * 200000 if retail_lots else 0
            ]
            
            fig_clients = px.bar(
                x=client_types,
                y=client_volumes,
                title="👥 Client Type Analysis",
                labels={'y': 'Volume (USD)', 'x': 'Client Type'},
                color=client_types,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_clients.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_clients, use_container_width=True)

    # ─── Detailed Tables Section ─────────────────────────────────────────────────

    if show_detailed_tables:
        st.markdown("---")
        st.header("📋 Detailed Analysis")
        
        # Create tabs for different analyses
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📚 Book Results", 
            "👥 Client Summary", 
            "🏮 Chinese Clients", 
            "⭐ VIP Analysis",
            "🧮 Final Calculations", 
            "📊 Raw Data"
        ])
        
        with tab1:
            st.subheader("📚 Book Results Analysis")
            
            book_tabs = st.tabs(["A Book", "B Book", "Multi Book"])
            
            with book_tabs[0]:
                if not results["A Book"].empty:
                    st.dataframe(results["A Book"], use_container_width=True)
                    st.caption(f"Total records: {len(results['A Book'])-1}")
                else:
                    st.info("No A Book data available")
            
            with book_tabs[1]:
                if not results["B Book"].empty:
                    st.dataframe(results["B Book"], use_container_width=True)
                    st.caption(f"Total records: {len(results['B Book'])-1}")
                else:
                    st.info("No B Book data available")
            
            with book_tabs[2]:
                if not results["Multi Book"].empty:
                    st.dataframe(results["Multi Book"], use_container_width=True)
                    st.caption(f"Total records: {len(results['Multi Book'])-1}")
                else:
                    st.info("No Multi Book data available")
        
        with tab2:
            st.subheader("👥 Consolidated Client Summary")
            if not client_summary.empty:
                st.dataframe(client_summary, use_container_width=True)
                st.caption(f"Total unique clients: {len(client_summary)-1}")
                
                # Client summary metrics
                if len(client_summary) > 1:
                    summary_row = client_summary.iloc[-1]
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Clients", len(client_summary)-1)
                    with col2:
                        st.metric("Total Volume", f"${summary_row['Total Volume']:,.0f}")
                    with col3:
                        st.metric("Total Commission", f"${summary_row['Commission']:,.0f}")
                    with col4:
                        st.metric("Net Result", f"${summary_row['Net']:,.0f}")
            else:
                st.info("No client summary data available")
        
        with tab3:
            st.subheader("🏮 Chinese Clients Analysis")
            if not chinese_clients.empty:
                st.dataframe(chinese_clients, use_container_width=True)
                st.caption(f"Total Chinese clients: {len(chinese_clients)-1}")
                
                # Chinese clients insights
                if len(chinese_clients) > 1:
                    chinese_summary_row = chinese_clients.iloc[-1]
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Chinese Clients", len(chinese_clients)-1)
                    with col2:
                        chinese_vol = chinese_summary_row['Total Volume']
                        chinese_pct = (chinese_vol / total_volume * 100) if total_volume > 0 else 0
                        st.metric("Chinese Volume %", f"{chinese_pct:.1f}%")
                    with col3:
                        st.metric("Chinese Net", f"${chinese_summary_row['Net']:,.0f}")
            else:
                st.info("No Chinese clients found")
        
        with tab4:
            st.subheader("⭐ VIP Client Analysis")
            
            if vip:
                # VIP client breakdown
                vip_breakdown = []
                for book_name, df in enriched.items():
                    if df.empty:
                        continue
                    for _, row in df.iterrows():
                        login = str(row.get("Login", "")).strip()
                        if login in vip:
                            vip_breakdown.append({
                                "Book": book_name,
                                "Login": login,
                                "Volume": float(row.get("Notional volume in USD", 0)),
                                "Trader Profit": float(row.get("Trader profit", 0)),
                                "Commission": float(row.get("Commission", 0))
                            })
                
                if vip_breakdown:
                    vip_df = pd.DataFrame(vip_breakdown)
                    # Aggregate by login
                    vip_agg = vip_df.groupby("Login").agg({
                        "Volume": "sum",
                        "Trader Profit": "sum", 
                        "Commission": "sum"
                    }).round(4)
                    vip_agg["Net"] = vip_agg["Trader Profit"] - vip_agg["Commission"]
                    
                    st.dataframe(vip_agg, use_container_width=True)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("VIP Clients", len(vip_agg))
                    with col2:
                        vip_pct = (vip_volume / total_volume * 100) if total_volume > 0 else 0
                        st.metric("VIP Volume %", f"{vip_pct:.1f}%")
                    with col3:
                        st.metric("VIP Net", f"${vip_agg['Net'].sum():,.0f}")
                else:
                    st.info("No VIP client activity found in the current dataset")
            else:
                st.info("No VIP client list uploaded")
        
        with tab5:
            st.subheader("🧮 Final Calculations & Business Metrics")
            if not final_calculations.empty:
                st.dataframe(final_calculations, use_container_width=True)
                
                # Key business insights
                st.markdown("### 💡 Key Business Insights")
                
                try:
                    final_data = final_calculations.set_index('Source')
                    
                    insight_col1, insight_col2 = st.columns(2)
                    
                    with insight_col1:
                        st.markdown("**📊 Profitability Analysis:**")
                        a_total = float(final_data.loc["Total A Book", "Value"])
                        b_total = float(final_data.loc["Total B Book", "Value"]) 
                        total_profit = a_total + b_total
                        
                        st.write(f"• A Book Contribution: ${a_total:,.0f}")
                        st.write(f"• B Book Contribution: ${b_total:,.0f}")
                        st.write(f"• **Total Profit: ${total_profit:,.0f}**")
                        
                        if total_profit != 0:
                            a_pct = (a_total / total_profit * 100)
                            b_pct = (b_total / total_profit * 100)
                            st.write(f"• A Book: {a_pct:.1f}% | B Book: {b_pct:.1f}%")
                    
                    with insight_col2:
                        st.markdown("**👥 Client Distribution:**")
                        total_lots = float(final_data.loc["Total Volume", "Value"])
                        chinese_lots = float(final_data.loc["Chinese Clients", "Value"])
                        vip_lots = float(final_data.loc["VIP Clients", "Value"])
                        retail_lots = float(final_data.loc["Retail Clients", "Value"])
                        
                        if total_lots > 0:
                            st.write(f"• Chinese: {(chinese_lots/total_lots*100):.1f}%")
                            st.write(f"• VIP: {(vip_lots/total_lots*100):.1f}%")
                            st.write(f"• Retail: {(retail_lots/total_lots*100):.1f}%")
                            st.write(f"• **Total: {total_lots:,.2f} lots**")
                        
                except Exception as e:
                    st.warning("Could not generate business insights from calculations")
            else:
                st.info("No final calculations available")
        
        with tab6:
            st.subheader("📊 Raw Data Preview")
            
            raw_tabs = st.tabs(["Original Data", "A Book Raw", "B Book Raw", "Multi Book Raw"])
            
            with raw_tabs[0]:
                st.write("**Original uploaded data:**")
                st.dataframe(raw.head(100), use_container_width=True)
                st.caption(f"Showing first 100 of {len(raw)} total records")
            
            with raw_tabs[1]:
                if not enriched["A Book"].empty:
                    st.dataframe(enriched["A Book"].head(50), use_container_width=True)
                    st.caption(f"Showing first 50 of {len(enriched['A Book'])} A Book records")
                else:
                    st.info("No A Book raw data")
            
            with raw_tabs[2]:
                if not enriched["B Book"].empty:
                    st.dataframe(enriched["B Book"].head(50), use_container_width=True) 
                    st.caption(f"Showing first 50 of {len(enriched['B Book'])} B Book records")
                else:
                    st.info("No B Book raw data")
            
            with raw_tabs[3]:
                if not enriched["Multi Book"].empty:
                    st.dataframe(enriched["Multi Book"].head(50), use_container_width=True)
                    st.caption(f"Showing first 50 of {len(enriched['Multi Book'])} Multi Book records")
                else:
                    st.info("No Multi Book raw data")

    # ─── Export Section ─────────────────────────────────────────────────────────

    st.markdown("---")
    st.header("📥 Export & Download")
    
    export_col1, export_col2 = st.columns(2)
    
    with export_col1:
        st.subheader("📊 Excel Report")
        
        # Generate comprehensive Excel report
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            # Raw data splits
            for name, df in books.items():
                if not df.empty:
                    df.to_excel(writer, name, index=False)
            
            # Enriched data
            for name, df in enriched.items():
                if not df.empty:
                    df.to_excel(writer, f"{name} Enriched", index=False)
            
            # Analysis results
            for name, df in results.items():
                if not df.empty:
                    df.to_excel(writer, f"{name} Result", index=False)
            
            # Additional analyses
            if not client_summary.empty:
                client_summary.to_excel(writer, "Client Summary", index=False)
            
            if not chinese_clients.empty:
                chinese_clients.to_excel(writer, "Chinese Clients", index=False)
            
            if not final_calculations.empty:
                final_calculations.to_excel(writer, "Final Calculations", index=False)
            
            # Reference data
            if ex_csv:
                pd.DataFrame(sorted(excluded), columns=["Login"]).to_excel(writer, "Excluded Accounts", index=False)
            if vip_csv:
                pd.DataFrame(sorted(vip), columns=["Login"]).to_excel(writer, "VIP Client List", index=False)
        
        excel_buffer.seek(0)
        filename = f"deals_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        st.download_button(
            "📊 Download Excel Report",
            data=excel_buffer,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Complete Excel workbook with all analyses and raw data"
        )
    
    with export_col2:
        st.subheader("📄 PDF Report")
        
        if generate_pdf:
            try:
                # Prepare data for PDF
                pdf_data = {
                    "Final Calculations": final_calculations,
                    "A Book Results": results.get("A Book", pd.DataFrame()),
                    "B Book Results": results.get("B Book", pd.DataFrame()),
                    "Multi Book Results": results.get("Multi Book", pd.DataFrame()),
                    "Client Summary": client_summary,
                    "Chinese Clients": chinese_clients
                }
                
                pdf_bytes = create_pdf_report(pdf_data, date_range_str)
                pdf_filename = f"deals_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                
                st.download_button(
                    "📄 Download PDF Report",
                    data=pdf_bytes,
                    file_name=pdf_filename,
                    mime="application/pdf",
                    help="Executive summary PDF report"
                )
                
            except Exception as e:
                st.error(f"PDF generation failed: {str(e)}")
                st.info("PDF generation requires additional dependencies. Excel export is always available.")
        else:
            st.info("Enable 'Generate PDF Report' in the sidebar to create PDF export")

    # ─── Status & Information ─────────────────────────────────────────────────────

    st.markdown("---")
    st.header("ℹ️ Processing Summary")
    
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    
    with summary_col1:
        st.markdown("**📊 Data Processing:**")
        st.write(f"• Total records processed: {len(raw):,}")
        st.write(f"• A Book records: {len(enriched.get('A Book', [])):,}")
        st.write(f"• B Book records: {len(enriched.get('B Book', [])):,}")
        st.write(f"• Multi Book records: {len(enriched.get('Multi Book', [])):,}")
    
    with summary_col2:
        st.markdown("**🔧 Configuration:**")
        st.write(f"• Excluded accounts: {len(excluded):,}")
        st.write(f"• VIP clients: {len(vip):,}")
        st.write(f"• Date filtering: {'✅ Active' if use_date_filter and start_dt and end_dt else '❌ Disabled'}")
        st.write(f"• Chinese clients found: {len(chinese_clients)-1 if not chinese_clients.empty else 0:,}")
    
    with summary_col3:
        st.markdown("**📈 Key Metrics:**")
        st.write(f"• Total volume: ${total_volume:,.0f}")
        st.write(f"• Total lots: {total_lots:,.2f}")
        st.write(f"• Unique clients: {len(client_summary)-1 if not client_summary.empty else 0:,}")
        st.write(f"• Processing status: ✅ Complete")

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 20px;'>"
        "🚀 <b>Advanced Deals Reporting Dashboard</b> | "
        "Built with Streamlit | "
        f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        "</div>", 
        unsafe_allow_html=True
    )

except Exception as e:
    st.error(f"❌ An error occurred during processing: {str(e)}")
    st.info("💡 Please check your CSV file format and try again. Ensure all required columns are present.")
    
    # Debug information
    with st.expander("🔍 Debug Information"):
        st.write("**Error details:**")
        st.code(str(e))
        
        if 'raw' in locals():
            st.write("**Available columns in uploaded file:**")
            st.write(list(raw.columns))
        
        st.write("**Expected columns:**")
        expected_cols = [
            "Processing rule", "Login", "Notional volume in USD", 
            "Trader profit", "Swaps", "Commission", "TP broker profit", 
            "Total broker profit", "Date & Time (UTC)", "Group"
        ]
        st.write(expected_cols)