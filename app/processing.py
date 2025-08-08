import pandas as pd
import numpy as np
from datetime import datetime

# ─── Helpers ────────────────────────────────────────────────────────────────

def round4(x):
    """Safely round a value to 4 decimal places."""
    try:
        return round(float(x), 4)
    except (ValueError, TypeError):
        return 0.0

def parse_custom_datetime(s: str):
    """Parse datetime in a custom format: dd.mm.yyyy hh:mm:ss"""
    try:
        return pd.to_datetime(s, format="%d.%m.%Y %H:%M:%S", utc=True)
    except (ValueError, TypeError):
        return pd.NaT

def sanitize_numeric_series(sr: pd.Series) -> pd.Series:
    """Clean a pandas Series to ensure it contains only numeric values."""
    return (
        sr.astype(str)
          .str.replace(r"[^\d\.\-]", "", regex=True)
          .replace(r"^\s*$", "0", regex=True)
          .astype(float)
          .fillna(0.0)
    )

def filter_by_date_range(df: pd.DataFrame, start_date, end_date, datetime_col="Date & Time (UTC)"):
    """Filter a DataFrame by a given date range."""
    if df.empty or datetime_col not in df.columns:
        return df

    if start_date and end_date:
        mask = pd.Series([True] * len(df))

        start_dt = parse_custom_datetime(start_date) if isinstance(start_date, str) else start_date
        end_dt = parse_custom_datetime(end_date) if isinstance(end_date, str) else end_date

        if pd.isna(start_dt) or pd.isna(end_dt):
             raise ValueError("Invalid start or end date format. Please use 'dd.mm.yyyy hh:mm:ss'")

        # This is more efficient than iterating row-by-row
        parsed_dts = df[datetime_col].apply(lambda x: parse_custom_datetime(str(x)))
        mask = (parsed_dts >= start_dt) & (parsed_dts <= end_dt)

        return df[mask].copy()
    return df

# ─── Core Processing ─────────────────────────────────────────────────────────

def process_and_split(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Convert USC to USD and split the DataFrame by 'Processing rule' into A/B/Multi books."""
    d = df.copy()
    # USC → USD conversion
    for col in d.select_dtypes(include="object"):
        d[col] = d[col].astype(str).str.replace(
            r"(?i)(\d[\d\.\-]*)\s*usc",
            lambda m: f"{round4(float(m.group(1)) / 100):.4f} USD",
            regex=True
        )

    if "Processing rule" not in d:
        raise ValueError("Missing 'Processing rule' column in the deals CSV.")

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
    """Add calculated columns and remove duplicate deals based on the first column."""
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
    """Aggregate book data, applying specific exclusion logic based on book type."""
    if df.empty:
        return pd.DataFrame()

    required = ["Login", "Notional volume in USD", "Trader profit", "Swaps", "Commission", "TP broker profit", "Total broker profit"]
    for col in required:
        if col not in df:
            raise ValueError(f"Missing required column '{col}' in the deals CSV.")
        if col != "Login":
            df[col] = sanitize_numeric_series(df[col])

    rows = []
    for login, group in df.groupby("Login", dropna=False):
        if pd.isna(login):
            continue

        login_str = str(int(login)).strip() if pd.notna(login) else ""
        is_excluded = login_str in excluded

        if book_type == "B Book" and is_excluded:
            continue

        comm, tp, bk = (0, 0, 0) if is_excluded and book_type in ["A Book", "Multi Book"] else (group["Commission"].sum(), group["TP broker profit"].sum(), group["Total broker profit"].sum())

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
    """Generate analysis for Chinese clients, excluding specified accounts."""
    chinese_prefixes = ['real\\Chines', 'BBOOK\\Chines']
    chinese_summary = {}

    for book_name, df in enriched_books.items():
        if df.empty:
            continue

        required_cols = ["Login", "Group", "Notional volume in USD", "Trader profit", "Swaps", "Commission", "TP broker profit", "Total broker profit"]
        if not all(col in df.columns for col in required_cols):
            continue

        for _, row in df.iterrows():
            login = str(int(row["Login"])).strip() if pd.notna(row["Login"]) else ""
            group = str(row["Group"]).strip()

            if not login or login in excluded or not any(group.startswith(prefix) for prefix in chinese_prefixes):
                continue

            if login not in chinese_summary:
                chinese_summary[login] = {"Total Volume": 0, "Trader Profit": 0, "Swaps": 0, "Commission": 0, "TP Profit": 0, "Broker Profit": 0}

            chinese_summary[login]["Total Volume"] += float(row["Notional volume in USD"] or 0)
            chinese_summary[login]["Trader Profit"] += float(row["Trader profit"] or 0)
            chinese_summary[login]["Swaps"] += float(row["Swaps"] or 0)
            chinese_summary[login]["Commission"] += float(row["Commission"] or 0)
            chinese_summary[login]["TP Profit"] += float(row["TP broker profit"] or 0)
            chinese_summary[login]["Broker Profit"] += float(row["Total broker profit"] or 0)

    if not chinese_summary:
        return pd.DataFrame(columns=["Login", "Total Volume", "Trader Profit", "Swaps", "Commission", "TP Profit", "Broker Profit", "Net"])

    rows = []
    for login, data in chinese_summary.items():
        net = data["Trader Profit"] + data["Swaps"] - data["Commission"]
        rows.append({"Login": login, **{k: round4(v) for k, v in data.items()}, "Net": round4(net)})

    df_chinese = pd.DataFrame(rows)

    if not df_chinese.empty:
        summary = {col: round4(df_chinese[col].sum()) for col in df_chinese.columns if col != "Login"}
        summary["Login"] = "Summary"
        df_chinese = pd.concat([df_chinese, pd.DataFrame([summary])], ignore_index=True)

    return df_chinese

def generate_client_summary(results: dict) -> pd.DataFrame:
    """Generate a consolidated client summary across all books."""
    all_clients = {}
    for book_name, df in results.items():
        if df.empty:
            continue
        client_data = df[df["Login"] != "Summary"].copy()
        for _, row in client_data.iterrows():
            login = row["Login"]
            if login not in all_clients:
                all_clients[login] = {"Total Volume": 0, "Trader Profit": 0, "Swaps": 0, "Commission": 0, "TP Profit": 0, "Broker Profit": 0, "Net": 0}
            for col in all_clients[login]:
                all_clients[login][col] += float(row.get(col, 0) or 0)

    if not all_clients:
        return pd.DataFrame()

    df_summary = pd.DataFrame([{ "Login": login, **{k: round4(v) for k, v in data.items()} } for login, data in all_clients.items()])

    if not df_summary.empty:
        summary = {col: round4(df_summary[col].sum()) for col in df_summary.columns if col != "Login"}
        summary["Login"] = "Summary"
        df_summary = pd.concat([df_summary, pd.DataFrame([summary])], ignore_index=True)

    return df_summary

def calculate_vip_volume(enriched_books: dict, vip_clients: set, excluded: set) -> float:
    """Calculate the total volume for VIP clients, excluding specified accounts."""
    total_vip_volume = 0
    for book_name, df in enriched_books.items():
        if df.empty or "Login" not in df.columns or "Notional volume in USD" not in df.columns:
            continue
        for _, row in df.iterrows():
            login = str(int(row["Login"])).strip() if pd.notna(row["Login"]) else ""
            if login and login in vip_clients and login not in excluded:
                total_vip_volume += float(row["Notional volume in USD"] or 0)
    return total_vip_volume

def generate_final_calculations(results: dict, chinese_df: pd.DataFrame, vip_volume: float, date_range: str = "") -> pd.DataFrame:
    """Generate the final summary calculations table."""
    def get_sum(book_name, column):
        if book_name not in results or results[book_name].empty: return 0
        summary_row = results[book_name][results[book_name]["Login"] == "Summary"]
        return float(summary_row[column].iloc[0] or 0) if not summary_row.empty else 0

    a_book_commission = get_sum("A Book", "Commission")
    a_book_tp = get_sum("A Book", "TP Profit")
    multi_commission = get_sum("Multi Book", "Commission")
    multi_tp = get_sum("Multi Book", "TP Profit")
    a_book_total = a_book_commission + a_book_tp + multi_commission + multi_tp

    b_book_tsm = get_sum("B Book", "Net") * -1
    multi_total_broker = get_sum("Multi Book", "Broker Profit")
    multi_tp_broker = get_sum("Multi Book", "TP Profit")
    b_book_extra = multi_total_broker - multi_tp_broker
    b_book_total = b_book_tsm + b_book_extra

    a_book_volume = get_sum("A Book", "Total Volume")
    b_book_volume = get_sum("B Book", "Total Volume")
    multi_volume = get_sum("Multi Book", "Total Volume")

    total_swaps = get_sum("A Book", "Swaps") + get_sum("Multi Book", "Swaps")

    a_book_lot = (a_book_volume + multi_volume) / 200000
    b_book_lot = b_book_volume / 200000

    chinese_volume = get_sum("Chinese Clients", "Total Volume") if not chinese_df.empty else 0
    chinese_lot = chinese_volume / 200000
    vip_lot = vip_volume / 200000
    retail_lot = a_book_lot + b_book_lot - chinese_lot - vip_lot
    total_lot = a_book_lot + b_book_lot

    calculations = []
    if date_range:
        calculations.extend([["DATE RANGE", "", date_range], ["", "", ""]])

    calculations.extend([
        ["A BOOK SUMMARY", "", ""], ["Source", "Description", "Value"],
        ["A Book Result", "Sum of TP Broker Profit + Commission", round4(a_book_tp + a_book_commission)],
        ["Multi Book Result", "Sum of TP Broker Profit + Commission", round4(multi_tp + multi_commission)],
        ["Total A Book", "Sum of above two values", round4(a_book_total)],
        ["", "", ""],
        ["B BOOK SUMMARY", "", ""], ["Source", "Description", "Value"],
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

def run_report_processing(deals_df: pd.DataFrame, excluded_df: pd.DataFrame, vip_df: pd.DataFrame, start_date: str = None, end_date: str = None):
    """
    Main orchestrator function to run the entire report generation process.
    """
    # 1. Load sets for excluded and vip clients
    excluded_logins = set(excluded_df.iloc[:, 0].astype(str).str.strip()) if not excluded_df.empty else set()
    vip_logins = set(vip_df.iloc[:, 0].astype(str).str.strip()) if not vip_df.empty else set()

    # 2. Process and split the main deals dataframe
    books = process_and_split(deals_df)
    enriched = {k: enrich_and_dedupe(v) for k, v in books.items()}

    # 3. Apply date filtering if enabled
    date_range_str = ""
    if start_date and end_date:
        date_range_str = f"From {start_date} to {end_date}"
        for k in enriched:
            enriched[k] = filter_by_date_range(enriched[k], start_date, end_date)

    # 4. Generate all analyses
    results = {
        book_name: aggregate_book(book_data, excluded_logins, book_name)
        for book_name, book_data in enriched.items()
    }

    chinese_clients = generate_chinese_clients(enriched, excluded_logins)
    client_summary = generate_client_summary(results)
    vip_volume = calculate_vip_volume(enriched, vip_logins, excluded_logins)
    final_calculations = generate_final_calculations(results, chinese_clients, vip_volume, date_range_str)

    return {
        "A Book Raw": enriched.get("A Book", pd.DataFrame()),
        "B Book Raw": enriched.get("B Book", pd.DataFrame()),
        "Multi Book Raw": enriched.get("Multi Book", pd.DataFrame()),
        "A Book Result": results.get("A Book", pd.DataFrame()),
        "B Book Result": results.get("B Book", pd.DataFrame()),
        "Multi Book Result": results.get("Multi Book", pd.DataFrame()),
        "Chinese Clients": chinese_clients,
        "Client Summary": client_summary,
        "Final Calculations": final_calculations,
        "VIP Volume": vip_volume
    }
