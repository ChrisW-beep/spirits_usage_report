import boto3
import pandas as pd
from configparser import ConfigParser
from io import BytesIO, StringIO
from datetime import datetime

# === CONFIG ===
BUCKET = "spiritsbackups"
PREFIX = "processed_csvs/"
report_date = datetime.today().date()
start_date = ""  # Optionally set a fixed or calculated value
end_date = ""    # Optionally set a fixed or calculated value

s3 = boto3.client("s3")
summary_rows = []

# === HELPERS ===
def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(obj['Body'])

def read_ini_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    content = obj['Body'].read().decode('utf-8', errors='ignore')
    config = ConfigParser()
    config.read_string("[Settings]\n" + content if not content.startswith('[') else content)
    return config

def days_since_last(df, cappname_col, target):
    if cappname_col not in df.columns or "rundate" not in df.columns:
        return ''
    df_filtered = df[df[cappname_col].str.upper() == target.upper()]
    if df_filtered.empty:
        return ''
    df_filtered['rundate'] = pd.to_datetime(df_filtered['rundate'], errors='coerce')
    last_run = df_filtered['rundate'].max()
    return (report_date - last_run.date()).days if pd.notnull(last_run) else ''

# === MAIN LOOP ===
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX, Delimiter="/")

for page in pages:
    for prefix in page.get("CommonPrefixes", []):
        store_prefix = prefix["Prefix"]
        s3_key_base = store_prefix.rstrip('/')

        try:
            jnl = read_csv_from_s3(f"{s3_key_base}/jnl.csv")
            str_df = read_csv_from_s3(f"{s3_key_base}/str.csv")
            reports = read_csv_from_s3(f"{s3_key_base}/reports.csv")
            stk = None
            try:
                stk = read_csv_from_s3(f"{s3_key_base}/stk.csv")
            except:
                pass
            try:
                ini = read_ini_from_s3(f"{s3_key_base}/spirits.ini")
            except:
                ini = ConfigParser()

            store_id = str_df.iloc[0]["store"] if "store" in str_df.columns else ""
            combined_id = f"{store_id} ({s3_key_base.split('/')[-1]})"

            row = {
                "store_id (s3_prefix)": combined_id,
                "report_date": report_date,
                "start_date": start_date,
                "end_date": end_date,
                "Use_Inventory_Counting_Report": days_since_last(reports, "cappname", "INVCOUNT.EXE"),
                "Use_Suggested_Order_Report": days_since_last(reports, "cappname", "SUGORDER.EXE"),
                "Use_NJ_Rips_Report": days_since_last(reports, "cappname", "BDRIPRPT.EXE"),
                "Use_NJ_Buydowns_Rips_Report": days_since_last(reports, "cappname", "BDRIPRPT.EXE"),
                "Use_inventory_value_analysis_report": days_since_last(reports, "cappname", "INVANAL"),
                "Use_frequent_shopper_report": days_since_last(reports, "cappname", "FSPURCHHST.EXE"),
                "Use_price_level_upcs": "",  # Logic TBD
                "Use_line_item_discount": "Y" if not jnl[(jnl["cat"].isin([60, 63])) & (jnl["rflag"] == 0)].empty else "N",
                "Use_club_list": "Y" if jnl["promo"].astype(str).str.contains("CLUB", case=False, na=False).any() else "N",
                "Use_corp_polling": "",
                "Num_of_stores_in_corp_polling": "",
                "Use_kits": "Y" if stk is not None and not stk[stk["stat"] == 9].empty else "N",
                "Use_TOMRA": "N" if ini.get("Settings", "RtnDeposCode", fallback="").strip() in ["", "99999"] else "Y",
                "Use_Quick_PO": "",
                "ecom_doordash": "",
                "ecom_ubereats": "",
                "ecom_cthive": "",
                "ecom_winefetch": "",
                "ecom_bottlenose": "",
                "ecom_bottlecaps": ""
            }

            summary_rows.append(row)

        except Exception as e:
            print(f"⚠️ Failed for prefix {store_prefix}: {e}")
            continue

# === SAVE FINAL REPORT ===
summary_df = pd.DataFrame(summary_rows)
import ace_tools as tools; tools.display_dataframe_to_user(name="Store Summary Report", dataframe=summary_df)
