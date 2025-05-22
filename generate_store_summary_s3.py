import boto3
import pandas as pd
import csv
from configparser import ConfigParser
from datetime import datetime

# === CONFIG ===
BUCKET = "spiritsbackups"
PREFIX = "processed_csvs/"
OUTPUT_FILE = "store_summary.csv"
report_date = datetime.today().date()
start_date = ""  # Optional
end_date = ""    # Optional

s3 = boto3.client("s3")

# === HELPERS ===
def read_csv(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(obj['Body'], low_memory=False, dtype=str)

def read_ini(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj['Body'].read().decode('utf-8', errors='ignore')
        config = ConfigParser()
        config.read_string("[Settings]\n" + content if not content.startswith('[') else content)
        return config
    except:
        return ConfigParser()

def days_since(df, cappname):
    if "cappname" not in df.columns or "rundate" not in df.columns:
        return ''
    df = df[df['cappname'].str.upper() == cappname.upper()]
    if df.empty:
        return ''
    df['rundate'] = pd.to_datetime(df['rundate'], errors='coerce')
    last = df['rundate'].max()
    return (report_date - last.date()).days if pd.notnull(last) else ''

# === WRITE CSV HEADER ===
headers = [
    "store_id (s3_prefix)", "report_date", "start_date", "end_date",
    "Use_Inventory_Counting_Report", "Use_Suggested_Order_Report",
    "Use_NJ_Rips_Report", "Use_NJ_Buydowns_Rips_Report",
    "Use_inventory_value_analysis_report", "Use_frequent_shopper_report",
    "Use_price_level_upcs", "Use_line_item_discount", "Use_club_list",
    "Use_corp_polling", "Num_of_stores_in_corp_polling",
    "Use_kits", "Use_TOMRA", "Use_Quick_PO",
    "ecom_doordash", "ecom_ubereats", "ecom_cthive",
    "ecom_winefetch", "ecom_bottlenose", "ecom_bottlecaps"
]
with open(OUTPUT_FILE, "w", newline="") as out:
    writer = csv.DictWriter(out, fieldnames=headers)
    writer.writeheader()

    # === LIST PREFIXES AND LOOP ===
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX, Delimiter="/")

    for page in pages:
        for pfx in page.get("CommonPrefixes", []):
            prefix = pfx["Prefix"]
            print(f"📦 Processing: {prefix}")
            try:
                base = prefix.rstrip('/')
                jnl = read_csv(f"{base}/jnl.csv")
                str_df = read_csv(f"{base}/str.csv")
                reports = read_csv(f"{base}/reports.csv")
                store_id = str_df.iloc[0]['store'] if "store" in str_df.columns else ''
                combined_id = f"{store_id} ({base.split('/')[-1]})"

                try:
                    stk = read_csv(f"{base}/stk.csv")
                except:
                    stk = pd.DataFrame()

                ini = read_ini(f"{base}/spirits.ini")

                row = {
                    "store_id (s3_prefix)": combined_id,
                    "report_date": report_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "Use_Inventory_Counting_Report": days_since(reports, "INVCOUNT.EXE"),
                    "Use_Suggested_Order_Report": days_since(reports, "SUGORDER.EXE"),
                    "Use_NJ_Rips_Report": days_since(reports, "BDRIPRPT.EXE"),
                    "Use_NJ_Buydowns_Rips_Report": days_since(reports, "BDRIPRPT.EXE"),
                    "Use_inventory_value_analysis_report": days_since(reports, "INVANAL"),
                    "Use_frequent_shopper_report": days_since(reports, "FSPURCHHST.EXE"),
                    "Use_price_level_upcs": "",
                    "Use_line_item_discount": "Y" if not jnl[(jnl["cat"].isin(["60", "63"])) & (jnl["rflag"] == "0")].empty else "N",
