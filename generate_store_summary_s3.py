import boto3
import csv
from configparser import ConfigParser
from datetime import datetime
from io import StringIO

# === CONFIG ===
BUCKET = "spiritsbackups"
PREFIX = "processed_csvs/"
OUTPUT_FILE = "store_summary.csv"
report_date = datetime.today().date()
start_date = ""
end_date = ""

s3 = boto3.client("s3")

# === HELPERS ===
def read_csv_lines(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return list(csv.DictReader(StringIO(obj['Body'].read().decode('utf-8', errors='ignore'))))
    except Exception:
        return []

def read_ini(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj['Body'].read().decode('utf-8', errors='ignore')
        config = ConfigParser()
        config.read_string("[Settings]\n" + content if not content.startswith('[') else content)
        return config
    except:
        return ConfigParser()

def days_since_last(rows, cappname):
    filtered = [r for r in rows if r.get("cappname", "").upper() == cappname.upper()]
    dates = []
    for row in filtered:
        try:
            dates.append(datetime.strptime(row["rundate"], "%Y-%m-%d").date())
        except:
            continue
    return (report_date - max(dates)).days if dates else ""

# === CSV SETUP ===
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

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX, Delimiter="/")

    found_any_prefix = False

    for page in pages:
        common_prefixes = page.get("CommonPrefixes", [])
        if not common_prefixes:
            print("⚠️ No CommonPrefixes found. Double-check if the folders contain at least one object.")

        for pfx in common_prefixes:
            found_any_prefix = True
            prefix = pfx["Prefix"]
            print(f"🔍 Processing: {prefix}")
            base = prefix.rstrip('/')

            try:
                str_rows = read_csv_lines(f"{base}/str.csv")
                if not str_rows:
                    print(f"⚠️ str.csv is missing or empty in {base}")
                    continue

                first_row = str_rows[0]
                possible_keys = ["Name", "name", "STORENAME", "Store Name"]
                store_name = next((first_row.get(k) for k in possible_keys if k in first_row), None)

                if store_name:
                    print(f"🏪 Store name: {store_name}")
                else:
                    print(f"⚠️ No valid name field in str.csv for {base}")
                    continue

                combined_id = f"{store_name} ({base.split('/')[-1]})"

                reports = read_csv_lines(f"{base}/reports.csv")
                jnl = read_csv_lines(f"{base}/jnl.csv")
                stk = read_csv_lines(f"{base}/stk.csv")
                ini = read_ini(f"{base}/spirits.ini")

                line_discount = any(r.get("cat") in ["60", "63"] and r.get("rflag") == "0" for r in jnl)
                club_used = any("CLUB" in r.get("promo", "").upper() for r in jnl)
                kits_used = any(r.get("stat") == "9" for r in stk)

                rtn_code = ini.get("Settings", "RtnDeposCode", fallback="").strip()
                use_tomra = "N" if rtn_code in ["", "99999"] else "Y"

                row = {
                    "store_id (s3_prefix)": combined_id,
                    "report_date": report_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "Use_Inventory_Counting_Report": days_since_last(reports, "INVCOUNT.EXE"),
                    "Use_Suggested_Order_Report": days_since_last(reports, "SUGORDER.EXE"),
                    "Use_NJ_Rips_Report": days_since_last(reports, "BDRIPRPT.EXE"),
                    "Use_NJ_Buydowns_Rips_Report": days_since_last(reports, "BDRIPRPT.EXE"),
                    "Use_inventory_value_analysis_report": days_since_last(reports, "INVANAL"),
                    "Use_frequent_shopper_report": days_since_last(reports, "FSPURCHHST.EXE"),
                    "Use_price_level_upcs": "",
                    "Use_line_item_discount": "Y" if line_discount else "N",
                    "Use_club_list": "Y" if club_used else "N",
                    "Use_corp_polling": "",
                    "Num_of_stores_in_corp_polling": "",
                    "Use_kits": "Y" if kits_used else "N",
                    "Use_TOMRA": use_tomra,
                    "Use_Quick_PO": "",
                    "ecom_doordash": "",
                    "ecom_ubereats": "",
                    "ecom_cthive": "",
                    "ecom_winefetch": "",
                    "ecom_bottlenose": "",
                    "ecom_bottlecaps": ""
                }

                writer.writerow(row)

            except Exception as e:
                print(f"⚠️ Skipping {prefix}: {e}")
                continue

    if not found_any_prefix:
        print("❌ No store prefixes were processed. Check if objects exist under each folder.")
