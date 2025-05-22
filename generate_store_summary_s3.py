import boto3
import csv
from configparser import ConfigParser
from datetime import datetime
import io
import os

BUCKET = "spiritsbackups"
PREFIX_BASE = "processed_csvs/"
REPORT_KEY = "store_reports/store_summary.csv"
LOCAL_TMP_PATH = "/tmp/store_summary.csv"

report_date = datetime.today().date()
start_date = ""
end_date = ""

s3 = boto3.client("s3")

def read_csv(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8", errors="ignore"))))
    except:
        return []

def read_ini(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj["Body"].read().decode("utf-8", errors="ignore")
        cfg = ConfigParser()
        cfg.read_string("[S]\n" + content if not content.startswith("[") else content)
        return cfg
    except:
        return ConfigParser()

def days_since_last(rows, cappname):
    dates = []
    for r in rows:
        if r.get("cappname", "").upper() == cappname.upper():
            try:
                dates.append(datetime.strptime(r["rundate"], "%Y-%m-%d").date())
            except:
                continue
    return (report_date - max(dates)).days if dates else ""

def process_prefix(prefix, writer):
    base = f"{PREFIX_BASE}{prefix}"
    str_rows = read_csv(f"{base}/str.csv")
    if not str_rows or "NAME" not in str_rows[0]:
        print(f"⚠️ Skipping {prefix} — str.csv missing or NAME column absent")
        return

    store_name = str_rows[0]["NAME"]
    reports = read_csv(f"{base}/reports.csv")
    jnl = read_csv(f"{base}/jnl.csv")
    stk = read_csv(f"{base}/stk.csv")
    ini = read_ini(f"{base}/spirits.ini")

    line_discount = any(r.get("cat") in ["60", "63"] and r.get("rflag") == "0" for r in jnl)
    club_used = any("CLUB" in r.get("promo", "").upper() for r in jnl)
    kits_used = any(r.get("stat") == "9" for r in stk)
    rtn_code = ini.get("S", "RtnDeposCode", fallback="").strip()
    use_tomra = "N" if rtn_code in ["", "99999"] else "Y"

    row = {
        "store_id (s3_prefix)": f"{store_name} ({prefix})",
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

def main():
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_BASE, Delimiter="/")

    fieldnames = [
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

    with open(LOCAL_TMP_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for page in result:
            for p in page.get("CommonPrefixes", []):
                prefix = p["Prefix"].split("/")[-2]
                process_prefix(prefix, writer)

    with open(LOCAL_TMP_PATH, "rb") as f:
        s3.put_object(Bucket=BUCKET, Key=REPORT_KEY, Body=f.read())

    print(f"✅ Uploaded store summary report to s3://{BUCKET}/{REPORT_KEY}")

if __name__ == "__main__":
    main()
