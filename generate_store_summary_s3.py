# generate_store_summary_s3.py
import boto3
import csv
import io
import os
import psutil
from configparser import ConfigParser
from datetime import datetime

BUCKET = "spiritsbackups"
PREFIX_BASE = "processed_csvs/"
REPORT_PREFIX = "store_reports/"
report_date = datetime.today().date()
start_date = ""
end_date = ""
LOG_KEY = f"{REPORT_PREFIX}generate_store_summary.log"

s3 = boto3.client("s3")
log_lines = []

def log(msg):
    timestamp = datetime.now().isoformat()
    entry = f"[{timestamp}] {msg}"
    print(entry, flush=True)
    log_lines.append(entry)

def log_memory(prefix):
    mem_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    log(f"🔍 Memory after processing {prefix}: {mem_mb:.2f} MB")

def read_csv(key, max_lines=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        decoded = obj["Body"].read().decode("utf-8", errors="ignore").splitlines()
        if max_lines:
            decoded = decoded[:max_lines + 1]  # include header
        return list(csv.DictReader(io.StringIO("\n".join(decoded))))
    except Exception as e:
        log(f"❌ Failed to read {key}: {e}")
        return []

def read_ini(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj["Body"].read().decode("utf-8", errors="ignore")
        cfg = ConfigParser()
        cfg.read_string("[S]\n" + content if not content.startswith("[") else content)
        return cfg
    except Exception as e:
        log(f"❌ Failed to read {key}: {e}")
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

def process_prefix(prefix):
    base = f"{PREFIX_BASE}{prefix}"
    str_rows = read_csv(f"{base}/str.csv")
    if not str_rows or "NAME" not in str_rows[0]:
        log(f"⚠️ Skipping {prefix} — str.csv missing or NAME column absent")
        return

    store_name = str_rows[0]["NAME"]
    reports = read_csv(f"{base}/reports.csv")
    jnl = read_csv(f"{base}/jnl.csv", max_lines=5000)
    stk = read_csv(f"{base}/stk.csv", max_lines=5000)
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
        "use_inventory_counting_report": days_since_last(reports, "INVCOUNT.EXE"),
        "use_suggested_order_report": days_since_last(reports, "SUGORDER.EXE"),
        "use_nj_rips_report": days_since_last(reports, "BDRIPRPT.EXE"),
        "use_nj_buydowns_rips_report": days_since_last(reports, "BDRIPRPT.EXE"),
        "use_inventory_value_analysis_report": days_since_last(reports, "INVANAL"),
        "use_frequent_shopper_report": days_since_last(reports, "FSPURCHHST.EXE"),
        "use_price_level_upcs": "Y",
        "use_line_item_discount": "Y" if line_discount else "N",
        "use_club_list": "Y" if club_used else "N",
        "use_corp_polling": "",
        "num_of_stores_in_corp_polling": "",
        "use_kits": "Y" if kits_used else "N",
        "use_TOMRA": use_tomra,
        "use_quick_po": "",
        "ecom_doordash": "",
        "ecom_ubereats": "",
        "ecom_cthive": "",
        "ecom_winefetch": "",
        "ecom_bottlenose": "",
        "ecom_bottlecaps": ""
    }

    # Write and upload single CSV for the store
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=row.keys())
    writer.writeheader()
    writer.writerow(row)
    s3.put_object(Bucket=BUCKET, Key=f"{REPORT_PREFIX}{prefix}_summary.csv", Body=buffer.getvalue().encode("utf-8"))
    log(f"✅ Uploaded {prefix}_summary.csv")

    # Clean up memory
    del str_rows, reports, jnl, stk, ini, row, buffer, writer
    log_memory(prefix)

def main():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_BASE, Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            prefix = p["Prefix"].split("/")[-2]
            try:
                process_prefix(prefix)
            except Exception as e:
                log(f"❌ Error processing {prefix}: {e}")

    # Final log upload
    s3.put_object(Bucket=BUCKET, Key=LOG_KEY, Body="\n".join(log_lines).encode("utf-8"))
    log(f"📝 Log uploaded to s3://{BUCKET}/{LOG_KEY}")

if __name__ == "__main__":
    main()
