# generate_store_summary_s3.py
import boto3
import csv
import io
import os
import re
from configparser import ConfigParser
from datetime import datetime

BUCKET = "spiritsbackups"
PREFIX_BASE = "processed_csvs/"
REPORT_PREFIX = "store_reports/"
report_date = datetime.today().date()
start_date = os.environ.get("START_DATE", "")
end_date = os.environ.get("END_DATE", "")

s3 = boto3.client("s3")

def read_csv(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return list(csv.DictReader(io.StringIO(obj["Body"].read().decode("latin1", errors="ignore"))))
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Failed to read {key}: {e}")
        return []

def read_ini_allow_duplicates(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        content = obj["Body"].read().decode("latin1", errors="ignore")

        section_counts = {}
        patched_lines = []
        for line in content.splitlines():
            section_match = re.match(r"\[(.+?)\]", line.strip())
            if section_match:
                section_name = section_match.group(1).strip()
                section_key = section_name.upper()
                if section_key in section_counts:
                    section_counts[section_key] += 1
                    new_section = f"{section_name}_{section_counts[section_key]}"
                else:
                    section_counts[section_key] = 1
                    new_section = section_name
                patched_lines.append(f"[{new_section}]")
            else:
                patched_lines.append(line)

        if not patched_lines or not patched_lines[0].strip().startswith("["):
            patched_lines.insert(0, "[DEFAULT]")

        patched_content = "\n".join(patched_lines)
        config = ConfigParser()
        config.read_string(patched_content)
        return config

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Failed to parse {key}: {e}", flush=True)
        return ConfigParser()

def days_since_last(rows, cappname):
    dates = []
    for r in rows:
        if r.get("cappname", "").strip().upper() == cappname.upper():
            try:
                dt = r.get("rundate", "").strip()
                if dt and dt not in ["/", "/ / /", ""]:
                    parsed = datetime.strptime(dt, "%m/%d/%y %I:%M:%S %p").date()
                    dates.append(parsed)
            except Exception as e:
                print(f"[{datetime.now()}] ⚠️ Skipped bad date for {cappname}: {dt} ({e})")
    return (report_date - max(dates)).days if dates else ""

def process_prefix(prefix):
    base = f"{PREFIX_BASE}{prefix}"
    str_rows = read_csv(f"{base}/str.csv")
    if not str_rows or "NAME" not in str_rows[0]:
        print(f"[{datetime.now()}] ⚠️ Skipping {prefix} — str.csv missing or NAME column absent")
        return

    store_name = str_rows[0]["NAME"]
    reports = read_csv(f"{base}/reports.csv")
    jnl = read_csv(f"{base}/jnl.csv")
    stk = read_csv(f"{base}/stk.csv")
    cnt = read_csv(f"{base}/cnt.csv")
    ini = read_ini_allow_duplicates(f"{base}/spirits.ini")

    line_discount = any(r.get("cat") in ["60", "63"] and r.get("rflag") == "0" for r in jnl)
    club_used = any("CLUB" in r.get("promo", "").upper() for r in jnl)
    kits_used = any(r.get("stat") == "9" for r in stk)
    rtn_code = ini.get("S", "RtnDeposCode", fallback="").strip()
    use_tomra = "N" if rtn_code in ["", "999999"] else "Y"

    corp_polling = ""
    for row in cnt:
        if row.get("CODE", "").strip().upper() == "CORPPOLL":
            val = row.get("DATA", "").strip().upper()
            corp_polling = "Y" if val == "YES" else "N"
            break

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
        "use_corp_polling": corp_polling,
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

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=row.keys())
    writer.writeheader()
    writer.writerow(row)

    key = f"{REPORT_PREFIX}{prefix}_summary.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_buffer.getvalue().encode("utf-8"))
    print(f"[{datetime.now()}] ✅ Uploaded {key}", flush=True)

def main():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_BASE, Delimiter="/")

    for page in pages:
        for p in page.get("CommonPrefixes", []):
            prefix = p["Prefix"].split("/")[-2]
            process_prefix(prefix)

if __name__ == "__main__":
    main()
