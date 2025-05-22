# combine_store_summaries.py
import boto3
import csv
import io

BUCKET = "spiritsbackups"
REPORT_PREFIX = "store_reports/"
FINAL_REPORT_KEY = "store_reports/store_summary.csv"

s3 = boto3.client("s3")

def get_store_csv_keys():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=BUCKET, Prefix=REPORT_PREFIX)

    for page in result:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("_summary.csv") and not key.endswith("store_summary.csv"):
                keys.append(key)
    return keys

def main():
    all_rows = []
    header = None

    for key in get_store_csv_keys():
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(lines)
        if not header:
            header = reader.fieldnames
        all_rows.extend(reader)

    if not header:
        print("⚠️ No summary CSVs found.")
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()
    writer.writerows(all_rows)

    s3.put_object(Bucket=BUCKET, Key=FINAL_REPORT_KEY, Body=output.getvalue().encode("utf-8"))
    print(f"✅ Combined summary uploaded to s3://{BUCKET}/{FINAL_REPORT_KEY}", flush=True)

if __name__ == "__main__":
    main()
