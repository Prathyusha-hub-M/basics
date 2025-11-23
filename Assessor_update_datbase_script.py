import os
import requests
from datetime import datetime
import mysql.connector
from mysql.connector import Error

API_URL = "https://datacatalog.cookcountyil.gov/resource/y282-6ig3.json"

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB_NAME"],
}

def to_decimal(value):
    try:
        return float(value) if value not in (None, "", " ") else None
    except:
        return None

# helpers to read and update last sync
def get_last_sync_ts(cursor):
    cursor.execute("SELECT last_updated_at FROM sync_metadata WHERE id = 1")
    row = cursor.fetchone()
    return row[0]  # returns datetime or None

def update_last_sync_ts(cursor, ts):
    cursor.execute(
        "UPDATE sync_metadata SET last_updated_at = %s WHERE id = 1",
        (ts,)
    )

def fetch_api_data(limit=5000, offset=0, last_sync_ts=None):
    where_clauses = ["year = 2025"]

    if last_sync_ts:
        # Socrata expects ISO string, usually UTC
        iso = last_sync_ts.isoformat(timespec="seconds")
        where_clauses.append(f":updated_at > '{iso}'")

    params = {
        "$limit": limit,
        "$offset": offset,
        "$where": " AND ".join(where_clauses),
        "$order": ":updated_at"
    }

    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def parse_socrata_datetime(val):
    # Usually like "2024-08-01T12:34:56.000"
    if not val:
        return None
    # remove trailing Z if present
    val = val.rstrip("Z")
    try:
        return datetime.fromisoformat(val)
    except ValueError:
        return None

def main():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO `assessor_appeals` (
            `pin`, `tax_year`, `class`, `township_code`, `case_no`,
            `appeal_type`, `status`, `mailed_bldg`, `mailed_land`, `mailed_tot`,
            `certified_bldg`, `certified_land`, `certified_tot`, `appeal_change`,
            `reason_code1`, `reason_desc1`, `reason_code2`, `reason_desc2`,
            `reason_code3`, `reason_desc3`, `agent_code`, `agent_name`, `row_id`
        ) VALUES (
            %(pin)s, %(tax_year)s, %(class)s, %(township_code)s, %(case_no)s,
            %(appeal_type)s, %(status)s, %(mailed_bldg)s, %(mailed_land)s, %(mailed_tot)s,
            %(certified_bldg)s, %(certified_land)s, %(certified_tot)s, %(appeal_change)s,
            %(reason_code1)s, %(reason_desc1)s, %(reason_code2)s, %(reason_desc2)s,
            %(reason_code3)s, %(reason_desc3)s, %(agent_code)s, %(agent_name)s, %(row_id)s
        )
        ON DUPLICATE KEY UPDATE
            `pin`            = VALUES(`pin`),
            `tax_year`       = VALUES(`tax_year`),
            `class`          = VALUES(`class`),
            `township_code`  = VALUES(`township_code`),
            `case_no`        = VALUES(`case_no`),
            `appeal_type`    = VALUES(`appeal_type`),
            `status`         = VALUES(`status`),
            `mailed_bldg`    = VALUES(`mailed_bldg`),
            `mailed_land`    = VALUES(`mailed_land`),
            `mailed_tot`     = VALUES(`mailed_tot`),
            `certified_bldg` = VALUES(`certified_bldg`),
            `certified_land` = VALUES(`certified_land`),
            `certified_tot`  = VALUES(`certified_tot`),
            `appeal_change`  = VALUES(`appeal_change`),
            `reason_code1`   = VALUES(`reason_code1`),
            `reason_desc1`   = VALUES(`reason_desc1`),
            `reason_code2`   = VALUES(`reason_code2`),
            `reason_desc2`   = VALUES(`reason_desc2`),
            `reason_code3`   = VALUES(`reason_code3`),
            `reason_desc3`   = VALUES(`reason_desc3`),
            `agent_code`     = VALUES(`agent_code`),
            `agent_name`     = VALUES(`agent_name`),
            `row_id`         = VALUES(`row_id`),
            `source_loaded_at` = CURRENT_TIMESTAMP;
        """

        last_sync_ts = get_last_sync_ts(cursor)
        max_seen_ts = last_sync_ts

        offset = 0
        total_loaded = 0

        while True:
            data = fetch_api_data(offset=offset, last_sync_ts=last_sync_ts)
            if not data:
                break

            cleaned = []
            for row in data:
                year_val = row.get("year")
                if year_val in (None, ""):
                    tax_year = None
                else:
                    tax_year = int(float(year_val))

                # track most recent :updated_at from this page
                updated_raw = row.get(":updated_at")
                dt = parse_socrata_datetime(updated_raw)
                if dt and (max_seen_ts is None or dt > max_seen_ts):
                    max_seen_ts = dt

                cleaned.append({
                    "pin": row.get("pin"),
                    "tax_year": tax_year,
                    "class": row.get("class"),
                    "township_code": row.get("township_code"),
                    "case_no": row.get("case_no"),
                    "appeal_type": row.get("appeal_type"),
                    "status": row.get("status"),
                    "mailed_bldg": to_decimal(row.get("mailed_bldg")),
                    "mailed_land": to_decimal(row.get("mailed_land")),
                    "mailed_tot": to_decimal(row.get("mailed_tot")),
                    "certified_bldg": to_decimal(row.get("certified_bldg")),
                    "certified_land": to_decimal(row.get("certified_land")),
                    "certified_tot": to_decimal(row.get("certified_tot")),
                    "appeal_change": row.get("change"),
                    "reason_code1": row.get("reason_code1"),
                    "reason_desc1": row.get("reason_desc1"),
                    "reason_code2": row.get("reason_code2"),
                    "reason_desc2": row.get("reason_desc2"),
                    "reason_code3": row.get("reason_code3"),
                    "reason_desc3": row.get("reason_desc3"),
                    "agent_code": row.get("agent_code"),
                    "agent_name": row.get("agent_name"),
                    "row_id": row.get("row_id"),
                })

            try:
                cursor.executemany(insert_query, cleaned)
                conn.commit()
            except mysql.connector.Error as e:
                print("Batch failed, trying one by one to locate the bad row...")
                for i, r in enumerate(cleaned, 1):
                    try:
                        cursor.execute(insert_query, r)
                    except mysql.connector.Error as e2:
                        print(f"Row {i} failed with error: {e2}")
                        print("Failing row data:", r)
                        raise
                conn.commit()

            total_loaded += len(cleaned)
            print(f"Loaded {total_loaded} rows so far...")
            offset += 5000

        # update sync timestamp if we saw anything newer
        if max_seen_ts and max_seen_ts != last_sync_ts:
            update_last_sync_ts(cursor, max_seen_ts)
            conn.commit()
            print(f"Updated last sync time to {max_seen_ts.isoformat()}")

        print(f"Done. Total rows upserted this run: {total_loaded}")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
