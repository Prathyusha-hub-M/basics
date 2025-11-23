import requests
import mysql.connector
from mysql.connector import errorcode

# ------------- CONFIGURATION -------------

API_BASE = "https://datacatalog.cookcountyil.gov/resource/y282-6ig3.json"

# Optional Socrata app token (recommended if you will hit the API often)
APP_TOKEN = ""  # put your token here or leave blank

MYSQL_CONFIG = {
    "user": "root",            # your MySQL username
    "password": "your_password",  # your MySQL password
    "host": "127.0.0.1",       # or your server host
    "port": 3306,
    "database": "cook_county", # must already exist
}

TABLE_NAME = "assessor_appeals"

PAGE_SIZE = 50000  # Socrata v2.0 max per page is 50,000

# ------------- HELPERS -------------

def parse_int(value):
    """Convert value to int or return None."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

# ------------- MAIN LOGIC -------------

def main():
    # Connect to MySQL
    try:
        cnx = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = cnx.cursor()
        print("Connected to MySQL.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Wrong MySQL user or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Database does not exist")
        else:
            print("MySQL error:", err)
        return

    # Make sure the table exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        row_id          VARCHAR(64) PRIMARY KEY,
        pin             VARCHAR(20),
        year            INT,
        class           VARCHAR(10),
        township_code   VARCHAR(10),
        case_no         VARCHAR(50),
        appeal_type     VARCHAR(100),
        status          VARCHAR(50),
        mailed_bldg     INT,
        mailed_land     INT,
        mailed_tot      INT,
        certified_bldg  INT,
        certified_land  INT,
        certified_tot   INT,
        change_amt      INT,
        reason_code1    VARCHAR(20),
        reason_desc1    VARCHAR(255),
        reason_code2    VARCHAR(20),
        reason_desc2    VARCHAR(255),
        reason_code3    VARCHAR(20),
        reason_desc3    VARCHAR(255),
        agent_code      VARCHAR(50),
        agent_name      VARCHAR(255),
        INDEX idx_pin_year (pin, year),
        INDEX idx_township_year (township_code, year)
    );
    """
    cursor.execute(create_table_sql)
    cnx.commit()
    print(f"Ensured table {TABLE_NAME} exists.")

    # Prepare insert statement
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        row_id,
        pin,
        year,
        class,
        township_code,
        case_no,
        appeal_type,
        status,
        mailed_bldg,
        mailed_land,
        mailed_tot,
        certified_bldg,
        certified_land,
        certified_tot,
        change_amt,
        reason_code1,
        reason_desc1,
        reason_code2,
        reason_desc2,
        reason_code3,
        reason_desc3,
        agent_code,
        agent_name
    ) VALUES (
        %(row_id)s,
        %(pin)s,
        %(year)s,
        %(class)s,
        %(township_code)s,
        %(case_no)s,
        %(appeal_type)s,
        %(status)s,
        %(mailed_bldg)s,
        %(mailed_land)s,
        %(mailed_tot)s,
        %(certified_bldg)s,
        %(certified_land)s,
        %(certified_tot)s,
        %(change_amt)s,
        %(reason_code1)s,
        %(reason_desc1)s,
        %(reason_code2)s,
        %(reason_desc2)s,
        %(reason_code3)s,
        %(reason_desc3)s,
        %(agent_code)s,
        %(agent_name)s
    )
    ON DUPLICATE KEY UPDATE
        pin = VALUES(pin),
        year = VALUES(year),
        class = VALUES(class),
        township_code = VALUES(township_code),
        case_no = VALUES(case_no),
        appeal_type = VALUES(appeal_type),
        status = VALUES(status),
        mailed_bldg = VALUES(mailed_bldg),
        mailed_land = VALUES(mailed_land),
        mailed_tot = VALUES(mailed_tot),
        certified_bldg = VALUES(certified_bldg),
        certified_land = VALUES(certified_land),
        certified_tot = VALUES(certified_tot),
        change_amt = VALUES(change_amt),
        reason_code1 = VALUES(reason_code1),
        reason_desc1 = VALUES(reason_desc1),
        reason_code2 = VALUES(reason_code2),
        reason_desc2 = VALUES(reason_desc2),
        reason_code3 = VALUES(reason_code3),
        reason_desc3 = VALUES(reason_desc3),
        agent_code = VALUES(agent_code),
        agent_name = VALUES(agent_name);
    """

    # HTTP session
    session = requests.Session()
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    offset = 0
    total_inserted = 0

    while True:
        params = {
            "$select": (
                "pin,year,class,township_code,case_no,appeal_type,status,"
                "mailed_bldg,mailed_land,mailed_tot,"
                "certified_bldg,certified_land,certified_tot,"
                "change,"
                "reason_code1,reason_desc1,"
                "reason_code2,reason_desc2,"
                "reason_code3,reason_desc3,"
                "agent_code,agent_name,row_id"
            ),
            "$where": "year > 2022",
            "$order": "year ASC NULL LAST",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        print(f"Requesting page with offset {offset} ...")
        resp = session.get(API_BASE, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            print("No more rows returned. Done.")
            break

        records = []
        for rec in data:
            mapped = {
                "row_id": rec.get("row_id"),
                "pin": rec.get("pin"),
                "year": parse_int(rec.get("year")),
                "class": rec.get("class"),
                "township_code": rec.get("township_code"),
                "case_no": rec.get("case_no"),
                "appeal_type": rec.get("appeal_type"),
                "status": rec.get("status"),
                "mailed_bldg": parse_int(rec.get("mailed_bldg")),
                "mailed_land": parse_int(rec.get("mailed_land")),
                "mailed_tot": parse_int(rec.get("mailed_tot")),
                "certified_bldg": parse_int(rec.get("certified_bldg")),
                "certified_land": parse_int(rec.get("certified_land")),
                "certified_tot": parse_int(rec.get("certified_tot")),
                "change_amt": parse_int(rec.get("change")),
                "reason_code1": rec.get("reason_code1"),
                "reason_desc1": rec.get("reason_desc1"),
                "reason_code2": rec.get("reason_code2"),
                "reason_desc2": rec.get("reason_desc2"),
                "reason_code3": rec.get("reason_code3"),
                "reason_desc3": rec.get("reason_desc3"),
                "agent_code": rec.get("agent_code"),
                "agent_name": rec.get("agent_name"),
            }
            records.append(mapped)

        cursor.executemany(insert_sql, records)
        cnx.commit()

        batch_count = len(records)
        total_inserted += batch_count
        print(f"Inserted or updated {batch_count} rows in this batch, total {total_inserted}.")

        offset += PAGE_SIZE

    cursor.close()
    cnx.close()
    print("Finished loading data into MySQL.")

if __name__ == "__main__":
    main()
