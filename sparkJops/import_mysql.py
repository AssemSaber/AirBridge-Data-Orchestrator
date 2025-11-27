import csv
import mysql.connector

# -----------------------------
# CONFIG     python3 sparkJops/import_mysql.py 
# -----------------------------

DB_HOST = "localhost"
DB_USER = "Assem"
DB_PASSWORD = "123456789"
DB_NAME = "GP"
TABLE_NAME = "flights"
CSV_FILE = "data/flight_data_2024.csv"
BATCH_SIZE = 5000  # Insert 5000 rows at a time
# -----------------------------

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def create_table(cursor):
    cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            year BIGINT   NULL,
            month BIGINT   NULL,
            day_of_month BIGINT   NULL,
            day_of_week BIGINT   NULL,
            fl_date DATETIME   NULL,
            op_unique_carrier VARCHAR(20)   NULL,
            op_carrier_fl_num DOUBLE   NULL,
            origin VARCHAR(10)   NULL,
            origin_city_name VARCHAR(255)   NULL,
            origin_state_nm VARCHAR(255)   NULL,
            dest VARCHAR(10)   NULL,
            dest_city_name VARCHAR(255)   NULL,
            dest_state_nm VARCHAR(255)   NULL,
            crs_dep_time BIGINT   NULL,
            dep_time DOUBLE NULL,
            dep_delay DOUBLE NULL,
            taxi_out DOUBLE NULL,
            wheels_off DOUBLE NULL,
            wheels_on DOUBLE NULL,
            taxi_in DOUBLE NULL,
            crs_arr_time BIGINT  NULL,
            arr_time DOUBLE NULL,
            arr_delay DOUBLE NULL,
            cancelled INT   NULL,
            cancellation_code VARCHAR(10) NULL,
            diverted INT   NULL,
            crs_elapsed_time DOUBLE  NULL,
            actual_elapsed_time DOUBLE NULL,
            air_time DOUBLE NULL,
            distance DOUBLE  NULL,
            carrier_delay INT  NULL,
            weather_delay INT  NULL,
            nas_delay INT  NULL,
            security_delay INT  NULL,
            late_aircraft_delay INT  NULL
        );
    """)

def get_columns(cursor):
    cursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME}")
    columns = [row[0] for row in cursor.fetchall() if row[0] != "id"]
    return columns

def clean_row(row):
    new_row = []
    for val in row:
        if val.strip() == "" or val.strip().upper() in ("NA", "N/A"):
            new_row.append(None)
        else:
            new_row.append(val)
    return new_row

def insert_in_batches(cursor, conn, sql, rows, batch_size):
    """Insert rows in batches to avoid max_allowed_packet error"""
    total_rows = len(rows)
    total_inserted = 0
    
    print(f"[INFO] Starting batch insertion of {total_rows} rows (batch size: {batch_size})...")
    
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            total_inserted += len(batch)
            
            # Progress indicator every 50,000 rows
            if total_inserted % 50000 == 0 or total_inserted == total_rows:
                print(f"[PROGRESS] {total_inserted}/{total_rows} rows inserted ({total_inserted/total_rows*100:.1f}%)")
        
        except Exception as e:
            print(f"[ERROR] Failed to insert batch starting at row {i}: {e}")
            conn.rollback()
            raise
    
    return total_inserted

def main():
    # 1. Connect to DB
    conn = connect_db()
    cursor = conn.cursor()

    # 2. Ask action
    print("Choose action:")
    print("1. Drop and insert (overwrite table)")
    print("2. Append (add to existing table)")

    while True:
        choice = input("Enter 1 or 2: ").strip()
        if choice in ("1", "2"):
            break
        print("Please enter 1 or 2.")

    # 3. Handle choice
    start_index = 0
    if choice == "1":
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.commit()
        print(f"[INFO] Table '{TABLE_NAME}' dropped successfully.")

        create_table(cursor)
        conn.commit()
        print(f"[INFO] Table '{TABLE_NAME}' created successfully.")
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        start_index = cursor.fetchone()[0]
        print(f"[INFO] Table '{TABLE_NAME}' has {start_index} existing rows.")

    # 4. Get columns from table
    columns = get_columns(cursor)
    print(f"[INFO] Table '{TABLE_NAME}' has {len(columns)} columns (excluding id).")

    # 5. Ask number of rows
    while True:
        try:
            n = int(input("How many rows do you want to import? "))
            if n > 0:
                break
            print("Enter a positive number.")
        except ValueError:
            print("Enter a valid integer.")

    # 6. Read CSV and clean rows
    print(f"[INFO] Reading CSV file...")
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        selected_rows = []

        for i, row in enumerate(reader):
            if i < start_index:  # Skip already inserted rows
                continue
            if len(selected_rows) >= n:
                break
            selected_rows.append(clean_row(row))
    
    print(f"[INFO] Finished reading {len(selected_rows)} rows from CSV.")

    # 7. Insert rows in batches
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join([f"`{c}`" for c in columns])
    sql = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"
    
    total_inserted = insert_in_batches(cursor, conn, sql, selected_rows, BATCH_SIZE)

    print(f"\n[SUCCESS] Imported {total_inserted} rows into '{TABLE_NAME}'.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()