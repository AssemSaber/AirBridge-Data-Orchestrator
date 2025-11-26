import csv
import mysql.connector

# -----------------------------
# CONFIG     python3 sparkJops/SIC_GP/import_mysql.py 
# -----------------------------

DB_HOST = "localhost"
DB_USER = "Assem"
DB_PASSWORD = "123456789"
DB_NAME = "GP"
TABLE_NAME = "flights"
CSV_FILE = "data/flight_data_2024_sample.csv"
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
            year BIGINT NOT NULL,
            month BIGINT NOT NULL,
            day_of_month BIGINT NOT NULL,
            day_of_week BIGINT NOT NULL,
            fl_date DATETIME NOT NULL,
            op_unique_carrier VARCHAR(20) NOT NULL,
            op_carrier_fl_num DOUBLE NOT NULL,
            origin VARCHAR(10) NOT NULL,
            origin_city_name VARCHAR(255) NOT NULL,
            origin_state_nm VARCHAR(255) NOT NULL,
            dest VARCHAR(10) NOT NULL,
            dest_city_name VARCHAR(255) NOT NULL,
            dest_state_nm VARCHAR(255) NOT NULL,
            crs_dep_time BIGINT NOT NULL,
            dep_time DOUBLE NULL,
            dep_delay DOUBLE NULL,
            taxi_out DOUBLE NULL,
            wheels_off DOUBLE NULL,
            wheels_on DOUBLE NULL,
            taxi_in DOUBLE NULL,
            crs_arr_time BIGINT NOT NULL,
            arr_time DOUBLE NULL,
            arr_delay DOUBLE NULL,
            cancelled INT NOT NULL,
            cancellation_code VARCHAR(10) NULL,
            diverted INT NOT NULL,
            crs_elapsed_time DOUBLE NOT NULL,
            actual_elapsed_time DOUBLE NULL,
            air_time DOUBLE NULL,
            distance DOUBLE NOT NULL,
            carrier_delay INT NOT NULL,
            weather_delay INT NOT NULL,
            nas_delay INT NOT NULL,
            security_delay INT NOT NULL,
            late_aircraft_delay INT NOT NULL
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

    # 7. Insert rows
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join([f"`{c}`" for c in columns])
    sql = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"
    cursor.executemany(sql, selected_rows)
    conn.commit()

    print(f"\n[SUCCESS] Imported {len(selected_rows)} rows into '{TABLE_NAME}'.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
