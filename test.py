import requests
import psycopg2
from psycopg2.extras import execute_batch

# PostgreSQL 연결
def connect_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="0000",
            database="postgres",
        )
        print("Database connected successfully!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# 테이블 생성
def create_table():
    create_query = """
    CREATE TABLE IF NOT EXISTS tb_cycle_station_info (
        sta_loc TEXT,
        rent_id TEXT PRIMARY KEY,
        rent_no TEXT,
        rent_nm TEXT,
        rent_id_nm TEXT,
        hold_num INTEGER,
        sta_add1 TEXT,
        sta_add2 TEXT,
        sta_lat DOUBLE PRECISION,
        sta_long DOUBLE PRECISION
    );
    """
    
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE tb_cycle_station_info CASCADE;")
            conn.commit
            print("deletesuccess")
            cursor.execute(create_query)
            conn.commit()
            print("Table created successfully!")
        except Exception as e:
            print(f"Table creation failed: {e}")
        finally:
            cursor.close()
            conn.close()

# API에서 데이터 가져오기
def fetch_data_from_api():
    api_url = "http://openapi.seoul.go.kr:8088/5457776d5073657938315043486944/json/tbCycleStationInfo/1/1000/"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()
        return data.get("stationInfo", {}).get("row", [])
    else:
        print(f"API Response Error: {response.status_code}")
        return []

# 데이터 삽입
def insert_data_in_batches(cursor, data, batch_size=500):
    insert_query = """
    INSERT INTO tb_cycle_station_info (
        sta_loc, rent_id, rent_no, rent_nm, rent_id_nm,
        hold_num, sta_add1, sta_add2, sta_lat, sta_long
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (rent_id) DO NOTHING;
    """
    
    # 데이터를 batch_size 만큼 나누어 삽입
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        execute_batch(cursor, insert_query, batch)
        print(f"Inserted batch {i//batch_size + 1} of {len(data)//batch_size + 1}")

# API에서 데이터 받아서 DB에 삽입
def fetch_and_store_data():
    try:
        # API에서 데이터 가져오기
        station_info = fetch_data_from_api()
        
        if not station_info:
            print("No data to insert.")
            return
        
        # 데이터 처리 및 준비
        processed_data = []
        for station in station_info:
            processed_data.append((
                station.get("STA_LOC", ""),
                station.get("RENT_ID", ""),
                station.get("RENT_NO", ""),
                station.get("RENT_NM", ""),
                station.get("RENT_ID_NM", ""),
                # "HOLD_NUM" 값을 int로 변환할 때 빈 문자열을 처리
                int(station.get("HOLD_NUM", 0)) if station.get("HOLD_NUM", '').strip() != '' else 0,
                station.get("STA_ADD1", ""),
                station.get("STA_ADD2", ""),
                float(station.get("STA_LAT", 0.0)) if station.get("STA_LAT", '').strip() != '' else 0.0,
                float(station.get("STA_LONG", 0.0)) if station.get("STA_LONG", '').strip() != '' else 0.0
            ))

        # 데이터베이스에 삽입
        conn = connect_db()
        if conn:
            cursor = conn.cursor()
            insert_data_in_batches(cursor, processed_data)
            conn.commit()
            print("Data inserted successfully!")
            cursor.close()
            conn.close()
    
    except Exception as e:
        print(f"Error: {e}")

# 프로그램 실행
if __name__ == "__main__":
    create_table()  # 테이블 생성
    fetch_and_store_data()  # API 데이터 가져오기 및 삽입
