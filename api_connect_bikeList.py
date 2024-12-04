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
    CREATE TABLE IF NOT EXISTS bikeList (
        rackTotCnt INTEGER,
        stationName TEXT,
        parkingBikeTotCnt INTEGER,     
        shared INTEGER,     
        stationLatitude DOUBLE PRECISION,
        stationLongitude DOUBLE PRECISION,
        stationId TEXT PRIMARY KEY
    );
    """
    
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(create_query)
            conn.commit()
            print("Table created successfully!")
        except Exception as e:
            print(f"Table creation failed: {e}")    
        finally:
            cursor.close()
            conn.close()

# API에서 데이터를 분할하여 가져오기
def fetch_data_from_api(start, end):
    api_url = f"http://openapi.seoul.go.kr:8088/4b597a696d7365793832456245614a/json/bikeList/{start}/{end}/"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()
        return data.get("rentBikeStatus", {}).get("row", [])
    else:
        print(f"API Response Error: {response.status_code}")
        return []

# API 데이터를 반복적으로 호출하여 전체 데이터 가져오기
def fetch_all_data_from_api(total_count, batch_size=1000):
    all_data = []
    for start in range(1, total_count + 1, batch_size):
        end = min(start + batch_size - 1, total_count)  # 마지막 범위 조정
        print(f"Fetching data from {start} to {end}...")
        data = fetch_data_from_api(start, end)
        all_data.extend(data)
    return all_data

# 데이터 삽입
def insert_data_in_batches(cursor, data, batch_size=500):
    insert_query = """
    INSERT INTO bikeList (
        rackTotCnt, stationName, parkingBikeTotCnt, shared, stationLatitude,
        stationLongitude, stationId  
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stationId) DO NOTHING;
    """
    
    # 데이터를 batch_size 만큼 나누어 삽입
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        execute_batch(cursor, insert_query, batch)
        print(f"Inserted batch {i//batch_size + 1} of {len(data)//batch_size + 1}")

# API에서 데이터 받아서 DB에 삽입
def fetch_and_store_data():
    try:
        # 전체 데이터 개수 설정 (예: 3143개)
        total_count = 3143
        batch_size = 500  # 한 번에 가져올 데이터 크기

        # API에서 모든 데이터 가져오기
        station_info = fetch_all_data_from_api(total_count, batch_size)
        
        if not station_info:
            print("No data to insert.")
            return
        
        # 데이터 처리 및 준비
        processed_data = []
        for station in station_info:
            processed_data.append((
                station.get("rackTotCnt", ""),
                station.get("stationName", ""),
                int(station.get("parkingBikeTotCnt", 0)) if station.get("parkingBikeTotCnt", '').strip() != '' else 0,
                int(station.get("shared", 0)) if station.get("shared", '').strip() != '' else 0,
                float(station.get("stationLatitude", 0.0)) if station.get("stationLatitude", '').strip() != '' else 0.0,
                float(station.get("stationLongitude", 0.0)) if station.get("stationLongitude", '').strip() != '' else 0.0,
                station.get("stationId", "")
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