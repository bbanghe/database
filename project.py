import psycopg2
import requests

#혼잡율 url
url1 = 'http://openapi.seoul.go.kr:8088/5457776d5073657938315043486944/json/tbCycleStationInfo/1/5/'
#정보 url
url2 = 'http://openapi.seoul.go.kr:8088/4b597a696d7365793832456245614a/json/bikeList/1/5/'

def create_table(cursor):
    # 테이블 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Station (
            stationId SERIAL PRIMARY KEY,
            stationName VARCHAR(255) NOT NULL,
            rackTotCnt INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS BikeStatus (
            statusId SERIAL PRIMARY KEY,
            stationId INTEGER REFERENCES Station(stationId) ON DELETE CASCADE,
            parkingBikeTotCnt INTEGER NOT NULL,
            Shared FLOAT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS StationLoc (
            locId SERIAL PRIMARY KEY,
            stationId INTEGER REFERENCES Station(stationId) ON DELETE CASCADE,
            stationLatitude FLOAT NOT NULL,
            stationLongitude FLOAT NOT NULL
        );
    """)

def parse_and_insert_data(conn, data):
    stations = {}
    bike_statuses = []
    station_locs = []

    # JSON 데이터 구조에 따라 파싱
    rows = data["tbCycleStationInfo"]["row"]  # API에서 row 데이터 접근
    for item in rows:
        station_id = item["RENT_ID"]
        station_name = item["RENT_NM"]
        rack_tot_cnt = int(item["HOLD_NUM"])  # 숫자로 변환

        # Station 데이터 생성
        if station_id not in stations:
            stations[station_id] = (station_name, rack_tot_cnt)

        # BikeStatus 데이터 생성
        bike_statuses.append({
            "stationId": station_id,
            "parkingBikeTotCnt": int(item.get("PARKING_BIKE_CNT", 0)),
            "Shared": float(item.get("SHARED", 0.0))
        })

        # StationLoc 데이터 생성
        station_locs.append({
            "stationId": station_id,
            "latitude": float(item["STA_LAT"]),
            "longitude": float(item["STA_LONG"])
        })

    cursor = conn.cursor()

    # Station 테이블에 삽입
    for station_id, (name, rack_cnt) in stations.items():
        cursor.execute("""
            INSERT INTO Station (stationName, rackTotCnt)
            VALUES (%s, %s)
            RETURNING stationId
        """, (name, rack_cnt))
        new_station_id = cursor.fetchone()[0]

        # BikeStatus 및 StationLoc의 stationId 업데이트
        for bike_status in bike_statuses:
            if bike_status["stationId"] == station_id:
                bike_status["stationId"] = new_station_id

        for loc in station_locs:
            if loc["stationId"] == station_id:
                loc["stationId"] = new_station_id

    # BikeStatus 테이블에 삽입
    for bike_status in bike_statuses:
        cursor.execute("""
            INSERT INTO BikeStatus (stationId, parkingBikeTotCnt, Shared)
            VALUES (%s, %s, %s)
        """, (bike_status["stationId"], bike_status["parkingBikeTotCnt"], bike_status["Shared"]))

    # StationLoc 테이블에 삽입
    for loc in station_locs:
        cursor.execute("""
            INSERT INTO StationLoc (stationId, stationLatitude, stationLongitude)
            VALUES (%s, %s, %s)
        """, (loc["stationId"], loc["latitude"], loc["longitude"]))

    conn.commit()
    cursor.close()

def main():
    try:
        # API 호출
        response = requests.get(url1)
        data = response.json()

        # PostgreSQL 데이터베이스에 연결
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="0000",
            database="postgres"
        )
        print("Connecting PostgreSQL database\n")

        with conn:
            cursor = conn.cursor()
            create_table(cursor)  # 테이블 생성
            parse_and_insert_data(conn, data)  # 데이터 삽입

            cursor.execute("""
                select * from Station
            """)
            print(f"{"no":<20}  {"a":<20} ")
            for i, row in enumerate(rows, start=1):
                print(f"{i:<20}  {row[0]:<20}")

        print("Data inserted successfully.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except requests.RequestException as e:
        print(f"API request error: {e}")
    finally:
        if conn:
            conn.close()  # 연결 해제

if __name__ == "__main__":
    main()
