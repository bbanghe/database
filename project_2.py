import requests
import psycopg2

# API URLs
url1 = 'http://openapi.seoul.go.kr:8088/5457776d5073657938315043486944/json/tbCycleStationInfo/1/5/' #대여소 정보
url2 = 'http://openapi.seoul.go.kr:8088/4b597a696d7365793832456245614a/json/bikeList/1/5/' #실시간 대여 정보


def fetch_data(api_url):
    """API 호출 및 JSON 데이터 반환"""
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print("success\n")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API 호출 실패: {e}")
        return None

def save_to_database(conn, stations, bike_status_list, station_locations):
    """데이터베이스에 데이터를 저장"""
    try:
        with conn.cursor() as cur:
            # 테이블 생성
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Station (
                    stationID VARCHAR PRIMARY KEY,
                    stationName VARCHAR,
                    rackTotCnt INTEGER
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS BikeStatus (
                    stationID VARCHAR PRIMARY KEY,
                    parkingBikeTotCnt INTEGER,
                    shared INTEGER
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS StationLocation (
                    stationID VARCHAR PRIMARY KEY,
                    STA_LOC VARCHAR,
                    STA_ADD1 VARCHAR,
                    STA_ADD2 VARCHAR,
                    STA_LAT NUMERIC,
                    STA_LONG NUMERIC
                );
            """)

            # 데이터 삽입
            for s in stations:
                cur.execute("""
                    INSERT INTO Station (stationID, stationName, rackTotCnt)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stationID) DO NOTHING;
                """, (s['stationID'], s['stationName'], s['rackTotCnt']))

            for b in bike_status_list:
                cur.execute("""
                    INSERT INTO BikeStatus (stationID, parkingBikeTotCnt, shared)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stationID) DO NOTHING;
                """, (b['stationID'], b['parkingBikeTotCnt'], b['shared']))

            for loc in station_locations:
                cur.execute("""
                    INSERT INTO StationLocation (stationID, STA_LOC, STA_ADD1, STA_ADD2, STA_LAT, STA_LONG)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stationID) DO NOTHING;
                """, (loc['stationID'], loc['STA_LOC'], loc['STA_ADD1'], loc['STA_ADD2'], loc['STA_LAT'], loc['STA_LONG']))

            conn.commit()
            print("데이터베이스 저장 완료!")
    except Exception as e:
        print(f"데이터베이스 작업 중 오류 발생: {e}")
        conn.rollback()

def main():
    # 데이터 가져오기
    station_info = fetch_data(url1)
    bike_status = fetch_data(url2)
    print("성공")

    

    if station_info and bike_status:
        station_data = station_info['tbCycleStationInfo']['row']
        bike_data = bike_status['bikeList']['row']

        # 데이터 분류
        stations = [
            {
                "stationID": item['RENT_ID'],
                "stationName": item['RENT_NM'],
                "rackTotCnt": int(item['HOLD_NUM'])
            }
            for item in station_data
        ]

        bike_status_list = [
            {
                "stationID": item['stationId'],
                "parkingBikeTotCnt": int(item['parkingBikeTotCnt']),
                "shared": int(item['shared'])
            }
            for item in bike_data
        ]

        station_locations = [
            {
                "stationID": item['RENT_ID'],
                "STA_LOC": item['STA_LOC'],
                "STA_ADD1": item['STA_ADD1'],
                "STA_ADD2": item['STA_ADD2'],
                "STA_LAT": float(item['STA_LAT']),
                "STA_LONG": float(item['STA_LONG'])
            }
            for item in station_data
        ]

        # PostgreSQL 데이터베이스에 연결
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                user="postgres",
                password="0000",
                database="postgres"
            )
            print("데이터베이스에 연결 성공!")
            
            # 데이터 저장
            save_to_database(conn, stations, bike_status_list, station_locations)

        except psycopg2.Error as e:
            print(f"데이터베이스 연결 실패: {e}")
        finally:
            if conn:
                conn.close()
    else:
        print("API 데이터를 가져오지 못했습니다.")
