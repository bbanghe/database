import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql

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

def main():
    # 1. 대여소 정보 테이블 생성 (Station)
    create_station_table_query = """
    CREATE TABLE IF NOT EXISTS Station AS
    SELECT A.RENT_ID AS stationID,
        A.RENT_NM AS stationName,
        B.rackTotCnt
    FROM tb_cycle_station_info A
    JOIN bikelist B ON A.RENT_ID = B.stationId;
    """

    # 2. 대여소 별 자전거 현황 테이블 생성 (BikeStatus)
    create_bikestatus_table_query = """
    CREATE TABLE IF NOT EXISTS BikeStatus AS
    SELECT A.RENT_ID AS stationID,
        B.parkingBikeTotCnt,
        B.shared
    FROM tb_cycle_station_info A
    JOIN bikelist B ON A.RENT_ID = B.stationId;
    """

    # 3. 대여소 위치 정보 테이블 생성 (StationLocation)
    create_stationlocation_table_query = """
    CREATE TABLE IF NOT EXISTS StationLocation AS
    SELECT A.RENT_ID AS stationID,
        A.STA_LOC,
        A.STA_ADD1,
        A.STA_ADD2,
        A.STA_LAT,
        A.STA_LONG
    FROM tb_cycle_station_info A;
    """

    insert_station_table_query = """
        INSERT INTO Station (stationID, stationName, rackTotCnt)
        SELECT RENT_ID, RENT_NM, HOLD_NUM
        FROM tb_cycle_station_info
    """

    insert_bikestatus_table_query = """
        INSERT INTO bikestatus (stationID, parkingBikeTotCnt, shared)
        SELECT stationId, parkingBikeTotCnt, shared
        FROM bikeList
    """

    insert_stationlocation_table_query = """
        INSERT INTO stationlocation (stationID, STA_LOC, STA_ADD1, STA_ADD2, STA_LAT, STA_LONG)
        SELECT RENT_ID, STA_LOC, STA_ADD1, STA_ADD2, STA_LAT, STA_LONG
        FROM tb_cycle_station_info
    """


    # PostgreSQL 연결
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            # 테이블 생성 쿼리 실행
            cursor.execute(create_station_table_query)
            cursor.execute(create_bikestatus_table_query)
            cursor.execute(create_stationlocation_table_query)

            
            # 커밋
            conn.commit()
            print("Tables created successfully!")

            #insert
            cursor.execute(insert_station_table_query)
            cursor.execute(insert_bikestatus_table_query)
            cursor.execute(insert_stationlocation_table_query)

            conn.commit()
        except Exception as e:
            print(f"Table creation failed: {e}")
        finally:
            # 커서 및 연결 종료
            cursor.close()
            conn.close()

    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()
