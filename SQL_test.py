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

    # 커서 생성
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            #test - 광진구에 있는 station 개수
            cursor.execute("""
            select count(*)
            from stationlocation
            where sta_loc = '광진구'
            """)
            conn.commit()
            rows = cursor.fetchall()
            print("광진구에 있는 station의 개수")
            print(f"{"no":<20}  {"count(*)":<20} ")
            for i, row in enumerate(rows, start=1):
                print(f"{i:<20}  {row[0]:<20} ")

        except Exception as e:
            print(f"Table creation failed: {e}")    
        finally:
            cursor.close()
            conn.close()


    print("테이블이 성공적으로 생성되었습니다.")
    
if __name__ == "__main__":
    main()