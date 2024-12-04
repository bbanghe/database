import psycopg2

def main():
    try:
        print("SQL Programming Test\n")
        
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
    
    
    except psycopg2.Error as e:
        print("Connection failure.")
        raise e

if __name__ == "__main__":
    main()