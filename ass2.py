import psycopg2

def main():
    try:
        print("SQL Programming Test\n")

        # port, user, password, database는 각자의 환경에 맞게 입력
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

            print("Creating College, Student, Apply relations\n")
            # 3 개 Table 생성: Create table 문 이용
            cursor.execute("""
                CREATE TABLE College (
                    cName varchar(20),
                    state char(2),
                    enrollment int
                );
            """)
            cursor.execute("""
                CREATE TABLE Student (
                    sID int,
                    sName varchar(20),
                    GPA numeric(2,1),
                    sizeHS int
                );
            """)
            cursor.execute("""
                CREATE TABLE Apply (
                    sID int,
                    cName varchar(20),
                    major varchar(20),
                    decision char
                );
            """)

            print("Inserting tuples to College, Student, Apply relations\n")
            # 3 개 Table 에 Tuple 생성: Insert 문 이용
            cursor.execute("""
                INSERT INTO College (cName, state, enrollment) VALUES
                ('Stanford', 'CA', 15000),
                ('Berkeley', 'CA', 36000),
                ('MIT', 'MA', 10000),
                ('Cornell', 'NY', 21000);
            """)
            cursor.execute("""
                INSERT INTO Student (sID, sName, GPA, sizeHS) VALUES
                (123, 'Amy', 3.9, 1000),
                (234, 'Bob', 3.6, 1500),
                (345, 'Craig', 3.5, 500),
                (456, 'Doris', 3.9, 1000),
                (567, 'Edward', 2.9, 2000),
                (678, 'Fay', 3.8, 200),
                (789, 'Gary', 3.4, 800),
                (987, 'Helen', 3.7, 800),
                (876, 'Irene', 3.9, 400),
                (765, 'Jay', 2.9, 1500),
                (654, 'Amy', 3.9, 1000),
                (543, 'Craig', 3.4, 2000);
            """)
            cursor.execute("""
                INSERT INTO Apply (sID, cName, major, decision) VALUES
                (123, 'Stanford', 'CS', 'Y'),
                (123, 'Stanford', 'EE', 'N'),
                (123, 'Berkeley', 'CS', 'Y'),
                (123, 'Cornell', 'EE', 'Y'),
                (234, 'Berkeley', 'biology', 'N'),
                (345, 'MIT', 'bioengineering', 'Y'),
                (345, 'Cornell', 'bioengineering', 'N'),
                (345, 'Cornell', 'CS', 'Y'),
                (345, 'Cornell', 'EE', 'N'),
                (678, 'Stanford', 'history', 'Y'),
                (987, 'Stanford', 'CS', 'Y'),
                (987, 'Berkeley', 'CS', 'Y'),
                (876, 'Stanford', 'CS', 'N'),
                (876, 'MIT', 'biology', 'Y'),
                (876, 'MIT', 'marine biology', 'N'),
                (765, 'Stanford', 'history', 'Y'),
                (765, 'Cornell', 'history', 'N'),
                (765, 'Cornell', 'psychology', 'Y'),
                (543, 'MIT', 'CS', 'N');
            """)

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 1")
                # Query 1을 실행: Select문 이용
                '''
                // Query 처리 결과는 적절한 Print문을 이용해 Display
                // Tuple print시 Tuple 번호도 함께 print
                // 예) no cName state
                // 1 Stanford CA
                // 2 MIT MA
                '''
                cursor.execute("SELECT * FROM Student")
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"sID":<20}  {"sName":<20} {"GPA":<20}  {"sizeHS":<20}")

                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}  {row[3]:<20}")
                    #i:<20 : i값을 왼쪽 정렬하고 최소 2자리의 폭을 이용
                    #row[0]:<20 : row[0]값을 왼쪽 정렬하고 최소 20자리의 폭을 사용!! 

            '''
            // Query 2 ~ Query 5에 대해 Query 1과 같은 방식으로 실행: Select문 이용
            // Query 처리 결과는 적절한 Print문을 이용해 Display
            // Tuple print시 Tuple 번호도 함께 print(예시는 위 “Query 1” 참조)
            '''

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 2")
                # Query 2을 실행: Select문 이용
                '''
                // Query 처리 결과는 적절한 Print문을 이용해 Display
                // Tuple print시 Tuple 번호도 함께 print
                // 예) no cName state
                // 1 Stanford CA
                // 2 MIT MA
                '''
                cursor.execute("SELECT * FROM College")
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"cName":<20}  {"state":<20} {"enrollment":<20}")
                
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20}  {row[2]:<20}")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 3")
                # Query 3을 실행: Select문 이용
                
                cursor.execute("SELECT * FROM Apply")
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20}  {"decision":<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}  {row[3]:<20}")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 4")
                cursor.execute("""
                    SELECT DISTINCT cName
                    FROM Apply A1
                    WHERE 6 > (SELECT COUNT(*) FROM Apply A2 WHERE A2.cName = A1.cName);
                """)
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"cName":<20} ")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 5")
                cursor.execute("""
                    SELECT cName, major, MIN(GPA), MAX(GPA)
                    FROM Student, Apply
                    WHERE Student.sID = Apply.sID
                    GROUP BY cName, major
                    HAVING MIN(GPA) > 3.0
                    ORDER BY cName, major;
                """)
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"cName":<20} {"major":<20} {"min(GPA)":<20} {"max(GPA)":<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20} {row[1]:<20} {row[2]:<20} {row[3]:<20}")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 6")
                major = input("Enter a major (Enter 'CS')\n")
                university = input("Enter a university (Enter 'Stanford')\n")

                cursor.execute("""
                    SELECT sName, GPA
                    FROM Student NATURAL JOIN Apply
                    WHERE major = %s AND cName = %s;
                """, (major, university))
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"sName":<20} {"GPA":<20} ")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20} {row[1]:<20} ")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 7")
                major = input("Enter the major the students applied (Enter 'CS')\n")
                nonmajor = input("Enter the major the students did not applied (Enter 'EE')\n")

                cursor.execute("""
                    SELECT sID, sName
                    FROM Student
                    WHERE sID = ANY (SELECT sID FROM Apply WHERE major = %s)
                    AND NOT sID = ANY (SELECT sID FROM Apply WHERE major = %s);
                """, (major, nonmajor))
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"sID":<20} {"sName":<20} ")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20} {row[1]:<20} ")


    except psycopg2.Error as e:
        print("Connection failure.")
        raise e

if __name__ == "__main__":
    main()