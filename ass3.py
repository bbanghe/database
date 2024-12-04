import psycopg2

def main():
    try:
        print("SQL Programming Test\n")

        # PostgreSQL 데이터베이스에 연결g
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="0000",
            database="postgres",
        )
        print("Connecting PostgreSQL database\n")

        with conn:
            cursor = conn.cursor()

            cursor.execute("""
                DROP TABLE IF EXISTS Apply, Student, College CASCADE;
            """)


            print("Creating College, Student, Apply relations\n")
            # 3 개 Table 생성: Create table 문 이용
            cursor.execute(
                """
                CREATE TABLE College (
                    cName varchar(20),
                    state char(2),
                    enrollment int
                );
            """
            )
            cursor.execute(
                """
                CREATE TABLE Student (
                    sID int,
                    sName varchar(20),
                    GPA numeric(2,1),
                    sizeHS int
                );
            """
            )
            cursor.execute(
                """
                CREATE TABLE Apply (
                    sID int,
                    cName varchar(20),
                    major varchar(20),
                    decision char
                );
            """
            )

            print("Inserting tuples to College, Student, Apply relations\n")
            # 3 개 Table 에 Tuple 생성: Insert 문 이용
            cursor.execute(
                """
                INSERT INTO College (cName, state, enrollment) VALUES
                ('Stanford', 'CA', 15000),
                ('Berkeley', 'CA', 36000),
                ('MIT', 'MA', 10000),
                ('Cornell', 'NY', 21000);
            """
            )
            cursor.execute(
                """
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
            """
            )
            cursor.execute(
                """
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
            """
            )

            print("\nTrigger test 1")

            # Trigger R3 생성
            cursor.execute(
                """
                create or replace function trigger3()
                returns trigger as $$
                begin
                    update College 
                    set cName = New.cName
                    where cName = old.cName;
                    return New;
                end;
                $$
                language 'plpgsql';
                
                create trigger R3
                after update of cName on College
                for each row
                execute procedure trigger3();
            """
            )

            # 2개의 update문 완료
            cursor.execute("update College set cName = 'Sford' where cName = 'Stanford';")
            cursor.execute("update College set cName = 'Bkeley' where cName = 'Berkeley';")

            print("\nQuery 1")
            cursor.execute("select * from College order by cName;")
            rows = cursor.fetchall()
            print(f"{"no":<20}  {"cName":<20}  {"state":<20} {"enrollment":<20}")
            for i, row in enumerate(rows, start=1):
                print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}")

            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nQuery 2")
                cursor.execute("select * from Apply order by sID, cName, major;")
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20}  {"decision":<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}  {row[3]:<20}")
                
            # Trigger R5 생성
            flag = int(input("Continue? (Enter 1 for continue)\n"))
            if flag == 1:
                print("\nTrigger test 2")
                cursor.execute(
                    """
                    create or replace function trigger5()
                    returns trigger as $$
                    begin
                        update College 
                        set cName = New.cName
                        where cName = old.cName;
                        return New;
                    end;
                    $$
                    language 'plpgsql';
                    
                    create trigger R5
                    after update of cName on College
                    for each row
                    execute procedure trigger5();
                """
                )

                # 2개의 update문 완료
                cursor.execute("update College set cName = 'Berkeley' where cName = 'Berkey';")
                cursor.execute("update College set cName = 'Stanford' where cName = 'Sford';")


                print("\nQuery 3")
                cursor.execute("select * from College order by cName;")
                rows = cursor.fetchall()
                print(f"{"no":<20}  {"cName":<20}  {"state":<20} {"enrollment":<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}")

                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    print("\nQuery 4")
                    cursor.execute("select * from Apply order by sID, cName, major;")
                    rows = cursor.fetchall()
                    print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20}  {"decision":<20}")
                    for i, row in enumerate(rows, start=1):
                        print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}  {row[3]:<20}")

                # View CSEE 생성
                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    print("\nView test 1")
                    cursor.execute("""
                        create or replace view CSEE as
                        select sId, cName, major
                        from Apply
                        where major = 'CS' or major = 'EE'
                    """)
                    conn.commit()

                print("\nQuery 5")
                cursor.execute("SELECT * FROM CSEE ORDER BY sID, cName, major;")
                rows = cursor.fetchall()  # CSEE 뷰의 결과를 가져옴
                print(f"{'no':<20}  {'sID':<20}  {'cName':<20} {'major':<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}")

                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    # Trigger CSEEinsert 생성
                    print("\nView test 2")
                    cursor.execute("""
                    create or replace function CSEEinsert() 
                    returns trigger as $$
                    begin
                        if New.major != 'CS' and New.major != 'EE' then
                            return NULL; 
                        end if;
                        
                        if not exists (select * from Apply where sID = NEW.sID and cName = NEW.cName and major = NEW.major) then
                            insert into Apply values (NEW.sID, NEW.cName, NEW.major, null);
                        end if;
                    return NEW;
                    end;
                    $$ language 'plpgsql';

                    drop trigger if exists CSEEinsert on CSEE;
                    create trigger CSEEinsert
                    instead of insert on CSEE
                    for each row
                    execute procedure CSEEinsert();
                    """)
                    conn.commit()

                    # Insert into CSEE
                    insert_query = "insert into CSEE (sID, cName, major) VALUES (%s, %s, %s);"
                    cursor.execute(insert_query, (777, 'Brown', 'CS'))
                    conn.commit()

                print("\nQuery 6")
                cursor.execute("select * from CSEE order by sID, cName, major;")
                rows = cursor.fetchall()  # 최신 데이터를 다시 가져옴
                print(f"{'no':<20}  {'sID':<20}  {'cName':<20} {'major':<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}")


                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    print("\nQuery 7")
                    cursor.execute("select * from Apply order by sID, cName, major;")
                    rows = cursor.fetchall()
                    print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20} {"decision":<20}")
                    for i, row in enumerate(rows, start=1):
                        row_list = list(row)
                        if row_list[3] == None:
                            row_list[3] = 'null'
                        print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20} {row_list[3]:<20}")

                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    #Insert문 실행 
                    print("\nView test 3")
                    insert_query = "insert into CSEE (sID, cName, major) VALUES (%s, %s, %s);"
                    cursor.execute(insert_query, (777, 'Brown', 'psychology'))

                print("\nQuery 8")
                cursor.execute("select * from CSEE order by sID, cName, major;")
                rows = cursor.fetchall()  # 최신 데이터를 다시 가져옴
                print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20}")
                for i, row in enumerate(rows, start=1):
                    print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20}")

                flag = int(input("Continue? (Enter 1 for continue)\n"))
                if flag == 1:
                    print("\nQuery 9")
                    cursor.execute("select * from Apply order by sID, cName, major;")
                    rows = cursor.fetchall()  # 최신 데이터를 다시 가져옴
                    print(f"{"no":<20}  {"sID":<20}  {"cName":<20} {"major":<20}{'decision':<20}")
                    for i, row in enumerate(rows, start=1):
                        row_list = list(row)
                        if row_list[3] == None:
                            row_list[3] = 'null'
                        print(f"{i:<20}  {row[0]:<20}  {row[1]:<20} {row[2]:<20} {row_list[3]:<20}")

    except psycopg2.Error as e:
        print("Connection failure.")
        raise e


if __name__ == "__main__":
    main()
