import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ This function is to drop if exists and then create the database Sparkify."""
    try:
        print ('Function start create_database')
        # connect to default database
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

        # close connection to default database
        conn.close()    

        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        print ('Function end create_database')
        return cur, conn
    
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()

def drop_tables(cur, conn):
    """With this functions all the tables (Songplays, Songs, Users, Artists, Time)are dropped."""
    try:
        print ('Function start drop_tables')
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()

        print ('Function end drop_tables')
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()

def create_tables(cur, conn):
    """With this functions all the tables (Songplays, Songs, Users, Artists, Time)are created."""
    try:
        print ('Function start create_tables')
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print ('Function end create_tables')
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()
        

def main():
    """This functions calls create_database, drop_tables, create_tables functions."""
    try:
        cur, conn = create_database()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()

if __name__ == "__main__":
    main()