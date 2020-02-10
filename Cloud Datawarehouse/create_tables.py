import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    "With this function al the tables are dropped if they are already exist in database"
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
    """With this functions all the tables (staging_events,staging_songs,Songplays, Songs, Users, Artists, Time)are created."""
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
            print('Main function is running')
            config = configparser.ConfigParser()
            config.read('dwh.cfg')

            conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
            cur = conn.cursor()

            drop_tables(cur, conn)
            create_tables(cur, conn)

            conn.close()
            print ('Main function ran successfully.')
        
    except psycopg2.Error as e:
            print("error")
            print(e)
            conn.rollback()

if __name__ == "__main__":
    main()