import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function will load staging tables from files"""
    try:
        print ('Function start load_staging_tables')
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
        print ('Function end load_staging_tables')
        
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()


def insert_tables(cur, conn):
    """This function will insert data into dimension and fact tables from staging tables"""
    try:
        print ('Function start insert_tables')
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
        print ('Function end insert_tables')
        
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()

def main():
    """This function calls load_staging_tables and insert_tables"""
    try:
        print ('Main function is running')
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        
        conn.close()
        print ('Main function ran successfully.')

    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()

if __name__ == "__main__":
    main()