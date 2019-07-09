import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
       copy data from json in s3 to staging tables in data warehouse. 
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
       load data from staging stables to fact table and dimension tables with necessary transformations. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
       Make connection to the data warehouse, copy data from json in s3 to staging tables in data warehouse, 
       and load data from staging stables to fact table and dimension tables with necessary transformations. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()