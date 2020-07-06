import configparser
import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data from S3 buckets into Redshift according to the statements present
    in the copy_table_queries list.

    Args:
        cur: A psycopg2.cursor object.
        conn: A psycopg2.connection object.

    Raises:
        psycopg2.DatabaseError: Exception raised for errors that are related to
        the database.
    '''

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert data from the staging tables into the fact and dimension tables
    according to the statements present in the insert_table_queries list.

    Args:
        cur: A psycopg2.cursor object.
        conn: A psycopg2.connection object.

    Raises:
        psycopg2.DatabaseError: Exception raised for errors that are related to
        the database.
    '''

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
