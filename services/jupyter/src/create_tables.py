import configparser
import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop all Redshift tables present in the drop_table_queries list.

    Args:
        cur: A psycopg2.cursor object.
        conn: A psycopg2.connection object.

    Raises:
        psycopg2.DatabaseError: Exception raised for errors that are related to
        the database.
    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create the Redshift tables present in the create_table_queries list.

    Args:
        cur: A psycopg2.cursor object.
        conn: A psycopg2.connection object.

    Raises:
        psycopg2.DatabaseError: Exception raised for errors that are related to
        the database.
    '''

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
