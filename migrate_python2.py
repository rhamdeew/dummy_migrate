from contextlib import closing
from datetime import datetime
import pymysql
from pymysql.cursors import DictCursor
import psycopg2
from psycopg2 import sql

def get_mysql_tables(cursor, excluded_tables = []):
    query = "SHOW TABLES;"
    cursor.execute(query)
    tables = []
    for row in cursor:
        tableName = list(row.values())[0]
        if tableName not in excluded_tables:
            tables.append(list(row.values())[0])
    return tables


def get_mysql_table_timestamp_columns(cursor, table):
    query = "SHOW COLUMNS FROM {} WHERE Type='timestamp';".format(table)
    cursor.execute(query)
    columns = []
    for row in cursor:
        columns.append(row['Field'])
    return columns


def get_mysql_table_data(cursor, table):
    query = "SELECT * FROM {}".format(table)
    cursor.execute(query)
    data = []
    for row in cursor:
        data.append(row)
    return data


def insert_postgres_table_data(conn, cursor, table, data, timestamp_columns):
    conn.autocommit = True

    if len(data) is 0:
        return False

    keys = []
    for k in data[0].keys():
        keys.append(sql.Identifier(k))

    for item in data:
        values = []
        for (k, v) in item.items():
            if k in timestamp_columns:
                if v is None:
                    v = datetime(2199, 1, 1, 1, 1, 1)
                if type(v) is str:
                    v = datetime(2099, 1, 1, 1, 1, 1)
            values.append(sql.Literal(v))

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(keys),
            sql.SQL(', ').join(values)
        )
        # print query.as_string(conn)
        cursor.execute(query)


if __name__ == "__main__":
    print 'Migrating'
    print '=============='

    pg_conn = psycopg2.connect(dbname='db_name', user='postgres', password='superpass', host='postgres')
    pg_cursor = pg_conn.cursor()

    with closing(pymysql.connect(
        host='db',
        user='root',
        password='superpass',
        db='db_name',
        charset='utf8mb4',
        cursorclass=DictCursor)) as connection:
        with connection.cursor() as cursor:
            tables = get_mysql_tables(cursor, ['document_type', 'migrations'])
            print 'Getting MySQL tables'
            print '=============='
            print tables

            for table in tables:
                print 'Getting MySQL table "{}" timestamp columns'.format(table)
                print '=============='
                timestamp_columns = get_mysql_table_timestamp_columns(cursor, table)

                print 'Getting MySQL table "{}" data'.format(table)
                print '=============='
                data = get_mysql_table_data(cursor, table)

                print 'Insert PostgreSQL table "{}" data'.format(table)
                print '=============='
                insert_postgres_table_data(pg_conn, pg_cursor, table, data, timestamp_columns)
