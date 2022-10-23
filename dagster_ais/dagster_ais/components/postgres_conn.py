import psycopg2
import csv
import pandas as pd
from io import StringIO
from psycopg2 import extras
from sqlalchemy import create_engine
from dagster_ais.components.my_secrets import USERNAME, PASSWORD, DATABASE, HOST, PORT

class PostgresConn:
    def __init__(self):
        self.credentials = f"://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        self.engine = create_engine(f"postgresql{self.credentials}")
        self.connection = self.get_psql_conn()
        self.cursor = self.connection.cursor()

    def get_psql_conn(self):
        conn = psycopg2.connect(f"postgres{self.credentials}")
        return conn

    def execute_sql(self, command):
        try:
            self.cursor.execute(command)
            self.connection.commit()
        except (Exception, psycopg2.Error) as e:
            raise e

    async def complete_transaction(self, table_name, df, cursor):
        if len(df) > 0:
            df_cols  = list(df)
            # create (col1,col2,)
            columns = ",".join(df_cols)

            # create VALUES('%s', '%s',...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_cols]))

            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
            # cursor.execute(f"truncate {table_name};") # avoid duplicates

            extras.execute_batch(cursor, insert_stmt, df.values)
        self.connection.commit()

    async def df_to_postgres(self, df):
        df.to_sql('ais.combined', self.engine, method=self.psql_insert_copy, if_exists='append', index=False)
        pd.set_option('display.max_rows', 10)
        print(df)
        await self.complete_transaction("ais.combined", df, self.cursor)


    async def geodf_to_postgis(self, gdf):
        # df.to_sql('ais.combined', self.engine, method=self.psql_insert_copy, if_exists='append', index=False)
        self.execute_sql("CREATE SCHEMA IF NOT EXISTS ais;")
        # self.execute_sql("CREATE EXTENSION IF NOT EXISTS postgis;")
        gdf.to_postgis("trajectories", self.engine, schema='ais', if_exists='append', index=False)

    def psql_insert_copy(self, table, conn, keys, data_iter):
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)