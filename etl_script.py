import os
import re

import pandas as pd
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder.appName('Squirrel Census ETL').getOrCreate()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['POSTGRES_PASSWORD']


def header_to_snake_case(name):
    name = re.sub(r'[^\w\s]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = name.lower()
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def process_csv(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    for old_name in df.columns:
        new_name = header_to_snake_case(old_name)
        df = df.withColumnRenamed(old_name, new_name)

    if 'park_id' in df.columns:
        df = df.withColumn('park_id', df['park_id'].cast('integer'))

    return df


def load_to_db(df, table_name):
    pandas_df = df.toPandas()

    print(f"Columns in {table_name} DataFrame:")
    print(pandas_df.columns.tolist())

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    columns = [f'"{col}" VARCHAR(255)' for col in pandas_df.columns]
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join(columns)}
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    columns = ', '.join([f'"{col}"' for col in pandas_df.columns])
    placeholders = ', '.join(['%s' for _ in pandas_df.columns])
    conflict_update = ', '.join(
        [f'"{col}" = EXCLUDED."{col}"' for col in pandas_df.columns])

    upsert_query = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT (id)
    DO UPDATE SET
        {conflict_update}
    """

    for _, row in pandas_df.iterrows():
        cursor.execute(upsert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()


def run_queries():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    with open('/opt/spark-apps/queries.sql', 'r') as file:
        queries = file.read().split(';')
    for query in queries:
        query = query.strip()
        if query:
            print(f'\nExecuting query:')
            print(query)
            print('-' * 40)
            df = pd.read_sql_query(query, conn)
            print(df.to_string(index=False))
            print('\n')
    conn.close()


if __name__ == '__main__':
    park_data_path = '/opt/spark-apps/data/park-data.csv'
    squirrel_data_path = '/opt/spark-apps/data/squirrel-data.csv'

    park_df = process_csv(park_data_path)
    squirrel_df = process_csv(squirrel_data_path)

    print("Columns in park_df:")
    print(park_df.columns)
    print("\nColumns in squirrel_df:")
    print(squirrel_df.columns)

    load_to_db(park_df, 'parks')
    load_to_db(squirrel_df, 'squirrels')

    print("ETL process completed successfully.")
    print('Running optimized queries:')
    run_queries()
