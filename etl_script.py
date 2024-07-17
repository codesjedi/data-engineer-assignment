import os
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder.appName('Squirrel Census ETL').getOrCreate()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['POSTGRES_PASSWORD']


def process_csv(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
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

    if table_name == 'parks':
        unique_columns = ['Park ID'] if 'Park ID' in pandas_df.columns else ['Park Name']
    elif table_name == 'squirrels':
        unique_columns = ['Park ID',
                          'Squirrel ID'] if 'Park ID' in pandas_df.columns and 'Squirrel ID' in pandas_df.columns else [
            'Park Name', 'Squirrel ID']
    else:
        raise ValueError(f"Unknown table name: {table_name}")

    missing_columns = [col for col in unique_columns if col not in pandas_df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns for unique identifier: {', '.join(missing_columns)}")

    index_name = f"{table_name}_unique_idx"
    create_index_query = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
    ON {table_name} ({', '.join([f'"{col}"' for col in unique_columns])})
    """
    cursor.execute(create_index_query)
    conn.commit()

    columns = ', '.join([f'"{col}"' for col in pandas_df.columns])
    placeholders = ', '.join(['%s' for _ in pandas_df.columns])
    conflict_update = ', '.join(
        [f'"{col}" = EXCLUDED."{col}"' for col in pandas_df.columns if col not in unique_columns])

    upsert_query = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT ({', '.join([f'"{col}"' for col in unique_columns])})
    DO UPDATE SET
        {conflict_update}
    """

    for _, row in pandas_df.iterrows():
        cursor.execute(upsert_query, tuple(row))

    conn.commit()
    cursor.close()
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
