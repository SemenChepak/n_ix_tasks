import datetime
import os

import boto3
from pyspark.sql import SparkSession, dataframe

from Spark_obj.creds import creds

def read_from_db() -> dataframe.DataFrame:
    """open connection to db, reading all data and :return it"""

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "driver/postgresql-42.3.1.jar") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", creds.url) \
        .option("dbtable", creds.dbtable) \
        .option("user", creds.user) \
        .option("password", creds.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df


def find_distinct_val(df: dataframe.DataFrame) -> list:
    """:return all distinct Currencies from db"""

    return df.select(df.currency_code).distinct().collect()


def add_column(df: dataframe.DataFrame) -> dataframe.DataFrame:
    """Add a column with the value of the currency to the dollar, :return new df"""

    return df.withColumn("dolar", 1 / df.rate)


def create_parts(df: dataframe.DataFrame) -> list:
    """create partitions and dir with parquet_files, :return list of dir_path"""

    list_of_dirs = []
    val = find_distinct_val(df)
    path = int(datetime.datetime.now().timestamp())

    for i in val:
        new = df.filter(df.currency_code == i.currency_code)
        new = add_column(new)
        new.write.parquet(
            f"task_2_parquets_files_{path}/{i.currency_code}")
        list_of_dirs.append(f"task_2_parquets_files_{path}/{i.currency_code}/")
    return list_of_dirs


def upload_directory(list_of_dirs: list) -> None:
    """open connection to S3 bucket and upload directory
     with created files in s3/bucket_name/files_generated/parquet_file/file_name"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key
    )
    for path in list_of_dirs:
        for root, dirs, files in os.walk(path):
            """upload all files from the path one by one"""
            for f in files:
                s3.upload_file(os.path.join(root, f), creds.bucket_name, f'{path}{f}')


if __name__ == '__main__':
    path = create_parts(read_from_db())
    upload_directory(path)
