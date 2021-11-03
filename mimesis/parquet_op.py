import datetime
import logging
import os

from boto3 import client
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import creds

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p', filename='execution.log', filemode='a')


def upload_directory(path: str):
    """open connection to S3 bucket and upload directory
     with created files in s3/bucket_name/files_generated/parquet_file/file_name"""
    s3 = client(
        's3',
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key
    )
    logging.info(f'{upload_directory.__name__}: Connection to S3 Successes: filename {path}')

    for root, dirs, files in os.walk(path):
        """upload all files from the path one by one"""
        for f in files:
            s3.upload_file(os.path.join(root, f), creds.bucket_name, f'files_generated/{path}/{f}')
    logging.info(f'{upload_directory.__name__}: uploaded file {path}')
    logging.info(f'{upload_directory.__name__} performed successfully')


def spark_create_parquet(file_name: str) -> str:
    """read csv file with schema, add columns year_of_birthday, timestamp, full_name,
    return path to created new parquet files"""

    spark = SparkSession.builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value").getOrCreate()
    logging.info(f'{spark_create_parquet.__name__}: SparkSession created')

    schema = StructType([
        StructField("last_name", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("telephone", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("username", StringType(), True),
        StructField("identifier", StringType(), True),
        StructField("create_at", TimestampType(), True),
    ])

    df = spark.read.csv(file_name, schema=schema, sep=",")
    logging.info(f'{spark_create_parquet.__name__}: Spark opened {file_name} successfully')

    df = df.withColumn("year_of_birthday", datetime.datetime.now().year - df.age)
    df = df.withColumn("timestamp", current_timestamp())
    df = df.withColumn("full_name", concat_ws(" ", df.first_name, df.last_name))
    logging.info(f'{spark_create_parquet.__name__}: Added column successfully')

    dir_name = f'parquet_file/spark_file_from_{file_name.split("/")[1].rsplit(".")[0]}'  # path to put parquet_file
    df.write.parquet(dir_name)
    logging.info(f'{spark_create_parquet.__name__}: created files {dir_name}')
    return dir_name
