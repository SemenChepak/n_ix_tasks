import csv
import datetime
import os
import creds
import boto3
from mimesis import Person
from mimesis.locales import Locale
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


def generation() -> list:
    """generate list[dict] data f persons, return list with persons_data"""

    person = Person(Locale.EN)  # location settings
    person_list = []  # empty list to append person_data

    for i in range(10000):  # generate persons and append them to person_list
        person_list.append(
            {
                "last_name": person.last_name(),
                "first_name": person.first_name(),
                "age": person.age(),
                "email": person.email(),
                "sex": person.sex(),
                "telephone": person.telephone(),
                "occupation": person.occupation(),
                "username": person.username(),
                "identifier": person.identifier(),
                "create_at": datetime.datetime.now().timestamp(),
            }
        )
    return person_list


def write_csv(person_list: list) -> str:
    """create csv file without headers from person_list, return name of created file"""
    with open(f'csv_file/person_generation{datetime.datetime.now().timestamp()}.csv', 'w') as opened_file:
        name = opened_file.name
        writer = csv.DictWriter(opened_file, fieldnames=list(person_list[0]))
        writer.writerows(person_list)
    return name


def upload_csv(file_name):
    """open connection to S3 bucket and upload created file in s3/bucket_name/files_generated/csv_file/file_name"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key
    )
    s3.upload_file(file_name, creds.bucket_name, f'files_generated/{file_name}')


def spark_create_parquet(file_name: str) -> str:
    """read csv file with schema, add columns year_of_birthday, timestamp, full_name,
    return path to created new parquet files"""

    headers_list = ["last_name", "first_name", "age", "email", "sex", "telephone", "occupation", "username",
                    "identifier", "create_at"]  # list to set up columns headers

    spark = SparkSession.builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value").getOrCreate()  # creating spark_session

    schema = StructType([
        StructField("_c0", StringType(), True),
        StructField("_c1", StringType(), True),
        StructField("_c2", IntegerType(), True),
        StructField("_c3", StringType(), True),
        StructField("_c4", StringType(), True),
        StructField("_c5", StringType(), True),
        StructField("_c6", StringType(), True),
        StructField("_c7", StringType(), True),
        StructField("_c8", StringType(), True),
        StructField("_c9", TimestampType(), True),
    ])

    df = spark.read.csv(file_name, schema=schema, sep=",")  # reading csv_file with schema

    for i in range(len(df.columns)):  # setting up column headers
        df = df.withColumnRenamed(f"_c{i}", headers_list[i])

    df = df.withColumn("year_of_birthday", datetime.datetime.now().year - df.age)  # create new column
    df = df.withColumn("timestamp", current_timestamp())  # create new column
    df = df.withColumn("full_name", concat_ws(" ", df.first_name, df.last_name))  # create new column

    dir_name = f'parquet_file/spark_file_from_{file.split("/")[1][:-4]}'  # path to put parquet_file
    df.write.parquet(dir_name)  # create parquet_file
    return dir_name


def upload_directory(path: str):
    """open connection to S3 bucket and upload directory
     with created files in s3/bucket_name/files_generated/parquet_file/file_name"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key
    )
    for root, dirs, files in os.walk(path):
        """upload all files from the path one by one"""
        for f in files:
            s3.upload_file(os.path.join(root, f), creds.bucket_name, f'files_generated/{path}/{f}')


if __name__ == '__main__':
    list_person = generation()
    file = write_csv(list_person)
    upload_csv(file)
    print(f'Finished! uploaded file: {file.split("/")[1]}')
    spark_file = spark_create_parquet(file)
    upload_directory(spark_file)
    print(f'Finished! uploaded dir: {spark_file.split("/")[1]}')
