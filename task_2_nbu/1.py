from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.3.1.jar") \
    .getOrCreate()

import psycopg2



df = spark.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/task_2") \
    .option("dbtable",  'nbu') \
    .option("user", "postgres") \
    .option("password", "1111") \
    .option("driver", "org.postgresql.Driver") \
    .load()
df.printSchema()