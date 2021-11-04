from pyspark.sql import SparkSession

import creds

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.3.1.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", creds.url) \
    .option("dbtable", creds.name) \
    .option("user", creds.user) \
    .option("password", creds.password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show()