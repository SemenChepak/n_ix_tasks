from pyspark.sql import SparkSession, dataframe


def read_from_db():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/postgresql-42.3.1.jar") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/task_2") \
        .option("dbtable", 'nbu') \
        .option("user", "postgres") \
        .option("password", "1111") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df.printSchema()
    return df


def find_distinct_val(df: dataframe.DataFrame):
    return df.select(df.currency_code).distinct().collect()


def add_column(df: dataframe.DataFrame):
    df = df.withColumn("dolar", 1 / df.rate)


def create_parts(df: dataframe.DataFrame):
    val = find_distinct_val(df)
    for i in val:
        new = df.filter(df.currency_code == i.currency_code)
        new.withColumn("dolar", 1 / df.rate)
        new.show()
        # new.write.parquet(i.currency_code)


if __name__ == '__main__':
    create_parts(read_from_db())
