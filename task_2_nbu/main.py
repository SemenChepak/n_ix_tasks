from datetime import date, datetime

from Nbu_API import Extractor
from db_work import insert_db
from Spark_obj import spark_op

if __name__ == '__main__':
    start_date = date(2021, 11, 1)
    end_date = datetime.now().date()
    data_to_insert = Extractor.get_data(start_date=start_date, end_date=end_date, list_of_currencies=[], sleep_opt=1)
    insert_db.insert_into_db(data_to_insert)
    df = spark_op.read_from_db()
    path = spark_op.create_parts(df)
    spark_op.upload_directory(path)
