import datetime
import logging
import os

from csv_op import write_csv, upload_csv
from generator import generation
from parquet_op import spark_create_parquet, upload_directory

logging.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p', filename='execution.log', filemode='w')

person_number = int(input('Number of persons: '))
logging.warning(f': script started :{os.getlogin()}, {datetime.datetime.now()}')
list_person = generation(person_number)
file = write_csv(list_person)
upload_csv(file)
print(f'Finished! uploaded file: {file.split("/")[1]}')
spark_file = spark_create_parquet(file)
upload_directory(spark_file)
print(f'Finished! uploaded dir: {spark_file.split("/")[1]}')
