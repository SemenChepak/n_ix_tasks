import csv
import datetime
import logging
import os

import boto3

import creds


logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p', filename='execution.log', filemode='a')


def upload_csv(file_name):
    """open connection to S3 bucket and upload created file in s3/bucket_name/files_generated/csv_file/file_name"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key
    )
    logging.info(f'{upload_csv.__name__}: Connection to S3 Successes: filename {file_name}')
    s3.upload_file(file_name, creds.bucket_name, f'files_generated/{file_name}')
    logging.info(f'{upload_csv.__name__}: uploaded file {file_name}')
    logging.info(f'{upload_csv.__name__} performed successfully')


def write_csv(person_list: list) -> str:
    """create csv file without headers from person_list, return name of created file"""
    try:
        with open(f'csv_file/person_generation{str(datetime.datetime.now().timestamp()).rsplit(".")[0]}.csv', 'w',
                  newline='') as opened_file:
            name = opened_file.name
            writer = csv.DictWriter(opened_file, fieldnames=list(person_list[0]))
            writer.writerows(person_list)
            logging.info(f'{write_csv.__name__}:created csv file {name}')

    except FileNotFoundError:
        os.mkdir('csv_file/')
        logging.info(f'Created directory "csv_file"')
        with open(f'csv_file/person_generation{str(datetime.datetime.now().timestamp()).rsplit(".")[0]}.csv', 'w',
                  newline='') as opened_file:
            name = opened_file.name
            writer = csv.DictWriter(opened_file, fieldnames=list(person_list[0]))
            writer.writerows(person_list)
            logging.info(f'{write_csv.__name__}: created csv file {name}')
    logging.info(f'{write_csv.__name__} performed successfully')
    return name
