import psycopg2

from task_2_nbu.Nbu_API import Extractor
from db import creds


def insert_into_db(data_for_insert: list[dict]) -> None:
    """open connection to db, insert all data"""

    postgresConnection = psycopg2.connect(f"dbname={creds.dbtable} user={creds.user} password={creds.password}")

    cursor = postgresConnection.cursor()

    for row in data_for_insert:
        cursor.execute(
            f'INSERT INTO nbu (r030, rate, currency_name, currency_code,exchange_date) VALUES (%s, %s, %s, %s, %s)',
            (row['r030'], row['rate'], row['txt'], row['cc'], row['exchangedate']))

    postgresConnection.commit()
    postgresConnection.close()


if __name__ == '__main__':
    import datetime
    start_date = datetime.date(2021, 11, 4)
    end_date = datetime.datetime.now().date()
    data = Extractor.get_data(start_date=start_date, end_date=end_date, list_of_currencies=[], sleep_opt=1)
    insert_into_db(data)
