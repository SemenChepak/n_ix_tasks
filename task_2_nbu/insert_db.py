import psycopg2

import Extractor
import creds


def insert_into_db(data_for_insert: list):

    postgresConnection = psycopg2.connect(f"dbname={creds.dbtable} user={creds.user} password={creds.password}")

    cursor = postgresConnection.cursor()

    for row in data_for_insert:
        cursor.execute(
            f'INSERT INTO nbu (r030,currency_id, currency_name, currency_code,exchange_date) VALUES (%s,%s, %s, %s, %s)',
            (row['r030'], row['r030'], row['txt'], row['cc'], row['exchangedate']))

    postgresConnection.commit()


if __name__ == '__main__':
    data = Extractor.get_data()
    insert_into_db(data)
