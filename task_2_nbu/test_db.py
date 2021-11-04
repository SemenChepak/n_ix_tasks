import psycopg2


import creds



postgresConnection = psycopg2.connect(f"dbname={creds.dbtable} user={creds.user} password={creds.password}")

cursor = postgresConnection.cursor()

cursor.execute(f'INSERT INTO nbu (r030,currency_id, currency_name, currency_code,exchange_date) VALUES (%s,%s, %s, %s, %s)', (27, 32, "currency_name_test", "currency_code_test", "2021/07/01"))

postgresConnection.commit()

