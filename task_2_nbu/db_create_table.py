import psycopg2

import creds

postgresConnection = psycopg2.connect(f"dbname={creds.dbtable} user={creds.user} password={creds.password}")

cursor = postgresConnection.cursor()

# Create table statement
sqlCreateTable = """create table nbu (
                                    db_id SERIAL NOT NULL PRIMARY KEY,
                                    r030 INT NOT NULL,
                                    rate text NOT NULL,
                                    currency_name TEXT NOT NULL,
                                    currency_code TEXT NOT NULL,
                                    exchange_date DATE NOT NULL DEFAULT CURRENT_DATE,
                                    acquisition_datetime timestamp NOT NULL DEFAULT NOW()
                                    );"""

# Create a table in PostgreSQL database

cursor.execute(sqlCreateTable)

postgresConnection.commit()

postgresConnection.close()
