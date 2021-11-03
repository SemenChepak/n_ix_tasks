import psycopg2
from psycopg2 import sql

con = psycopg2.connect("dbname=test_db user=postgres password=1111")
con.autocommit = True

cur = con.cursor()
cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, data varchar);")

con.commit()


