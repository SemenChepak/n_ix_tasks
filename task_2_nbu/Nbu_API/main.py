from datetime import date, datetime

from Extractor import get_data
from db_work import insert_db

if __name__ == '__main__':
    start_date = date(2021, 11, 1)
    end_date = datetime.now().date()
    data_to_insert = get_data(start_date=start_date, end_date=end_date, list_of_currencies=[], sleep_opt=1)
    insert_db.insert_into_db(data_to_insert)
