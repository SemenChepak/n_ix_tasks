import time
from datetime import date, timedelta, datetime

import requests

currency = ['USD', 'EUR', 'MXN', 'INR', 'IRR', 'GEL']


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def create_url(val: str, date_f: str) -> str:
    """create url for API"""
    return f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode={val}&date={date_f}&json'


def check_res(response):
    """check result. will add logs to write status_code"""
    return True if response.json() != [{"message": "Wrong parameters format"}] and len(response.json()) > 0 else False


def get_data(start_date, end_date) -> list:
    """read all results from Api from 2021/01/01, :return list_of_data"""
    data_list = []

    for single_date in daterange(start_date, end_date):
        for val in currency:
            response = requests.get(create_url(val, single_date.strftime("%Y%m%d")))
            if check_res(response):
                data_list.append(response.json()[0])
        time.sleep(1)
    return data_list


if __name__ == '__main__':
    start_date = date(2021, 9, 1)
    end_date = datetime.now().date()
    get_data(start_date, end_date)