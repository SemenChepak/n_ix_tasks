import time
from datetime import date, timedelta, datetime

import requests

from currencies.currencies import currency


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def create_url(val: str, date_f: str) -> str:
    """create url for API"""
    return f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode={val}&date={date_f}&json'


def check_res(response):
    """check result. will add logs to write status_code"""
    return True if response.json() != [{"message": "Wrong parameters format"}] and len(response.json()) > 0 else False


def check_currencies(curr: list):
    """check currencies. if input is null return basic currencies"""
    return curr if curr != [] else currency


def get_data(start_date, end_date, list_of_currencies: list[str], sleep_opt: int) -> list:
    """read all results from Api from 2021/01/01, :return list_of_data"""
    data_list = []
    currency_list = check_currencies(list_of_currencies)
    print(currency_list)
    for single_date in daterange(start_date, end_date):
        for val in currency_list:
            response = requests.get(create_url(val, single_date.strftime("%Y%m%d")))
            print(response.json())
            if check_res(response):
                data_list.append(response.json()[0])
                time.sleep(sleep_opt)
    return data_list


if __name__ == '__main__':
    start_date = date(2021, 9, 1)
    end_date = datetime.now().date()
    get_data(start_date=start_date, end_date=end_date, list_of_currencies=[], sleep_opt=1)
