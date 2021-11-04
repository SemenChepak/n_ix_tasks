import time

import requests

Currency = ['USD', 'EUR', 'MXN', 'INR', 'IRR', 'GEL']


def check_month(month: int) -> str:
    """check month, if < 10 :return like 0month"""
    return f'0{month}' if month < 10 else month


def check_day(day: int) -> str:
    """check day, if < 10 :return like 0day"""
    return f'0{day}' if day < 10 else day


def create_url(val: str, month: str, day: str) -> str:
    """create url for API"""
    return f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode={val}&date=2021{month}{day}&json'


def check_res(response):
    """check result. will add logs to write status_code"""
    return True if response.json() != [{"message": "Wrong parameters format"}] and len(response.json()) > 0 else False


def get_data() -> list:
    """read all results from Api from 2021/01/01, :return list_of_data"""
    data_list = []

    for month in range(1, 12):
        month_for_url = check_month(month)

        for day in range(1, 31):
            day_for_url = check_day(day)

            for val in Currency:
                response = requests.get(create_url(val, month_for_url, day_for_url))
                if check_res(response):
                    data_list.append(response.json()[0])
                    print(response.json()[0])
            time.sleep(1)
    return data_list
