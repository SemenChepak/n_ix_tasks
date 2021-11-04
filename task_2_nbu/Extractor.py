import time

import requests

Currency = ['USD', 'EUR', 'MXN', 'INR', 'IRR', 'GEL']


def check_month(month: int):
    return f'0{month}' if month < 10 else month


def check_day(day: int):
    return f'0{day}' if day < 10 else day


def create_path(val: str, month: str, day: str):
    return f'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode={val}&date=2021{month}{day}&json'


def check_res(response):
    return True if response.json() != [{"message": "Wrong parameters format"}] and len(response.json()) > 0 else False


def get_data():
    data_list = []

    for month in range(1, 12):
        month_for_path = check_month(month)

        for day in range(1, 31):
            day_for_path = check_day(day)

            for val in Currency:
                response = requests.get(create_path(val, month_for_path, day_for_path))
                if check_res(response):
                    data_list.append(response.json()[0])
                    print(response.json()[0])
            time.sleep(1)
    return data_list


