import datetime
import logging

from mimesis import Person
from mimesis.locales import Locale


logging.basicConfig(level=logging.INFO,datefmt='%m/%d/%Y %I:%M:%S %p', filename='execution.log', filemode='a')


def generation(person_number) -> list:
    """generate list[dict] data f persons, return list with persons_data"""

    logging.info(f'{generation.__name__}: start created {person_number} persons_data')

    person = Person(Locale.EN)  # location settings
    person_list = []  # empty list to append person_data

    for i in range(person_number):  # generate persons and append them to person_list
        person_list.append(
            {
                "last_name": person.last_name(),
                "first_name": person.first_name(),
                "age": person.age(),
                "email": person.email(),
                "sex": person.sex(),
                "telephone": person.telephone(),
                "occupation": person.occupation(),
                "username": person.username(),
                "identifier": person.identifier(),
                "create_at": datetime.datetime.now().timestamp(),
            }
        )
    logging.info(f'{generation.__name__}: performed successfully. created {person_number} persons')
    return person_list
