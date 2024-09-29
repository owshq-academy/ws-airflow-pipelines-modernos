from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Restaurants(object):
    """
    A class that represents restaurant objects and provides methods for retrieving restaurant data.
    """

    __slots__ = [
                    "restaurant_id",
                    "uuid",
                    "name",
                    "address",
                    "city",
                    "country",
                    "phone_number",
                    "cuisine_type",
                    "opening_time",
                    "closing_time",
                    "average_rating",
                    "num_reviews",
                    "dt_current_timestamp"
                ]

    def __init__(self):
        """
        Initialize a Restaurant object with fake restaurant data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.restaurant_id = randint(0, 10000)
        self.uuid = fake.uuid4()
        self.name = fake.company() + " Restaurant"
        self.address = fake.address()
        self.city = fake.city()
        self.country = fake.country()
        self.phone_number = fake.phone_number()
        self.cuisine_type = fake.random_element(elements=("Italian", "Chinese", "Japanese", "Mexican", "Indian", "American", "French"))
        self.opening_time = "10:00 AM"
        self.closing_time = "10:00 PM"
        self.average_rating = round(randint(1, 50) / 10.0, 1)
        self.num_reviews = randint(0, 10000)
        self.dt_current_timestamp = formatted_timestamp

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of restaurant data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing restaurant data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "restaurant_id": randint(0, 10000),
                "uuid": fake.uuid4(),
                "name": fake.company() + " Restaurant",
                "address": fake.address(),
                "city": fake.city(),
                "country": fake.country(),
                "phone_number": fake.phone_number(),
                "cuisine_type": fake.random_element(elements=("Italian", "Chinese", "Japanese", "Mexican", "Indian", "American", "French")),
                "opening_time": "10:00 AM",
                "closing_time": "10:00 PM",
                "average_rating": round(randint(1, 50) / 10.0, 1),
                "num_reviews": randint(0, 10000),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
