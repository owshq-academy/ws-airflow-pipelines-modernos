from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Merchants:
    """
    A class that represents merchant-related data and provides methods for generating that data.
    """

    __slots__ = [
        "merchant_id",
        "city",
        "address",
        "min_order",
        "group_id",
        "price_range",
        "shift_opening",
        "shift_closing",
        "week_availability",
        "dt_current_timestamp"
    ]

    def __init__(self):
        """
        Initialize a Merchant object with fake data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.merchant_id = randint(1, 100000)
        self.city = fake.city()
        self.address = fake.address()
        self.min_order = round(fake.random_number(digits=3) / 10, 2)
        self.group_id = fake.random_element(elements=("McClain", "Local Eats", "Fine Restaurants Group", "Global Foods"))
        self.price_range = randint(100, 500)
        self.shift_opening, self.shift_closing = self.generate_shift()
        self.week_availability = fake.random_element(elements=("Monday to Saturday", "Monday to Sunday", "Monday to Friday"))
        self.dt_current_timestamp = formatted_timestamp

    def generate_shift(self):
        """
        Generate shift opening and closing times as integers representing hours of the day.
        """
        opening_hour = randint(6, 10)
        closing_hour = opening_hour + randint(8, 14)
        return opening_hour, closing_hour

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Generate multiple rows of Merchant data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing Merchant data.
        """

        list_return_data = []
        for _ in range(gen_dt_rows):
            merchant = Merchants()
            get_faker_dt = {
                "merchant_id": merchant.merchant_id,
                "city": merchant.city,
                "address": merchant.address,
                "min_order": merchant.min_order,
                "group_id": merchant.group_id,
                "price_range": merchant.price_range,
                "shift_opening": merchant.shift_opening,
                "shift_closing": merchant.shift_closing,
                "week_availability": merchant.week_availability,
                "dt_current_timestamp": merchant.dt_current_timestamp
            }
            list_return_data.append(get_faker_dt)

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
