from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Menu(object):
    """
    A class that represents restaurant menu objects and provides methods for retrieving menu data linked to a restaurant.
    """

    __slots__ = [
        "menu_id",
        "restaurant_id",
        "uuid",
        "item_name",
        "item_description",
        "price",
        "category",
        "availability",
        "dt_current_timestamp"
    ]

    def __init__(self):
        """
        Initialize a Menu object with fake menu data linked to a restaurant.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.menu_id = randint(0, 50000)
        self.restaurant_id = randint(0, 10000)
        self.uuid = fake.uuid4()
        self.item_name = str(fake.word().capitalize()) + " " + str(fake.random_element(elements=["Salad", "Pizza", "Pasta", "Burger", "Soup", "Dessert"]))

        self.item_description = fake.sentence()
        self.price = round(fake.random_number(digits=3) / 10, 2)
        self.category = fake.random_element(elements=["Appetizer", "Main Course", "Dessert", "Beverage"])
        self.availability = fake.boolean(chance_of_getting_true=85)
        self.dt_current_timestamp = formatted_timestamp

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of menu data linked to a specific restaurant.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing menu data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "menu_id": randint(0, 50000),
                "restaurant_id": randint(0, 10000),
                "uuid": fake.uuid4(),
                "item_name": str(fake.word().capitalize()) + " " + str(fake.random_element(elements=["Salad", "Pizza", "Pasta", "Burger", "Soup", "Dessert"])),
                "item_description": fake.sentence(),
                "price": round(fake.random_number(digits=3) / 10, 2),
                "category": fake.random_element(elements=["Appetizer", "Main Course", "Dessert", "Beverage"]),
                "availability": fake.boolean(chance_of_getting_true=85),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
