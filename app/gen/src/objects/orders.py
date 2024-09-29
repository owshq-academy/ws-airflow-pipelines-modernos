from faker import Faker
import pandas as pd
from random import choice
import uuid

fake = Faker()


class Orders(object):
    """
    A class that represents orders.
    """

    __slots__ = [
        "order_id",
        "user_id",
        "restaurant_cnpj",
        "menu_id",
        "driver_license_number",
        "order_date",
        "total_amount",
        "payment_id"
    ]

    def __init__(self, user_cpfs, restaurant_cnpjs, menu_ids, driver_license_numbers, payment_ids):
        """
        Initialize an Order object with references to other tables.
        """

        self.order_id = str(uuid.uuid4())
        self.user_id = choice(user_cpfs)
        self.restaurant_cnpj = choice(restaurant_cnpjs)
        self.menu_id = choice(menu_ids)
        self.driver_license_number = choice(driver_license_numbers)
        self.order_date = fake.date_time_this_year()
        self.total_amount = round(fake.random_number(digits=4) / 100, 2)
        self.payment_id = choice(payment_ids)

    @staticmethod
    def get_multiple_rows(gen_dt_rows, user_cpfs, restaurant_cnpjs, menu_ids, driver_license_numbers, payment_ids):
        """
        Get multiple rows of order data.

        The choice list is used to randomly select a value from the list.
        - user_cpfs
        - restaurant_cnpjs
        - menu_ids
        - driver_license_numbers
        - payment_ids

        Args:
            gen_dt_rows: The number of rows to generate.
            user_cpfs: A list of CPFs from HubUsers.
            restaurant_cnpjs: A list of CNPJs from HubRestaurants.
            menu_ids: A list of Menu IDs from HubRestaurantsMenu.
            driver_license_numbers: A list of license numbers from HubDrivers.
            payment_ids: A list of Payment IDs from HubPayments.

        Returns:
            list: A list of dictionaries representing order data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):
            get_faker_dt = {
                "order_id": str(uuid.uuid4()),
                "user_id": choice(user_cpfs),
                "restaurant_cnpj": choice(restaurant_cnpjs),
                "menu_id": choice(menu_ids),
                "driver_license_number": choice(driver_license_numbers),
                "order_date": str(fake.date_time_this_year()),
                "total_amount": round(fake.random_number(digits=4) / 100, 2),
                "payment_id": choice(payment_ids)
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
