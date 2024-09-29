from random import randint
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class CreditCard(object):
    """
    A class that represents credit card objects and provides methods for retrieving credit card data.
    """

    __slots__ = [
        "id",
        "uid",
        "credit_card_number",
        "credit_card_expiry_date",
        "credit_card_type"
    ]

    def __init__(self):
        """
        Initialize a CreditCard object with fake credit card data.
        """

        self.id = randint(0, 10000)
        self.uid = fake.uuid4()
        self.credit_card_number = fake.credit_card_number(card_type=None).replace(" ", "-")
        self.credit_card_expiry_date = fake.credit_card_expire(start="now", end="+10y", date_format="%Y-%m-%d")
        self.credit_card_type = fake.random_element(elements=[
            "visa", "mastercard", "american_express", "discover", "maestro",
            "diners_club", "jcb", "switch", "laser", "dankort"
        ])

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of credit card data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing credit card data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "id": randint(0, 10000),
                "uid": fake.uuid4(),
                "credit_card_number": fake.credit_card_number(card_type=None).replace(" ", "-"),
                "credit_card_expiry_date": fake.credit_card_expire(start="now", end="+10y", date_format="%Y-%m-%d"),
                "credit_card_type": fake.random_element(elements=[
                    "visa", "mastercard", "american_express", "discover", "maestro",
                    "diners_club", "jcb", "switch", "laser", "dankort"
                ])
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
