from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Payments(object):
    """
    A class that represents payment objects and provides methods for retrieving payment data.
    """

    __slots__ = [
                    "payment_id",
                    "uuid",
                    "order_id",
                    "amount",
                    "currency",
                    "payment_method",
                    "payment_date",
                    "status",
                    "dt_current_timestamp"
                ]

    def __init__(self):
        """
        Initialize a Payment object with fake payment data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.payment_id = randint(0, 1000000)
        self.uuid = fake.uuid4()
        self.order_id = randint(0, 100000)
        self.amount = round(fake.random_number(digits=4) / 100, 2)
        self.currency = fake.currency_code()
        self.payment_method = fake.random_element(elements=("Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Cash"))
        self.payment_date = fake.date_time_this_year()
        self.status = fake.random_element(elements=("Completed", "Pending", "Failed", "Refunded"))
        self.dt_current_timestamp = formatted_timestamp

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of payment data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing payment data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "payment_id": randint(0, 1000000),
                "uuid": fake.uuid4(),
                "order_id": randint(0, 100000),
                "amount": round(fake.random_number(digits=4) / 100, 2),
                "currency": fake.currency_code(),
                "payment_method": fake.random_element(elements=("Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Cash")),
                "payment_date": str(fake.date_time_this_year()),
                "status": fake.random_element(elements=("Completed", "Pending", "Failed", "Refunded")),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
