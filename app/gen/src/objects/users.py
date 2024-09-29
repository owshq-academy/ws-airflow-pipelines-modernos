from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Users(object):
    """
    A class that represents user objects and provides methods for retrieving user data.
    """

    __slots__ = [
                    "user_id",
                    "uuid",
                    "first_name",
                    "last_name",
                    "date_birth",
                    "city",
                    "country",
                    "company_name",
                    "job",
                    "phone_number",
                    "last_access_time",
                    "time_zone",
                    "dt_current_timestamp"
                ]

    def __init__(self):
        """
        Initialize a User object with fake user data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.user_id = randint(0, 10000)
        self.uuid = fake.uuid4()
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.date_birth = fake.date_of_birth()
        self.city = fake.city()
        self.country = fake.country()
        self.company_name = fake.company()
        self.job = fake.job()
        self.phone_number = fake.phone_number()
        self.last_access_time = fake.iso8601()
        self.time_zone = fake.timezone()
        self.dt_current_timestamp = formatted_timestamp

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of user data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing user data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "user_id": randint(0, 10000),
                "uuid": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "date_birth": str(fake.date_of_birth()),
                "city": fake.city(),
                "country": fake.country(),
                "company_name": fake.company(),
                "job": fake.job(),
                "phone_number": fake.phone_number(),
                "last_access_time": fake.iso8601(),
                "time_zone": fake.timezone(),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
