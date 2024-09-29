from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class Drivers(object):
    """
    A class that represents driver objects and provides methods for retrieving driver data, including vehicle information.
    """

    __slots__ = [
                    "driver_id",
                    "uuid",
                    "first_name",
                    "last_name",
                    "date_birth",
                    "city",
                    "country",
                    "phone_number",
                    "license_number",
                    "vehicle_type",
                    "vehicle_make",
                    "vehicle_model",
                    "vehicle_year",
                    "vehicle_license_plate",
                    "dt_current_timestamp"
                ]

    def __init__(self):
        """
        Initialize a Driver object with fake driver and vehicle data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.driver_id = randint(0, 10000)
        self.uuid = fake.uuid4()
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.date_birth = fake.date_of_birth()
        self.city = fake.city()
        self.country = fake.country()
        self.phone_number = fake.phone_number()
        self.license_number = fake.bothify(text='??######')

        self.vehicle_type = fake.random_element(elements=("Motorcycle", "Car", "Bicycle", "Scooter", "E-Bike", "Walk"))
        self.vehicle_make = fake.company()
        self.vehicle_model = fake.word().capitalize()
        self.vehicle_year = fake.year()
        self.vehicle_license_plate = fake.bothify(text='???###')
        self.dt_current_timestamp = formatted_timestamp

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of driver data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing driver data.
        """

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "driver_id": randint(0, 10000),
                "uuid": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "date_birth": str(fake.date_of_birth()),
                "city": fake.city(),
                "country": fake.country(),
                "phone_number": fake.phone_number(),
                "license_number": fake.bothify(text='??######'),
                "vehicle_type": fake.random_element(elements=("Motorcycle", "Car", "Bicycle", "Scooter", "E-Bike", "Walk")),
                "vehicle_make": fake.company(),
                "vehicle_model": fake.word().capitalize(),
                "vehicle_year": fake.year(),
                "vehicle_license_plate": fake.bothify(text='???###'),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
