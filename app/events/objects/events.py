from random import randint
from datetime import datetime
from faker import Faker
from faker_airtravel import AirTravelProvider

fake = Faker()
fake.add_provider(AirTravelProvider)
Faker.seed(randint(0, 10000000000))


class Events(object):

    @staticmethod
    def get_user_events(gen_dt_rows):

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):

            get_faker_dt = {
                "user_id": randint(0, 100000),
                "uuid": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "date_birth": str(fake.date_of_birth()),
                "phone_number": str(fake.phone_number()),
                "city": fake.city(),
                "web_browser": fake.user_agent(),
                "last_access_time": fake.iso8601(),
                "time_zone": fake.timezone(),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        return list_return_data

    @staticmethod
    def get_flight_events(gen_dt_rows):

        i = 0
        list_return_data = []
        while i < int(gen_dt_rows):
            get_faker_dt = {
                "user_id": randint(0, 100000),
                "uuid": fake.uuid4(),
                "flight": fake.flight(),
                "dt_current_timestamp": str(datetime.now())
            }
            list_return_data.append(get_faker_dt)
            i += 1

        return list_return_data