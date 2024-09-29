from faker import Faker
import random
import uuid

fake = Faker()


class Customers(object):
    """
    A class that generates fake customer data.
    """

    def __init__(self):
        self.id = random.randint(1, 10000)
        self.uid = str(uuid.uuid4())
        self.password = fake.password()
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.username = f"{self.first_name.lower()}.{self.last_name.lower()}"
        self.email = f"{self.first_name.lower()}.{self.last_name.lower()}@{fake.free_email_domain()}"
        self.avatar = fake.image_url(width=300, height=300)
        self.gender = fake.random_element(elements=("Male", "Female"))
        self.phone_number = fake.phone_number()
        self.social_insurance_number = fake.ssn()
        self.date_of_birth = fake.date_of_birth()
        self.employment = {
            "title": fake.job(),
            "key_skill": fake.random_element(elements=("Self-motivated", "Teamwork", "Leadership", "Problem-solving"))
        }
        self.address = {
            "city": fake.city(),
            "street_name": fake.street_name(),
            "street_address": fake.street_address(),
            "zip_code": fake.zipcode(),
            "state": fake.state(),
            "country": fake.country(),
            "coordinates": {
                "lat": float(fake.latitude()),
                "lng": float(fake.longitude())
            }
        }
        self.credit_card = {
            "cc_number": fake.credit_card_number(card_type=None).replace(" ", "-")
        }
        self.subscription = {
            "plan": fake.random_element(elements=("Free", "Basic", "Business", "Premium")),
            "status": fake.random_element(elements=("Active", "Blocked", "Cancelled")),
            "payment_method": fake.random_element(elements=("Credit Card", "Money transfer", "PayPal")),
            "term": fake.random_element(elements=("Monthly", "Annual"))
        }

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Get multiple rows of customer data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing customer data.
        """

        list_return_data = []
        for _ in range(gen_dt_rows):
            customer = Customers()
            list_return_data.append({
                "id": customer.id,
                "uid": customer.uid,
                "password": customer.password,
                "first_name": customer.first_name,
                "last_name": customer.last_name,
                "username": customer.username,
                "email": customer.email,
                "avatar": customer.avatar,
                "gender": customer.gender,
                "phone_number": customer.phone_number,
                "social_insurance_number": customer.social_insurance_number,
                "date_of_birth": str(customer.date_of_birth),
                "employment": customer.employment,
                "address": customer.address,
                "credit_card": customer.credit_card,
                "subscription": customer.subscription
            })

        return list_return_data
