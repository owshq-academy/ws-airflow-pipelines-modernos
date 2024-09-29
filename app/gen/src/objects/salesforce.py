from random import randint
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(randint(0, 10000000000))


class SalesForce:
    """
    A class that represents Salesforce-related data and provides methods for generating that data.
    """

    __slots__ = [
        "merchant_id",
        "name",
        "contract_type",
        "category",
        "sub_category",
        "delivery_time",
        "dt_current_timestamp"
    ]

    def __init__(self):
        """
        Initialize a SalesForce object with fake data.
        """

        current_datetime = datetime.now()
        formatted_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        self.merchant_id = randint(1, 100000)
        self.name = fake.company()
        self.contract_type = fake.random_element(elements=("Own", "Platform", "Hybrid"))
        self.category, self.sub_category = self.generate_category()
        self.delivery_time = randint(15, 60)
        self.dt_current_timestamp = formatted_timestamp

    def generate_category(self):
        """
        Generate a restaurant category and sub-category.
        """
        category_pool = {
            "Fast Food": ["Burger", "Pizza", "Fried Chicken", "Tacos"],
            "Casual Dining": ["American", "Italian", "Mexican", "Japanese"],
            "Fine Dining": ["French", "Steakhouse", "Sushi", "Mediterranean"],
            "Café": ["Coffee", "Bakery", "Brunch", "Pastry"],
            "Buffet": ["Chinese", "Indian", "Seafood", "Global"],
            "Food Truck": ["Street Food", "Fusion", "Gourmet", "BBQ"]
        }

        main_category = fake.random_element(elements=list(category_pool.keys()))
        sub_category = fake.random_element(elements=category_pool[main_category])
        return main_category, sub_category

    @staticmethod
    def get_multiple_rows(gen_dt_rows):
        """
        Generate multiple rows of Salesforce data.

        Args:
            gen_dt_rows: The number of rows to generate.

        Returns:
            list: A list of dictionaries representing Salesforce data.
        """

        list_return_data = []
        for _ in range(gen_dt_rows):
            salesforce = SalesForce()
            get_faker_dt = {
                "merchant_id": salesforce.merchant_id,
                "name": salesforce.name,
                "contract_type": salesforce.contract_type,
                "category": salesforce.category,
                "sub_category": salesforce.sub_category,
                "delivery_time": salesforce.delivery_time,
                "dt_current_timestamp": salesforce.dt_current_timestamp
            }
            list_return_data.append(get_faker_dt)

        df_list_data = pd.DataFrame(list_return_data)
        return df_list_data.to_dict('records')
