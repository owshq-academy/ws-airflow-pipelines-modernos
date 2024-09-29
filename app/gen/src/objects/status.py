from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()


class OrderStatus(object):
    """
    A class to generate the status of an order from placement to delivery.
    """

    def __init__(self, order_id):
        self.order_id = order_id
        self.timestamp = datetime.now()
        self.status = self.generate_status()

    def generate_status(self):
        """
        Generate a sequence of statuses for an order.
        """
        statuses = [
            "Order Placed",
            "In Analysis",
            "Accepted",
            "Preparing",
            "Ready for Pickup",
            "Picked Up",
            "Out for Delivery",
            "Delivered",
            "Completed"
        ]

        status_sequence = []
        for status in statuses:
            status_entry = {
                "order_id": self.order_id,
                "status": status,
                "timestamp": str(self.timestamp)
            }
            status_sequence.append(status_entry)
            self.timestamp += timedelta(minutes=random.randint(5, 15))

        return status_sequence

    @staticmethod
    def get_multiple_order_statuses(order_ids):
        """
        Generate multiple orders' statuses.

        Args:
            order_ids (list): A list of order IDs.

        Returns:
            list: A list of dictionaries representing the status progression of multiple orders.
        """

        all_statuses = []
        for order_id in order_ids:
            order_status = OrderStatus(order_id=order_id)
            all_statuses.extend(order_status.status)

        return all_statuses
