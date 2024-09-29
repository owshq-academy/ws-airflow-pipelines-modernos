import numpy as np
import requests
from datetime import datetime
from requests.exceptions import HTTPError
from pycpfcnpj import gen


class Requests(object):
    """
    A class that provides methods for generating user IDs, timestamps, and making API GET requests.

    Returns:
        https://random-data-api.com/api/
    """

    @staticmethod
    def gen_user_id():
        """
        Generate a random user ID.

        Returns:
            numpy.ndarray: An array of random user IDs.
        """

        return np.random.randint(1, 50000, size=100)

    @staticmethod
    def gen_cpf():
        """
        Generate a cpf number.

        Returns:
            str: A formatted cpf string.
        """

        return gen.cpf_with_punctuation()

    @staticmethod
    def gen_cnpj():
        """
        Generate a cnpj number.

        Returns:
            str: A formatted cnpj string.
        """

        return gen.cnpj_with_punctuation()

    @staticmethod
    def gen_timestamp():
        """
        Generate a formatted timestamp.

        Returns:
            str: A formatted timestamp string.
        """

        current_datetime = datetime.now()
        return current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @staticmethod
    def api_get_request(url, params):
        """
        Make an API GET request.

        Args:
            url (str): The URL of the API endpoint.
            params (dict): The parameters to include in the request.

        Returns:
            dict: The JSON response from the API.

        Raises:
            HTTPError: If an HTTP error occurs during the request.
            Exception: If the API is not available at the moment.
        """

        dt_request = requests.get(url=url, params=params)
        for url in [url]:
            try:
                response = requests.get(url)
                response.raise_for_status()
                dict_request = dt_request.json()

            except HTTPError as http_err:
                print(f'http error occurred: {http_err}')
            except Exception as err:
                print(f'api not available at this moment: {err}')
            else:
                return dict_request
