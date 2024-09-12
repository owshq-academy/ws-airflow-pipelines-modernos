def get_users(**kwargs):
    """
    :param kwargs: A variable-length dictionary argument used to pass additional keyword arguments.
    :return: A success message indicating that user data was successfully fetched.
    :raises Exception: If the API request fails, an exception is raised with the status code.
    """
    import requests

    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        kwargs['ti'].xcom_push(key='raw_user_data', value=data)
        return "Successfully fetched user data"
    else:
        raise Exception({response.status_code})
