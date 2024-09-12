def process_users(**kwargs):
    """
    :param kwargs: Arbitrary keyword arguments. Expected to contain 'ti', the task instance.
    :return: A string message indicating the successful processing of user data.
    """

    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='get_users', key='raw_user_data')

    if not raw_data:
        raise ValueError("No data received from get_users task")

    processed_data = []
    for user in raw_data:
        processed_user = {
            'id': user['id'],
            'name': user['name'],
            'email': user['email'],
            'company': user['company']['name']
        }
        processed_data.append(processed_user)

    ti.xcom_push(key='processed_user_data', value=processed_data)
    return "Successfully processed user data"
