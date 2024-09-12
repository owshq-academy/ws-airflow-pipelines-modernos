def store_users(**kwargs):
    """
    :param kwargs: Dictionary containing keyword arguments. Expected to have 'ti' for TaskInstance.
    :return: Success message indicating the user data has been stored.
    """

    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_users', key='processed_user_data')

    if not processed_data:
        raise ValueError("No processed data received from process_users task")

    print(f"Storing processed user data: {processed_data}")
    return "Successfully stored user data"
