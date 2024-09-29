# import libraries
import pulsar
import json

# connect into cluster and topic
client = pulsar.Client('pulsar://143.198.245.228:6650')
producer = client.create_producer('persistent://owshq/rides/src-rides-json')

# event message [list] ~ 5
events = [{'user_id': 100, 'time_stamp': 1543680476688, 'source': 'West End', 'destination': 'Boston University',
          'distance': 2.8, 'price': 11.0, 'surge_multiplier': 1.0, 'id': '256c3023-a5cf-434f-9776-ac8f5ab5104c',
          'product_id': '997acbb5-e102-41e1-b155-9df7de0a73f2', 'name': 'UberPool', 'cab_type': 'Uber',
          'dt_current_timestamp': '2022-04-26 15:49:14.472547'},
         {'user_id': 953, 'time_stamp': 1544863511489, 'source': 'Fenway', 'destination': 'West End', 'distance': 2.72,
          'price': 22.0, 'surge_multiplier': 1.0, 'id': '11800674-354e-4fc9-ae2f-e506b4b5d692',
          'product_id': '6c84fd89-3f11-4782-9b50-97c468b19529', 'name': 'Black', 'cab_type': 'Uber',
          'dt_current_timestamp': '2022-04-26 15:49:14.472547'},
         {'user_id': 211, 'time_stamp': 1543706280776, 'source': 'Boston University', 'destination': 'Beacon Hill',
          'distance': 2.66, 'price': 3.5, 'surge_multiplier': 1.0, 'id': 'ca0ba30c-06a4-4265-b1f7-bcd111aac0f1',
          'product_id': 'lyft_line', 'name': 'Shared', 'cab_type': 'Lyft',
          'dt_current_timestamp': '2022-04-26 15:49:14.472547'},
         {'user_id': 655, 'time_stamp': 1543759082026, 'source': 'Beacon Hill', 'destination': 'Boston University',
          'distance': 2.33, 'price': 8.5, 'surge_multiplier': 1.0, 'id': '0da2312f-780c-423e-be29-4e6da7a70c27',
          'product_id': '997acbb5-e102-41e1-b155-9df7de0a73f2', 'name': 'UberPool', 'cab_type': 'Uber',
          'dt_current_timestamp': '2022-04-26 15:49:14.472547'},
         {'user_id': 938, 'time_stamp': 1543719482066, 'source': 'Back Bay', 'destination': 'Northeastern University',
          'distance': 1.1, 'price': 26.5, 'surge_multiplier': 1.0, 'id': 'd55f12a6-db2f-44e2-b4aa-7d0f5987a4cc',
          'product_id': '6d318bcc-22a3-4af6-bddd-b409bfce1546', 'name': 'Black SUV', 'cab_type': 'Uber',
          'dt_current_timestamp': '2022-04-26 15:49:14.472547'}]

# loop through messages
for event in events:

    # encode message
    # send message
    data = json.dumps(event).encode('utf-8')
    producer.send(data)

# close client
client.close()