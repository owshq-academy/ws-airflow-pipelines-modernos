# import libraries
import pulsar

# client and consumer option
client = pulsar.Client('pulsar://143.198.245.228:6650')
consumer = client.subscribe('persistent://owshq/rides/src-rides-json', 's-test-rides-perst-01-json')

# loop to read messages [events]
while True:
    msg = consumer.receive()
    try:
        print("received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except Exception:
        # message failed to be processed
        consumer.negative_acknowledge(msg)
# close loop
client.close()