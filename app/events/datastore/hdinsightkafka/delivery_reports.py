# [json]
def on_delivery_json(err, msg):

    if err is not None:
        print('message delivery failed: {}'.format(err))
    else:
        print('message successfully produced to {} [{}] at offset {}'.format(msg.topic(), msg.partition(), msg.offset()))