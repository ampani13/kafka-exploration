from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '37GRWPN6VR5SPF4I',
    'sasl.password': 'aF47aQPkWqcVxSRWwU2mXZvkHANp8VG/Fiu3kaepsMmzCy6FbgeQoTybYJrBbm0e'
}

topic = 'amiya_test_1'


producer = Producer(config)

messages = [
    {'key': 'asd1', 'value': 'asd1'},
    {'key': 'asd2', 'value': 'asd2'},
    {'key': 'asd3', 'value': 'asd4'},
    # Add more messages as needed
]

def delivery_callback(err, msg):
    if err:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), 'partition', msg.partition())

for message in messages:
    message_key = message['key']
    message_value = message['value']
    producer.produce(topic, key=message_key, value=message_value, callback=delivery_callback)


producer.flush()
