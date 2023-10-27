from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '37GRWPN6VR5SPF4I',
    'sasl.password': 'aF47aQPkWqcVxSRWwU2mXZvkHANp8VG/Fiu3kaepsMmzCy6FbgeQoTybYJrBbm0e'
}

topic = 'amiya_test_1'


def delivery_callback(err, msg):
    if err:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), 'partition', msg.partition())

producer = Producer(config)

message_key = 'amiya1'  # Optional: Set a message key
message_value = 'amiya1'

producer.produce(topic, key=message_key, value=message_value, callback=delivery_callback)

producer.flush()

