from confluent_kafka import Consumer
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'group.id': 'amiya-consumer',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '37GRWPN6VR5SPF4I',
    'sasl.password': 'aF47aQPkWqcVxSRWwU2mXZvkHANp8VG/Fiu3kaepsMmzCy6FbgeQoTybYJrBbm0e'
}

c = Consumer(config)
print("consumer created")

c.subscribe(['amiya_test_1']) # add in the list if you want to read data from more topics

no_of_message_consumed = 0
no_messages_consumed = 0
while True:
    logger.info("consumer running")
    msg = c.poll(1.0)

    if msg is None:
        print('msg None')
        logger.info("no message received")
        no_of_message_consumed+=1
        if no_of_message_consumed >= 20:
            break
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        logger.warning("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))


c.close()
print("consumer closed")