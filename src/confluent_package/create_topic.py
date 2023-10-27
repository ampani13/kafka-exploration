from confluent_kafka.admin import AdminClient, NewTopic

admin_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '37GRWPN6VR5SPF4I',
    'sasl.password': 'aF47aQPkWqcVxSRWwU2mXZvkHANp8VG/Fiu3kaepsMmzCy6FbgeQoTybYJrBbm0e'
}


admin_client = AdminClient(admin_config)

# Create a NewTopic object with the topic name, number of partitions, and replication factor
topic_name = 'amiya_test_1'
num_partitions = 10
replication_factor = 3
new_topic = NewTopic(topic_name, num_partitions, replication_factor)
print(new_topic)


topic_creation_result = admin_client.create_topics([new_topic])


for topic, future in topic_creation_result.items():
    try:
        future.result()  # Wait for the topic creation to complete
        print(f"Topic '{topic}' created successfully!")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {str(e)}")