from time import sleep
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


def fetch_last_value():
    """get last message from topic"""
    consumer = KafkaConsumer(
        'test_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='my_group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    # last_msg = next(consumer)
    ps = [TopicPartition('test_topic', p) for p in consumer.partitions_for_topic('test_topic')]
    last_msg = consumer.end_offsets(ps)
    print(last_msg)
    return sorted(list(last_msg.values()), reverse=True)[0]


if __name__ == '__main__':
    last = fetch_last_value()
    for data in range(last, last + 5):
        message = {'test_1': data}
        producer.send('test_topic_1', value=message)
        sleep(1)
        message = {'test': data}
        producer.send('test_topic', value=message)
        sleep(1)
