from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

if __name__=='__main__':
    for data in range(5):
        message = {'test_1': data}
        producer.send('test_topic_1', value=message)
        sleep(1)
        message = {'test': data}
        producer.send('test_topic', value=message)
        sleep(1)
