from kafka import KafkaConsumer
from json import loads
import psycopg2
from config_db import HOST_DB, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


def create_db():
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS model_prediction ( predict VARCHAR ( 50 ) NOT NULL)")


def add_predict(text):
    with conn.cursor() as curr:
        curr.execute("INSERT INTO model_prediction (predict) VALUES(%s)", (text,))


def db_show():
    with conn.cursor() as curr:
        curr.execute("SELECT * FROM model_prediction")
        print(curr.fetchall()[-5:])


def model_predict(data: int):
    return data * data


if __name__ == '__main__':
    conn = psycopg2.connect(host=HOST_DB, port=5432, database=POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD)
    create_db()

    for i, message in enumerate(consumer):
        print(message.value)
        prediction = model_predict(message.value['test'])
        add_predict(prediction)
        if i % 5 == 4:
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
            db_show()

    conn.commit()
    conn.close()
