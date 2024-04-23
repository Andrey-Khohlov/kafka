from kafka import KafkaConsumer
from json import loads
import psycopg2

HOST_DB = '0.0.0.0'
PORT = 5432
POSTGRES_USER = 'unicorn_user'
POSTGRES_PASSWORD = 'magical_password'
POSTGRES_DB = 'rainbow_database'

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


def add_predict(text):
    cur.execute("INSERT INTO model_prediction (predict) VALUES(%s)", (text,))




if __name__ == '__main__':
    conn = psycopg2.connect(host=HOST_DB, port=5432, database=POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS model_prediction ( predict VARCHAR ( 50 ) NOT NULL)")

    for message in consumer:
        message = message.value
        print(message)
        add_predict(message['test'])
        cur.execute("SELECT * FROM model_prediction")
        print(cur.fetchall())
    conn.commit()
    conn.close()
    cur.close()