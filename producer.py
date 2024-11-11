from kafka import KafkaProducer
import time
import pandas as pd


def produce():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    df = pd.read_csv('./docs/test.csv')

    for index, row in df.iterrows():
        message = row.to_json().encode('utf-8')
        producer.send('flights_test', message)
        print(f"Sent Message {index+1}")
        # print("Message: ", message)
        time.sleep(0.10)
    producer.flush()
    producer.close()


if __name__ == "__main__":
    produce()
