from kafka import KafkaProducer
import time
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092') #bootstrap_servers – ‘host[:port]’ string (or list of ‘host[:port]’ strings) that the producer should contact to bootstrap initial cluster metadata. 

df = pd.read_csv('./docs/books.csv')
df[3791:]

for _, row in df.iterrows():
    message = row.to_json().encode('utf-8')
    producer.send('books', message)
    print("Sent:", message)
    time.sleep(0.10)
producer.flush()
producer.close()
