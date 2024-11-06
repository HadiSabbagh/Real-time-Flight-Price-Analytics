from kafka import KafkaProducer
import time
import pandas as pd
import os
from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager



def get_last_offset(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return int(f.read().strip())
    return 0


def save_last_offset(file_path, offset):
    with open(file_path, 'w') as f:
        f.write(str(offset))



def produce():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    df = pd.read_csv('./docs/test.csv')
    offset = get_last_offset("./offset.txt")

    for index, row in df.iterrows():
        if index > offset:
            message = row.to_json().encode('utf-8')
            producer.send('flights_test', message)
            print(f"Sent Message {index+1}")
            # print("Message: ", message)
            time.sleep(0.10)
            save_last_offset("./offset.txt", index+1)
    producer.flush()
    producer.close()

@asynccontextmanager
async def start_producer(app: FastAPI):
    produce()
    yield
    
app = FastAPI(title="producer_api", lifespan=start_producer)