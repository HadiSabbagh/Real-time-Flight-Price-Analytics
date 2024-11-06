import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

con = sqlite3.connect('flights_db.db')
cur = con.cursor()


def create_table(query, cur, con):
    cur.execute(query)
    con.commit()



flights_train = """
CREATE TABLE IF NOT EXISTS flights_train (
    id INTEGER,
    airline TEXT,
    flight TEXT,
    source_city TEXT,
    departure_time TEXT,
    stops TEXT ,
    arrival_time TEXT,
    destination_city TEXT,
    class TEXT,
    duration FLOAT,
    days_left INTEGER,
    price INTEGER
)
"""
create_table(flights_train, cur, con)


data = pd.read_csv("./docs/Clean_Dataset.csv")
train, test = train_test_split(data,test_size=0.30)
train['price'] = train['price'].astype(float)
test['price'] = test['price'].astype(float)

train.to_csv("./docs/train.csv",index=False)
test.to_csv("./docs/test.csv",index=False)

train.to_sql("flights_train", con, if_exists="replace", index=False, index_label='id')
# test.to_sql("flights_test", con, if_exists="replace", index=False,index_label='id')


cur.close()
con.close()
