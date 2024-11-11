import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split


data = pd.read_csv("./docs/Clean_Dataset.csv")
train, test = train_test_split(data,test_size=0.30)
train['price'] = train['price'].astype(float)
test['price'] = test['price'].astype(float)

train.to_csv("./docs/train.csv",index=False)
test.to_csv("./docs/test.csv",index=False)

