import pandas as pd
import numpy as np

dataset = pd.read_csv(
    "kaggle-twitter-dataset/twitter_dataset.csv",
    nrows=1000,
    sep=",",
    usecols=[5],
    dtype={"text": "str"}
)
print(dataset)

dataset.to_csv('kaggle-twitter-dataset/twitter_dataset_1000.csv',index=0)