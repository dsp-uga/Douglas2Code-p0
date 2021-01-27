"""

This project is for basic NLP operation like word segementation, counting and TF-IDF score calculation

"""
# import necessary package

from dask.distributed import Client, progress
from dask import delayed
import dask.bag as db
import pandas as pd
from time import sleep
import json

# create 8 parallel workers
client = Client(n_workers=8)

df = db.read_text('../data/handout/data/Documents/*.txt').to_dataframe(columns={"sentence"})

Dictionary = set()
All_words = []

for index, row in df.iterrows():
#     print(row["sentence"])
    result = row["sentence"].strip( )
#     print(result)
    for _ in result:
        if _ == "\ufeff" or _ == "\n":
            continue
        else:
            Dictionary.add(_.lower())
            All_words.append(_.lower())

new_dict = dict(db.from_sequence(All_words, partition_size=10).frequencies(sort=True).compute())

top_40_dict = []
i = 1
for key, item in new_dict.items():
    if i <= 40:
        top_40_dict.append((key,item))
        i = i+1


with open('sp1.json', 'w') as f:
    json.dump(top_40_dict, f)