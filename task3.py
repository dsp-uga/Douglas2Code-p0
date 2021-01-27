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

f = open("../data/handout/data/stopwords.txt", "r")

for _ in f:
    new_dict.pop(_.strip())


punc_list = ["!",":","?",",",".",";","'"]

del_list = []
for key, item in new_dict.items():
    if len(key)==1:
        del_list.append(key)

[new_dict.pop(key) for key in del_list] 

{k[1:] if k[0] in punc_list else k:v for k,v in new_dict.items()}

top_40_dict = []
i = 1
for key, item in new_dict.items():
    if i <= 40:
        top_40_dict.append((key,item))
        i = i+1

top_40_dict = dict(top_40_dict)

with open('sp3.json', 'w') as f:
    json.dump(top_40_dict, f)