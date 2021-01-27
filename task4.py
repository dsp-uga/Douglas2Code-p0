from dask.distributed import Client, progress
from dask import delayed
import dask.bag as db
import pandas as pd
from time import sleep
import json
import os

# create 8 parallel workers
client = Client(n_workers=8)

def computeIDF(documents):
    import math
    N = len(documents)
    
    idfDict = dict.fromkeys(documents[0].keys(), 0)
    for document in documents:
        for word, val in document.items():
            if val > 0:
                idfDict[word] += 1
    
    for word, val in idfDict.items():
        idfDict[word] = math.log(N / float(val))
    return idfDict

def computeTF(wordDict, bagOfWords):
    tfDict = {}
    bagOfWordsCount = len(bagOfWords)
    for word, count in wordDict.items():
        tfDict[word] = count / float(bagOfWordsCount)
    return tfDict

def computeTFIDF(tfBagOfWords, idfs):
    tfidf = {}
    for word, val in tfBagOfWords.items():
        tfidf[word] = val * idfs[word]
    return tfidf

data_dir = "../data/handout/data/Documents/"

df_list = []
for filename in os.listdir(data_dir):
    if filename.endswith(".txt"): 
        df_list.append(db.read_text(data_dir+filename).to_dataframe(columns={"sentence"}))
         # print(os.path.join(directory, filename))
        continue
    else:
        continue

All_word_List = []
for item in df_list:
    All_words_tmp = []

    for index, row in item.iterrows():
        result = row["sentence"].split( )
        for _ in result:
            if _ == "\ufeff" or _ == "\n" or len(_)==1:
                continue
            else:
                All_words_tmp.append(_.strip("!:?,.;'").lower())
    All_word_List.append(All_words_tmp)

uniqueWords = set(All_word_List[0]).union(set(All_word_List[1])).union(set(All_word_List[2])).union(set(All_word_List[3])).union(set(All_word_List[4])).union(set(All_word_List[5])).union(set(All_word_List[6])).union(set(All_word_List[7]))

numOfWords1 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[0]:
    numOfWords1[word] += 1
numOfWords2 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[1]:
    numOfWords2[word] += 1
numOfWords3 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[2]:
    numOfWords3[word] += 1
numOfWords4 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[3]:
    numOfWords4[word] += 1
numOfWords5 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[4]:
    numOfWords5[word] += 1
numOfWords6 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[5]:
    numOfWords6[word] += 1
numOfWords7 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[6]:
    numOfWords7[word] += 1
numOfWords8 = dict.fromkeys(uniqueWords, 0)
for word in All_word_List[7]:
    numOfWords8[word] += 1

tf1 = computeTF(numOfWords1, All_word_List[0])
tf2 = computeTF(numOfWords2, All_word_List[1])
tf3 = computeTF(numOfWords3, All_word_List[2])
tf4 = computeTF(numOfWords4, All_word_List[3])
tf5 = computeTF(numOfWords5, All_word_List[4])
tf6 = computeTF(numOfWords6, All_word_List[5])
tf7 = computeTF(numOfWords7, All_word_List[6])
tf8 = computeTF(numOfWords8, All_word_List[7])

tfidf1 = computeTFIDF(tf1, idfs)
tfidf2 = computeTFIDF(tf2, idfs)
tfidf3 = computeTFIDF(tf3, idfs)
tfidf4 = computeTFIDF(tf4, idfs)
tfidf5 = computeTFIDF(tf5, idfs)
tfidf6 = computeTFIDF(tf6, idfs)
tfidf7 = computeTFIDF(tf7, idfs)
tfidf8 = computeTFIDF(tf8, idfs)
df = pd.DataFrame([tfidf1, tfidf2, tfidf3, tfidf4, tfidf5, tfidf6, tfidf7, tfidf8])

sorted_tfidf1 = sorted(tfidf1.items(), key=lambda x: x[1], reverse=True)
top_40_dict = []
i = 1
for item in sorted_tfidf1:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf2 = sorted(tfidf2.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf2:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf3 = sorted(tfidf3.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf3:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf4 = sorted(tfidf4.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf4:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf5 = sorted(tfidf5.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf5:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf6 = sorted(tfidf6.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf6:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf7 = sorted(tfidf7.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf7:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

sorted_tfidf8 = sorted(tfidf8.items(), key=lambda x: x[1], reverse=True)
i = 1
for item in sorted_tfidf8:
    if i <= 5:
        top_40_dict.append(item)
        i = i+1
print (top_40_dict)

top_40_dict = dict(top_40_dict)

with open('sp4.json', 'w') as f:
    json.dump(top_40_dict, f)

