import pandas as pd
import requests
import re
import time
from bs4 import BeautifulSoup
import threading
import redis
import pymongo as mongo
client = mongo.MongoClient("mongodb://127.0.0.1:27017")

r = redis.Redis()

ls = []

url = 'https://www.blockchain.com/btc/unconfirmed-transactions'
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

df = pd.DataFrame()
df2 = pd.DataFrame()

def printit():
    threading.Timer(5.0, printit).start()
    
    url = 'https://www.blockchain.com/btc/unconfirmed-transactions'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    Hash = soup.find(class_='sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK')
    Time = soup.find(class_='sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC')
    BTC = soup.find(class_='sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC', text=re.compile('BTC'))
    
    ls.append({'Hash': Hash.text, 'Time': Time.text, 'BTC': BTC.text})
    df = pd.DataFrame(ls)
    df2.append(df)
    
    print(df)
    
    r.set(Hash.text, BTC.text, ex=300)
    r.get(Hash.text)
    
    transaction = {"_id": Hash.text, "Hash": Hash.text, 'Time': Time.text, 'BTC': BTC.text}
    transactions_db = client["transactions"]
    col_transactions = transactions_db["transactions"]
    
    col_transactions.insert_one(transaction)

printit()