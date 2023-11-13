import pandas as pd
import json
from pymongo import MongoClient

df = pd.read_csv('bd_dataset/goalscorers.csv')
df.to_json("goalscorers.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df = pd.read_csv('bd_dataset/shootouts.csv')
df.to_json("shootouts.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df = pd.read_csv('bd_dataset/results.csv')
df.to_json("results.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)