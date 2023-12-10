import subprocess
import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *

with open('after_optimization/performance_mysql_ao.csv', 'w') as querys_archive:
    querys_archive.write("Simple Query1, Simple Query2, ComplexQuery1, ComplexQuery2, Simple Query1 indexing, Simple Query2 indexing, ComplexQuery1 indexing, ComplexQuery2 indexing\n")

for i in range(30):
    subprocess.run(['python', 'after_optimization/mysql_creation_op.py'])
    subprocess.run(['python', 'after_optimization/mysql_indexes_op.py'])