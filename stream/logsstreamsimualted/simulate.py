from random import randrange
import numpy as np
import time
from datetime import datetime
import json
import csv

fields = ['userID', 'score', 'time']

with open('log2.csv', 'a') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(fields)

i= 0

while True and i < 200:
    print("i = ", i)
    userID = randrange(10)
    sleeptimer = randrange(5)
    print("going to sleep for ", sleeptimer)
    time.sleep(sleeptimer)
    print("out of sleep, back...")
    score = randrange(5)
    now = time.time()
    data = [userID,score,now]



    with open('log2.csv', 'a') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(data)

    i = i+1
