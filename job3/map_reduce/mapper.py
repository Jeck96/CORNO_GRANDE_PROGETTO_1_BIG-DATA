#!/usr/bin/python3
"""mapper.py"""
import sys
import csv
import pydoop.hdfs as hdfs
import json
import costanct as C


azienda_map = {}
with hdfs.open('input/historical_stocks.csv','rt') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count > 0:
            ticker,_,name,_,_ = row
            azienda_map[ticker] = {'name':name}
        line_count += 1

def toJson(azione):
    dic = {   
        "ticker" : azione[0],
        "name": azienda_map[azione[0]],
        "close": azione[2],
        "date" : azione[7],
    }
    return json.dumps(dic)
for line in sys.stdin:
    azione = line.split(',')
    ticker,_,close,_,_,_,_,data = azione
    anno_azione = int(data.split('-')[0])
    #if (anno_azione>=2016 and anno_azione<=2018):
    if (anno_azione in C.TRIENNIO):
        print(f'{toJson(azione)}')