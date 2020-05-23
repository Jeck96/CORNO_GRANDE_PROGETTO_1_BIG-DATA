#!/usr/bin/python3
"""mapper.py"""
import sys
import csv
import json
import costanct as C

def toJson(azione):
    dic = {   
        "ticker" : azione[0],
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