#!/usr/bin/python3
"""mapper.py"""

import pandas as pd
import sys
import json

def toJson(azione):
    dic = {
     "ticker" : azione[0],
     #"open" : azione[1],
     "close" : azione[2],
     #"low" : azione[4],
     #"high" : azione[5],
     "volume" : azione[6],
    "date" : azione[7]
    }
    return json.dumps(dic)

#settori = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test_1.csv", sep=';')
#settori = settori.values

azioni = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv", sep=',')
azioni = azioni.values

def toJsonChiave(k1,k2):
    diz = {
        'k1':k1,
        'k2':k2
    }
    return json.dumps(diz)
for line in sys.stdin:
    azione = line.split(',')
    anno_azione = int(azione[7].split('-')[0])
    if(anno_azione>=2008):
        print('%s;%s' % (toJsonChiave(azione[0],anno_azione),toJson(azione)))