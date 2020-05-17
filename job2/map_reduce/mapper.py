#!/usr/bin/python3
"""mapper.py"""

import pandas as pd
import sys
import json

def toJsonValore(azione):
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

def toJsonChiave(k1,k2):
    diz = {
        'k1':k1,
        'k2':k2
    }
    return json.dumps(diz)

#test
#settori = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test_1.csv", sep=';')
#settori = settori.values
#azioni = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv", sep=',')
#azioni = azioni.values
"""
La coppia (K,V) che verrà generata in output, sarà così definita:
    -K: (ticker;anno) separiamo i due elementi della chiave con un ; per poterli poi gestire in maniera diversa
                      rispetto al resto della stringa, in input al combier
    -V: [ticker,close,volume,date] 
"""
for line in sys.stdin:
    azione = line.split(',')
    anno_azione = int(azione[7].split('-')[0])
    if(anno_azione>=2008):
        
        print('%s;%s' % (toJsonChiave(azione[0],anno_azione), toJsonValore(azione)))