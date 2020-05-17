#!/usr/bin/python3
"""combiner.py"""

import sys
import json

def toJsonValore(data_iniziale, prezzo_iniziale, data_finale, prezzo_finale, somma_volume, somma_prezzo_close, count):
    dic = {
     'data_iniziale': data_iniziale,
     'prezzo_iniziale': prezzo_iniziale,
     'data_finale': data_finale,
     'prezzo_finale': prezzo_finale,
     'somma_volume': somma_volume,
     'somma_prezzo_close': somma_prezzo_close,
     'count': count
    }
    return json.dumps(dic)

def toJsonChiave(k1,k2):
    diz = {
        'k1':k1,
        'k2':k2
    }
    return json.dumps(diz)
""""
In input riceviamo una stringa contenente K e V in formato Json:
In particolare: 
    Formato di K:
        "k1": 'AHH'
        "k2": 2014
    Formato di V:  
        "ticker" : 'AHH',
        "close" : 11.6,
        "volume" : 50,
        "date" : '2010-11-20'
"""
"""
Il dizionario azioni_per_anno conterà tutte le informazioni parziali sulle azioni (ottenute attraverso opportune
operazioni sui dati che arrivano al combiner) raggruppate per chiave (ticker,anno)
In particolare per ciascuna chiave viene mantenuta un'informazione parziale (ai soli dati processati
dal combiner), dizionario contenente:
    -data_iniziale      (si intende la data minima nell'anno per un particolare ticker)
    -prezzo_iniziale    (prezzo di chiusura associato alla data iniziale)
    -data_finale        (si intende la data massima nell'anno per un particolare ticker)
    -prezzo_finale      (presso di chiusura associato alla data finale)
    -somma_volume       (somma del volume di tutte le azioni del ticker nell'anno)
    -somma_prezzo_close (somma di tutti i prezzi di chiusura di tutte le azioni del ticker nell'anno)
    -count              (conteggio del numero di azioni con lo stesso ticker nell'anno)
"""
azioni_per_anno = {}

for line in sys.stdin:
    a = line.split(';',1)

    #K
    chiave = json.loads(a[0])
    chiave = (list(chiave.values())[0],list(chiave.values())[1])
    #V
    azione  = json.loads(a[1])

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(azione)
    else:
        azioni_per_anno[chiave] = [azione]

for k in azioni_per_anno:
    azioni_per_anno[k] = sorted(azioni_per_anno[k], key=lambda a: a['date'])

    data_iniziale = azioni_per_anno[k][0]['date']
    prezzo_iniziale = azioni_per_anno[k][0]['close']

    data_finale = azioni_per_anno[k][len(azioni_per_anno[k])-1]['date']
    prezzo_finale = azioni_per_anno[k][len(azioni_per_anno[k])-1]['close']

    somma_volume = 0
    somma_prezzo_close = 0
    count = 0

    for a in azioni_per_anno[k]:
        somma_volume += float(a['volume'])
        somma_prezzo_close += float(a['close'])
        count += 1

    print(f'{toJsonChiave(k[0],k[1])};\
            {toJsonValore(data_iniziale, prezzo_iniziale,data_finale, prezzo_finale,somma_volume, somma_prezzo_close, count)}')