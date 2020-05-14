#!/usr/bin/python3
"""reducer.py"""

import json
import sys
import pandas as pd

"""
Questo è l'elemento dizionario associato a ciascuna chiave (simbolo_azione,anno) che arriva in input
dic = {
     'data_iniziale': data_iniziale,
     'prezzo_iniziale': prezzo_iniziale,
     'data_finale': data_finale,
     'prezzo_finale': prezzo_finale,
     'somma_volume': somma_volume,
     'somma_prezzo_close': somma_prezzo_close,
     'count': count
}
"""

azioni_per_anno = {}

for line in sys.stdin:
    line = line.split(';',1)
    chiave = line[0]
    dati_per_anno  = json.loads(line[1])

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(dati_per_anno)
    else:
        azioni_per_anno[chiave] = [dati_per_anno]

results = []
for k in azioni_per_anno:

    azioni_per_anno[k] = sorted(azioni_per_anno[k], key=lambda a: a['data_iniziale'])
    data_iniziale = azioni_per_anno[k][0]['data_iniziale']
    prezzo_iniziale = float(azioni_per_anno[k][0]['prezzo_iniziale'])

    azioni_per_anno[k] = sorted(azioni_per_anno[k], key=lambda a: a['data_finale'], reverse=True)
    data_finale = azioni_per_anno[k][0]['data_finale']
    prezzo_finale = float(azioni_per_anno[k][0]['prezzo_finale'])

    somma_volume = 0
    somma_prezzo_close = 0
    count = 0
    for a in azioni_per_anno[k]:
        somma_volume += float(a['somma_volume'])
        somma_prezzo_close += float(a['somma_prezzo_close'])

        count += int(a['count'])

    results.append({'chiave':chiave,'variazione_annuale':100*(prezzo_finale-prezzo_iniziale/prezzo_iniziale),
                    'volume_medio':somma_volume/count,'var_giornaliera':somma_prezzo_close/count})

    for a in results:
        print (a)