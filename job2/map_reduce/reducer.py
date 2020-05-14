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

settori = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test_1.csv", sep=';')
settori = settori.values
"""
simboli_per_settore sarà un dizionario che conterrà come chiavi i settori e come elementi array di stringhe che 
rappresentano i ticker associati al settore
"""
ticker_per_settore = {}
for settore in settori:
    if ticker_per_settore.get(settore[3]):
        ticker_per_settore[settore[3]].append(settore[0])
    else:
        ticker_per_settore[settore[3]]=[settore[0]]


azioni_per_anno = {}

for line in sys.stdin:
    a = line.split(';',1)
    chiave = json.loads(a[0])
    chiave = (list(chiave.values())[0], list(chiave.values())[1])
    #print(chiave[0])
    dati_per_anno  = json.loads(a[1])
    #print(dati_per_anno)

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(dati_per_anno)
    else:
        azioni_per_anno[chiave] = [dati_per_anno]

results_azioni_per_anno = []
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

    #print (f'{k[0]},{k[1]}')
    results_azioni_per_anno.append({k:{'variazione_annuale': (100 * (prezzo_finale - prezzo_iniziale) / prezzo_iniziale),
                                        'somma_volume': somma_volume,
                                        'somma_prezzo_close': somma_prezzo_close,
                                        'count': count}})

settori_per_anno = {}
for aa in results_azioni_per_anno:
    kaa = list(aa.keys())[0]
    print(kaa)
    for k1 in ticker_per_settore:
        if (kaa[0] in ticker_per_settore.get(k1, [])):
            if settori_per_anno.get((k1,kaa[1]),[]):
                #print("c'è")
                settori_per_anno[(k1,kaa[1])].append(list(aa.values())[0])
            else:
                settori_per_anno[(k1,kaa[1])] = [list(aa.values())[0]]
                #print("non c'è")
#for k in settori_per_anno:
#    print(settori_per_anno[k])

result_settori_per_anno  = []
for k in settori_per_anno:
    somma_volume = 0
    somma_prezzo_close = 0
    somma_count = 0
    somma_var_annuale = 0
    for aa in settori_per_anno[k]:
        somma_volume += aa['somma_volume']
        somma_prezzo_close += aa['somma_prezzo_close']
        somma_count += aa['count']
        somma_var_annuale += aa['variazione_annuale']

    var_annuale_media = somma_var_annuale/(len(settori_per_anno[k]))
    result_settori_per_anno.append({k:{'variazione_annuale_media':var_annuale_media,
                                        'volume_medio':somma_volume/somma_count,
                                        'var_giornaliera_media':somma_prezzo_close/somma_count}
                                    })
for s in result_settori_per_anno:
    print(s)
