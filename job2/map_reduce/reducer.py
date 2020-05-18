#!/usr/bin/python3
"""reducer.py"""

import json
import sys
import pandas as pd

"""
In input riceviamo una stringa contenente K e V in formato Json:
In particolare: 
    Formato di K:
        "k1": 'AHH'
        "k2": 2014
    Formato di V:
     'data_iniziale': '2010-01-20',
     'prezzo_iniziale': 11.57,
     'data_finale': '2010-12-28',
     'prezzo_finale': 10.44,
     'somma_volume': 155,
     'somma_prezzo_close': 89.76,
     'count': 10
}
"""
"""
Importiamo il dataset historical_stocks le informazioni necessarie su aziende e settori, 
in particolare  presente le relazioni tra le aziene e quindi i settori con i ticker
"""
settori = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/historical_stocks.csv", sep=',')
settori = settori.values
"""
La singola tupla associata all'array numPy settori è di questo formato:
    • ticker: simbolo dell’azione                       [0]
    • exchange: NYSE o NASDAQ                           [1]
    • name: nome dell’azienda                           [2]
    • sector: settore dell’azienda                      [3]
    • industry: industria di riferimento per l’azienda  [4]
"""
"""
ticker_per_settore sarà un dizionario che conterrà come chiavi il settore e come elementi di ciascuna chiave,
un array di ticker associati al settore
"""
ticker_per_settore = {}
for settore in settori:
    if ticker_per_settore.get(settore[3]):
        ticker_per_settore[settore[3]].append(settore[0])
    else:
        ticker_per_settore[settore[3]]=[settore[0]]

"""
Il dizionario azioni_per_anno conterà tutte le informazioni COMPLETE sulle azioni (ottenute attraverso
opportune operazioni sui dati PARZIALI che arrivano dal combiner) raggruppate per chiave (ticker,anno)
In particolare per ciascuna chiave:
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
    chiave = json.loads(a[0])
    chiave = (list(chiave.values())[0], list(chiave.values())[1])
    dati_per_anno  = json.loads(a[1])

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(dati_per_anno)
    else:
        azioni_per_anno[chiave] = [dati_per_anno]

"""
results_azioni_per_anno conterrà un primo risultato che sarà per ciascuna chiave(ticker,anno):
    -variazionie annuale
    -somma del volume               (per il calcolo del volume medio)
    -somma dei prezzi di chiusura   (per il calcolo della variazione giornaliera)
    -count delle azioni (per chiave)
"""
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

    results_azioni_per_anno.append({k:{'variazione_annuale': (100 * (prezzo_finale - prezzo_iniziale) / prezzo_iniziale),
                                        'somma_volume': somma_volume,
                                        'somma_prezzo_close': somma_prezzo_close,
                                        'count': count}})
"""
settori_per_anno conterrà per ciascun settore:
    -somma variazione annuale di tutte le azioni del settore nell'anno
    -somma volulme di tutte le azioni del settore nell'anno
    -somma variazione giornaliera di tutte le azioni del settore nell'anno
"""
settori_per_anno = {}
for aa in results_azioni_per_anno:
    kaa = list(aa.keys())[0]

    for k1 in ticker_per_settore:
        if (kaa[0] in ticker_per_settore.get(k1, [])):
            if settori_per_anno.get((k1,kaa[1]),[]):
                settori_per_anno[(k1,kaa[1])].append(list(aa.values())[0])
            else:
                settori_per_anno[(k1,kaa[1])] = [list(aa.values())[0]]

"""
result_settori_per_anno conterrò il risultato finale, ovvero per ciascun settore:
    -variazione annuale media
    -volume medio
    -variazione giornaliera
"""
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