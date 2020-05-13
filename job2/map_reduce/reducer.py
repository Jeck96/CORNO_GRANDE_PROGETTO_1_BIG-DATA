import json
import sys
import pandas as pd

"""
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
    line = line.split(',')
    chiave = line[0]
    dati_per_anno  = json.load(line[1])

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(dati_per_anno)
    else:
        azioni_per_anno[chiave] = [dati_per_anno]

#info_azioni_per_anno = {}
results = []
for k in azioni_per_anno:

    azioni_per_anno[k] = sorted(azioni_per_anno[k], key=lambda a:a['data_iniziale'])
    data_iniziale = azioni_per_anno[k][0]['date']
    prezzo_iniziale = azioni_per_anno[k][0]['close']

    azioni_per_anno[k] = sorted(azioni_per_anno[k], key=lambda a:a.get['data_finale'], reverse=True)
    data_finale = azioni_per_anno[k][len(azioni_per_anno[k])-1]['date']
    prezzo_finale = azioni_per_anno[k][len(azioni_per_anno[k])-1]['close']

    somma_volume = 0
    somma_prezzo_close = 0
    count = 0
    for a in azioni_per_anno[k]:
        somma_volume += int(a['somma_volume'])
        somma_prezzo_close += int(a['somma_prezzo_close'])

        count += int(a['count'])

    results.append({'chiave':chiave,'variazione_annuale':100*(prezzo_finale-prezzo_iniziale/prezzo_iniziale),
                    'volume_medio':somma_volume/count,'var_giornaliera':somma_prezzo_close/count})

    for a in results:
        print (a)