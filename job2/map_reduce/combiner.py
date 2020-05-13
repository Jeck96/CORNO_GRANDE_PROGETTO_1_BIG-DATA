import sys
import json

def toJson(data_iniziale, prezzo_iniziale, data_finale, prezzo_finale, somma_volume,somma_prezzo_close,count):
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


""""
Questo è il dizionario in formato json che viene passato nello stream per ciascuna chiave (simbolo_azione,anno)
dic = {   
     "ticker" : azione[0],
     "close" : azione[2],
     "volume" : azione[6],
     "date" : azione[7]
    }
"""
"""
questo dizionario conterà tutte le informazioni parziali sulle azioni (ottenute attraverso opportune operazione sui dati
che arrivano al combiner) raggruppate per chiave (simbolo_azione,anno)
In particolare per ciascuna chiave viene mantenuto un informazione parziale relativa
ai soldi dati processati dal combiner che è un dizionario contenente:
    -data_iniziale
    -prezzo_iniziale
    -data_finale
    -prezzo_finale
    -somma_volume
    -somma_prezzo_close
    -count                  (per tenere traccia del numero di azioni con la stessa chiave, utile nel reducer 
                             per il calcolo della media)
"""

azioni_per_anno = {}

for line in sys.stdin:
    line = line.split(',')
    chiave = line[0]
    azione  = json.load(line[1])

    if azioni_per_anno.get(chiave):
        azioni_per_anno[chiave].append(azione)
    else:
        azioni_per_anno[chiave] = [azione]

#info_azioni_per_anno = {}
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
        somma_volume += int(a['volume'])
        somma_prezzo_close += int(a['close'])
        count += 1

#   info_azioni_per_anno[k] = {'data_iniziale':data_iniziale, 'prezzo_iniziale':prezzo_iniziale,
#                                'data_finale':data_finale, 'prezzo_finale':prezzo_finale, 'somma_volume':somma_volume}

    print('%s,%s' % ( k, toJson(data_iniziale,prezzo_iniziale,data_finale,prezzo_finale,somma_volume,count) ))


