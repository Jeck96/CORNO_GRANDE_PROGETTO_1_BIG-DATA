#!/usr/bin/python3
"""reducer.py"""

import sys
import json
dizionario = {}
for line in sys.stdin:
    #Costruiamo un dizionario con chiave il simbolo dell'azione e
    # valore l'azione che comprende quindi tutti i campi
    line = line.split('\t')
    simbolo = line[0]
    azione = json.loads(line[1])

    if dizionario.get(simbolo):
       dizionario[simbolo].append(azione)
    else:
        dizionario[simbolo] = [azione]

for k in dizionario:

    # è equivalente al for sotto, ma visto che dovremmo fare altre cose conviene il for esplicito
    #prezzi_min = [float(a['low']) for a in dizionario[k]] 
    
    prezzi_min = [] 
    for a in dizionario[k]:
        prezzi_min.append(float(a['low']))
    print(f'{k}\tprezzo_min:{min(prezzi_min)}')