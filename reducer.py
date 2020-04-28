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
    """
    formato dell'oggetto caricato:
    {
        "ticker" : "GDR",
        "open" : "9.46724, 
        "close" : "9.743632",
        "low" : "9.141",
        "high" : "10.287228",
        "volume" : "57393",
        "date" : "2017-04-27"
    }
    """
    #Riempimento del dizionario
    if dizionario.get(simbolo):
       dizionario[simbolo].append(azione)
    else:
        dizionario[simbolo] = [azione]

#Iterarazione sul dizionario per calcolare i vari campi richiesti
for k in dizionario:

    # è equivalente al for sotto, ma visto che dovremmo fare altre cose conviene il for esplicito
    #prezzi_min = [float(a['low']) for a in dizionario[k]] 
    
    prezzi_min = [] 
    prezzi_max = []
    for a in dizionario[k]:
        prezzi_min.append(float(a['low']))
        prezzi_max.append(float(a['high']))

    lowest_price = min(prezzi_min)
    hieght_price = max(prezzi_max)

    print(f'{{\nsimbolo:{k}\nprezzo_min:{lowest_price}\nprezzo_massimo:{hieght_price}\n}}')