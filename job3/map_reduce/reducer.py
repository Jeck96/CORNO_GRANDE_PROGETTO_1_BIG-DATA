#!/usr/bin/python3
"""reducer.py"""

import sys
import json
import costanct as C

dizionario = {}
for line in sys.stdin:
    #Costruiamo un dizionario con chiave il nome dell'azienda e
    # valore una lista di azioni corrispondeti a quell'azienda 
    azione = json.loads(line)
    del line
    """
    formato dell'oggetto caricato:
    {   
        "ticker" : APP,
        "name": Apple,
        "close": 17.2984y92,
        "date" : 2017-01-25,
    }
    """
    #Riempimento del dizionario
    nome = azione['name']['name']
    if dizionario.get(nome):
       dizionario[nome].append(azione)
    else:
        dizionario[nome] = [azione]

def print_dict(dic):
    for k in dic:
        print(k)
        for v in dic[k]:
            print('\t',v)
        print("\n")

anni_map={}
for k in dizionario:
    anni_map[C.A1] = []
    anni_map[C.A2] = []
    anni_map[C.A3] = []
    for v in dizionario[k]:
        anno_azione = int(v['date'].split('-')[0])
        anni_map[anno_azione].append(v)
    variazioni_annue = {}
    for anno in C.TRIENNIO:
        if anni_map[anno] != []:
            anni_map[anno] = sorted(anni_map[anno],key=lambda a: a['date'])
            initial_price = float(anni_map[anno][0]['close'])
            final_price = float(anni_map[anno][len(anni_map[anno])-1]['close'])
            variazioni_annue[anno] = round(100*(final_price-initial_price)/initial_price,2)
        else:
            variazioni_annue[anno] = None
    print(f'{k}:\n{C.A1}:{variazioni_annue[C.A1]},{C.A2}:{variazioni_annue[C.A2]},{C.A3}:{variazioni_annue[C.A3]}')
#print_dict(dizionario)
    