#!/usr/bin/python3
"""reducer.py"""

import sys
import json
import costanct as C
import pandas as pd

aziende = pd.read_csv("/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stocks.csv", sep=',')
aziende = aziende.values
azienda_map = {}
for row in aziende:
    ticker,_,name,_,_ = row
    azienda_map[ticker] = name
del aziende

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
        "close": 17.2984y92,
        "date" : 2017-01-25,
    }
    """
    #associo il nome dell'azienda al ticker
    azione['name'] = azienda_map[azione['ticker']]
    #Riempimento del dizionario
    nome = azione['name']
    if dizionario.get(nome):
       dizionario[nome].append(azione)
    else:
        dizionario[nome] = [azione]

def print_dict(dic):
    for k in dic:
        print(k,dic[k])
        """
        for v in dic[k]:
            print('\t',v)
        print("\n")
        """
anni_map={}
variazioni_list = []
for k in dizionario:
    anni_map[C.A1] = []
    anni_map[C.A2] = []
    anni_map[C.A3] = []
    for v in dizionario[k]:
        anno_azione = int(v['date'].split('-')[0])
        anni_map[anno_azione].append(v)
    variazioni_annue = {}
    flag = True
    for anno in C.TRIENNIO:
        if anni_map[anno] != []:
            anni_map[anno] = sorted(anni_map[anno],key=lambda a: a['date'])
            initial_price = float(anni_map[anno][0]['close'])
            final_price = float(anni_map[anno][len(anni_map[anno])-1]['close'])
            variazioni_annue[anno] = round(100*(final_price-initial_price)/initial_price,0)
        else:
            flag = False
    if(flag):
        tmp = (k,(variazioni_annue[C.A1],variazioni_annue[C.A2],variazioni_annue[C.A3]))
        variazioni_list.append(tmp)
    #print(f'{k}:\n{C.A1}:{variazioni_annue[C.A1]},{C.A2}:{variazioni_annue[C.A2]},{C.A3}:{variazioni_annue[C.A3]}')
del dizionario
#variazioni_list = sorted(variazioni_list,key=lambda x: (x[1][0],x[1][1],x[1][2]),reverse=True)
dizionario_triplette = {}
for nome,tripletta in variazioni_list:
    if(dizionario_triplette.get(tripletta)):
        dizionario_triplette[tripletta].append(nome)
    else:
        dizionario_triplette[tripletta] = [nome]
result_list = []
for k in dizionario_triplette:
    result_list.append((dizionario_triplette[k],k))
del dizionario_triplette
result_list = sorted(result_list,key=lambda c: len(c[0]),reverse=True)
for a,v in result_list:
    print(f'{a}->{{{C.A1}:{v[0]}%, {C.A2}:{v[1]}% ,{C.A3}:{v[2]}%}}')
#print_dict(dizionario_triplette)
    