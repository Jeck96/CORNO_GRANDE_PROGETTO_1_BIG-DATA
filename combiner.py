#!/usr/bin/python3
"""combiner.py"""

import sys
import json

def toJson(ticker,low,high,sum_volum,count_volume,fist_date,last_date,first_price,last_price):
    dic = { "ticker" : ticker, 
            "low" : low,
            "high" : high,
            "sum_volume" : sum_volum,
            "count_volume" : count_volume,
            "first_date" : fist_date,
            "first_price" : first_price,
            "last_date" : last_date,
            "last_price" : last_price
    }
    return json.dumps(dic)

dizionario = {}
for line in sys.stdin:
    #Costruiamo un dizionario con chiave il simbolo dell'azione e
    # valore l'azione che comprende quindi tutti i campi
    line = line.split(',',1)
    simbolo = line[0]
    azione = json.loads(line[1])
    del line
    """
    formato dell'oggetto caricato:
    {
        "ticker" : "GDR",
        "close" : "9.743632",
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
result = []
for k in dizionario:
    all_price = [] 
    sum_volums = 0
    count = 0
    for a in dizionario[k]:
        all_price.append(float(a['close']))
        sum_volums+=(int(a['volume']))
        count=count+1

    lowest_price = min(all_price)
    hieght_price = max(all_price)
    #average_volume = sum_volums/len(dizionario[k])
    
    del all_price
    
    #calcolo variazione percentuale di prezzo
    dizionario[k] = sorted(dizionario[k], key=lambda a: a['date']) #ordino per data
    first_date = dizionario[k][0]['date']
    initial_price = float(dizionario[k][0]['close'])
    final_price = float(dizionario[k][len(dizionario[k])-1]['close'])
    last_date = dizionario[k][len(dizionario[k])-1]['date']

    print(f'{k},{toJson(k,lowest_price,hieght_price,sum_volums,count,first_date,last_date,initial_price,final_price)}')
    