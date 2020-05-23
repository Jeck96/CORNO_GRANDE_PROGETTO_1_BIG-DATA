#!/usr/bin/python3
"""reducer.py"""

import sys
import json
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
        "ticker" : ticker, 
        "low" : low,
        "high" : high,
        "sum_volume" : sum_volum,
        "count_volume" : count_volume,
        "first_date" : fist_date,
        "first_price" : first_price,
        "last_date" : last_date,
        "last_price" : last_price
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
    low_prices = []
    hig_prices = []
    sum_volums = 0
    count = 0
    for a in dizionario[k]:
        low_prices.append(float(a['low']))
        hig_prices.append(float(a['high']))
        sum_volums+=(int(a['sum_volume']))
        count+=(int(a['count_volume']))

    lowest_price = min(low_prices)
    hieght_price = max(hig_prices)
    average_volume = sum_volums/count
    
    
    #calcolo variazione percentuale di prezzo
    dizionario[k] = sorted(dizionario[k], key=lambda a: a['first_date']) #ordino per first_date
    initial_price = float(dizionario[k][0]['first_price'])
    
    dizionario[k] = sorted(dizionario[k], key=lambda a: a['last_date'],reverse=True) #ordino per last_date   
    final_price = float(dizionario[k][0]['last_price'])
    x_cent_dif = 100 * (final_price-initial_price) / initial_price

    #print(f'{{\nsimbolo:{k}\nvariazone_percentuale:{x_cent_dif}%\nprezzo_min:{lowest_price}\nprezzo_massimo:{hieght_price}\nvolume_medio:{average_volume}\n}}')
 
    result.append({
        "ticker" : k,
        "variazione_percentuale" : x_cent_dif,
        "prezzo_minimo" : lowest_price,
        "prezzo_massimo" : hieght_price,
        "volume_medio" : average_volume
    })
del dizionario #risparmiamo memoria
result = sorted(result, key= lambda a: a['variazione_percentuale'],reverse=True)
for v in result:
    print(v)
    