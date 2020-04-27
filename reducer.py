#!/usr/bin/python3
"""reducer.py"""

import sys
dizionario = {}
for line in sys.stdin:
    #Costruiamo un dizionario con chiave il simbolo dell'azione e
    # valore l'azione che comprende quindi tutti i campi
    line = line.split('\t')
    simbolo = line[0]
    azione = line[1]
    #azione = {'prezzo_min':line[1][4]}

    if dizionario.get(simbolo):
       dizionario[simbolo].append(azione)
    else:
        dizionario[simbolo] = [azione]




    #calcolo del prezzo minimo per ogni azione:
    """
    if dizionario.get(simbolo):
        if (int)dizionario[simbolo]>costo_attuale
    """

print (dizionario)