#!/usr/bin/python3
"""output_formatter.py"""

fo = open('result/to_formatting.txt','r')
list_variazioni =[]
for line in fo:
    line = line.split("\\")
    list_variazioni.append((line[0],line[1]))

dizionario_triplette = {}
for nome,tripletta in list_variazioni:
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
    print(f'{a}->{{{v}}}')