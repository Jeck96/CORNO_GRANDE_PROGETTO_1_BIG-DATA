#!/usr/bin/python3:

with open("historical_stock_prices_update.csv",'r') as f:
    with open(".csv_progetto/historical_stock_prices_2.csv",'a') as f1: 
        for line in f:
            ticker,_,close,_,_,_,_,data = line.split(',')
            if(data>'2008-01-01'):
                f1.write(line)

 
