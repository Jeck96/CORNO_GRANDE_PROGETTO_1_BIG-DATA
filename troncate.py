#!/usr/bin/python3

with open("historical_stock_prices.csv",'r') as f:
    with open("historical_stock_prices_update.csv",'w') as f1:
        next(f) # skip header line
        for line in f:
            f1.write(line)

 
