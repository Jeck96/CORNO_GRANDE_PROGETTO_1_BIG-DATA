DROP TABLE IF EXISTS stock_price;

CREATE TABLE stock_price(
	ticker STRING,
	open FLOAT,
	close FLOAT,
	adj_close FLOAT,
	low_price FLOAT,
	high_price FLOAT,
	volume INT,
	data DATE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv'
OVERWRITE INTO TABLE stock_price;


--/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices_update.csv
