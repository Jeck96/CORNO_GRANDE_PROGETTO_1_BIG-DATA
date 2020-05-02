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

LOAD DATA INPATH 'input/hist_sto_pri_pt1.csv'
OVERWRITE INTO TABLE stock_price;


--/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices_update.csv
--input/hist_sto_pri_pt1.csv
--input/historical_stock_prices_update.csv