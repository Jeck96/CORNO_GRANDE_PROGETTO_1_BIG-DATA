DROP TABLE IF EXISTS azioni_test;

CREATE TABLE azioni_test(
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

LOAD DATA LOCAL INPATH '/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/file_grandi/historical_stock_prices_update_02.csv'
OVERWRITE INTO TABLE azioni_test;

DROP TABLE IF EXISTS view_azioni;

CREATE TABLE view_azioni AS(
	SELECT ticker,open,close,volume,data
	FROM azioni_test
	WHERE YEAR(data)>=2008);

DROP TABLE IF EXISTS info_azioni;

CREATE TABLE info_azioni AS(
	SELECT ticker,min(data) AS data_min, max(data) AS data_max, min(close) AS prezzo_minimo, max(close) AS prezzo_massimo, avg(volume) AS volume_medio
	FROM view_azioni
	GROUP BY ticker);

DROP TABLE IF EXISTS results;

CREATE TABLE results AS(
	SELECT finale.ticker, (100*(finale.close-iniziale.close)/iniziale.close) AS variazione_percentuale, prezzo_minimo, prezzo_massimo, volume_medio
	FROM view_azioni AS finale 
			JOIN
	 	info_azioni AS ia 
	 		ON finale.ticker = ia.ticker 
		 	JOIN
		view_azioni AS iniziale 
			ON finale.ticker=iniziale.ticker
	WHERE finale.data = ia.data_max AND iniziale.data = ia.data_min);

--query con i risultati
INSERT OVERWRITE LOCAL DIRECTORY 'results'
SELECT ticker, variazione_percentuale,prezzo_minimo,prezzo_massimo,volume_medio
FROM results
ORDER BY variazione_percentuale DESC;