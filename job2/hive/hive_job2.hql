---creazione tabella dal datset azioni
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

LOAD DATA LOCAL INPATH '/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/dataset/historical_stock_prices_200.csv'
OVERWRITE INTO TABLE azioni_test;


--creazione tabella dal dataset settori

DROP TABLE IF EXISTS settori_test;
CREATE TABLE settori_test(
	ticker STRING,
	echange STRING,
	name STRING,
	sector STRING,
	industry STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH '/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stocks_update_per_hive.csv' OVERWRITE INTO TABLE settori_test;


--creazione tabelle di supporto alle query job2

DROP TABLE IF EXISTS info_azioni_per_anno;
CREATE TABLE info_azioni_per_anno AS 
SELECT data_min_max.ticker,anno,(100*(finale.close-iniziale.close)/iniziale.close) AS var_annuale
FROM 
	 (SELECT at.ticker,YEAR(data) AS anno, min(data) AS data_min, max(data) AS data_max
	 FROM azioni_test at
	 WHERE YEAR(data)>=2008
	 GROUP BY at.ticker,YEAR(data) ) AS data_min_max 
	JOIN azioni_test AS finale ON (data_min_max.ticker=finale.ticker AND finale.data=data_min_max.data_max)
	JOIN azioni_test AS iniziale ON (iniziale.ticker=finale.ticker AND iniziale.data=data_min_max.data_min);

DROP TABLE IF EXISTS settori_raggruppati;
CREATE TABLE settori_raggruppati AS (SELECT ticker,sector FROM settori_test GROUP BY ticker,sector);

--query

--punti: a,c (volume medio e variazione giornaliera media)
SELECT st.sector,YEAR(data), avg(volume), avg(at.close) as var_giornaliera
FROM settori_raggruppati AS st
	JOIN
      azioni_test AS at ON st.ticker=at.ticker
WHERE YEAR(data)>=2008
GROUP BY st.sector,YEAR(data);


--b (variazione annualle media)
SELECT st.sector, it.anno, avg(var_annuale)
FROM info_azioni_per_anno AS it JOIN settori_raggruppati AS st ON it.ticker=st.ticker
GROUP BY st.sector,it.anno;
