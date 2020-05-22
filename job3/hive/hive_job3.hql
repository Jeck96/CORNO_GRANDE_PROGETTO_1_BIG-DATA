DROP TABLE IF EXISTS stock_price;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS unixtable;
DROP TABLE IF EXISTS dataMinMax;
DROP TABLE IF EXISTS initialPrice;
DROP TABLE IF EXISTS finalPrice;
DROP TABLE IF EXISTS variazioni;
DROP TABLE IF EXISTS result;

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

CREATE TABLE stocks(
	ticker STRING,
    exchamge STRING,
    name STRING,
    sector STRING,
    industry STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'input/historical_stock_prices_3.csv'
OVERWRITE INTO TABLE stock_price;

LOAD DATA INPATH 'input/historical_stocks.csv'
OVERWRITE INTO TABLE stocks;

CREATE TABLE unixtable as
    SELECT stocks.ticker,name,close,data,YEAR(data) as anno
    FROM stock_price JOIN stocks ON stock_price.ticker=stocks.ticker
    WHERE YEAR(stock_price.data)>='2016';

CREATE TABLE dataMinMax as
    SELECT name, anno , min(TO_DATE(data)) as dataMin,  max(TO_DATE(data)) as dataMax
    FROM unixtable
    GROUP BY name,anno;

CREATE TABLE initialPrice as
    SELECT ut.name, ut.anno ,SUM(ut.close) as price
    FROM unixtable as ut,dataMinMax as dmm
    WHERE ut.name=dmm.name AND ut.anno=dmm.anno AND ut.data = dmm.dataMin
    GROUP BY ut.name,ut.anno;

CREATE TABLE finalPrice as
    SELECT ut.name, ut.anno ,SUM(ut.close)  as price
    FROM unixtable as ut,dataMinMax as dmm
    WHERE ut.name=dmm.name AND ut.anno=dmm.anno AND ut.data = dmm.dataMax
    GROUP BY ut.name,ut.anno;

CREATE TABLE variazioni as
    SELECT initialPrice.name, initialPrice.anno , round((100*(finalPrice.price-initialPrice.price)/initialPrice.price),0) AS var
    FROM initialPrice JOIN finalPrice ON initialPrice.name=finalPrice.name AND initialPrice.anno = finalPrice.anno;

CREATE TABLE result as 
    SELECT v1.name, '2016:'||v1.var||'%, 2017:'||v2.var||'%, 2018:'||v3.var||'%' as tripletta
    FROM (SELECT * FROM variazioni WHERE anno = '2016') as v1,
        (SELECT * FROM variazioni WHERE anno = '2017')as v2, 
        (SELECT * FROM variazioni WHERE anno = '2018')as v3
    WHERE v1.name = v2.name AND v1.name = v3.name
    ORDER BY tripletta;

--SELECT * FROM result;