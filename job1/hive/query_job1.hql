--questa query corrisponde al punto b del Job 1. In particolare viene fatto un join tra
--la tabella stok_price con una tabella che è il risultato di una query dove per ogni
--simbolo abbiamo la data minima e la data massima.
--Ancora viene fatto un join con la tabella stock_price. Il primo join con la tabella
--stock_price serve per prendere il valore di chiusura finale dell'azione (quindi quello --con la cui tupla contiene come data data_max) il secondo join serve per prendere il valore
--di chiusura iniziale (quindi quello con cui la tupla contiene come data: data_min)

--INSERT OVERWRITE DIRECTORY 'output/result_hive_partial'
SELECT var_perc.ticker, variazione_percentuale, prezzo_minimo, prezzo_massimo, volume_medio
FROM 
		(SELECT finale.ticker, (100*(finale.close-iniziale.close)/iniziale.close) AS variazione_percentuale
		FROM stock_price AS finale 
				JOIN
			(SELECT ticker,min(data) AS data_min, max(data) AS data_max
			 FROM stock_price
			 WHERE data>='2008-01-01'
			 GROUP BY ticker) AS data_min_max 
			 	ON finale.ticker = data_min_max.ticker 
			 	JOIN
			stock_price AS iniziale 
				ON finale.ticker=iniziale.ticker
		WHERE finale.data = data_min_max.data_max AND iniziale.data = data_min_max.data_min) AS var_perc
	JOIN
		(SELECT ticker, min(close) AS prezzo_minimo, max(close) AS prezzo_massimo, avg(volume) AS volume_medio
		FROM	stock_price
		WHERE data>='2008-01-01'
		GROUP BY ticker) AS min_max_avg
	ON var_perc.ticker=min_max_avg.ticker;
--ORDER BY variazione_percentuale DESC;



--questa query riporta i risultati dei punti a,c,d,e,f del Job 1
