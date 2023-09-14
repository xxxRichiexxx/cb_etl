DROP TABLE IF EXISTS sttgaz.stage_cb_stavka;
CREATE TABLE sttgaz.stage_cb_stavka(
    DT TIMESTAMP,
    rate NUMERIC(4,2),
    load_date date
)
ORDER BY DT
PARTITION BY load_date;	

DROP TABLE IF EXISTS sttgaz.stage_cb_news;
CREATE TABLE sttgaz.stage_cb_news(
	Doc_id INT,
	DocDate TIMESTAMP,
    Title  VARCHAR(1000),
    url VARCHAR(500),
    load_date date
)
ORDER BY DocDate
PARTITION BY load_date;	
	