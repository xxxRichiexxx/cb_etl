DROP TABLE IF EXISTS sttgaz.stage_cb_stavka;
CREATE TABLE sttgaz.stage_cb_stavka(
    DT TIMESTAMP,
    rate NUMERIC(4,2),
    load_date date
)
ORDER BY DT
PARTITION BY load_date;	