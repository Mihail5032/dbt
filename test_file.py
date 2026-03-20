-- Пример 1: ищем PDDATE в RAW
SELECT fieldname, fieldvalue, retailsequencenumber
FROM ice_baby.raw_table.raw_bplineitemextensio
WHERE transactionsequencenumber = '100000000009451237'
AND retailstoreid = '5695'
AND fieldname IN ('PDDATE', 'DATAMATRIX')
AND businessdaydate = date '2026-03-19';

-- Пример 2: ищем PDDATE в RAW
SELECT fieldname, fieldvalue, retailsequencenumber
FROM ice_baby.raw_table.raw_bplineitemextensio
WHERE transactionsequencenumber = '100000000005294220'
AND retailstoreid = '2465'
AND fieldname IN ('PDDATE', 'DATAMATRIX')
AND businessdaydate = date '2026-03-19';

-- Пример 3: ищем PDDATE в RAW
SELECT fieldname, fieldvalue, retailsequencenumber
FROM ice_baby.raw_table.raw_bplineitemextensio
WHERE transactionsequencenumber = '100000000009013632'
AND retailstoreid = '7252'
AND fieldname IN ('PDDATE', 'DATAMATRIX')
AND businessdaydate = date '2026-03-19';
