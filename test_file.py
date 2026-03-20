Взял новый источник core_flow_ing_raw.raw_bplineitemextensio
Беру dm, по которому не протянулось поле pddate (в сапе оно 2026-02-20)
Нахожу нужны строки в core_flow_ing_raw.raw_bplineitemextensio по номеру транзакции
И там нет PDDATE в fieldname (только DM)
select *
from ice_baby.core_flow_ing_raw.raw_bplineitemextensio
where true 
and transactionsequencenumber = '100000000009451237'
and fieldname IN ('PDDATE', 'DATAMATRIX')
and businessdaydate = date '2026-03-19'
and load_date_xml = date '2026-03-19';
Олегу вопросы скинул - пока не ответил
(про NULL в pst_pddate и про странные значения в САПе 0000-00-00 и 1990-01-01)
Пример записи (скрин как раз выше), по которой не заполнено pddate в core_flow_ing_raw.raw_bplineitemextensio (и в старой staging_.bplineitemextensio_new_pst тоже не заполнено) 
Вот ключевые поля
transactionsequencenumber = '100000000009451237'
retailstoreid = '5695'
workstationid = '1'
businessdaydate = date '2026-03-19'
В КАРе pd_date = 2026-02-20
18:04
Переслано от
Martynenko, Sergey
17:42
Пример 2 - нет pd_date
а в САПе 2026-02-05
-- EXAMPLE 2 - нет pd_date
select *
from ice_baby.core_flow_ing_raw.raw_bplineitemextensio
where true 
and fieldvalue = '0105035766060253215MlAWXU' -- transnumer = 100000000005294220
and businessdaydate = date '2026-03-19'
and load_date_xml = date '2026-03-19';
select *
from ice_baby.core_flow_ing_raw.raw_bplineitemextensio
where true 
and transactionsequencenumber = '100000000005294220'
and retailstoreid = '2465'
and fieldname IN ('PDDATE', 'DATAMATRIX')
and businessdaydate = date '2026-03-19'
and load_date_xml = date '2026-03-19';
transactionsequencenumber = '100000000005294220'
retailstoreid = '2465'
workstationid = '2'
businessdaydate = date '2026-03-19'
18:04
Переслано от
Martynenko, Sergey
17:44
Логика такая:
1) находим по фильтру на dm номер транзакции (transactionsequencenumber )
2) и потом выполняем второй запрос с фильтром на эту транзакцию и магазин
18:04
Переслано от
Martynenko, Sergey
17:52
Пример 3 - здесь в САПе лажа (1999-01-01)
-- EXAMPLE 3 - нет pd_date
select *
from ice_baby.core_flow_ing_raw.raw_bplineitemextensio
where true 
and fieldvalue = '0104810014020927215qZoKQV' -- transnumer = 100000000009013632
and businessdaydate = date '2026-03-19'
and load_date_xml = date '2026-03-19';
select *
from ice_baby.core_flow_ing_raw.raw_bplineitemextensio
where true 
and transactionsequencenumber = '100000000009013632'
and retailstoreid = '7252'
and fieldname IN ('PDDATE', 'DATAMATRIX')
and businessdaydate = date '2026-03-19'
and load_date_xml = date '2026-03-19';
transactionsequencenumber = '100000000009013632'
retailstoreid = '7252'
workstationid = '1'
businessdaydate = date '2026-03-19'
