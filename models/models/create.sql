-- Создать схему
CREATE SCHEMA IF NOT EXISTS ice_baby.core_flow_ing_raw
WITH (location = 's3a://warehouse/core_flow_ing_raw');

-- 11 PST таблиц (схема из PST таблиц test_staging, имена как в raw_table)
CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransaction
AS SELECT * FROM ice_baby.test_staging.bptransaction_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpfinancialmovemen
AS SELECT * FROM ice_baby.test_staging.bpfinancialmovemen_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactextensio
AS SELECT * FROM ice_baby.test_staging.bptransactextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpfinacialmovement
AS SELECT * FROM ice_baby.test_staging.bpfinancialmovementextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpretaillineitem
AS SELECT * FROM ice_baby.test_staging.bpretaillineitem_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemextensio
AS SELECT * FROM ice_baby.test_staging.bplineitemextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscount
AS SELECT * FROM ice_baby.test_staging.bplineitemdiscount_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscext
AS SELECT * FROM ice_baby.test_staging.bplineitemdiscountext_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactdiscext
AS SELECT * FROM ice_baby.test_staging.bptransactdiscext_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptender
AS SELECT * FROM ice_baby.test_staging.bptender_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptenderextensions
AS SELECT * FROM ice_baby.test_staging.bptenderextensions_new_pst_2 WHERE 1=0;

-- 5 RAW таблиц (схема из raw_table)
CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpsourcedocumentli
AS SELECT * FROM ice_baby.raw_table.raw_bpsourcedocumentli WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemtax
AS SELECT * FROM ice_baby.raw_table.raw_bplineitemtax WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpretailtotals
AS SELECT * FROM ice_baby.raw_table.raw_bpretailtotals WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptendertotals
AS SELECT * FROM ice_baby.raw_table.raw_bptendertotals WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactiondisco
AS SELECT * FROM ice_baby.raw_table.raw_bptransactiondisco WHERE 1=0;
