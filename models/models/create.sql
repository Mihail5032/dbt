-- Создать схему
CREATE SCHEMA IF NOT EXISTS ice_baby.core_flow_ing_raw
WITH (location = 's3a://warehouse/core_flow_ing_raw');

-- === 11 PST таблиц (схема из test_staging, нейминг из raw_table) ===

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransaction
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bptransaction_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpfinancialmovemen
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bpfinancialmovemen_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactextensio
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bptransactextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpfinacialmovement
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bpfinancialmovementextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpretaillineitem
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bpretaillineitem_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemextensio
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bplineitemextensio_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscount
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bplineitemdiscount_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscext
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bplineitemdiscountext_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactdiscext
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bptransactdiscext_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptender
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bptender_new_pst_2 WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptenderextensions
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.test_staging.bptenderextensions_new_pst_2 WHERE 1=0;

-- === 5 RAW таблиц (схема из raw_table) ===

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpsourcedocumentli
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'])
AS SELECT * FROM ice_baby.raw_table.raw_bpsourcedocumentli WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bplineitemtax
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.raw_table.raw_bplineitemtax WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bpretailtotals
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.raw_table.raw_bpretailtotals WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptendertotals
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.raw_table.raw_bptendertotals WHERE 1=0;

CREATE TABLE ice_baby.core_flow_ing_raw.raw_bptransactiondisco
WITH (format = 'PARQUET', partitioning = ARRAY['load_date_xml'], sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml'])
AS SELECT * FROM ice_baby.raw_table.raw_bptransactiondisco WHERE 1=0;
