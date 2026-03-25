-- ============================================================
-- CREATE TABLE statements for core_flow_ice.core_flow_ing_raw
-- Based on reference schemas (эталон)
-- ============================================================

-- 1. raw_bpfinacialmovement
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bpfinacialmovement (
    rtl_txn_rk varbinary,
    rtl_txn_fin_rk varbinary,
    retailstoreid varchar,
    businessdaydate date,
    transactiontypecode varchar,
    workstationid varchar,
    transactionsequencenumber varchar,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 2. raw_bpfinancialmovemen
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bpfinancialmovemen (
    rtl_txn_rk varbinary,
    rtl_txn_fin_rk varbinary,
    amount decimal(32,2),
    businessdaydate date,
    financialcurrency varchar,
    financialcurrency_iso varchar,
    financialsequencenumber varchar,
    financialtypecode varchar,
    retailstoreid varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    refererenceid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 3. raw_bplineitemdiscext
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bplineitemdiscext (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    rtl_txn_item_disc_rk varbinary,
    retailstoreid varchar,
    businessdaydate date,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    workstationid varchar,
    transactionsequencenumber varchar,
    retailsequencenumber integer,
    discountsequencenumber integer,
    transactiontypecode varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 4. raw_bplineitemdiscount
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bplineitemdiscount (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    rtl_txn_item_disc_rk varbinary,
    businessdaydate date,
    discountid varchar,
    discountsequencenumber integer,
    discounttypecode varchar,
    reductionamount decimal(32,2),
    retailsequencenumber integer,
    retailstoreid varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 5. raw_bplineitemextensio
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bplineitemextensio (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    businessdaydate date,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    retailsequencenumber integer,
    retailstoreid varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 6. raw_bplineitemtax
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bplineitemtax (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    rtl_txn_item_tax_rk varbinary,
    businessdaydate date,
    retailsequencenumber integer,
    retailstoreid varchar,
    taxamount decimal(32,2),
    taxsequencenumber integer,
    taxtypecode varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_ts_xml timestamp(6),
    load_date_xml date,
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 7. raw_bpretaillineitem
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bpretaillineitem (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    businessdaydate date,
    itemid varchar,
    itemidentrymethodcode varchar,
    itemidqualifier varchar,
    normalsalesamount decimal(32,2),
    promotionid varchar,
    retailquantity decimal(32,3),
    retailsequencenumber integer,
    retailstoreid varchar,
    retailtypecode varchar,
    retailreasoncode varchar,
    salesamount decimal(32,2),
    salesunitofmeasure varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    units decimal(32,3),
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 8. raw_bpretailtotals
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bpretailtotals (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    businessdaydate date,
    lineitemcount integer,
    retailstoreid varchar,
    retailtypecode varchar,
    salesamount decimal(32,2),
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_ts_xml timestamp(6),
    load_date_xml date,
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 9. raw_bpsourcedocumentli
-- NOTE: В этой таблице НЕТ rtl_txn_rk, поэтому sorted_by только по load_ts_xml
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bpsourcedocumentli (
    key varchar,
    type varchar,
    logicalsystem varchar,
    is_aligned_tran boolean,
    load_ts timestamp(6),
    load_ts_xml timestamp(6),
    load_date_xml date
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['load_ts_xml']
);

-- 10. raw_bptender
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptender (
    rtl_txn_rk varbinary,
    rtl_txn_tender_rk varbinary,
    transactionsequencenumber varchar,
    retailstoreid varchar,
    businessdaydate date,
    transactiontypecode varchar,
    workstationid varchar,
    tenderamount decimal(38,2),
    tendercurrency varchar,
    tenderid varchar,
    tendersequencenumber integer,
    tendertypecode varchar,
    referenceid varchar,
    accountnumber varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 11. raw_bptenderextensions
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptenderextensions (
    rtl_txn_rk varbinary,
    rtl_txn_tender_rk varbinary,
    businessdaydate date,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    retailstoreid varchar,
    tendersequencenumber integer,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 12. raw_bptendertotals
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptendertotals (
    rtl_txn_rk varbinary,
    rtl_txn_item_rk varbinary,
    actualamount decimal(32,2),
    bankamount decimal(32,2),
    businessdaydate date,
    overamount decimal(32,2),
    removalamount decimal(32,2),
    removalcount integer,
    retailstoreid varchar,
    shortamount decimal(32,2),
    tenderamount decimal(32,2),
    tendercount integer,
    tendertypecode varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_ts_xml timestamp(6),
    load_date_xml date,
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 13. raw_bptransactdiscext
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptransactdiscext (
    rtl_txn_rk varbinary,
    rtl_txn_disc_rk varbinary,
    retailstoreid varchar,
    businessdaydate date,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    workstationid varchar,
    transactionsequencenumber varchar,
    discountsequencenumber integer,
    transactiontypecode varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 14. raw_bptransactextensio
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptransactextensio (
    rtl_txn_rk varbinary,
    businessdaydate date,
    fieldgroup varchar,
    fieldname varchar,
    fieldvalue varchar,
    retailstoreid varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 15. raw_bptransaction
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptransaction (
    rtl_txn_rk varbinary,
    begindatetimestamp timestamp(6),
    businessdaydate date,
    enddatetimestamp timestamp(6),
    operatorid varchar,
    retailstoreid varchar,
    transactioncurrency varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_date_xml date,
    load_ts_xml timestamp(6),
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);

-- 16. raw_bptransactiondisco
CREATE TABLE IF NOT EXISTS core_flow_ice.core_flow_ing_raw.raw_bptransactiondisco (
    rtl_txn_rk varbinary,
    rtl_txn_disc_rk varbinary,
    bonusbuyid varchar,
    businessdaydate date,
    discountid varchar,
    discountidqualifier varchar,
    discountreasoncode varchar,
    discountsequencenumber integer,
    discounttypecode varchar,
    offerid varchar,
    reductionamount decimal(32,2),
    retailstoreid varchar,
    storefinancialledgeraccountid varchar,
    transactionsequencenumber varchar,
    transactiontypecode varchar,
    workstationid varchar,
    load_ts timestamp(6),
    load_ts_xml timestamp(6),
    load_date_xml date,
    is_aligned_tran boolean
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['load_date_xml'],
    sorted_by = ARRAY['rtl_txn_rk', 'load_ts_xml']
);
