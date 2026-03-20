-- Добавить is_aligned_tran в таблицы где её нет
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bptransactextensio ADD COLUMN is_aligned_tran BOOLEAN;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bptransactdiscext ADD COLUMN is_aligned_tran BOOLEAN;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bplineitemextensio ADD COLUMN is_aligned_tran BOOLEAN;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscount ADD COLUMN is_aligned_tran BOOLEAN;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bplineitemdiscext ADD COLUMN is_aligned_tran BOOLEAN;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bptenderextensions ADD COLUMN is_aligned_tran BOOLEAN;

-- Переименовать is_corr_receipt → is_aligned_tran
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bpretaillineitem RENAME COLUMN is_corr_receipt TO is_aligned_tran;
ALTER TABLE ice_baby.core_flow_ing_raw.raw_bptender RENAME COLUMN is_corr_receipt TO is_aligned_tran;
