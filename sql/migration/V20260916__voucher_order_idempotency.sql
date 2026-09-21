-- Final database guard: at most one NORMAL order per user and voucher.
-- Cancelled orders produce NULL in active_voucher_id, so a later purchase remains possible.
-- Existing duplicate NORMAL orders must be cleaned before applying this migration.
ALTER TABLE hmdp_0.tb_voucher_order_0
    ADD COLUMN active_voucher_id BIGINT UNSIGNED
        GENERATED ALWAYS AS (CASE WHEN status = 1 THEN voucher_id ELSE NULL END) STORED,
    ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id);
ALTER TABLE hmdp_0.tb_voucher_order_1
    ADD COLUMN active_voucher_id BIGINT UNSIGNED
        GENERATED ALWAYS AS (CASE WHEN status = 1 THEN voucher_id ELSE NULL END) STORED,
    ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id);
ALTER TABLE hmdp_1.tb_voucher_order_0
    ADD COLUMN active_voucher_id BIGINT UNSIGNED
        GENERATED ALWAYS AS (CASE WHEN status = 1 THEN voucher_id ELSE NULL END) STORED,
    ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id);
ALTER TABLE hmdp_1.tb_voucher_order_1
    ADD COLUMN active_voucher_id BIGINT UNSIGNED
        GENERATED ALWAYS AS (CASE WHEN status = 1 THEN voucher_id ELSE NULL END) STORED,
    ADD UNIQUE KEY uk_voucher_order_user_active_voucher (user_id, active_voucher_id);
