-- Apply once to EACH existing shard schema, with all writers/workers stopped.
-- First drain Kafka V2/V3, Redis HELD reservations and actionable Outbox events.
-- Back up both schemas and old Redis state before running. MySQL DDL is not atomic.
-- Never run with mysql --force. Fresh installations use new_tables.sql instead.
DELIMITER $$
DROP PROCEDURE IF EXISTS upgrade_seckill_order_identity$$
CREATE PROCEDURE upgrade_seckill_order_identity()
BEGIN
  IF (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema=DATABASE() AND column_name='reservation_id'
        AND table_name IN ('tb_seckill_request_0','tb_seckill_request_1',
                           'tb_seckill_active_purchase_0','tb_seckill_active_purchase_1')) <> 4 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Expected old schema; upgrade already applied or partially applied';
  END IF;
  IF EXISTS (SELECT 1 FROM tb_seckill_request_0 WHERE status='PROCESSING')
     OR EXISTS (SELECT 1 FROM tb_seckill_request_1 WHERE status='PROCESSING')
     OR EXISTS (SELECT 1 FROM tb_seckill_voucher_0 WHERE reserved_stock<>0)
     OR EXISTS (SELECT 1 FROM tb_seckill_voucher_1 WHERE reserved_stock<>0) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Drain processing requests before upgrade';
  END IF;
  IF EXISTS (SELECT 1 FROM tb_seckill_outbox_0 WHERE status<>'SENT' AND event_type<>'REMINDER')
     OR EXISTS (SELECT 1 FROM tb_seckill_outbox_1 WHERE status<>'SENT' AND event_type<>'REMINDER') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Drain actionable Outbox before upgrade';
  END IF;
  -- Force an authoritative snapshot in the NEW Redis namespace at startup.
  -- Preserve PAUSED/INVARIANT activities for manual diagnosis.
  START TRANSACTION;
  UPDATE tb_seckill_voucher_0 SET admission_state='REBUILDING',admission_epoch=admission_epoch+1
    WHERE admission_state='OPEN';
  UPDATE tb_seckill_voucher_1 SET admission_state='REBUILDING',admission_epoch=admission_epoch+1
    WHERE admission_state='OPEN';
  COMMIT;
  ALTER TABLE tb_seckill_request_0 DROP COLUMN reservation_id;
  ALTER TABLE tb_seckill_request_1 DROP COLUMN reservation_id;
  ALTER TABLE tb_seckill_active_purchase_0 DROP COLUMN reservation_id;
  ALTER TABLE tb_seckill_active_purchase_1 DROP COLUMN reservation_id;
END$$
CALL upgrade_seckill_order_identity()$$
DROP PROCEDURE upgrade_seckill_order_identity$$
DELIMITER ;
