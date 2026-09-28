"""Run only with an explicitly supplied disposable MySQL socket; never use app settings."""
import argparse
import pathlib
import subprocess
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]


def verify(socket):
    schema = 'identity_upgrade_' + uuid.uuid4().hex

    def mysql(sql, database=None, ok=True):
        args = ['mysql', '--no-defaults', '--protocol=SOCKET', '--socket=' + socket,
                '-uroot', '--batch', '--skip-column-names']
        if database:
            args.append(database)
        result = subprocess.run(args, input=sql, text=True, capture_output=True)
        if ok and result.returncode:
            raise AssertionError(result.stderr)
        if not ok and not result.returncode:
            raise AssertionError('Unsafe upgrade unexpectedly succeeded')
        return result.stdout.strip() if ok else result.stderr

    mysql('CREATE DATABASE ' + schema)
    try:
        for n in (0, 1):
            mysql(f'''
CREATE TABLE tb_seckill_request_{n}(id BIGINT PRIMARY KEY,status VARCHAR(16),reservation_id VARCHAR(64) NOT NULL);
CREATE TABLE tb_seckill_active_purchase_{n}(voucher_id BIGINT,user_id BIGINT,order_id BIGINT,reservation_id VARCHAR(64) NOT NULL,PRIMARY KEY(voucher_id,user_id));
CREATE TABLE tb_seckill_voucher_{n}(voucher_id BIGINT PRIMARY KEY,reserved_stock INT,admission_state VARCHAR(16),admission_epoch BIGINT);
CREATE TABLE tb_seckill_outbox_{n}(event_id VARCHAR(64) PRIMARY KEY,status VARCHAR(16),event_type VARCHAR(16));
INSERT INTO tb_seckill_request_{n} VALUES(101,'SUCCEEDED','old-uuid'),(102,'CANCELLED','cancelled-uuid');
INSERT INTO tb_seckill_active_purchase_{n} VALUES(1,7,101,'old-uuid');
INSERT INTO tb_seckill_voucher_{n} VALUES(1,0,'OPEN',7);
INSERT INTO tb_seckill_outbox_{n} VALUES('future-reminder','PENDING','REMINDER');
''', schema)
        upgrade = (ROOT / 'sql/v2/upgrade_order_identity.sql').read_text()
        mysql("UPDATE tb_seckill_request_0 SET status='PROCESSING' WHERE id=101", schema)
        assert 'Drain processing' in mysql(upgrade, schema, ok=False)
        assert mysql('SELECT admission_epoch FROM tb_seckill_voucher_0', schema) == '7'
        mysql("UPDATE tb_seckill_request_0 SET status='SUCCEEDED' WHERE id=101; INSERT INTO tb_seckill_outbox_1 VALUES('pending','PENDING','REDIS')", schema)
        assert 'Drain actionable Outbox' in mysql(upgrade, schema, ok=False)
        mysql("UPDATE tb_seckill_outbox_1 SET status='SENT' WHERE event_id='pending'", schema)
        mysql(upgrade, schema)
        assert mysql("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND column_name='reservation_id'", schema) == '0'
        for n in (0, 1):
            assert mysql(f'SELECT id,status FROM tb_seckill_request_{n} ORDER BY id', schema) == '101\tSUCCEEDED\n102\tCANCELLED'
            assert mysql(f'SELECT order_id FROM tb_seckill_active_purchase_{n}', schema) == '101'
            assert mysql(f'SELECT admission_state,admission_epoch FROM tb_seckill_voucher_{n}', schema) == 'REBUILDING\t8'
        assert 'Expected old schema' in mysql(upgrade, schema, ok=False)
        assert mysql('SELECT admission_epoch FROM tb_seckill_voucher_0', schema) == '8'
        print('Order identity upgrade guards and history preservation passed on ' + socket)
    finally:
        mysql('DROP DATABASE ' + schema)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    verify(parser.parse_args().socket)
