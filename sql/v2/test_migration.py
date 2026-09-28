import importlib.util
from pathlib import Path
import tempfile
import json
import argparse
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'seckill_v2_migrate.py'
spec = importlib.util.spec_from_file_location('migration', SCRIPT)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class MigrationTest(unittest.TestCase):
    def fixture(self):
        return {
            'tb_voucher': [{'id': 5}],
            'tb_seckill_voucher': [{'id': 91, 'voucher_id': 5, 'init_stock': 3, 'stock': 2}],
            'tb_voucher_order': [{'id': 100, 'voucher_id': 5, 'user_id': 7, 'status': 1},
                                 {'id': 101, 'voucher_id': 5, 'user_id': 7, 'status': 2}],
            'tb_voucher_reconcile_log': [{'id': 201, 'voucher_id': 5, 'order_id': 100}],
        }

    def test_four_nodes_and_large_ids(self):
        self.assertEqual([migration.route(v) for v in range(4)], [(0, 0), (1, 0), (0, 1), (1, 1)])
        self.assertEqual(migration.route(9007199254740995), (1, 1))

    def test_reject_duplicate_active_purchases(self):
        rows = self.fixture()
        rows['tb_voucher_order'].append({'id': 102, 'voucher_id': 5, 'user_id': 7, 'status': 1})
        with self.assertRaisesRegex(ValueError, 'duplicate active'):
            migration.transform(rows, [])

    def test_reject_stock_inconsistency(self):
        rows = self.fixture()
        rows['tb_seckill_voucher'][0]['stock'] = 1
        with self.assertRaisesRegex(ValueError, 'conservation'):
            migration.transform(rows, [])

    def test_preserve_history_create_requests_and_bindings(self):
        rows = self.fixture()
        transformed = migration.transform(rows, [])
        self.assertEqual(transformed['tb_voucher_order'], rows['tb_voucher_order'])
        self.assertEqual(transformed['tb_voucher_reconcile_log'], rows['tb_voucher_reconcile_log'])
        self.assertEqual(len(transformed['tb_seckill_active_purchase']), 1)
        self.assertEqual(transformed['tb_seckill_active_purchase'][0]['order_id'], 100)
        states = {r['id']: r['status'] for r in transformed['tb_seckill_request']}
        self.assertEqual(states, {100: 'SUCCEEDED', 101: 'CANCELLED'})
        stock = transformed['tb_seckill_voucher'][0]
        self.assertEqual((stock['reserved_stock'], stock['sold_stock'], stock['admission_state']), (0, 1, 'PAUSED'))

    def test_subscription_order_and_duplicates(self):
        subscriptions = [{'voucher_id': 5, 'user_id': 9, 'subscribed_at': '2026-01-01 00:00:00.123'}]
        result = migration.transform(self.fixture(), subscriptions)
        self.assertEqual(result['tb_seckill_subscription'][0]['subscribed_at'], subscriptions[0]['subscribed_at'])
        with self.assertRaisesRegex(ValueError, 'duplicate subscription'):
            migration.transform(self.fixture(), subscriptions * 2)

    def test_orphan_orders_fail(self):
        rows = self.fixture()
        rows['tb_voucher_order'][0]['voucher_id'] = 999
        with self.assertRaisesRegex(ValueError, 'unknown voucher'):
            migration.transform(rows, [])

    def test_sql_literals_preserve_text_without_backslash_mode_dependency(self):
        self.assertEqual(migration.sql_literal("a'b\\c"), "CONVERT(0x6127625c63 USING utf8mb4)")
        self.assertEqual(migration.sql_literal(None), 'NULL')
        self.assertEqual(migration.sql_literal(9007199254740995), '9007199254740995')

    def test_existing_output_dir_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileExistsError):
                migration.new_output_dir(Path(directory))

    def test_negative_and_oversized_ids_rejected(self):
        for value in (-1, 9223372036854775808):
            with self.assertRaises(ValueError):
                migration.route(value)

    def test_duplicate_orders_fail_even_across_shards(self):
        rows = self.fixture()
        rows['tb_voucher_order'].append(dict(rows['tb_voucher_order'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate order'):
            migration.transform(rows, [])

    def test_fresh_schema_has_constraints_and_all_tables(self):
        sql = '\n'.join(migration.schema_statements(0))
        for suffix in range(2):
            for table in migration.NEW:
                self.assertIn('CREATE TABLE `' + table + '_' + str(suffix) + '`', sql)
            self.assertIn('chk_inventory_' + str(suffix), sql)
        self.assertNotIn('DROP TABLE', sql)
        self.assertNotIn('IF NOT EXISTS', sql)

    def test_prepare_routes_history_and_keeps_router_without_touching_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory) / 'source'
            source_dir.mkdir()
            rows = self.fixture()
            for db in range(2):
                tables = {}
                for table in migration.LEGACY:
                    for suffix in range(2):
                        name = table + '_' + str(suffix)
                        tables[name] = {'ddl': 'CREATE TABLE `' + name + '` (`id` bigint)',
                                        'rows': rows[table] if db == 0 and suffix == 0 else []}
                tables['tb_voucher_order_router_0'] = {'ddl': 'CREATE TABLE `tb_voucher_order_router_0` (`id` bigint)',
                    'rows': [{'id': 100, 'order_id': 100}] if db == 0 else []}
                (source_dir / ('shard_' + str(db) + '.json')).write_text(json.dumps({'schema': 'old_' + str(db), 'tables': tables}))
            migration.manifest(source_dir, {})
            source_before = (source_dir / 'shard_0.json').read_bytes()
            args = argparse.Namespace(source_dir=str(source_dir), output_dir=str(Path(directory) / 'output'),
                target_schemas=['new_0', 'new_1'], subscriptions=None, pending=None)
            migration.prepare(args)
            target = Path(args.output_dir)
            first, second = (target / 'stage_0.sql').read_text(), (target / 'stage_1.sql').read_text()
            self.assertIn('INSERT INTO `tb_voucher_order_router_0`', first)
            self.assertNotIn('INSERT INTO `tb_voucher_order_0`', first)
            self.assertIn('INSERT INTO `tb_voucher_order_0`', second)
            self.assertIn('INSERT INTO `tb_seckill_active_purchase_0`', second)
            self.assertEqual(source_before, (source_dir / 'shard_0.json').read_bytes())


if __name__ == '__main__':
    unittest.main()
