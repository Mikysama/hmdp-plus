#!/usr/bin/env python3
"""Offline, fail-closed V2 migration. Database access is read-only export only.

prepare/init produce SQL for *new* schemas; neither imports nor modifies a DB.
Export requires PyMySQL (install in a separate tooling environment).
"""
import argparse
import copy
import datetime as dt
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
LEGACY = ('tb_voucher', 'tb_seckill_voucher', 'tb_voucher_order', 'tb_voucher_reconcile_log')
NEW = ('tb_seckill_request', 'tb_seckill_active_purchase', 'tb_seckill_operation',
       'tb_seckill_outbox', 'tb_seckill_subscription', 'tb_seckill_recovery', 'tb_seckill_notification')
IDENTIFIER = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')


def identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError('Unsafe SQL identifier: ' + value)
    return value


def route(voucher_id):
    value = int(voucher_id)
    if value < 0 or value > 9223372036854775807:
        raise ValueError('voucher ID outside signed BIGINT range')
    return value % 2, (value // 2) % 2


def new_output_dir(path):
    path.mkdir(parents=True, exist_ok=False)
    return path


def json_default(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat(sep=' ') if isinstance(value, dt.datetime) else value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {'__bytes_hex__': value.hex()}
    raise TypeError(type(value).__name__)


def sql_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        raise ValueError('Floating point source value is unsafe; export exact decimal as text')
    if isinstance(value, dict) and set(value) == {'__bytes_hex__'}:
        if not re.fullmatch(r'[0-9a-fA-F]*', value['__bytes_hex__']):
            raise ValueError('Invalid binary value')
        return "X'" + value['__bytes_hex__'] + "'"
    if not isinstance(value, str):
        raise ValueError('Unsupported SQL value')
    if not value:
        return "''"
    return 'CONVERT(0x' + value.encode('utf-8').hex() + ' USING utf8mb4)'


def transform(rows, subscriptions):
    """Validate all source records before creating any SQL migration output."""
    result = {name: copy.deepcopy(rows.get(name, [])) for name in LEGACY}
    result.update({name: [] for name in NEW})
    vouchers = {}
    for voucher in result['tb_voucher']:
        vid = int(voucher['id'])
        if vid in vouchers:
            raise ValueError('duplicate voucher ID: ' + str(vid))
        route(vid)
        vouchers[vid] = voucher
    stocks = {}
    for stock in result['tb_seckill_voucher']:
        vid = int(stock['voucher_id'])
        if vid not in vouchers:
            raise ValueError('stock references unknown voucher: ' + str(vid))
        if vid in stocks:
            raise ValueError('duplicate stock voucher: ' + str(vid))
        stocks[vid] = stock
    active, order_ids, sold = set(), set(), {}
    for order in result['tb_voucher_order']:
        oid, vid, uid = int(order['id']), int(order['voucher_id']), int(order['user_id'])
        if vid not in stocks:
            raise ValueError('order references unknown voucher: ' + str(vid))
        if oid in order_ids:
            raise ValueError('duplicate order ID: ' + str(oid))
        order_ids.add(oid)
        status = int(order['status'])
        if status not in (1, 2):
            raise ValueError('unsupported legacy order status: ' + str(status))
        if status == 1:
            if (vid, uid) in active:
                raise ValueError('duplicate active purchase: ' + str((vid, uid)))
            active.add((vid, uid))
            sold[vid] = sold.get(vid, 0) + 1
            result['tb_seckill_active_purchase'].append(dict(voucher_id=vid, user_id=uid, order_id=oid))
        created = order.get('create_time', '1970-01-01 00:00:00.000')
        result['tb_seckill_request'].append(dict(id=oid, voucher_id=vid, user_id=uid,
            request_id='migrated-' + str(oid), epoch=1,
            status='SUCCEEDED' if status == 1 else 'CANCELLED', expires_at=created,
            reason_code='MIGRATED', version=0, auto_issue=0, create_time=created,
            update_time=order.get('update_time', created)))
    for vid, stock in stocks.items():
        available, total, count = int(stock['stock']), int(stock['init_stock']), sold.get(vid, 0)
        if min(available, total) < 0 or total != available + count:
            raise ValueError('stock conservation failure for voucher ' + str(vid))
        stock.update(reserved_stock=0, sold_stock=count, version=0, rule_version=0,
                     admission_epoch=1, admission_state='PAUSED', projection_seq=0)
        result['tb_seckill_operation'].append(dict(voucher_id=vid, operation_id='migration-baseline',
            order_id=None, kind='MIGRATION', available_delta=0, reserved_delta=0, sold_delta=0,
            after_available=available, after_reserved=0, after_sold=count,
            payload=json.dumps({'source': 'legacy', 'historyPreserved': True}, separators=(',', ':'))))
    log_ids = set()
    for log in result['tb_voucher_reconcile_log']:
        if int(log['voucher_id']) not in stocks:
            raise ValueError('log references unknown voucher')
        if int(log['id']) in log_ids:
            raise ValueError('duplicate reconciliation log ID')
        log_ids.add(int(log['id']))
    seen_subscriptions = set()
    for subscription in subscriptions:
        vid, uid = int(subscription['voucher_id']), int(subscription['user_id'])
        if vid not in stocks:
            raise ValueError('subscription references unknown voucher')
        if (vid, uid) in seen_subscriptions:
            raise ValueError('duplicate subscription')
        seen_subscriptions.add((vid, uid))
        subscribed_at = subscription.get('subscribed_at')
        if not isinstance(subscribed_at, str):
            raise ValueError('subscription requires original subscribed_at timestamp')
        dt.datetime.fromisoformat(subscribed_at)
        result['tb_seckill_subscription'].append(dict(voucher_id=vid, user_id=uid,
            subscribed_at=subscribed_at, status='ASSIGNED' if (vid, uid) in active else 'WAITING',
            order_id=next((p['order_id'] for p in result['tb_seckill_active_purchase'] if p['voucher_id'] == vid and p['user_id'] == uid), None),
            version=0))
    return result


def schema_statements(db_index, source=None):
    if source is None:
        text = (ROOT / 'sql' / ('hmdp_' + str(db_index) + '.sql')).read_text()
        statements = re.findall(r'CREATE TABLE `[^`]+` \([\s\S]*?\) ENGINE=[^;]+;', text)
    else:
        statements = [entry['ddl'].rstrip(';') + ';' for entry in source['tables'].values()]
    # No DROP, no IF NOT EXISTS: importing into an existing schema must fail.
    output = list(statements)
    for suffix in range(2):
        output.append(f'''ALTER TABLE `tb_seckill_voucher_{suffix}`
  ADD COLUMN `reserved_stock` int NOT NULL DEFAULT 0,
  ADD COLUMN `sold_stock` int NOT NULL DEFAULT 0,
  ADD COLUMN `version` bigint NOT NULL DEFAULT 0,
  ADD COLUMN `rule_version` bigint NOT NULL DEFAULT 0,
  ADD COLUMN `admission_epoch` bigint NOT NULL DEFAULT 1,
  ADD COLUMN `admission_state` varchar(16) NOT NULL DEFAULT 'PAUSED',
  ADD COLUMN `projection_seq` bigint NOT NULL DEFAULT 0,
  ADD UNIQUE KEY `uk_voucher` (`voucher_id`),
  ADD CONSTRAINT `chk_inventory_{suffix}` CHECK
    (`stock` >= 0 AND `reserved_stock` >= 0 AND `sold_stock` >= 0
      AND `init_stock` = `stock` + `reserved_stock` + `sold_stock`);''')
        output.append((ROOT / 'sql' / 'v2' / 'new_tables.sql').read_text().replace('__N__', str(suffix)))
    return output


def insert_sql(table, row):
    columns = ','.join('`' + identifier(column) + '`' for column in row)
    return 'INSERT INTO `' + identifier(table) + '` (' + columns + ') VALUES (' + ','.join(sql_literal(v) for v in row.values()) + ');'


def manifest(directory, extra):
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir()) if p.is_file()}
    data = dict(format=2, generated_at=dt.datetime.now(dt.timezone.utc).isoformat(), files=hashes, **extra)
    (directory / 'manifest.json').write_text(json.dumps(data, ensure_ascii=False, indent=2))


def prepare(args):
    source_dir = Path(args.source_dir).resolve()
    sources = [json.loads((source_dir / ('shard_' + str(i) + '.json')).read_text()) for i in range(2)]
    if len(set(args.target_schemas)) != 2 or set(args.target_schemas) & {s['schema'] for s in sources}:
        raise ValueError('Target schemas must be distinct and different from source schemas')
    source_manifest = json.loads((source_dir / 'manifest.json').read_text())
    for filename, expected in source_manifest['files'].items():
        if Path(filename).name != filename:
            raise ValueError('Invalid manifest path')
        if hashlib.sha256((source_dir / filename).read_bytes()).hexdigest() != expected:
            raise ValueError('Source export checksum mismatch: ' + filename)
    combined = {table: [] for table in LEGACY}
    for source in sources:
        for table in LEGACY:
            for suffix in range(2):
                combined[table].extend(source['tables'][table + '_' + str(suffix)]['rows'])
    subscriptions = json.loads(Path(args.subscriptions).read_text()) if args.subscriptions else []
    transformed = transform(combined, subscriptions)
    exported = json.loads(Path(args.pending).read_text()) if args.pending else []
    existing_orders = {int(o['id']) for o in transformed['tb_voucher_order']}
    audit = []
    for entry in exported:
        oid = int(entry['order_id']) if entry.get('order_id') is not None else None
        audit.append(dict(source=entry, disposition='ALREADY_PERSISTED' if oid in existing_orders else 'TERMINATED_AT_MIGRATION'))
    # Construct and validate all SQL in memory before creating output.
    generated = []
    for db, schema in enumerate(args.target_schemas):
        identifier(schema)
        sql = ['-- Offline V2 staged import; never use mysql --force.', 'SET NAMES utf8mb4;', "SET time_zone = '+00:00';",
               'CREATE DATABASE `' + schema + '` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;', 'USE `' + schema + '`;']
        sql.extend(schema_statements(db, sources[db]))
        for table, entry in sources[db]['tables'].items():
            if not any(table in (base + '_0', base + '_1') for base in LEGACY):
                sql.extend(insert_sql(table, row) for row in entry['rows'])
        for table, rows in transformed.items():
            for row in rows:
                voucher_id = row['id'] if table == 'tb_voucher' else row['voucher_id']
                row_db, suffix = route(voucher_id)
                if row_db == db:
                    sql.append(insert_sql(table + '_' + str(suffix), row))
        generated.append('\n\n'.join(sql) + '\n')
    directory = new_output_dir(Path(args.output_dir))
    for db, sql in enumerate(generated):
        (directory / ('stage_' + str(db) + '.sql')).write_text(sql)
    (directory / 'terminated_pending.json').write_text(json.dumps(audit, ensure_ascii=False, indent=2))
    (directory / 'counts.json').write_text(json.dumps({k: len(v) for k, v in transformed.items()}, indent=2))
    manifest(directory, dict(source_dir=str(source_dir), target_schemas=args.target_schemas,
                            source_manifest_sha256=hashlib.sha256((source_dir / 'manifest.json').read_bytes()).hexdigest(),
                            subscription_export_supplied=bool(args.subscriptions), pending_export_supplied=bool(args.pending)))
    print('Validated migration generated in ' + str(directory) + '; no database writes performed.')


def fresh_init(args):
    if len(set(args.target_schemas)) != 2:
        raise ValueError('Target schemas must be distinct')
    for schema in args.target_schemas:
        identifier(schema)
    directory = new_output_dir(Path(args.output_dir))
    for db, schema in enumerate(args.target_schemas):
        statements = ['SET NAMES utf8mb4;', "SET time_zone = '+00:00';", 'CREATE DATABASE `' + schema + '` CHARACTER SET utf8mb4;', 'USE `' + schema + '`;']
        statements.extend(schema_statements(db))
        (directory / ('init_' + str(db) + '.sql')).write_text('\n\n'.join(statements) + '\n')
    manifest(directory, dict(target_schemas=args.target_schemas, mode='fresh-empty'))
    print('Empty V2 schema SQL generated; no database writes performed.')


def export(args):
    try:
        import pymysql
    except ImportError:
        raise ValueError('Export requires PyMySQL in the tooling environment') from None
    directory = new_output_dir(Path(args.output_dir))
    for db, schema in enumerate(args.source_schemas):
        identifier(schema)
        prefix = 'SECKILL_SOURCE_' + str(db) + '_'
        connection = pymysql.connect(host=os.environ[prefix + 'HOST'], port=int(os.environ.get(prefix + 'PORT', '3306')),
            user=os.environ[prefix + 'USER'], password=os.environ[prefix + 'PASSWORD'], database=schema,
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, autocommit=False)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET time_zone = '+00:00'")
                cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
                cursor.execute('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE=%s', (schema, 'BASE TABLE'))
                names = sorted(r['TABLE_NAME'] for r in cursor.fetchall())
                if any(name.startswith(NEW) for name in names):
                    raise ValueError('Source contains V2 tables; migration is legacy-only')
                tables = {}
                for name in names:
                    identifier(name)
                    cursor.execute('SHOW CREATE TABLE `' + name + '`')
                    ddl = cursor.fetchone()['Create Table']
                    cursor.execute('SELECT * FROM `' + name + '`')
                    tables[name] = dict(ddl=ddl, rows=cursor.fetchall())
                (directory / ('shard_' + str(db) + '.json')).write_text(json.dumps(dict(schema=schema, tables=tables), default=json_default, ensure_ascii=False))
            connection.rollback()
        finally:
            connection.close()
    manifest(directory, dict(source_schemas=args.source_schemas, mode='read-only-export'))
    print('Read-only export complete in ' + str(directory))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    init = commands.add_parser('init', help='Generate empty V2 SQL in a new directory')
    init.add_argument('--target-schemas', nargs=2, required=True)
    init.add_argument('--output-dir', required=True)
    init.set_defaults(action=fresh_init)
    exp = commands.add_parser('export', help='Read-only source export; stop all writers first')
    exp.add_argument('--source-schemas', nargs=2, required=True)
    exp.add_argument('--output-dir', required=True)
    exp.set_defaults(action=export)
    prep = commands.add_parser('prepare', help='Validate offline export and generate staged SQL')
    prep.add_argument('--source-dir', required=True)
    prep.add_argument('--target-schemas', nargs=2, required=True)
    prep.add_argument('--output-dir', required=True)
    prep.add_argument('--subscriptions', help='JSON array with voucher_id,user_id,subscribed_at')
    prep.add_argument('--pending', help='JSON array of Redis/MQ pending entries; order_id optional')
    prep.set_defaults(action=prepare)
    args = parser.parse_args()
    try:
        args.action(args)
    except (ValueError, KeyError, FileExistsError) as error:
        print('Migration refused: ' + str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
