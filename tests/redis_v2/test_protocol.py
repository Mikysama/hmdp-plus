"""Isolated Redis Lua protocol tests. Starts only a temporary UNIX-socket server."""
import json, pathlib, socket, subprocess, tempfile, time, unittest
ROOT = pathlib.Path(__file__).resolve().parents[2]
LUA = ROOT / 'hmdp-core-service/src/main/resources/lua/v2'

class Redis:
    def __init__(self, path):
        self.s = socket.socket(socket.AF_UNIX); self.s.connect(path); self.f = self.s.makefile('rb')
    def command(self, *parts):
        data = [str(p).encode() for p in parts]
        self.s.sendall(b'*%d\r\n' % len(data) + b''.join(b'$%d\r\n' % len(p)+p+b'\r\n' for p in data))
        return self.read()
    def read(self):
        line = self.f.readline(); kind, body = line[:1], line[1:-2]
        if kind == b'+': return body.decode()
        if kind == b'-': raise RuntimeError(body.decode())
        if kind == b':': return int(body)
        if kind == b'$':
            n=int(body)
            if n<0: return None
            data=self.f.read(n); self.f.read(2); return data.decode()
        if kind == b'*': return [self.read() for _ in range(int(body))]
        raise RuntimeError(repr(line))
    def close(self): self.f.close(); self.s.close()

class ProtocolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory(); cls.sock=cls.tmp.name+'/redis.sock'
        cls.process=subprocess.Popen(['redis-server','--port','0','--unixsocket',cls.sock,'--save','','--appendonly','no'],stdout=subprocess.DEVNULL)
        for _ in range(1000):
            if pathlib.Path(cls.sock).exists(): break
            time.sleep(.01)
        subprocess.run(['redis-cli','-s',cls.sock,'PING'],check=True,stdout=subprocess.DEVNULL)
        cls.r=Redis(cls.sock)
    @classmethod
    def tearDownClass(cls):
        cls.r.close(); cls.process.terminate(); cls.process.wait(); cls.tmp.cleanup()
    def setUp(self):
        self.r.command('FLUSHDB'); self.epoch='1'; self.prefix='test:{9}'; self.prepare()
    def keys(self, user='7', epoch=None):
        b=self.prefix+':epoch:'+str(epoch or self.epoch)
        return [self.prefix+':active',b+':meta',b+':stock',b+':users',b+':requests',b+':reservations',b+':pending',b+':inflight',b+':bucket',self.prefix+':token:'+str(user)]
    def script(self, name, keys, *args):
        p=LUA/(name+'.lua')
        self.assertTrue(p.exists(), 'Protocol Lua not implemented: '+name)
        if name in ('stage','lease','reserve','audit','audit_page'):
            keys=list(keys)+[self.prefix+':fence']
        if name == 'reserve': keys=list(keys)+[self.prefix+':admission-outbox']
        return self.r.command('EVAL',p.read_text(),len(keys),*keys,*args)
    def prepare(self, stock=3, epoch='1'):
        k=self.keys(epoch=epoch)
        now=int(time.time()*1000)
        self.r.command('SET',self.prefix+':fence',epoch); self.r.command('SET',k[0],epoch); self.r.command('SET',k[2],stock)
        self.r.command('HSET',k[1],'epoch',epoch,'state','READY','status','1','ruleVersion','1','begin','0','end',str(now+3600000),'leaseUntil',str(now+3600000),'sequence','0')
        for key in (k[3],k[4],k[5]): self.r.command('HSET',key,'__sentinel','1')
        for key in (k[6],k[7]): self.r.command('ZADD',key,0,'__sentinel')
    def token(self,user='7'):
        return self.script('issue_token',[self.keys(user)[9]],'token-'+str(user),30000)
    def reserve(self,user='7',request='r1',order='101',epoch=None):
        self.token(user)
        return self.script('reserve',self.keys(user,epoch),str(epoch or self.epoch),9,user,request,'token-'+str(user),order,1000000,1000000,30000)
    def project(self,seq,kind,order='101',user='7',epoch=None,delta=0):
        return self.script('project',self.keys(user,epoch)[:8],str(epoch or self.epoch),seq,'event-'+str(seq),kind,user,order,delta)
    def attempt(self, mode, identifier='a1', capacity=2, lease=30000):
        return self.script('attempt_slot',[self.prefix+':attempts'],mode,identifier,capacity,lease)
    def test_reservation_persists_delivery_intent_and_retry_does_not_append(self):
        held=json.loads(self.reserve())
        self.reserve(order='102')
        rows=self.r.command('XRANGE',self.prefix+':admission-outbox','-','+')
        self.assertEqual(1,len(rows))
        fields=dict(zip(rows[0][1][::2],rows[0][1][1::2]))
        self.assertEqual(held,json.loads(fields['reservation']))
        self.assertEqual('false',fields['autoIssue'])
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
    def test_wrong_stream_type_rejects_before_stock_or_user_mutation(self):
        self.r.command('SET',self.prefix+':admission-outbox','corrupt')
        with self.assertRaisesRegex(RuntimeError,'CORRUPT_OR_MISSING_12'):
            self.reserve()
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
        self.assertIsNone(self.r.command('HGET',self.keys()[3],'7'))
    def test_auto_issue_is_persisted_in_original_intent(self):
        self.token()
        self.script('reserve',self.keys(),'1',9,'7','auto_7_1','token-7','101',100,32,30000,'true')
        rows=self.r.command('XRANGE',self.prefix+':admission-outbox','-','+')
        fields=dict(zip(rows[0][1][::2],rows[0][1][1::2]))
        self.assertEqual('true',fields['autoIssue'])
    def test_retry_cannot_change_original_delivery_mode(self):
        self.token()
        args=['1',9,'7','auto_7_1','token-7','101',100,32,30000]
        original=self.script('reserve',self.keys(),*args,'true')
        self.assertEqual(original,self.script('reserve',self.keys(),*args,'true'))
        with self.assertRaisesRegex(RuntimeError,'AUTO_ISSUE_MISMATCH'):
            self.script('reserve',self.keys(),*args,'false')
        self.assertEqual(1,self.r.command('XLEN',self.prefix+':admission-outbox'))
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))

    def test_rebuild_keeps_unacknowledged_stream_entries(self):
        self.reserve()
        self.script('stage',self.keys(epoch='2')[:9],'2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]')
        self.assertEqual(1,self.r.command('XLEN',self.prefix+':admission-outbox'))

    def test_order_id_collision_does_not_overwrite_another_hold(self):
        original=json.loads(self.reserve(order='9223372036854775806'))
        with self.assertRaisesRegex(RuntimeError,'ORDER_ID_CONFLICT'):
            self.reserve(user='8',request='other',order='9223372036854775806')
        self.assertEqual(original,json.loads(self.r.command('HGET',self.keys()[5],'9223372036854775806')))
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
        self.assertIsNone(self.r.command('HGET',self.keys()[3],'8'))
    def test_projection_cannot_release_another_users_order(self):
        self.reserve()
        with self.assertRaisesRegex(RuntimeError,'RESERVATION_MISMATCH'):
            self.project(1,'RELEASE',user='8')
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
        self.assertEqual('101',self.r.command('HGET',self.keys()[3],'7'))
    def test_snapshot_duplicate_order_fails_before_writing_batch(self):
        k=self.keys(epoch='2')[:9]
        args=['2','BEGIN','worker',1,1,'1',0,9999999999999,0,'[]']
        self.script('stage',k,*args)
        records=[dict(requestId='r'+str(u),orderId='101',voucherId='9',userId=str(u),epoch='2',ruleVersion='1',createdAt=1,state='COMMITTED') for u in (7,8)]
        with self.assertRaisesRegex(RuntimeError,'DUPLICATE_ORDER'):
            self.script('stage',k,'2','APPEND',*args[2:-1],json.dumps(records))
        self.assertIsNone(self.r.command('HGET',k[5],'101'))
        self.assertIsNone(self.r.command('HGET',k[3],'7'))
    def test_attempt_slots_limit_distinct_calls_and_duplicate_is_idempotent(self):
        self.assertEqual('ENTERED',self.attempt('ENTER','a1',1))
        self.assertEqual('ENTERED',self.attempt('ENTER','a1',1))
        self.assertEqual('BUSY',self.attempt('ENTER','a2',1))
        self.assertEqual('LEFT',self.attempt('LEAVE','a1',1))
        self.assertEqual('ENTERED',self.attempt('ENTER','a2',1))
    def test_attempt_expiry_allows_reentry_without_inventory_changes(self):
        self.attempt('ENTER','a1',1,1);time.sleep(.01)
        self.assertEqual('ENTERED',self.attempt('ENTER','a2',1))
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_attempt_wrong_type_fails_without_overwrite(self):
        self.r.command('SET',self.prefix+':attempts','bad')
        with self.assertRaisesRegex(RuntimeError,'CORRUPT'):self.attempt('ENTER')
        self.assertEqual('bad',self.r.command('GET',self.prefix+':attempts'))
    def test_old_attempt_leave_does_not_remove_other_attempt(self):
        self.attempt('ENTER','a1',1,1);time.sleep(.01);self.attempt('ENTER','a2',1)
        self.attempt('LEAVE','a1',1)
        self.assertEqual('BUSY',self.attempt('ENTER','a3',1))
    def test_rebuild_repairs_wrongtype_pointer_fence_and_epoch_maps(self):
        self.r.command('DEL',self.keys()[0],self.prefix+':fence')
        self.r.command('HSET',self.keys()[0],'broken','1'); self.r.command('HSET',self.prefix+':fence','broken','1')
        k=self.keys(epoch='2')[:9]
        self.r.command('HSET',k[2],'broken','1');self.r.command('SET',k[3],'broken')
        args=['2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]']
        self.script('stage',k,*args);self.script('stage',k,'2','FINISH',*args[2:])
        self.assertEqual('ACTIVATED',self.script('lease',k[:8],'2','ACTIVATE',5000))
        self.assertEqual('3',self.r.command('GET',k[2]))
    def test_wrongtype_pointer_does_not_erase_newer_valid_fence(self):
        self.r.command('DEL',self.keys()[0]);self.r.command('HSET',self.keys()[0],'broken','1')
        self.r.command('SET',self.prefix+':fence',4)
        with self.assertRaisesRegex(RuntimeError,'STAGING_FENCED'):
            self.script('stage',self.keys(epoch='2')[:9],'2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]')
        self.assertEqual('4',self.r.command('GET',self.prefix+':fence'))
    def test_paged_audit_validates_large_voucher(self):
        k=self.keys();self.r.command('SET',k[2],0)
        for i in range(2200):
            r=dict(orderId=str(10000+i),userId=str(i),requestId='r'+str(i),epoch='1',state='COMMITTED')
            self.r.command('HSET',k[5],r['orderId'],json.dumps(r))
            self.r.command('HSET',k[3],r['userId'],r['orderId'])
            self.r.command('HSET',k[4],str(i)+':r'+str(i),r['orderId'])
        live=set();version='';stock=None
        for phase in ('reservations','users','requests'):
            cursor='0'
            while True:
                result=json.loads(self.script('audit_page',k[:8],'1',0,phase,cursor,version))
                self.assertEqual('PAGE',result['status']);version=str(result['version']);stock=result['stock']
                live.update(result['live']);cursor=result['cursor']
                if cursor=='0':break
        self.assertEqual(2200,stock+len(live))
    def test_paged_audit_detects_concurrent_reservation(self):
        first=json.loads(self.script('audit_page',self.keys()[:8],'1',0,'reservations','0',''))
        self.reserve()
        result=json.loads(self.script('audit_page',self.keys()[:8],'1',0,'users','0',str(first['version'])))
        self.assertEqual('BUSY',result['status'])
    def test_new_staging_fences_old_activation_and_staging(self):
        args=['2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]']
        k=self.keys(epoch='2')[:9]
        self.script('stage',k,*args); self.script('stage',k,'2','FINISH',*args[2:])
        self.script('stage',self.keys(epoch='3')[:9],'3',*args[1:])
        self.assertEqual('STALE',self.script('lease',self.keys(epoch='2')[:8],'2','ACTIVATE',5000))
        with self.assertRaisesRegex(RuntimeError,'STAGING_FENCED'):
            self.script('stage',k,*args)
        self.assertEqual('1',self.r.command('GET',k[0]))
    def test_fence_stops_old_lease_and_reserve(self):
        self.r.command('SET',self.prefix+':fence','2')
        self.assertEqual('STALE',self.script('lease',self.keys()[:8],'1','RENEW',5000))
        with self.assertRaisesRegex(RuntimeError,'STALE'): self.reserve()
    def test_audit_detects_missing_binding_and_ignores_seq_race(self):
        self.reserve()
        self.assertEqual('MATCH',self.script('audit',self.keys()[:8],'1',0,3,2000))
        self.r.command('HDEL',self.keys()[3],'7')
        self.assertEqual('BUSY',self.script('audit',self.keys()[:8],'1',1,3,2000))
        with self.assertRaisesRegex(RuntimeError,'CORRUPT'):
            self.script('audit',self.keys()[:8],'1',0,3,2000)
    def test_audit_counts_live_and_ignores_released(self):
        self.reserve(); self.project(1,'COMMIT')
        self.assertEqual('MATCH',self.script('audit',self.keys()[:8],'1',1,3,2000))
        self.project(2,'CANCEL')
        self.assertEqual('MATCH',self.script('audit',self.keys()[:8],'1',2,3,2000))
    def stats(self,event,kind,order='o1'):
        return self.script('buyer_stats',['stats:{shop}:scores','stats:{shop}:events','stats:{shop}:orders'],event,kind,'7',order)
    def test_buyer_stats_cancel_first_then_success_stays_zero(self):
        self.stats('cancel','CANCEL'); self.stats('success','SUCCESS')
        self.assertIsNone(self.r.command('ZSCORE','stats:{shop}:scores','7'))
    def test_buyer_stats_replay_and_old_order_do_not_decrement_new(self):
        self.stats('s1','SUCCESS'); self.stats('s1','SUCCESS'); self.stats('c1','CANCEL')
        self.stats('s2','SUCCESS','o2'); self.stats('late-c1','CANCEL'); self.stats('late-s1','SUCCESS')
        self.assertEqual('1',self.r.command('ZSCORE','stats:{shop}:scores','7'))
    def test_stats_conflicting_event_does_not_mutate_count(self):
        self.stats('same','SUCCESS')
        with self.assertRaisesRegex(RuntimeError,'EVENT_CONFLICT'): self.stats('same','CANCEL')
        self.assertEqual('1',self.r.command('ZSCORE','stats:{shop}:scores','7'))
    def test_stats_wrongtype_does_not_write_event(self):
        self.r.command('SET','stats:{shop}:orders','broken')
        with self.assertRaisesRegex(RuntimeError,'CORRUPT_STATS'): self.stats('s1','SUCCESS')
        self.assertIsNone(self.r.command('HGET','stats:{shop}:events','s1'))
    def test_audit_is_bounded_and_does_not_claim_match(self):
        self.reserve(); self.reserve('8','r2','102')
        self.assertEqual('BUSY',self.script('audit',self.keys()[:8],'1',0,3,1))
    def test_missing_reservation_blocks_projection_without_stock_write(self):
        self.reserve()
        self.r.command('HDEL',self.keys()[5],'101')
        with self.assertRaisesRegex(RuntimeError,'MISSING_RESERVATION'): self.project(1,'RELEASE')
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
        self.assertEqual('0',self.r.command('HGET',self.keys()[1],'sequence'))
    def test_expired_lease_rejects_before_write(self):
        self.r.command('HSET',self.keys()[1],'leaseUntil','1')
        with self.assertRaisesRegex(RuntimeError,'ADMISSION_CLOSED'): self.reserve()
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_adjustment_replay_adds_stock_only_once(self):
        self.project(1,'ADJUST',delta=5); self.project(1,'ADJUST',delta=5)
        self.assertEqual('8',self.r.command('GET',self.keys()[2]))
    def test_credential_issuance_reuses_unbound_then_rotates_bound(self):
        first=self.token()
        self.assertEqual(first,self.script('issue_token',[self.keys()[9]],'next',30000))
        self.reserve()
        self.assertEqual('next',self.script('issue_token',[self.keys()[9]],'next',30000))
    def test_rate_rejects_without_deducting(self):
        self.r.command('HSET',self.keys()[8],'tokens',0,'time',int(time.time()*1000)+10000)
        with self.assertRaisesRegex(RuntimeError,'RATE_LIMITED'): self.reserve()
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_lease_refuses_to_hide_missing_bindings(self):
        self.r.command('DEL',self.keys()[5])
        with self.assertRaisesRegex(RuntimeError,'CORRUPT'):
            self.script('lease',self.keys()[:8],'1','RENEW',5000)
    def test_committed_requires_cancel_to_release(self):
        self.reserve(); self.project(1,'COMMIT')
        with self.assertRaisesRegex(RuntimeError,'CANCEL_REQUIRED'): self.project(2,'RELEASE')
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
        self.project(2,'CANCEL'); self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_staging_cannot_overwrite_active_epoch(self):
        k=self.keys(epoch='2')[:9]
        args=['2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]']
        self.assertEqual('STAGED',self.script('stage',k,*args))
        self.assertEqual('STAGED',self.script('stage',k,'2','FINISH',*args[2:]))
        self.script('lease',self.keys(epoch='2')[:8],'2','ACTIVATE',5000)
        with self.assertRaisesRegex(RuntimeError,'ACTIVE_EPOCH'):
            self.script('stage',k,*args)
        self.assertEqual('3',self.r.command('GET',k[2]))
    def test_staging_preserves_large_identifiers(self):
        k=self.keys(epoch='2')[:9]; n='9223372036854775806'
        args=['2','BEGIN','worker',3,1,'1',0,9999999999999,0,'[]']
        self.script('stage',k,*args)
        data=[dict(requestId='r',orderId=n,voucherId='9',userId=n,epoch='2',ruleVersion='1',createdAt=1,state='COMMITTED')]
        self.script('stage',k,'2','APPEND',*args[2:-1],json.dumps(data))
        r=json.loads(self.r.command('HGET',k[5],n))
        self.assertEqual(n,r['userId'])
    def test_lease_activation_and_epoch_fencing(self):
        self.prepare(epoch='2')
        self.r.command('SET',self.keys()[0],'1')
        k=self.keys(epoch='2')
        self.assertEqual('ACTIVATED',self.script('lease',self.keys(epoch='2')[:8],'2','ACTIVATE',5000))
        self.assertEqual('RENEWED',self.script('lease',self.keys(epoch='2')[:8],'2','RENEW',5000))
        self.assertEqual('STALE',self.script('lease',self.keys()[:8],'1','ACTIVATE',5000))
    def test_inflight_bound_rejects_before_deduction(self):
        self.reserve()
        self.token('8')
        with self.assertRaisesRegex(RuntimeError,'ADMISSION_BUSY'):
            self.script('reserve',self.keys('8'),'1',9,'8','r2','token-8','102',100,1,30000)
        self.assertEqual('2',self.r.command('GET',self.keys()[2]))
    def test_snowflake_identifiers_stay_exact(self):
        n='9223372036854775806'
        record=json.loads(self.reserve(user=n,order=n))
        self.assertEqual(n,record['userId']); self.assertEqual(n,record['orderId'])
    def test_duplicate_request_does_not_deduct_twice(self):
        a=json.loads(self.reserve()); b=json.loads(self.reserve(order='102'))
        self.assertEqual(a['orderId'],b['orderId']); self.assertEqual('2',self.r.command('GET',self.keys()[2]))
    def test_independent_release_and_repeated_release(self):
        self.reserve(); self.reserve('8','r2','102')
        self.project(1,'RELEASE'); self.project(2,'RELEASE','102','8'); self.project(2,'RELEASE','102','8')
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_old_release_cannot_delete_new_purchase(self):
        self.reserve(); self.project(1,'RELEASE'); self.reserve(request='r2',order='102')
        self.project(2,'RELEASE')
        self.assertEqual('102',self.r.command('HGET',self.keys()[3],'7')); self.assertEqual('2',self.r.command('GET',self.keys()[2]))
    def test_wrong_type_is_rejected_before_inventory_write(self):
        self.r.command('DEL',self.keys()[5]); self.r.command('SET',self.keys()[5],'wrong')
        with self.assertRaisesRegex(RuntimeError,'CORRUPT'): self.reserve()
        self.assertEqual('3',self.r.command('GET',self.keys()[2])); self.assertIsNone(self.r.command('HGET',self.keys()[3],'7'))
    def test_old_epoch_never_changes_new_epoch(self):
        self.reserve(); self.prepare(stock=2,epoch='2')
        self.assertEqual('STALE',self.project(1,'RELEASE'))
        with self.assertRaisesRegex(RuntimeError,'STALE'): self.reserve('8','r2','102')
        self.assertEqual('2',self.r.command('GET',self.keys(epoch='2')[2]))
    def test_gap_blocks_and_terminal_state_does_not_regress(self):
        self.reserve()
        with self.assertRaisesRegex(RuntimeError,'SEQUENCE_GAP'): self.project(2,'COMMIT')
        self.project(1,'RELEASE'); self.project(2,'ACCEPT'); self.project(3,'COMMIT')
        self.assertEqual('RELEASED',json.loads(self.r.command('HGET',self.keys()[5],'101'))['state'])
        self.assertEqual('3',self.r.command('GET',self.keys()[2]))
    def test_other_request_cannot_reuse_bound_token(self):
        self.reserve(); self.project(1,'RELEASE')
        with self.assertRaisesRegex(RuntimeError,'TOKEN_BOUND'):
            self.script('reserve',self.keys(),'1',9,'7','r2','token-7','102',100,32,30000)

if __name__=='__main__': unittest.main()
