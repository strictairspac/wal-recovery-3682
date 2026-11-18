import copy
import sys
sys.path.insert(0, '/workspace/backend')
from src.services.wal_engine import validate, recover

# 1) 原有种子日志：无 range_read，纯写锁场景，行为保持兼容
seed = [
 {'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},
 {'op':'checkpoint'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},
 {'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},
 {'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'}]
init = {'sample_A':100,'sample_B':50}
assert validate(init, seed) is True
r = recover(init, seed, 10)
assert r['recovered'] == {'sample_A':60,'sample_B':50}, r['recovered']
assert r['invariant_passed'] and r['losers']==['T2'] and r['committed']==['T3']
assert r['committed_all']==['T1','T3']
assert r['survivors']==['T1','T3'] and r['sacrificed']==[] and r['serializable'] is True
assert r['serial_order']==['T1','T3'] and r['replay_verified']
r_full = recover(init, seed, 11)
assert r_full['recovered']=={'sample_A':60,'sample_B':35}
assert r_full['serial_order']==['T1','T2','T3']  # 字典序拓扑（无冲突边，三者写不同键）
print('case1 legacy ok:', r['recovered'], r_full['serial_order'])

# 2) 两笔事务更新不同点 + 区间读互相矛盾 -> 冲突环，必须舍弃一笔
# 初始 A=100, B=50
# T1 写 A=60；T1 区间读 [40,80]：A=60 命中、B=50 命中 -> 读到 {A:60,B:50}
# T2 写 B=90；T2 区间读 [40,80]：B 已不在区间，A=100 不在区间 -> 读到 {}
cyc = [
 {'op':'begin','tx':'T1'},
 {'op':'begin','tx':'T2'},
 {'op':'write','tx':'T1','key':'sample_A','value':60},
 {'op':'write','tx':'T2','key':'sample_B','value':90},
 {'op':'range_read','tx':'T1','low':40,'high':80},   # 命中 A:60, B:50
 {'op':'range_read','tx':'T2','low':40,'high':80},   # 空
 {'op':'commit','tx':'T1'},
 {'op':'commit','tx':'T2'},
]
assert validate(init, cyc) is True
r2 = recover(init, cyc, len(cyc))
print('case2 plan:', r2['serializable'], r2['conflict_cycles'], 'sac=', r2['sacrificed'], 'surv=', r2['survivors'], 'order=', r2['serial_order'])
assert r2['serializable'] is False
assert len(r2['conflict_cycles'])>=1
# 最大保留=1，字典序最小幸存集 -> 保留 T1，舍弃 T2
assert r2['survivors']==['T1'] and r2['sacrificed']==['T2']
assert r2['recovered']=={'sample_A':60,'sample_B':50}, r2['recovered']
assert r2['invariant_passed']
# 读观察内容
obs = {o['tx']:o['hits'] for o in r2['read_observations']}
assert obs['T1']=={'sample_A':60,'sample_B':50} and obs['T2']=={}, obs
# 舍弃轨迹存在
assert any(t['phase']=='undo_sacrificed' and t['tx']=='T2' for t in r2['trace'])

# 3) 读己之所写 + 不见他人未提交值
ryw = [
 {'op':'begin','tx':'T1'},{'op':'begin','tx':'T2'},
 {'op':'write','tx':'T1','key':'sample_A','value':70},
 {'op':'write','tx':'T2','key':'sample_B','value':45},
 {'op':'range_read','tx':'T1','low':40,'high':80},  # A:70(自己), B:50(提交态，不见T2的45)
 {'op':'commit','tx':'T1'},{'op':'commit','tx':'T2'}]
r3 = recover(init, ryw, len(ryw))
o3 = {o['tx']:o['hits'] for o in r3['read_observations']}
assert o3['T1']=={'sample_A':70,'sample_B':50}, o3
print('case3 read-your-writes ok:', o3)

# 4) 可串行化的区间读：T2 的读不与 T1 冲突
ser = [
 {'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':60},
 {'op':'commit','tx':'T1'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':90},
 {'op':'range_read','tx':'T2','low':40,'high':95},  # A:60,B:90 都命中，T1->T2 可解释
 {'op':'commit','tx':'T2'}]
r4 = recover(init, ser, len(ser))
assert r4['serializable'] and r4['survivors']==['T1','T2'] and r4['serial_order']==['T1','T2']
assert r4['replay_verified'] and r4['recovered']=={'sample_A':60,'sample_B':90}
print('case4 serializable ok:', r4['serial_order'])

# 5) 崩溃在提交前：loser 撤销，不参与串行分析
crash = cyc[:6]  # 两笔都没 commit
r5 = recover(init, crash, len(crash))
assert r5['losers']==['T1','T2'] and r5['committed']==[]
assert r5['survivors']==[] and r5['recovered']==init and r5['invariant_passed']
print('case5 losers ok:', r5['recovered'])

# 6) 录入核对：顺序与余额
r6 = recover(init, cyc, len(cyc), expected_order='T1', expected_balances={'sample_A':60,'sample_B':50})
assert r6['check']['order_match'] is True
assert r6['check']['balances_match'] is True and r6['check']['balances_complete'] is True
r6b = recover(init, cyc, len(cyc), expected_order='T2, T1', expected_balances={'sample_A':99})
assert r6b['check']['order_match'] is False
assert r6b['check']['balances_match'] is False and r6b['check']['balances_complete'] is False
print('case6 check ok:', r6['check'])

# 7) 非法输入拒绝
def must_fail(initial_, records_, msg):
    try:
        validate(initial_, records_)
    except ValueError as e:
        assert msg in str(e), (msg, str(e))
        return
    raise AssertionError('应拒绝: '+msg)
bad_read = [{'op':'begin','tx':'T1'},{'op':'range_read','tx':'T1','low':80,'high':40}]
must_fail(init, bad_read, 'low<=high')
bad_tx = [{'op':'range_read','tx':'T9','low':0,'high':10}]
must_fail(init, bad_tx, '尚未begin')
must_fail(init, [{'op':'range_read','tx':'T1'}], '尚未begin')
# 同键写锁仍拒绝
lock_conflict = [{'op':'begin','tx':'T1'},{'op':'begin','tx':'T2'},
 {'op':'write','tx':'T1','key':'sample_A','value':1},
 {'op':'write','tx':'T2','key':'sample_A','value':2}]
must_fail(init, lock_conflict, '严格写锁')
# 崩溃点越界
try:
    recover(init, cyc, 99)
    raise AssertionError('应拒绝崩溃点')
except ValueError as e:
    assert '崩溃点' in str(e)
print('case7 validation ok')

# 8) 检查点语义：检查点后崩溃，durable 为检查点账本，恢复结果仍正确
cp = [
 {'op':'begin','tx':'T0'},{'op':'write','tx':'T0','key':'sample_A','value':80},{'op':'commit','tx':'T0'},
 {'op':'checkpoint'},
 {'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':60},{'op':'commit','tx':'T1'}]
r8 = recover(init, cp, 6)
assert r8['checkpoint_lsn']==4 and r8['durable_checkpoint']=={'sample_A':80,'sample_B':50}
assert r8['losers']==['T1'] and r8['recovered']=={'sample_A':80,'sample_B':50} and r8['invariant_passed']
r8b = recover(init, cp, 7)
assert r8b['recovered']=={'sample_A':60,'sample_B':50} and r8b['survivors']==['T0','T1']
print('case8 checkpoint ok:', r8['durable_checkpoint'], r8['recovered'], r8b['recovered'])

# 9) abort 撤销且不参与分析
ab = [
 {'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':60},{'op':'abort','tx':'T1'},
 {'op':'begin','tx':'T2'},{'op':'range_read','tx':'T2','low':90,'high':110},{'op':'commit','tx':'T2'}]
r9 = recover(init, ab, len(ab))
assert r9['aborted']==['T1'] and r9['survivors']==['T2'] and r9['recovered']==init
o9 = r9['read_observations'][0]
assert o9['hits']=={'sample_A':100}, o9
print('case9 abort ok:', r9['recovered'])

print('\nALL ENGINE TESTS PASSED')
