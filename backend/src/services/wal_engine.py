"""Quiescent-checkpoint WAL teaching model.

Adds range reads (read-your-writes, never reading another active transaction's
uncommitted values) and commit-time serializability analysis: a precedence graph
is built from range-read observations, a serial order reproducing every observed
hit set is searched, and when no order exists the smallest deterministic set of
committed transactions is sacrificed so the conflict-free commits survive.
Recovery then undoes both unfinished losers and sacrificed commits.
"""
import copy
import hashlib
import json
import re

BOUND = 1_000_000_000
READ_OPS = ('begin', 'write', 'commit', 'abort', 'checkpoint', 'range_read')
MAX_GRAPH_ENUM = 12  # beyond this many cyclic nodes the exact subset search falls back to greedy peeling


def _is_int(value):
    return not isinstance(value, bool) and isinstance(value, int) and abs(value) <= BOUND


def _hits(visible, low, high):
    return {key: value for key, value in visible.items() if low <= value <= high}


def validate(initial, records):
    if not initial or len(initial) > 30 or not records or len(records) > 200:
        raise ValueError('需1—30个键及1—200条日志')
    for key, value in initial.items():
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,50}', key) or not _is_int(value):
            raise ValueError('初始键为字母数字下划线短横线，值为绝对值不超过10亿的整数')
    state = copy.deepcopy(initial)
    committed = copy.deepcopy(initial)
    own = {}
    active = set()
    used = set()
    locks = {}
    for lsn, rec in enumerate(records, 1):
        op = rec.get('op')
        tx = rec.get('tx')
        if op == 'checkpoint':
            if active:
                raise ValueError(f'LSN {lsn}: 仅支持无活动事务的静止检查点')
            continue
        if not tx:
            raise ValueError(f'LSN {lsn}: 需要事务标识')
        if op == 'begin':
            if tx in used:
                raise ValueError(f'LSN {lsn}: 事务标识不得重复')
            active.add(tx)
            used.add(tx)
            own[tx] = {}
        elif op == 'write':
            if tx not in active:
                raise ValueError(f'LSN {lsn}: 写入事务尚未begin')
            key = rec.get('key')
            value = rec.get('value')
            if key not in initial:
                raise ValueError(f'LSN {lsn}: 写入键必须存在于初始账本')
            if not _is_int(value):
                raise ValueError(f'LSN {lsn}: 数值超限')
            if key in locks and locks[key] != tx:
                raise ValueError(f'LSN {lsn}: 违反严格写锁，活动事务持有同键')
            locks[key] = tx
            own[tx][key] = value
            state[key] = value
        elif op == 'range_read':
            if tx not in active:
                raise ValueError(f'LSN {lsn}: 区间读事务尚未begin')
            low, high = rec.get('low'), rec.get('high')
            if not _is_int(low) or not _is_int(high) or low > high:
                raise ValueError(f'LSN {lsn}: 区间读需要 low<=high 且均为绝对值不超过10亿的整数')
            visible = dict(committed)
            visible.update(own[tx])
            _hits(visible, low, high)
        elif op in ('commit', 'abort'):
            if tx not in active:
                raise ValueError(f'LSN {lsn}: 事务未开启或已结束')
            if op == 'abort':
                for key in reversed(list(own[tx].keys())):
                    state[key] = committed[key]
            else:
                committed.update(own[tx])
            del own[tx]
            active.discard(tx)
            locks = {k: v for k, v in locks.items() if v != tx}
        else:
            raise ValueError(f'LSN {lsn}: 未知操作，支持 {"/".join(READ_OPS)}')
    return True


def _topo_order(nodes, pred):
    """Kahn topological sort with lexicographic tie-breaking; None when cyclic."""
    remaining = {t: set(pred.get(t, ())) for t in nodes}
    done = set()
    order = []
    while len(order) < len(nodes):
        ready = sorted(t for t in nodes if t not in done and not (remaining[t] - done))
        if not ready:
            return None
        pick = ready[0]
        order.append(pick)
        done.add(pick)
    return order


def _tarjan_sccs(nodes, succ):
    index = {}
    stack = []
    low = {}
    on_stack = set()
    counter = [0]
    components = []

    def strongconnect(v):
        index[v] = counter[0]
        low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on_stack.add(v)
        for w in sorted(succ[v]):
            if w not in index:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            component = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                component.append(w)
                if w == v:
                    break
            components.append(sorted(component))

    for t in sorted(nodes):
        if t not in index:
            strongconnect(t)
    return components


def _successor_map(nodes, pred):
    # pred[a] contains b means edge b -> a; invert it into a successor map.
    return {t: {n for n in nodes if t in pred.get(n, ())} for t in nodes}


def _find_cycles(nodes, pred):
    """Representative cycles, one per cyclic SCC (deterministic)."""
    succ = _successor_map(nodes, pred)
    cycles = []
    for component in _tarjan_sccs(nodes, succ):
        if len(component) > 1 or component[0] in succ[component[0]]:
            cycles.append(component)
    return cycles


def _cyclic_nodes(nodes, pred):
    """Nodes that participate in any cycle: members of cyclic SCCs."""
    succ = _successor_map(nodes, pred)
    bad = set()
    for component in _tarjan_sccs(nodes, succ):
        if len(component) > 1 or component[0] in succ[component[0]]:
            bad.update(component)
    return bad


def _replay_matches(order, ops_by_tx, reads_by_tx, initial):
    """Serially replay whole transactions in `order`; every range read must hit
    exactly the same keys with exactly the same values as the observed history."""
    state = copy.deepcopy(initial)
    for tx in order:
        local = {}
        read_cursor = 0
        for kind, *payload in ops_by_tx.get(tx, ()):
            if kind == 'w':
                key, value = payload
                local[key] = value
            else:
                low, high = payload
                visible = dict(state)
                visible.update(local)
                expected = reads_by_tx[tx][read_cursor]
                read_cursor += 1
                if _hits(visible, low, high) != expected:
                    return False
        state.update(local)
    return True


def _serialization_plan(winners, reads, writes_by_tx, before_by_tx, ops_by_tx, initial):
    nodes = set(winners)
    pred = {t: set() for t in nodes}
    # Build the precedence graph from every committed range-read observation.
    for rd in reads:
        ti = rd['tx']
        if ti not in nodes:
            continue
        hits, low, high = rd['hits'], rd['low'], rd['high']
        for tj in nodes:
            if tj == ti:
                continue
            for key, value_j in writes_by_tx.get(tj, {}).items():
                if key in writes_by_tx.get(ti, {}):
                    continue  # Ti owns the latest value of this key: read-your-writes, no inter-tx conflict
                base = before_by_tx[tj].get(key, initial.get(key))
                actual = hits.get(key)
                # Serial order Tj -> Ti means Ti would observe value_j for key.
                j_first_ok = (actual == value_j) if low <= value_j <= high else (key not in hits)
                # Serial order Ti -> Tj means Ti would still observe the pre-Tj base value.
                i_first_ok = (actual == base) if low <= base <= high else (key not in hits)
                if not j_first_ok:
                    pred[tj].add(ti)  # Tj must not run first, hence Ti -> Tj
                if not i_first_ok:
                    pred[ti].add(tj)  # Ti must not run first, hence Tj -> Ti
    cycles = _find_cycles(nodes, pred)
    if not cycles:
        order = _topo_order(nodes, pred)
        verified = _replay_matches(order, ops_by_tx, _reads_by_tx(reads, nodes), initial)
        return {'survivors': order, 'sacrificed': [], 'serial_order': order,
                'conflict_cycles': [], 'serializable': True, 'replay_verified': verified,
                'pred': pred}
    cyclic = _cyclic_nodes(nodes, pred)
    reads_by_tx = _reads_by_tx(reads, nodes)
    outside = sorted(nodes - cyclic)  # acyclic transactions never need sacrificing
    cyclic_list = sorted(cyclic)
    chosen = None
    if len(cyclic_list) <= MAX_GRAPH_ENUM:
        # Exact search: keep the largest cyclic subset, lexicographically smallest on ties.
        # Outside (conflict-free) transactions are always part of the survivor set.
        for size in range(len(cyclic_list), -1, -1):
            candidates = []
            for keep in _combinations(cyclic_list, size):
                survivors = sorted(outside + list(keep))
                sub_pred = {t: {p for p in pred[t] if p in survivors} for t in survivors}
                order = _topo_order(survivors, sub_pred)
                if order is None:
                    continue
                if _replay_matches(order, ops_by_tx, reads_by_tx, initial):
                    candidates.append(survivors)
            if candidates:
                chosen = sorted(candidates)[0]
                break
    if chosen is None:
        # Greedy fallback: peel the lexicographically largest member off a detected cycle;
        # if the graph is acyclic yet replay still fails, sacrifice the largest tx and retry.
        survivors = set(nodes)
        while survivors:
            sub_pred = {t: {p for p in pred[t] if p in survivors} for t in survivors}
            order = _topo_order(survivors, sub_pred)
            if order is not None and _replay_matches(order, ops_by_tx, reads_by_tx, initial):
                break
            rings = _find_cycles(survivors, sub_pred)
            victim = sorted(rings[0])[-1] if rings else sorted(survivors)[-1]
            survivors.discard(victim)
        chosen = sorted(survivors)
    sub_pred = {t: {p for p in pred[t] if p in chosen} for t in chosen}
    order = _topo_order(chosen, sub_pred)
    verified = _replay_matches(order, ops_by_tx, reads_by_tx, initial)
    return {'survivors': chosen, 'sacrificed': sorted(nodes - set(chosen)),
            'serial_order': order, 'conflict_cycles': cycles,
            'serializable': False, 'replay_verified': verified, 'pred': pred}


def _combinations(items, size):
    if size == 0:
        yield ()
        return
    for i, item in enumerate(items):
        for rest in _combinations(items[i + 1:], size - 1):
            yield (item,) + rest


def _reads_by_tx(reads, nodes):
    out = {}
    for rd in reads:
        if rd['tx'] in nodes:
            out.setdefault(rd['tx'], []).append(rd['hits'])
    return out


def _parse_expected_order(text):
    if not text:
        return None
    parts = [p.strip() for p in re.split(r'[,\s]+', text.strip()) if p.strip()]
    return parts or None


def recover(initial, records, crash_after, expected_order=None, expected_balances=None):
    validate(initial, records)
    if not isinstance(crash_after, int) or not 0 <= crash_after <= len(records):
        raise ValueError('崩溃点超出日志范围')
    prefix = records[:crash_after]
    state = copy.deepcopy(initial)
    committed = copy.deepcopy(initial)
    durable = copy.deepcopy(initial)
    own = {}
    active = set()
    locks = {}
    winners = []
    winners_all = []
    aborted = []
    checkpoint = 0
    trace = []
    all_writes = []
    rows_by_tx = {}
    writes_by_tx = {}
    before_by_tx = {}
    ops_by_tx = {}
    reads = []
    for lsn, rec in enumerate(prefix, 1):
        op = rec['op']
        tx = rec.get('tx')
        if op == 'begin':
            active.add(tx)
            own[tx] = {}
            rows_by_tx[tx] = []
            writes_by_tx[tx] = {}
            before_by_tx[tx] = {}
            ops_by_tx[tx] = []
        elif op == 'write':
            row = {'lsn': lsn, 'tx': tx, 'key': rec['key'], 'before': state[rec['key']], 'after': rec['value']}
            rows_by_tx[tx].append(row)
            all_writes.append(row)
            writes_by_tx[tx][rec['key']] = rec['value']
            before_by_tx[tx].setdefault(rec['key'], row['before'])
            ops_by_tx[tx].append(('w', rec['key'], rec['value']))
            locks[rec['key']] = tx
            own[tx][rec['key']] = rec['value']
            state[rec['key']] = rec['value']
            trace.append(dict(row, phase='redo'))
        elif op == 'range_read':
            low, high = rec['low'], rec['high']
            visible = dict(committed)
            visible.update(own[tx])  # read-your-writes; other active txs stay invisible
            hits = _hits(visible, low, high)
            reads.append({'lsn': lsn, 'tx': tx, 'low': low, 'high': high,
                          'hits': hits, 'hit_keys': sorted(hits)})
            ops_by_tx[tx].append(('r', low, high))
        elif op == 'abort':
            for row in reversed(rows_by_tx[tx]):
                state[row['key']] = row['before']
                trace.append(dict(row, phase='redo_abort_undo'))
            del own[tx]
            active.discard(tx)
            aborted.append(tx)
            locks = {k: v for k, v in locks.items() if v != tx}
        elif op == 'commit':
            committed.update(own[tx])
            del own[tx]
            active.discard(tx)
            winners.append(tx)
            winners_all.append(tx)
            locks = {k: v for k, v in locks.items() if v != tx}
        elif op == 'checkpoint':
            checkpoint = lsn
            durable = copy.deepcopy(committed)
            trace = []
            winners = []  # 保持旧语义：committed 仅统计检查点之后的提交
    # Simulated crash: durable pages are exactly the last quiescent checkpoint.
    after_redo = copy.deepcopy(state)
    losers = sorted(active)
    for tx in losers:
        for row in reversed(rows_by_tx.get(tx, ())):
            state[row['key']] = row['before']
            trace.append(dict(row, phase='undo_loser'))
    # Serializability analysis over every transaction committed before the crash.
    plan = _serialization_plan(winners_all, reads, writes_by_tx, before_by_tx, ops_by_tx, initial)
    survivors = plan['survivors']
    sacrificed = plan['sacrificed']
    for tx in sacrificed:
        for row in reversed(rows_by_tx.get(tx, ())):
            state[row['key']] = row['before']
            trace.append(dict(row, phase='undo_sacrificed'))
    # Independent winner-only oracle: apply only survivor commits from the initial ledger.
    oracle = copy.deepcopy(initial)
    for row in sorted(all_writes, key=lambda r: r['lsn']):
        if row['tx'] in survivors:
            oracle[row['key']] = row['after']
    check = _build_check(expected_order, plan['serial_order'], expected_balances, state)
    checksum = hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
    source = json.dumps({'initial': initial, 'records': records, 'crash_after': crash_after}, sort_keys=True)
    return {'simulation': True, 'model_version': 'strict-wal-quiescent-v2', 'crash_after': crash_after,
            'checkpoint_lsn': checkpoint, 'durable_checkpoint': durable, 'after_redo': after_redo,
            'recovered': state, 'oracle': oracle, 'invariant_passed': state == oracle,
            'committed': winners, 'committed_all': winners_all, 'aborted': aborted, 'losers': losers,
            'survivors': survivors, 'sacrificed': sacrificed,
            'serializable': plan['serializable'], 'serial_order': plan['serial_order'],
            'conflict_cycles': plan['conflict_cycles'], 'replay_verified': plan['replay_verified'],
            'read_observations': reads,
            'redo_count': len([t for t in trace if t['phase'] == 'redo']),
            'undo_count': len([t for t in trace if t['phase'] in ('undo_loser', 'undo_sacrificed', 'redo_abort_undo')]),
            'trace': trace, 'check': check,
            'result_sha256': checksum, 'input_sha256': hashlib.sha256(source.encode()).hexdigest(),
            'boundary': 'Python WAL教学模型；严格写锁、静止检查点、整数键值、日志前缀持久。区间读读己之所写且不见他人未提交值；'
                        '冲突环无法排出串行顺序时舍弃最少已提交事务，恢复剔除未提交与被舍弃写入。'
                        '没有注入真实PostgreSQL崩溃，未执行数据库物理恢复。'}


def _build_check(expected_order, serial_order, expected_balances, recovered):
    check = {'expected_order': None, 'order_match': None,
             'expected_balances': None, 'balances_match': None, 'balances_complete': None}
    wanted = _parse_expected_order(expected_order)
    if wanted is not None:
        check['expected_order'] = wanted
        check['order_match'] = wanted == serial_order
    if expected_balances:
        check['expected_balances'] = expected_balances
        check['balances_match'] = all(isinstance(v, int) and recovered.get(k) == v for k, v in expected_balances.items())
        check['balances_complete'] = set(expected_balances) == set(recovered)
    return check
