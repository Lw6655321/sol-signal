"""Planned DEMO exits over the actual router/transport and offline exchange IO."""
import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from src.demo_route_transport import DemoRouteTransport
from src.demo_transport import CREDENTIAL_NAMES
from src.execution import build_intent
from src.order_router import OrderRouter
from test_execution import NOW, inputs
from test_demo_router_integration import Exchange


class PlannedExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.close_unknown = False
        self.hide_close_receipt = False
        self.cancel_unknown = False
        self.cancel_stays_live = False
        self.close_partial = False
        self.fill_entry_on_cancel = None
        self.oco_wins_close_race = False
        self.pending_entry = False
        self.allow_unprotected_close = False
        self.fail_protection = False
        self.fail_amend = False
        self.close_live_after_oco_exit = False
        self.omit_live_close_from_pending = False
        self.order_events = []

    def get(self, url, headers, timeout):
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        if parsed.path.endswith('/trade/fills-history') or parsed.path.endswith('/trade/fills'):
            data = []
            for row in self.orders.values():
                if Decimal(row.get('accFillSz', '0')) <= 0:
                    continue
                entry = row['ordType'] in {'limit', 'post_only'}
                data.append(dict(instId=row['instId'], instType='SWAP', posSide='net', side=row['side'],
                    ordId=row['ordId'], clOrdId=row['clOrdId'], tradeId='trade'+row['ordId'],
                    billId='bill'+row['ordId'], fillTime=str(NOW), ts=str(NOW), fillSz=row['accFillSz'],
                    feeCcy='USDT', fee='0', fillPnl='0', subType=('3' if row['side']=='buy' else '4') if entry else '5'))
            return 200, {'code':'0', 'data':data}
        if parsed.path.endswith('/trade/order'):
            client = query.get('clOrdId', [''])[0]
            row = self.orders.get(client)
            if row and row.get('ordType') == 'market' and self.hide_close_receipt:
                return 200, {'code': '0', 'data': []}
            if query.get('ordId'):
                row = next((item for item in self.orders.values() if item['ordId'] == query['ordId'][0]), None)
                return 200, {'code': '0', 'data': [deepcopy(row)] if row else []}
        if parsed.path.endswith('/trade/orders-algo-pending'):
            return 200, {'code': '0', 'data': [deepcopy(row) for row in self.algos.values()
                if row['ordType'] == query['ordType'][0] and row['state'] == 'live']}
        if parsed.path.endswith('/trade/orders-pending'):
            return 200, {'code': '0', 'data': [deepcopy(row) for row in self.orders.values()
                if row['state'] in {'live', 'partially_filled'} and not (
                    self.omit_live_close_from_pending and row.get('ordType') == 'market')]}
        return super().get(url, headers, timeout)

    def post(self, url, headers, body, timeout):
        data = json.loads(body)
        if url.endswith('/trade/cancel-order'):
            self.posts.append((url, data))
            self.order_events.append('cancel_entry')
            row = self.orders[data['clOrdId']]
            if self.fill_entry_on_cancel is not None:
                quantity = self.fill_entry_on_cancel
                row.update(state='canceled', accFillSz=str(quantity), avgPx='100')
                self.positions = [dict(instId=row['instId'], pos=str(quantity), posSide='net',
                    mgnMode='cross', posId='position1', cTime=str(NOW))]
            else:
                row['state'] = 'canceled'
            return 200, {'code': '0', 'data': [{'sCode': '0', 'ordId': row['ordId']}]}
        if url.endswith('/trade/order-algo') and self.fail_protection:
            self.posts.append((url, data))
            return 200, {'code': '0', 'data': [{'sCode': '51000'}]}
        if url.endswith('/trade/amend-algos'):
            self.posts.append((url, data))
            if self.fail_amend:
                return 200, {'code': '0', 'data': [{'sCode': '51000'}]}
            self.order_events.append('amend_protection')
            algo = next(item for item in self.algos.values() if item['algoId'] == data['algoId'])
            algo['sz'] = data['newSz']
            return 200, {'code': '0', 'data': [{'sCode': '0', 'algoId': algo['algoId']}]}
        if url.endswith('/trade/cancel-algos'):
            self.posts.append((url, data))
            self.order_events.append('cancel_protection')
            assert self.positions == [], 'protection canceled before independent flatness'
            assert len(data) == 1
            algo = next(item for item in self.algos.values() if item['algoId'] == data[0]['algoId'])
            if not self.cancel_stays_live:
                algo['state'] = 'canceled'
            if self.cancel_unknown:
                raise TimeoutError('lost cancellation ACK')
            return 200, {'code': '0', 'data': [{'sCode': '0', 'algoId': algo['algoId']}]}
        if url.endswith('/trade/order') and data['ordType'] == 'market':
            self.posts.append((url, data))
            self.order_events.append('close')
            assert self.allow_unprotected_close or any(row['state'] == 'live' for row in self.algos.values()), 'close lost its OCO'
            actual = Decimal(data['sz']) / 2 if self.close_partial else Decimal(data['sz'])
            if self.oco_wins_close_race:
                actual = Decimal('0')
                algo = next(iter(self.algos.values()))
                algo.update(state='effective', ordIdList=['80001'])
                self.orders['ocochild'] = dict(instId=data['instId'], ordId='80001',
                    clOrdId='', algoId=algo['algoId'], side=data['side'], posSide='net', tdMode='cross',
                    ordType='market', reduceOnly=True, sz=data['sz'], accFillSz=data['sz'], state='filled', avgPx='100')
            state = 'filled' if actual == Decimal(data['sz']) else 'canceled'
            if self.close_live_after_oco_exit:
                state = 'live'
            row = dict(data, ordId='plannedorder', state=state, accFillSz=str(actual), avgPx='100')
            self.orders[data['clOrdId']] = row
            self.positions = [] if not self.close_partial else [dict(self.positions[0], pos=str(Decimal(data['sz'])-actual))]
            if self.close_unknown:
                raise TimeoutError('lost market ACK')
            return 200, {'code': '0', 'data': [{'sCode': '0', 'ordId': row['ordId']}]}
        result = super().post(url, headers, body, timeout)
        if url.endswith('/trade/order-algo'):
            self.order_events.append('protection')
        if url.endswith('/trade/order') and self.pending_entry:
            row = self.orders[data['clOrdId']]
            row.update(state='live', accFillSz='0', avgPx='')
            self.positions = []
        return result


class PlannedCloseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(patch.stopall)
        patch('socket.socket.connect', side_effect=AssertionError('NO_NETWORK')).start()
        patch('src.demo_route_transport.load_demo_credentials', side_effect=AssertionError('NO_CREDENTIALS')).start()
        self.root = Path(self.temp.name)
        signal, self.state, self.config = inputs()
        self.intent = build_intent(signal, self.state, self.config)
        self.state.update(demo_verified=True, observed_at_ms=NOW, account_hash='a'*64)
        self.exchange = PlannedExchange()
        self.credentials = dict(zip(CREDENTIAL_NAMES, ('fake-key', 'fake-secret', 'fake-pass')))
        self.router = self.new_router()

    def new_router(self):
        transport = DemoRouteTransport(self.credentials, enable_writes=True,
            http_get=self.exchange.get, http_post=self.exchange.post, log_root=self.root)
        return OrderRouter(transport, config=self.config,
            state_provider=lambda: deepcopy(self.state),
            market_provider=lambda: {'last':'100', 'timestamp_ms':NOW},
            account_key='DEMO-AUTO-SOL-V1', account_identity='a'*64, log_root=self.root,
            journal_path=self.root/'journal.sqlite3', clock=lambda:NOW)

    def row(self):
        with sqlite3.connect(self.router.journal_path) as db:
            return json.loads(db.execute('SELECT data FROM routes').fetchone()[0])

    def update_row(self, **changes):
        row = self.row()
        row.update(changes)
        with sqlite3.connect(self.router.journal_path) as db:
            db.execute('UPDATE routes SET status=?,data=?', (row['status'], json.dumps(row)))

    def start(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')

    def closes(self):
        return [body for _, body in self.exchange.posts if isinstance(body, dict) and body.get('ordType') == 'market']

    def cancels(self):
        return [body for url, body in self.exchange.posts if url.endswith('/trade/cancel-algos')]

    def test_normal_close_proves_fill_then_cancels_owned_oco_and_preserves_ledger(self):
        self.start()
        result = self.router.planned_close(self.intent)
        self.assertEqual((result.status, result.code), ('CLOSED', 'DEMO_TECHNICAL_TEST_EXIT'))
        self.assertEqual(self.exchange.order_events[-2:], ['close', 'cancel_protection'])
        self.assertEqual(len(self.closes()), 1)
        self.assertIs(self.closes()[0]['reduceOnly'], True)
        row = self.row()
        self.assertEqual(row['close_order_ids'], ['plannedorder'])
        self.assertEqual((row['position_id'], row['position_opened_at_ms']), ('position1', NOW))
        self.assertTrue(row['close_attempted'])
        self.assertTrue(row['cancel_protection_attempted'])
        self.assertFalse(self.router.halt_path.exists())

    def test_terminal_repeated_calls_do_not_touch_later_position(self):
        self.start()
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')
        self.exchange.positions = [dict(instId='SOL-USDT-SWAP', pos='1', posSide='net', mgnMode='cross')]
        writes = len(self.exchange.posts)
        for fn in [self.router.planned_close, self.router.execute, self.router.reconcile_existing]:
            self.assertEqual(fn(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.exchange.posts), writes)

    def test_no_existing_route_never_submits(self):
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.code, 'RECONCILE_NOT_FOUND')
        self.assertEqual(self.exchange.posts, [])

    def test_wrong_account_binding_and_intent_fingerprint_never_write(self):
        self.start()
        before = len(self.exchange.posts)
        from dataclasses import replace
        changed = replace(self.intent, stop=self.intent.stop-Decimal('1'))
        self.assertEqual(self.router.planned_close(changed).code, 'IDEMPOTENCY_CONFLICT')
        self.router.account_identity = 'b'*64
        self.assertEqual(self.router.planned_close(self.intent).code, 'DEMO_ACCOUNT_CHANGED')
        self.assertEqual(len(self.exchange.posts), before)

    def test_close_ack_lost_but_get_filled_completes_without_resend(self):
        self.start()
        self.exchange.close_unknown = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.closes()), 1)

    def test_unknown_close_receipt_does_not_cancel_protection_or_resend(self):
        self.start()
        self.exchange.close_unknown = self.exchange.hide_close_receipt = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertTrue(self.row()['close_id'])

    def test_restart_with_uncertain_close_checkpoint_uses_get_only(self):
        self.start()
        self.exchange.close_unknown = self.exchange.hide_close_receipt = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.router = self.new_router()
        self.router.recover_demo_owners()
        self.assertEqual(self.router.reconcile_existing(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.closes()), 1)
        self.exchange.hide_close_receipt = False
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.closes()), 1)

    def test_planned_checkpoint_blocks_emergency_fallback_post_across_restart(self):
        self.start()
        self.exchange.close_unknown = self.exchange.hide_close_receipt = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.router.emergency_reduce(self.intent).status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.router = self.new_router()
        self.router.recover_demo_owners()
        self.assertEqual(self.router.emergency_reduce(self.intent).status, 'HALTED')
        self.assertEqual(self.router.resume_emergency(self.intent).status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])

    def test_lost_cancel_ack_confirmed_by_get_does_not_halt(self):
        self.start()
        self.exchange.cancel_unknown = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.cancels()), 1)
        self.assertFalse(self.router.halt_path.exists())

    def test_residual_oco_prevents_closed_and_restart_never_reposts_cancel(self):
        self.start()
        self.exchange.cancel_stays_live = self.exchange.cancel_unknown = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.router = self.new_router()
        self.router.recover_demo_owners()
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.cancels()), 1)
        self.assertEqual(len(self.closes()), 1)
        next(iter(self.exchange.algos.values()))['state'] = 'canceled'
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')

    def test_partial_market_close_keeps_protection_and_never_resubmits(self):
        self.start()
        self.exchange.close_partial = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.cancels(), [])
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.row()['close_order_ids'], ['plannedorder'])

    def test_external_position_is_never_reduced_or_unprotected(self):
        self.start()
        self.exchange.positions.append(dict(instId='ETH-USDT-SWAP', pos='1', posSide='net', mgnMode='cross'))
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.cancels(), [])

    def test_same_symbol_replacement_lifecycle_is_never_reduced(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        self.exchange.positions[0]['posId'] = 'differentposition'
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(self.closes(), [])

    def test_foreign_pending_or_algo_is_never_touched(self):
        self.start()
        algo = dict(next(iter(self.exchange.algos.values())), algoId='foreign', algoClOrdId='other')
        self.exchange.algos['other'] = algo
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.cancels(), [])

    def test_corrupt_stop_on_proven_owned_bracket_emergency_reduces_and_halts(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        next(iter(self.exchange.algos.values()))['slTriggerPx'] = '98'
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertEqual(self.cancels(), [])

    def test_sticky_halt_is_not_cleared_by_planned_exit(self):
        self.start()
        self.router.halt_path.write_text('MANUAL_REVIEW', encoding='utf-8')
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.router.halt_path.read_text(encoding='utf-8'), 'MANUAL_REVIEW')

    def test_unfilled_pending_entry_only_cancels_without_claiming_a_trade(self):
        self.exchange.pending_entry = True
        self.assertEqual(self.router.execute(self.intent).status, 'PENDING')
        result = self.router.planned_close(self.intent)
        self.assertEqual((result.status, result.code), ('CANCELED', 'DEMO_TEST_UNFILLED_CANCELED'))
        self.assertEqual(result.filled_qty, 0)
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.exchange.order_events, ['cancel_entry'])

    def test_cancel_race_fill_is_protected_before_close(self):
        self.exchange.pending_entry = True
        self.assertEqual(self.router.execute(self.intent).status, 'PENDING')
        self.exchange.fill_entry_on_cancel = self.intent.qty
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'CLOSED')
        self.assertEqual(self.exchange.order_events, ['cancel_entry', 'protection', 'close', 'cancel_protection'])
        self.assertEqual(result.filled_qty, self.intent.qty)

    def test_partial_entry_cancel_race_amends_full_protection_before_exit(self):
        self.start()
        half = self.intent.qty / 2
        client = self.row()['client_order_id']
        self.exchange.orders[client].update(state='partially_filled', accFillSz=str(half))
        self.exchange.positions[0]['pos'] = str(half)
        next(iter(self.exchange.algos.values()))['sz'] = str(half)
        self.update_row(status='PENDING', entry_state='partially_filled', filled_qty=str(half),
                        protected_qty=str(half), protection_requested_qty=str(half))
        # Resume a previously partially protected process; do not retain the
        # fixture's earlier full-size create request in transport memory.
        self.router = self.new_router()
        self.exchange.fill_entry_on_cancel = self.intent.qty
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'CLOSED')
        self.assertEqual(self.exchange.order_events[-4:], ['cancel_entry', 'amend_protection', 'close', 'cancel_protection'])
        self.assertEqual(self.closes()[0]['sz'], str(self.intent.qty))

    def test_close_checkpoint_is_durable_before_network_call(self):
        self.start()
        actual = self.router.transport.emergency_close
        def assert_checkpoint(*args, **kwargs):
            row = self.row()
            self.assertEqual(row['status'], 'CLOSING')
            self.assertIs(row['close_attempted'], True)
            self.assertEqual(row['close_id'], args[3])
            self.assertEqual(row['close_qty'], str(args[2]))
            return actual(*args, **kwargs)
        with patch.object(self.router.transport, 'emergency_close', side_effect=assert_checkpoint):
            self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')

    def test_cancel_checkpoint_is_durable_before_network_call(self):
        self.start()
        actual = self.router.transport.cancel_protection
        def assert_checkpoint(*args, **kwargs):
            row = self.row()
            self.assertIs(row['cancel_protection_attempted'], True)
            self.assertEqual(row['status'], 'CLOSING')
            self.assertEqual(row['confirmed_closed_qty'], row['filled_qty'])
            self.assertEqual(self.exchange.positions, [])
            return actual(*args, **kwargs)
        with patch.object(self.router.transport, 'cancel_protection', side_effect=assert_checkpoint):
            self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')

    def test_closing_replay_never_enters_advance_or_recreates_oco(self):
        self.start()
        self.exchange.hide_close_receipt = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        with patch.object(self.router, '_advance', side_effect=AssertionError('MUST_NOT_ADVANCE')):
            for fn in [self.router.execute, self.router.reconcile_existing, self.router.reconcile_exit]:
                self.assertEqual(fn(self.intent).status, 'CLOSING')
        self.assertEqual(self.exchange.order_events.count('protection'), 1)
        self.assertEqual(len(self.closes()), 1)

    def test_lost_protection_before_close_emergency_reduces_owned_position_and_halts(self):
        self.start()
        self.exchange.algos.clear()
        self.exchange.allow_unprotected_close = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(result.code, 'DEMO_PLANNED_PROTECTION_UNVERIFIED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())
        self.assertEqual(self.cancels(), [])

    def test_bound_unsafe_protection_matrix_emergency_reduces_once_without_canceling_algo(self):
        variants = [
            ('sz', str(self.intent.qty / 2)), ('sz', str(self.intent.qty * 2)),
            ('side', 'buy'), ('reduceOnly', False), ('slTriggerPx', '98'),
            ('tpTriggerPx', '102'), ('slOrdPx', '99'), ('tpOrdPx', '103'),
            ('tdMode', 'isolated'), ('posSide', 'long'), ('ordType', 'conditional'),
            ('state', 'unrecognized_state'), ('slTriggerPx', 'NaN')]
        for field, value in variants:
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
                self.root = Path(directory)
                self.exchange = PlannedExchange()
                self.router = self.new_router()
                self.start()
                self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
                next(iter(self.exchange.algos.values()))[field] = value
                self.exchange.allow_unprotected_close = True
                result = self.router.planned_close(self.intent)
                self.assertEqual(result.status, 'HALTED')
                self.assertEqual(len(self.closes()), 1)
                self.assertEqual(self.exchange.positions, [])
                self.assertEqual(self.cancels(), [])
                self.assertTrue(self.router.halt_path.exists())

    def test_effective_terminal_partial_child_reduces_only_proved_remaining_position(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        half = self.intent.qty / 2
        self.activate_oco_child(state='canceled', actual=str(half))
        self.exchange.positions[0]['pos'] = str(half)
        self.exchange.allow_unprotected_close = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(Decimal(self.closes()[0]['sz']), half)
        self.assertEqual(self.exchange.positions, [])
        self.assertEqual(set(self.row()['close_order_ids']), {'80001', 'plannedorder'})
        self.assertTrue(self.router.halt_path.exists())

    def test_effective_live_child_keeps_owned_position_without_duplicate_reduce(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        self.activate_oco_child(state='live')
        self.exchange.omit_live_close_from_pending = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.cancels(), [])
        self.assertNotEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())

    def test_canceled_bracket_before_close_emergency_reduces_owned_naked_position(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        next(iter(self.exchange.algos.values()))['state'] = 'canceled'
        self.exchange.allow_unprotected_close = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(result.code, 'DEMO_PLANNED_PROTECTION_UNVERIFIED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())

    def test_failed_bracket_before_close_emergency_reduces_owned_naked_position(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        next(iter(self.exchange.algos.values()))['state'] = 'order_failed'
        self.exchange.allow_unprotected_close = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(result.code, 'DEMO_PLANNED_PROTECTION_UNVERIFIED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())

    def test_unqueryable_protection_before_close_reduces_proven_lifecycle_and_halts(self):
        self.start()
        self.exchange.protection_unknown = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(result.code, 'DEMO_PLANNED_PROTECTION_UNVERIFIED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())

    def test_cancel_race_first_protection_failure_emergency_reduces_and_halts(self):
        self.exchange.pending_entry = True
        self.assertEqual(self.router.execute(self.intent).status, 'PENDING')
        self.exchange.fill_entry_on_cancel = self.intent.qty
        self.exchange.fail_protection = self.exchange.allow_unprotected_close = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())

    def test_cancel_race_protection_amend_failure_emergency_reduces_and_halts(self):
        self.start()
        half = self.intent.qty / 2
        client = self.row()['client_order_id']
        self.exchange.orders[client].update(state='partially_filled', accFillSz=str(half))
        self.exchange.positions[0]['pos'] = str(half)
        next(iter(self.exchange.algos.values()))['sz'] = str(half)
        self.update_row(status='PENDING', entry_state='partially_filled', filled_qty=str(half),
                        protected_qty=str(half), protection_requested_qty=str(half))
        self.router = self.new_router()
        self.exchange.fill_entry_on_cancel = self.intent.qty
        self.exchange.fail_amend = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())
        self.assertEqual(self.cancels(), [])

    def test_protection_lost_after_close_attempt_never_sends_second_close(self):
        self.start()
        self.exchange.close_partial = True
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.exchange.algos.clear()
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.router.emergency_reduce(self.intent).status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)

    def test_oco_wins_market_race_requires_real_child_receipt(self):
        self.start()
        self.exchange.oco_wins_close_race = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'CLOSED')
        self.assertEqual(self.row()['close_order_ids'], ['80001'])
        self.assertEqual(self.row()['planned_close_receipt_state'], 'canceled')
        self.assertEqual(self.row()['planned_oco_closed_qty'], str(self.intent.qty))
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertFalse(self.router.halt_path.exists())

    def test_live_zero_close_plus_oco_full_exit_and_missing_pending_is_not_closed(self):
        self.start()
        self.exchange.oco_wins_close_race = True
        self.exchange.close_live_after_oco_exit = True
        self.exchange.omit_live_close_from_pending = True
        result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'CLOSING')
        self.assertEqual(result.code, 'DEMO_PLANNED_CLOSE_PENDING')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.exchange.orders[self.row()['close_id']]['state'] = 'canceled'
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.closes()), 1)

    def test_effective_oco_with_missing_child_evidence_never_claims_closed(self):
        self.start()
        self.exchange.oco_wins_close_race = True
        actual = self.exchange.post
        def no_child_list(url, headers, body, timeout):
            result = actual(url, headers, body, timeout)
            if url.endswith('/trade/order') and json.loads(body).get('ordType') == 'market':
                next(iter(self.exchange.algos.values())).pop('ordIdList', None)
            return result
        with patch.object(self.router.transport, '_http_post', side_effect=no_child_list):
            self.assertEqual(self.router.planned_close(self.intent).status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertTrue(self.router.halt_path.exists())

    def activate_oco_child(self, *, state='live', actual='0'):
        parent = next(iter(self.exchange.algos.values()))
        parent.update(state='effective', ordIdList=['80001'])
        self.exchange.orders['cancel-race-child'] = dict(
            instId=self.intent.inst_id, ordId='80001', clOrdId='', algoId=parent['algoId'],
            side='sell', posSide='net', tdMode='cross', ordType='market', reduceOnly=True,
            sz=str(self.intent.qty), accFillSz=actual, state=state, avgPx='100')

    def test_cancel_internal_get_triggers_live_child_omitted_from_pending_never_closed(self):
        self.start()
        self.exchange.omit_live_close_from_pending = True
        cancel = self.router.transport.cancel_protection
        def trigger_inside_cancel(*args):
            self.activate_oco_child()
            return cancel(*args)
        with patch.object(self.router.transport, 'cancel_protection', side_effect=trigger_inside_cancel):
            result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertTrue(self.router.halt_path.exists())

    def test_cancel_ack_parent_still_live_beats_empty_aggregate_across_restart(self):
        self.start()
        self.exchange.cancel_stays_live = True
        def omit_after_cancel(url, headers, timeout):
            if self.cancels() and urlsplit(url).path.endswith('/trade/orders-algo-pending'):
                return 200, {'code': '0', 'data': []}
            return self.exchange.get(url, headers, timeout)
        with patch.object(self.router.transport._reader, '_http_get', side_effect=omit_after_cancel):
            self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.cancels()), 1)
        self.router = self.new_router()
        self.router.recover_demo_owners()
        with patch.object(self.router.transport._reader, '_http_get', side_effect=omit_after_cancel):
            self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.cancels()), 1)
        self.assertEqual(len(self.closes()), 1)
        next(iter(self.exchange.algos.values()))['state'] = 'canceled'
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSED')

    def test_cancel_triggered_zero_fill_terminal_child_can_complete_after_exact_proof(self):
        self.start()
        cancel = self.router.transport.cancel_protection
        def trigger_terminal_inside_cancel(*args):
            self.activate_oco_child(state='canceled')
            return cancel(*args)
        with patch.object(self.router.transport, 'cancel_protection', side_effect=trigger_terminal_inside_cancel):
            result = self.router.planned_close(self.intent)
        self.assertEqual(result.status, 'CLOSED')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])
        self.assertEqual(self.row()['planned_oco_closed_qty'], '0')
        self.assertFalse(self.router.halt_path.exists())

    def test_known_effective_parent_cannot_disappear_to_hide_unknown_children(self):
        self.start()
        cancel = self.router.transport.cancel_protection
        def effective_then_absent(*args):
            self.activate_oco_child()
            result = cancel(*args)
            self.exchange.algos.clear()
            self.exchange.orders.pop('cancel-race-child')
            return result
        with patch.object(self.router.transport, 'cancel_protection', side_effect=effective_then_absent):
            result = self.router.planned_close(self.intent)
        self.assertEqual((result.status, result.code), ('CLOSING', 'DEMO_PLANNED_OCO_CHILDREN_UNKNOWN'))
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(len(self.closes()), 1)
        self.assertEqual(self.cancels(), [])

    def test_absent_oco_without_exit_receipt_never_counts_as_closed(self):
        self.start()
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        self.exchange.positions, self.exchange.algos = [], {}
        self.assertEqual(self.router.planned_close(self.intent).status, 'CLOSING')
        self.assertEqual(self.closes(), [])
        self.assertEqual(self.cancels(), [])


if __name__ == '__main__':
    unittest.main()
