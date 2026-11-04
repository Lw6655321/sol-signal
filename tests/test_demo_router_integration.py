"""Real DEMO transport + real router over deterministic, network-free HTTP."""
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


class Exchange:
    def __init__(self):
        self.orders, self.algos, self.positions, self.posts = {}, {}, [], []
        self.protection_unknown = False

    def get(self, url, headers, timeout):
        assert headers['x-simulated-trading'] == '1'
        assert url.startswith('https://openapi.okx.com/api/v5/')
        parsed = urlsplit(url)
        params = parse_qs(parsed.query)
        if parsed.path.endswith('/public/time'):
            data = [{'ts': str(NOW)}]
        elif parsed.path.endswith('/trade/fills-history') or parsed.path.endswith('/trade/fills'):
            data = []
            for row in reversed(list(self.orders.values())):
                if row.get('state') != 'filled':
                    continue
                is_entry = row['ordType'] in {'limit', 'post_only'}
                data.append(dict(instId=row['instId'], instType='SWAP', posSide='net',
                                 side=row['side'], ordId=row['ordId'], clOrdId=row['clOrdId'],
                                 tradeId='trade' + row['ordId'], billId='bill' + row['ordId'],
                                 fillTime=str(NOW), ts=str(NOW), fillSz=row['accFillSz'],
                                 feeCcy='USDT', fee='0', fillPnl='0',
                                 subType=('3' if row['side'] == 'buy' else '4') if is_entry else '5'))
        elif parsed.path.endswith('/trade/order'):
            row = self.orders.get(params['clOrdId'][0])
            data = [row] if row else []
        elif parsed.path.endswith('/trade/order-algo'):
            if self.protection_unknown:
                raise TimeoutError('fake read uncertainty')
            row = self.algos.get(params['algoClOrdId'][0])
            data = [row] if row else []
        elif parsed.path.endswith('/account/positions'):
            data = self.positions
        elif parsed.path.endswith('/trade/orders-pending'):
            data = [row for row in self.orders.values() if row['state'] == 'live']
        elif parsed.path.endswith('/trade/orders-algo-pending'):
            data = [row for row in self.algos.values() if row['ordType'] == params['ordType'][0]]
        else:
            raise AssertionError('unexpected read')
        return 200, {'code': '0', 'data': deepcopy(data)}

    def post(self, url, headers, body, timeout):
        assert headers['x-simulated-trading'] == '1'
        data = json.loads(body)
        self.posts.append((url, data))
        if url.endswith('/trade/order'):
            row = dict(data, ordId='order' + str(len(self.orders) + 1), state='filled',
                       accFillSz=data['sz'], avgPx=data.get('px', '100'), fillTime=str(NOW),
                       cTime=str(NOW), uTime=str(NOW))
            self.orders[data['clOrdId']] = row
            if data['ordType'] == 'market':
                self.positions = []
                self.algos = {}
            else:
                signed_size = str(Decimal(data['sz']) * (1 if data['side'] == 'buy' else -1))
                self.positions = [dict(instId=data['instId'], pos=signed_size, posSide='net',
                                       mgnMode='cross', posId='position1', cTime=str(NOW))]
            ack = {'sCode': '0', 'ordId': row['ordId']}
        elif url.endswith('/trade/order-algo'):
            row = dict(data, algoId='algo1', state='live')
            self.algos[data['algoClOrdId']] = row
            ack = {'sCode': '0', 'algoId': 'algo1'}
        else:
            raise AssertionError('unexpected write')
        return 200, {'code': '0', 'data': [ack]}


class DemoRouterIntegration(unittest.TestCase):
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
        self.exchange = Exchange()
        credentials = dict(zip(CREDENTIAL_NAMES, ('fake-key', 'fake-secret', 'fake-pass')))
        self.transport = DemoRouteTransport(credentials, enable_writes=True,
                         http_get=self.exchange.get, http_post=self.exchange.post, log_root=self.root)
        self.router = OrderRouter(self.transport, config=self.config,
                         state_provider=lambda: deepcopy(self.state),
                         market_provider=lambda: {'last':'100', 'timestamp_ms':NOW},
                         account_key='DEMO-AUTO-SOL-V1', account_identity='a'*64, log_root=self.root,
                         journal_path=self.root/'journal.sqlite3', clock=lambda:NOW)

    def test_protected_route_and_independently_verified_exit_are_terminal(self):
        result = self.router.execute(self.intent)
        self.assertEqual(result.status, 'PROTECTED')
        self.assertEqual(result.filled_qty, result.protected_qty)
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        writes = len(self.exchange.posts)
        self.exchange.positions, self.exchange.algos = [], {}
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'CLOSED')
        self.assertEqual(self.router.execute(self.intent).status, 'CLOSED')
        self.assertEqual(self.router.reconcile_existing(self.intent).status, 'CLOSED')
        self.assertEqual(len(self.exchange.posts), writes)

    def test_reconciliation_without_durable_route_never_submits(self):
        self.assertEqual(self.router.reconcile_existing(self.intent).code, 'RECONCILE_NOT_FOUND')
        self.assertEqual(self.exchange.posts, [])

    def test_demo_risk_not_verified_never_submits(self):
        self.state['demo_verified'] = False
        self.assertEqual(self.router.execute(self.intent).code, 'DEMO_STATE_UNVERIFIED')
        self.assertEqual(self.exchange.posts, [])

    def test_fake_demo_label_cannot_bypass_transport_capability(self):
        class Impostor:
            mode = 'demo'
        self.router.transport = Impostor()
        self.assertEqual(self.router.execute(self.intent).code, 'DEMO_TRANSPORT_REQUIRED')
        self.assertEqual(self.exchange.posts, [])

    def test_stale_demo_account_never_submits(self):
        self.state['observed_at_ms'] = NOW - 5001
        self.assertEqual(self.router.execute(self.intent).code, 'DEMO_STATE_STALE')
        self.assertEqual(self.exchange.posts, [])

    def test_changed_account_identity_never_submits(self):
        self.state['account_hash'] = 'b'*64
        self.assertEqual(self.router.execute(self.intent).code, 'DEMO_ACCOUNT_CHANGED')
        self.assertEqual(self.exchange.posts, [])

    def test_route_records_exchange_ledger_identity(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'PROTECTED')
        with sqlite3.connect(self.router.journal_path) as db:
            row = json.loads(db.execute('SELECT data FROM routes').fetchone()[0])
        self.assertEqual(row['account_hash'], 'a'*64)
        self.assertEqual(row['inst_id'], 'SOL-USDT-SWAP')
        self.assertEqual(row['position_id'], 'position1')
        self.assertEqual(row['position_opened_at_ms'], NOW)

    def test_existing_route_cannot_be_replayed_for_another_account(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')
        writes = len(self.exchange.posts)
        self.router.account_identity = 'b'*64
        self.state['account_hash'] = 'b'*64
        self.assertEqual(self.router.reconcile_existing(self.intent).code, 'DEMO_ACCOUNT_CHANGED')
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'UNKNOWN')
        self.assertEqual(len(self.exchange.posts), writes)

    def test_uncertain_protection_read_uses_reduce_only_emergency(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')
        self.exchange.protection_unknown = True
        result = self.router.reconcile_exit(self.intent)
        self.assertEqual(result.status, 'HALTED')
        self.assertEqual(result.emergency_status, 'FLAT_CONFIRMED')
        self.assertEqual(self.exchange.positions, [])
        closes = [body for _, body in self.exchange.posts if body.get('ordType') == 'market']
        self.assertEqual(len(closes), 1)
        self.assertIs(closes[0]['reduceOnly'], True)

    def test_short_emergency_verifies_signed_position_and_buys_reduce_only(self):
        signal, _, _ = inputs()
        signal.update(id='DEMO-SHORT-SAFETY', side='short', stop='101', tp1='97', tp2='96')
        intent = build_intent(signal, self.state, self.config)
        self.assertEqual(self.router.execute(intent).status, 'PROTECTED')
        self.assertLess(Decimal(self.exchange.positions[0]['pos']), 0)
        self.exchange.protection_unknown = True
        result = self.router.reconcile_exit(intent)
        self.assertEqual(result.emergency_status, 'FLAT_CONFIRMED')
        closes = [body for _, body in self.exchange.posts if body.get('ordType') == 'market']
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]['side'], 'buy')
        self.assertIs(closes[0]['reduceOnly'], True)

    def test_lifecycle_audit_disk_failure_does_not_strand_proved_exposure(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')
        self.exchange.protection_unknown = True
        original = Path.write_text
        def fail_lifecycle(path, *args, **kwargs):
            if path.name.startswith('lifecycle-'):
                raise OSError('offline audit disk failure')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'write_text', autospec=True, side_effect=fail_lifecycle):
            result = self.router.reconcile_exit(self.intent)
        self.assertEqual(result.emergency_status, 'FLAT_CONFIRMED')
        self.assertEqual(self.exchange.positions, [])
        self.assertTrue(self.router.halt_path.exists())
        self.assertEqual(len([body for _, body in self.exchange.posts if body.get('ordType') == 'market']), 1)

    def test_unrelated_account_position_is_never_flattened(self):
        self.assertEqual(self.router.execute(self.intent).status, 'PROTECTED')
        writes = len(self.exchange.posts)
        self.exchange.positions.append(dict(instId='BTC-USDT-SWAP', pos='1', posSide='net', mgnMode='cross'))
        self.assertEqual(self.router.reconcile_exit(self.intent).status, 'UNKNOWN')
        self.assertEqual(len(self.exchange.posts), writes)


if __name__ == '__main__':
    unittest.main()
