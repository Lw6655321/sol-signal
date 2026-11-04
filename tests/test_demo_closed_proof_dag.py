"""Dependency-ready closed proof GETs; actual network and credentials forbidden."""
from concurrent.futures import ThreadPoolExecutor as RealExecutor
import threading
import unittest
from unittest.mock import patch

from src import demo_route_transport as T
import test_demo_halted_exit_receipts as fixtures


class ClosedProofDAGTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.ClosedHaltedReceipts()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_first_server_time_and_full_exposure_are_really_parallel(self):
        public, exposure = threading.Event(), threading.Event()
        calls = 0

        def hook(path, _params):
            nonlocal calls
            if path.endswith('/public/time'):
                calls += 1
                if calls == 1:
                    public.set()
                    self.assertTrue(exposure.wait(1), 'exposure was serialized after first server time')
            elif path.endswith('/account/positions') and self.helper.position_reads == 0:
                exposure.set()
                self.assertTrue(public.wait(1), 'server time was serialized after exposure')

        self.helper.on_get = hook
        proof = self.helper.verify()
        self.assertTrue(proof['closed_lifecycle_verified'])
        self.assertEqual(proof['observed_at_ms'], fixtures.NOW)
        self.assertEqual(len(self.helper.gets), 29)
        self.assertEqual(self.helper.posts, [])

    def test_validated_parent_releases_children_before_slow_independent_entry(self):
        child = threading.Event()

        def hook(path, params):
            if path.endswith('/trade/order'):
                if params.get('clOrdId') == [self.helper.client]:
                    self.assertTrue(child.wait(1), 'children waited for the unrelated entry GET')
                else:
                    self.assertEqual(params['ordId'], ['301'])
                    child.set()

        self.helper.on_get = hook
        self.assertTrue(self.helper.verify()['closed_lifecycle_verified'])
        self.assertTrue(child.is_set())
        self.assertEqual(self.helper.posts, [])

    def test_invalid_parent_never_dispatches_any_child_or_second_wave(self):
        self.helper.parent['slTriggerPx'] = '98'
        self.helper.refused('DEMO_CLOSED_PROTECTION_MISMATCH')
        children = [params for path, params in self.helper.gets if path.endswith('/trade/order') and 'ordId' in params]
        self.assertEqual(children, [])
        self.assertLessEqual(self.helper.position_reads, 1)
        self.assertEqual(self.helper.parent_reads, 1)

    def test_second_wave_waits_for_every_first_read_and_all_children(self):
        child_started, independent_done, child_done = (threading.Event() for _ in range(3))

        def hook(path, params):
            if path.endswith('/account/config'):
                self.assertTrue(child_started.wait(1))
                independent_done.set()
            elif path.endswith('/trade/order') and 'ordId' in params:
                child_started.set()
                self.assertTrue(independent_done.wait(1))
                child_done.set()
            elif path.endswith('/trade/order-algo') and self.helper.parent_reads == 1:
                self.assertTrue(independent_done.is_set())
                self.assertTrue(child_done.is_set())

        self.helper.on_get = hook
        proof = self.helper.verify()
        self.assertTrue(proof['closed_lifecycle_verified'])
        self.assertEqual(self.helper.position_reads, 2)
        self.assertEqual(self.helper.parent_reads, 2)

    def test_first_full_account_flat_check_precedes_second_wave(self):
        self.helper.positions = [{'instId': 'BTC-USDT-SWAP', 'posSide': 'net', 'pos': '1'}]
        self.helper.refused('DEMO_CLOSED_EXPOSURE_NOT_FLAT')
        self.assertEqual(self.helper.position_reads, 1)
        self.assertEqual(self.helper.parent_reads, 1)

    def test_real_end_time_keeps_unfiltered_tail_and_history_queries(self):
        initial = self.helper.server
        times = 0

        def hook(path, _params):
            nonlocal times
            if path.endswith('/public/time'):
                times += 1
                if times == 2:
                    self.helper.server = initial + 1

        self.helper.on_get = hook
        proof = self.helper.verify()
        self.assertEqual(proof['observed_at_ms'], initial)
        self.assertEqual(proof['history_through_ms'], initial + 1)
        fills = [params for path, params in self.helper.gets if path.endswith('/trade/fills-history')]
        history = [params for path, params in self.helper.gets if path.endswith('/account/positions-history')]
        self.assertEqual(fills, [
            {'instType': ['SWAP'], 'instId': [T.INST], 'begin': [str(self.helper.flat)],
             'end': [str(initial)], 'limit': ['100']},
            {'instType': ['SWAP'], 'instId': [T.INST], 'begin': [str(initial + 1)],
             'end': [str(initial + 1)], 'limit': ['100']}])
        self.assertEqual([row['after'] for row in history], [[str(initial + 1)], [str(initial + 2)]])
        self.assertEqual(len(self.helper.gets), 31)
        self.assertEqual(self.helper.posts, [])

    def test_first_time_failure_cannot_dispatch_history_dependants(self):
        def hook(path, _params):
            if path.endswith('/public/time'):
                raise TimeoutError('offline failed GET')

        self.helper.on_get = hook
        self.helper.refused('DEMO_READ_UNKNOWN')
        self.assertFalse(any(path.endswith(('/trade/fills-history', '/account/positions-history'))
                             for path, _ in self.helper.gets))
        self.assertEqual(sum(path.endswith('/public/time') for path, _ in self.helper.gets), 1)

    def test_failure_cancels_queued_work_and_drains_every_inflight_get(self):
        all_submitted, failed, cancelled, release = (threading.Event() for _ in range(4))
        lock = threading.Lock()
        futures, active = [], set()
        submission_count = 0
        failed_once = False
        parent = self.helper.parent
        parent['ordIdList'] = ['301', '302', '303', '304']

        class ObservedExecutor(RealExecutor):
            def __init__(inner, *args, **kwargs):
                self.assertEqual(kwargs.get('max_workers'), 16)
                super().__init__(*args, **kwargs)

            def submit(inner, *args, **kwargs):
                nonlocal submission_count
                future = super().submit(*args, **kwargs)
                future.add_done_callback(lambda done: cancelled.set() if done.cancelled() else None)
                futures.append(future)
                submission_count += 1
                if submission_count == 20:
                    all_submitted.set()
                return future

        original = self.helper.transport._reader._http_get

        def get(url, headers, timeout):
            nonlocal failed_once
            from urllib.parse import parse_qs, urlsplit
            path, params = urlsplit(url).path, parse_qs(urlsplit(url).query)
            identity = threading.get_ident()
            with lock:
                active.add(identity)
            try:
                if path.endswith('/public/time') or path.endswith('/trade/order-algo'):
                    return original(url, headers, timeout)
                if path.endswith('/trade/order') and 'ordId' in params:
                    self.assertTrue(all_submitted.wait(1))
                    with lock:
                        fail = not failed_once
                        failed_once = True
                    if fail:
                        failed.set()
                        raise TimeoutError('offline single GET failure')
                self.assertTrue(release.wait(2))
                return original(url, headers, timeout)
            finally:
                with lock:
                    active.remove(identity)

        self.helper.transport._reader._http_get = get
        with patch.object(T, 'ThreadPoolExecutor', ObservedExecutor), RealExecutor(max_workers=1) as supervisor:
            run = supervisor.submit(self.helper.verify)
            try:
                self.assertTrue(failed.wait(1.5))
                self.assertFalse(run.done(), 'proof returned while a failed wave still had live GETs')
                self.assertTrue(cancelled.wait(1), 'queued reads were not cancelled before draining')
            finally:
                release.set()
            with self.assertRaises(T.DemoRouteError):
                run.result(timeout=2)
        self.assertTrue(any(future.cancelled() for future in futures))
        self.assertEqual(active, set())
        self.assertFalse(any(thread.name.startswith('demo-closed-read') for thread in threading.enumerate()))
        self.assertEqual(self.helper.posts, [])
        self.assertEqual(sum(path.endswith('/public/time') for path, _ in self.helper.gets), 1)


if __name__ == '__main__':
    unittest.main()
