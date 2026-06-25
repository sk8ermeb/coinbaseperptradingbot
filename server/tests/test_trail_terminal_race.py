"""Regression tests for LiveTrader._emit_order_terminal_event.

Focus: the trail-terminal race that retired a still-live trailing order.

Background (post-mortem 2026-06-24, liveorder row 33, a Buy trail):
A storm of place -> async-FAILED -> re-place cycles ran for ~45 min. The
killing blow was a single async FAILED for the row's just-placed exchange
order arriving *while a retry thread still owned the row* (the row id was
still in `_trail_retrying_rows`). The old guard only handed the FAILED to
the retry loop when the row was NOT in `_trail_retrying_rows`; otherwise it
fell through and terminally marked the row `status='failed'`. That dropped
the row from `pendingpositions` ("Coinbase order, no internal order") and
froze it so it stopped trailing.

These tests pin the invariant the code comments already claim: while a retry
is in-flight, the retry thread is the ONLY authority allowed to terminate a
trail row. A genuine FILL still terminates; a FAILED with no retry in-flight
still kicks off a fresh retry.

No network: `_emit_order_terminal_event` never calls Coinbase directly (only
`_start_trail_retry` does, and we stub it). The util singleton is pointed at
a throwaway SQLite file so the real db.sqlite is never touched.

Run:  venv/bin/python -m unittest discover -s server/tests
"""

import os
import sys
import sqlite3
import tempfile
import threading
import time
import unittest

# This file lives at server/tests/ — make server/ importable so `import live`
# (which does `import util`, `from coinbase_http import ...`) resolves.
SERVER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

import live  # noqa: E402

# Mirror of the production liveorder schema (see server/util.py). Kept inline
# so the test owns its fixture and doesn't depend on the real DB's table.
LIVEORDER_SCHEMA = """CREATE TABLE liveorder (
    id INTEGER PRIMARY KEY,
    scriptid INTEGER,
    coinbase_order_id TEXT,
    internal_id TEXT UNIQUE,
    tradetype TEXT,
    limitprice REAL,
    stopprice REAL,
    amount REAL,
    limittrailpercent REAL,
    stoptrailpercent REAL,
    status TEXT,
    time INTEGER,
    activated INTEGER DEFAULT 0,
    peak_price REAL DEFAULT 0,
    hard_stopprice REAL DEFAULT 0,
    entry_costbasis REAL DEFAULT 0
)"""


class TrailTerminalRaceTest(unittest.TestCase):
    def setUp(self):
        # Redirect the util singleton at a throwaway DB. runupdate/runselect
        # reconnect to `self._sqlfile` on every call, so this fully isolates
        # the test from server/data/db.sqlite.
        fd, self._tmp_db = tempfile.mkstemp(suffix='.sqlite')
        os.close(fd)
        self._orig_sqlfile = live.lutil._sqlfile
        live.lutil._sqlfile = self._tmp_db
        conn = sqlite3.connect(self._tmp_db)
        conn.execute(LIVEORDER_SCHEMA)
        conn.commit()
        conn.close()

        # Construct a LiveTrader WITHOUT __init__ (which builds a Coinbase
        # client, namespace, etc.). Wire only what the method under test reads.
        lt = live.LiveTrader.__new__(live.LiveTrader)
        lt._trail_retrying_rows = set()
        lt._trail_canceling_ids = set()
        lt._trail_recent_cancels = {}
        lt._trail_death_lock = threading.Lock()
        lt._trail_death_streak = {}
        lt._trail_last_place = {}
        lt._placed_order_ids = set()
        lt.namespace = {'realposition': 0}
        lt._ws_product_id = 'BIP-20DEC30-CDE'
        lt.pair = 'BIP-20DEC30-CDE'
        lt.running = True

        # Capture side-effects instead of performing them.
        self.logs = []
        self.events = []
        self.retry_calls = []
        lt._livelog = lambda msg: self.logs.append(msg)
        lt._log_event = lambda et, data, notify=True: self.events.append(
            (et, data, notify))
        lt._start_trail_retry = lambda *a, **k: self.retry_calls.append((a, k))
        self.lt = lt

    def tearDown(self):
        live.lutil._sqlfile = self._orig_sqlfile
        try:
            os.unlink(self._tmp_db)
        except OSError:
            pass

    # -- fixtures ---------------------------------------------------------

    def _make_row(self, **over):
        """Insert a trailing Buy liveorder and return it as the dict the
        WS/reconciliation path would hand to _emit_order_terminal_event."""
        row = dict(
            id=33, scriptid=4, coinbase_order_id='b7b3e9b7', internal_id='int-33',
            tradetype='Buy', limitprice=60761.6, stopprice=60761.6, amount=1.0,
            limittrailpercent=0.01, stoptrailpercent=0.0, status='open',
            time=0, activated=1, peak_price=60160.0, hard_stopprice=0.0,
            entry_costbasis=0.0,
        )
        row.update(over)
        cols = ','.join(row.keys())
        ph = ','.join('?' for _ in row)
        conn = sqlite3.connect(self._tmp_db)
        conn.execute(f"INSERT INTO liveorder ({cols}) VALUES ({ph})",
                     tuple(row.values()))
        conn.commit()
        conn.close()
        return row

    def _db_status(self, row_id=33):
        rows = live.lutil.runselect(
            "SELECT status FROM liveorder WHERE id=?", (row_id,))
        return rows[0]['status']

    def _event_types(self):
        return [et for et, _data, _notify in self.events]

    # -- tests ------------------------------------------------------------

    def test_async_fail_during_retry_keeps_row_open(self):
        """THE REGRESSION: a FAILED for the row's order while a retry thread
        owns the row must NOT terminate the row. It stays 'open' (still in
        pendingpositions, still trailing) and no second retry is spawned."""
        row = self._make_row()
        self.lt._trail_retrying_rows.add(row['id'])  # retry in-flight

        result = self.lt._emit_order_terminal_event(row, 'FAILED', {})

        self.assertTrue(result)
        self.assertEqual(self._db_status(), 'open',
                         "row must stay open while a retry owns it")
        self.assertEqual(self.retry_calls, [],
                         "must not start a second retry — one already owns it")
        self.assertNotIn('fail:Buy', self._event_types(),
                         "must not emit a terminal fail event")

    def test_async_fail_without_retry_starts_retry(self):
        """No retry in-flight: a FAILED hands off to the retry loop and the
        row stays open (the retry's give-up path is the only terminator)."""
        row = self._make_row()

        result = self.lt._emit_order_terminal_event(row, 'FAILED', {})

        self.assertTrue(result)
        self.assertEqual(self._db_status(), 'open')
        self.assertEqual(len(self.retry_calls), 1,
                         "should kick off exactly one retry")
        self.assertIn('trail-order-died:Buy', self._event_types())
        self.assertNotIn('fail:Buy', self._event_types())

    def test_fill_terminates_even_with_retry_in_flight(self):
        """A genuine FILL is always terminal — the stop did its job — even if
        a retry happened to be in-flight. No retry is (re)started."""
        row = self._make_row()
        self.lt._trail_retrying_rows.add(row['id'])
        payload = {'filled_size': '1', 'average_filled_price': '60970',
                   'total_fees': '0'}

        result = self.lt._emit_order_terminal_event(row, 'FILLED', payload)

        self.assertTrue(result)
        self.assertEqual(self._db_status(), 'filled')
        self.assertEqual(self.retry_calls, [])
        self.assertIn('fill:Buy', self._event_types())

    def test_async_fail_gives_up_past_cap(self):
        """Once the death streak passes the cap, the async-death path stops
        re-placing and gives up for real: row -> cancelled, a notifying
        cancel:gaveup event, and NO further retry scheduled."""
        row = self._make_row()
        # Pre-load the streak right at the cap and a recent placement so the
        # next death escalates past the max (no survival reset).
        self.lt._trail_death_streak[row['id']] = self.lt._TRAIL_MAX_DEATH_STREAK
        self.lt._trail_last_place[row['id']] = time.monotonic()

        result = self.lt._emit_order_terminal_event(row, 'FAILED', {})

        self.assertTrue(result)
        self.assertEqual(self._db_status(), 'cancelled', "must give up terminally")
        self.assertEqual(self.retry_calls, [], "must not schedule another retry")
        gaveup = [e for e in self.events if e[0] == 'cancel:gaveup:Buy']
        self.assertEqual(len(gaveup), 1, "must emit one gaveup event")
        self.assertTrue(gaveup[0][2], "gaveup must notify the user")
        # Breaker state cleared on terminal.
        self.assertNotIn(row['id'], self.lt._trail_death_streak)

    def test_fail_without_trail_terminates(self):
        """A non-trailing order (limittrailpercent=0) that FAILs is terminal
        immediately — the trail-protection branch must not apply to it."""
        row = self._make_row(limittrailpercent=0.0)

        result = self.lt._emit_order_terminal_event(row, 'FAILED', {})

        self.assertTrue(result)
        self.assertEqual(self._db_status(), 'failed')
        self.assertEqual(self.retry_calls, [])
        self.assertIn('fail:Buy', self._event_types())

    def test_already_terminal_is_noop(self):
        """Idempotency: a row already terminal short-circuits with no writes
        or events (both WS and reconciliation can deliver the same status)."""
        row = self._make_row(status='failed')

        result = self.lt._emit_order_terminal_event(row, 'FAILED', {})

        self.assertTrue(result)
        self.assertEqual(self.events, [])
        self.assertEqual(self.retry_calls, [])


class TrailCircuitBreakerTest(unittest.TestCase):
    """The #2 fix: a flapping trail order's re-place backs off via an
    escalating cooldown keyed on a death streak, and the streak resets only
    when an order actually survives on the book (not merely because the rate
    dropped). These exercise the pure breaker bookkeeping — no DB/network."""

    def setUp(self):
        lt = live.LiveTrader.__new__(live.LiveTrader)
        lt._trail_death_lock = threading.Lock()
        lt._trail_death_streak = {}
        lt._trail_last_place = {}
        self.lt = lt

    def test_cooldown_schedule(self):
        c = self.lt._trail_death_cooldown
        self.assertEqual(c(1), 0.0)
        self.assertEqual(c(2), 0.0)
        self.assertEqual(c(3), 5.0)
        self.assertEqual(c(4), 10.0)
        self.assertEqual(c(7), 45.0)
        self.assertEqual(c(8), 60.0, "last escalation step")
        # Past the schedule the caller gives up; the function just clamps.
        self.assertEqual(c(50), 60.0)

    def test_max_streak_is_two_grace_plus_schedule(self):
        self.assertEqual(self.lt._TRAIL_MAX_DEATH_STREAK,
                         2 + len(self.lt._TRAIL_DEATH_COOLDOWNS))

    def test_streak_escalates_while_orders_die_fast(self):
        """Repeated deaths of just-placed orders escalate the cooldown and it
        stays capped — no oscillation back down to 0 just because we slowed."""
        cooldowns = []
        for _ in range(10):
            # Each cycle: order goes on the book, then dies almost immediately.
            self.lt._note_trail_placement(33)
            streak, cooldown = self.lt._record_trail_death(33)
            cooldowns.append(cooldown)
        self.assertEqual(cooldowns[0], 0.0)             # first death: transient
        self.assertEqual(cooldowns[-1], 60.0)           # persistent storm: capped
        self.assertTrue(all(b >= a for a, b in zip(cooldowns, cooldowns[1:])),
                        f"cooldown must be monotonic non-decreasing: {cooldowns}")

    def test_streak_resets_when_order_survives(self):
        """An order that lives past the survival threshold before dying starts
        a fresh streak (cooldown back to 0), even after a prior storm."""
        for _ in range(6):  # build up a streak
            self.lt._note_trail_placement(33)
            self.lt._record_trail_death(33)
        self.assertGreater(self.lt._trail_death_streak[33], 2)

        # Now an order that was placed well in the past (survived) finally dies.
        self.lt._trail_last_place[33] = time.monotonic() - (
            self.lt._TRAIL_SURVIVAL_RESET + 5)
        streak, cooldown = self.lt._record_trail_death(33)
        self.assertEqual(streak, 1, "survival must restart the streak")
        self.assertEqual(cooldown, 0.0)

    def test_clear_forgets_row(self):
        self.lt._note_trail_placement(33)
        self.lt._record_trail_death(33)
        self.lt._clear_trail_deaths(33)
        self.assertNotIn(33, self.lt._trail_death_streak)
        self.assertNotIn(33, self.lt._trail_last_place)


class PlacedOrderSeedTest(unittest.TestCase):
    """Restart safety: _seed_placed_orders rebuilds the set of ids we placed
    from recent liveevents — including rotated-out trail ids that never land in
    a liveorder row, which are exactly the ones that orphan."""

    def setUp(self):
        fd, self._tmp_db = tempfile.mkstemp(suffix='.sqlite')
        os.close(fd)
        self._orig_sqlfile = live.lutil._sqlfile
        live.lutil._sqlfile = self._tmp_db
        conn = sqlite3.connect(self._tmp_db)
        conn.execute("CREATE TABLE liveevent (id INTEGER PRIMARY KEY, "
                     "scriptid INTEGER, eventtype TEXT, eventdata TEXT, time INTEGER)")
        conn.commit()
        conn.close()
        lt = live.LiveTrader.__new__(live.LiveTrader)
        lt.scriptid = 4
        lt._placed_order_ids = set()
        lt._livelog = lambda m: None
        self.lt = lt

    def tearDown(self):
        live.lutil._sqlfile = self._orig_sqlfile
        try:
            os.unlink(self._tmp_db)
        except OSError:
            pass

    def _event(self, cb_id, t, scriptid=4):
        conn = sqlite3.connect(self._tmp_db)
        conn.execute("INSERT INTO liveevent (scriptid, eventtype, eventdata, time) "
                     "VALUES (?,?,?,?)",
                     (scriptid, 'trail:Buy',
                      '{"coinbase_order_id": "%s"}' % cb_id, t))
        conn.commit()
        conn.close()

    def test_seeds_recent_ids_skips_old_and_other_scripts(self):
        now = int(time.time())
        self._event('recent-1', now - 100)
        self._event('recent-2', now - 3600)
        self._event('ancient-1', now - 30 * 24 * 3600)        # older than lookback
        self._event('other-script', now - 100, scriptid=99)   # different script
        self.lt._seed_placed_orders()
        self.assertIn('recent-1', self.lt._placed_order_ids)
        self.assertIn('recent-2', self.lt._placed_order_ids)
        self.assertNotIn('ancient-1', self.lt._placed_order_ids)
        self.assertNotIn('other-script', self.lt._placed_order_ids)


class FakeCB:
    """Minimal Coinbase client stand-in for the orphan sweep: records every
    cancel_orders call and returns success by default."""

    def __init__(self, succeed=True):
        self.cancelled = []
        self._succeed = succeed

    def cancel_orders(self, ids):
        self.cancelled.extend(ids)
        return {'results': [{'success': self._succeed,
                             'failure_reason': '' if self._succeed else 'UNKNOWN'}
                            for _ in ids]}


class OrphanSweepTest(unittest.TestCase):
    """The #3 fix: cancel resting orders WE placed that no open row references,
    while never touching foreign/manual orders or in-flight rotations."""

    def setUp(self):
        lt = live.LiveTrader.__new__(live.LiveTrader)
        lt._placed_order_ids = set()
        lt._trail_canceling_ids = set()
        lt._trail_recent_cancels = {}
        lt.pair = 'BIP-20DEC30-CDE'
        self.events = []
        self.logs = []
        lt._livelog = lambda m: self.logs.append(m)
        lt._log_event = lambda et, data, notify=True: self.events.append(
            (et, data, notify))
        self.lt = lt

    def _order(self, oid, side='BUY'):
        return {'order_id': oid, 'side': side, 'product_id': 'BIP-20DEC30-CDE'}

    def _swept(self):
        return [e for e in self.events if e[0] == 'orphan-cancel']

    def test_cancels_our_unreferenced_resting_order(self):
        self.lt._placed_order_ids = {'orphan-1'}
        cb = FakeCB()
        self.lt._sweep_orphan_orders(cb, [self._order('orphan-1')], local_by_id={})
        self.assertEqual(cb.cancelled, ['orphan-1'])
        self.assertEqual(len(self._swept()), 1)
        self.assertTrue(self._swept()[0][1]['success'])
        self.assertNotIn('orphan-1', self.lt._placed_order_ids,
                         "a confirmed-cancelled orphan stops being tracked")

    def test_never_touches_foreign_order(self):
        # Not in _placed_order_ids → a manual or another script's order.
        cb = FakeCB()
        self.lt._sweep_orphan_orders(cb, [self._order('foreign-1')], local_by_id={})
        self.assertEqual(cb.cancelled, [], "must never cancel orders we didn't place")
        self.assertEqual(self._swept(), [])

    def test_never_touches_referenced_order(self):
        self.lt._placed_order_ids = {'live-1'}
        cb = FakeCB()
        # live-1 is the current order of an open row → keep it.
        self.lt._sweep_orphan_orders(cb, [self._order('live-1')],
                                     local_by_id={'live-1': {'id': 1}})
        self.assertEqual(cb.cancelled, [])

    def test_respects_trail_cancel_grace(self):
        self.lt._placed_order_ids = {'rotating-1'}
        # Mark it as a trail-cancel in flight (rotation just happened).
        self.lt._trail_recent_cancels['rotating-1'] = time.monotonic()
        cb = FakeCB()
        self.lt._sweep_orphan_orders(cb, [self._order('rotating-1')], local_by_id={})
        self.assertEqual(cb.cancelled, [],
                         "must not fight an in-flight trail rotation/cancel")

    def test_failed_cancel_keeps_tracking_for_retry(self):
        self.lt._placed_order_ids = {'stuck-1'}
        cb = FakeCB(succeed=False)
        self.lt._sweep_orphan_orders(cb, [self._order('stuck-1')], local_by_id={})
        self.assertEqual(cb.cancelled, ['stuck-1'])
        self.assertFalse(self._swept()[0][1]['success'])
        self.assertIn('stuck-1', self.lt._placed_order_ids,
                      "a cancel that didn't confirm stays tracked so it re-sweeps")


if __name__ == '__main__':
    unittest.main()
