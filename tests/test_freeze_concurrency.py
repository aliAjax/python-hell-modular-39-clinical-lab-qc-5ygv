import tempfile
import threading
import time
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FreezeConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "concurrent.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.operator = Actor("bench-op", "operator")
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "qc-1"})
        self.instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        run = self.service.create(
            self.operator,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": self.instrument["id"],
                "value": 5.01,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        self.run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})
        self.batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": self.instrument["id"],
                "qc_run_id": self.run["id"],
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 8,
            },
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_freeze_then_release_later_party_sees_frozen_state_with_blockers(self):
        """Freeze is first; a release arriving after the interception answers 409 frozen."""
        self.service.report_freeze(
            self.operator,
            self.instrument["id"],
            "suspected drift",
            cause="manual_report",
            cause_key="INC-1",
        )
        self.assertEqual(self.service.get(self.run["id"])["status"], "invalidated")
        self.assertEqual(self.service.get(self.batch["id"])["status"], "intercepted")
        with self.assertRaises(ConflictError) as raised:
            self.service.transition(
                self.supervisor, self.batch["id"], "release", {"reviewer_id": "qc-2"}
            )
        details = raised.exception.details
        self.assertEqual(details["state"], "frozen")
        kinds = {item["kind"] for item in details["blockers"]}
        self.assertIn("freeze_order", kinds)
        self.assertIn("result_batch", kinds)

    def test_freeze_and_release_overlap_is_serialized_by_lock(self):
        """Both requests run at the same time; exactly one ordering occurs, both are safe."""
        results = {}
        release_waiting = threading.Event()
        freeze_past_run = threading.Event()
        release_released = threading.Event()

        def release():
            release_waiting.set()
            try:
                results["release"] = self.service.transition(
                    self.supervisor, self.batch["id"], "release", {"reviewer_id": "qc-2"}
                )
                release_released.set()
            except BaseException as exc:
                results["release_error"] = exc

        release_thread = threading.Thread(target=release)

        def hook(stage, order, item):
            if stage == "before_item_lock" and item["kind"] == "result_batch":
                # The QC run is already invalidated and committed; now race the batch item
                # against a release that has been waiting for the instrument lock.
                release_waiting.clear()
                release_thread.start()
                release_waiting.wait(2)
                time.sleep(0.1)

        self.service.on_freeze_event = hook
        freeze_result = self.service.report_freeze(
            self.operator, self.instrument["id"], "race report", cause="manual_report", cause_key="INC-9"
        )
        self.service.on_freeze_event = None
        release_thread.join(timeout=5)

        batch = self.service.get(self.batch["id"])
        order_id = freeze_result["order"]["id"]
        if "release" in results:
            # Release won: frozen QC can no longer back a release, but the batch left the
            # department first, so the freeze skips it and keeps it untouched.
            self.assertEqual(batch["status"], "released")
            self.assertEqual(results["release"]["status"], "released")
        else:
            # Freeze won: the later release is rejected against the frozen state.
            self.assertEqual(batch["status"], "intercepted")
            exc = results["release_error"]
            self.assertIsInstance(exc, ConflictError)
            self.assertEqual(exc.details["state"], "frozen")
            self.assertTrue(
                any(item["kind"] == "freeze_order" and item["id"] == order_id for item in exc.details["blockers"])
            )

    def test_release_then_freeze_later_freeze_preserves_released_batch(self):
        """Release is first; the freeze item landing later skips the released batch."""
        released = self.service.transition(
            self.supervisor, self.batch["id"], "release", {"reviewer_id": "qc-2"}
        )
        self.assertEqual(released["status"], "released")
        result = self.service.report_freeze(
            self.operator, self.instrument["id"], "late report", cause="manual_report", cause_key="INC-2"
        )
        self.assertEqual(self.service.get(self.batch["id"])["status"], "released")
        batch_item = [
            item
            for item in result["order"]["data"]["items"]
            if item["id"] == self.batch["id"]
        ][0]
        self.assertEqual(batch_item["state"], "skipped")
        self.assertIn("released before freeze", batch_item["note"])

    def test_guarded_update_rejects_status_change_when_guard_stale(self):
        # Cross-process safety net: a state guard that no longer matches blocks the write.
        updated = self.service.repository.update_entity_guarded(
            self.batch["id"],
            self.batch["version"],
            "intercepted",
            self.batch["data"],
            status_guard=("released",),
        )
        self.assertIsNone(updated)
        self.assertEqual(self.service.get(self.batch["id"])["status"], "waiting")

    def test_atomic_release_and_intercept_cannot_both_win(self):
        """Exercise the conditional writes directly: one must lose the race."""
        data = dict(self.batch["data"], held_by_freeze="order-x")

        def noop_validate(connection, batch):
            return dict(batch["data"], released_by="qc-2")

        claimed = self.service.repository.claim_and_intercept_batch(self.batch["id"], data)
        self.assertEqual(claimed["status"], "intercepted")
        # The atomic release now must refuse: the row no longer matches status='waiting'.
        with self.assertRaises(ConflictError) as raised:
            self.service.repository.release_batch_atomically(
                self.batch["id"], claimed["version"], noop_validate
            )
        self.assertEqual(raised.exception.details["state"], "frozen")


if __name__ == "__main__":
    unittest.main()
