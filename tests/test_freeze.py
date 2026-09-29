import tempfile
import threading
import unittest
from pathlib import Path
from uuid import uuid4

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FreezeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "freeze.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.operator = Actor("qc-operator", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _base(self, serial="Analyzer A", due="2099-01-01"):
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
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": serial, "serial": serial, "calibration_due": due},
        )
        return assay, lot, instrument

    def _accepted_run(self, assay, lot, instrument, value=5.0, run_at="2026-09-27T08:00:00Z"):
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": run_at,
            },
        )
        return self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})

    def _waiting_batch(self, assay, instrument, run, run_at="2026-09-27T08:05:00Z", count=5):
        return self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": run_at,
                "patient_count": count,
            },
        )

    def _open_freeze(self, instrument_id):
        orders = [
            order
            for order in self.service.list("freeze_order", status="open")
            if order["data"].get("instrument_id") == instrument_id
        ]
        return orders[0] if orders else None

    def test_calibration_change_voids_runs_and_intercepts_batches(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        released = self._waiting_batch(assay, instrument, run, run_at="2026-09-27T09:05:00Z")
        released = self.service.transition(self.supervisor, released["id"], "release", {"reviewer_id": "qc-2"})

        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2026-10-15", "certificate_id": "CERT-1"},
        )

        self.assertEqual(self.service.get(run["id"])["status"], "voided")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        self.assertEqual(self.service.get(released["id"])["status"], "released")

    def test_instrument_fail_triggers_freeze(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)

        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "pump failure"})

        self.assertEqual(self.service.get(run["id"])["status"], "voided")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        freeze = self._open_freeze(instrument["id"])
        self.assertIsNotNone(freeze)
        self.assertEqual(freeze["data"]["reason"], "instrument_failure")

    def test_same_reason_repeated_report_keeps_one_freeze_order(self):
        assay, lot, instrument = self._base()
        self._accepted_run(assay, lot, instrument)
        first = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
        second = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
        self.assertEqual(first["id"], second["id"])
        orders = [
            order
            for order in self.service.list("freeze_order")
            if order["data"].get("instrument_id") == instrument["id"]
            and order["data"].get("reason") == "calibration_changed"
        ]
        self.assertEqual(len(orders), 1)

    def test_release_while_frozen_returns_blocking_items(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")

        with self.assertRaises(ConflictError) as context:
            self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        details = context.exception.details
        self.assertIn("freeze_order_id", details)
        self.assertEqual(details["freeze_order_id"], self._open_freeze(instrument["id"])["id"])
        self.assertIn("items", details)
        self.assertTrue(details["items"])

    def test_release_after_intercept_lists_freeze_items(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2026-10-15", "certificate_id": "CERT-1"},
        )
        batch = self.service.get(batch["id"])
        self.assertEqual(batch["status"], "intercepted")

        with self.assertRaises(ConflictError) as context:
            self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        self.assertIn("freeze_order_id", context.exception.details)

    def test_failed_item_is_kept_and_retried(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        freeze = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")

        # Simulate a past failure on the batch item: it stays pending/failed and is retried.
        for item in freeze["data"]["items"]:
            if item["kind"] == "result_batch":
                item["status"] = "failed"
                item["error"] = "simulated conflict"
        self.service.repository.update_entity(
            freeze["id"], freeze["version"], freeze["status"], freeze["data"]
        )
        retried = self.service.apply_freeze(self.supervisor, freeze["id"])
        batch_item = [item for item in retried["data"]["items"] if item["kind"] == "result_batch"][0]
        self.assertEqual(batch_item["status"], "done")
        self.assertIsNone(batch_item["error"])

    def test_interruption_resumes_only_unfinished_items(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        freeze = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")

        # Crash after the run was voided but before the batch was intercepted.
        self.service.repository.update_entity(
            batch["id"], self.service.get(batch["id"])["version"], "waiting", dict(batch["data"])
        )
        for item in freeze["data"]["items"]:
            if item["kind"] == "result_batch":
                item["status"] = "pending"
                item["error"] = None
        self.service.repository.update_entity(
            freeze["id"], freeze["version"], freeze["status"], freeze["data"]
        )

        run_audit_before = len(self.service.audit_log(run["id"]))
        batch_audit_before = len(self.service.audit_log(batch["id"]))
        freeze = self.service.apply_freeze(self.supervisor, freeze["id"])
        run_audit_after = len(self.service.audit_log(run["id"]))
        batch_audit_after = len(self.service.audit_log(batch["id"]))

        self.assertEqual(run_audit_after - run_audit_before, 0)
        self.assertEqual(batch_audit_after - batch_audit_before, 1)
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        self.assertTrue(all(item["status"] == "done" for item in freeze["data"]["items"]))

    def test_recover_reevaluates_held_batches_and_keeps_released(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        released = self._waiting_batch(assay, instrument, run, run_at="2026-09-27T09:05:00Z")
        released = self.service.transition(self.supervisor, released["id"], "release", {"reviewer_id": "qc-2"})
        freeze = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")

        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        recovered = self.service.recover_freeze(self.supervisor, freeze["id"], note="recalibrated")
        self.assertEqual(recovered["status"], "recovered")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")
        self.assertEqual(self.service.get(released["id"])["status"], "released")
        # The old run stays voided; the batch cannot be released with it.
        with self.assertRaises(ConflictError):
            self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})

    def test_instrument_restore_auto_recovers_open_freezes(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        self.service.transition(self.supervisor, instrument["id"], "fail", {"reason": "pump failure"})
        freeze = self._open_freeze(instrument["id"])
        self.assertEqual(freeze["status"], "open")

        self.service.transition(self.supervisor, instrument["id"], "restore", {})
        freeze = self.service.get(freeze["id"])
        self.assertEqual(freeze["status"], "recovered")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")

    def test_new_batch_created_during_freeze_is_caught_up_on_apply(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        freeze = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
        self.assertEqual(len(freeze["data"]["items"]), 1)  # only the run

        # A new waiting batch arrives while the freeze is open.
        batch = self._waiting_batch(assay, instrument, run, run_at="2026-09-27T08:10:00Z")
        freeze = self.service.apply_freeze(self.supervisor, freeze["id"])
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        batch_items = [item for item in freeze["data"]["items"] if item["kind"] == "result_batch"]
        self.assertEqual(len(batch_items), 1)
        self.assertEqual(batch_items[0]["status"], "done")

    def test_concurrent_freeze_and_release_are_consistent(self):
        assay, lot, instrument = self._base()
        run = self._accepted_run(assay, lot, instrument)
        batch = self._waiting_batch(assay, instrument, run)
        errors = []

        def freeze():
            try:
                self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
            except Exception as exc:  # noqa: BLE001
                errors.append(("freeze", type(exc).__name__))

        def release():
            try:
                self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
            except Exception as exc:  # noqa: BLE001
                errors.append(("release", type(exc).__name__))

        t1 = threading.Thread(target=freeze)
        t2 = threading.Thread(target=release)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        batch_status = self.service.get(batch["id"])["status"]
        run_status = self.service.get(run["id"])["status"]
        # Consistent end-state: either the freeze won (run voided, batch intercepted,
        # release got 409) or the release won (batch released before the freeze check).
        # A released batch may still reference a run the freeze later voided; that is
        # fine because released batches stay as-is.
        if batch_status == "released":
            self.assertIn(run_status, ("accepted", "voided"))
        else:
            self.assertEqual(batch_status, "intercepted")
            self.assertEqual(run_status, "voided")
            self.assertTrue(any(kind == "release" for kind, _ in errors))

    def test_recover_requires_open_freeze(self):
        assay, lot, instrument = self._base()
        self._accepted_run(assay, lot, instrument)
        freeze = self.service.report_freeze(self.supervisor, instrument["id"], "calibration_changed")
        self.service.recover_freeze(self.supervisor, freeze["id"])
        with self.assertRaises(ConflictError):
            self.service.recover_freeze(self.supervisor, freeze["id"])


if __name__ == "__main__":
    unittest.main()
