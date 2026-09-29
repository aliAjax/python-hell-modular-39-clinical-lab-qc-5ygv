import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FreezeWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "freeze.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.operator = Actor("bench-op", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup(self, value=5.02, calibration_due="2099-01-01"):
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
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": calibration_due},
        )
        run = self.service.create(
            self.operator,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})
        batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 12,
            },
        )
        return assay, lot, instrument, run, batch

    def _fresh_run(self, assay, lot, instrument, value=5.01, run_at="2099-03-01T08:00:00Z"):
        fresh = self.service.create(
            self.operator,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": run_at,
            },
        )
        return self.service.transition(self.supervisor, fresh["id"], "evaluate", {"evaluated_by": "qc-9"})

    def test_instrument_failure_freezes_qc_and_holds_batch(self):
        assay, lot, instrument, run, batch = self._setup()
        updated = self.service.transition(
            self.operator,
            instrument["id"],
            "fail",
            {"reason": "probe error"},
        )
        self.assertEqual(updated["status"], "failed")
        self.assertEqual(self.service.get(run["id"])["status"], "invalidated")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        orders = self.service.list("freeze_orders")
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["data"]["cause"], "instrument_failed")
        # The held batch cannot be released while the freeze is active.
        with self.assertRaises(ConflictError) as raised:
            self.service.transition(
                self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"}
            )
        blockers = raised.exception.details["blockers"]
        self.assertEqual(raised.exception.details["state"], "frozen")
        self.assertTrue(any(item["kind"] == "freeze_order" for item in blockers))
        self.assertTrue(any(item["kind"] == "result_batch" for item in blockers))

    def test_calibration_date_change_auto_opens_freeze(self):
        assay, lot, instrument, run, batch = self._setup()
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-06-30", "certificate_id": "CERT-NEW", "reason": "recalibration"},
        )
        self.assertEqual(self.service.get(run["id"])["status"], "invalidated")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        orders = self.service.list("freeze_orders")
        self.assertEqual(orders[0]["data"]["cause"], "calibration_changed")

    def test_calibration_after_failure_does_not_open_second_freeze(self):
        assay, lot, instrument, run, batch = self._setup()
        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "fault"})
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-12-31", "certificate_id": "CERT-2"},
        )
        self.assertEqual(len(self.service.list("freeze_orders")), 1)

    def test_duplicate_report_for_same_cause_keeps_single_sheet(self):
        assay, lot, instrument, run, batch = self._setup()
        first = self.service.report_freeze(
            self.operator, instrument["id"], "suspected drift", cause="instrument_failed", cause_key="E-7"
        )
        second = self.service.report_freeze(
            self.operator, instrument["id"], "suspected drift again", cause="instrument_failed", cause_key="E-7"
        )
        self.assertEqual(first["order"]["id"], second["order"]["id"])
        self.assertTrue(second["duplicate_report"])
        self.assertEqual(len(self.service.list("freeze_orders")), 1)
        # A different cause still gets its own sheet.
        other = self.service.report_freeze(
            self.operator, instrument["id"], "different cause", cause="manual_report", cause_key="X-1"
        )
        self.assertNotEqual(other["order"]["id"], first["order"]["id"])

    def test_release_then_freeze_leaves_released_batch_untouched(self):
        assay, lot, instrument, run, batch = self._setup()
        batch = self.service.transition(
            self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"}
        )
        self.assertEqual(batch["status"], "released")
        result = self.service.report_freeze(
            self.operator, instrument["id"], "post-release incident", cause="manual_report"
        )
        self.assertEqual(self.service.get(batch["id"])["status"], "released")
        skipped = [item for item in result["skipped"] if item["id"] == batch["id"]]
        self.assertEqual(skipped[0]["note"], "released before freeze; left untouched")
        untouched_payload = [item["id"] for item in result["skipped"]]
        self.assertIn(batch["id"], untouched_payload)

    def test_failed_item_is_retained_and_resume_finishes_it(self):
        assay, lot, instrument, run, batch = self._setup()
        calls = {"count": 0}

        def fail_first_batch(stage, order, item):
            if stage == "before_item" and item["kind"] == "result_batch":
                calls["count"] += 1
                if calls["count"] == 1:
                    raise RuntimeError("disk full")

        self.service.on_freeze_event = fail_first_batch
        result = self.service.report_freeze(
            self.operator, instrument["id"], "flaky storage", cause="manual_report"
        )
        self.service.on_freeze_event = None
        self.assertEqual(self.service.get(run["id"])["status"], "invalidated")
        unfinished = result["unfinished"]
        self.assertEqual([item["id"] for item in unfinished], [batch["id"]])
        self.assertEqual(unfinished[0]["state"], "failed")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")
        retried = self.service.resume_freeze(self.operator, result["order"]["id"])
        self.assertEqual(retried["unfinished"], [])
        self.assertEqual(retried["state"], "frozen")
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        # Resume is idempotent: no duplicate audit rows for the run.
        invalidate_rows = [
            row
            for row in self.service.audit_log(run["id"])
            if row["action"] == "invalidate"
        ]
        self.assertEqual(len(invalidate_rows), 1)

    def test_interrupted_process_only_resumes_unfinished_item(self):
        assay, lot, instrument, run, batch = self._setup()
        touched = []

        def crash(stage, order, item):
            if stage == "before_item":
                touched.append((item["kind"], item["attempts"]))
                if item["kind"] == "result_batch" and item["attempts"] == 1:
                    raise SystemExit("simulated crash")

        self.service.on_freeze_event = crash
        with self.assertRaises(SystemExit):
            self.service.report_freeze(
                self.operator, instrument["id"], "power loss", cause="manual_report"
            )
        self.service.on_freeze_event = None
        # Simulate a brand new process reopening the same database.
        restarted = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "freeze.db"),
            RuleEngine(),
        )
        orders = restarted.list("freeze_orders")
        self.assertEqual(orders[0]["status"], "freezing")
        result = restarted.resume_freeze(self.operator, orders[0]["id"])
        self.assertEqual(result["state"], "frozen")
        # Run item was done before the crash and must not be reapplied.
        self.assertEqual(
            [(item["kind"], item["attempts"]) for item in result["order"]["data"]["items"]],
            [("qc_run", 1), ("result_batch", 2)],
        )
        self.assertEqual(restarted.get(batch["id"])["status"], "intercepted")

    def test_recovery_reevaluates_held_batches_with_fresh_qc(self):
        assay, lot, instrument, run, batch = self._setup()
        frozen = self.service.transition(
            self.operator, instrument["id"], "fail", {"reason": "fault"}
        )
        order_id = self.service.list("freeze_orders")[0]["id"]
        # Restore the instrument and run fresh QC after the freeze.
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-12-31", "certificate_id": "CERT-OK"},
        )
        fresh = self._fresh_run(assay, lot, instrument)
        result = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(result["state"], "recovered")
        requeued = [item["id"] for item in result["requeued"]]
        self.assertEqual(requeued, [batch["id"]])
        updated_batch = self.service.get(batch["id"])
        self.assertEqual(updated_batch["status"], "waiting")
        self.assertEqual(updated_batch["data"]["qc_run_id"], fresh["id"])
        self.assertEqual(updated_batch["data"]["previous_qc_run_id"], run["id"])
        # And it can now be released.
        released = self.service.transition(
            self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"}
        )
        self.assertEqual(released["status"], "released")

    def test_recovery_without_fresh_qc_keeps_batches_blocked(self):
        assay, lot, instrument, run, batch = self._setup()
        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "fault"})
        order_id = self.service.list("freeze_orders")[0]["id"]
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-12-31", "certificate_id": "CERT-OK"},
        )
        result = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(result["state"], "frozen")
        self.assertEqual(result["still_blocked"][0]["id"], batch["id"])
        self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        # Once fresh QC arrives, the same recovery can be retried.
        self._fresh_run(assay, lot, instrument)
        retried = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(retried["state"], "recovered")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")

    def test_released_batch_is_preserved_through_recovery(self):
        assay, lot, instrument, run, batch = self._setup()
        second_batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T09:05:00Z",
                "patient_count": 3,
            },
        )
        # Release the first batch before the freeze lands.
        self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "fault"})
        order_id = self.service.list("freeze_orders")[0]["id"]
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-12-31", "certificate_id": "CERT-OK"},
        )
        self._fresh_run(assay, lot, instrument)
        result = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(self.service.get(batch["id"])["status"], "released")
        self.assertEqual(self.service.get(second_batch["id"])["status"], "waiting")
        self.assertEqual(
            [item["id"] for item in result["untouched"]], [batch["id"]]
        )

    def test_new_batch_under_freeze_with_invalidated_qc_cannot_release(self):
        assay, lot, instrument, run, batch = self._setup()
        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "fault"})
        late_batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T10:00:00Z",
                "patient_count": 2,
            },
        )
        with self.assertRaises(ConflictError) as raised:
            self.service.transition(
                self.supervisor, late_batch["id"], "release", {"reviewer_id": "qc-2"}
            )
        self.assertEqual(raised.exception.details["state"], "frozen")
        kinds = {item["kind"] for item in raised.exception.details["blockers"]}
        self.assertIn("freeze_order", kinds)
        self.assertIn("qc_run", kinds)

    def test_freeze_report_requires_operator_or_above(self):
        assay, lot, instrument, run, batch = self._setup()
        with self.assertRaises(PermissionDenied):
            self.service.report_freeze(
                Actor("viewer", "viewer"), instrument["id"], "reason", cause="manual_report"
            )

    def test_recovery_requires_supervisor_and_ready_instrument(self):
        assay, lot, instrument, run, batch = self._setup()
        self.service.transition(self.operator, instrument["id"], "fail", {"reason": "fault"})
        order_id = self.service.list("freeze_orders")[0]["id"]
        with self.assertRaises(PermissionDenied):
            self.service.recover_freeze(self.operator, order_id)
        with self.assertRaises(ConflictError):
            self.service.recover_freeze(self.supervisor, order_id)
        self.service.transition(
            self.supervisor,
            instrument["id"],
            "calibrate",
            {"calibration_due": "2099-12-31", "certificate_id": "CERT-OK"},
        )
        self._fresh_run(assay, lot, instrument)
        recovered = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(recovered["state"], "recovered")
        # Repeating recovery is a no-op against the already recovered sheet.
        repeated = self.service.recover_freeze(self.supervisor, order_id)
        self.assertEqual(repeated["state"], "recovered")
        self.assertEqual(repeated["order"]["id"], order_id)


if __name__ == "__main__":
    unittest.main()
