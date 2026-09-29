import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "failures.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_run(self, value):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Assay", "unit": "u", "allowed_low": 0, "allowed_high": 10},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "L", "target": 5, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "a"})
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "I", "serial": "S", "calibration_due": "2099-01-01"},
        )
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        return assay, lot, instrument, run

    def test_rejected_result_batch_cannot_release(self):
        assay, lot, instrument, run = self._setup_run(9.9)
        run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "a"})
        self.assertEqual(run["status"], "rejected")
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T08:01:00Z",
                "patient_count": 1,
            },
        )
        with self.assertRaises(ConflictError):
            self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "r"})

    def test_permission_duplicate_and_version_conflict(self):
        assay, lot, instrument, run = self._setup_run(5.0)
        with self.assertRaises(PermissionDenied):
            self.service.create(
                Actor("viewer", "viewer"),
                "assay",
                {"name": "X", "unit": "u", "allowed_low": 0, "allowed_high": 1},
            )
        with self.assertRaises(ConflictError):
            self.service.create(
                self.supervisor,
                "qc_lot",
                {"assay_id": assay["id"], "lot_no": lot["data"]["lot_no"], "target": 5, "sd": 0.1, "expires_at": "2099-01-01"},
            )
        with self.assertRaises(ConflictError):
            self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "a"}, expected_version=999)

    def test_idempotent_qc_run_creation(self):
        assay, lot, instrument, _ = self._setup_run(5.0)
        payload = {
            "assay_id": assay["id"],
            "qc_lot_id": lot["id"],
            "instrument_id": instrument["id"],
            "value": 5.01,
            "run_at": "2026-09-27T08:02:00Z",
        }
        first = self.service.create(self.supervisor, "qc_run", payload, "same-qc-run")
        second = self.service.create(self.supervisor, "qc_run", payload, "same-qc-run")
        self.assertEqual(first["id"], second["id"])


if __name__ == "__main__":
    unittest.main()
