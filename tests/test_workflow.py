import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "workflow.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _base(self):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {
                "name": "Glucose",
                "unit": "mmol/L",
                "allowed_low": 3.9,
                "allowed_high": 6.1,
                "rule_config": {"limit_sd": 3, "trend_n": 4, "consecutive_n": 4},
            },
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
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        return assay, lot, instrument

    def test_qc_accept_and_result_release(self):
        assay, lot, instrument = self._base()
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": 5.02,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})
        self.assertEqual(run["status"], "accepted")
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 12,
            },
        )
        batch = self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})
        self.assertEqual(batch["status"], "released")

    def test_lot_switch_links_previous_batch(self):
        assay, first, _ = self._base()
        second = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-2", "target": 5.1, "sd": 0.1, "expires_at": "2099-06-01"},
        )
        second = self.service.transition(
            self.supervisor,
            second["id"],
            "switch_in",
            {"previous_lot_id": first["id"], "switched_at": "2026-09-27T09:00:00Z"},
        )
        self.assertEqual(second["status"], "active")
        self.assertEqual(second["data"]["replaces_lot_id"], first["id"])


if __name__ == "__main__":
    unittest.main()
