import json
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from pathlib import Path

from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def post(url, body, headers=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


class FreezeHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "http.db"),
            RuleEngine(),
        )
        self.server = create_server("127.0.0.1", 0, service, RuleEngine(), str(Path("static")))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.supervisor = {"X-User-Id": "sup", "X-Role": "supervisor"}
        self.operator = {"X-User-Id": "op", "X-Role": "operator"}
        self.base = "http://127.0.0.1:%s" % self.port

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _create_ready_batch(self):
        _, assay = post(self.base + "/api/assays", {
            "name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1
        }, self.supervisor)
        _, lot = post(self.base + "/api/qc_lots", {
            "assay_id": assay["id"], "lot_no": "L-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"
        }, self.supervisor)
        post(self.base + "/api/entities/%s/actions" % lot["id"], {"action": "activate", "data": {"activated_by": "q"}}, self.supervisor)
        _, instrument = post(self.base + "/api/instruments", {
            "name": "A", "serial": "S-1", "calibration_due": "2099-01-01"
        }, self.supervisor)
        _, run = post(self.base + "/api/qc_runs", {
            "assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": instrument["id"],
            "value": 5.01, "run_at": "2026-09-27T08:00:00Z"
        }, self.operator)
        post(self.base + "/api/entities/%s/actions" % run["id"], {"action": "evaluate", "data": {"evaluated_by": "q"}}, self.supervisor)
        _, batch = post(self.base + "/api/result_batches", {
            "assay_id": assay["id"], "instrument_id": instrument["id"], "qc_run_id": run["id"],
            "run_at": "2026-09-27T08:05:00Z", "patient_count": 4
        }, self.operator)
        return assay, lot, instrument, run, batch

    def test_freeze_report_resume_recover_over_http(self):
        assay, lot, instrument, run, batch = self._create_ready_batch()
        status, body = post(self.base + "/api/instruments/freeze", {
            "instrument_id": instrument["id"], "reason": "suspected drift", "cause": "manual_report", "cause_key": "INC-1"
        }, self.operator)
        self.assertEqual(status, 200)
        order = body["order"]
        self.assertEqual(body["state"], "frozen")
        self.assertEqual(body["unfinished"], [])
        self.assertEqual(body["completed"], [run["id"], batch["id"]])

        # Repeated report for the same cause returns the same single sheet.
        status, duplicate = post(self.base + "/api/instruments/freeze", {
            "instrument_id": instrument["id"], "reason": "again", "cause": "manual_report", "cause_key": "INC-1"
        }, self.operator)
        self.assertEqual(status, 200)
        self.assertTrue(duplicate["duplicate_report"])
        self.assertEqual(duplicate["order"]["id"], order["id"])

        # Release is rejected with frozen state and blockers.
        status, conflict = post(self.base + "/api/entities/%s/actions" % batch["id"], {
            "action": "release", "data": {"reviewer_id": "r"}
        }, self.supervisor)
        self.assertEqual(status, 409)
        self.assertEqual(conflict["details"]["state"], "frozen")
        kinds = {item["kind"] for item in conflict["details"]["blockers"]}
        self.assertIn("freeze_order", kinds)

        # Resume endpoint is idempotent.
        status, resumed = post(self.base + "/api/freeze_orders/%s/resume" % order["id"], {}, self.operator)
        self.assertEqual(status, 200)
        self.assertEqual(resumed["state"], "frozen")

        # Restore calibration and submit fresh QC, then recover.
        post(self.base + "/api/entities/%s/actions" % instrument["id"], {
            "action": "calibrate",
            "data": {"calibration_due": "2099-12-31", "certificate_id": "C-9"},
        }, self.supervisor)
        _, fresh = post(self.base + "/api/qc_runs", {
            "assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": instrument["id"],
            "value": 5.0, "run_at": "2099-03-01T08:00:00Z"
        }, self.operator)
        post(self.base + "/api/entities/%s/actions" % fresh["id"], {"action": "evaluate", "data": {"evaluated_by": "q"}}, self.supervisor)
        status, recovery = post(self.base + "/api/freeze_orders/%s/recover" % order["id"], {
            "fresh_qc_run_id": fresh["id"]
        }, self.supervisor)
        self.assertEqual(status, 200)
        self.assertEqual(recovery["state"], "recovered")
        self.assertEqual([item["id"] for item in recovery["requeued"]], [batch["id"]])

        status, updated = get(self.base + "/api/entities/%s" % batch["id"])
        self.assertEqual(updated["status"], "waiting")
        self.assertEqual(updated["data"]["qc_run_id"], fresh["id"])

        # And release now succeeds.
        status, released = post(self.base + "/api/entities/%s/actions" % batch["id"], {
            "action": "release", "data": {"reviewer_id": "r"}
        }, self.supervisor)
        self.assertEqual(status, 200)
        self.assertEqual(released["status"], "released")

    def test_freeze_requires_reason_and_known_instrument(self):
        status, body = post(self.base + "/api/instruments/freeze", {}, self.operator)
        self.assertEqual(status, 400)
        status, body = post(self.base + "/api/instruments/freeze", {
            "instrument_id": "does-not-exist", "reason": "x"
        }, self.operator)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
