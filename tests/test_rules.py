import unittest

from src.rules import calibration_is_valid, evaluate_qc


class RulesTest(unittest.TestCase):
    def test_single_point_rule_rejects_outlier(self):
        result = evaluate_qc([], 5.4, 5.0, 0.1, {"limit_sd": 3})
        self.assertFalse(result["accepted"])
        self.assertIn("1_3s", result["flags"])

    def test_consecutive_bias_and_trend(self):
        bias = evaluate_qc([5.12, 5.13, 5.14], 5.15, 5.0, 0.1, {"consecutive_n": 4, "consecutive_sd": 1})
        self.assertIn("bias_high", bias["flags"])
        trend = evaluate_qc([5.00, 5.01, 5.02], 5.03, 5.0, 0.1, {"trend_n": 4})
        self.assertIn("trend_up", trend["flags"])

    def test_calibration_date_comparison(self):
        self.assertTrue(calibration_is_valid("2026-09-30", "2026-09-27T08:00:00Z"))
        self.assertFalse(calibration_is_valid("2026-09-26", "2026-09-27T08:00:00Z"))


if __name__ == "__main__":
    unittest.main()
