import sqlite3
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, InvalidTransition, NotFoundError, PermissionDenied, ValidationError
from .repository import utcnow
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        if kind == "freeze_order":
            self.rules.validate_create(actor, kind, payload, self._lookup)
            order = self._create_freeze_order(actor, payload)
            if idempotency_key:
                self.repository.save_idempotency(actor.user_id, idempotency_key, order["id"])
            return order
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        if entity["kind"] == "freeze_order":
            if action == "apply":
                self.rules.validate_transition(actor, entity, "apply", dict(data or {}), self._lookup)
                return self.apply_freeze(actor, entity_id)
            if action == "recover":
                self.rules.validate_transition(actor, entity, "recover", dict(data or {}), self._lookup)
                return self.recover_freeze(actor, entity_id, (data or {}).get("note"))
            raise InvalidTransition("unknown action %s for freeze_order" % action)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if entity["kind"] == "instrument":
            self._instrument_hooks(actor, entity, updated, action)
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 隔离与恢复：冻结单（freeze order）持久化 saga
    # ------------------------------------------------------------------

    def report_freeze(self, actor, instrument_id, reason, trigger="report", calibration_due=None):
        """上报一次冻结；同一仪器同一原因只保留一张未完成的冻结单。"""
        return self._create_freeze_order(
            actor,
            {
                "instrument_id": instrument_id,
                "reason": reason or "manual",
                "trigger": trigger,
                "calibration_due": calibration_due,
            },
        )

    def _create_freeze_order(self, actor, payload):
        instrument_id = payload.get("instrument_id")
        reason = str(payload.get("reason") or "manual")
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + str(instrument_id))
        order = None
        for existing in self.repository.list_entities(kind="freeze_order", status="open"):
            if existing["data"].get("instrument_id") == instrument_id and existing["data"].get("reason") == reason:
                order = existing
                break
        if order is None:
            data = {
                "instrument_id": instrument_id,
                "reason": reason,
                "trigger": payload.get("trigger") or "report",
                "calibration_due": payload.get("calibration_due") or instrument["data"].get("calibration_due"),
                "items": [],
                "recovered_at": None,
                "recovered_by": None,
                "recovery_note": None,
            }
            try:
                order = self.repository.create_entity(
                    str(uuid4()), "freeze_order", "open", data, actor.user_id
                )
            except sqlite3.IntegrityError:
                for existing in self.repository.list_entities(kind="freeze_order", status="open"):
                    if existing["data"].get("instrument_id") == instrument_id and existing["data"].get("reason") == reason:
                        order = existing
                        break
                if order is None:
                    raise
            self.audit.record(
                order["id"],
                actor,
                "freeze_report",
                None,
                "open",
                {"instrument_id": instrument_id, "reason": reason},
            )
        return self.apply_freeze(actor, order["id"])

    def apply_freeze(self, actor, freeze_order_id):
        """处理冻结单中所有未完成项；已完成项不再重复处理，失败项保留可重试。"""
        order = self.repository.get_entity(freeze_order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        if order["status"] != "open":
            raise ConflictError(
                "freeze order is not open",
                {"freeze_order_id": order["id"], "status": order["status"]},
            )
        self._refresh_freeze_items(order)
        for item in order["data"]["items"]:
            if item["status"] == "done":
                continue
            item["status"] = self._apply_freeze_item(actor, order, item)
        return self._save_freeze_order(order)

    def _apply_freeze_item(self, actor, order, item):
        kind, item_id = item["kind"], item["id"]
        try:
            if kind == "qc_run":
                run = self.repository.get_entity(item_id)
                if not run:
                    item["error"] = "qc run not found"
                    return "failed"
                if run["status"] == "voided":
                    item["error"] = None
                    return "done"
                self.transition(actor, item_id, "void", {"freeze_order_id": order["id"]})
                item["error"] = None
                return "done"
            if kind == "result_batch":
                batch = self.repository.get_entity(item_id)
                if not batch:
                    item["error"] = "result batch not found"
                    return "failed"
                if batch["status"] == "released":
                    item["error"] = "batch already released; released batches stay as-is"
                    return "failed"
                if batch["status"] in ("intercepted", "investigating", "resolved"):
                    item["error"] = None
                    return "done"
                self.transition(
                    actor,
                    item_id,
                    "intercept",
                    {"reason": order["data"].get("reason"), "freeze_order_id": order["id"]},
                )
                item["error"] = None
                return "done"
            item["error"] = "unknown item kind: " + str(kind)
            return "failed"
        except (ConflictError, InvalidTransition, ValidationError, NotFoundError) as exc:
            item["error"] = str(exc)
            return "failed"

    def _refresh_freeze_items(self, order):
        """把冻结范围内尚未出科的结果批次和未作废的质控结果补进冻结单。"""
        instrument_id = order["data"]["instrument_id"]
        have = {(item["kind"], item["id"]) for item in order["data"]["items"]}
        for run in self.repository.find_entities("qc_run", "instrument_id", instrument_id):
            if run["status"] == "voided":
                continue
            if ("qc_run", run["id"]) not in have:
                order["data"]["items"].append(
                    {"kind": "qc_run", "id": run["id"], "status": "pending", "error": None}
                )
                have.add(("qc_run", run["id"]))
        for batch in self.repository.find_entities("result_batch", "instrument_id", instrument_id):
            if batch["status"] == "released":
                continue
            if ("result_batch", batch["id"]) not in have:
                order["data"]["items"].append(
                    {"kind": "result_batch", "id": batch["id"], "status": "pending", "error": None}
                )
                have.add(("result_batch", batch["id"]))

    def _save_freeze_order(self, order):
        try:
            return self.repository.update_entity(
                order["id"], order["version"], order["status"], order["data"]
            )
        except ConflictError:
            return self.repository.get_entity(order["id"])

    def recover_freeze(self, actor, freeze_order_id, note=None):
        """校准恢复后，受影响批次重新评估；已出科批次保留原状。"""
        order = self.repository.get_entity(freeze_order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        if order["status"] != "open":
            raise ConflictError(
                "freeze order is not open",
                {"freeze_order_id": order["id"], "status": order["status"]},
            )
        for item in order["data"]["items"]:
            if item["kind"] != "result_batch":
                continue
            batch = self.repository.get_entity(item["id"])
            if not batch or batch["status"] == "released":
                continue
            if batch["status"] in ("intercepted", "investigating", "resolved"):
                try:
                    self.transition(actor, item["id"], "reevaluate", {"freeze_order_id": order["id"]})
                except (ConflictError, InvalidTransition) as exc:
                    item["error"] = str(exc)
        order["status"] = "recovered"
        order["data"]["recovered_at"] = utcnow()
        order["data"]["recovered_by"] = actor.user_id
        order["data"]["recovery_note"] = note
        order = self._save_freeze_order(order)
        self.audit.record(
            order["id"], actor, "recover", "open", "recovered", {"note": note}
        )
        return order

    def _recover_open_freezes(self, actor, instrument_id):
        for order in self.repository.list_entities(kind="freeze_order", status="open"):
            if order["data"].get("instrument_id") != instrument_id:
                continue
            try:
                self.recover_freeze(actor, order["id"], note="instrument restored")
            except (ConflictError, InvalidTransition, PermissionDenied):
                continue

    def _instrument_hooks(self, actor, before, updated, action):
        if action == "fail":
            self.report_freeze(actor, before["id"], "instrument_failure", trigger="fail")
        elif action == "calibrate":
            if str(updated["data"].get("calibration_due")) != str(before["data"].get("calibration_due")):
                self.report_freeze(
                    actor,
                    before["id"],
                    "calibration_changed",
                    trigger="calibrate",
                    calibration_due=updated["data"].get("calibration_due"),
                )
        elif action == "restore":
            self._recover_open_freezes(actor, before["id"])
