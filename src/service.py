import json
import threading
from datetime import datetime
from uuid import uuid4

from .audit import AuditTrail
from .domain import (
    ConflictError,
    NotFoundError,
    ValidationError,
)
from .repository import utcnow
from .rules import RuleEngine, _validate_release

# Only patient batches that have not left the department can be held.
HOLDABLE_BATCH_STATUS = ("waiting",)
# QC results that remain authoritative for patient batches until a freeze invalidates them.
INVALIDATABLE_RUN_STATUS = ("accepted",)


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        # One lock per instrument serializes freeze / release / recovery in this process.
        # It is a plain mutex held only by public entry points (never by internal helpers),
        # so a release arriving while a freeze is between items is serialized in lock order;
        # cross-process safety additionally comes from the guarded SQLite writes.
        self._locks = {}
        self._locks_guard = threading.Lock()
        # Test hook: invoked with ("before_item", order, item) before each item is applied.
        self.on_freeze_event = None

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _instrument_lock(self, instrument_id):
        with self._locks_guard:
            lock = self._locks.get(instrument_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[instrument_id] = lock
            return lock

    def _emit(self, stage, order, item=None):
        if self.on_freeze_event:
            self.on_freeze_event(stage, order, item)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ------------------------------------------------------------------ create

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
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

    # ------------------------------------------------------------ transitions

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        probe = self.repository.get_entity(entity_id)
        if not probe:
            raise NotFoundError("entity not found: " + entity_id)
        payload = dict(data or {})
        kind = self.rules.normalize_kind(probe["kind"])
        instrument_action = kind == "instrument" and action in ("fail", "calibrate")
        releasing = kind == "result_batch" and action == "release"
        if instrument_action:
            # The instrument status change and the automatic freeze are separate lock scopes;
            # the freeze itself re-acquires the instrument lock (a plain mutex, not reentrant).
            with self._instrument_lock(entity_id):
                updated = self._do_transition(actor, entity_id, action, payload, expected_version)
            self._after_transition(actor, probe, updated, action, payload)
            return updated
        if releasing:
            # Release competes with freezes on the same instrument; the atomic repository
            # path validates and commits in one transaction, so whichever request holds the
            # write transaction decides the order — the loser answers against frozen state.
            with self._instrument_lock(probe["data"].get("instrument_id")):
                return self._do_release(actor, probe, payload, expected_version)
        return self._do_transition(actor, entity_id, action, payload, expected_version)

    def _do_release(self, actor, probe, payload, expected_version):
        self.rules.authorize_action(actor, "result_batch", "release")
        self.rules._require(payload, ("reviewer_id",))
        expected = int(expected_version) if expected_version is not None else probe["version"]
        from_status = {"value": probe["status"]}

        def validate(connection, batch):
            def lookup(kind, field, value):
                rows = connection.execute(
                    "SELECT * FROM entities WHERE kind = ? ORDER BY created_at, id", (kind,)
                ).fetchall()
                items = [
                    {
                        "id": row["id"],
                        "kind": row["kind"],
                        "status": row["status"],
                        "version": int(row["version"]),
                        "data": json.loads(row["data"]),
                    }
                    for row in rows
                ]
                if field == "id":
                    return [item for item in items if item["id"] == value]
                return [item for item in items if item["data"].get(field) == value]

            patch = _validate_release(actor, batch, payload, lookup)
            from_status["value"] = batch["status"]
            merged = dict(batch["data"])
            merged.update(patch)
            return merged

        updated = self.repository.release_batch_atomically(probe["id"], expected, validate)
        self.audit.record(
            probe["id"],
            actor,
            "release",
            from_status["value"],
            "released",
            {"patch": {"released_by": actor.user_id}},
        )
        return updated

    def _do_transition(self, actor, entity_id, action, data, expected_version):
        entity = self.repository.get_entity(entity_id)
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
        return updated

    def _after_transition(self, actor, before, after, action, patch):
        if after["kind"] != "instrument":
            return
        if action == "fail":
            # Instrument breakdown immediately invalidates its QC authority.
            order, duplicate = self._open_freeze(
                actor,
                after,
                cause="instrument_failed",
                reason=patch.get("reason") or "instrument reported out of service",
                cause_key=patch.get("reason"),
                fold_any_active=True,
            )
            if not duplicate:
                self._process_items(order)
            return
        if action == "calibrate":
            before_due = before["data"].get("calibration_due")
            before_cert = before["data"].get("certificate_id")
            after_due = after["data"].get("calibration_due")
            before_maintenance = before["status"] in ("failed", "maintenance")
            if not before_maintenance and (before_due != after_due or before_cert != patch.get("certificate_id")):
                # A calibration date/certificate change on a ready instrument voids prior QC.
                order, duplicate = self._open_freeze(
                    actor,
                    after,
                    cause="calibration_changed",
                    reason=patch.get("reason")
                    or "calibration date or certificate changed: %s -> %s" % (before_due, after_due),
                    cause_key="%s:%s" % (after_due, patch.get("certificate_id")),
                    fold_any_active=True,
                )
                if not duplicate:
                    self._process_items(order)

    # ------------------------------------------------------------ freeze flow

    def report_freeze(self, actor, instrument_id, reason, cause=None, cause_key=None,
                      idempotency_key=None):
        """Report an instrument incident: invalidate QC and hold unreleased patient batches."""
        self.rules.authorize_action(actor, "instrument", "freeze")
        if not instrument_id:
            raise ValidationError("instrument_id is required")
        if not str(reason or "").strip():
            raise ValidationError("freeze reason is required")
        instrument = self.repository.get_entity(instrument_id)
        if not instrument or instrument["kind"] != "instrument":
            raise NotFoundError("instrument not found: " + instrument_id)
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                duplicate = self.repository.get_entity(existing)
                if duplicate:
                    return self._freeze_response(duplicate, duplicate_report=True)
        with self._instrument_lock(instrument_id):
            order, duplicate_report = self._open_freeze(actor, instrument, cause, reason, cause_key)
        # Items are processed after the sheet exists; the per-item loop takes the lock itself,
        # which leaves a gap between items for a concurrently arriving release.
        order = self._process_items(order)
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, order["id"])
        return self._freeze_response(order, duplicate_report=duplicate_report)

    def _open_freeze(self, actor, instrument, cause, reason, cause_key=None, fold_any_active=False):
        """Create the single freeze sheet for a cause (caller holds the instrument lock).

        A manual report of the same cause folds onto its existing sheet; an instrument
        event (failure/calibration) folds onto *any* active sheet for the instrument rather
        than opening a second, competing freeze.
        """
        active = self._active_freeze_orders(instrument["id"])
        for existing in active:
            same_cause = existing["data"].get("cause") == cause and existing["data"].get("cause_key") == (cause_key or None)
            if same_cause or fold_any_active:
                return existing, True

        runs = [
            run
            for run in self._lookup("qc_run", "instrument_id", instrument["id"])
            if run["status"] in INVALIDATABLE_RUN_STATUS
        ]
        all_batches = self._lookup("result_batch", "instrument_id", instrument["id"])
        items = (
            [
                {
                    "kind": "qc_run",
                    "id": run["id"],
                    "assay_id": run["data"].get("assay_id"),
                    "state": "pending",
                    "attempts": 0,
                }
                for run in runs
            ]
            + [
                {
                    "kind": "result_batch",
                    "id": batch["id"],
                    "assay_id": batch["data"].get("assay_id"),
                    "state": "pending" if batch["status"] in HOLDABLE_BATCH_STATUS else "skipped",
                    "attempts": 0,
                    "note": None
                    if batch["status"] in HOLDABLE_BATCH_STATUS
                    else (
                        "released before freeze; left untouched"
                        if batch["status"] == "released"
                        else "batch status %s is not holdable" % batch["status"]
                    ),
                }
                for batch in all_batches
            ]
        )
        order_id = str(uuid4())
        now = utcnow()
        payload = {
            "instrument_id": instrument["id"],
            "cause": cause,
            "cause_key": cause_key or None,
            "reason": reason,
            "reported_by": actor.user_id,
            "frozen_at": now,
            "items": items,
            "retries": 0,
        }
        order = self.repository.create_entity(order_id, "freeze_order", "freezing", payload, actor.user_id)
        self.audit.record(
            order_id,
            actor,
            "freeze_report",
            None,
            "freezing",
            {"instrument_id": instrument["id"], "cause": cause, "item_count": len(items)},
        )
        self._touch_instrument(order, actor, instrument["id"], "freeze_order_id")
        return order, False

    def resume_freeze(self, actor, order_id):
        """Retry the unfinished items of a freeze sheet (idempotent: completed items stay done)."""
        self.rules.authorize_action(actor, "freeze_order", "resume")
        order = self.repository.get_entity(order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + order_id)
        if order["status"] == "recovered":
            return self._freeze_response(order)
        order = self._process_items(order)
        return self._freeze_response(order)

    def _process_items(self, order):
        """Apply pending/failed items one at a time, checkpoints between them.

        The instrument lock is acquired per item rather than for the whole run, so a release
        arriving between items is serialized by lock acquisition order instead of blocking
        behind a potentially long freeze. A checkpoint recording an ``attempt`` is written
        before each item, so a process killed mid-item resumes with that item still marked
        unfinished; finished items are never redone.
        """
        instrument_id = order["data"]["instrument_id"]
        items = list(order["data"].get("items") or [])
        progressed = False
        for index, item in enumerate(items):
            if item["state"] in ("done", "skipped"):
                continue
            # Emitted without the instrument lock: a test can park a competing request here.
            self._emit("before_item_lock", order, item)
            with self._instrument_lock(instrument_id):
                order = self.repository.get_entity(order["id"])
                items = list(order["data"].get("items") or [])
                item = items[index]
                if item["state"] in ("done", "skipped"):
                    continue
                item["attempts"] = int(item.get("attempts", 0)) + 1
                item["state"] = "attempting"
                order = self._save_order(order, items)
                progressed = True
                try:
                    self._emit("before_item", order, item)
                    outcome = self._apply_item(order, items[index])
                except BaseException as exc:  # failure is retained as an unfinished item
                    if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                        raise
                    items[index]["state"] = "failed"
                    items[index]["error"] = str(exc)
                else:
                    items[index]["state"] = outcome["state"]
                    if outcome.get("note"):
                        items[index]["note"] = outcome["note"]
                    items[index].pop("error", None)
                order = self._save_order(order, items)
            # Lock released here: a concurrent request now observes this item's commit.
        status = self._order_status(items)
        final = self.repository.get_entity(order["id"])
        if status != final["status"] or progressed:
            with self._instrument_lock(instrument_id):
                final = self.repository.get_entity(order["id"])
                final_items = list(final["data"].get("items") or [])
                final = self._save_order(final, final_items, status=self._order_status(final_items))
        return final

    def _apply_item(self, order, item):
        entity = self.repository.get_entity(item["id"])
        if not entity:
            return {"state": "skipped", "note": "entity no longer exists"}
        reason = "%s (%s)" % (order["data"].get("reason"), order["id"])
        if item["kind"] == "qc_run":
            if entity["status"] == "invalidated":
                return {"state": "done", "note": "already invalidated"}
            if entity["status"] not in INVALIDATABLE_RUN_STATUS:
                return {"state": "skipped", "note": "qc run status %s needs no invalidation" % entity["status"]}
            updated = self.repository.update_entity_guarded(
                entity["id"],
                entity["version"],
                "invalidated",
                dict(entity["data"], invalidated_by_freeze=order["id"], invalidated_reason=reason),
                status_guard=INVALIDATABLE_RUN_STATUS,
            )
            if updated is None:
                return {"state": "skipped", "note": "qc run changed before invalidation"}
            self.audit.record(
                entity["id"],
                _freeze_actor(order),
                "invalidate",
                entity["status"],
                "invalidated",
                {"freeze_order_id": order["id"], "reason": reason},
            )
            return {"state": "done"}
        if item["kind"] == "result_batch":
            if entity["status"] == "intercepted":
                return {"state": "done", "note": "already intercepted"}
            if entity["status"] == "released":
                # The batch left the department before the freeze landed; it stays as-is.
                return {"state": "skipped", "note": "released before freeze; left untouched"}
            if entity["status"] != "waiting":
                return {"state": "skipped", "note": "batch status %s is not holdable" % entity["status"]}
            claimed = self.repository.claim_and_intercept_batch(
                entity["id"],
                dict(entity["data"], held_by_freeze=order["id"], hold_reason=reason),
            )
            if claimed == "released":
                return {"state": "skipped", "note": "released before freeze; left untouched"}
            if claimed is None:
                return {"state": "skipped", "note": "batch changed before interception"}
            self.audit.record(
                entity["id"],
                _freeze_actor(order),
                "intercept",
                "waiting",
                "intercepted",
                {"freeze_order_id": order["id"], "reason": reason},
            )
            return {"state": "done"}
        return {"state": "skipped", "note": "unknown item kind"}

    def recover_freeze(self, actor, order_id, fresh_qc_run_id=None):
        """Re-evaluate held batches after calibration is restored.

        Held batches linked to a fresh accepted QC result taken after the freeze go back to
        the waiting queue; batches with no fresh QC remain blocked. Released batches were
        never touched and stay untouched.
        """
        self.rules.authorize_action(actor, "freeze_order", "recover")
        order = self.repository.get_entity(order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + order_id)
        if order["status"] == "recovered":
            # Recovery is one-way; a repeat call is answered as a no-op rather than rejected,
            # because the caller may simply have lost the first response.
            items = order["data"].get("items") or []
            requeued = [
                {"id": item["id"], "fresh_qc_run_id": None}
                for item in items
                if item.get("note", "").startswith("reassessed with QC")
            ]
            untouched = []
            for item in items:
                if item["kind"] != "result_batch":
                    continue
                batch = self.repository.get_entity(item["id"])
                if batch and batch["status"] == "released":
                    untouched.append({"id": batch["id"], "note": "released batch preserved"})
            return self._freeze_response(order, requeued=requeued, untouched=untouched)
        with self._instrument_lock(order["data"]["instrument_id"]):
            instrument = self.repository.get_entity(order["data"]["instrument_id"])
            if not instrument or instrument["status"] != "ready":
                raise ConflictError("instrument must be ready and calibrated before recovery")
            order = self.repository.get_entity(order_id)
            items = list(order["data"].get("items") or [])
            frozen_at = order["data"].get("frozen_at", "")
            # Batch snapshots created at freeze time, so same-moment QC is "fresh" only if it
            # has a strictly later timestamp; UTC comparison falls back to created_at.
            fresh = self._fresh_accepted_runs(order["data"]["instrument_id"], frozen_at, order["created_at"])
            if fresh_qc_run_id:
                run = self.repository.get_entity(fresh_qc_run_id)
                if not run or run["kind"] != "qc_run" or run["status"] != "accepted":
                    raise ValidationError("fresh_qc_run_id must reference an accepted QC result")
                if run["data"].get("instrument_id") != instrument["id"]:
                    raise ValidationError("fresh QC result belongs to another instrument")
                if not self._run_is_fresh(run, frozen_at, order["created_at"]):
                    raise ValidationError("fresh QC result predates the freeze")
                fresh = dict(fresh)
                fresh[run["data"]["assay_id"]] = run
            requeued, still_blocked, untouched = [], [], []
            for index, item in enumerate(items):
                if item["kind"] != "result_batch":
                    continue
                batch = self.repository.get_entity(item["id"])
                if not batch:
                    continue
                if batch["status"] == "released":
                    untouched.append({"id": batch["id"], "note": "released batch preserved"})
                    continue
                if batch["status"] == "waiting" and batch["data"].get("reassessed_from_freeze") == order["id"]:
                    # A previous recovery attempt already put this batch back in the queue.
                    requeued.append({"id": batch["id"], "fresh_qc_run_id": batch["data"].get("qc_run_id")})
                    continue
                if batch["status"] != "intercepted":
                    continue
                candidate = fresh.get(batch["data"].get("assay_id"))
                if not candidate:
                    still_blocked.append({"id": batch["id"], "reason": "no fresh accepted QC result yet"})
                    continue
                data = dict(batch["data"])
                data["previous_qc_run_id"] = data.get("qc_run_id")
                data["qc_run_id"] = candidate["id"]
                data["reassessed_from_freeze"] = order["id"]
                updated = self.repository.update_entity_guarded(
                    batch["id"],
                    batch["version"],
                    "waiting",
                    data,
                    status_guard=("intercepted",),
                )
                if updated is None:
                    still_blocked.append({"id": batch["id"], "reason": "batch changed during recovery"})
                    continue
                items[index]["state"] = "done"
                items[index]["note"] = "reassessed with QC %s" % candidate["id"]
                self.audit.record(
                    batch["id"],
                    actor,
                    "reassess",
                    "intercepted",
                    "waiting",
                    {"freeze_order_id": order["id"], "fresh_qc_run_id": candidate["id"]},
                )
                requeued.append({"id": batch["id"], "fresh_qc_run_id": candidate["id"]})
            payload = dict(order["data"])
            payload["items"] = items
            payload["recovered_by"] = actor.user_id
            payload["recovered_at"] = utcnow()
            if still_blocked:
                payload["retries"] = int(payload.get("retries", 0)) + 1
                order = self.repository.update_entity(
                    order["id"], order["version"], "frozen", payload
                )
                self.audit.record(
                    order["id"], actor, "recover_attempt", "frozen", "frozen",
                    {"requeued": requeued, "still_blocked": still_blocked},
                )
                self._touch_instrument(order, actor, instrument["id"], "last_freeze_order_id")
                return self._freeze_response(
                    order, requeued=requeued, still_blocked=still_blocked, untouched=untouched
                )
            order = self.repository.update_entity(order["id"], order["version"], "recovered", payload)
            self.audit.record(
                order["id"], actor, "recover", "frozen", "recovered",
                {"requeued": requeued, "untouched": untouched},
            )
            self._touch_instrument(order, actor, instrument["id"], "last_freeze_order_id")
        return self._freeze_response(order, requeued=requeued, still_blocked=[], untouched=untouched)

    @staticmethod
    def _parse_time(value):
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _run_is_fresh(self, run, frozen_at, frozen_created_at):
        run_time = self._parse_time(run["data"].get("run_at"))
        freeze_time = self._parse_time(frozen_at)
        if run_time and freeze_time:
            if run_time != freeze_time:
                return run_time >= freeze_time
            # Identical business timestamps: only a row created after the freeze counts.
            return run["created_at"] > frozen_created_at
        if str(run["data"].get("run_at", "")) != str(frozen_at):
            return str(run["data"].get("run_at", "")) >= str(frozen_at)
        return run["created_at"] > frozen_created_at

    def _fresh_accepted_runs(self, instrument_id, frozen_at, frozen_created_at):
        accepted = {}
        for run in self._lookup("qc_run", "instrument_id", instrument_id):
            if run["status"] != "accepted":
                continue
            if not self._run_is_fresh(run, frozen_at, frozen_created_at):
                continue
            assay_id = run["data"].get("assay_id")
            previous = accepted.get(assay_id)
            run_time = self._parse_time(run["data"].get("run_at"))
            previous_time = self._parse_time(previous["data"].get("run_at")) if previous else None
            if previous is None or (
                run_time is not None
                and (previous_time is None or run_time >= previous_time)
            ):
                accepted[assay_id] = run
        return accepted

    def _active_freeze_orders(self, instrument_id):
        return [
            order
            for order in self._lookup("freeze_order", "instrument_id", instrument_id)
            if order["status"] in ("freezing", "frozen")
        ]

    def _save_order(self, order, items, status=None):
        payload = dict(order["data"])
        payload["items"] = items
        if status is None:
            status = self._order_status(items)
        return self.repository.update_entity(order["id"], order["version"], status, payload)

    @staticmethod
    def _order_status(items):
        states = {item["state"] for item in items}
        if states & {"pending", "attempting", "failed"}:
            return "freezing" if states & {"pending", "attempting"} else "frozen"
        return "frozen"

    def _touch_instrument(self, order, actor, instrument_id, field):
        instrument = self.repository.get_entity(instrument_id)
        if not instrument:
            return
        data = dict(instrument["data"])
        data[field] = order["id"]
        data["last_freeze_order_id"] = order["id"]
        data["last_freeze_cause"] = order["data"].get("cause")
        self.repository.update_entity(instrument["id"], instrument["version"], instrument["status"], data)

    def _freeze_response(self, order, duplicate_report=None, requeued=None, still_blocked=None,
                         untouched=None):
        items = order["data"].get("items") or []
        return {
            "order": order,
            "state": order["status"],
            "duplicate_report": bool(duplicate_report),
            "unfinished": [
                {"kind": item["kind"], "id": item["id"], "state": item["state"], "error": item.get("error")}
                for item in items
                if item["state"] in ("pending", "attempting", "failed")
            ],
            "completed": [item["id"] for item in items if item["state"] == "done"],
            "skipped": [
                {"id": item["id"], "note": item.get("note")}
                for item in items
                if item["state"] == "skipped"
            ],
            "requeued": requeued or [],
            "still_blocked": still_blocked or [],
            "untouched": untouched or [],
        }

    # -------------------------------------------------------------- queries

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


class _freeze_actor:
    """Audit identity for changes the freeze workflow performs on behalf of the reporter."""

    def __init__(self, order):
        self.user_id = order["data"].get("reported_by", "freeze-workflow")
        self.role = "operator"
