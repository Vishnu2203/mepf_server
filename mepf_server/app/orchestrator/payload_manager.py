# -*- coding: utf-8 -*-
"""
payload_manager.py
=============================================================================
Server-side payload folder  ->  deterministic routing  ->  command queue.

    payloads/inbox/*.json
        -> scan (settle check, hash, dedupe)            DISCOVERED
        -> validate (schema + pre-flight vs. handlers)  VALIDATED | INVALID
        -> resolve target (selector / alias / file rule) TARGET_SELECTED | NO_TARGET | AMBIGUOUS_TARGET
        -> create_command()  (existing, audited path)    QUEUED
        -> agent polls /next                             CLAIMED
        -> agent /start                                  EXECUTING
        -> agent /result                                 SUCCEEDED | PARTIAL | EXECUTION_FAILED | ...

Design rules
  * The DB (payloads table) is the source of truth; files are moved to done/ or
    failed/ only for operator visibility. A restart re-derives everything.
  * The router NEVER picks "any online machine". A payload must resolve to exactly
    one online document, otherwise it waits (NO_TARGET / AMBIGUOUS_TARGET).
  * A payload is re-sent automatically ONLY when it is certain that nothing was
    created in Revit (routing/target errors, never-claimed commands). Anything
    where Revit may have committed elements (timeout, lease expiry during
    execution, PARTIAL) is held for review, because a blind retry duplicates elements.
  * Every state change is a compare-and-set UPDATE, so several server workers
    (or a manual /scan racing the background loop) cannot double-dispatch.
=============================================================================
"""
import os
import re
import json
import time
import shutil
import fnmatch
import hashlib
import logging
import threading
import datetime as dt
import uuid

from fastapi import HTTPException
from sqlalchemy import text as sql_text
from pathlib import Path

from app.models.db import (
    SessionLocal, engine, PayloadRecord, PayloadEventRecord, CommandRecord,
    CommandResultRecord, DocumentRecord, SystemRecord, now,
)
from app.orchestrator.routing_engine import resolve_candidates
from app.routers.commands import CreateCommand, create_command, ALLOWED_SELECTOR_KEYS, MAX_ITEMS, _expire_leases

log = logging.getLogger("payload_manager")
APP_ROOT = Path(__file__).resolve().parents[2]

# ----------------------------------------------------------------------------
# configuration (all env-overridable)
# ----------------------------------------------------------------------------
PAYLOAD_DIR = os.environ.get(
    "MEPF_PAYLOAD_DIR",
    str(APP_ROOT / "payloads")
)
SCAN_SEC = float(os.environ.get("MEPF_PAYLOAD_SCAN_SEC", "5"))
SETTLE_SEC = float(os.environ.get("MEPF_PAYLOAD_SETTLE_SEC", "2"))          # file must be unchanged this long
PARTIAL_WRITE_GRACE_SEC = float(os.environ.get("MEPF_PAYLOAD_PARSE_GRACE_SEC", "15"))
PENDING_GRACE_SEC = float(os.environ.get("MEPF_PAYLOAD_PENDING_GRACE_SEC", "45"))   # target vanished while queued
QUEUED_TTL_SEC = float(os.environ.get("MEPF_PAYLOAD_QUEUED_TTL_SEC", "600"))        # target alive but never polled
NO_TARGET_TTL_SEC = float(os.environ.get("MEPF_PAYLOAD_NO_TARGET_TTL_SEC", str(7 * 86400)))
BACKOFF_SEC = [5, 30, 120, 600]
DEFAULT_MAX_ATTEMPTS = int(os.environ.get("MEPF_PAYLOAD_MAX_ATTEMPTS", "3"))

SUB_DIRS = ("inbox", "done", "failed", "duplicate")

# payload-level states
WAITING = {"DISCOVERED", "VALIDATED", "NO_TARGET", "AMBIGUOUS_TARGET", "RETRY", "TARGET_SELECTED"}
INFLIGHT = {"QUEUED", "CLAIMED", "EXECUTING"}
TERMINAL_OK = {"SUCCEEDED"}
TERMINAL_BAD = {"INVALID", "DUPLICATE", "CANCELLED", "EXPIRED", "PARTIAL",
                "EXECUTION_FAILED", "TIMEOUT", "DELIVERY_FAILED", "AGENT_OFFLINE"}
TERMINAL = TERMINAL_OK | TERMINAL_BAD

# what the Revit-side receiver actually understands (receiver_verified.PlacementHandler.Execute)
KNOWN_PLACEMENTS = {"point_family", "curve_mep", "fitting", "inline_fitting", "hosted_family", "connect"}
CURVE_SYSTEMS = {"duct", "flex_duct", "pipe", "flex_pipe", "cable_tray", "conduit"}
SELECTOR_NOISE_KEYS = {"note", "notes", "description", "label", "comment"}
# machine_name is stored in systems.machine_name but is not an accepted key of /api/commands/create; the payload
# manager resolves it itself (case-insensitive) and then pins the command to concrete ids.
LOCAL_SELECTOR_KEYS = {"machine_name"}

_tick_lock = threading.Lock()
_scan_sig = {}          # rel path -> (mtime, size) of the last file version we fully processed


# ----------------------------------------------------------------------------
# folders / files
# ----------------------------------------------------------------------------
def ensure_dirs():
    for d in SUB_DIRS:
        os.makedirs(os.path.join(PAYLOAD_DIR, d), exist_ok=True)


def _inbox():
    return os.path.join(PAYLOAD_DIR, "inbox")


def _safe_move(rel_name, dest_sub, sidecar=None):
    """Atomically move inbox/<rel_name> to <dest_sub>/ (never overwrites)."""
    src = os.path.join(_inbox(), rel_name)
    if not os.path.isfile(src):
        return None
    dest_dir = os.path.join(PAYLOAD_DIR, dest_sub)
    os.makedirs(dest_dir, exist_ok=True)
    base = os.path.basename(rel_name)
    dest = os.path.join(dest_dir, base)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(base)
        dest = os.path.join(dest_dir, "{}.{}{}".format(stem, dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S%f"), ext))
    os.replace(src, dest)
    if sidecar is not None:
        try:
            with open(dest + ".status.json", "w", encoding="utf-8") as f:
                json.dump(sidecar, f, indent=2, default=str)
        except Exception:
            log.exception("could not write sidecar for %s", dest)
    return dest


def _canonical_hash(obj):
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _event(db, payload_id, event, details=None):
    db.add(PayloadEventRecord(payload_id=payload_id, event=event, details=details or {}))


def _cas(db, p, expected, **fields):
    """Compare-and-set on payloads.status. Returns True if this worker won the transition."""
    fields.setdefault("updated_at", now())
    n = (db.query(PayloadRecord)
         .filter(PayloadRecord.payload_id == p.payload_id, PayloadRecord.status.in_(list(expected)))
         .update(fields, synchronize_session=False))
    db.commit()
    db.refresh(p)
    return n == 1


# ----------------------------------------------------------------------------
# routing map (alias / file rules) - the mapping information the registry lacks
# ----------------------------------------------------------------------------
_routing_cache = {"mtime": None, "data": {"aliases": {}, "file_rules": []}}


def load_routing_map(force=False):
    path = os.path.join(PAYLOAD_DIR, "routing_map.json")
    try:
        mt = os.path.getmtime(path)
    except OSError:
        _routing_cache.update(mtime=None, data={"aliases": {}, "file_rules": []})
        return _routing_cache["data"]
    if force or _routing_cache["mtime"] != mt:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data.setdefault("aliases", {})
            data.setdefault("file_rules", [])
            _routing_cache.update(mtime=mt, data=data)
        except Exception as ex:
            log.error("routing_map.json unreadable: %s", ex)
            # keep the last good map rather than routing with nothing
    return _routing_cache["data"]


def _clean_selector(sel):
    return {k: v for k, v in (sel or {}).items() if k not in SELECTOR_NOISE_KEYS and str(v or "").strip() != ""}


def resolve_target_spec(body, file_name):
    """Return (selector, source) or (None, reason).  Precedence:
       1. body.target.selector (explicit identity fields)
       2. body.target.alias   -> routing_map.aliases[alias]
       3. routing_map.file_rules (fnmatch on the file name)
    No merging between sources: what wins is used as-is, so behaviour is predictable."""
    target = body.get("target") if isinstance(body.get("target"), dict) else {}
    rmap = load_routing_map()
    if isinstance(target.get("selector"), dict) and target["selector"]:
        return _clean_selector(target["selector"]), "payload.selector"
    # identity fields placed directly under "target" are accepted as a selector too
    direct = {k: v for k, v in target.items() if k in ALLOWED_SELECTOR_KEYS | LOCAL_SELECTOR_KEYS}
    if direct:
        return _clean_selector(direct), "payload.selector"
    if target.get("alias"):
        sel = rmap["aliases"].get(str(target["alias"]))
        if not isinstance(sel, dict):
            return None, "unknown_alias:{}".format(target["alias"])
        return _clean_selector(sel), "payload.alias"
    for rule in rmap["file_rules"]:
        pat = str(rule.get("file_glob") or "")
        if pat and fnmatch.fnmatch(os.path.basename(file_name).lower(), pat.lower()):
            if rule.get("selector"):
                return _clean_selector(rule["selector"]), "file_rule"
            sel = rmap["aliases"].get(str(rule.get("alias") or ""))
            if isinstance(sel, dict):
                return _clean_selector(sel), "file_rule"
    # No target information is allowed: the router will apply the deterministic
    # automatic selection policy in route_waiting().  Keeping this as a distinct
    # source lets the API show that the machine was selected by policy rather than
    # by a caller-supplied selector.
    return {}, "automatic"


# ----------------------------------------------------------------------------
# validation (pre-flight against what the Revit handlers really require)
# ----------------------------------------------------------------------------
def validate_body(obj, file_name):
    """Returns (normalised_body, errors[], warnings[])."""
    errors, warnings = [], []
    if isinstance(obj, list):
        obj = {"items": obj}
    if not isinstance(obj, dict):
        return None, ["payload must be a JSON object or a list of items"], warnings
    items = obj.get("items")
    if items is None and isinstance(obj.get("payload"), dict):
        items = obj["payload"].get("items")
    if not isinstance(items, list) or not items:
        return None, ["'items' must be a non-empty list"], warnings
    if len(items) > MAX_ITEMS:
        return None, ["items exceeds COMMAND_MAX_ITEMS ({})".format(MAX_ITEMS)], warnings

    defined = set()
    seen_nodes = set()
    later_nodes = {str(i.get("node_id")) for i in items if isinstance(i, dict) and i.get("node_id")}
    for idx, it in enumerate(items):
        tag = "items[{}]".format(idx)
        if not isinstance(it, dict):
            errors.append("{} is not an object".format(tag)); continue
        kind = it.get("placement")
        node = it.get("node_id")
        if kind not in KNOWN_PLACEMENTS:
            errors.append("{}: unknown placement {!r} (allowed: {})".format(tag, kind, ", ".join(sorted(KNOWN_PLACEMENTS))))
            continue
        if node:
            if str(node) in seen_nodes:
                errors.append("{}: duplicate node_id {!r}".format(tag, node))
            seen_nodes.add(str(node))
        if kind == "point_family":
            for k in ("family", "type", "level"):
                if not it.get(k):
                    errors.append("{} ({}): '{}' is required".format(tag, node or kind, k))
        elif kind == "curve_mep":
            if it.get("system") not in CURVE_SYSTEMS:
                errors.append("{} ({}): system must be one of {}".format(tag, node or kind, sorted(CURVE_SYSTEMS)))
            if not it.get("level"):
                errors.append("{} ({}): 'level' is required".format(tag, node or kind))
        elif kind in ("inline_fitting", "hosted_family"):
            hn = it.get("host_node")
            if not hn:
                errors.append("{} ({}): 'host_node' is required".format(tag, node or kind))
            elif str(hn) not in defined:
                errors.append("{} ({}): host_node {!r} is not created by an EARLIER item (items run in list order)".format(tag, node or kind, hn))
        elif kind == "connect":
            for a, raw in (("from_node", "element_id_a"), ("to_node", "element_id_b")):
                ref = it.get(a)
                if ref and str(ref) in defined:
                    continue
                if it.get(raw):
                    continue
                errors.append("{}: {} {!r} is not created by an EARLIER item and no {} fallback given".format(tag, a, ref, raw))
        elif kind == "fitting":
            for c in (it.get("_connectors") or []):
                peer = c.get("connected_to_node_id") if isinstance(c, dict) else None
                if peer and str(peer) not in defined:
                    where = "a LATER item" if str(peer) in later_nodes else "no item"
                    warnings.append("{} ({}): peer {!r} is created by {} - Revit side silently skips unresolved peers".format(tag, node or kind, peer, where))
        if node:
            defined.add(str(node))

    body = {
        "items": items,
        "action": str(obj.get("action") or "place_mep_elements"),
    }
    for k in ("payload_id", "target", "priority", "depends_on", "on_uncertain", "max_attempts", "allow_duplicate", "schema_version"):
        if k in obj:
            body[k] = obj[k]
    if errors:
        return None, errors, warnings
    return body, [], warnings


# ----------------------------------------------------------------------------
# 1) scan the folder
# ----------------------------------------------------------------------------
def scan_folder(db):
    ensure_dirs()
    found = 0
    root = _inbox()
    for dirpath, _dirs, files in os.walk(root):
        for fn in sorted(files):
            if not fn.lower().endswith(".json") or fn.startswith((".", "~")) or fn.lower().endswith(".status.json"):
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace("\\", "/")
            try:
                st = os.stat(full)
                age = time.time() - st.st_mtime
            except OSError:
                continue
            if age < SETTLE_SEC:
                continue                                            # still being copied in
            sig = (st.st_mtime, st.st_size)
            if _scan_sig.get(rel) == sig and db.query(PayloadRecord).filter(
                    PayloadRecord.file_name == rel, PayloadRecord.status.notin_(list(TERMINAL))).first() is not None:
                continue                                            # unchanged and still in progress: don't re-hash 5 MB every tick
            try:
                raw = open(full, "rb").read()
            except OSError:
                continue
            try:
                obj = json.loads(raw.decode("utf-8-sig"))
                content_hash = _canonical_hash(obj)
            except Exception as ex:
                if age < PARTIAL_WRITE_GRACE_SEC:
                    continue                                        # probably a partial write; look again next tick
                content_hash = hashlib.sha256(raw).hexdigest()
                obj = None
                parse_error = "invalid JSON: {}".format(ex)
            else:
                parse_error = None

            rows = db.query(PayloadRecord).filter(PayloadRecord.sha256 == content_hash).all()
            this_file = [r for r in rows if r.file_name == rel]
            if any(r.status not in TERMINAL for r in this_file):
                continue                                            # this exact file is already being processed
            if any(r.status == "DUPLICATE" and r.completed_at and (now() - r.completed_at).total_seconds() < 60 for r in this_file):
                continue                                            # duplicate already recorded; waiting for the file move
            same = next((r for r in rows if r.status not in ("INVALID", "CANCELLED", "EXPIRED", "DUPLICATE")), None)
            allow_dup = isinstance(obj, dict) and bool(obj.get("allow_duplicate"))

            # Changed content under the same file name: supersede the old one if it never reached Revit.
            old = (db.query(PayloadRecord).filter(PayloadRecord.file_name == rel, PayloadRecord.sha256 != content_hash,
                                                  PayloadRecord.status.in_(list(WAITING | {"QUEUED"}))).all())
            for o in old:
                _cancel_payload(db, o, "superseded by edited file")

            p = PayloadRecord(
                payload_id="PLD-{}".format(uuid.uuid4()), file_name=rel, sha256=content_hash,
                logical_id=(obj.get("payload_id") if isinstance(obj, dict) and obj.get("payload_id") else os.path.splitext(os.path.basename(rel))[0]),
                status="DISCOVERED", max_attempts=DEFAULT_MAX_ATTEMPTS,
            )
            db.add(p)
            _event(db, p.payload_id, "DISCOVERED", {"file": rel, "sha256": content_hash})
            if parse_error:
                p.status, p.error_code, p.last_error, p.completed_at = "INVALID", "invalid_json", parse_error, now()
                _event(db, p.payload_id, "INVALID", {"errors": [parse_error]})
            elif same is not None and not allow_dup and same.status not in ("INVALID", "CANCELLED", "EXPIRED"):
                p.status, p.error_code, p.completed_at = "DUPLICATE", "duplicate_of:{}".format(same.payload_id), now()
                p.last_error = ("identical content already tracked as {} ({}); to run it again use POST /api/payloads/{}/retry?force=true "
                                "or set allow_duplicate=true in the file").format(same.payload_id, same.status, same.payload_id)
                _event(db, p.payload_id, "DUPLICATE", {"of": same.payload_id})
            else:
                p.body = obj
            db.commit()
            _scan_sig[rel] = sig
            found += 1
    return found


# ----------------------------------------------------------------------------
# 2) validate
# ----------------------------------------------------------------------------
def validate_discovered(db):
    n = 0
    for p in db.query(PayloadRecord).filter(PayloadRecord.status == "DISCOVERED").all():
        body, errors, warnings = validate_body(p.body, p.file_name)
        if errors:
            if _cas(db, p, {"DISCOVERED"}, status="INVALID", error_code="validation_failed",
                    last_error="; ".join(errors[:10]), completed_at=now()):
                _event(db, p.payload_id, "INVALID", {"errors": errors})
            continue
        spec, source = resolve_target_spec(body, p.file_name)
        if _cas(db, p, {"DISCOVERED"}, status="VALIDATED", body={k: body[k] for k in ("items", "action", "target") if k in body},
                action=body["action"], item_count=len(body["items"]),
                priority=int(body.get("priority", 100)), depends_on=body.get("depends_on") or [],
                on_uncertain=("retry" if body.get("on_uncertain") == "retry" else "hold"),
                max_attempts=int(body.get("max_attempts", DEFAULT_MAX_ATTEMPTS)),
                target_spec=spec, target_source=source, error_code=None):
            _event(db, p.payload_id, "VALIDATED", {"items": len(body["items"]), "warnings": warnings, "target_source": source})
            n += 1
    return n


# ----------------------------------------------------------------------------
# 3) route + dispatch
# ----------------------------------------------------------------------------
def _online_candidates(db, selector):
    selector = dict(selector)
    wanted_name = str(selector.pop("machine_name", "") or "").strip().lower()
    out = []
    for c in resolve_candidates(db, selector):
        sysrow = db.get(SystemRecord, c["machine_id"])
        if sysrow is None or sysrow.status != "online":
            continue
        if wanted_name and str(sysrow.machine_name or "").strip().lower() != wanted_name:
            continue
        out.append(c)
    return out


def _doc_busy(db, document_id, exclude_payload_id):
    return db.query(PayloadRecord).filter(
        PayloadRecord.selected_document_id == document_id,
        PayloadRecord.payload_id != exclude_payload_id,
        PayloadRecord.status.in_(list(INFLIGHT | {"TARGET_SELECTED"})),
    ).first() is not None


def _candidate_load(db, document_id):
    """Return the current deterministic load for one live Revit document.

    The load includes both payload-router work and legacy commands already
    pinned to the same document.  This is deliberately a count, not a random
    or time-based choice, so the same registry state produces the same winner.
    """
    payload_load = db.query(PayloadRecord).filter(
        PayloadRecord.selected_document_id == document_id,
        PayloadRecord.status.in_(list(INFLIGHT | {"TARGET_SELECTED"})),
    ).count()
    command_rows = db.query(CommandRecord).filter(
        CommandRecord.status.in_(["PENDING", "CLAIMED", "EXECUTING"])
    ).all()
    command_load = sum(1 for row in command_rows
                       if str((row.routing or {}).get("document_id") or "") == str(document_id))
    return payload_load + command_load


def _choose_candidate(db, candidates):
    """Choose one candidate using a documented, deterministic policy.

    Priority:
      1. do not select a document that is already busy when another candidate
         is free;
      2. lowest active workload;
      3. stable identity tie-breakers (machine_id, process/session, document_id).

    No random choice and no dependence on database row order.
    """
    if not candidates:
        return None
    free = [c for c in candidates if not _doc_busy(db, c["document_id"], "")]
    pool = free or candidates
    scored = []
    for c in pool:
        scored.append((_candidate_load(db, c["document_id"]),
                       str(c.get("machine_id") or ""),
                       str(c.get("revit_process_id") or ""),
                       str(c.get("session_id") or ""),
                       str(c.get("revit_instance_id") or ""),
                       str(c.get("document_id") or ""), c))
    scored.sort(key=lambda x: x[:-1])
    return scored[0][-1]


def _deps_state(db, p):
    """'ok' | 'wait' | ('failed', dep)"""
    for dep in (p.depends_on or []):
        rows = db.query(PayloadRecord).filter(PayloadRecord.logical_id == str(dep)).all()
        if any(r.status == "SUCCEEDED" for r in rows):
            continue
        if rows and all(r.status in TERMINAL_BAD for r in rows):
            return ("failed", dep)
        return "wait"
    return "ok"


def route_waiting(db):
    routed = 0
    cur = now()
    q = (db.query(PayloadRecord)
         .filter(PayloadRecord.status.in_(["VALIDATED", "NO_TARGET", "AMBIGUOUS_TARGET", "RETRY"]))
         .order_by(PayloadRecord.priority.desc(), PayloadRecord.file_name.asc(), PayloadRecord.discovered_at.asc()))
    for p in q.all():
        if p.status == "RETRY" and p.next_attempt_at and p.next_attempt_at > cur:
            continue
        prior = p.status
        dep = _deps_state(db, p)
        if isinstance(dep, tuple):
            if _cas(db, p, {prior}, status="CANCELLED", error_code="dependency_failed",
                    last_error="dependency {!r} failed".format(dep[1]), completed_at=cur):
                _event(db, p.payload_id, "CANCELLED", {"reason": "dependency_failed", "dependency": dep[1]})
            continue
        if dep == "wait":
            continue

        spec, source = resolve_target_spec(p.body or {}, p.file_name)      # cheap; map is cached by mtime
        # An empty selector means automatic routing.  Explicit selectors are
        # still honoured, but if they match multiple live documents the same
        # deterministic workload/tie-break policy is used instead of guessing
        # from database order.
        if spec is None:
            spec = {}
        if spec != p.target_spec or source != p.target_source:
            p.target_spec, p.target_source = spec, source
            db.commit()

        strong = {"agent_id", "document_id", "revit_instance_id", "revit_process_id", "session_id", "machine_id", "machine_name", "project_uid", "document_path"}
        bad = sorted(set(spec) - ALLOWED_SELECTOR_KEYS - LOCAL_SELECTOR_KEYS)
        if bad or (spec and not (set(spec) & strong)):
            if _cas(db, p, {prior}, status="INVALID", error_code="bad_selector", completed_at=cur,
                    last_error="selector unusable (unknown keys {} or no identity field): {}".format(bad, spec)):
                _event(db, p.payload_id, "INVALID", {"selector": spec})
            continue

        cands = _online_candidates(db, spec)
        if len(cands) == 0:
            if NO_TARGET_TTL_SEC and p.discovered_at and (cur - p.discovered_at).total_seconds() > NO_TARGET_TTL_SEC:
                if _cas(db, p, {prior}, status="EXPIRED", error_code="no_target_expired", completed_at=cur,
                        last_error="no online document matched for {}s".format(int(NO_TARGET_TTL_SEC))):
                    _event(db, p.payload_id, "EXPIRED", {"selector": spec})
                continue
            _mark_waiting(db, p, prior, "NO_TARGET", "no_target_online", "no online document matches {}".format(spec), [])
            continue
        # Deterministic automatic selection.  For an exact document_id this
        # normally yields one candidate.  For broader selectors (including a
        # shared project_uid) or no selector at all, select the least-loaded
        # free live document, then stable machine/process/session/document IDs.
        doc = _choose_candidate(db, cands)
        if doc is None:
            continue
        if _doc_busy(db, doc["document_id"], p.payload_id):
            continue
        if not _cas(db, p, {prior}, status="TARGET_SELECTED", selected_document_id=doc["document_id"],
                    selected_agent_id=doc["agent_id"], selected_machine_id=doc["machine_id"],
                    selected_process_id=doc.get("revit_process_id"), selected_session_id=doc.get("session_id"),
                    candidates=None, error_code=None, last_error=None):
            continue                                              # another worker took it
        _event(db, p.payload_id, "TARGET_SELECTED", {"document_id": doc["document_id"], "agent_id": doc["agent_id"], "machine_id": doc["machine_id"]})
        if _dispatch(db, p, doc):
            routed += 1
    return routed


def _mark_waiting(db, p, prior, status, code, msg, cands):
    changed = (p.status != status) or (p.error_code != code)
    if _cas(db, p, {prior}, status=status, error_code=code, last_error=msg, candidates=cands or None):
        if changed:
            _event(db, p.payload_id, status, {"message": msg, "candidates": cands})


def _dispatch(db, p, doc):
    """TARGET_SELECTED -> QUEUED via the existing create_command() (audited, idempotent)."""
    attempt = (p.attempts or 0) + 1
    pinned = {"document_id": doc["document_id"], "revit_process_id": doc.get("revit_process_id"),
              "session_id": doc.get("session_id"), "machine_id": doc["machine_id"]}
    body = CreateCommand(action=p.action, items=p.body["items"], target_selector={k: v for k, v in pinned.items() if v},
                         idempotency_key="payload:{}:{}".format(p.payload_id, attempt), max_attempts=2)
    try:
        created = create_command(body, db)
    except HTTPException as ex:
        db.rollback()
        detail = ex.detail if isinstance(ex.detail, dict) else {"message": str(ex.detail)}
        # target vanished between resolve and create: not a failure, just go back to waiting
        _cas(db, p, {"TARGET_SELECTED"}, status="NO_TARGET", error_code=detail.get("code") or "create_rejected",
             last_error=str(detail.get("message") or detail)[:500], selected_document_id=None)
        _event(db, p.payload_id, "DISPATCH_REJECTED", {"http": ex.status_code, "detail": detail})
        return False
    if not _cas(db, p, {"TARGET_SELECTED"}, status="QUEUED", command_id=created["command_id"], attempts=attempt, queued_at=now()):
        return False
    _event(db, p.payload_id, "QUEUED", {"command_id": created["command_id"], "attempt": attempt, "routing": created.get("routing")})
    return True


# ----------------------------------------------------------------------------
# 4) reconcile in-flight payloads with their commands
# ----------------------------------------------------------------------------
def classify_failure(result):
    """(payload_status, safe_to_auto_retry, code) for a command that ended FAILED."""
    if not isinstance(result, dict):
        return "EXECUTION_FAILED", False, "no_result_body"
    if result.get("status") == "timeout":
        # The agent stopped waiting, but the ExternalEvent may still fire later inside Revit.
        return "TIMEOUT", False, "agent_execute_timeout"
    rerr = result.get("routing_error")
    if isinstance(rerr, dict):
        # smart_target/_resolve_target_document reject BEFORE the transaction opens: nothing was created.
        return "DELIVERY_FAILED", True, str(rerr.get("code") or "routing_error")
    if result.get("committed") is False and result.get("fatal_error"):
        # Execute() caught an exception and rolled the transaction back. Deterministic - retrying alone won't help.
        return "EXECUTION_FAILED", False, "transaction_rolled_back"
    return "EXECUTION_FAILED", False, "failed"


def _latest_result(db, command_id):
    row = (db.query(CommandResultRecord).filter(CommandResultRecord.command_id == command_id)
           .order_by(CommandResultRecord.received_at.desc()).first())
    return row.result if row else {}


def _summarise(result):
    if not isinstance(result, dict):
        return {}
    bad = [{"index": i.get("index"), "node_id": i.get("node_id"), "placement": i.get("placement"), "error": i.get("error")}
           for i in (result.get("items") or []) if isinstance(i, dict) and not i.get("ok")]
    return {"committed": result.get("committed"), "total": result.get("total"), "succeeded": result.get("succeeded"),
            "failed": result.get("failed"), "fatal_error": result.get("fatal_error"), "routing_error": result.get("routing_error"),
            "resolved_document_title": result.get("resolved_document_title"), "failed_items": bad[:50]}


def _schedule_retry_or_fail(db, p, expected, final_status, code, msg):
    """Only called when a retry is SAFE (nothing created in Revit)."""
    if (p.attempts or 0) < (p.max_attempts or DEFAULT_MAX_ATTEMPTS):
        delay = BACKOFF_SEC[min((p.attempts or 1) - 1, len(BACKOFF_SEC) - 1)]
        if _cas(db, p, expected, status="RETRY", error_code=code, last_error=msg, command_id=None,
                selected_document_id=None, next_attempt_at=now() + dt.timedelta(seconds=delay)):
            _event(db, p.payload_id, "RETRY", {"code": code, "in_sec": delay, "attempt": p.attempts})
    else:
        if _cas(db, p, expected, status=final_status, error_code=code, last_error=msg + " (retries exhausted)", completed_at=now()):
            _event(db, p.payload_id, final_status, {"code": code, "retries_exhausted": True})


def _cancel_command_if_pending(db, command_id, reason):
    """CAS: only cancels a command nobody has claimed yet (never yank a command from under Revit)."""
    n = (db.query(CommandRecord).filter(CommandRecord.command_id == command_id, CommandRecord.status == "PENDING")
         .update({"status": "CANCELLED", "completed_at": now(), "last_error": reason, "updated_at": now()}, synchronize_session=False))
    db.commit()
    return n == 1


def reconcile_inflight(db):
    changed = 0
    cur = now()
    for p in db.query(PayloadRecord).filter(PayloadRecord.status.in_(list(INFLIGHT))).all():
        cmd = db.get(CommandRecord, p.command_id) if p.command_id else None
        if cmd is None:
            _schedule_retry_or_fail(db, p, INFLIGHT, "DELIVERY_FAILED", "command_missing", "command row not found")
            changed += 1; continue
        db.refresh(cmd)
        st = cmd.status
        if st == "PENDING":
            age = (cur - (cmd.created_at or cur)).total_seconds()
            doc = db.get(DocumentRecord, p.selected_document_id) if p.selected_document_id else None
            gone = (doc is None or not doc.is_online or str(doc.session_id or "") != str(p.selected_session_id or "")
                    or str(doc.revit_process_id or "") != str(p.selected_process_id or ""))
            if gone and age > PENDING_GRACE_SEC:
                if _cancel_command_if_pending(db, cmd.command_id, "target Revit session gone before claim"):
                    _schedule_retry_or_fail(db, p, INFLIGHT, "AGENT_OFFLINE", "target_session_gone",
                                            "target document/session went offline before the command was claimed")
                    changed += 1
            elif age > QUEUED_TTL_SEC:
                if _cancel_command_if_pending(db, cmd.command_id, "not claimed within queued TTL"):
                    _schedule_retry_or_fail(db, p, INFLIGHT, "DELIVERY_FAILED", "not_claimed",
                                            "agent is online but did not poll the command within {}s (Revit busy / modal dialog?)".format(int(QUEUED_TTL_SEC)))
                    changed += 1
            elif p.status != "QUEUED":
                _cas(db, p, INFLIGHT, status="QUEUED")
        elif st == "CLAIMED":
            if p.status != "CLAIMED" and _cas(db, p, INFLIGHT, status="CLAIMED"):
                _event(db, p.payload_id, "CLAIMED", {"owner": cmd.lease_owner}); changed += 1
        elif st == "EXECUTING":
            if p.status != "EXECUTING" and _cas(db, p, INFLIGHT, status="EXECUTING"):
                _event(db, p.payload_id, "EXECUTING", {}); changed += 1
        elif st == "SUCCEEDED":
            if _cas(db, p, INFLIGHT, status="SUCCEEDED", completed_at=cmd.completed_at or cur, error_code=None, last_error=None,
                    result_summary=_summarise(_latest_result(db, cmd.command_id))):
                _event(db, p.payload_id, "SUCCEEDED", {"command_id": cmd.command_id}); changed += 1
        elif st == "PARTIAL":
            summ = _summarise(_latest_result(db, cmd.command_id))
            if _cas(db, p, INFLIGHT, status="PARTIAL", completed_at=cur, error_code="partial_commit",
                    last_error="{} of {} items failed; committed elements remain in the model - review before re-running".format(summ.get("failed"), summ.get("total")),
                    result_summary=summ):
                _event(db, p.payload_id, "PARTIAL", summ); changed += 1
        elif st == "FAILED":
            res = _latest_result(db, cmd.command_id)
            final, safe, code = classify_failure(res)
            p.result_summary = _summarise(res)
            if safe:
                _schedule_retry_or_fail(db, p, INFLIGHT, final, code, str(cmd.last_error or code)[:500])
            elif final == "TIMEOUT" and p.on_uncertain == "retry":
                _schedule_retry_or_fail(db, p, INFLIGHT, final, code, "agent timeout; on_uncertain=retry")
            elif _cas(db, p, INFLIGHT, status=final, error_code=code, last_error=str(cmd.last_error or code)[:500], completed_at=cur,
                      result_summary=_summarise(res)):
                _event(db, p.payload_id, final, {"code": code}); 
            changed += 1
        elif st == "DEAD_LETTER":
            uncertain = "EXECUTING" in (cmd.last_error or "") or "outcome unknown" in (cmd.last_error or "")
            if uncertain and p.on_uncertain != "retry":
                if _cas(db, p, INFLIGHT, status="TIMEOUT", error_code="outcome_unknown", completed_at=cur,
                        last_error="agent went silent while executing; elements may or may not exist in Revit - verify, then POST /api/payloads/{id}/retry"):
                    _event(db, p.payload_id, "TIMEOUT", {"command_id": cmd.command_id}); changed += 1
            else:
                _schedule_retry_or_fail(db, p, INFLIGHT, "DELIVERY_FAILED", "never_started", str(cmd.last_error or "dead letter")[:500]); changed += 1
        elif st == "CANCELLED":
            if _cas(db, p, INFLIGHT, status="CANCELLED", error_code="command_cancelled", completed_at=cur, last_error=cmd.last_error):
                _event(db, p.payload_id, "CANCELLED", {"command_id": cmd.command_id}); changed += 1
    return changed


# ----------------------------------------------------------------------------
# 5) file lifecycle
# ----------------------------------------------------------------------------
def finalize_files(db):
    moved = 0
    for p in db.query(PayloadRecord).filter(PayloadRecord.status.in_(list(TERMINAL))).all():
        if not os.path.isfile(os.path.join(_inbox(), p.file_name)):
            continue
        # a newer row for the same file name (edited file) still lives in inbox/ - do not move its file
        newer = db.query(PayloadRecord).filter(PayloadRecord.file_name == p.file_name, PayloadRecord.payload_id != p.payload_id,
                                               PayloadRecord.discovered_at > p.discovered_at, PayloadRecord.status.notin_(list(TERMINAL))).first()
        if newer is not None:
            continue
        dest = "done" if p.status == "SUCCEEDED" else ("duplicate" if p.status == "DUPLICATE" else "failed")
        side = {"payload_id": p.payload_id, "status": p.status, "error_code": p.error_code, "last_error": p.last_error,
                "command_id": p.command_id, "agent_id": p.selected_agent_id, "machine_id": p.selected_machine_id,
                "document_id": p.selected_document_id, "attempts": p.attempts, "result": p.result_summary}
        try:
            if _safe_move(p.file_name, dest, side):
                moved += 1
        except OSError:
            log.exception("could not move %s to %s/", p.file_name, dest)
    return moved


# ----------------------------------------------------------------------------
# operator actions
# ----------------------------------------------------------------------------
def _cancel_payload(db, p, reason):
    if p.status in INFLIGHT and p.command_id:
        if not _cancel_command_if_pending(db, p.command_id, reason):
            return False                                            # already claimed: cannot be cancelled safely
    p.status, p.error_code, p.last_error, p.completed_at = "CANCELLED", "cancelled", reason, now()
    _event(db, p.payload_id, "CANCELLED", {"reason": reason})
    db.commit()
    return True


def cancel(db, payload_id, reason="cancelled by operator"):
    p = db.get(PayloadRecord, payload_id)
    if p is None or p.status in TERMINAL:
        return p, False
    return p, _cancel_payload(db, p, reason)


def retry(db, payload_id, force=False):
    """Operator re-queue. Refuses SUCCEEDED/PARTIAL/TIMEOUT unless force=True, because Revit may already hold the elements."""
    p = db.get(PayloadRecord, payload_id)
    if p is None:
        return None, "not_found"
    if p.status in WAITING or p.status in INFLIGHT:
        return p, "already_active"
    risky = p.status in ("SUCCEEDED", "PARTIAL", "TIMEOUT")
    if risky and not force:
        return p, "elements_may_exist_use_force"
    if not p.body:
        return p, "no_body_stored"
    p.status, p.error_code, p.last_error, p.completed_at = "VALIDATED", None, None, None
    p.command_id, p.selected_document_id, p.next_attempt_at = None, None, None
    p.max_attempts = max(p.max_attempts or 1, (p.attempts or 0) + 1)
    _event(db, p.payload_id, "MANUAL_RETRY", {"force": force})
    db.commit()
    return p, "requeued"


def preview(db, body, file_name="preview.json"):
    body_n, errors, warnings = validate_body(body, file_name)
    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}
    spec, source = resolve_target_spec(body_n, file_name)
    out = {"valid": True, "warnings": warnings, "target_source": source, "selector": spec, "item_count": len(body_n["items"])}
    c = _online_candidates(db, spec or {})
    out["candidates"] = c
    if not c:
        out["outcome"] = "NO_TARGET"
    else:
        chosen = _choose_candidate(db, c)
        out["selected"] = chosen
        out["selection_policy"] = "least_active_workload_then_machine_id_then_process_id_then_session_id_then_instance_id_then_document_id"
        out["outcome"] = "TARGET_SELECTED"
    return out


# ----------------------------------------------------------------------------
# tick + background loop
# ----------------------------------------------------------------------------
ADVISORY_LOCK_KEY = 727001


def _acquire_cluster_lock():
    """Postgres only: make sure just ONE server worker scans the folder. Returns (ok, connection).
    Held on a dedicated connection for the duration of the tick; Postgres drops it automatically if
    the connection dies. SQLite (single process) needs no lock. NOTE: the Postgres branch is not
    exercised by tests/test_payload_router.py (SQLite); fails OPEN with a warning so a lock problem
    can never stop payloads from being processed on a single-worker deployment."""
    if engine.dialect.name != "postgresql":
        return True, None
    try:
        conn = engine.connect()
        got = conn.execute(sql_text("SELECT pg_try_advisory_lock(:k)"), {"k": ADVISORY_LOCK_KEY}).scalar()
        if not got:
            conn.close()
            return False, None
        return True, conn
    except Exception:
        log.warning("advisory lock unavailable; continuing without it", exc_info=True)
        return True, None


def _release_cluster_lock(conn):
    if conn is None:
        return
    try:
        conn.execute(sql_text("SELECT pg_advisory_unlock(:k)"), {"k": ADVISORY_LOCK_KEY})
    finally:
        conn.close()


def tick():
    """One full pass. Safe to call concurrently: the lock protects this process, the advisory lock protects
    other workers (Postgres), and every state change is a compare-and-set."""
    if not _tick_lock.acquire(blocking=False):
        return {"skipped": "tick already running"}
    ok, lock_conn = _acquire_cluster_lock()
    if not ok:
        _tick_lock.release()
        return {"skipped": "another worker holds the payload scanner lock"}
    db = SessionLocal()
    try:
        # Lease expiry used to run only when SOME agent polled /next. If every agent is dead (crash, network
        # loss, Revit closed) nobody polls, so EXECUTING/CLAIMED commands would never be expired. The manager
        # therefore sweeps leases (and the PENDING TTL) itself on every pass.
        _expire_leases(db)
        stats = {"discovered": scan_folder(db)}
        stats["validated"] = validate_discovered(db)
        stats["reconciled"] = reconcile_inflight(db)      # before routing, so finished payloads free their document
        stats["routed"] = route_waiting(db)
        stats["reconciled_after"] = reconcile_inflight(db)
        stats["files_moved"] = finalize_files(db)
        return stats
    finally:
        db.close()
        _release_cluster_lock(lock_conn)
        _tick_lock.release()


_thread = None


def start_background():
    global _thread
    if os.environ.get("MEPF_PAYLOAD_WATCHER", "1") in ("0", "false", "False"):
        log.info("payload watcher disabled by MEPF_PAYLOAD_WATCHER")
        return
    if _thread is not None and _thread.is_alive():
        return
    ensure_dirs()

    def loop():
        while True:
            try:
                tick()
            except Exception:
                log.exception("payload tick failed")
            time.sleep(SCAN_SEC)

    _thread = threading.Thread(target=loop, name="payload-watcher", daemon=True)
    _thread.start()
    log.info("payload watcher started; dir=%s every %ss", PAYLOAD_DIR, SCAN_SEC)
