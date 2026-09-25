# -*- coding: utf-8 -*-
import uuid
import datetime as dt
import hashlib
import json
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Header
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.auth import require_api_key
from app.models.db import (
    get_db, CommandRecord, CommandResultRecord, AuditEventRecord, IdempotencyRecord, now
)
from app.orchestrator.routing_engine import find_target_document, build_routing_block, RoutingError, resolve_candidates

router = APIRouter(prefix="/api/commands", tags=["commands"], dependencies=[Depends(require_api_key)])
LEASE_SEC = int(__import__('os').environ.get("COMMAND_LEASE_SEC", "300"))
MAX_ATTEMPTS = int(os.environ.get("COMMAND_MAX_ATTEMPTS", "3"))
MAX_ITEMS = int(os.environ.get("COMMAND_MAX_ITEMS", "5000"))
MAX_PAYLOAD_BYTES = int(os.environ.get("COMMAND_MAX_PAYLOAD_BYTES", "5000000"))
ALLOWED_SELECTOR_KEYS = {"document_id", "project_uid", "machine_id", "document_title", "document_path", "revit_version", "revit_instance_id", "revit_process_id", "session_id"}

class CreateCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: str = "place_mep_elements"
    items: list = Field(default_factory=list)
    target_selector: dict = Field(default_factory=dict)
    idempotency_key: str | None = None
    max_attempts: int = Field(default=MAX_ATTEMPTS, ge=1, le=10)

class CommandResult(BaseModel):
    command_id: str
    status: str
    routing: dict = Field(default_factory=dict)
    result: dict = Field(default_factory=dict)
    lease_token: str | None = None
    lease_owner: str | None = None

def _audit(db, command_id, event_type, actor, details=None):
    db.add(AuditEventRecord(command_id=command_id, event_type=event_type, actor=actor, details=details or {}))


def _request_fingerprint(body: CreateCommand) -> str:
    canonical = {
        "action": body.action,
        "items": body.items,
        "target_selector": body.target_selector,
        "max_attempts": body.max_attempts,
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validate_create_request(body: CreateCommand):
    if not body.action.strip():
        raise HTTPException(status_code=422, detail="action must not be empty")
    if not isinstance(body.target_selector, dict) or not body.target_selector:
        raise HTTPException(status_code=422, detail="target_selector is required; never route by list position or by an implicit single-agent match")
    unknown = sorted(set(body.target_selector) - ALLOWED_SELECTOR_KEYS)
    if unknown:
        raise HTTPException(status_code=422, detail={"code": "unknown_target_selector_fields", "fields": unknown})
    if not any(str(body.target_selector.get(k) or "").strip() for k in ("document_id", "revit_instance_id", "revit_process_id", "session_id", "machine_id", "project_uid", "document_path")):
        raise HTTPException(status_code=422, detail="target_selector must contain at least one real identity field")
    if len(body.items) > MAX_ITEMS:
        raise HTTPException(status_code=413, detail="items exceeds COMMAND_MAX_ITEMS")
    try:
        payload_size = len(json.dumps(body.items, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8"))
    except Exception as ex:
        raise HTTPException(status_code=422, detail="items could not be serialized: {}".format(ex))
    if payload_size > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="items payload exceeds COMMAND_MAX_PAYLOAD_BYTES")

def _expire_leases(db):
    current = now()
    rows = db.query(CommandRecord).filter(
        CommandRecord.status.in_(["CLAIMED", "EXECUTING"]),
        CommandRecord.lease_expires_at.isnot(None),
        CommandRecord.lease_expires_at < current,
    ).all()
    changed = False
    for row in rows:
        if (row.attempts or 0) >= (row.max_attempts or MAX_ATTEMPTS):
            row.status = "DEAD_LETTER"
            row.last_error = "Execution lease expired after maximum attempts."
            _audit(db, row.command_id, "DEAD_LETTER", row.lease_owner, {"attempts": row.attempts})
        else:
            row.status = "PENDING"
            row.lease_token = None
            row.lease_owner = None
            row.lease_expires_at = None
            row.last_error = "Execution lease expired; command returned to queue."
            _audit(db, row.command_id, "LEASE_EXPIRED_REQUEUED", None, {"attempts": row.attempts})
        changed = True
    if changed:
        db.commit()

@router.post("/create")
def create_command(body: CreateCommand, db: Session = Depends(get_db)):
    _expire_leases(db)
    if not body.items:
        raise HTTPException(status_code=422, detail="items must contain at least one work item")
    _validate_create_request(body)
    request_hash = _request_fingerprint(body)
    if body.idempotency_key:
        existing = db.get(IdempotencyRecord, body.idempotency_key)
        if existing:
            if existing.request_hash and existing.request_hash != request_hash:
                raise HTTPException(status_code=409, detail="idempotency_key was already used for a different command payload")
            cmd = db.get(CommandRecord, existing.command_id)
            if cmd is None:
                raise HTTPException(status_code=409, detail="idempotency_key points to a missing command")
            return {"status": "existing", "command_id": cmd.command_id, "machine_id": cmd.machine_id, "routing": cmd.routing, "command_status": cmd.status}
    try:
        doc_row = find_target_document(db, body.target_selector)
    except RoutingError as ex:
        raise HTTPException(status_code=409, detail={"code": ex.code, "message": ex.message, "candidates": ex.candidates})
    command_id = "CMD-{0}".format(uuid.uuid4())
    routing = build_routing_block(doc_row)
    row = CommandRecord(command_id=command_id, machine_id=doc_row.machine_id, routing=routing, action=body.action, items=body.items, status="PENDING", max_attempts=body.max_attempts)
    db.add(row)
    if body.idempotency_key:
        db.add(IdempotencyRecord(key=body.idempotency_key, command_id=command_id, request_hash=request_hash))
    _audit(db, command_id, "CREATED", "api", {"machine_id": doc_row.machine_id, "action": body.action})
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        if body.idempotency_key:
            existing = db.get(IdempotencyRecord, body.idempotency_key)
            if existing:
                cmd = db.get(CommandRecord, existing.command_id)
                if cmd and (not existing.request_hash or existing.request_hash == request_hash):
                    return {"status": "existing", "command_id": cmd.command_id, "machine_id": cmd.machine_id, "routing": cmd.routing, "command_status": cmd.status}
        raise HTTPException(status_code=409, detail="Command could not be committed because a unique record already exists")
    return {"status": "queued", "routing_status": "target_resolved", "command_id": command_id, "machine_id": doc_row.machine_id, "routing": routing, "command_status": "PENDING"}

@router.get("/next")
def next_command(machine_id: str = Query(default=""), revit_process_id: str = Query(default=""), session_id: str = Query(default=""), x_workstation_id: str = Header(default=""), db: Session = Depends(get_db)):
    if not machine_id or not revit_process_id or not session_id:
        return {"commands": []}
    if not x_workstation_id or x_workstation_id != machine_id:
        raise HTTPException(status_code=401, detail="X-Workstation-Id must match machine_id")
    _expire_leases(db)
    owner = "{}:{}:{}".format(machine_id, revit_process_id, session_id)
    candidates = (db.query(CommandRecord)
                  .filter(CommandRecord.machine_id == machine_id, CommandRecord.status == "PENDING")
                  .order_by(CommandRecord.created_at.asc())
                  .with_for_update(skip_locked=True)
                  .limit(20).all())
    for row in candidates:
        routing = row.routing or {}
        if str(routing.get("revit_process_id") or "") != str(revit_process_id):
            continue
        if str(routing.get("session_id") or "") != str(session_id):
            continue
        # Row is claimed in the current transaction; with_for_update protects Postgres workers.
        token = str(uuid.uuid4())
        current = now()
        row.status = "CLAIMED"
        row.claimed_at = current
        row.delivered_at = current
        row.lease_expires_at = current + dt.timedelta(seconds=LEASE_SEC)
        row.lease_token = token
        row.lease_owner = owner
        row.attempts = (row.attempts or 0) + 1
        db.add(row)
        _audit(db, row.command_id, "CLAIMED", owner, {"attempt": row.attempts})
        db.commit()
        return {"commands": [{"command_id": row.command_id, "action": row.action, "routing": row.routing, "lease_token": token, "lease_expires_at": row.lease_expires_at.isoformat(), "payload": {"items": row.items}}]}
    return {"commands": []}

@router.post("/{command_id}/start")
def start_command(command_id: str, body: dict, db: Session = Depends(get_db)):
    cmd = db.get(CommandRecord, command_id)
    if cmd is None:
        raise HTTPException(status_code=404, detail="Unknown command_id")
    if cmd.status in ("SUCCEEDED", "PARTIAL", "FAILED", "DEAD_LETTER", "CANCELLED"):
        return {"status":"already_terminal", "command_status":cmd.status}
    if cmd.status != "CLAIMED" or body.get("lease_token") != cmd.lease_token:
        raise HTTPException(status_code=409, detail="Command is not claimed by this lease")
    cmd.status = "EXECUTING"
    cmd.execution_started_at = now()
    cmd.lease_expires_at = now() + dt.timedelta(seconds=LEASE_SEC)
    _audit(db, command_id, "EXECUTING", body.get("lease_owner") or cmd.lease_owner)
    db.commit()
    return {"status":"ok", "command_id":command_id, "command_status":"EXECUTING", "lease_expires_at":cmd.lease_expires_at.isoformat()}

@router.post("/{command_id}/result")
def post_result(command_id: str, body: CommandResult, db: Session = Depends(get_db)):
    cmd = db.get(CommandRecord, command_id)
    if cmd is None:
        raise HTTPException(status_code=404, detail="Unknown command_id")
    if body.command_id != command_id:
        raise HTTPException(status_code=422, detail="body.command_id must match the URL command_id")
    # Terminal result is durable/idempotent.
    if cmd.status in ("SUCCEEDED", "PARTIAL", "FAILED", "DEAD_LETTER", "CANCELLED"):
        return {"status": "already_terminal", "command_id": command_id, "command_status": cmd.status}
    if not body.lease_token or body.lease_token != cmd.lease_token:
        raise HTTPException(status_code=409, detail="Invalid or missing lease_token")
    if cmd.lease_expires_at and cmd.lease_expires_at < now():
        raise HTTPException(status_code=409, detail="Command lease expired; command will be retried")
    expected = cmd.routing or {}
    supplied = body.routing or {}
    for key in ("machine_id", "revit_process_id", "session_id", "document_id"):
        if expected.get(key) and supplied.get(key) and str(expected.get(key)) != str(supplied.get(key)):
            raise HTTPException(status_code=409, detail="Result routing does not match command routing: {}".format(key))
    status = body.status.upper()
    if status not in ("SUCCESS", "PARTIAL", "FAILED"):
        raise HTTPException(status_code=422, detail="status must be SUCCESS, PARTIAL or FAILED")
    cmd.execution_started_at = cmd.execution_started_at or now()
    cmd.completed_at = now()
    cmd.status = {"SUCCESS":"SUCCEEDED", "PARTIAL":"PARTIAL", "FAILED":"FAILED"}[status]
    cmd.lease_expires_at = None
    cmd.last_error = None if status in ("SUCCESS", "PARTIAL") else str((body.result or {}).get("error") or "Agent reported failure")
    db.add(CommandResultRecord(command_id=command_id, status=status, routing=supplied, result=body.result or {}))
    _audit(db, command_id, "RESULT_" + status, body.lease_owner or cmd.lease_owner, {"attempts": cmd.attempts})
    db.commit()
    return {"status": "ok", "command_id": command_id, "command_status": cmd.status}

@router.post("/{command_id}/cancel")
def cancel_command(command_id: str, db: Session = Depends(get_db)):
    cmd = db.get(CommandRecord, command_id)
    if cmd is None:
        raise HTTPException(status_code=404, detail="Unknown command_id")
    if cmd.status in ("SUCCEEDED", "PARTIAL", "FAILED", "DEAD_LETTER"):
        return {"status":"already_terminal", "command_status":cmd.status}
    cmd.status = "CANCELLED"
    cmd.completed_at = now()
    cmd.lease_expires_at = None
    _audit(db, command_id, "CANCELLED", "api")
    db.commit()
    return {"status":"cancelled", "command_id":command_id}

@router.post("/resolve")
def resolve_target(body: dict, db: Session = Depends(get_db)):
    selector = body.get("target_selector") or {}
    candidates = resolve_candidates(db, selector)
    return {"match_count": len(candidates), "candidates": candidates}

@router.get("/list")
def list_commands(db: Session = Depends(get_db)):
    _expire_leases(db)
    rows = db.query(CommandRecord).order_by(CommandRecord.created_at.desc()).limit(100).all()
    return [{"command_id":r.command_id,"machine_id":r.machine_id,"action":r.action,"status":r.status,"attempts":r.attempts,"lease_expires_at":r.lease_expires_at.isoformat() if r.lease_expires_at else None,"created_at":r.created_at.isoformat() if r.created_at else None} for r in rows]

@router.get("/{command_id}")
def command_detail(command_id: str, db: Session = Depends(get_db)):
    row = db.get(CommandRecord, command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown command_id")
    results = db.query(CommandResultRecord).filter(CommandResultRecord.command_id == command_id).order_by(CommandResultRecord.received_at.desc()).all()
    events = db.query(AuditEventRecord).filter(AuditEventRecord.command_id == command_id).order_by(AuditEventRecord.created_at.asc()).all()
    return {"command_id":row.command_id,"status":row.status,"machine_id":row.machine_id,"routing":row.routing,"action":row.action,"attempts":row.attempts,"max_attempts":row.max_attempts,"created_at":row.created_at.isoformat() if row.created_at else None,"delivered_at":row.delivered_at.isoformat() if row.delivered_at else None,"execution_started_at":row.execution_started_at.isoformat() if row.execution_started_at else None,"completed_at":row.completed_at.isoformat() if row.completed_at else None,"last_error":row.last_error,"lease_owner":row.lease_owner,"lease_expires_at":row.lease_expires_at.isoformat() if row.lease_expires_at else None,"results":[{"status":r.status,"result":r.result,"received_at":r.received_at.isoformat()} for r in results],"audit":[{"event_type":e.event_type,"actor":e.actor,"details":e.details,"created_at":e.created_at.isoformat()} for e in events]}
