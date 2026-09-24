# -*- coding: utf-8 -*-
import uuid
import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session

from app.auth import require_api_key
from app.models.db import (
    get_db, CommandRecord, CommandResultRecord, AuditEventRecord, IdempotencyRecord, now
)
from app.orchestrator.routing_engine import find_target_document, build_routing_block, RoutingError, resolve_candidates

router = APIRouter(prefix="/api/commands", tags=["commands"], dependencies=[Depends(require_api_key)])
LEASE_SEC = int(__import__('os').environ.get("COMMAND_LEASE_SEC", "300"))
MAX_ATTEMPTS = int(__import__('os').environ.get("COMMAND_MAX_ATTEMPTS", "3"))

class CreateCommand(BaseModel):
    model_config = ConfigDict(extra="allow")
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
    if body.idempotency_key:
        existing = db.get(IdempotencyRecord, body.idempotency_key)
        if existing:
            cmd = db.get(CommandRecord, existing.command_id)
            return {"status": "existing", "command_id": cmd.command_id, "machine_id": cmd.machine_id, "routing": cmd.routing}
    try:
        doc_row = find_target_document(db, body.target_selector)
    except RoutingError as ex:
        raise HTTPException(status_code=409, detail={"code": ex.code, "message": ex.message, "candidates": ex.candidates})
    command_id = "CMD-{0}".format(uuid.uuid4())
    routing = build_routing_block(doc_row)
    row = CommandRecord(command_id=command_id, machine_id=doc_row.machine_id, routing=routing, action=body.action, items=body.items, status="PENDING", max_attempts=body.max_attempts)
    db.add(row)
    if body.idempotency_key:
        db.add(IdempotencyRecord(key=body.idempotency_key, command_id=command_id))
    _audit(db, command_id, "CREATED", "api", {"machine_id": doc_row.machine_id, "action": body.action})
    db.commit()
    return {"status": "routed", "command_id": command_id, "machine_id": doc_row.machine_id, "routing": routing}

@router.get("/next")
def next_command(machine_id: str = Query(default=""), revit_process_id: str = Query(default=""), session_id: str = Query(default=""), db: Session = Depends(get_db)):
    if not machine_id or not revit_process_id or not session_id:
        return {"commands": []}
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
    return {"command_id":row.command_id,"status":row.status,"machine_id":row.machine_id,"routing":row.routing,"action":row.action,"attempts":row.attempts,"max_attempts":row.max_attempts,"lease_owner":row.lease_owner,"lease_expires_at":row.lease_expires_at.isoformat() if row.lease_expires_at else None,"results":[{"status":r.status,"result":r.result,"received_at":r.received_at.isoformat()} for r in results],"audit":[{"event_type":e.event_type,"actor":e.actor,"details":e.details,"created_at":e.created_at.isoformat()} for e in events]}
