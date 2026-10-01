# -*- coding: utf-8 -*-
"""
/api/payloads  -  operator API for the server-side payload folder.

  GET  /api/payloads                      list (filter: ?status=&limit=)
  GET  /api/payloads/routing-map          the alias / file-rule map currently in force
  POST /api/payloads/upload               write a payload into inbox/ (for hosts with no shell access, e.g. Render)
  POST /api/payloads/preview              dry-run: validate + show which document would be selected. Creates nothing.
  POST /api/payloads/scan                 run one watcher pass now
  GET  /api/payloads/{payload_id}         full record + event history + command detail
  POST /api/payloads/{payload_id}/retry   re-queue (force=true needed when elements may already exist)
  POST /api/payloads/{payload_id}/cancel  cancel if not yet claimed by an agent
"""
import os
import json

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from app.auth import require_api_key
from app.models.db import get_db, PayloadRecord, PayloadEventRecord, CommandRecord, SessionLocal
from app.orchestrator import payload_manager as pm

router = APIRouter(prefix="/api/payloads", tags=["payloads"], dependencies=[Depends(require_api_key)])


def _row(p: PayloadRecord, full=False):
    d = {
        "payload_id": p.payload_id, "logical_id": p.logical_id, "file_name": p.file_name, "status": p.status,
        "error_code": p.error_code, "last_error": p.last_error, "item_count": p.item_count, "priority": p.priority,
        "target_source": p.target_source, "target_spec": p.target_spec,
        "selected": {"agent_id": p.selected_agent_id, "machine_id": p.selected_machine_id, "document_id": p.selected_document_id,
                     "revit_instance_id": p.selected_instance_id,
                     "revit_process_id": p.selected_process_id, "session_id": p.selected_session_id},
        "command_id": p.command_id, "attempts": p.attempts, "max_attempts": p.max_attempts,
        "discovered_at": p.discovered_at.isoformat() if p.discovered_at else None,
        "queued_at": p.queued_at.isoformat() if p.queued_at else None,
        "completed_at": p.completed_at.isoformat() if p.completed_at else None,
        "next_attempt_at": p.next_attempt_at.isoformat() if p.next_attempt_at else None,
        "result_summary": p.result_summary,
    }
    if p.status in ("NO_TARGET", "AMBIGUOUS_TARGET") and p.candidates:
        d["candidates"] = p.candidates
    if full:
        d["depends_on"] = p.depends_on
        d["on_uncertain"] = p.on_uncertain
    return d


MAX_UPLOAD_BYTES = int(os.environ.get("MEPF_PAYLOAD_MAX_UPLOAD_BYTES", os.environ.get("COMMAND_MAX_PAYLOAD_BYTES", "5000000")))


def _latest_for_file(db, file_name):
    return (db.query(PayloadRecord)
            .filter(PayloadRecord.file_name == file_name)
            .order_by(PayloadRecord.discovered_at.desc())
            .first())


@router.get("")
def list_payloads(status: str | None = None, limit: int = Query(default=100, le=500), db: Session = Depends(get_db)):
    q = db.query(PayloadRecord)
    if status:
        q = q.filter(PayloadRecord.status == status.upper())
    return [_row(p) for p in q.order_by(PayloadRecord.discovered_at.desc()).limit(limit).all()]


@router.get("/routing-map")
def routing_map():
    return pm.load_routing_map(force=True)


@router.post("/upload")
async def upload(file: UploadFile = File(...)):
    """Upload a real payload.json file and immediately run one routing pass.

    The body is validated as JSON before it is accepted.  The file is written
    atomically for audit/operator visibility, while the parsed payload is also
    stored in PostgreSQL by the payload manager, so execution does not depend
    on Render's ephemeral filesystem surviving a restart.
    """
    name = os.path.basename((file.filename or "").strip())
    if not name or not name.lower().endswith(".json") or name.startswith("."):
        raise HTTPException(status_code=422, detail="file must be a plain *.json filename")

    raw = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail={
            "code": "payload_too_large",
            "max_bytes": MAX_UPLOAD_BYTES,
        })
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except Exception as ex:
        raise HTTPException(status_code=422, detail={
            "code": "invalid_json",
            "message": str(ex),
        })

    pm.ensure_dirs()
    tmp = os.path.join(pm._inbox(), "." + name + ".tmp")
    final = os.path.join(pm._inbox(), name)
    if os.path.exists(final):
        raise HTTPException(status_code=409, detail="a file with this name is already waiting in inbox/")

    # Store the exact uploaded JSON atomically.  The scanner is the single
    # path that creates the durable PayloadRecord, avoiding duplicate rows.
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, final)

    # Do not make the caller wait for Revit.  One synchronous pass gives an
    # immediate routing/validation result; the background watcher continues
    # tracking CLAIMED/EXECUTING/SUCCEEDED/FAILED afterwards.
    pm.tick()

    db = SessionLocal()
    try:
        p = _latest_for_file(db, name)
        if p is None:
            raise HTTPException(status_code=500, detail="payload was uploaded but not discovered by the router")
        response = _row(p, full=True)
    finally:
        db.close()

    response["status_url"] = "/api/payloads/{}".format(response["payload_id"])
    response["execution_is_async"] = True

    if response["status"] in ("INVALID", "DUPLICATE"):
        raise HTTPException(status_code=422 if response["status"] == "INVALID" else 409, detail=response)
    if response["status"] == "NO_TARGET":
        # The payload remains persisted and will be retried automatically if a
        # live agent appears, but this request is NOT reported as accepted for execution.
        raise HTTPException(status_code=409, detail={
            **response,
            "message": "No eligible live Revit document is currently available. The payload is retained and will be retried automatically.",
        })
    return response


@router.post("/preview")
def preview(body: dict, file_name: str = "preview.json", db: Session = Depends(get_db)):
    return pm.preview(db, body, file_name)


@router.post("/scan")
def scan_now():
    return pm.tick()


@router.get("/{payload_id}")
def detail(payload_id: str, db: Session = Depends(get_db)):
    p = db.get(PayloadRecord, payload_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Unknown payload_id")
    events = db.query(PayloadEventRecord).filter(PayloadEventRecord.payload_id == payload_id).order_by(PayloadEventRecord.id.asc()).all()
    out = _row(p, full=True)
    out["events"] = [{"event": e.event, "details": e.details, "at": e.created_at.isoformat()} for e in events]
    if p.command_id:
        c = db.get(CommandRecord, p.command_id)
        out["command"] = {"status": c.status, "attempts": c.attempts, "lease_owner": c.lease_owner, "last_error": c.last_error} if c else None
    return out


@router.post("/{payload_id}/retry")
def retry(payload_id: str, force: bool = False, db: Session = Depends(get_db)):
    p, outcome = pm.retry(db, payload_id, force=force)
    if outcome == "not_found":
        raise HTTPException(status_code=404, detail="Unknown payload_id")
    if outcome != "requeued":
        raise HTTPException(status_code=409, detail={"code": outcome, "status": p.status if p else None})
    return {"status": "requeued", "payload_id": payload_id}


@router.post("/{payload_id}/cancel")
def cancel(payload_id: str, db: Session = Depends(get_db)):
    p, ok = pm.cancel(db, payload_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Unknown payload_id")
    if not ok:
        raise HTTPException(status_code=409, detail={"code": "cannot_cancel", "status": p.status,
                                                     "hint": "already claimed by an agent or already finished"})
    return {"status": "cancelled", "payload_id": payload_id}
