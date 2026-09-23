# -*- coding: utf-8 -*-
"""
commands.py
=============================================================================
Implements the three endpoints fetch_worker.py / receiver_verified.py /
endpoint_config.py already expect:

  POST /api/commands/create        <- how a job becomes a routed command
                                       (operator / processing engine calls this)
  GET  /api/commands/next          <- polled every POLL_SEC by fetch_worker.py
  POST /api/commands/{command_id}/result   <- result posted back by fetch_worker.py

--------------------------------------------------------------------------
GET /api/commands/next contract (must match fetch_worker._normalize):
  Called with query params: machine_id, revit_process_id, session_id
  (see fetch_worker._poll_url). Must return EITHER:
    - {} / null / 204-like empty body -> no work
    - {"command_id": "...", "routing": {...}, "payload": {"items": [...]}}
  routing must contain the exact fields smart_target.validate_target reads.
--------------------------------------------------------------------------
POST /api/commands/{command_id}/result contract (sent by
  fetch_worker._send_command_result):
  {
    "command_id": "...",
    "status": "SUCCESS" | "PARTIAL" | "FAILED",
    "routing": {...},
    "result": { ...receiver_verified report... }
  }
=============================================================================
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.models.db import get_db, CommandRecord, CommandResultRecord, now
from app.orchestrator.routing_engine import (
    find_target_document, build_routing_block, RoutingError, resolve_candidates,
)

router = APIRouter(prefix="/api/commands", tags=["commands"])


# ============================================================
# CREATE + ROUTE  (Processing Engine -> Orchestrator -> Command Manager)
# ============================================================
@router.post("/create")
def create_command(body: dict, db: Session = Depends(get_db)):
    """
    body:
    {
      "action": "place_mep_elements",
      "items": [ ...VAV/duct placement objects... ],
      "target_selector": {            # any subset of these identity fields
         "document_id" | "project_uid" | "machine_id" |
         "document_title" | "revit_version": "..."
      }
    }
    Routes immediately if exactly one online document matches; otherwise
    the command stays "pending" with no machine_id (visible via /list) so
    an operator can narrow the selector and re-route.
    """
    action = body.get("action", "place_mep_elements")
    items = body.get("items", [])
    selector = body.get("target_selector") or {}

    command_id = "CMD-{0}".format(uuid.uuid4())

    try:
        doc_row = find_target_document(db, selector)
    except RoutingError as ex:
        raise HTTPException(
            status_code=409,
            detail={"code": ex.code, "message": ex.message, "candidates": ex.candidates},
        )

    routing = build_routing_block(doc_row)

    row = CommandRecord(
        command_id=command_id,
        machine_id=doc_row.machine_id,
        routing=routing,
        action=action,
        items=items,
        status="pending",
    )
    db.add(row)
    db.commit()

    return {"status": "routed", "command_id": command_id, "machine_id": doc_row.machine_id, "routing": routing}


# ============================================================
# POLL  (fetch_worker.py hits this every POLL_SEC)
# ============================================================
@router.get("/next")
def next_command(
    machine_id: str = Query(default=""),
    revit_process_id: str = Query(default=""),
    session_id: str = Query(default=""),
    db: Session = Depends(get_db),
):
    """
    Returns ALL pending commands for this machine in one response, not
    just one. A single Revit process can have multiple documents open
    at once, each with its own pending command targeted via document_id
    inside "routing" - the agent's poll loop only runs once per machine,
    so if we handed back one command at a time, a second document's
    command would sit queued until the NEXT poll cycle. Handing back
    every pending command now lets fetch_worker.py submit all of them
    within the same cycle, so multiple documents get their commands
    together instead of one-per-cycle.

    Shape: {"commands": [ {command_id, routing, payload}, ... ]}
    Empty list -> no work. (Old single-command shape is no longer used;
    fetch_worker.py has been updated to read "commands".)
    """
    if not machine_id:
        return {"commands": []}

    rows = (
        db.query(CommandRecord)
        .filter(CommandRecord.machine_id == machine_id, CommandRecord.status == "pending")
        .order_by(CommandRecord.created_at.asc())
        .all()
    )
    if not rows:
        return {"commands": []}

    out = []
    for row in rows:
        row.status = "delivered"
        row.delivered_at = now()
        out.append({
            "command_id": row.command_id,
            "routing": row.routing,
            "payload": {"items": row.items},
        })
    db.commit()

    return {"commands": out}


# ============================================================
# RESULT  (fetch_worker._send_command_result posts here)
# ============================================================
@router.post("/{command_id}/result")
def post_result(command_id: str, body: dict, db: Session = Depends(get_db)):
    cmd = db.get(CommandRecord, command_id)
    if cmd is None:
        raise HTTPException(status_code=404, detail="Unknown command_id")

    status = body.get("status", "FAILED")
    result = body.get("result", {})
    routing = body.get("routing", {})

    cmd.status = "committed" if status == "SUCCESS" else ("committed" if status == "PARTIAL" else "failed")

    db.add(CommandResultRecord(
        command_id=command_id,
        status=status,
        routing=routing,
        result=result,
    ))
    db.commit()

    return {"status": "ok"}


# ============================================================
# RESOLVE  (preview matching BEFORE sending a real command - use this to
#           debug 409s instead of guessing. Same matching rules as /create,
#           but never raises: always returns exactly what would happen.)
# ============================================================
@router.post("/resolve")
def resolve_target(body: dict, db: Session = Depends(get_db)):
    """
    body: { "target_selector": { ...same fields as /create... } }

    Returns:
      {"match_count": 0, "candidates": []}                -> would 409 no_target_online
      {"match_count": 1, "candidates": [ {...one doc...} ]} -> would route cleanly
      {"match_count": N, "candidates": [ {...N docs...} ]}  -> would 409 ambiguous_target,
                                                                here's exactly why
    Call this with your intended target_selector before POSTing the real
    payload to /api/commands/create so you can see which machine/project/
    document it will hit (or why it's ambiguous) without creating a command.
    """
    selector = body.get("target_selector") or {}
    candidates = resolve_candidates(db, selector)
    return {"match_count": len(candidates), "candidates": candidates}


# ============================================================
# DEBUG / MONITORING
# ============================================================
@router.get("/list")
def list_commands(db: Session = Depends(get_db)):
    rows = db.query(CommandRecord).order_by(CommandRecord.created_at.desc()).limit(100).all()
    return [
        {
            "command_id": r.command_id,
            "machine_id": r.machine_id,
            "action": r.action,
            "status": r.status,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
