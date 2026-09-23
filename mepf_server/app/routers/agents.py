# -*- coding: utf-8 -*-
"""
agents.py
=============================================================================
Implements:  POST /api/agents/register
             POST /api/agents/heartbeat
             GET  /api/agents/list

These are called every 30s by agent_registry.py on every Local Agent
(pyRevit extension). This is the "System Registry" + "Document Registry" 
in your diagram — built dynamically, exactly as you decided:
  Local Agent starts -> auto-registers itself + every open document.

Body shape sent by agent_registry.py's _body():
{
  "machine_id": "...",
  "revit_process_id": "...",
  "session_id": "...",
  "timestamp": "...",
  "documents": [
     {  # from routing_identity.document_descriptor()
       "machine_id", "machine_name", "revit_version", "revit_build",
       "revit_process_id", "revit_instance_id", "project_uid",
       "document_id", "document_path", "document_title", "session_id"
     }, ...
  ]
}

/list behavior (fix applied):
  A machine only appears in /api/agents/list while its agent is actively
  sending heartbeats (every 30s). If a machine hasn't been heard from in
  OFFLINE_THRESHOLD_SEC (i.e. Revit was closed / agent stopped on THAT
  machine), it is dropped from the list. Other machines that are still
  sending heartbeats are completely unaffected.
=============================================================================
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.db import get_db, SystemRecord, DocumentRecord, now

router = APIRouter(prefix="/api/agents", tags=["agents"])

# Agent sends a heartbeat every 30s (see agent_registry.py / config
# heartbeat_interval_sec). If a machine hasn't been heard from within this
# many seconds, treat it as "Revit closed / agent stopped" for that
# specific machine and drop it from /list. 3x the heartbeat interval gives
# a small buffer for one missed beat or a slow request, without other
# online machines being affected.
OFFLINE_THRESHOLD_SEC = 90


def _upsert_system(db: Session, machine_id: str, machine_name: str, status: str):
    sys_row = db.get(SystemRecord, machine_id)
    if sys_row is None:
        sys_row = SystemRecord(machine_id=machine_id, machine_name=machine_name, status=status)
        db.add(sys_row)
    else:
        sys_row.machine_name = machine_name or sys_row.machine_name
        sys_row.status = status
        sys_row.last_seen = now()
    return sys_row


def _upsert_documents(db: Session, machine_id: str, documents: list):
    seen_ids = set()
    for doc in documents or []:
        document_id = doc.get("document_id")
        if not document_id:
            continue
        seen_ids.add(document_id)
        row = db.get(DocumentRecord, document_id)
        if row is None:
            row = DocumentRecord(document_id=document_id)
            db.add(row)
        row.machine_id = machine_id
        row.revit_instance_id = doc.get("revit_instance_id")
        row.revit_process_id = doc.get("revit_process_id")
        row.session_id = doc.get("session_id")
        row.project_uid = doc.get("project_uid")
        row.document_title = doc.get("document_title")
        row.document_path = doc.get("document_path")
        row.revit_version = doc.get("revit_version")
        row.is_online = True
        row.last_seen = now()

    # Any document previously registered for this machine but NOT present
    # in this register/heartbeat call is now closed -> mark offline.
    existing = db.query(DocumentRecord).filter(DocumentRecord.machine_id == machine_id).all()
    for row in existing:
        if row.document_id not in seen_ids:
            row.is_online = False


@router.post("/register")
def register_agent(body: dict, db: Session = Depends(get_db)):
    machine_id = body.get("machine_id", "")
    documents = body.get("documents", [])
    machine_name = documents[0].get("machine_name") if documents else None

    _upsert_system(db, machine_id, machine_name, status="online")
    _upsert_documents(db, machine_id, documents)
    db.commit()
    return {"status": "registered", "machine_id": machine_id, "documents_seen": len(documents)}


@router.post("/heartbeat")
def heartbeat_agent(body: dict, db: Session = Depends(get_db)):
    machine_id = body.get("machine_id", "")
    documents = body.get("documents", [])
    machine_name = documents[0].get("machine_name") if documents else None

    _upsert_system(db, machine_id, machine_name, status="online")
    _upsert_documents(db, machine_id, documents)
    db.commit()
    return {"status": "ok", "machine_id": machine_id}


@router.get("/list")
def list_agents(db: Session = Depends(get_db)):
    """
    Returns only machines that are CURRENTLY online, i.e. whose agent has
    sent a heartbeat within OFFLINE_THRESHOLD_SEC. Each machine appears as
    ONE row, with all of its currently-open Revit instances/documents
    nested under "documents" — so multiple Revit instances on the same PC
    are combined into a single entry instead of duplicating the machine
    on every refresh.

    - Revit open (one or more instances) + agent heartbeating on a
      machine -> that machine appears once, documents array has one
      entry per open instance.
    - Revit closed on a machine (heartbeats stop) -> that machine alone
      drops out once it goes stale. Every other machine still heart-
      beating stays in the list untouched.
    - If a machine is online but just closed one of several open
      documents, that document alone disappears from its "documents"
      array (is_online flips False in _upsert_documents) while the
      machine row itself stays, and its other open documents remain.
    """
    rows = db.query(SystemRecord).all()
    current_time = now()
    result = []
    for r in rows:
        if r.last_seen is None:
            continue
        seconds_since = (current_time - r.last_seen).total_seconds()
        if seconds_since > OFFLINE_THRESHOLD_SEC:
            # Stale -> agent stopped heartbeating (Revit closed on this
            # machine). Skip it; don't touch any other machine's row.
            continue

        docs = (
            db.query(DocumentRecord)
            .filter(DocumentRecord.machine_id == r.machine_id, DocumentRecord.is_online == True)
            .all()
        )

        result.append({
            "machine_id": r.machine_id,
            "machine_name": r.machine_name,
            "status": "online",
            "last_seen": r.last_seen.isoformat(),
            "documents": [
                {
                    "document_id": d.document_id,
                    "revit_instance_id": d.revit_instance_id,
                    "revit_process_id": d.revit_process_id,
                    "session_id": d.session_id,
                    "project_uid": d.project_uid,
                    "document_title": d.document_title,
                    "document_path": d.document_path,
                    "revit_version": d.revit_version,
                    "last_seen": d.last_seen.isoformat() if d.last_seen else None,
                }
                for d in docs
            ],
        })
    return result
