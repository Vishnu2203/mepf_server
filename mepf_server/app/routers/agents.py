# -*- coding: utf-8 -*-
"""
agents.py
=============================================================================
Implements:  POST /api/agents/register
             POST /api/agents/heartbeat

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
=============================================================================
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.db import get_db, SystemRecord, DocumentRecord, now

router = APIRouter(prefix="/api/agents", tags=["agents"])


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
    """Debug helper: view the current System Registry."""
    rows = db.query(SystemRecord).all()
    return [
        {"machine_id": r.machine_id, "machine_name": r.machine_name,
         "status": r.status, "last_seen": r.last_seen.isoformat() if r.last_seen else None}
        for r in rows
    ]
