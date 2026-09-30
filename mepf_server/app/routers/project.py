# -*- coding: utf-8 -*-
"""
project.py
=============================================================================
Implements:  POST /api/project/ingest-auto

Called by shared_sender.send_combined_payload() (FamilyExtract.extension)
with the full payload_builder output:
  { "meta": {...}, "buildings": [...], "summary": {...}, "project": {...} }

"project" contains (see payload_builder._PROJECT_FIELDS):
  document_title, project_info_guid, revit_build, project_uid,
  document_path, central_path, ... etc.

This is the "Extraction Data (with identity)" arrow -> Central Server
-> Database (extractions table) in your diagram.
=============================================================================
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException
from app.auth import require_api_key
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.db import get_db, ExtractionRecord, DocumentRecord, SystemRecord
from app.orchestrator import payload_manager

router = APIRouter(prefix="/api/project", tags=["project"], dependencies=[Depends(require_api_key)])


@router.post("/ingest-auto")
def ingest_auto(body: dict, db: Session = Depends(get_db)):
    envelope = body.get("data") if isinstance(body.get("data"), dict) else body
    routing = body.get("routing") or {}
    project = envelope.get("project") or {}
    meta = envelope.get("meta") or {}

    project_uid = routing.get("project_uid") or project.get("project_uid")
    document_path = routing.get("document_path") or project.get("document_path")

    # Resolve the extraction to the exact live document when identity metadata
    # is present. project_uid alone is NOT unique when the same project is open
    # on multiple systems, so never silently choose the most recently-seen
    # document in that case.
    agent_id = (routing.get("agent_id") or meta.get("agent_id") or "").strip()
    machine_id = (routing.get("machine_id") or meta.get("machine_id") or "").strip()
    document_id = (routing.get("document_id") or meta.get("document_id") or "").strip()
    revit_instance_id = (routing.get("revit_instance_id") or meta.get("revit_instance_id") or "").strip()
    session_id = (routing.get("session_id") or meta.get("session_id") or "").strip()

    if document_id:
        doc_row = db.get(DocumentRecord, document_id)
        if doc_row is None:
            raise HTTPException(status_code=409, detail="document_id is not registered")
        if project_uid and doc_row.project_uid and doc_row.project_uid != project_uid:
            raise HTTPException(status_code=409, detail="project_uid does not match document_id")
        if machine_id and doc_row.machine_id != machine_id:
            raise HTTPException(status_code=409, detail="machine_id does not own document_id")
        if revit_instance_id and doc_row.revit_instance_id != revit_instance_id:
            raise HTTPException(status_code=409, detail="revit_instance_id does not match document_id")
        if session_id and doc_row.session_id != session_id:
            raise HTTPException(status_code=409, detail="session_id does not match document_id")
        if document_path and doc_row.document_path and doc_row.document_path != document_path:
            raise HTTPException(status_code=409, detail="document_path does not match document_id")
        if agent_id:
            system = db.get(SystemRecord, doc_row.machine_id)
            if system is None or system.agent_id != agent_id:
                raise HTTPException(status_code=409, detail="agent_id does not own document_id")
        if not doc_row.is_online:
            raise HTTPException(status_code=409, detail="document_id is not currently online")
    else:
        # document_id may be absent when FamilyExtract is running without the
        # routing envelope (for example an older extension was left installed).
        # In that case document_path is the strongest available document-level
        # identity and MUST be used. project_uid alone is not sufficient when
        # the same project is open on multiple systems.
        q = db.query(DocumentRecord).filter(DocumentRecord.is_online.is_(True))
        if project_uid:
            q = q.filter(DocumentRecord.project_uid == project_uid)
        if document_path:
            # Windows paths are case-insensitive. Match normalized case so a
            # path casing difference does not create a false "ambiguous / not
            # found" result. Slash normalization is handled by the clients.
            q = q.filter(func.lower(DocumentRecord.document_path) == document_path.lower())
        if machine_id:
            q = q.filter(DocumentRecord.machine_id == machine_id)
        if revit_instance_id:
            q = q.filter(DocumentRecord.revit_instance_id == revit_instance_id)
        if session_id:
            q = q.filter(DocumentRecord.session_id == session_id)
        if agent_id:
            q = q.join(SystemRecord, DocumentRecord.machine_id == SystemRecord.machine_id).filter(SystemRecord.agent_id == agent_id)
        matches = q.all()
        if len(matches) == 1:
            doc_row = matches[0]
        elif len(matches) == 0:
            raise HTTPException(status_code=409, detail=(
                "extraction target document could not be resolved; "
                "no online document matches the supplied project/document identity"
            ))
        else:
            raise HTTPException(status_code=409, detail=(
                "extraction target document is ambiguous; provide document_id "
                "or a unique agent_id + document_path"
            ))

    extraction_id = routing.get("extraction_id") or meta.get("extraction_id") or "EXT-{0}".format(uuid.uuid4())

    row = ExtractionRecord(
        extraction_id=extraction_id,
        machine_id=machine_id or (doc_row.machine_id if doc_row else None),
        agent_id=agent_id or (db.get(SystemRecord, doc_row.machine_id).agent_id if doc_row and db.get(SystemRecord, doc_row.machine_id) else None),
        document_id=(doc_row.document_id if doc_row else document_id),
        project_uid=project_uid,
        payload=body,
    )
    db.add(row)
    db.commit()

    # Bridge the legacy FamilyExtract ingestion path with the server-side
    # payload inbox.  Ingestion itself remains an extraction-record operation;
    # we do NOT turn the large extraction body into a Revit command.  Instead,
    # once the current Revit document has been confirmed online, immediately
    # run one payload-router pass so any JSON already waiting in payloads/inbox
    # can be selected for this live document.  The background watcher remains
    # the normal retry mechanism.
    try:
        payload_scan = payload_manager.tick()
    except Exception as exc:
        # The extraction was already committed successfully. Do not turn a
        # transient payload-router problem into a false ingestion failure.
        payload_scan = {"error": str(exc)[:500]}

    return {
        "status": "received",
        "extraction_id": extraction_id,
        "matched_document_id": doc_row.document_id if doc_row else None,
        "agent_id": agent_id or (db.get(SystemRecord, doc_row.machine_id).agent_id if doc_row and db.get(SystemRecord, doc_row.machine_id) else None),
        "payload_router": payload_scan,
    }
