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

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.db import get_db, ExtractionRecord, DocumentRecord

router = APIRouter(prefix="/api/project", tags=["project"])


@router.post("/ingest-auto")
def ingest_auto(body: dict, db: Session = Depends(get_db)):
    project = body.get("project") or {}
    meta = body.get("meta") or {}

    project_uid = project.get("project_uid")
    document_path = project.get("document_path")

    # Best-effort resolve which registered document this extraction belongs
    # to, by project_uid (falls back to unmatched if not registered yet -
    # the extraction is still stored either way).
    document_id = None
    if project_uid:
        doc_row = (
            db.query(DocumentRecord)
            .filter(DocumentRecord.project_uid == project_uid)
            .order_by(DocumentRecord.last_seen.desc())
            .first()
        )
        if doc_row:
            document_id = doc_row.document_id

    extraction_id = meta.get("extraction_id") or "EXT-{0}".format(uuid.uuid4())

    row = ExtractionRecord(
        extraction_id=extraction_id,
        machine_id=None,
        document_id=document_id,
        project_uid=project_uid,
        payload=body,
    )
    db.add(row)
    db.commit()

    return {"status": "received", "extraction_id": extraction_id, "matched_document_id": document_id}
