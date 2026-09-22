# -*- coding: utf-8 -*-
"""
routing_engine.py
=============================================================================
"Routing Engine" from your diagram:
    document_id -> Revit instance -> system -> connection

Given a target selector (whatever identity fields the caller knows -
document_id, project_uid, machine_id, document_title...), find the single
best-matching online DocumentRecord, then build the exact `routing` block
that smart_target.validate_target() on the agent side checks field-by-field.
=============================================================================
"""
from sqlalchemy.orm import Session

from app.models.db import DocumentRecord, SystemRecord


class RoutingError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(message)


def find_target_document(db: Session, selector: dict) -> DocumentRecord:
    """
    selector may contain any of: document_id, project_uid, document_title,
    document_path, machine_id, revit_version.
    Narrows the online document registry down using whichever fields are
    given. Raises RoutingError if zero or more-than-one match remains.
    """
    q = db.query(DocumentRecord).filter(DocumentRecord.is_online.is_(True))

    if selector.get("document_id"):
        q = q.filter(DocumentRecord.document_id == selector["document_id"])
    if selector.get("project_uid"):
        q = q.filter(DocumentRecord.project_uid == selector["project_uid"])
    if selector.get("machine_id"):
        q = q.filter(DocumentRecord.machine_id == selector["machine_id"])
    if selector.get("document_title"):
        q = q.filter(DocumentRecord.document_title == selector["document_title"])
    if selector.get("revit_version"):
        q = q.filter(DocumentRecord.revit_version == selector["revit_version"])

    matches = q.all()

    if len(matches) == 0:
        raise RoutingError("no_target_online", "No online document matches the given selector.")
    if len(matches) > 1:
        raise RoutingError(
            "ambiguous_target",
            "{0} online documents match this selector - narrow the selector "
            "(e.g. add document_id or project_uid).".format(len(matches)),
        )
    return matches[0]


def build_routing_block(doc_row: DocumentRecord) -> dict:
    """
    Produces the exact field names smart_target.validate_target() reads:
    project_uid, document_id, document_path, document_title, revit_version,
    machine_id, revit_process_id, session_id.
    """
    return {
        "machine_id": doc_row.machine_id,
        "revit_process_id": doc_row.revit_process_id,
        "session_id": doc_row.session_id,
        "project_uid": doc_row.project_uid,
        "document_id": doc_row.document_id,
        "document_path": doc_row.document_path,
        "document_title": doc_row.document_title,
        "revit_version": doc_row.revit_version,
        # rebind_allowed left False by default: strict process/session match.
        # Set True explicitly by a caller that knows the agent restarted.
        "rebind_allowed": False,
    }


def system_status(db: Session, machine_id: str) -> str:
    row = db.get(SystemRecord, machine_id)
    return row.status if row else "unknown"
