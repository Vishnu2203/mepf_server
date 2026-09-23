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
from app.routers.agents import mark_stale_offline


class RoutingError(Exception):
    def __init__(self, code, message, candidates=None):
        self.code = code
        self.message = message
        self.candidates = candidates or []
        super().__init__(message)


def find_target_document(db: Session, selector: dict) -> DocumentRecord:
    """
    selector may contain any of: document_id, project_uid, document_title,
    document_path, machine_id, revit_version.
    Narrows the online document registry down using whichever fields are
    given. Raises RoutingError if zero or more-than-one match remains.

    Sweeps stale documents/systems to offline first (same rule /api/agents
    /list uses) so a machine whose agent crashed or stopped heartbeating -
    and therefore can never send a fresh heartbeat to flip its old rows
    to is_online=False itself - doesn't linger forever as a false "online"
    match. Left unswept, those stale rows are exactly what produces
    "ambiguous_target" (multiple stale + live rows matching the same
    project_uid/machine_id) or silent routing to a dead agent that will
    never actually create the elements.
    """
    mark_stale_offline(db)
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
    if selector.get("revit_instance_id"):
        q = q.filter(DocumentRecord.revit_instance_id == selector["revit_instance_id"])
    if selector.get("revit_process_id"):
        q = q.filter(DocumentRecord.revit_process_id == selector["revit_process_id"])
    if selector.get("session_id"):
        q = q.filter(DocumentRecord.session_id == selector["session_id"])

    matches = q.all()

    if len(matches) == 0:
        raise RoutingError("no_target_online", "No online document matches the given selector.")
    if len(matches) > 1:
        # Diagnostic detail so the caller can see exactly WHY it's ambiguous
        # instead of guessing - each candidate's full identity is listed.
        candidates = [
            {
                "document_id": m.document_id,
                "machine_id": m.machine_id,
                "project_uid": m.project_uid,
                "document_title": m.document_title,
                "document_path": m.document_path,
                "revit_version": m.revit_version,
            }
            for m in matches
        ]
        raise RoutingError(
            "ambiguous_target",
            "{0} online documents match this selector - narrow the selector "
            "(add document_id, or machine_id + project_uid together). "
            "See 'candidates' for exactly which documents matched.".format(len(matches)),
            candidates=candidates,
        )
    return matches[0]


def resolve_candidates(db: Session, selector: dict) -> list:
    """
    Non-raising version of find_target_document's matching step, for a
    debug/preview endpoint: returns every online document that matches the
    given selector (0, 1, or many) so a caller can see what would happen
    BEFORE sending a real command and hitting a 409.
    """
    mark_stale_offline(db)
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
    if selector.get("revit_instance_id"):
        q = q.filter(DocumentRecord.revit_instance_id == selector["revit_instance_id"])
    if selector.get("revit_process_id"):
        q = q.filter(DocumentRecord.revit_process_id == selector["revit_process_id"])
    if selector.get("session_id"):
        q = q.filter(DocumentRecord.session_id == selector["session_id"])

    return [
        {
            "document_id": m.document_id,
            "machine_id": m.machine_id,
            "project_uid": m.project_uid,
            "document_title": m.document_title,
            "document_path": m.document_path,
            "revit_version": m.revit_version,
            "revit_process_id": m.revit_process_id,
            "session_id": m.session_id,
            "last_seen": m.last_seen.isoformat() if m.last_seen else None,
        }
        for m in q.all()
    ]


def build_routing_block(doc_row: DocumentRecord) -> dict:
    """
    Produces the exact field names smart_target.validate_target() reads:
    project_uid, document_id, document_path, document_title, revit_version,
    machine_id, revit_process_id, session_id.
    """
    return {
        "machine_id": doc_row.machine_id,
        "revit_instance_id": doc_row.revit_instance_id,
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
