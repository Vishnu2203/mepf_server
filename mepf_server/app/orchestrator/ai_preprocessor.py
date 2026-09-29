# -*- coding: utf-8 -*-
"""
ai_preprocessor.py
==============================================================================
AI preprocessing layer: JSON project payload -> Revit room analysis ->
LLM-based MEP decision -> structured place_mep_elements command.

Workflow (see AI_PREPROCESSING_DESIGN.md for the full design/rationale):

  1. Create a "get_rooms" command against the target document and wait for
     the Revit agent to execute it (read-only; no elements are touched).
  2. Send the returned room data + the caller's project payload to Claude,
     constrained to a strict JSON schema (see DECISION_SYSTEM_PROMPT).
  3. Validate every decision against real data: every room_id must exist in
     step 1's result, every family/type must come from an allowed catalog,
     every quantity must be within configured bounds. Anything that fails
     validation is DROPPED with a reason - never silently "fixed" or
     guessed at.
  4. Convert the surviving decisions into the same "items" schema the
     existing place_mep_elements command already understands (point_family),
     and create that command through the normal, already-audited path.

This module reuses create_command()/CreateCommand from commands.py rather
than re-implementing routing, idempotency, leasing, or auditing.
==============================================================================
"""
import os
import time
import json
import math
import logging

from app.models.db import CommandRecord, CommandResultRecord
from app.routers.commands import CreateCommand, create_command, _describe_failure

logger = logging.getLogger("ai_preprocessor")

ANTHROPIC_MODEL = os.environ.get("MEPF_AI_MODEL", "claude-sonnet-4-6")
ROOM_QUERY_TIMEOUT_SEC = int(os.environ.get("MEPF_ROOM_QUERY_TIMEOUT_SEC", "60"))
DECISION_POLL_SEC = 2

# ---------------------------------------------------------------------------
# Family/type catalog - THE ONLY families the AI is allowed to specify.
# This is a guardrail, not a convenience list: an LLM output naming anything
# outside this catalog is rejected in _validate_decisions() and never
# reaches Revit. Replace with your project's actual loaded family/type
# names (they must match handler_point_family.py's _find_symbol lookup).
# ---------------------------------------------------------------------------
ALLOWED_CATALOG = {
    # room department/type (lowercased) -> allowed (family, type) pairs
    "office": [("Supply Air Diffuser", "24x24 Face"), ("Return Air Grille", "24x24")],
    "corridor": [("Supply Air Diffuser", "24x24 Face")],
    "restroom": [("Exhaust Fan Grille", "12x12")],
    "default": [("Supply Air Diffuser", "24x24 Face")],
}

MAX_ELEMENTS_PER_ROOM = int(os.environ.get("MEPF_MAX_ELEMENTS_PER_ROOM", "6"))


class AIPreprocessingError(Exception):
    def __init__(self, code, message, detail=None):
        self.code = code
        self.message = message
        self.detail = detail or {}
        super().__init__(message)


# ---------------------------------------------------------------------------
# STEP 1 - Room extraction
# ---------------------------------------------------------------------------

def _wait_for_command(db, command_id, timeout_sec):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        row = db.get(CommandRecord, command_id)
        db.refresh(row)
        status = row.status
        if status in ("SUCCEEDED", "PARTIAL", "FAILED", "DEAD_LETTER", "CANCELLED"):
            results = (
                db.query(CommandResultRecord)
                .filter(CommandResultRecord.command_id == command_id)
                .order_by(CommandResultRecord.received_at.desc())
                .all()
            )
            result = results[0].result if results else {}
            return status, result
        time.sleep(DECISION_POLL_SEC)
    return "TIMEOUT", {}


def extract_rooms(db, target_selector):
    """Runs a get_rooms command against the target and returns
    (usable_rooms, unbounded_rooms). Raises AIPreprocessingError on anything
    short of a clean room set - there is no reasonable way to place MEP
    elements against partial/failed room data, so this fails loudly rather
    than guessing.
    """
    body = CreateCommand(
        action="get_rooms",
        items=[{"query": "rooms"}],  # opaque placeholder; get_rooms ignores items
        target_selector=target_selector,
        max_attempts=2,
    )
    created = create_command(body, db)
    command_id = created["command_id"]

    status, result = _wait_for_command(db, command_id, ROOM_QUERY_TIMEOUT_SEC)
    if status != "SUCCEEDED":
        raise AIPreprocessingError(
            "room_query_failed",
            "Could not read rooms from the target document: {}".format(
                _describe_failure(result, status)
            ),
            {"command_id": command_id, "status": status, "result": result},
        )

    rooms = result.get("rooms") or []
    if not rooms:
        raise AIPreprocessingError(
            "no_rooms_found",
            "Target document has no placed rooms. Place rooms in Revit "
            "(Architecture > Room) before running AI preprocessing.",
            {"command_id": command_id},
        )

    unbounded = [r for r in rooms if r.get("unbounded") or r.get("error")]
    usable = [r for r in rooms if not r.get("unbounded") and not r.get("error")]
    if not usable:
        raise AIPreprocessingError(
            "all_rooms_unbounded",
            "Every room in the target document is unbounded or unreadable - "
            "fix room boundaries in Revit before running AI preprocessing.",
            {"unbounded_count": len(unbounded)},
        )

    return usable, unbounded


# ---------------------------------------------------------------------------
# STEP 2 - LLM decision
# ---------------------------------------------------------------------------

DECISION_SYSTEM_PROMPT = """You design HVAC/MEP element placement for a \
Revit model. You are given:
  1. project_requirements - the caller's design intent/constraints (JSON).
  2. rooms - the ACTUAL rooms in the target Revit model, each with a real \
room_id, name, number, department/type, area, and a centroid location \
already computed in meters.
  3. allowed_catalog - the ONLY families/types you may specify, keyed by \
room department (a "default" entry applies to any department not listed).

For each room, decide which MEP elements (if any) belong in it. Only use \
room_id values that appear in the rooms list. Only use family/type pairs \
from allowed_catalog for that room's department (or "default").

Respond with ONLY a JSON object in this exact shape - no prose, no markdown \
fences:

{
  "decisions": [
    {
      "room_id": "<must match a room_id from the input>",
      "family": "<must be an allowed family for this room's department>",
      "type": "<must be an allowed type for that family>",
      "quantity": <integer, 1-6>,
      "reasoning": "<one sentence: why this element, this room, this count>"
    }
  ]
}
"""


def _call_claude(rooms, project_requirements):
    import anthropic  # imported lazily so this module can be imported even
                       # where the anthropic package/key isn't configured
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

    user_content = json.dumps({
        "project_requirements": project_requirements,
        "rooms": [
            {
                "room_id": r["room_id"],
                "name": r.get("name"),
                "number": r.get("number"),
                "department": r.get("department"),
                "area_m2": r.get("area_m2"),
                "location": r.get("location"),
            }
            for r in rooms
        ],
        "allowed_catalog": ALLOWED_CATALOG,
    })

    response = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=4000,
        system=DECISION_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    text = "".join(
        b.text for b in response.content if getattr(b, "type", "") == "text"
    ).strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    try:
        parsed = json.loads(text)
    except Exception as ex:
        raise AIPreprocessingError(
            "llm_output_not_json",
            "AI decision step did not return valid JSON: {}".format(ex),
            {"raw_text": text[:2000]},
        )
    return parsed.get("decisions") or []


# ---------------------------------------------------------------------------
# STEP 3 - Validation / guardrails
# ---------------------------------------------------------------------------

def _validate_decisions(decisions, rooms_by_id):
    """Every rule here REJECTS a bad decision (dropping it, with a logged
    reason) instead of trying to silently coerce it into something valid.
    Rejections are returned to the caller so a human can see exactly what
    the AI proposed and why any of it didn't get built."""
    accepted = []
    rejected = []
    counts_by_room = {}

    for d in decisions:
        room_id = str(d.get("room_id") or "")
        room = rooms_by_id.get(room_id)
        if room is None:
            rejected.append({"decision": d, "reason": "unknown_room_id"})
            continue

        family = d.get("family")
        type_name = d.get("type")
        dept = (room.get("department") or "default").strip().lower()
        catalog = ALLOWED_CATALOG.get(dept) or ALLOWED_CATALOG["default"]
        if (family, type_name) not in catalog:
            rejected.append({"decision": d, "reason": "family_type_not_in_catalog"})
            continue

        try:
            qty = int(d.get("quantity") or 0)
        except Exception:
            rejected.append({"decision": d, "reason": "quantity_not_an_integer"})
            continue
        if qty < 1:
            rejected.append({"decision": d, "reason": "quantity_below_minimum"})
            continue

        so_far = counts_by_room.get(room_id, 0)
        if so_far + qty > MAX_ELEMENTS_PER_ROOM:
            rejected.append({"decision": d, "reason": "exceeds_room_element_cap"})
            continue

        counts_by_room[room_id] = so_far + qty
        merged = dict(d)
        merged["quantity"] = qty
        merged["room"] = room
        accepted.append(merged)

    return accepted, rejected


# ---------------------------------------------------------------------------
# STEP 4 - Placement (centroid + simple grid spread) and item assembly
# ---------------------------------------------------------------------------

def _polygon_centroid(boundary_m):
    """Shoelace-formula centroid of the room's outer boundary polygon.
    Falls back to the plain average of vertices for a degenerate polygon
    (near-zero signed area, e.g. a boundary that didn't close cleanly)."""
    if not boundary_m or len(boundary_m) < 3:
        return None
    area2 = 0.0
    cx = 0.0
    cy = 0.0
    n = len(boundary_m)
    for i in range(n):
        x0, y0 = boundary_m[i]
        x1, y1 = boundary_m[(i + 1) % n]
        cross = x0 * y1 - x1 * y0
        area2 += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    if abs(area2) < 1e-9:
        xs = [p[0] for p in boundary_m]
        ys = [p[1] for p in boundary_m]
        return sum(xs) / len(xs), sum(ys) / len(ys)
    area2 *= 3.0
    return cx / area2, cy / area2


def _grid_offsets_m(count, spacing_m=1.5):
    """Centered grid of offsets around (0,0) so N elements in one room don't
    overlap. Good enough for diffusers/grilles/sensors; swap in a proper
    room-fitting layout for anything more demanding."""
    if count <= 1:
        return [(0.0, 0.0)]
    cols = int(math.ceil(math.sqrt(count)))
    rows = int(math.ceil(count / float(cols)))
    offsets = []
    for r in range(rows):
        for c in range(cols):
            if len(offsets) >= count:
                break
            offsets.append((
                (c - (cols - 1) / 2.0) * spacing_m,
                (r - (rows - 1) / 2.0) * spacing_m,
            ))
    return offsets


def _build_items(accepted_decisions):
    items = []
    node_seq = 0
    for d in accepted_decisions:
        room = d["room"]
        centroid = _polygon_centroid(room.get("boundary")) or (
            (room.get("location") or {}).get("x_m", 0.0),
            (room.get("location") or {}).get("y_m", 0.0),
        )
        z_m = (room.get("location") or {}).get("z_m", 0.0)
        for dx_m, dy_m in _grid_offsets_m(d["quantity"]):
            node_seq += 1
            items.append({
                "node_id": "ai-{0:04d}".format(node_seq),
                "placement": "point_family",
                "family_name": d["family"],
                "type_name": d["type"],
                "level_name": room.get("level"),
                "location_mm": [
                    (centroid[0] + dx_m) * 1000.0,
                    (centroid[1] + dy_m) * 1000.0,
                    z_m * 1000.0,
                ],
                "meta": {
                    "room_id": room["room_id"],
                    "room_name": room.get("name"),
                    "room_number": room.get("number"),
                    "ai_reasoning": d.get("reasoning"),
                },
            })
    return items


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_and_place(db, project_requirements, target_selector, max_attempts=3):
    """Full pipeline: rooms -> AI decision -> validation -> placement command.

    Returns a report describing every stage, so the caller (the /api/ai
    router, or a script) can see exactly what was found, decided, accepted,
    rejected, and ultimately queued for Revit - never a black box.
    """
    usable_rooms, unbounded_rooms = extract_rooms(db, target_selector)
    rooms_by_id = {r["room_id"]: r for r in usable_rooms}

    raw_decisions = _call_claude(usable_rooms, project_requirements)
    accepted, rejected = _validate_decisions(raw_decisions, rooms_by_id)

    if not accepted:
        raise AIPreprocessingError(
            "no_valid_decisions",
            "AI proposed 0 valid element placements after validation.",
            {"raw_decisions": raw_decisions, "rejected": rejected},
        )

    items = _build_items(accepted)

    body = CreateCommand(
        action="place_mep_elements",
        items=items,
        target_selector=target_selector,
        max_attempts=max_attempts,
    )
    created = create_command(body, db)

    return {
        "status": "queued",
        "command_id": created["command_id"],
        "routing_status": created.get("routing_status"),
        "rooms_found": len(usable_rooms) + len(unbounded_rooms),
        "rooms_usable": len(usable_rooms),
        "rooms_unbounded_skipped": len(unbounded_rooms),
        "decisions_proposed": len(raw_decisions),
        "decisions_accepted": len(accepted),
        "decisions_rejected": rejected,
        "items_generated": len(items),
    }
