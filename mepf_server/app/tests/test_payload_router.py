"""
End-to-end tests for the payload folder router, with simulated Revit agents.
Agents speak exactly the protocol the pyRevit extension speaks:
  POST /api/agents/heartbeat, GET /api/commands/next, POST /{id}/start, POST /{id}/result
Run:  python tests/test_payload_router.py
"""
import os, sys, json, tempfile, shutil, datetime as dt
TMP = tempfile.mkdtemp()
os.environ.update(DATABASE_URL="sqlite:///%s/t.db" % TMP, MEPF_API_KEY="k", MEPF_PAYLOAD_DIR=os.path.join(TMP, "payloads"),
                  MEPF_PAYLOAD_WATCHER="0", MEPF_PAYLOAD_SETTLE_SEC="0", MEPF_PAYLOAD_PARSE_GRACE_SEC="0")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.models import db as dbm
from app.orchestrator import payload_manager as pm

H = {"X-Api-Key": "k"}
PASS = []

def check(cond, msg):
    assert cond, "FAIL: " + msg
    PASS.append(msg); print("   ok  -", msg)

class Agent:
    """One Revit process on one PC, with the identity fields the extension reports."""
    def __init__(self, c, machine, pid, docs, agent_id=None):
        self.c, self.machine, self.pid = c, machine, str(pid)
        self.sess = "S-%s-%s" % (machine, pid)
        self.agent_id = agent_id or "AGENT-" + machine
        self.docs = docs                      # list of (document_id, project_uid, title, path)
        self.h = dict(H, **{"X-Workstation-Id": machine})
    def beat(self):
        d = [{"machine_id": self.machine, "machine_name": self.machine, "revit_process_id": self.pid, "session_id": self.sess,
              "revit_instance_id": "INST-%s-%s" % (self.machine, self.pid), "document_id": i, "project_uid": u,
              "document_title": t, "document_path": p, "revit_version": "2025"} for (i, u, t, p) in self.docs]
        r = self.c.post("/api/agents/heartbeat", headers=self.h, json={"agent_id": self.agent_id, "machine_id": self.machine,
                        "revit_process_id": self.pid, "session_id": self.sess, "documents": d})
        assert r.status_code == 200, r.text
    def poll(self):
        r = self.c.get("/api/commands/next", headers=self.h, params={"machine_id": self.machine, "revit_process_id": self.pid, "session_id": self.sess})
        cs = r.json()["commands"]
        return cs[0] if cs else None
    def start(self, cmd):
        return self.c.post("/api/commands/%s/start" % cmd["command_id"], headers=self.h, json={"lease_token": cmd["lease_token"]})
    def finish(self, cmd, status, result):
        return self.c.post("/api/commands/%s/result" % cmd["command_id"], headers=self.h, json={
            "command_id": cmd["command_id"], "status": status, "routing": cmd["routing"], "result": result, "lease_token": cmd["lease_token"]})
    def run_ok(self, cmd, n):
        self.start(cmd); self.finish(cmd, "SUCCESS", {"committed": True, "total": n, "succeeded": n, "failed": 0, "items": []})

GOOD = lambda **kw: dict({"items": [
    {"placement": "point_family", "node_id": "e1", "family": "VAV", "type": "Std", "level": "Level 1", "x_mm": 0, "y_mm": 0},
    {"placement": "curve_mep", "node_id": "d1", "system": "duct", "level": "Level 1"},
    {"placement": "connect", "from_node": "e1", "to_node": "d1"}]}, **kw)

def drop(name, obj):
    with open(os.path.join(pm._inbox(), name), "w") as f:
        json.dump(obj, f)

def reset():
    with dbm.engine.begin() as conn:
        for t in ("payload_events", "payloads", "audit_events", "idempotency_keys", "command_results", "commands", "documents", "systems"):
            conn.execute(text("DELETE FROM " + t))
    shutil.rmtree(pm.PAYLOAD_DIR, ignore_errors=True); pm.ensure_dirs(); pm._routing_cache.update(mtime=None, data={"aliases": {}, "file_rules": []})
    pm.PENDING_GRACE_SEC, pm.QUEUED_TTL_SEC = 45, 600

def status_of(c, logical):
    rows = [p for p in c.get("/api/payloads", headers=H).json() if p["logical_id"] == logical]
    return rows[0]["status"] if rows else None

def one(c, logical):
    return [p for p in c.get("/api/payloads", headers=H).json() if p["logical_id"] == logical][0]

def write_map(m):
    with open(os.path.join(pm.PAYLOAD_DIR, "routing_map.json"), "w") as f: json.dump(m, f)

def age_command(cmd_id, seconds):
    with dbm.SessionLocal() as s:
        s.query(dbm.CommandRecord).filter_by(command_id=cmd_id).update({"created_at": dbm.now() - dt.timedelta(seconds=seconds)}); s.commit()

with TestClient(app) as c:

    print("\n[T1] 4 machines / 4 payloads / 3 different targeting styles -> each lands on the right document, nothing else")
    reset()
    agents = [Agent(c, "PC%d" % i, 100 + i, [("DOC-%d" % i, "UID-%d" % i, "Model %d" % i, r"D:\m%d.rvt" % i)]) for i in range(1, 5)]
    for a in agents: a.beat()
    write_map({"aliases": {"VAV2": {"agent_id": "AGENT-PC2", "project_uid": "UID-2"}},
               "file_rules": [{"file_glob": "Duct_Level3*.json", "alias": "DUCT3"}, ]})
    m = json.load(open(os.path.join(pm.PAYLOAD_DIR, "routing_map.json"))); m["aliases"]["DUCT3"] = {"document_path": r"D:\m3.rvt"}; write_map(m)
    drop("HVAC_Level1.json", GOOD(target={"selector": {"document_id": "DOC-1"}}))       # explicit selector
    drop("VAV_Level2.json", GOOD(target={"alias": "VAV2"}))                            # alias in map
    drop("Duct_Level3.json", GOOD())                                                    # NO target in file: file rule
    drop("Extra_Level4.json", GOOD(target={"machine_id": "PC4", "project_uid": "UID-4"}))
    st = pm.tick()
    check(st["routed"] == 4, "tick routed all 4 payloads (%s)" % st)
    for logical, machine in (("HVAC_Level1", "PC1"), ("VAV_Level2", "PC2"), ("Duct_Level3", "PC3"), ("Extra_Level4", "PC4")):
        row = one(c, logical)
        check(row["status"] == "QUEUED" and row["selected"]["machine_id"] == machine, "%s -> %s (%s)" % (logical, machine, row["target_source"]))
    for a in agents:
        cmd = a.poll(); check(cmd is not None and cmd["routing"]["machine_id"] == a.machine, "%s received exactly its own command" % a.machine)
        check(a.poll() is None, "%s has nothing else queued (no cross-delivery)" % a.machine)
        a.run_ok(cmd, 3)
    pm.tick()
    check(all(p["status"] == "SUCCEEDED" for p in c.get("/api/payloads", headers=H).json()), "all 4 SUCCEEDED")
    check(sorted(os.listdir(os.path.join(pm.PAYLOAD_DIR, "done"))) == sorted(["HVAC_Level1.json", "VAV_Level2.json", "Duct_Level3.json", "Extra_Level4.json"] + [f + ".status.json" for f in ["HVAC_Level1.json", "VAV_Level2.json", "Duct_Level3.json", "Extra_Level4.json"]]), "files moved to done/ with status sidecars")
    check(os.listdir(pm._inbox()) == [], "inbox is empty")

    print("\n[T2] project_uid only + same central model open on two PCs -> deterministic least-load selection; agent_id still narrows it")
    reset()
    a1 = Agent(c, "PC1", 1, [("DOC-A1", "UID-CENTRAL", "Model_alice", r"C:\alice\m.rvt")]); a2 = Agent(c, "PC2", 2, [("DOC-A2", "UID-CENTRAL", "Model_bob", r"C:\bob\m.rvt")])
    a1.beat(); a2.beat()
    drop("p1.json", GOOD(target={"project_uid": "UID-CENTRAL"})); pm.tick()
    row = one(c, "p1"); check(row["status"] == "QUEUED" and row["selected"]["machine_id"] == "PC1", "two matching machines -> stable machine_id tie-break selects PC1")
    check(a2.poll() is None, "PC2 receives nothing when PC1 wins the deterministic tie-break")
    drop("p2.json", GOOD(target={"project_uid": "UID-CENTRAL", "agent_id": "AGENT-PC2"})); pm.tick()
    row = one(c, "p2"); check(row["status"] == "QUEUED" and row["selected"]["machine_id"] == "PC2", "agent_id narrows the target to PC2")

    print("\n[T2b] no target block -> automatically selects the least-loaded live document deterministically")
    reset()
    a1 = Agent(c, "PC1", 1, [("DOC-A", "UID-A", "A", r"C:\\a.rvt")])
    a2 = Agent(c, "PC2", 2, [("DOC-B", "UID-B", "B", r"C:\\b.rvt")])
    a1.beat(); a2.beat()
    drop("busy.json", GOOD(target={"document_id": "DOC-A"})); pm.tick()
    drop("auto.json", GOOD()); pm.tick()
    row = one(c, "auto")
    check(row["status"] == "QUEUED" and row["selected"]["machine_id"] == "PC2", "automatic selection avoids busy DOC-A and routes to free DOC-B")
    check(row["target_source"] == "automatic", "selection is explicitly recorded as automatic")
    check(a2.poll() is not None, "automatically selected agent receives the command")

    print("\n[T3] no agent online -> NO_TARGET; agent appears later -> routed automatically")
    reset()
    drop("late.json", GOOD(target={"agent_id": "AGENT-PC9"})); pm.tick()
    check(status_of(c, "late") == "NO_TARGET", "NO_TARGET while agent is absent")
    a9 = Agent(c, "PC9", 9, [("DOC-9", "UID-9", "M9", r"C:\9.rvt")]); a9.beat(); pm.tick()
    check(status_of(c, "late") == "QUEUED", "auto-routed once the agent registered")

    print("\n[T4] duplicates: same content under another name, re-dropped after success, and allow_duplicate")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    body = GOOD(target={"document_id": "DOC-1"})
    drop("a.json", body); pm.tick(); cmd = a.poll(); a.run_ok(cmd, 3); pm.tick()
    check(status_of(c, "a") == "SUCCEEDED", "first copy succeeded")
    drop("a_copy.json", body); pm.tick()
    check(status_of(c, "a_copy") == "DUPLICATE" and a.poll() is None, "copy under a new name -> DUPLICATE, no second command")
    check(os.path.exists(os.path.join(pm.PAYLOAD_DIR, "duplicate", "a_copy.json")), "duplicate moved to duplicate/")
    drop("a.json", body); pm.tick(); pm.tick()
    check(len([p for p in c.get("/api/payloads", headers=H).json() if p["logical_id"] == "a" and p["status"] == "DUPLICATE"]) == 1, "re-dropping the finished file creates exactly one DUPLICATE record (no loop)")
    drop("forced.json", dict(body, allow_duplicate=True)); pm.tick()
    check(status_of(c, "forced") == "QUEUED", "allow_duplicate=true bypasses the duplicate guard")

    print("\n[T5] validation catches payloads the Revit handlers would choke on (before anything is sent)")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("q.json", {"target": {"document_id": "DOC-1"}, "items": [{"query": "rooms"}]})
    drop("order.json", {"target": {"document_id": "DOC-1"}, "items": [{"placement": "connect", "from_node": "x", "to_node": "y"}]})
    drop("bad_system.json", {"target": {"document_id": "DOC-1"}, "items": [{"placement": "curve_mep", "system": "steam", "level": "L1"}]})
    with open(os.path.join(pm._inbox(), "broken.json"), "w") as f: f.write('{"items": [')
    pm.tick()
    for n in ("q", "order", "bad_system", "broken"):
        check(status_of(c, n) == "INVALID", "%s -> INVALID: %s" % (n, one(c, n)["last_error"][:90]))
    check(a.poll() is None, "no invalid payload reached the agent")
    check(sorted(x for x in os.listdir(os.path.join(pm.PAYLOAD_DIR, "failed")) if not x.endswith("status.json")) == ["bad_system.json", "broken.json", "order.json", "q.json"], "invalid files moved to failed/")

    print("\n[T6] per-document serialisation + priority: payload_002 waits for payload_001 on the same document")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("payload_001.json", GOOD(target={"document_id": "DOC-1"}, priority=100))
    drop("payload_002.json", GOOD(target={"document_id": "DOC-1"}, items=GOOD()["items"] + [{"placement": "point_family", "node_id": "e2", "family": "VAV", "type": "Std", "level": "Level 1"}]))
    pm.tick()
    check(status_of(c, "payload_001") == "QUEUED" and status_of(c, "payload_002") == "VALIDATED", "001 queued, 002 held back (same document busy)")
    cmd = a.poll(); a.run_ok(cmd, 3); pm.tick()
    check(status_of(c, "payload_001") == "SUCCEEDED" and status_of(c, "payload_002") == "QUEUED", "002 released only after 001 finished")

    print("\n[T7] depends_on: child cancelled if parent fails, released if parent succeeds")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("parent.json", GOOD(target={"document_id": "DOC-1"}))
    drop("child.json", GOOD(target={"document_id": "DOC-1"}, depends_on=["parent"], items=GOOD()["items"][:1]))
    pm.tick(); check(status_of(c, "child") == "VALIDATED", "child waits")
    cmd = a.poll(); a.start(cmd); a.finish(cmd, "FAILED", {"committed": False, "fatal_error": "Family/type not loaded", "items": []}); pm.tick(); pm.tick()
    check(status_of(c, "parent") == "EXECUTION_FAILED" and status_of(c, "child") == "CANCELLED", "parent EXECUTION_FAILED (no blind retry) -> child CANCELLED")

    print("\n[T8] routing error from Revit (nothing created) is retried automatically after backoff; execution error is NOT")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("r.json", GOOD(target={"document_id": "DOC-1"})); pm.tick()
    cmd = a.poll(); a.start(cmd)
    a.finish(cmd, "FAILED", {"committed": False, "fatal_error": "Target Revit document is not currently open", "routing_error": {"code": "target_document_not_open"}, "items": []})
    pm.tick(); row = one(c, "r")
    check(row["status"] == "RETRY" and row["error_code"] == "target_document_not_open" and row["next_attempt_at"], "-> RETRY with backoff (%s)" % row["error_code"])
    with dbm.SessionLocal() as s:
        s.query(dbm.PayloadRecord).update({"next_attempt_at": dbm.now() - dt.timedelta(seconds=1)}); s.commit()
    pm.tick(); check(status_of(c, "r") == "QUEUED" and one(c, "r")["attempts"] == 2, "backoff elapsed -> re-routed as attempt 2")

    print("\n[T9] Revit restarted while command was queued (new pid/session) -> stranded command is cancelled and payload re-routed to the new session")
    reset()
    old = Agent(c, "PC1", 111, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); old.beat()
    drop("s.json", GOOD(target={"document_id": "DOC-1"})); pm.tick(); first = one(c, "s")["command_id"]
    new = Agent(c, "PC1", 222, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); new.beat()        # same doc, new process/session
    check(new.poll() is None, "(current server behaviour) new session cannot claim a command pinned to the dead session")
    pm.PENDING_GRACE_SEC = 0; pm.tick(); pm.tick()
    row = one(c, "s")
    check(row["attempts"] == 1 and row["status"] == "RETRY" and row["error_code"] == "target_session_gone", "session mismatch detected -> old command cancelled, payload RETRY (attempts counts dispatches)")
    with dbm.SessionLocal() as s:
        s.query(dbm.PayloadRecord).update({"next_attempt_at": dbm.now() - dt.timedelta(seconds=1)}); s.commit()
    pm.tick(); cmd = new.poll()
    check(cmd is not None and cmd["command_id"] != first and cmd["routing"]["session_id"] == new.sess, "new session receives a fresh command pinned to it")
    check(c.get("/api/commands/%s" % first, headers=H).json()["status"] == "CANCELLED", "old command is CANCELLED (not left PENDING forever)")

    print("\n[T10] agent dies AFTER /start (Revit may have committed) -> lease expiry does NOT re-queue; payload held as TIMEOUT")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("x.json", GOOD(target={"document_id": "DOC-1"})); pm.tick(); cmd = a.poll(); a.start(cmd); pm.tick()
    check(status_of(c, "x") == "EXECUTING", "payload shows EXECUTING")
    with dbm.SessionLocal() as s:
        s.query(dbm.CommandRecord).update({"lease_expires_at": dbm.now() - dt.timedelta(seconds=5)}); s.commit()
    pm.tick(); pm.tick()
    check(c.get("/api/commands/%s" % cmd["command_id"], headers=H).json()["status"] == "DEAD_LETTER", "command -> DEAD_LETTER (not PENDING)")
    check(status_of(c, "x") == "TIMEOUT" and a.poll() is None, "payload TIMEOUT, nothing re-sent (avoids duplicate elements)")
    r = c.post("/api/payloads/%s/retry" % one(c, "x")["payload_id"], headers=H); check(r.status_code == 409 and r.json()["detail"]["code"] == "elements_may_exist_use_force", "manual retry refused without force")
    r = c.post("/api/payloads/%s/retry?force=true" % one(c, "x")["payload_id"], headers=H); check(r.status_code == 200, "manual retry with force accepted")

    print("\n[T11] agent claims but never starts (Revit closed between poll and start) -> lease expiry re-queues safely")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("y.json", GOOD(target={"document_id": "DOC-1"})); pm.tick(); cmd = a.poll(); pm.tick()
    check(status_of(c, "y") == "CLAIMED", "payload CLAIMED")
    with dbm.SessionLocal() as s:
        s.query(dbm.CommandRecord).update({"lease_expires_at": dbm.now() - dt.timedelta(seconds=5)}); s.commit()
    pm.tick(); cmd2 = a.poll()
    check(cmd2 is not None and cmd2["command_id"] == cmd["command_id"], "same command handed out again (nothing had run)")

    print("\n[T12] PARTIAL commit is terminal + held for review (elements already exist); moved to failed/")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("z.json", GOOD(target={"document_id": "DOC-1"})); pm.tick(); cmd = a.poll(); a.start(cmd)
    a.finish(cmd, "PARTIAL", {"committed": True, "total": 3, "succeeded": 2, "failed": 1, "items": [{"index": 2, "node_id": "c1", "placement": "connect", "ok": False, "error": "no free connector"}]})
    pm.tick(); row = one(c, "z")
    check(row["status"] == "PARTIAL" and row["result_summary"]["failed_items"][0]["node_id"] == "c1", "PARTIAL with failing item listed")
    check(os.path.exists(os.path.join(pm.PAYLOAD_DIR, "failed", "z.json")), "moved to failed/ for review")

    print("\n[T13] server restart: in-flight state is rebuilt from the DB, no double dispatch")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    drop("k.json", GOOD(target={"document_id": "DOC-1"})); pm.tick()
    pm._routing_cache.update(mtime=None, data={"aliases": {}, "file_rules": []})           # "process restarted": all in-memory state gone
    pm.tick(); pm.tick()
    n = c.get("/api/commands/list", headers=H).json()
    check(len(n) == 1, "still exactly one command after restart + extra ticks (%d)" % len(n))

    print("\n[T14] agent offline (heartbeats stopped) -> payload waits (NO_TARGET), does not queue to a dead machine")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    with dbm.SessionLocal() as s:
        s.query(dbm.DocumentRecord).update({"last_seen": dbm.now() - dt.timedelta(seconds=300)}); s.commit()
    drop("o.json", GOOD(target={"document_id": "DOC-1"})); pm.tick()
    check(status_of(c, "o") == "NO_TARGET", "stale registry row is not a valid target")

    print("\n[T15] head-of-line blocking fixed: 25 commands for process A no longer hide process B's command")
    reset()
    A = Agent(c, "PC1", 100, [("DOC-A", "UA", "A", r"C:\a.rvt")]); B = Agent(c, "PC1", 200, [("DOC-B", "UB", "B", r"C:\b.rvt")]); A.beat(); B.beat()
    for i in range(25):
        c.post("/api/commands/create", headers=H, json={"items": [{"placement": "point_family", "i": i}], "target_selector": {"document_id": "DOC-A"}})
    c.post("/api/commands/create", headers=H, json={"items": [{"placement": "point_family"}], "target_selector": {"document_id": "DOC-B"}})
    check(B.poll() is not None, "process B receives its command")

    print("\n[T16] /preview dry-run and /upload endpoint")
    reset()
    a = Agent(c, "PC1", 1, [("DOC-1", "UID-1", "M1", r"C:\1.rvt")]); a.beat()
    r = c.post("/api/payloads/preview", headers=H, json=GOOD(target={"document_id": "DOC-1"})).json()
    check(r["valid"] and r["outcome"] == "TARGET_SELECTED" and len(r["candidates"]) == 1, "preview reports TARGET_SELECTED without creating anything")
    check(c.get("/api/commands/list", headers=H).json() == [], "preview created no command")
    upload_body = json.dumps(GOOD(target={"document_id": "DOC-1"})).encode("utf-8")
    r = c.post("/api/payloads/upload", headers=H, files={"file": ("up.json", upload_body, "application/json")}); check(r.status_code == 200 and r.json()["status"] == "QUEUED", "multipart upload stored and immediately routed")
    check(status_of(c, "up") == "QUEUED", "uploaded file was picked up and routed")

print("\n[T17] routing by human machine_name (case-insensitive) works; unknown name -> NO_TARGET")
with TestClient(app) as c:
    reset()
    hall = Agent(c, "8453725f-uuid", 5, [("DOC-H", "UID-H", "Hall model", r"D:\hall.rvt")]); hall.beat()
    with dbm.SessionLocal() as s:
        s.query(dbm.SystemRecord).filter_by(machine_id="8453725f-uuid").update({"machine_name": "DESIGN-PC-07"}); s.commit()
    drop("n.json", GOOD(target={"machine_name": "design-pc-07", "document_path": r"D:\hall.rvt"})); drop("n2.json", GOOD(target={"machine_name": "NOPE", "project_uid": "UID-H"}))
    pm.tick()
    check(status_of(c, "n") == "QUEUED" and one(c, "n")["selected"]["machine_id"] == "8453725f-uuid", "machine_name resolved to the machine_id")
    check(status_of(c, "n2") == "NO_TARGET", "wrong machine_name never falls back to another machine")

print("\nALL %d CHECKS PASSED" % len(PASS))
shutil.rmtree(TMP, ignore_errors=True)
