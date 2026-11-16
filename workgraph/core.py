"""Shared, transactional task graph backed by embedded Relate."""
from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
import fcntl
import math
from pathlib import Path
import threading
import time
import uuid

import relate

OPEN, CLAIMED, DONE, FAILED = "open", "claimed", "done", "failed"


def _atomic(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        with self._session():
            return method(self, *args, **kwargs)
    return call


class _Rows:
    """Materialized results survive closing the operation's connection."""
    def __init__(self, rows):
        self._rows = iter(rows)

    def fetchone(self):
        return next(self._rows, None)

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return self._rows


class _Database:
    """Keep the existing wg.db.execute(...).fetch*() escape hatch usable."""
    def __init__(self, graph):
        self.graph = graph

    def execute(self, query, parameters=None):
        with self.graph._session():
            return _Rows(self.graph._connection.execute(query, parameters or {}).fetchall())

    def close(self):
        self.graph.close()


class WorkGraph:
    """One shared store; short transactions serialize local processes.

    Relate holds an exclusive writer lock for the connection lifetime. Open
    only inside an operation, under flock, so handlers never hold that lock.
    The sibling .lock file contains no state and must not be unlinked while
    clients run. SIGKILL releases its OS lock automatically.
    """
    def __init__(self, path: str = "workgraph.rlf"):
        self.path = str(Path(path).resolve()) if path != ":memory:" else path
        self._lock = threading.RLock()
        self._connection = None
        self._depth = 0
        self._closed = False
        self.db = _Database(self)
        with self._session():
            pass

    @contextmanager
    def _session(self):
        with self._lock:
            if self._closed:
                raise RuntimeError("WorkGraph is closed")
            if self._depth:
                self._depth += 1
                try:
                    yield
                finally:
                    self._depth -= 1
                return
            lock = None
            try:
                if self.path != ":memory:":
                    lock = open(self.path + ".lock", "a+b")
                    fcntl.flock(lock, fcntl.LOCK_EX)
                if self._connection is None:
                    self._connection = relate.connect(self.path)
                    self._connection.autocommit = False
                self._depth = 1
                try:
                    yield
                    self._connection.commit()
                except BaseException:
                    self._connection.rollback()
                    raise
                finally:
                    self._depth = 0
            finally:
                try:
                    if self.path != ":memory:" and self._connection is not None:
                        self._connection.close()
                        self._connection = None
                finally:
                    if lock is not None:
                        lock.close()

    @_atomic
    def add_task(self, title: str, task_id: str | None = None,
                 blocked_by: list[str] | None = None) -> str:
        task_id = task_id or "t-" + uuid.uuid4().hex[:8]
        if not title.strip() or self.get_task(task_id):
            raise ValueError("Task needs a title and a unique id")
        for dep in blocked_by or []:
            if not self.get_task(dep):
                raise ValueError(f"Unknown dependency: {dep}")
        self.db.execute(
            "CREATE (:Task {id: $id, title: $title, status: 'open',"
            " claimed_by: '', claimed_at: 0.0, lease_expires: 0.0, lease_token: '',"
            " result: '', created_at: $now, updated_at: $now})",
            {"id": task_id, "title": title, "now": time.time()})
        for dep in dict.fromkeys(blocked_by or []):
            self.db.execute(
                "MATCH (a:Task {id: $id}), (b:Task {id: $dep})"
                " CREATE (a)-[:BLOCKED_BY]->(b)", {"id": task_id, "dep": dep})
        self._event("task_added", None, task_id, title)
        return task_id

    @_atomic
    def get_task(self, task_id: str) -> dict | None:
        row = self.db.execute("MATCH (t:Task {id: $id}) RETURN t", {"id": task_id}).fetchone()
        return row.to_dict()["t"]["properties"] if row else None

    @_atomic
    def claimable(self) -> list[dict]:
        """Open tasks with no live lease and no unfinished dependencies."""
        return [r.to_dict()["t"]["properties"] for r in self.db.execute(
            "MATCH (t:Task {status: 'open'})"
            " WHERE coalesce(t.lease_expires, 0) <= $now AND NOT EXISTS {"
            " MATCH (t)-[:BLOCKED_BY]->(d:Task) WHERE d.status <> 'done'"
            " } RETURN t ORDER BY t.created_at, t.id", {"now": time.time()}).fetchall()]

    @_atomic
    def claim(self, agent: str, task_id: str, claim_ttl: float = 45.0) -> bool:
        """Atomically claim ready work and record its lease and event."""
        if not agent.strip() or not math.isfinite(claim_ttl) or claim_ttl <= 0:
            raise ValueError("Agent and positive finite claim_ttl required")
        if not any(t["id"] == task_id for t in self.claimable()):
            return False
        if not self.db.execute("MATCH (a:Agent {name: $name}) RETURN a", {"name": agent}).fetchone():
            self.register_agent(agent)
        task = self.get_task(task_id)
        now = time.time()
        self.db.execute(
            "MATCH (t:Task {id: $id}) SET t.status = 'claimed', t.claimed_by = $agent,"
            " t.claimed_at = $now, t.updated_at = $now, t.lease_expires = $expires,"
            " t.lease_token = $token",
            {"id": task_id, "agent": agent, "now": now,
             "expires": now + claim_ttl, "token": uuid.uuid4().hex})
        self.db.execute(
            "MATCH (a:Agent {name: $agent}), (t:Task {id: $id})"
            " CREATE (a)-[:CLAIMED]->(t)", {"agent": agent, "id": task_id})
        self._event("reclaimed" if task.get("orphaned") else "claimed", agent, task_id, "")
        self.db.execute("MATCH (t:Task {id: $id}) SET t.orphaned = false", {"id": task_id})
        return True

    def _owns(self, agent, task_id, lease_token):
        task = self.get_task(task_id)
        return (task and task["status"] == CLAIMED and task["claimed_by"] == agent
                and task.get("lease_expires", 0) > time.time()
                and (lease_token is None or task.get("lease_token") == lease_token))

    def _release(self, task_id):
        self.db.execute("MATCH (:Agent)-[r:CLAIMED]->(:Task {id: $id}) DELETE r", {"id": task_id})
        self.db.execute("MATCH (t:Task {id: $id}) SET t.lease_expires = 0.0, t.lease_token = ''",
                        {"id": task_id})

    @_atomic
    def complete(self, agent: str, task_id: str, result: str = "", *, lease_token: str | None = None) -> None:
        if not self._owns(agent, task_id, lease_token):
            return
        self.db.execute(
            "MATCH (t:Task {id: $id}) SET t.status = 'done', t.result = $result, t.updated_at = $now",
            {"id": task_id, "result": result, "now": time.time()})
        self._release(task_id)
        self._event("completed", agent, task_id, result)

    @_atomic
    def fail(self, agent: str, task_id: str, reason: str, *, lease_token: str | None = None) -> None:
        """Release owned work back to open for another attempt."""
        if not self._owns(agent, task_id, lease_token):
            return
        self.db.execute(
            "MATCH (t:Task {id: $id}) SET t.status = 'open', t.claimed_by = '', t.updated_at = $now",
            {"id": task_id, "now": time.time()})
        self._release(task_id)
        self._event("released", agent, task_id, reason)

    @_atomic
    def register_agent(self, name: str) -> None:
        if not name.strip():
            raise ValueError("Agent name required")
        self.db.execute("MERGE (a:Agent {name: $name}) SET a.state = 'alive', a.last_seen = $now",
                        {"name": name, "now": time.time()})
        self._event("agent_registered", name, "", "")

    @_atomic
    def heartbeat(self, name: str, claim_ttl: float | None = None,
                  leases: dict[str, str] | None = None) -> None:
        now = time.time()
        self.db.execute("MATCH (a:Agent {name: $name}) SET a.last_seen = $now",
                        {"name": name, "now": now})
        if claim_ttl is not None:
            if not math.isfinite(claim_ttl) or claim_ttl <= 0:
                raise ValueError("Positive finite claim_ttl required")
            for row in self.db.execute(
                "MATCH (t:Task {status: 'claimed', claimed_by: $name})"
                " WHERE t.lease_expires > $now RETURN t", {"name": name, "now": now}).fetchall():
                task = row.to_dict()["t"]["properties"]
                if leases is None or leases.get(task["id"]) == task["lease_token"]:
                    self.db.execute("MATCH (t:Task {id: $id}) SET t.lease_expires = $expires",
                                    {"id": task["id"], "expires": now + claim_ttl})

    @_atomic
    def mark_dead(self, name: str) -> None:
        self.db.execute("MATCH (a:Agent {name: $name}) SET a.state = 'dead'", {"name": name})
        self._event("agent_died", name, "", "")

    @_atomic
    def recover_orphans(self, stale_after: float = 30.0) -> list[str]:
        now = time.time()
        rows = self.db.execute(
            "MATCH (t:Task {status: 'claimed'}) OPTIONAL MATCH (a:Agent {name: t.claimed_by})"
            " WITH t, a WHERE coalesce(t.lease_expires, $now + 1) <= $now"
            " OR a.last_seen < $cutoff OR a.state = 'dead' RETURN t, a",
            {"now": now, "cutoff": now - stale_after}).fetchall()
        recovered = []
        for row in rows:
            task = row.to_dict()["t"]["properties"]
            tid, owner = task["id"], task["claimed_by"]
            self.db.execute("MATCH (a:Agent {name: $name}) SET a.state = 'dead'", {"name": owner})
            self._event("agent_died", owner, tid, "inferred from stale heartbeat or expired lease")
            self._event("orphaned", owner, tid, "claim lost")
            self._release(tid)
            self.db.execute(
                "MATCH (t:Task {id: $id}) SET t.status = 'open', t.claimed_by = '',"
                " t.orphaned = true, t.updated_at = $now", {"id": tid, "now": now})
            self._event("recovered", owner, tid, "requeued from dead agent")
            recovered.append(tid)
        return recovered

    @_atomic
    def snapshot(self) -> dict:
        tasks = [r.to_dict()["t"]["properties"] for r in self.db.execute("MATCH (t:Task) RETURN t").fetchall()]
        agents = [r.to_dict()["a"]["properties"] for r in self.db.execute("MATCH (a:Agent) RETURN a").fetchall()]
        deps = [{"from": r.to_dict()["a"]["properties"]["id"], "to": r.to_dict()["b"]["properties"]["id"]}
                for r in self.db.execute("MATCH (a:Task)-[:BLOCKED_BY]->(b:Task) RETURN a, b").fetchall()]
        events = [r.to_dict()["e"]["properties"] for r in self.db.execute(
            "MATCH (e:Event) RETURN e ORDER BY e.ts DESC LIMIT 50").fetchall()]
        return {"tasks": tasks, "agents": agents, "dependencies": deps, "events": events}

    def _event(self, kind: str, agent: str | None, task_id: str, detail: str) -> None:
        # add_task has no actor argument in the original API. Attribute its
        # event to the graph itself so every lineage hop still has a BY edge.
        if agent is None:
            agent = "__workgraph__"
            self.db.execute(
                "MERGE (a:Agent {name: $name}) SET a.state = 'system', a.last_seen = $now",
                {"name": agent, "now": time.time()})
        eid = "e-" + uuid.uuid4().hex
        self.db.execute(
            "CREATE (:Event {id: $eid, ts: $ts, kind: $kind, agent: $agent, task_id: $task, detail: $detail})",
            {"eid": eid, "ts": time.time(), "kind": kind, "agent": agent or "", "task": task_id, "detail": detail})
        if agent:
            self.db.execute(
                "MATCH (e:Event {id: $eid}), (a:Agent {name: $agent}) CREATE (e)-[:BY]->(a)",
                {"eid": eid, "agent": agent})
        if task_id:
            previous = self.db.execute(
                "MATCH (t:Task {id: $tid})<-[:ON]-(e:Event)"
                " WHERE NOT EXISTS { MATCH (e)-[:NEXT]->(:Event) } RETURN e",
                {"tid": task_id}).fetchone()
            self.db.execute("MATCH (e:Event {id: $eid}), (t:Task {id: $tid}) CREATE (e)-[:ON]->(t)",
                            {"eid": eid, "tid": task_id})
            if previous:
                self.db.execute("MATCH (p:Event {id: $pid}), (e:Event {id: $eid}) CREATE (p)-[:NEXT]->(e)",
                                {"pid": previous.to_dict()["e"]["properties"]["id"], "eid": eid})

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._closed = True
