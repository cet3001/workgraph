"""V2 acceptance tests, including actual POSIX process death and recovery."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from workgraph.core import WorkGraph
from workgraph.queries import Queries
from workgraph.worker import Worker

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def graph(tmp_path):
    graph = WorkGraph(str(tmp_path / "test.rlf"))
    yield graph
    graph.close()


def wait_for(predicate, processes=(), timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        for process in processes:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                pytest.fail(f"Child exited {process.returncode}: {stdout}\n{stderr}")
        time.sleep(0.05)
    pytest.fail("Timed out waiting for subprocess state")


def spawn(code, *args, env=None):
    return subprocess.Popen([sys.executable, "-c", code, *map(str, args)], cwd=ROOT,
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    stdout, stderr = process.communicate()
    assert process.returncode == 0, (process.returncode, stdout, stderr)


def expire(graph, task_id):
    graph.db.execute("MATCH (t:Task {id: $id}) SET t.lease_expires = $past",
                     {"id": task_id, "past": time.time() - 1})


def test_claim_dependencies_and_live_lease(graph):
    a = graph.add_task("root", "a")
    b = graph.add_task("child", "b", [a])
    assert not graph.claim("worker", b)
    assert [t["id"] for t in graph.claimable()] == [a]
    assert graph.claim("worker", a)
    task = graph.get_task(a)
    assert task["claimed_at"] <= time.time() < task["lease_expires"]
    assert task["claimed_by"] == "worker"
    assert not graph.claim("other", a)
    assert graph.recover_orphans() == []
    assert graph.claimable() == []
    graph.complete("worker", a, "ok")
    assert [t["id"] for t in graph.claimable()] == [b]
    # Even inconsistent legacy open state cannot override an outstanding lease.
    graph.db.execute("MATCH (t:Task {id: $id}) SET t.lease_expires = $future",
                     {"id": b, "future": time.time() + 60})
    assert not graph.claim("other", b)


def test_expired_lease_recovery_and_fencing(graph):
    tid = graph.add_task("recover")
    graph.claim("same-name", tid)
    old = graph.get_task(tid)["lease_token"]
    expire(graph, tid)
    graph.complete("same-name", tid, "late", lease_token=old)
    assert graph.get_task(tid)["status"] == "claimed"
    assert graph.recover_orphans(stale_after=1000) == [tid]
    assert graph.recover_orphans() == []
    graph.register_agent("same-name")
    assert graph.claim("same-name", tid)
    graph.complete("same-name", tid, "stale", lease_token=old)
    graph.fail("same-name", tid, "stale", lease_token=old)
    assert graph.get_task(tid)["status"] == "claimed"
    graph.complete("same-name", tid, "fresh", lease_token=graph.get_task(tid)["lease_token"])
    assert graph.get_task(tid)["result"] == "fresh"
    assert [e["kind"] for e in Queries(graph).lineage(tid)] == [
        "task_added", "claimed", "agent_died", "orphaned", "recovered", "reclaimed", "completed"]


@pytest.mark.parametrize("cause", ["heartbeat", "marked_dead"])
def test_legacy_recovery_triggers(graph, cause):
    tid = graph.add_task("orphan")
    graph.register_agent("old")
    graph.claim("old", tid)
    if cause == "heartbeat":
        graph.db.execute("MATCH (a:Agent {name: 'old'}) SET a.last_seen = 0.0")
    else:
        graph.mark_dead("old")
    assert graph.recover_orphans() == [tid]
    assert graph.get_task(tid)["status"] == "open"


def test_release_and_wrong_owner_do_not_create_events(graph):
    tid = graph.add_task("retry")
    graph.claim("a", tid)
    before = Queries(graph).lineage(tid)
    graph.complete("b", tid)
    graph.fail("b", tid, "wrong owner")
    assert Queries(graph).lineage(tid) == before
    graph.fail("a", tid, "handler failed")
    assert graph.get_task(tid)["status"] == "open"
    assert graph.get_task(tid)["lease_expires"] == 0
    assert graph.claim("b", tid)


def test_validation_rolls_back(graph):
    graph.add_task("original", "one")
    with pytest.raises(ValueError):
        graph.add_task("duplicate", "one")
    with pytest.raises(ValueError):
        graph.add_task("bad", "bad", ["missing"])
    assert graph.get_task("bad") is None
    assert len(graph.snapshot()["tasks"]) == 1
    with pytest.raises(RuntimeError):
        with graph._session():
            graph.add_task("rollback", "rollback")
            raise RuntimeError("abort")
    assert graph.get_task("rollback") is None


def test_graph_queries_diamond_and_waiting_agents(graph):
    for tid, deps in [("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]), ("e", ["d"])]:
        graph.add_task(tid.upper(), tid, deps)
    q = Queries(graph)
    assert len(q.critical_path()) == 4
    assert q.critical_path()[0]["id"] == "a"
    assert q.critical_path()[-1]["id"] == "e"
    blockers = q.blocked_by("e")
    assert {t["id"]: t["depth"] for t in blockers} == {"d": 1, "b": 2, "c": 2, "a": 3}
    assert all("status" in t and "claimed_by" in t for t in blockers)
    graph.claim("owner", "a")
    graph.register_agent("waiting")
    graph.db.execute("MATCH (a:Agent {name:'waiting'}), (t:Task {id:'e'}) CREATE (a)-[:WAITING_ON]->(t)")
    radius = q.blast_radius("owner")
    assert [t["id"] for t in radius["claimed_tasks"]] == ["a"]
    assert {t["id"] for t in radius["downstream_tasks"]} == {"b", "c", "d", "e"}
    assert [a["name"] for a in radius["waiting_agents"]] == ["waiting"]
    graph.complete("owner", "a")
    assert "a" not in {t["id"] for t in q.blocked_by("e")}
    assert len(q.critical_path()) == 3
    assert q.blast_radius("owner") == {"claimed_tasks": [], "downstream_tasks": [], "waiting_agents": []}


def test_empty_and_done_queries(graph):
    q = Queries(graph)
    assert q.critical_path() == q.blocked_by("missing") == q.lineage("missing") == []
    tid = graph.add_task("only")
    assert len(q.critical_path()) == 1
    graph.claim("a", tid)
    graph.complete("a", tid)
    assert q.critical_path() == []


def test_full_lineage_is_graph_linked_not_snapshot_limit(graph):
    tid = graph.add_task("many attempts")
    for _ in range(26):
        assert graph.claim("retry", tid)
        graph.fail("retry", tid, "retry")
    assert graph.claim("retry", tid)
    graph.complete("retry", tid)
    lineage = Queries(graph).lineage(tid)
    assert len(lineage) == 55
    assert lineage[0]["kind"] == "task_added"
    assert lineage[-1]["kind"] == "completed"
    assert [e["hop"] for e in lineage] == list(range(55))
    assert all("ts" in e and "agent" in e for e in lineage)
    linked = graph.db.execute(
        "MATCH (:Task {id: $id})<-[:ON]-(e:Event)-[:BY]->(:Agent) RETURN e", {"id": tid}).fetchall()
    assert len(linked) == 55
    assert len(graph.snapshot()["events"]) == 50


def test_competing_processes_claim_once(graph, tmp_path):
    tid = graph.add_task("race")
    gate = tmp_path / "go"
    code = '''
import pathlib, sys, time
from workgraph.core import WorkGraph
w = WorkGraph(sys.argv[1])
while not pathlib.Path(sys.argv[3]).exists(): time.sleep(.01)
print(w.claim(sys.argv[2], sys.argv[4]), flush=True)
w.close()
'''
    children = [spawn(code, graph.path, name, gate, tid) for name in ("one", "two", "three")]
    try:
        gate.touch()
        outputs = [p.communicate(timeout=15) for p in children]
        assert all(p.returncode == 0 for p in children), outputs
        assert sorted(out.strip() for out, _ in outputs) == ["False", "False", "True"]
        assert len([e for e in Queries(graph).lineage(tid) if e["kind"] == "claimed"]) == 1
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()
                p.wait()


WORKER_CODE = '''
import pathlib, sys, time
from workgraph.worker import Worker
def handler(task):
    pathlib.Path(sys.argv[3]).write_text(task['id'])
    time.sleep(float(sys.argv[4]))
    return 'finished by ' + sys.argv[2]
Worker(sys.argv[2], sys.argv[1], handler, heartbeat_interval=.1, claim_ttl=1.5).run()
'''


def test_sigkill_crash_recovery(tmp_path):
    # The parent keeps its WorkGraph object alive across both child processes.
    # This catches stale connection snapshots as well as missing persistence.
    path = tmp_path / "crash.rlf"
    graph = WorkGraph(str(path))
    tid = graph.add_task("Survive an actual SIGKILL", "crash-task")
    marker = tmp_path / "started"
    first = spawn(WORKER_CODE, path, "doomed", marker, 60)
    second = None
    try:
        wait_for(marker.exists, [first])
        assert graph.get_task(tid)["status"] == "claimed"
        os.kill(first.pid, signal.SIGKILL)
        first.wait(timeout=5)
        assert first.returncode == -signal.SIGKILL
        assert graph.get_task(tid)["status"] == "claimed"
        second = spawn(WORKER_CODE, path, "rescuer", tmp_path / "rescued", 0)
        wait_for(lambda: graph.get_task(tid)["status"] == "done", [second])
        stop(second)
        task = graph.get_task(tid)
        assert task["claimed_by"] == "rescuer"
        assert task["result"] == "finished by rescuer"
        lineage = Queries(graph).lineage(tid)
        assert [e["kind"] for e in lineage] == [
            "task_added", "claimed", "agent_died", "orphaned", "recovered", "reclaimed", "completed"]
        assert [e["agent"] for e in lineage] == ["__workgraph__", "doomed", "doomed", "doomed", "doomed", "rescuer", "rescuer"]
        assert [e["ts"] for e in lineage] == sorted(e["ts"] for e in lineage)
        print(f"\nPersisted crash-recovery store: {path}")
    finally:
        for p in (first, second):
            if p is not None and p.poll() is None:
                p.kill()
                p.wait()
        graph.close()
    reopened = WorkGraph(str(path))
    try:
        assert reopened.get_task(tid)["status"] == "done"
        assert len(Queries(reopened).lineage(tid)) == 7
    finally:
        reopened.close()


def test_heartbeat_renewal_and_graceful_sigterm(graph, tmp_path):
    tid = graph.add_task("slow", "slow")
    following = graph.add_task("must stay open", "next", [tid])
    marker = tmp_path / "started"
    child = spawn(WORKER_CODE, graph.path, "patient", marker, 3)
    try:
        wait_for(marker.exists, [child])
        initial = graph.get_task(tid)["lease_expires"]
        child.terminate()
        assert child.poll() is None
        wait_for(lambda: graph.get_task(tid)["lease_expires"] > initial + 1.5, [child])
        assert graph.recover_orphans() == []
        stop(child)
        assert graph.get_task(tid)["status"] == "done"
        assert graph.get_task(following)["status"] == "open"
        assert not graph.db.execute("MATCH (:Agent)-[:CLAIMED]->(:Task) RETURN 1").fetchall()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_handler_exception_releases_then_retries(graph):
    tid = graph.add_task("retry")
    code = '''
import sys
from workgraph.worker import Worker
attempt = 0
def handler(task):
    global attempt
    attempt += 1
    if attempt == 1: raise RuntimeError('expected handler failure')
    worker.stop()
    return 'retried'
worker = Worker('retry', sys.argv[1], handler, .1, 1.5)
worker.run()
'''
    child = spawn(code, graph.path)
    try:
        out, err = child.communicate(timeout=15)
        assert child.returncode == 0, (out, err)
        assert graph.get_task(tid)["result"] == "retried"
        assert [e["kind"] for e in Queries(graph).lineage(tid)] == [
            "task_added", "claimed", "released", "claimed", "completed"]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def cli(*args, env=None):
    return subprocess.run([sys.executable, "-m", "workgraph", *map(str, args)],
                          cwd=ROOT, env=env, capture_output=True, text=True, timeout=15)


def test_cli_commands_and_module_handler(tmp_path):
    path = tmp_path / "cli.rlf"
    assert cli("init", path).returncode == 0
    root = cli("add", "First", "--db", path)
    assert root.returncode == 0, root.stderr
    tid = root.stdout.strip()
    child = cli("add", "Second", "--depends-on", tid, "--db", path)
    assert child.returncode == 0, child.stderr
    status = cli("status", "--db", path)
    assert status.returncode == 0
    assert all(text in status.stdout for text in ["BOARD", "CRITICAL PATH", "AGENT HEALTH", "First", "Second"])
    for query, target in [("critical-path", None), ("blocked-by", child.stdout.strip()),
                          ("blast-radius", "missing"), ("lineage", tid)]:
        args = ["query", query] + ([target] if target else []) + ["--db", str(path)]
        result = cli(*args)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) is not None
    assert cli("query", "lineage", "--db", path).returncode != 0
    assert cli("query", "critical-path", "extra", "--db", path).returncode != 0
    (tmp_path / "custom_handler.py").write_text("def handler(task):\n    return 'custom:' + task['title']\n")
    env = dict(os.environ, PYTHONPATH=str(tmp_path) + os.pathsep + str(ROOT))
    process = subprocess.Popen([sys.executable, "-m", "workgraph", "worker", "cli-worker", "--db", str(path),
                                "--module", "custom_handler"], cwd=ROOT, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    graph = WorkGraph(str(path))
    try:
        wait_for(lambda: all(t["status"] == "done" for t in graph.snapshot()["tasks"]), [process])
        stop(process)
        assert graph.get_task(tid)["result"] == "custom:First"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        graph.close()


def test_import_safe(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    result = subprocess.run([sys.executable, "-c",
        "import workgraph.core, workgraph.worker, workgraph.queries, workgraph.cli, workgraph.__main__"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout == result.stderr == ""
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("interval,ttl", [(0, 1), (1, 1), (2, 1), (.1, float('inf'))])
def test_worker_timing_validation(interval, ttl):
    with pytest.raises(ValueError):
        Worker("invalid", "unused.rlf", lambda task: "", interval, ttl)
