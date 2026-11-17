# WorkGraph v2 - build spec (Relate by Neo4j Hackathon, target: 1st place)

Goal: turn WorkGraph from a simulated demo into a real crash-resilient coordination layer for teams of AI agents. Real processes, real crashes, queries only a graph can answer. Everything single-file embedded Relate (relate-db), no servers, no signups. Python 3.10+, stdlib + relate-db only.

Module 1 - workgraph/worker.py (real worker runtime):
class Worker(name: str, store_path: str, handler: callable, heartbeat_interval: float = 5.0, claim_ttl: float = 45.0). Runs in its own process; handler(task: dict) -> str result. Loop: claim next claimable task -> run handler -> complete() (fail() on exception). Heartbeats on a background thread. Claims are LEASES: claim() writes claimed_by, claimed_at, lease_expires (epoch) on the task node; claimable() excludes tasks with a live lease. If the process dies (SIGKILL), heartbeat goes stale and lease expires; recover_orphans() requeues claimed tasks with full event trail preserved (extend core.py's existing recover_orphans to check lease expiry too, not just heartbeat staleness). Import-safe: no side effects at import. Graceful SIGTERM: finish current task, release, exit. SIGKILL leaves orphans by design - that is the demo.

Module 2 - workgraph/queries.py (all Relate/Cypher traversals over the same store, no side tables):
- critical_path() -> list[dict]: longest chain of unfinished tasks by dependency depth, with titles and current owner.
- blocked_by(task_id) -> list[dict]: transitive closure of everything blocking a task, with status and claimed_by per hop.
- blast_radius(agent_name) -> dict: claimed tasks + every transitively-blocked downstream task + agents waiting on those, if this agent dies now.
- lineage(task_id) -> list[dict]: full event ancestry as a path: created -> claimed -> (orphaned -> reclaimed)* -> done, agent + timestamp per hop. Events are nodes linked to task AND agent nodes; this is a traversal, not a log scan.

Module 3 - workgraph/cli.py (python -m workgraph ...):
init PATH; add "title" [--depends-on ID ...] [--db PATH]; worker NAME [--db PATH] [--module path.to.handler]; status [--db PATH] (board + critical path + agent health); query critical-path|blocked-by ID|blast-radius AGENT|lineage ID [--db PATH]. All against the same single file; server.py stays optional.

Module 5 - tests/test_v2.py:
pytest. Real subprocess test: start worker, kill -9 mid-task, assert task is reclaimed and completed by another worker, assert lineage shows the death and recovery hops. Must pass on macOS and Linux.

Rules: small readable code, stdlib + relate-db only, keep core.py public API stable (extend, don't rename), no new deps, no network calls in core/worker/queries. Skip Module 4 (demo2.py) - packet 2 after review.
