"""Demo: a three-agent team ships a feature on a shared WorkGraph.

Watch it:  python demo.py          then open http://localhost:8471
Headless:  python demo.py --no-ui  prints the run to the terminal

The scenario: three agents (scout, builder, closer) divide up a small
feature build. Midway, builder dies without warning. Nothing it claimed is
lost, because the work state never lived in the process - it lives in the
shared Relate graph. closer sees the orphaned task and finishes it.

Agents run as stepped coroutines here so the demo fits in one file and one
process. The same graph API works across processes and machines: point
every agent at the same graph path.
"""

from __future__ import annotations

import argparse
import os
import shutil
import threading
import time

from workgraph.core import WorkGraph
from workgraph.server import serve
from workgraph.queries import Queries

TASKS = [
    ("Map the legacy checkout flow", []),
    ("Write the failing test for discount stacking", [0]),
    ("Trace the bug to its module", [1]),
    ("Patch the discount logic", [2]),
    ("Add regression coverage", [3]),
    ("Update the changelog and ship notes", [4]),
]


class DemoAgent:
    def __init__(self, name: str, die_at: float | None = None,
                 work_seconds: float = 1.5):
        self.name = name
        self.die_at = die_at
        self.work_seconds = work_seconds
        self.busy_until = 0.0
        self.task = None
        self.done_working = False

    def step(self, wg: WorkGraph, now: float) -> None:
        if self.die_at and now >= self.die_at:
            if not self.done_working:
                wg.mark_dead(self.name)  # process vanishes: no cleanup
                self.done_working = True
            return
        wg.heartbeat(self.name)
        if self.task:
            if now >= self.busy_until:
                wg.complete(self.name, self.task, result="ok")
                self.task = None
            return
        for task in wg.claimable():
            if wg.claim(self.name, task["id"]):
                self.task = task["id"]
                self.busy_until = now + self.work_seconds
                return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-ui", action="store_true")
    ap.add_argument("--db", default="/tmp/workgraph-demo.rlf")
    args = ap.parse_args()

    if os.path.isdir(args.db):
        shutil.rmtree(args.db)
    elif os.path.exists(args.db):
        os.remove(args.db)

    wg = WorkGraph(args.db)

    ids = [wg.add_task(title) for title, _ in TASKS]
    for i, (_, deps) in enumerate(TASKS):
        for d in deps:
            wg.db.execute(
                "MATCH (a:Task {id: $a}), (b:Task {id: $b})"
                " CREATE (a)-[:BLOCKED_BY]->(b)",
                {"a": ids[i], "b": ids[d]},
            )

    latest = [wg.snapshot()]
    if not args.no_ui:
        threading.Thread(target=serve, args=(lambda: latest[0],),
                         kwargs={"get_lineage": Queries(wg).lineage,
                                 "get_critical_path": Queries(wg).critical_path},
                         daemon=True).start()

    agents = [
        DemoAgent("scout"),
        DemoAgent("builder", die_at=time.time() + 5, work_seconds=30),  # dies mid-task at t=5s
        DemoAgent("closer"),
    ]
    for a in agents:
        wg.register_agent(a.name)

    while True:
        now = time.time()
        wg.recover_orphans(stale_after=3)
        for a in agents:
            a.step(wg, now)
        latest[0] = wg.snapshot()
        if all(t["status"] == "done" for t in latest[0]["tasks"]):
            break
        time.sleep(0.4)

    snap = wg.snapshot()
    done = sum(1 for t in snap["tasks"] if t["status"] == "done")
    print(f"\nAll {done}/{len(snap['tasks'])} tasks done.")
    print("Event log:")
    for e in reversed(snap["events"]):
        print(f"  {time.strftime('%H:%M:%S', time.localtime(e['ts']))}"
              f"  {e['kind']:<16} {e['agent']:<8} {e['task_id']} {e['detail']}")

    if not args.no_ui:
        print("\nUI still up on http://localhost:8471 - Ctrl+C to quit.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
