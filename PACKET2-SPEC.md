# WorkGraph v2 - Build Packet 2: demo2 + board polish

Environment notes for the coding agent (not part of the spec):
- Repo: ~/Projects/workgraph, branch v2-runtime (already checked out). NEVER touch main.
- Use the project venv: ./.venv/bin/python (has relate-db 1.0.2, imports as `relate`, and pytest). System python is PEP 668 managed.
- Python stdlib + relate-db only. No network calls. No new dependencies.
- Do NOT git commit or git push. The supervising agent commits after independently rerunning your tests.
- At the end of your run, print: files created/changed, full test summary, and any deviations from this spec.

Spec:

Module 4 - demo2.py (replaces the simulated demo as the flagship):

1. Real work, real processes. Build a small but real pipeline of 6-8 dependent tasks that produce inspectable artifacts under ./demo_out/ - e.g. a mini release-notes pipeline: fetch a small text fixture from disk, extract items, summarize counts, cross-check two sources, render a markdown report. Handlers live in examples/pipeline_handlers.py, each exporting handler(task) -> str, writing real files. No network calls.
2. Spawn 3 real worker subprocesses (scout, builder, closer) with subprocess.Popen running python -m workgraph worker NAME --module examples.pipeline_handlers --db demo.rlf. Each worker's stdout flows to the parent and is teed into the board.
3. The kill: when builder is mid-task on the longest task (handler sleeps ~8s in chunks so the kill lands mid-work), demo2 sends os.kill(pid, signal.SIGKILL). The board must show builder red within one heartbeat interval, its lease expiring, then a teammate reclaiming the orphaned task and the pipeline finishing green. Print the orphan's lineage() at the end showing created -> claimed(builder) -> orphaned -> reclaimed(closer) -> done with timestamps.
4. Make the story reproducible: python demo2.py runs the whole arc headless in under 60s and exits 0; python demo2.py --ui also serves the live board (extend server.py below).

server.py upgrades (same optional UI):
- Claimed nodes show lease countdown (seconds left) and owner.
- Agent strip shows last-heartbeat age per agent; red when stale; skull/down state after mark_dead or lease expiry.
- Add per-task lineage view endpoint GET /api/lineage/<task_id> and surface it as a click on a task node (simple text panel is fine).
- Add critical path endpoint GET /api/critical-path, highlighted on the board.

Tests (tests/test_demo2.py): run demo2.py headless as a subprocess, assert exit 0, assert the final report file exists in demo_out/, assert stdout contains the lineage hops orphaned -> reclaimed, assert total runtime < 90s. Keep total suite under 2 min.

Also update README.md "The demo" section to describe demo2 (real processes, real kill) while keeping demo.py mentioned as the original walkthrough. Do not change the copy tone: plain, direct, no marketing words. Note the store is a directory (workgraph.rlf/), not a single file; say "directory" in README.
