"""Disk-only release pipeline. Outputs are replaced atomically on retry."""
import hashlib
import json
from pathlib import Path
import time

OUT = Path('demo_out')
FIXTURES = Path(__file__).parent / 'fixtures'


def write(name, text, task):
    temporary = OUT / (name + '.' + task['lease_token'] + '.tmp')
    temporary.write_text(text)
    temporary.replace(OUT / name)
    return str(OUT / name)


def handler(task) -> str:
    OUT.mkdir(exist_ok=True)
    tid = task['id']
    print(f"{task['claimed_by']}: starting {tid}", flush=True)
    if tid == 'fetch':
        result = write('changes.txt', (FIXTURES / 'changes.txt').read_text(), task)
    elif tid == 'extract':
        items = [dict(zip(('id', 'kind', 'title'), line.split('|')))
                 for line in (OUT / 'changes.txt').read_text().splitlines()]
        result = write('items.json', json.dumps(items, indent=2), task)
    elif tid == 'counts':
        items = json.loads((OUT / 'items.json').read_text())
        counts = {kind: sum(i['kind'] == kind for i in items)
                  for kind in sorted({i['kind'] for i in items})}
        result = write('counts.json', json.dumps(counts, indent=2), task)
    elif tid == 'crosscheck':
        items = json.loads((OUT / 'items.json').read_text())
        approved = set((FIXTURES / 'approved.txt').read_text().splitlines())
        # Leave inspectable progress from the interrupted attempt. Only publish
        # the final cross-check once all work is complete.
        progress = OUT / f"crosscheck-{task['claimed_by']}.progress"
        for step in range(16):
            progress.write_text(f'{step + 1}/16\n')
            time.sleep(.5)
        if {i['id'] for i in items} != approved:
            raise ValueError('Release items differ from approved IDs')
        result = write('crosscheck.json', json.dumps({'matched': len(approved)}), task)
    elif tid == 'render':
        items = json.loads((OUT / 'items.json').read_text())
        counts = json.loads((OUT / 'counts.json').read_text())
        matched = json.loads((OUT / 'crosscheck.json').read_text())['matched']
        report = '# Release notes\n\n' + '\n'.join(
            f"- {i['id']} ({i['kind']}): {i['title']}" for i in items)
        report += '\n\nCounts: ' + ', '.join(f'{k}: {v}' for k, v in counts.items())
        report += f'\n\nCross-check: {matched} approved items matched.\n'
        result = write('report.md', report, task)
    elif tid == 'manifest':
        names = ['changes.txt', 'items.json', 'counts.json', 'crosscheck.json', 'report.md']
        manifest = {name: hashlib.sha256((OUT / name).read_bytes()).hexdigest() for name in names}
        result = write('manifest.json', json.dumps(manifest, indent=2), task)
    elif tid == 'verify':
        manifest = json.loads((OUT / 'manifest.json').read_text())
        for name, digest in manifest.items():
            if hashlib.sha256((OUT / name).read_bytes()).hexdigest() != digest:
                raise ValueError(f'Checksum mismatch: {name}')
        result = write('verified.txt', f'{len(manifest)} artifacts verified\n', task)
    else:
        raise ValueError(f'Unknown pipeline task: {tid}')
    print(f"{task['claimed_by']}: wrote {result}", flush=True)
    return result
