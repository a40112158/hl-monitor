import os
import sys
import time
import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

url = os.getenv('TURSO_DATABASE_URL', '').strip()
token = os.getenv('TURSO_AUTH_TOKEN', '').strip()
repo = os.getenv('GITHUB_REPOSITORY', '')
run_id = os.getenv('GITHUB_RUN_ID', '')
sha = os.getenv('GITHUB_SHA', '')[:12]

print('[PROBE] start')
print(f'[PROBE] repo={repo} run_id={run_id} sha={sha}')
print(f'[PROBE] TURSO_DATABASE_URL set={bool(url)} length={len(url)}')
print(f'[PROBE] TURSO_AUTH_TOKEN set={bool(token)} length={len(token)}')

if not url or not token:
    print('[PROBE][ERROR] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN is missing')
    sys.exit(2)

parsed = urlparse(url)
print(f'[PROBE] url_scheme={parsed.scheme} url_host={parsed.netloc}')
if parsed.hostname:
    try:
        print(f'[PROBE] dns={parsed.hostname} -> {socket.gethostbyname(parsed.hostname)}')
    except Exception as e:
        print(f'[PROBE][WARN] DNS lookup failed: {type(e).__name__}: {e}')

try:
    import libsql
except Exception as e:
    print(f'[PROBE][ERROR] import libsql failed: {type(e).__name__}: {e}')
    sys.exit(3)

try:
    print('[PROBE] connecting to Turso...')
    conn = libsql.connect(url, auth_token=token)
    cur = conn.cursor()
    print('[PROBE] connected')

    print('[PROBE] creating debug_probe table...')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS debug_probe (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            repo TEXT,
            github_run_id TEXT,
            github_sha TEXT,
            note TEXT
        )
    ''')
    conn.commit()

    now = datetime.now(timezone.utc).isoformat()
    note = 'turso probe insert from github actions'
    print('[PROBE] inserting one row...')
    cur.execute(
        'INSERT INTO debug_probe(created_at, repo, github_run_id, github_sha, note) VALUES (?, ?, ?, ?, ?)',
        (now, repo, run_id, sha, note),
    )
    conn.commit()

    cur.execute('SELECT COUNT(*), MAX(id), MAX(created_at) FROM debug_probe')
    count, max_id, max_created = cur.fetchone()
    print(f'[PROBE] debug_probe count={count} max_id={max_id} max_created_at={max_created}')

    # Also try to list a few existing app tables without failing if missing.
    for table in ['runs', 'wallet_states', 'perp_positions', 'spot_balances']:
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            exists = cur.fetchone() is not None
            if exists:
                cur.execute(f'SELECT COUNT(*) FROM {table}')
                c = cur.fetchone()[0]
                print(f'[PROBE] table {table}: exists count={c}')
            else:
                print(f'[PROBE] table {table}: missing')
        except Exception as e:
            print(f'[PROBE][WARN] table check failed {table}: {type(e).__name__}: {e}')

    conn.close()
    print('[PROBE] success')
except Exception as e:
    print(f'[PROBE][ERROR] Turso write/read failed: {type(e).__name__}: {e}')
    sys.exit(4)
