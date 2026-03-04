# Repository hygiene

## Checklist

- [ ] **Install Node deps (root):** `npm install` — installs from `package.json`; lockfile (`package-lock.json` or `pnpm-lock.yaml` / `yarn.lock`) is committed.
- [ ] **Install worker deps:** From `worker-local/` run `npm install` if that folder has its own `package.json`.
- [ ] **Python / worker venv:** If the worker uses Python, create and use a virtualenv (e.g. `python -m venv venv` in `worker-local/`); activate and `pip install -r requirements.txt` (or equivalent). Do not commit `venv/` or `node_modules/`.

## Intentionally ignored (do not commit)

- **`node_modules/`**, **`**/node_modules/`** — Node dependencies; reinstall with `npm i`.
- **`venv/`**, **`.venv/`**, **`**/venv/`**, **`**/.venv/`** — Python virtualenvs; recreate locally.
- **`.next/`**, **`out/`** — Next.js build output.
- **`__pycache__/`**, **`*.pyc`** — Python cache.
- **`.env`**, **`.env.local`** — Secrets (never commit).
- **`*.log`**, **`logs/`** — Log files.
- **`.DS_Store`**, **`Thumbs.db`**, **`.idea/`**, **`.vscode/`** — OS/editor cruft.
- **`tmp/`**, **`dist/`**, **`build/`** — Worker/build artifacts.

## Worker poll loop (env)

When running the worker (`worker-local/worker.mjs`), these env vars control polling and idle backoff (defaults in parentheses):

- **`POLL_MS`** (2000) — Base interval in ms between queue fetches.
- **`POLL_IDLE_MAX_MS`** (60000, min 60000) — When the queue is empty, the worker backs off exponentially; this is the cap in ms.
- **`POLL_IDLE_LOG_EVERY_MS`** (30000, min 5000) — Idle and loop-error logs are throttled to at most once per this many ms.

Backoff resets to `POLL_MS` as soon as a fetch returns one or more jobs (or a job is claimed). No new dependencies.

## After clone

1. `npm install` (at repo root).
2. If using worker-local: `cd worker-local && npm install` (and create/use a venv if required).
3. Copy `.env.example` to `.env` and fill in secrets if needed.
