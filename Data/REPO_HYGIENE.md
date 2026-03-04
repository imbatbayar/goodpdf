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

## After clone

1. `npm install` (at repo root).
2. If using worker-local: `cd worker-local && npm install` (and create/use a venv if required).
3. Copy `.env.example` to `.env` and fill in secrets if needed.
