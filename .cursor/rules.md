# GOODPDF Project Rules

This project is GOODPDF.

Main stack:

- Next.js frontend
- Node.js worker
- Python tools for PDF processing
- Cloudflare R2 for storage
- Supabase for database

Project structure:

src/
Main application code (Next.js).

src/app/api/
API routes.

worker/
Production worker logic.

worker-local/
Local development worker.

tools/
Python scripts used for PDF processing.

supabase/
Database configuration.

Important rules:

- Prefer minimal edits when modifying code.
- Do NOT rewrite entire files unless necessary.
- Keep worker logic simple and isolated.
- Keep frontend logic inside src/.
- API logic should stay inside src/app/api/.
- Python utilities stay inside tools/.

Folders to ignore:

- node_modules
- .next
- dist
- build
- venv
- Data

General coding style:

- Prefer simple and readable code.
- Avoid unnecessary refactoring.
- Follow existing project structure.
