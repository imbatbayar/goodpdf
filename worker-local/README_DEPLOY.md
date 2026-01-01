# GOODPDF Worker — Production Deploy (Docker)

Энэ worker нь `jobs` хүснэгтээс `status=UPLOADED` job-уудыг авч боловсруулаад:
- Ghostscript-аар шахна
- splitMb зорилтоор хэсэглэнэ
- ZIP үүсгэнэ
- `jobs-output` bucket руу upload хийнэ
- DB дээр `status=DONE` + `output_zip_path` бичнэ

## 1) Build

Repo root дотроос:

```bash
docker build -t goodpdf-worker -f worker-local/Dockerfile .
```

## 2) Run (local / server)

```bash
docker run --rm \
  -e SUPABASE_URL="https://<PROJECT_REF>.supabase.co" \
  -e SUPABASE_SERVICE_ROLE_KEY="<SERVICE_ROLE_KEY>" \
  -e CONCURRENCY=1 \
  -e POLL_MS=2000 \
  goodpdf-worker
```

### ENV (required)
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY` (⚠️ зөвхөн server/worker дээр)

### ENV (optional)
- `CONCURRENCY` (default=1)
- `POLL_MS` (default=2000)
- `BUCKET_IN` (default=job-input)
- `BUCKET_OUT` (default=jobs-output)
- `GS_EXE` (default=gs)
- `QPDF_EXE` (default=qpdf)
- `SEVEN_Z_EXE` (default=7z)

## 3) Production платформ сонголт

Энэ Docker image-г дараах дээр шууд байршуулж болно:
- Fly.io
- Render
- Google Cloud Run
- Railway

> Ямар платформ сонгосноос хамаараад би яг copy-paste deploy command-уудыг нь гаргаад өгнө.
