# GOODPDF Worker (production)
# - Ghostscript + qpdf + 7z deps
# - Runs worker-local/worker.mjs as a long-running service

FROM node:20-bookworm-slim

# OS deps
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     ghostscript qpdf p7zip-full ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps
COPY worker-local/package.json ./package.json
RUN npm install --omit=dev

# Copy worker
COPY worker-local/worker.mjs ./worker.mjs

# Runtime defaults (Linux binaries)
ENV GS_EXE=gs \
    QPDF_EXE=qpdf \
    SEVEN_Z_EXE=7z \
    POLL_MS=2000 \
    CONCURRENCY=1 \
    BUCKET_IN=job-input \
    BUCKET_OUT=jobs-output

# Required env at runtime:
# - SUPABASE_URL
# - SUPABASE_SERVICE_ROLE_KEY

CMD ["node", "worker.mjs"]
