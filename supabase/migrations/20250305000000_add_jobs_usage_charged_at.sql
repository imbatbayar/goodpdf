-- Add jobs.usage_charged_at for idempotent billing: charge exactly 1 token only when job completes (DONE).
-- Run this migration on your Supabase project (e.g. via Supabase Dashboard SQL or CLI).

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS usage_charged_at timestamptz NULL;

COMMENT ON COLUMN jobs.usage_charged_at IS 'Set when 1 token has been charged for this job (idempotent charge on DONE only).';
