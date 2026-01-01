# goodpdf.org — UI MVP bundle

This bundle focuses on **UI + page routing + button wiring + a mock backend** so you can open and start integrating Supabase/Worker later.

## Run
```bash
npm install
npm run dev
```

## What works now
- Landing -> Upload/Pricing navigation
- Upload screen: file pick, quality select, split size, Start progress, Done, Download fallback, Done confirm
- Header (Login button placeholder) + Account summary placeholder
- Mock API under `/api/mock/*` so UI runs without external services

## Next step (your work)
Swap `/api/mock/*` calls to real `/api/jobs/*` + Supabase/Worker.
