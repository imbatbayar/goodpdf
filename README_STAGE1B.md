
STAGE 1B — Header/Footer wired safely (no Tailwind yet)
------------------------------------------------------
✅ Core jobs logic untouched.
✅ Existing routes untouched.
✅ Existing import paths kept stable via wrapper components.

What changed:
- New UI layer: src/ui/layouts/Header.tsx + AppShell.tsx (+ CSS modules)
- components/layouts/AppShell.tsx now wraps ui AppShell
- components/layouts/Header.tsx re-exports ui Header
- Tooltip added: src/ui/primitives/Tooltip.tsx

Next (optional):
- Install Tailwind AFTER this (you requested).
- STAGE 2: add hamburger drawer, country-based billing UI, feature flags.
