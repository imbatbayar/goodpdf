/**
 * Compatibility route:
 * Some Next setups may not map route-groups as expected during dev.
 * Keeping this file ensures `/` always resolves.
 * Canonical page lives in `src/app/(public)/page.tsx`.
 */
import PublicHome from "@/app/(public)/page";
export default PublicHome;
