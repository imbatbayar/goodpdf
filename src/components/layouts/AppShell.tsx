
import { AppShell as UiAppShell } from "@/ui/layouts/AppShell";

// Wrapper: keeps existing import paths stable (no logic change)
export function AppShell({ children }: { children: React.ReactNode }) {
  return <UiAppShell>{children}</UiAppShell>;
}
