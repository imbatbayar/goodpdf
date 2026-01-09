import "@/ui/styles/tailwind.css";
import "@/app/globals.css";
import { AppShell } from "@/components/layouts/AppShell";

export const metadata = {
  title: "goodpdf.org",
  description: "Compress and split PDFs online.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-zinc-50 text-zinc-900">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
