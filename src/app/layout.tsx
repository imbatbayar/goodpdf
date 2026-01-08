import "@/app/globals.css";
import "@/ui/styles/tailwind.css";

import "@/app/globals.css";
import { AppShell } from "@/components/layouts/AppShell";

export const metadata = {
  title: "Compress PDF & Split by Size Online | goodpdf.org",
  description: "Compress PDFs and split by size to fit any limit. Clean output, fast processing, and simple download.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
