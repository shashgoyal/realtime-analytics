import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Realtime Analytics",
  description: "Event ingestion and analytics dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}>
      <body className="min-h-full flex flex-col">
        <nav className="border-b border-border bg-card sticky top-0 z-10">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center gap-8">
            <span className="font-semibold text-lg tracking-tight">Realtime Analytics</span>
            <div className="flex gap-1">
              <Link
                href="/"
                className="px-3 py-1.5 rounded-md text-sm font-medium hover:bg-accent/10 transition-colors"
              >
                Event Sender
              </Link>
              <Link
                href="/analytics"
                className="px-3 py-1.5 rounded-md text-sm font-medium hover:bg-accent/10 transition-colors"
              >
                Analytics
              </Link>
            </div>
          </div>
        </nav>
        <main className="flex-1">{children}</main>
      </body>
    </html>
  );
}
