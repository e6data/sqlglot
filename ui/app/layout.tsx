import type { Metadata } from "next";
import { Sidebar } from "@/components/navigation/Sidebar";
import "./globals.css";

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: "Transpiler - E6 SQL Converter",
  description: "Convert SQL queries to E6 dialect",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <Sidebar />
        <main className="ml-64 overflow-auto">{children}</main>
      </body>
    </html>
  );
}
