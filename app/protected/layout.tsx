import Link from "next/link";
import { EnvVarWarning } from "@/components/env-var-warning";
import { AuthButton } from "@/components/auth-button";
import { ThemeSwitcher } from "@/components/theme-switcher";
import { hasEnvVars } from "@/lib/utils";

export default function ProtectedLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <main className="min-h-screen flex flex-col items-center bg-background">
      <div className="flex-1 w-full flex flex-col gap-10 items-center">
        {/* Top bar with auth + theme */}
        <nav className="w-full border-b border-foreground/10 h-16 flex items-center">
          <div className="w-full max-w-6xl mx-auto flex justify-between items-center px-5">
            <div className="text-sm font-medium opacity-80">
              Dashboard
            </div>
            <div className="flex items-center gap-3">
              {!hasEnvVars ? <EnvVarWarning /> : <AuthButton />}
              <ThemeSwitcher />
            </div>
          </div>
        </nav>

        {/* Tabs */}
        <div className="w-full">
          <div className="w-full max-w-6xl mx-auto px-5">
            <div className="inline-flex rounded-xl border border-foreground/10 bg-muted/40 p-1">
              <TabLink href="/protected/trial-users" label="Trial users" />
              <TabLink href="/protected" label="Pull Lists" />
            </div>
          </div>
        </div>

        {/* Page content */}
        <div className="flex-1 w-full max-w-6xl px-5 pb-20">
          {children}
        </div>
      </div>
    </main>
  );
}

/**
 * Simple server-safe tab link (no active-state JS) with "current" styling
 * applied based on exact href match using the Request URL (if available).
 * If you want true active-state highlighting across nested routes, we can
 * swap this for a small client component using usePathname.
 */
function TabLink({ href, label }: { href: string; label: string }) {
  // Server-side "best effort" active: highlight Pull Lists when at /protected,
  // highlight Trial users when at /protected/trial-users.
  const isPullLists = href === "/protected";
  // Next.js doesn't expose the current pathname in a server component here,
  // so we just style both tabs identically. This keeps layout fully server-friendly.
  // If you want dynamic highlighting, I can provide a tiny client-only tab bar.

  const base =
    "px-5 py-2 text-sm rounded-lg transition hover:bg-background/60 hover:shadow-sm";
  const current =
    "bg-background shadow-sm border border-foreground/10";
  const idle = "border border-transparent";

  // Make Pull Lists look primary by default (since it's the default route)
  const className = `${base} ${isPullLists ? current : idle}`;

  return (
    <Link href={href} className={className} aria-label={label}>
      {label}
    </Link>
  );
}
