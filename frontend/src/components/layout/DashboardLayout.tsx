import { NavLink, Outlet } from "react-router-dom";
import { Activity, ShieldAlert, Terminal, Network, LayoutDashboard } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { to: "/", label: "Dashboard", icon: Activity, end: true },
  { to: "/demo", label: "DS Demo", icon: LayoutDashboard, end: false },
  { to: "/decode", label: "Transaction Detail", icon: Terminal, end: false },
];

export function DashboardLayout() {
  return (
    <div className="min-h-screen bg-background text-foreground flex flex-col font-sans selection:bg-neon-red selection:text-white">
      {/* Top Header */}
      <header className="border-b border-border/50 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 sticky top-0 z-50">
        <div className="flex h-14 items-center px-4 gap-4">
          <div className="flex items-center gap-2 font-bold tracking-tighter shrink-0 select-none">
            <ShieldAlert className="h-5 w-5 text-neon-red animate-pulse" />
            <span className="hidden sm:inline-block">MEMPOOL</span>
            <span className="text-muted-foreground">INTERCEPT</span>
          </div>
          <div className="h-4 w-px bg-border mx-2" />
          <nav className="flex items-center gap-1 text-sm font-medium w-full overflow-x-auto">
            {NAV.map(({ to, label, icon: Icon, end }) => (
              <NavLink
                key={to}
                to={to}
                end={end}
                className={({ isActive }) =>
                  cn(
                    "flex items-center gap-2 px-2.5 py-1.5 whitespace-nowrap border transition-colors",
                    isActive
                      ? "border-foreground/40 bg-white/5 text-foreground"
                      : "border-transparent text-foreground/60 hover:text-foreground/90 hover:bg-white/[0.03]",
                  )
                }
              >
                <Icon className="h-4 w-4" />
                <span className="hidden md:inline">{label}</span>
              </NavLink>
            ))}
          </nav>
          <div className="ml-auto text-xs font-mono text-acid-green animate-pulse hidden lg:flex items-center gap-2 shrink-0">
            <div className="h-2 w-2 rounded-none bg-acid-green" />
            SYSTEM ONLINE
          </div>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 overflow-auto bg-zinc-950/50 relative">
        {/* Subtle grid background for tech feel */}
        <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(to_right,#80808012_1px,transparent_1px),linear-gradient(to_bottom,#80808012_1px,transparent_1px)] bg-[size:24px_24px]" />

        <div className="relative p-6 max-w-[1600px] mx-auto space-y-6">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
