// frontend/src/components/ui/StatusBadge.tsx
// Status badge using the app's established color language.
import { cn } from "@/lib/utils";
import type { StageStatus } from "@/lib/pipeline";
import { statusColor } from "@/lib/pipeline";

export function StatusBadge({
  status,
  pulse = false,
  className,
}: {
  status: StageStatus;
  pulse?: boolean;
  className?: string;
}) {
  const c = statusColor(status);
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 px-1.5 py-0.5 text-[10px] font-mono uppercase tracking-wider border",
        c.border,
        c.bg,
        c.text,
        className,
      )}
    >
      <span className={cn("h-1.5 w-1.5", c.dot, pulse && status === "processing" && "animate-pulse")} />
      {c.label}
    </span>
  );
}

// Generic label badge (confidence levels, free-form tags).
export function TagBadge({
  label,
  tone = "muted",
}: {
  label: string;
  tone?: "high" | "medium" | "low" | "green" | "blue" | "muted";
}) {
  const map: Record<string, string> = {
    high: "border-neon-red/50 text-neon-red bg-neon-red/10",
    medium: "border-yellow-500/50 text-yellow-500 bg-yellow-500/10",
    low: "border-border text-muted-foreground bg-white/5",
    green: "border-acid-green/50 text-acid-green bg-acid-green/10",
    blue: "border-blue-400/50 text-blue-400 bg-blue-400/10",
    muted: "border-border text-muted-foreground bg-white/5",
  };
  return (
    <span className={cn("inline-flex items-center px-1.5 py-0.5 text-[10px] font-mono uppercase tracking-wider border", map[tone])}>
      {label}
    </span>
  );
}
