// frontend/src/components/ui/Panel.tsx
// Reusable bordered panel matching the existing dashboard aesthetic:
// sharp corners, mono uppercase header, decorative corner ticks, grid-friendly.
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface PanelProps {
  title?: ReactNode;
  icon?: ReactNode;
  right?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
  corners?: boolean;
}

export function Panel({
  title,
  icon,
  right,
  children,
  className,
  bodyClassName,
  corners = false,
}: PanelProps) {
  return (
    <div
      className={cn(
        "relative border border-border/50 bg-background/30 backdrop-blur-sm flex flex-col",
        className,
      )}
    >
      {corners && (
        <>
          <div className="absolute top-0 left-0 w-2 h-2 border-t border-l border-foreground opacity-50 z-10" />
          <div className="absolute top-0 right-0 w-2 h-2 border-t border-r border-foreground opacity-50 z-10" />
          <div className="absolute bottom-0 left-0 w-2 h-2 border-b border-l border-foreground opacity-50 z-10" />
          <div className="absolute bottom-0 right-0 w-2 h-2 border-b border-r border-foreground opacity-50 z-10" />
        </>
      )}

      {(title || right) && (
        <div className="border-b border-border/50 p-3 flex items-center justify-between bg-zinc-900/50 z-10">
          <h3 className="text-sm font-mono tracking-widest uppercase flex items-center gap-2 text-muted-foreground">
            {icon}
            {title}
          </h3>
          {right}
        </div>
      )}

      <div className={cn("relative", bodyClassName ?? "p-4")}>{children}</div>
    </div>
  );
}

// Small uppercase mono label used throughout the new pages.
export function FieldLabel({ children }: { children: ReactNode }) {
  return (
    <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest mb-1">
      {children}
    </p>
  );
}
