// frontend/src/pages/TransactionFlowPage.tsx
// Interactive, step-by-step visualization of one transaction moving through the
// entire detection pipeline. Fully client-side (demo-mode safe).
import { useMemo, useState } from "react";
import {
  Play, Pause, SkipForward, RotateCcw, GitBranch, ArrowRight,
  CheckCircle2, XCircle, Filter, Loader2, Circle, Gauge,
} from "lucide-react";
import { Panel, FieldLabel } from "@/components/ui/Panel";
import { StatusBadge, TagBadge } from "@/components/ui/StatusBadge";
import { PIPELINE_STAGES, statusColor } from "@/lib/pipeline";
import type { StageStatus } from "@/lib/pipeline";
import { SAMPLE_TRANSACTIONS } from "@/lib/sampleTransactions";
import type { SampleTx } from "@/lib/sampleTransactions";
import { useSimulation } from "@/hooks/useSimulation";
import { cn } from "@/lib/utils";

function confTone(c: SampleTx["confidence"]) {
  return c === "HIGH" ? "high" : c === "MEDIUM" ? "medium" : c === "LOW" ? "low" : "muted";
}

function StatusIcon({ status }: { status: StageStatus }) {
  const c = statusColor(status);
  if (status === "success") return <CheckCircle2 className={cn("h-4 w-4", c.text)} />;
  if (status === "failed") return <XCircle className={cn("h-4 w-4", c.text)} />;
  if (status === "filtered") return <Filter className={cn("h-4 w-4", c.text)} />;
  if (status === "processing") return <Loader2 className={cn("h-4 w-4 animate-spin", c.text)} />;
  return <Circle className={cn("h-4 w-4", c.text)} />;
}

export default function TransactionFlowPage() {
  const [sample, setSample] = useState<SampleTx>(SAMPLE_TRANSACTIONS[0]);
  const sim = useSimulation(sample);

  const stages = useMemo(
    () => PIPELINE_STAGES.map((stage, index) => ({ stage, index })),
    [],
  );

  const reachedCount = sim.progress + 1;
  const totalReachable = sim.lastIndex + 1;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
          <GitBranch className="h-6 w-6 text-acid-green" />
          Transaction Flow
        </h1>
        <p className="text-sm text-muted-foreground font-mono">
          Follow one transaction from the mock node through every stage of the distributed pipeline.
        </p>
      </div>

      {/* Sample picker */}
      <Panel title="Example Transaction" icon={<GitBranch className="h-4 w-4" />}>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-2">
          {SAMPLE_TRANSACTIONS.map((s) => {
            const selected = s.id === sample.id;
            const tone =
              s.badge === "FILTERED" ? "medium" :
              s.badge === "FAIL" ? "high" :
              s.badge === "HIGH" ? "green" :
              s.badge === "LOW" ? "low" : "muted";
            return (
              <button
                key={s.id}
                onClick={() => setSample(s)}
                className={cn(
                  "text-left border p-3 transition-colors group",
                  selected
                    ? "border-foreground/60 bg-white/5"
                    : "border-border/50 hover:border-foreground/30 hover:bg-white/[0.02]",
                )}
              >
                <div className="flex items-center justify-between mb-2">
                  <TagBadge label={s.badge} tone={tone as never} />
                  {selected && <span className="h-1.5 w-1.5 bg-acid-green" />}
                </div>
                <p className="text-xs font-mono leading-snug text-foreground">{s.label}</p>
              </button>
            );
          })}
        </div>
      </Panel>

      {/* Selected tx meta + controls */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Panel title="Selected Transaction" className="lg:col-span-2" corners>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="col-span-2 md:col-span-4">
              <FieldLabel>Tx Hash</FieldLabel>
              <p className="font-mono text-xs break-all text-foreground">{sample.txHash}</p>
            </div>
            <div>
              <FieldLabel>Protocol</FieldLabel>
              <p className="font-mono text-sm">{sample.protocol.toUpperCase()}</p>
            </div>
            <div>
              <FieldLabel>Selector</FieldLabel>
              <p className="font-mono text-sm">{sample.selector}</p>
            </div>
            <div>
              <FieldLabel>Token</FieldLabel>
              <p className="font-mono text-sm">{sample.token}</p>
            </div>
            <div>
              <FieldLabel>Confidence</FieldLabel>
              <TagBadge label={sample.confidence} tone={confTone(sample.confidence) as never} />
            </div>
            <div className="col-span-2 md:col-span-4 border-t border-border/30 pt-3">
              <FieldLabel>What happens</FieldLabel>
              <p className="text-xs text-muted-foreground leading-relaxed">{sample.description}</p>
            </div>
          </div>
        </Panel>

        {/* Controls */}
        <Panel title="Simulation" icon={<Gauge className="h-4 w-4" />}>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-2">
              {!sim.running ? (
                <button
                  onClick={sim.play}
                  className="flex items-center justify-center gap-2 border border-acid-green/50 bg-acid-green/10 text-acid-green hover:bg-acid-green/20 py-2 text-xs font-mono uppercase tracking-wider transition-colors"
                >
                  <Play className="h-3.5 w-3.5" /> {sim.done ? "Replay" : "Play"}
                </button>
              ) : (
                <button
                  onClick={sim.pause}
                  className="flex items-center justify-center gap-2 border border-yellow-500/50 bg-yellow-500/10 text-yellow-500 hover:bg-yellow-500/20 py-2 text-xs font-mono uppercase tracking-wider transition-colors"
                >
                  <Pause className="h-3.5 w-3.5" /> Pause
                </button>
              )}
              <button
                onClick={sim.step}
                disabled={sim.done}
                className="flex items-center justify-center gap-2 border border-border/50 hover:border-foreground/40 hover:bg-white/5 py-2 text-xs font-mono uppercase tracking-wider transition-colors disabled:opacity-40 disabled:pointer-events-none"
              >
                <SkipForward className="h-3.5 w-3.5" /> Step
              </button>
            </div>
            <button
              onClick={sim.reset}
              className="w-full flex items-center justify-center gap-2 border border-border/50 hover:border-foreground/40 hover:bg-white/5 py-2 text-xs font-mono uppercase tracking-wider transition-colors"
            >
              <RotateCcw className="h-3.5 w-3.5" /> Reset
            </button>

            <div>
              <div className="flex items-center justify-between mb-2">
                <FieldLabel>Speed</FieldLabel>
                <span className="text-xs font-mono text-muted-foreground">{sim.speed}×</span>
              </div>
              <div className="grid grid-cols-3 gap-1">
                {[0.5, 1, 2].map((s) => (
                  <button
                    key={s}
                    onClick={() => sim.setSpeed(s)}
                    className={cn(
                      "py-1.5 text-xs font-mono border transition-colors",
                      sim.speed === s
                        ? "border-foreground/60 bg-white/5 text-foreground"
                        : "border-border/50 text-muted-foreground hover:border-foreground/30",
                    )}
                  >
                    {s}×
                  </button>
                ))}
              </div>
            </div>

            <div className="border-t border-border/30 pt-3">
              <div className="flex items-center justify-between text-xs font-mono">
                <span className="text-muted-foreground">PROGRESS</span>
                <span className="text-foreground">{Math.max(0, reachedCount)} / {totalReachable}</span>
              </div>
              <div className="mt-2 h-1 bg-zinc-800 overflow-hidden">
                <div
                  className="h-full bg-acid-green transition-all duration-300"
                  style={{ width: `${(Math.max(0, reachedCount) / totalReachable) * 100}%` }}
                />
              </div>
            </div>
          </div>
        </Panel>
      </div>

      {/* Stage stepper */}
      <Panel
        title="Pipeline Stages"
        icon={<ArrowRight className="h-4 w-4" />}
        right={
          <span className="text-xs font-mono text-muted-foreground">
            {PIPELINE_STAGES.length} stages
          </span>
        }
        bodyClassName="p-0"
      >
        <div className="divide-y divide-border/30">
          {stages.map(({ stage, index }) => {
            const status = sim.statusFor(stage.id, index);
            const outcome = sample.outcomes[stage.id];
            const reached = index <= sim.lastIndex;
            const isActive = index === sim.active;
            const c = statusColor(status);
            const dim = status === "waiting";

            return (
              <div
                key={stage.id}
                className={cn(
                  "px-4 py-3 transition-colors relative",
                  isActive && "bg-blue-400/[0.04]",
                  status === "success" && "bg-acid-green/[0.02]",
                  status === "failed" && "bg-neon-red/[0.03]",
                  status === "filtered" && "bg-yellow-500/[0.03]",
                )}
              >
                {isActive && <div className="absolute top-0 left-0 w-0.5 h-full bg-blue-400" />}
                <div className="flex items-start gap-3">
                  {/* index + icon */}
                  <div className="flex flex-col items-center pt-0.5 shrink-0 w-6">
                    <StatusIcon status={status} />
                    <span className="text-[10px] font-mono text-muted-foreground/50 mt-1">
                      {String(index + 1).padStart(2, "0")}
                    </span>
                  </div>

                  {/* body */}
                  <div className={cn("flex-1 min-w-0", dim && "opacity-45")}>
                    <div className="flex items-center justify-between gap-2 flex-wrap">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm font-medium text-foreground">{stage.name}</span>
                        <span className="text-[10px] font-mono text-muted-foreground border border-border/40 px-1.5 py-0.5">
                          {stage.component}
                        </span>
                      </div>
                      <StatusBadge status={status} pulse />
                    </div>

                    <p className="text-xs text-muted-foreground mt-1.5 leading-relaxed">
                      {stage.explanation}
                    </p>

                    {/* IO row — only show once reached & resolved */}
                    {reached && outcome && index <= sim.progress && (
                      <div className="mt-2 grid grid-cols-1 md:grid-cols-[1fr_auto_1fr] gap-2 items-center bg-black/30 border border-border/40 p-2">
                        <div className="min-w-0">
                          <FieldLabel>Input</FieldLabel>
                          <p className="font-mono text-[11px] text-zinc-300 break-all">{outcome.input}</p>
                        </div>
                        <ArrowRight className="h-3.5 w-3.5 text-muted-foreground/50 hidden md:block" />
                        <div className="min-w-0">
                          <FieldLabel>Output</FieldLabel>
                          <p className={cn("font-mono text-[11px] break-all", c.text)}>{outcome.output}</p>
                        </div>
                        {outcome.note && (
                          <p className="md:col-span-3 text-[11px] text-muted-foreground/80 italic border-t border-border/30 pt-1.5 mt-0.5">
                            ⓘ {outcome.note}
                          </p>
                        )}
                      </div>
                    )}
                  </div>

                  {/* latency */}
                  {reached && outcome && index <= sim.progress && (
                    <div className="shrink-0 text-right">
                      <span className="text-[11px] font-mono text-muted-foreground">
                        {outcome.latencyMs < 1
                          ? `${(outcome.latencyMs * 1000).toFixed(0)}µs`
                          : `${outcome.latencyMs.toFixed(outcome.latencyMs < 10 ? 1 : 0)}ms`}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </Panel>
    </div>
  );
}
