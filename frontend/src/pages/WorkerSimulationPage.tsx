// frontend/src/pages/WorkerSimulationPage.tsx
// Interactive distributed-systems sandbox: scale workers/partitions, inject
// crashes, and watch throughput / queueing / partition assignment respond.
import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  BarChart, Bar, Cell,
} from "recharts";
import {
  Cpu, Server, Layers, Timer, Zap, Activity, Database,
  AlertOctagon, RotateCcw, Box,
} from "lucide-react";
import { Panel, FieldLabel } from "@/components/ui/Panel";
import { cn } from "@/lib/utils";
import type { WorkerControls } from "@/lib/workerModel";
import { computeMetrics, bottleneckLabel } from "@/lib/workerModel";

const PARTITION_COLORS = ["#84cc16", "#3b82f6", "#eab308", "#ef4444"];

function Stepper({
  label, value, min, max, onChange, icon, hint,
}: {
  label: string; value: number; min: number; max: number;
  onChange: (v: number) => void; icon: ReactNode; hint?: string;
}) {
  return (
    <div className="border border-border/50 p-3 bg-background/30">
      <div className="flex items-center gap-2 mb-2">
        {icon}
        <span className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">{label}</span>
      </div>
      <div className="flex items-center gap-2">
        <button
          onClick={() => onChange(Math.max(min, value - 1))}
          className="h-7 w-7 border border-border/50 hover:border-foreground/40 hover:bg-white/5 font-mono text-sm transition-colors"
        >−</button>
        <span className="flex-1 text-center font-mono text-lg tabular-nums">{value}</span>
        <button
          onClick={() => onChange(Math.min(max, value + 1))}
          className="h-7 w-7 border border-border/50 hover:border-foreground/40 hover:bg-white/5 font-mono text-sm transition-colors"
        >+</button>
      </div>
      {hint && <p className="text-[10px] text-muted-foreground/70 mt-2 leading-tight">{hint}</p>}
    </div>
  );
}

function Metric({ label, value, unit, tone = "default", sub }: {
  label: string; value: string | number; unit?: string;
  tone?: "default" | "green" | "red" | "yellow" | "blue"; sub?: string;
}) {
  const toneMap = {
    default: "text-foreground", green: "text-acid-green", red: "text-neon-red",
    yellow: "text-yellow-500", blue: "text-blue-400",
  };
  return (
    <div className="border border-border/50 bg-background/30 p-3">
      <FieldLabel>{label}</FieldLabel>
      <p className={cn("font-mono text-xl tabular-nums", toneMap[tone])}>
        {value}{unit && <span className="text-xs text-muted-foreground ml-1">{unit}</span>}
      </p>
      {sub && <p className="text-[10px] text-muted-foreground/70 mt-0.5">{sub}</p>}
    </div>
  );
}

export default function WorkerSimulationPage() {
  const [controls, setControls] = useState<WorkerControls>({
    ingestionWorkers: 4,
    sparkWorkers: 4,
    partitions: 4,
    rpcDelayMs: 100,
    txRate: 80,
    cacheHitRate: 0.8,
    crashedIngestion: 0,
    crashedSpark: 0,
  });

  const set = (patch: Partial<WorkerControls>) => setControls((c) => ({ ...c, ...patch }));
  const metrics = useMemo(() => computeMetrics(controls), [controls]);

  // Live throughput history (jitter around the computed steady-state value).
  const [history, setHistory] = useState<{ t: number; tput: number; offered: number }[]>([]);
  const tick = useRef(0);
  useEffect(() => {
    const id = setInterval(() => {
      tick.current += 1;
      const jitter = (Math.random() - 0.5) * Math.max(2, metrics.throughput * 0.06);
      setHistory((h) => {
        const next = [...h, {
          t: tick.current,
          tput: Math.max(0, +(metrics.throughput + jitter).toFixed(1)),
          offered: controls.txRate,
        }];
        return next.slice(-40);
      });
    }, 700);
    return () => clearInterval(id);
  }, [metrics.throughput, controls.txRate]);

  const ingLoadData = Array.from({ length: controls.ingestionWorkers }, (_, i) => {
    const crashed = i >= metrics.activeIngestion;
    return {
      name: `I${i}`,
      load: crashed ? 0 : +metrics.perIngestionLoad.toFixed(1),
      crashed,
    };
  });

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
          <Cpu className="h-6 w-6 text-acid-green" />
          Worker Simulation
        </h1>
        <p className="text-sm text-muted-foreground font-mono">
          Scale workers and partitions to see when parallelism helps — and when it doesn't.
        </p>
      </div>

      {/* Controls */}
      <Panel title="Controls" icon={<Activity className="h-4 w-4" />}>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          <Stepper label="Ingestion Workers" value={controls.ingestionWorkers} min={1} max={8}
            onChange={(v) => set({ ingestionWorkers: v, crashedIngestion: Math.min(controls.crashedIngestion, v) })}
            icon={<Server className="h-3.5 w-3.5 text-acid-green" />}
            hint="Parallel RPC fetchers" />
          <Stepper label="Spark Workers" value={controls.sparkWorkers} min={1} max={8}
            onChange={(v) => set({ sparkWorkers: v, crashedSpark: Math.min(controls.crashedSpark, v) })}
            icon={<Cpu className="h-3.5 w-3.5 text-blue-400" />}
            hint="Capped by partitions" />
          <Stepper label="Kafka Partitions" value={controls.partitions} min={1} max={4}
            onChange={(v) => set({ partitions: v })}
            icon={<Layers className="h-3.5 w-3.5 text-yellow-500" />}
            hint="Ceiling on Spark parallelism" />
          <div className="border border-border/50 p-3 bg-background/30">
            <div className="flex items-center gap-2 mb-2">
              <Timer className="h-3.5 w-3.5 text-neon-red" />
              <span className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">RPC Delay</span>
            </div>
            <input type="range" min={10} max={300} step={10} value={controls.rpcDelayMs}
              onChange={(e) => set({ rpcDelayMs: +e.target.value })}
              className="w-full accent-neon-red" />
            <p className="text-center font-mono text-sm mt-1">{controls.rpcDelayMs}ms</p>
          </div>
          <div className="border border-border/50 p-3 bg-background/30">
            <div className="flex items-center gap-2 mb-2">
              <Zap className="h-3.5 w-3.5 text-acid-green" />
              <span className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">Tx Rate</span>
            </div>
            <input type="range" min={10} max={300} step={10} value={controls.txRate}
              onChange={(e) => set({ txRate: +e.target.value })}
              className="w-full accent-acid-green" />
            <p className="text-center font-mono text-sm mt-1">{controls.txRate}/s</p>
          </div>
          <div className="border border-border/50 p-3 bg-background/30">
            <div className="flex items-center gap-2 mb-2">
              <Database className="h-3.5 w-3.5 text-blue-400" />
              <span className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">Cache Hit</span>
            </div>
            <input type="range" min={0} max={100} step={5} value={Math.round(controls.cacheHitRate * 100)}
              onChange={(e) => set({ cacheHitRate: +e.target.value / 100 })}
              className="w-full accent-blue-400" />
            <p className="text-center font-mono text-sm mt-1">{Math.round(controls.cacheHitRate * 100)}%</p>
          </div>
        </div>

        {/* Fault injection */}
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            onClick={() => set({ crashedIngestion: Math.min(controls.ingestionWorkers, controls.crashedIngestion + 1) })}
            disabled={metrics.activeIngestion <= 1}
            className="flex items-center gap-2 border border-neon-red/50 bg-neon-red/10 text-neon-red hover:bg-neon-red/20 px-3 py-1.5 text-xs font-mono uppercase tracking-wider transition-colors disabled:opacity-40 disabled:pointer-events-none"
          >
            <AlertOctagon className="h-3.5 w-3.5" /> Crash ingestion worker
          </button>
          <button
            onClick={() => set({ crashedSpark: Math.min(controls.sparkWorkers, controls.crashedSpark + 1) })}
            disabled={metrics.activeSpark <= 1}
            className="flex items-center gap-2 border border-neon-red/50 bg-neon-red/10 text-neon-red hover:bg-neon-red/20 px-3 py-1.5 text-xs font-mono uppercase tracking-wider transition-colors disabled:opacity-40 disabled:pointer-events-none"
          >
            <AlertOctagon className="h-3.5 w-3.5" /> Crash spark worker
          </button>
          <button
            onClick={() => set({ crashedIngestion: 0, crashedSpark: 0 })}
            disabled={controls.crashedIngestion === 0 && controls.crashedSpark === 0}
            className="flex items-center gap-2 border border-border/50 hover:border-foreground/40 hover:bg-white/5 px-3 py-1.5 text-xs font-mono uppercase tracking-wider transition-colors disabled:opacity-40 disabled:pointer-events-none"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Restore all
          </button>
        </div>
      </Panel>

      {/* Bottleneck banner */}
      <div className={cn(
        "border p-4 flex items-center justify-between gap-4",
        metrics.bottleneck === "ingestion" ? "border-neon-red/40 bg-neon-red/5" :
        metrics.bottleneck === "kafka_spark" ? "border-yellow-500/40 bg-yellow-500/5" :
        "border-acid-green/40 bg-acid-green/5",
      )}>
        <div className="flex items-center gap-3">
          <Activity className={cn("h-5 w-5",
            metrics.bottleneck === "ingestion" ? "text-neon-red" :
            metrics.bottleneck === "kafka_spark" ? "text-yellow-500" : "text-acid-green",
          )} />
          <div>
            <FieldLabel>Current Bottleneck</FieldLabel>
            <p className="font-mono text-sm font-semibold">{bottleneckLabel(metrics.bottleneck)}</p>
          </div>
        </div>
        <div className="text-right">
          <FieldLabel>Throughput</FieldLabel>
          <p className="font-mono text-2xl text-acid-green tabular-nums">{metrics.throughput.toFixed(0)}<span className="text-sm text-muted-foreground ml-1">tx/s</span></p>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Metric label="Ingestion Capacity" value={metrics.ingestionCapacity.toFixed(0)} unit="tx/s" tone="green"
          sub={`${metrics.activeIngestion} active × ${(1000 / controls.rpcDelayMs).toFixed(1)}/s`} />
        <Metric label="Spark Capacity" value={metrics.sparkCapacity.toFixed(0)} unit="tx/s" tone="blue"
          sub={`${metrics.effectiveSparkParallelism} effective × ${(1000 / metrics.procTimeMs).toFixed(0)}/s`} />
        <Metric label="Queue Depth" value={metrics.queueDepth} unit="msg" tone={metrics.queueDepth > 0 ? "yellow" : "default"}
          sub={metrics.queueDepth > 0 ? "backlog building" : "draining cleanly"} />
        <Metric label="End-to-End Latency" value={metrics.endToEndLatencyMs.toFixed(0)} unit="ms"
          tone={metrics.endToEndLatencyMs > 250 ? "red" : "default"} />
        <Metric label="Cache Hits" value={metrics.cacheHits.toFixed(0)} unit="/s" tone="blue" />
        <Metric label="Cache Misses" value={metrics.cacheMisses.toFixed(0)} unit="/s"
          tone={metrics.cacheMisses > metrics.cacheHits ? "yellow" : "default"} />
        <Metric label="Mongo Writes" value={metrics.mongoWrites.toFixed(0)} unit="/s" />
        <Metric label="Retried" value={metrics.retriedPerSec} unit="/s"
          tone={metrics.retriedPerSec > 0 ? "red" : "default"}
          sub={metrics.retriedPerSec > 0 ? "replayed from offset" : "none"} />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Panel title="Throughput vs Offered Load" icon={<Activity className="h-4 w-4" />} bodyClassName="p-4 h-[280px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={history} margin={{ top: 5, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="t" tick={{ fontSize: 10, fontFamily: "monospace", fill: "#71717a" }} />
              <YAxis tick={{ fontSize: 10, fontFamily: "monospace", fill: "#71717a" }} />
              <Tooltip
                contentStyle={{ background: "#09090b", border: "1px solid #27272a", fontFamily: "monospace", fontSize: 11 }}
                labelStyle={{ color: "#a1a1aa" }}
              />
              <Line type="monotone" dataKey="offered" stroke="#52525b" strokeWidth={1} strokeDasharray="4 4" dot={false} isAnimationActive={false} name="Offered" />
              <Line type="monotone" dataKey="tput" stroke="#84cc16" strokeWidth={2} dot={false} isAnimationActive={false} name="Throughput" />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Per-Ingestion-Worker Load" icon={<Server className="h-4 w-4" />} bodyClassName="p-4 h-[280px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={ingLoadData} margin={{ top: 5, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="name" tick={{ fontSize: 10, fontFamily: "monospace", fill: "#71717a" }} />
              <YAxis tick={{ fontSize: 10, fontFamily: "monospace", fill: "#71717a" }} />
              <Tooltip
                contentStyle={{ background: "#09090b", border: "1px solid #27272a", fontFamily: "monospace", fontSize: 11 }}
                cursor={{ fill: "#ffffff08" }}
              />
              <Bar dataKey="load" isAnimationActive={false}>
                {ingLoadData.map((d, i) => (
                  <Cell key={i} fill={d.crashed ? "#3f1d1d" : "#84cc16"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Panel>
      </div>

      {/* Partition assignment */}
      <Panel title="Kafka → Spark Partition Assignment" icon={<Layers className="h-4 w-4" />}>
        <div className="space-y-3">
          <div className="flex flex-wrap gap-2">
            {Array.from({ length: controls.partitions }, (_, p) => (
              <div key={p} className="flex items-center gap-2 border border-border/50 px-2 py-1">
                <span className="h-2 w-2" style={{ background: PARTITION_COLORS[p % PARTITION_COLORS.length] }} />
                <span className="font-mono text-xs text-muted-foreground">partition {p}</span>
              </div>
            ))}
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {metrics.partitionAssignment.map((parts, i) => {
              const crashed = i >= metrics.activeSpark;
              const idle = !crashed && parts.length === 0;
              return (
                <div key={i} className={cn(
                  "border p-3 relative",
                  crashed ? "border-neon-red/40 bg-neon-red/5" :
                  idle ? "border-border/30 bg-background/20" :
                  "border-blue-400/40 bg-blue-400/5",
                )}>
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-1.5">
                      <Box className="h-3.5 w-3.5 text-muted-foreground" />
                      <span className="font-mono text-xs">spark-{i}</span>
                    </div>
                    <span className={cn("text-[9px] font-mono uppercase tracking-wider px-1 py-0.5 border",
                      crashed ? "border-neon-red/50 text-neon-red" :
                      idle ? "border-yellow-500/50 text-yellow-500" :
                      "border-acid-green/50 text-acid-green",
                    )}>
                      {crashed ? "crashed" : idle ? "idle" : "active"}
                    </span>
                  </div>
                  {crashed ? (
                    <p className="text-[10px] font-mono text-neon-red/70">partitions reassigned</p>
                  ) : idle ? (
                    <p className="text-[10px] font-mono text-muted-foreground/70">no partition — surplus worker</p>
                  ) : (
                    <div className="flex flex-wrap gap-1">
                      {parts.map((p) => (
                        <span key={p} className="font-mono text-[10px] px-1.5 py-0.5"
                          style={{ background: `${PARTITION_COLORS[p % PARTITION_COLORS.length]}22`, color: PARTITION_COLORS[p % PARTITION_COLORS.length] }}>
                          P{p}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          {metrics.idleSparkWorkers > 0 && (
            <div className="border border-yellow-500/40 bg-yellow-500/5 p-3 text-xs font-mono text-yellow-500">
              ⚠ {metrics.idleSparkWorkers} Spark worker{metrics.idleSparkWorkers > 1 ? "s" : ""} sitting idle — there are only {controls.partitions} partitions, so extra consumers in the group get no work. Spark parallelism is capped at the partition count.
            </div>
          )}
        </div>
      </Panel>

      {/* Teaching callouts */}
      <Panel title="Why This Happens" icon={<Cpu className="h-4 w-4" />}>
        <ul className="space-y-2 text-xs text-muted-foreground leading-relaxed">
          <li className="flex gap-2"><span className="text-acid-green">▸</span> More ingestion workers raise throughput <span className="text-foreground">only while ingestion is the bottleneck</span> and the work is parallelizable (independent RPC fetches). Once Spark or offered load binds, adding fetchers does nothing.</li>
          <li className="flex gap-2"><span className="text-yellow-500">▸</span> Kafka partition count is a <span className="text-foreground">hard ceiling on Spark consumer parallelism</span>. With {controls.partitions} partitions, at most {controls.partitions} Spark workers do work concurrently.</li>
          <li className="flex gap-2"><span className="text-blue-400">▸</span> Spark workers beyond the partition count stay <span className="text-foreground">idle</span> — they join the consumer group but receive no partition.</li>
          <li className="flex gap-2"><span className="text-blue-400">▸</span> Higher Redis cache-hit rate lowers per-record processing time, raising Spark capacity without adding workers.</li>
          <li className="flex gap-2"><span className="text-neon-red">▸</span> A crashed worker's partitions are <span className="text-foreground">reassigned</span> and its in-flight records replayed from the last committed offset — throughput dips but no data is lost.</li>
        </ul>
      </Panel>
    </div>
  );
}
