// frontend/src/pages/DemoPage.tsx
//
// Live-presentation page — 6 distributed system characteristics.
// All animations are client-side only; no Docker containers are touched.
// Real measured data is embedded from DISTRIBUTED_SYSTEM_ANALYSIS.md.

import { useEffect, useRef, useState, useCallback } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ReferenceLine, ResponsiveContainer,
} from "recharts";
import {
  Play, RotateCcw, ShieldAlert, Zap, Activity, Network, Cpu, Terminal,
  Plus, Minus, Database,
} from "lucide-react";

// ── palette ──────────────────────────────────────────────────────────────────
const BG    = "#0f1419";
const PNL   = "#161d27";
const LINE  = "#2a3441";
const TEXT  = "#e7ebf0";
const MUT   = "#8a96a6";
const GOLD  = "#ffd24a";
const BLUE  = "#4a9eff";
const GREEN = "#5fd07a";
const RED   = "#ff5d5d";
const AMBER = "#f0a93b";
const PURPLE = "#b08cf5";

// ── sub-nav anchors ───────────────────────────────────────────────────────────
const SECTIONS = [
  { id: "concurrency",           label: "Concurrency" },
  { id: "message-passing",       label: "Message Passing" },
  { id: "fault-tolerance",       label: "Fault Tolerance" },
  { id: "scalability",           label: "Scalability" },
  { id: "location-transparency", label: "Location Transparency" },
  { id: "no-spof",               label: "No SPOF" },
];

// ── helpers ───────────────────────────────────────────────────────────────────
function Card({ children, id }: { children: React.ReactNode; id: string }) {
  return (
    <section id={id} className="rounded-[16px] overflow-hidden" style={{ background: PNL, border: `1px solid ${LINE}` }}>
      {children}
    </section>
  );
}

function SectionHeader({ num, title, badge }: { num: string; title: string; badge?: string }) {
  return (
    <div className="flex items-center justify-between px-6 pt-6 pb-4">
      <div className="flex items-center gap-3">
        <span className="text-[11px] font-mono px-2 py-0.5 rounded" style={{ background: LINE, color: MUT }}>{num}</span>
        <h2 className="text-[18px] font-semibold tracking-tight" style={{ color: TEXT }}>{title}</h2>
      </div>
      {badge && (
        <span className="text-[11px] font-mono px-2.5 py-1 rounded" style={{ background: "#1c2531", color: GOLD, border: `1px solid ${LINE}` }}>
          {badge}
        </span>
      )}
    </div>
  );
}

function Divider() {
  return <div style={{ height: 1, background: LINE, margin: "0 24px" }} />;
}

function Oneliner({ children }: { children: React.ReactNode }) {
  return (
    <p className="px-6 py-3 text-[13px] leading-relaxed" style={{ color: MUT }}>
      {children}
    </p>
  );
}

function Evidence({ lines }: { lines: string[] }) {
  return (
    <div className="px-6 py-4 text-[11.5px] leading-loose" style={{ fontFamily: "monospace", color: MUT, background: BG, borderTop: `1px solid ${LINE}` }}>
      {lines.map((l, i) => (
        <div key={i} style={{ color: l.startsWith("#") ? MUT + "88" : (l.includes("→") || l.includes("✅") || l.includes("✓") ? TEXT : MUT) }}>
          {l}
        </div>
      ))}
    </div>
  );
}

// ── §1 Concurrency — static pipeline SVG ─────────────────────────────────────
function ConcurrencyVisual() {
  const SVGNS = "http://www.w3.org/2000/svg";
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;
    svg.innerHTML = "";

    const el = (tag: string, attrs: Record<string, string | number>) => {
      const e = document.createElementNS(SVGNS, tag);
      for (const k in attrs) e.setAttribute(k, String(attrs[k]));
      return e;
    };
    const txt = (x: number, y: number, s: string, fill: string, size = 12) => {
      const t = el("text", { x, y, "text-anchor": "middle", "dominant-baseline": "central", fill, "font-size": size });
      t.textContent = s;
      return t;
    };
    const box = (x: number, y: number, w: number, h: number, fill: string, stroke: string, rx = 8) =>
      el("rect", { x, y, width: w, height: h, rx, fill, stroke, "stroke-width": 1 });
    const fillFor = (hex: string) => hex + "22";

    const LANES = [60, 130, 200, 270];

    // Mock node
    svg.appendChild(box(10, 130, 80, 50, fillFor(BLUE), BLUE));
    svg.appendChild(txt(50, 155, "Mock node", TEXT, 10));

    // listener_mp.py (outer)
    svg.appendChild(box(110, 50, 130, 230, fillFor(BLUE), BLUE, 10));
    svg.appendChild(txt(175, 64, "listener_mp.py", BLUE, 9));
    svg.appendChild(box(125, 76, 100, 28, fillFor(BLUE), BLUE, 5));
    svg.appendChild(txt(175, 90, "feeder", TEXT, 10));
    LANES.forEach((y, i) => {
      svg.appendChild(box(130, y + 120, 90, 24, fillFor(BLUE), BLUE, 4));
      svg.appendChild(txt(175, y + 132, `worker-${i}`, TEXT, 9));
    });
    svg.appendChild(el("line", { x1: 90, y1: 155, x2: 110, y2: 155, stroke: LINE, "stroke-width": 1.5, "marker-end": "url(#demo-ar)" }));

    // Kafka
    svg.appendChild(box(260, 40, 110, 240, fillFor(AMBER), AMBER, 10));
    svg.appendChild(txt(315, 56, "Kafka", AMBER, 11));
    svg.appendChild(txt(315, 70, "raw_txns", MUT, 9));
    LANES.forEach((y, i) => {
      svg.appendChild(box(270, y + 50, 90, 28, fillFor(AMBER), AMBER, 5));
      svg.appendChild(txt(315, y + 64, `Partition ${i}`, TEXT, 9));
      svg.appendChild(el("line", { x1: 220, y1: LANES[i] + 132, x2: 270, y2: LANES[i] + 64, stroke: LINE, "stroke-width": 1, "marker-end": "url(#demo-ar)" }));
    });

    // Spark master
    svg.appendChild(box(400, 40, 130, 40, fillFor(PURPLE), PURPLE, 8));
    svg.appendChild(txt(465, 60, "Spark Master", TEXT, 10));

    LANES.forEach((y, i) => {
      svg.appendChild(box(400, y + 50, 130, 28, fillFor(GREEN), GREEN, 5));
      svg.appendChild(txt(465, y + 64, `Spark Worker ${i + 1}`, TEXT, 9));
      svg.appendChild(el("line", { x1: 360, y1: LANES[i] + 64, x2: 400, y2: LANES[i] + 64, stroke: LINE, "stroke-width": 1, "marker-end": "url(#demo-ar)" }));
      svg.appendChild(el("line", { x1: 530, y1: LANES[i] + 64, x2: 570, y2: 160, stroke: LINE, "stroke-width": 1, "marker-end": "url(#demo-ar)" }));
    });

    svg.appendChild(box(570, 120, 110, 80, fillFor(PURPLE), PURPLE, 8));
    svg.appendChild(txt(625, 160, "MongoDB", TEXT, 10));

    const defs = el("defs", {});
    const m = el("marker", { id: "demo-ar", viewBox: "0 0 10 10", refX: "8", refY: "5", markerWidth: "5", markerHeight: "5", orient: "auto-start-reverse" });
    const path = el("path", { d: "M2 1L8 5L2 9", fill: "none", stroke: "#2a3441", "stroke-width": "1.4" });
    m.appendChild(path);
    defs.appendChild(m);
    svg.insertBefore(defs, svg.firstChild);
  }, []);

  return (
    <div className="px-6 py-4">
      <svg ref={svgRef} viewBox="0 0 700 310" className="w-full h-auto" style={{ maxHeight: 280 }} />
      <p className="text-[11px] mt-2" style={{ color: MUT, fontFamily: "monospace" }}>
        4 Spark workers process partitions in parallel — each executes its UDF chain independently at the same time.
      </p>
    </div>
  );
}

// ── §2 Message Passing — dot trace animation ──────────────────────────────────
const TRACE_STAGES = [
  { label: "Listener", x: 60,  detail: '{ tx_hash: "0xabc...", input: "0xab9c4b5d..." }' },
  { label: "Kafka",    x: 200, detail: "topic=raw_txns  partition=2  offset=4821" },
  { label: "Spark",    x: 340, detail: "flashLoanSimple  amount_usd=$1.2M  confidence=HIGH" },
  { label: "FastAPI",  x: 480, detail: 'event: detections  data: [{ tx_hash, confidence }]' },
  { label: "React",    x: 620, detail: "Card shown in live detections table ✓" },
];

const TRACE_PLACEHOLDER = 'Press "Trace a transaction" to start →';

function MessagePassingVisual() {
  const [dotT, setDotT] = useState(0);
  const [active, setActive] = useState(-1);
  const rafRef = useRef<number>(0);
  const startRef = useRef<number>(0);
  const runningRef = useRef(false);
  const DURATION = 3000;

  const startAnim = useCallback(() => {
    if (runningRef.current) return;
    runningRef.current = true;
    setDotT(0); setActive(-1);
    startRef.current = performance.now();
    const tick = (now: number) => {
      const t = Math.min(1, (now - startRef.current) / DURATION);
      setDotT(t);
      const stageIdx = Math.min(TRACE_STAGES.length - 1, Math.floor(t * TRACE_STAGES.length));
      setActive(stageIdx);
      if (t < 1) rafRef.current = requestAnimationFrame(tick);
      else runningRef.current = false;
    };
    rafRef.current = requestAnimationFrame(tick);
  }, []);

  useEffect(() => () => { cancelAnimationFrame(rafRef.current); }, []);

  const dotX = (() => {
    const first = TRACE_STAGES[0].x;
    const last  = TRACE_STAGES[TRACE_STAGES.length - 1].x;
    return first + (last - first) * dotT;
  })();

  return (
    <div className="px-6 py-4 space-y-4">
      <div className="relative overflow-x-auto">
        <svg viewBox="0 50 700 120" className="w-full" style={{ minWidth: 480, height: 120 }}>
          <line x1="60" y1="80" x2="620" y2="80" stroke={LINE} strokeWidth="2" />
          {TRACE_STAGES.map((s, i) => (
            <g key={i}>
              <rect
                x={s.x - 44} y={60} width={88} height={40} rx={7}
                fill={active === i ? GOLD + "33" : BLUE + "22"}
                stroke={active === i ? GOLD : BLUE}
                strokeWidth={active === i ? 1.5 : 1}
              />
              <text x={s.x} y={82} textAnchor="middle" dominantBaseline="central"
                fill={active === i ? GOLD : TEXT} fontSize={11} fontFamily="monospace">
                {s.label}
              </text>
            </g>
          ))}
          {dotT > 0 && (
            <g>
              <circle cx={dotX} cy={80} r={9} fill={GOLD} opacity={0.18} />
              <circle cx={dotX} cy={80} r={5} fill={GOLD} />
            </g>
          )}
        </svg>
      </div>
      <div className="rounded-[10px] px-4 py-3 min-h-[48px] text-[11.5px]"
        style={{ background: BG, border: `1px solid ${LINE}`, fontFamily: "monospace", color: active >= 0 ? GOLD : MUT }}>
        {active >= 0 ? TRACE_STAGES[active].detail : TRACE_PLACEHOLDER}
      </div>
      <button
        onClick={startAnim}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-[9px] text-[13px] font-medium"
        style={{ color: BG, background: GOLD, border: `1px solid ${GOLD}` }}
      >
        <RotateCcw className="h-3.5 w-3.5" /> Trace a transaction
      </button>
    </div>
  );
}

// ── §3 Fault Tolerance — INTERACTIVE worker kill/revive diagram ──────────────
// Click any worker to kill it; click again to revive. Partitions auto-reassign
// to surviving workers (round-robin). Same logic as the live system: when a
// worker dies, its partitions move to the others; when it returns, rebalance.

interface FtWorker {
  id: number;
  alive: boolean;
}

function FaultToleranceVisual() {
  const NUM_PARTITIONS = 4;
  const [workers, setWorkers] = useState<FtWorker[]>([
    { id: 0, alive: true },
    { id: 1, alive: true },
    { id: 2, alive: true },
    { id: 3, alive: true },
  ]);
  const [eventLog, setEventLog] = useState<{ type: "err" | "ok" | "warn" | "info"; msg: string }[]>([
    { type: "info", msg: "System started — 4 partitions, 4 workers, all healthy." },
  ]);
  const [mongoCount, setMongoCount] = useState(0);
  const [tick, setTick] = useState(0);

  // assignment: which worker owns each partition (recomputed from workers state)
  const assignment: number[] = (() => {
    const aliveIds = workers.filter((w) => w.alive).map((w) => w.id);
    if (aliveIds.length === 0) return Array(NUM_PARTITIONS).fill(-1);
    const result: number[] = [];
    for (let p = 0; p < NUM_PARTITIONS; p++) {
      result.push(aliveIds[p % aliveIds.length]);
    }
    return result;
  })();

  // MongoDB counter increments only when at least one worker is alive
  useEffect(() => {
    const id = setInterval(() => {
      setTick((t) => t + 1);
      setMongoCount((c) => {
        const alive = workers.filter((w) => w.alive).length;
        return alive > 0 ? c + alive : c;
      });
    }, 600);
    return () => clearInterval(id);
  }, [workers]);

  const log = (type: "err" | "ok" | "warn" | "info", msg: string) => {
    setEventLog((prev) => [{ type, msg }, ...prev].slice(0, 8));
  };

  const toggleWorker = (id: number) => {
    setWorkers((prev) => {
      const next = prev.map((w) => (w.id === id ? { ...w, alive: !w.alive } : w));
      const target = prev.find((w) => w.id === id)!;
      if (target.alive) {
        // killing
        log("err", `Worker ${id + 1} stopped responding (process died).`);
        const aliveAfter = next.filter((w) => w.alive);
        if (aliveAfter.length === 0) {
          log("err", "All workers dead — Kafka holds messages, processing paused.");
        } else {
          const owned = assignment.map((wk, p) => (wk === id ? p : -1)).filter((p) => p >= 0);
          owned.forEach((p, i) => {
            const tgt = aliveAfter[(p + i) % aliveAfter.length].id;
            log("ok", `Partition ${p} reassigned to Worker ${tgt + 1} (resume from checkpoint).`);
          });
        }
      } else {
        // reviving
        log("ok", `Worker ${id + 1} back online.`);
        log("info", `Partition ${id} rebalanced back to Worker ${id + 1}.`);
      }
      return next;
    });
  };

  const aliveCount = workers.filter((w) => w.alive).length;
  const allDead = aliveCount === 0;

  // packet flow animation — only flows through alive workers
  const flowPhase = tick % 4;

  return (
    <div className="px-6 py-4 space-y-5">
      {/* Interactive diagram */}
      <div className="rounded-[12px] p-4" style={{ background: BG, border: `1px solid ${LINE}` }}>
        <div className="text-[11px] uppercase tracking-widest mb-3" style={{ color: MUT }}>
          Click a worker to kill or revive it
        </div>
        <svg viewBox="0 0 720 320" className="w-full h-auto" style={{ maxHeight: 280 }}>
          <defs>
            <marker id="ft-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
              <path d="M2 1L8 5L2 9" fill="none" stroke="#2a3441" strokeWidth="1.4" />
            </marker>
            <filter id="ft-glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="2.5" />
            </filter>
          </defs>

          {/* Kafka box */}
          <rect x="30" y="40" width="110" height="240" rx="10" fill={AMBER + "22"} stroke={AMBER} />
          <text x="85" y="58" textAnchor="middle" fill={AMBER} fontSize="11" fontFamily="monospace">Kafka</text>
          <text x="85" y="74" textAnchor="middle" fill={MUT} fontSize="9" fontFamily="monospace">raw_txns</text>
          {[0, 1, 2, 3].map((p) => (
            <g key={`p${p}`}>
              <rect x="40" y={90 + p * 48} width="90" height="36" rx="5" fill={AMBER + "22"} stroke={AMBER} />
              <text x="85" y={108 + p * 48} textAnchor="middle" fill={TEXT} fontSize="10" fontFamily="monospace">
                partition {p}
              </text>
            </g>
          ))}

          {/* Workers */}
          {workers.map((w, i) => {
            const wy = 90 + i * 48;
            const isOwner = assignment[i] === w.id;
            const ownedParts = assignment.map((wk, p) => (wk === w.id ? p : -1)).filter((p) => p >= 0);
            const dead = !w.alive;
            const stroke = dead ? RED : GREEN;
            const fill = dead ? RED + "22" : GREEN + "22";

            return (
              <g key={w.id} style={{ cursor: "pointer" }} onClick={() => toggleWorker(w.id)}>
                {/* worker box */}
                <rect
                  x="260" y={wy} width="180" height="36" rx="7"
                  fill={fill} stroke={stroke} strokeWidth="1.4"
                  strokeDasharray={dead ? "5 4" : undefined}
                />
                <text x="350" y={wy + 14} textAnchor="middle" fill={TEXT} fontSize="11" fontFamily="monospace">
                  Spark Worker {w.id + 1}
                </text>
                <text x="350" y={wy + 28} textAnchor="middle" fill={dead ? RED : MUT} fontSize="9" fontFamily="monospace">
                  {dead ? "DOWN — click to revive" : (ownedParts.length ? `partition ${ownedParts.join(", ")}` : "idle")}
                </text>

                {/* partition -> worker assignment line */}
                {isOwner && !dead && (
                  <line
                    x1="130" y1={wy + 18}
                    x2="258" y2={wy + 18}
                    stroke={LINE} strokeWidth="1.2"
                    markerEnd="url(#ft-ar)"
                  />
                )}

                {/* worker -> mongo line */}
                {!dead && (
                  <line
                    x1="440" y1={wy + 18}
                    x2="558" y2="160"
                    stroke={LINE} strokeWidth="1"
                    markerEnd="url(#ft-ar)"
                  />
                )}

                {/* packet animation: gold dot moving along the partition-to-worker path */}
                {!dead && flowPhase === i && (
                  <circle
                    cx={130 + (258 - 130) * 0.6} cy={wy + 18}
                    r="5" fill={GOLD} filter="url(#ft-glow)"
                  />
                )}
              </g>
            );
          })}

          {/* re-route lines: partitions owned by another worker (visualize reassignment) */}
          {assignment.map((wk, p) => {
            if (wk === -1 || wk === p) return null; // skip dead or natural assignment
            const targetW = workers.find((w) => w.id === wk);
            if (!targetW || !targetW.alive) return null;
            const py = 108 + p * 48;
            const wy = 108 + wk * 48;
            return (
              <line
                key={`reassign-${p}-${wk}`}
                x1="130" y1={py}
                x2="258" y2={wy}
                stroke={GOLD} strokeWidth="1.3" strokeDasharray="4 3"
                markerEnd="url(#ft-ar)"
                opacity="0.7"
              />
            );
          })}

          {/* MongoDB */}
          <rect x="560" y="120" width="130" height="80" rx="10" fill={PURPLE + "22"} stroke={PURPLE} />
          <text x="625" y="146" textAnchor="middle" fill={TEXT} fontSize="11" fontFamily="monospace">MongoDB</text>
          <text x="625" y="166" textAnchor="middle" fill={GOLD} fontSize="20" fontFamily="monospace" fontWeight="600">
            {mongoCount}
          </text>
          <text x="625" y="188" textAnchor="middle" fill={MUT} fontSize="9" fontFamily="monospace">docs written</text>
        </svg>

        {/* Status row */}
        <div className="flex items-center gap-4 mt-3 text-[11.5px]" style={{ fontFamily: "monospace" }}>
          <span style={{ color: MUT }}>workers alive: <span style={{ color: aliveCount > 0 ? GREEN : RED }}>{aliveCount}/4</span></span>
          <span style={{ color: MUT }}>state: <span style={{ color: allDead ? RED : (aliveCount < 4 ? AMBER : GREEN) }}>
            {allDead ? "PAUSED" : (aliveCount < 4 ? "DEGRADED" : "HEALTHY")}
          </span></span>
          <span style={{ color: MUT }}>data loss: <span style={{ color: GREEN }}>0</span></span>
        </div>
      </div>

      {/* Event log */}
      <div className="rounded-[10px] p-4" style={{ background: BG, border: `1px solid ${LINE}` }}>
        <div className="text-[11px] uppercase tracking-widest mb-2" style={{ color: MUT }}>Event log</div>
        <ul className="space-y-1 text-[12px]" style={{ fontFamily: "monospace", listStyle: "none", padding: 0, margin: 0 }}>
          {eventLog.map((ev, i) => {
            const color = ev.type === "err" ? RED : ev.type === "ok" ? GREEN : ev.type === "warn" ? AMBER : BLUE;
            const tag = ev.type === "err" ? "✕" : ev.type === "ok" ? "✓" : ev.type === "warn" ? "!" : "›";
            return (
              <li key={i} className="flex gap-2">
                <span style={{ color, width: 14, textAlign: "center", flexShrink: 0 }}>{tag}</span>
                <span style={{ color: TEXT, opacity: 1 - i * 0.08 }}>{ev.msg}</span>
              </li>
            );
          })}
        </ul>
      </div>

      {/* Reset button */}
      <button
        onClick={() => {
          setWorkers([
            { id: 0, alive: true }, { id: 1, alive: true },
            { id: 2, alive: true }, { id: 3, alive: true },
          ]);
          setEventLog([{ type: "info", msg: "Reset — all workers revived." }]);
          setMongoCount(0);
        }}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-[9px] text-[13px] font-medium"
        style={{ color: TEXT, background: "#1c2531", border: `1px solid ${LINE}` }}
      >
        <RotateCcw className="h-3.5 w-3.5" /> Reset all workers
      </button>
    </div>
  );
}

// ── §4 Scalability — INTERACTIVE worker scaler ────────────────────────────────
// Add/remove workers up to 8. Bar chart and SVG diagram both update.
// Real measured numbers from DISTRIBUTED_SYSTEM_ANALYSIS.md power the chart.

type ScalabilityLayer = "ingestion" | "spark";

const INGESTION_THROUGHPUT: Record<number, number> = {
  1: 637, 2: 1519, 3: 1500, 4: 1440, 5: 1300, 6: 1200, 7: 1150, 8: 1095,
};
const SPARK_THROUGHPUT: Record<number, number> = {
  1: 0.85, 2: 1.66, 3: 2.30, 4: 2.91, 5: 2.91, 6: 2.91, 7: 2.91, 8: 2.91,
};

function ScalabilityVisual() {
  const [layer, setLayer] = useState<ScalabilityLayer>("ingestion");
  const [workerCount, setWorkerCount] = useState(4);

  const dataMap = layer === "ingestion" ? INGESTION_THROUGHPUT : SPARK_THROUGHPUT;
  const chartData = Object.entries(dataMap).map(([w, t]) => ({
    workers: Number(w),
    throughput: t,
    isCurrent: Number(w) === workerCount,
  }));
  const currentThroughput = dataMap[workerCount];
  const baseline = dataMap[1];
  const speedup = currentThroughput / baseline;
  const ceiling = layer === "spark" ? 4 : 2;
  const overCeiling = workerCount > ceiling;

  return (
    <div className="px-6 py-4 space-y-5">
      {/* Layer toggle */}
      <div className="flex gap-2">
        {(["ingestion", "spark"] as ScalabilityLayer[]).map((l) => (
          <button key={l} onClick={() => setLayer(l)}
            className="px-3 py-1.5 rounded-[8px] text-[12px] font-medium"
            style={layer === l
              ? { color: BG, background: GOLD, border: `1px solid ${GOLD}` }
              : { color: TEXT, background: "#1c2531", border: `1px solid ${LINE}` }}>
            {l === "ingestion" ? "Ingestion layer (listener_mp.py)" : "Spark layer (4-partition ceiling)"}
          </button>
        ))}
      </div>

      {/* Interactive worker scaler */}
      <div className="rounded-[12px] p-4" style={{ background: BG, border: `1px solid ${LINE}` }}>
        <div className="flex items-center justify-between mb-3">
          <div className="text-[11px] uppercase tracking-widest" style={{ color: MUT }}>
            Add or remove workers to see throughput change
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={() => setWorkerCount(Math.max(1, workerCount - 1))}
              disabled={workerCount === 1}
              className="w-8 h-8 rounded-[7px] flex items-center justify-center transition-colors disabled:opacity-30"
              style={{ background: "#1c2531", border: `1px solid ${LINE}`, color: TEXT }}>
              <Minus className="h-4 w-4" />
            </button>
            <span className="text-[20px] font-mono font-semibold min-w-[28px] text-center" style={{ color: GOLD }}>
              {workerCount}
            </span>
            <button
              onClick={() => setWorkerCount(Math.min(8, workerCount + 1))}
              disabled={workerCount === 8}
              className="w-8 h-8 rounded-[7px] flex items-center justify-center transition-colors disabled:opacity-30"
              style={{ background: "#1c2531", border: `1px solid ${LINE}`, color: TEXT }}>
              <Plus className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Worker grid visualization */}
        <div className="flex flex-wrap gap-2 mb-4">
          {Array.from({ length: workerCount }).map((_, i) => {
            const isIdle = layer === "spark" && i >= 4;
            return (
              <div
                key={i}
                className="rounded-[7px] px-3 py-2 text-[11px]"
                style={{
                  fontFamily: "monospace",
                  background: isIdle ? AMBER + "18" : GREEN + "22",
                  border: `1px solid ${isIdle ? AMBER : GREEN}`,
                  color: TEXT,
                  minWidth: 84,
                }}
              >
                <div style={{ color: isIdle ? AMBER : GREEN, fontSize: 10 }}>
                  {layer === "spark" ? `worker-${i + 1}` : `proc-${i + 1}`}
                </div>
                <div>{isIdle ? "idle" : "active"}</div>
              </div>
            );
          })}
        </div>

        {/* Metrics */}
        <div className="grid grid-cols-3 gap-3 text-[12px]" style={{ fontFamily: "monospace" }}>
          <div className="rounded-[8px] p-3" style={{ background: PNL, border: `1px solid ${LINE}` }}>
            <div className="text-[10px] uppercase tracking-widest mb-1" style={{ color: MUT }}>Throughput</div>
            <div className="text-[18px] font-semibold" style={{ color: GOLD }}>
              {layer === "ingestion" ? `${currentThroughput.toFixed(0)} det/s` : `${currentThroughput.toFixed(2)} tx/s`}
            </div>
          </div>
          <div className="rounded-[8px] p-3" style={{ background: PNL, border: `1px solid ${LINE}` }}>
            <div className="text-[10px] uppercase tracking-widest mb-1" style={{ color: MUT }}>Speedup vs 1 worker</div>
            <div className="text-[18px] font-semibold" style={{ color: speedup >= 1 ? GREEN : RED }}>
              {speedup.toFixed(2)}×
            </div>
          </div>
          <div className="rounded-[8px] p-3" style={{ background: PNL, border: `1px solid ${LINE}` }}>
            <div className="text-[10px] uppercase tracking-widest mb-1" style={{ color: MUT }}>State</div>
            <div className="text-[13px] font-semibold pt-1" style={{ color: overCeiling ? AMBER : GREEN }}>
              {overCeiling
                ? (layer === "spark" ? "PLATEAU (partition-bound)" : "DIMINISHING RETURNS")
                : "SCALING"}
            </div>
          </div>
        </div>
      </div>

      {/* Bar chart with reference line at user's current worker count */}
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={LINE} />
          <XAxis dataKey="workers" tick={{ fill: MUT, fontSize: 11, fontFamily: "monospace" }}
            label={{ value: "workers", position: "insideBottom", offset: -2, fill: MUT, fontSize: 11 }} />
          <YAxis tick={{ fill: MUT, fontSize: 11, fontFamily: "monospace" }}
            label={{ value: layer === "ingestion" ? "det/s" : "tx/s", angle: -90, position: "insideLeft", fill: MUT, fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: PNL, border: `1px solid ${LINE}`, borderRadius: 8, fontFamily: "monospace", fontSize: 12 }}
            labelStyle={{ color: MUT }}
            itemStyle={{ color: GOLD }}
          />
          <Legend wrapperStyle={{ fontSize: 11, fontFamily: "monospace", color: MUT }} />
          <Bar dataKey="throughput" name={layer === "ingestion" ? "det/s" : "tx/s"} radius={[4, 4, 0, 0]}>
            {chartData.map((d, i) => (
              <Bar key={i} dataKey="throughput" fill={d.isCurrent ? GOLD : BLUE} />
            ))}
          </Bar>
          <ReferenceLine x={workerCount} stroke={GOLD} strokeDasharray="5 3"
            label={{ value: "you are here", position: "top", fill: GOLD, fontSize: 10 }} />
          {layer === "spark" && (
            <ReferenceLine x={4} stroke={RED} strokeDasharray="3 3"
              label={{ value: "partition ceiling", position: "insideTopRight", fill: RED, fontSize: 10 }} />
          )}
        </BarChart>
      </ResponsiveContainer>

      <div className="rounded-[10px] px-4 py-3 text-[12px] leading-relaxed" style={{ background: BG, border: `1px solid ${LINE}`, color: MUT, fontFamily: "monospace" }}>
        {layer === "ingestion"
          ? "Gains taper past 2 workers — same spawn overhead, less work per worker (Amdahl's Law). Best at 2 workers: 2.39× speedup."
          : "Plateau at 4 workers = 4 Kafka partitions. Adding a 5th worker finds no task to run — partition-bound. Scale partitions first."}
      </div>
    </div>
  );
}

// ── §5 Location Transparency — code block ────────────────────────────────────
const CODE_LINES = [
  { file: "broker/kafka_producer.py",    code: 'KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",', color: BLUE },
  { file: "",                             code: '                  "kafka-1:9092,kafka-2:9092,kafka-3:9092")', color: BLUE },
  { file: "processing/streaming_job.py", code: 'REDIS_HOST = os.getenv("REDIS_HOST", "redis")', color: GREEN },
  { file: "backend/Main.py",             code: 'MONGO_URI  = os.getenv("MONGODB_URI")   # atlas DNS, never an IP', color: AMBER },
];

function LocationTransparencyVisual() {
  return (
    <div className="px-6 py-4 space-y-4">
      <div className="rounded-[10px] overflow-hidden" style={{ background: BG, border: `1px solid ${LINE}` }}>
        {CODE_LINES.map((l, i) => (
          <div key={i} className="px-4 py-1 text-[12px]" style={{ fontFamily: "monospace", borderBottom: i < CODE_LINES.length - 1 ? `1px solid ${LINE}` : undefined }}>
            {l.file && <span style={{ color: MUT }}># {l.file}{"\n"}</span>}
            <span style={{ color: l.color }}>{l.code}</span>
          </div>
        ))}
      </div>
      <div className="rounded-[10px] px-4 py-3 text-[12px]" style={{ background: "#0c1a0c", border: `1px solid ${GREEN}22`, fontFamily: "monospace", color: GREEN }}>
        $ grep -r "[0-9]{1,3}\.[0-9]{1,3}" --include="*.py" .  →  zero matches
      </div>
      <p className="text-[12px] leading-relaxed" style={{ color: MUT, fontFamily: "monospace" }}>
        Docker embedded DNS on <span style={{ color: TEXT }}>broker_net</span> resolves container names to IPs at runtime.
        MongoDB Atlas provides a URI — the physical AWS/GCP host is completely transparent to application code.
      </p>
    </div>
  );
}

// ── §6 No SPOF — kill map with interactive buttons ───────────────────────────
interface KillTarget {
  id: string;
  label: string;
  what: string;
  recovery: string;
  time: string;
  dataLost: string;
}

const KILL_TARGETS: KillTarget[] = [
  { id: "kafka",   label: "Kill Kafka broker", what: "ISR drops 3→2, leader election starts",     recovery: "ZooKeeper leader election",          time: "~10 s",   dataLost: "0 (replication_factor=3, acks=all)" },
  { id: "spark",   label: "Kill Spark worker", what: "Spark master drops worker, reassigns",       recovery: "Checkpoint offset replay",            time: "~6 s",    dataLost: "0 (checkpoint at last committed offset)" },
  { id: "redis",   label: "Kill Redis",        what: "Price UDF Redis lookup fails",               recovery: "Static price fallback in price_udf",  time: "instant", dataLost: "0 (stablecoins → 1.0, WETH/WBTC → static)" },
  { id: "fastapi", label: "Kill FastAPI",      what: "React shows disconnected state",             recovery: "EventSource auto-reconnects",         time: "~3 s",    dataLost: "0 (MongoDB unchanged, SSE resumes on reconnect)" },
];

const TOLERANCE_TABLE = [
  { component: "Kafka broker", tolerance: "lose 1 of 3", mechanism: "ZooKeeper leader election" },
  { component: "Spark worker", tolerance: "lose 1 of 4", mechanism: "checkpoint offset replay" },
  { component: "Redis",        tolerance: "full loss",   mechanism: "static price fallback in price_udf" },
  { component: "FastAPI",      tolerance: "full loss",   mechanism: "EventSource auto-reconnect (browser)" },
];

function NoSpofVisual() {
  const [active, setActive]       = useState<string | null>(null);
  const [recovering, setRecovering] = useState<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const kill = (id: string) => {
    if (timerRef.current) clearTimeout(timerRef.current);
    setActive(id); setRecovering(null);
    timerRef.current = setTimeout(() => {
      setRecovering(id);
      timerRef.current = setTimeout(() => { setActive(null); setRecovering(null); }, 2000);
    }, 2000);
  };

  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current); }, []);

  const target = KILL_TARGETS.find((t) => t.id === active);

  return (
    <div className="px-6 py-4 space-y-5">
      <div className="flex flex-wrap gap-2.5">
        {KILL_TARGETS.map((t) => (
          <button key={t.id} onClick={() => kill(t.id)}
            className="px-4 py-2 rounded-[9px] text-[12px] font-medium transition-all"
            style={active === t.id
              ? { color: "#fff", background: RED + "44", border: `1px solid ${RED}` }
              : { color: TEXT, background: "#1c2531", border: `1px solid ${LINE}` }}>
            {t.label}
          </button>
        ))}
      </div>

      <div className="rounded-[12px] p-4 min-h-[120px] transition-all"
        style={{ background: BG, border: `1px solid ${active ? (recovering ? GREEN : RED) : LINE}` }}>
        {!active && (
          <p className="text-[12px]" style={{ color: MUT, fontFamily: "monospace" }}>Click a kill button to see what happens ↑</p>
        )}
        {active && target && (
          <div className="space-y-2 text-[12px]" style={{ fontFamily: "monospace" }}>
            <div style={{ color: recovering ? GREEN : RED, fontSize: 13, fontWeight: 600 }}>
              {recovering
                ? `✅ ${target.label.replace("Kill", "Recovered:")}`
                : `💥 ${target.label}`}
            </div>
            <div style={{ color: MUT }}>What happened: <span style={{ color: TEXT }}>{target.what}</span></div>
            <div style={{ color: MUT }}>Recovery: <span style={{ color: GREEN }}>{target.recovery}</span></div>
            <div style={{ color: MUT }}>Recovery time: <span style={{ color: AMBER }}>{target.time}</span></div>
            <div style={{ color: MUT }}>Data lost: <span style={{ color: GREEN }}>{target.dataLost}</span></div>
            <div style={{ color: recovering ? GREEN : RED, fontWeight: 600 }}>
              {recovering ? "✅ PIPELINE CONTINUES" : "⚠ RECOVERING..."}
            </div>
          </div>
        )}
      </div>

      <div className="rounded-[10px] overflow-hidden" style={{ border: `1px solid ${LINE}` }}>
        <table className="w-full text-[11.5px]" style={{ fontFamily: "monospace", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ background: LINE }}>
              <th className="px-4 py-2 text-left font-normal" style={{ color: MUT }}>Component</th>
              <th className="px-4 py-2 text-left font-normal" style={{ color: MUT }}>Tolerance</th>
              <th className="px-4 py-2 text-left font-normal" style={{ color: MUT }}>Recovery mechanism</th>
            </tr>
          </thead>
          <tbody>
            {TOLERANCE_TABLE.map((row, i) => (
              <tr key={i} style={{ borderTop: `1px solid ${LINE}` }}>
                <td className="px-4 py-2" style={{ color: TEXT }}>{row.component}</td>
                <td className="px-4 py-2" style={{ color: AMBER }}>{row.tolerance}</td>
                <td className="px-4 py-2" style={{ color: MUT }}>{row.mechanism}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <button
        onClick={() => { kill("kafka"); setTimeout(() => kill("spark"), 500); }}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-[9px] text-[13px] font-medium"
        style={{ color: BG, background: RED, border: `1px solid ${RED}` }}>
        <Zap className="h-3.5 w-3.5" /> Kill Kafka + Spark simultaneously
      </button>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function DemoPage() {
  const scrollTo = (id: string) => {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-[22px] font-semibold tracking-tight flex items-center gap-2">
          <ShieldAlert className="h-5 w-5" style={{ color: GOLD }} />
          Distributed System Demo
        </h1>
        <p className="mt-1 text-[13px]" style={{ color: MUT, fontFamily: "monospace" }}>
          6 characteristics · live evidence · 100% client-side · scroll or use anchors below
        </p>
      </header>

      <nav className="sticky top-14 z-40 flex flex-wrap gap-1.5 px-3 py-2.5 rounded-[12px] overflow-x-auto"
        style={{ background: PNL + "ee", border: `1px solid ${LINE}`, backdropFilter: "blur(8px)" }}>
        {SECTIONS.map((s) => (
          <button key={s.id} onClick={() => scrollTo(s.id)}
            className="px-3 py-1 rounded-[7px] text-[12px] font-medium whitespace-nowrap transition-colors"
            style={{ color: MUT, background: "transparent", border: "none" }}
            onMouseEnter={(e) => { (e.target as HTMLButtonElement).style.color = TEXT; }}
            onMouseLeave={(e) => { (e.target as HTMLButtonElement).style.color = MUT; }}>
            {s.label}
          </button>
        ))}
      </nav>

      {/* §1 Concurrency */}
      <Card id="concurrency">
        <SectionHeader num="01" title="Concurrency" badge="4 workers · 4 partitions" />
        <Oneliner>Multiple computations happen at the same time — not one after another.</Oneliner>
        <Divider />
        <ConcurrencyVisual />
        <Evidence lines={[
          "Ingestion:  4 worker processes (PIDs: distinct)  |  sum(loads) == rows",
          "Processing: 4 Spark workers  |  1 partition per worker  |  parallel micro-batches",
          "865-row benchmark:  workers=1 → 637 det/s  |  workers=2 → 1519 det/s (2.39×)",
        ]} />
      </Card>

      {/* §2 Message Passing */}
      <Card id="message-passing">
        <SectionHeader num="02" title="Message Passing" badge="no shared memory" />
        <Oneliner>Every stage communicates through a channel — no component reads another's memory.</Oneliner>
        <Divider />
        <MessagePassingVisual />
        <Evidence lines={[
          "Channels: Kafka topic raw_txns | MongoDB collection transactions | SSE /stream/detections",
          "No component calls another directly — all communication is through durable channels",
          "Listener → Kafka → Spark → MongoDB → FastAPI → React  (5 independent hops)",
        ]} />
      </Card>

      {/* §3 Fault Tolerance */}
      <Card id="fault-tolerance">
        <SectionHeader num="03" title="Fault Tolerance" badge="checkpoint · ISR · fallback" />
        <Oneliner>When a component fails, the system recovers automatically with no data lost. Click any worker below to kill or revive it.</Oneliner>
        <Divider />
        <FaultToleranceVisual />
        <Evidence lines={[
          "Kafka:  replication_factor=3  |  acks=all  |  enable.idempotence=True",
          "Spark:  checkpoint at /tmp/spark-checkpoints/  |  resume from last Kafka offset",
          "Result: MongoDB count monotonically increasing through all kills",
        ]} />
      </Card>

      {/* §4 Scalability */}
      <Card id="scalability">
        <SectionHeader num="04" title="Scalability" badge="partition-bound ceiling" />
        <Oneliner>Adding more workers increases throughput — until the partition ceiling. Use +/− to add or remove workers.</Oneliner>
        <Divider />
        <ScalabilityVisual />
        <Evidence lines={[
          "865-row offline sweep (ingestion layer):",
          "  workers=1  →   637 det/s  (1.00×)",
          "  workers=2  →  1519 det/s  (2.39×)  ← best",
          "  workers=4  →  1440 det/s  (2.26×)",
          "  workers=8  →  1095 det/s  (1.72×)",
          "",
          "Spark layer:  1w → ~0.85 tx/s  |  2w → ~1.66 tx/s  |  4w → ~2.91 tx/s  |  5w+ → plateau",
        ]} />
      </Card>

      {/* §5 Location Transparency */}
      <Card id="location-transparency">
        <SectionHeader num="05" title="Location Transparency" badge="zero hardcoded IPs" />
        <Oneliner>Components talk to each other by name — not by IP address.</Oneliner>
        <Divider />
        <LocationTransparencyVisual />
        <Evidence lines={[
          "Docker bridge network: broker_net (embedded DNS resolves container names)",
          "MongoDB Atlas: cloud endpoint — physical host completely transparent",
          "Result: zero hardcoded IPs anywhere in application source code",
        ]} />
      </Card>

      {/* §6 No SPOF */}
      <Card id="no-spof">
        <SectionHeader num="06" title="No Single Point of Failure" badge="kill anything" />
        <Oneliner>No individual component's failure stops the pipeline.</Oneliner>
        <Divider />
        <NoSpofVisual />
        <Evidence lines={[
          "Proved by live kill sequence: dashboard stays live through broker + worker kills",
          "Single-process baseline:  crash at tx #11 → 0 docs  |  Spark: crash → 10 docs preserved",
          "React EventSource auto-reconnects on FastAPI restart — no user action required",
        ]} />
      </Card>

      {/* legend */}
      <div className="rounded-[12px] p-4" style={{ background: PNL, border: `1px solid ${LINE}` }}>
        <div className="flex flex-wrap gap-x-6 gap-y-2 text-[11.5px]" style={{ fontFamily: "monospace", color: MUT }}>
          <span className="flex items-center gap-1.5"><Activity className="h-3.5 w-3.5" style={{ color: BLUE }} /> ingestion (listener_mp.py)</span>
          <span className="flex items-center gap-1.5"><Network className="h-3.5 w-3.5" style={{ color: AMBER }} /> Kafka (3 brokers · 4 partitions)</span>
          <span className="flex items-center gap-1.5"><Cpu className="h-3.5 w-3.5" style={{ color: PURPLE }} /> Spark (master + 4 workers)</span>
          <span className="flex items-center gap-1.5"><Database className="h-3.5 w-3.5" style={{ color: GREEN }} /> MongoDB Atlas</span>
          <span className="flex items-center gap-1.5"><Terminal className="h-3.5 w-3.5" style={{ color: GOLD }} /> FastAPI SSE → React</span>
        </div>
      </div>
    </div>
  );
}
