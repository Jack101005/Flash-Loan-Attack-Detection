// frontend/src/pages/PipelineSimulatorPage.tsx
// Animated, fully client-side SVG simulator of the real detection pipeline.
// NOTHING here touches real containers — it is a teaching visualization only.
import { useEffect, useRef, useState } from "react";
import { Play, Pause, RotateCcw, Zap, Database, Activity, Network } from "lucide-react";

// ── palette ────────────────────────────────────────────────────────────────────
const C = {
  blue:   "#4a9eff",
  amber:  "#f0a93b",
  green:  "#5fd07a",
  purple: "#b08cf5",
  red:    "#ff5d5d",
  gold:   "#ffd24a",
  mut:    "#8a96a6",
  line:   "#2a3441",
  text:   "#e7ebf0",
};

// ── Layout constants (single source of truth) ─────────────────────────────────
//
// All positions derived here so nothing can conflict.
//
// Listener group internal layout:
//   LY          outer box top
//   LY+16       "listener_mp.py" label
//   LY+30       feeder box top  (h=36) → bottom = LY+66
//   LY+66+12    "Queue" label centred in gap
//   LY+66+24    fan lines origin (fanStart)
//   LANES[i]    worker box centres — MUST be ≥ fanStart + WWH/2 + 20px clearance
//
// W0_CENTER = LY + 30 + 36 + 24 + 20 + WWH/2
//           = 40  + 66  + 24 + 20 + 18 = 168
//
// Worker pitch = WWH + 16 = 52
// LANES = [168, 220, 272, 324]
//
// Kafka: header needs ~50px (title at K_TOP+16, subtitle at K_TOP+32, gap=18 to first partition)
//   K_TOP = LANES[0] - 68 → subtitle at K_TOP+32, first partition top at LANES[0]-20
//   gap = (LANES[0]-20) - (K_TOP+32) = 68-20-32 = 16px ✓

const LY        = 40;    // listener outer box top
const LX        = 196;   // listener outer box left
const LW        = 148;   // listener outer box width
const WWH       = 36;    // worker sub-box height
const WWW       = 70;    // worker sub-box width
const W_PITCH   = 52;    // vertical pitch between worker centres (WWH + 16px gap)
const W0_CENTER = LY + 30 + 36 + 24 + 20 + WWH / 2; // = 168

const LANES: number[] = Array.from({ length: 4 }, (_, i) => W0_CENTER + i * W_PITCH);
// → [168, 220, 272, 324]

// Kafka ── outer box top set so header (title + subtitle) has 16px margin above partition boxes
const KX    = 430;
const KW    = 148;
const K_TOP = LANES[0] - 68;  // first partition top = LANES[0]-20, subtitle at K_TOP+32 → 16px gap
const K_BOT = LANES[3] + 36;
const K_H   = K_BOT - K_TOP;

// Spark workers (aligned to same LANES → straight horizontal arrows)
const SX = 674;
const SW = 186;

// MongoDB spans the full lane range vertically
const MX = 930;
const MY = LANES[0] - 20;
const MH = LANES[3] - LANES[0] + 40 + 20;
const MW = 168;

// Dashboard below MongoDB with 28px gap
const DX = MX;
const DY = MY + MH + 28;
const DW = MW;
const DH = 96;

// Spark Master sits above the workers
const MASTER_Y = 30;

// SVG canvas size computed from layout
const SVG_H = DY + DH + 24;
const SVG_W = MX + MW + 30;

// ── Benchmark data ─────────────────────────────────────────────────────────────
type DatasetKey = 60 | 865;
interface SweepRow { workers: number; speedup: number; throughput?: number; timeS?: number; loads?: number[]; }
interface DatasetBench { rows: number; detected: number; filtered: number; best: string; takeaway: string; sweep: SweepRow[]; }

const BENCH: Record<DatasetKey, DatasetBench> = {
  60: {
    rows: 60, detected: 35, filtered: 25, best: "1 worker",
    takeaway: "Too little work to amortize process-spawn cost — multiprocessing is a net loss; single-process wins.",
    sweep: [
      { workers: 1, speedup: 1.0,  loads: [60] },
      { workers: 2, speedup: 0.96 },
      { workers: 4, speedup: 0.91, loads: [16, 12, 13, 19] },
      { workers: 8, speedup: 0.84 },
    ],
  },
  865: {
    rows: 865, detected: 429, filtered: 436, best: "2 workers · 2.39×",
    takeaway: "Enough total work to amortize spawn overhead — 2–4 workers win; gains taper past 4 as per-worker share shrinks.",
    sweep: [
      { workers: 1, speedup: 1.0,  throughput: 636.6,  timeS: 0.674, loads: [865] },
      { workers: 2, speedup: 2.39, throughput: 1519.0, timeS: 0.282, loads: [440, 425] },
      { workers: 4, speedup: 2.26, throughput: 1439.6, timeS: 0.298, loads: [227, 207, 222, 209] },
      { workers: 8, speedup: 1.72, throughput: 1094.5, timeS: 0.392, loads: [110, 114, 105, 110, 104, 105, 116, 101] },
    ],
  },
};

// ── Types ──────────────────────────────────────────────────────────────────────
interface Worker { id: number; lane: number; alive: boolean; pulse: number; }
interface Packet  { partition: number; worker: number | null; seg: number; t: number; held: boolean; }
type LogType = "info" | "ok" | "warn" | "err";

// ── Component ──────────────────────────────────────────────────────────────────
export default function PipelineSimulatorPage() {
  const [running, setRunning] = useState(true);
  const [speed,   setSpeed  ] = useState(1);
  const [dataset, setDataset] = useState<DatasetKey>(865);

  const runningRef = useRef(running);
  const speedRef   = useRef(speed);
  const datasetRef = useRef<DatasetKey>(dataset);

  const linksRef   = useRef<SVGGElement>(null);
  const hbRef      = useRef<SVGGElement>(null);
  const packetsRef = useRef<SVGGElement>(null);
  const nodesRef   = useRef<SVGGElement>(null);
  const cntRef     = useRef<HTMLSpanElement>(null);
  const logRef     = useRef<HTMLUListElement>(null);

  const apiRef = useRef<{
    killRandom: () => void; reviveAll: () => void; reset: () => void; redrawNodes: () => void;
  } | null>(null);

  useEffect(() => { runningRef.current = running; }, [running]);
  useEffect(() => { speedRef.current   = speed;   }, [speed]);
  useEffect(() => { datasetRef.current = dataset; apiRef.current?.redrawNodes(); }, [dataset]);

  useEffect(() => {
    const SVGNS = "http://www.w3.org/2000/svg";
    let workers: Worker[] = [];
    let assign: number[] = [];
    let packets: Packet[] = [];
    let count = 0, spawnAcc = 0, last = 0, rrCounter = 0, dashFlash = 0;
    let lastDash = -1, lastPulseKey = "";
    let raf = 0;
    const timeouts = new Set<ReturnType<typeof setTimeout>>();
    const setT = (fn: () => void, ms: number) => {
      const id = setTimeout(() => { timeouts.delete(id); fn(); }, ms);
      timeouts.add(id); return id;
    };

    // ── SVG helpers ─────────────────────────────────────────────────────────
    const el = (tag: string, attrs: Record<string, string | number>) => {
      const e = document.createElementNS(SVGNS, tag);
      for (const k in attrs) e.setAttribute(k, String(attrs[k]));
      return e as SVGElement;
    };
    const txt = (x: number, y: number, s: string, fill: string, size = 13, weight = "normal") => {
      const t = el("text", { x, y, "text-anchor": "middle", "dominant-baseline": "central", fill, "font-size": size, "font-weight": weight });
      t.textContent = s; return t;
    };
    const box = (x: number, y: number, w: number, h: number, fill: string, stroke: string, rx = 10) =>
      el("rect", { x, y, width: w, height: h, rx, fill, stroke, "stroke-width": 1.2 });
    const fillFor = (hex: string) => hex + "22";
    const mkLine = (x1: number, y1: number, x2: number, y2: number, stroke: string, sw = 2.5, dash?: string) => {
      const l = el("line", { x1, y1, x2, y2, stroke, "stroke-width": sw });
      if (dash) l.setAttribute("stroke-dasharray", dash);
      return l;
    };
    const arrow = (x1: number, y1: number, x2: number, y2: number) => {
      const l = mkLine(x1, y1, x2, y2, C.gold);
      l.setAttribute("marker-end", "url(#plsim-ar)");
      return l;
    };

    // ── drawStatic ──────────────────────────────────────────────────────────
    const drawStatic = () => {
      const g = linksRef.current!; g.innerHTML = "";
      const mockCY = (LANES[0] + LANES[3]) / 2;
      g.appendChild(arrow(LX - 46, mockCY, LX - 2, mockCY));       // mock → listener
      g.appendChild(arrow(MX + MW / 2, MY + MH + 2, MX + MW / 2, DY - 2)); // mongo → dashboard
    };

    // ── drawHeartbeats ──────────────────────────────────────────────────────
    const drawHeartbeats = () => {
      const g = hbRef.current!; g.innerHTML = "";
      workers.forEach((w) => {
        if (!w.alive) return;
        const l = mkLine(SX + SW / 2, MASTER_Y + 54, SX + SW / 2, w.lane - 20, C.green, 1, "2 6");
        l.setAttribute("opacity", "0.45");
        (l as SVGLineElement).style.animation = "plsim-hb 1.2s linear infinite";
        g.appendChild(l);
      });
    };

    // ── drawListenerGroup ───────────────────────────────────────────────────
    // Internal layout (pixel-precise, verified against constants):
    //   Outer box:   LX, LY  width=LW
    //   Label:       LY+16
    //   Feeder:      LY+30 → LY+66  (h=36)
    //   Queue text:  LY+66+12 = LY+78
    //   fanStart:    LY+66+24 = LY+90
    //   w-0 top:     LANES[0]-WWH/2 = 168-18 = 150  gap from fanStart: 150-(LY+90)=150-130=20px ✓
    const drawListenerGroup = (g: SVGGElement) => {
      const lBot = LANES[3] + WWH / 2 + 16;
      g.appendChild(box(LX, LY, LW, lBot - LY, fillFor(C.blue), C.blue, 12));
      g.appendChild(txt(LX + LW / 2, LY + 16, "listener_mp.py", C.blue, 11));

      // feeder box
      const fTop = LY + 30;
      g.appendChild(box(LX + 14, fTop, LW - 28, 36, fillFor(C.blue), C.blue, 7));
      g.appendChild(txt(LX + LW / 2, fTop + 18, "feeder", C.text, 12));

      // Queue label + tick below feeder
      const feederBot = fTop + 36;                 // = LY+66
      const qLabelY   = feederBot + 12;            // = LY+78  ("Queue" text centre)
      const fanStart  = feederBot + 24;            // = LY+90  (fan lines origin)
      g.appendChild(txt(LX + LW / 2, qLabelY, "Queue", C.mut, 10));
      const tick = mkLine(LX + LW / 2, feederBot, LX + LW / 2, fanStart, C.blue, 1.2);
      tick.setAttribute("marker-end", "url(#plsim-ar)");
      g.appendChild(tick);

      // worker sub-boxes, fan lines, → kafka arrows
      const wx = LX + LW / 2 - WWW / 2;
      ["w-0", "w-1", "w-2", "w-3"].forEach((label, i) => {
        const cy  = LANES[i];
        const top = cy - WWH / 2;
        g.appendChild(box(wx, top, WWW, WWH, fillFor(C.blue), C.blue, 6));
        g.appendChild(txt(wx + WWW / 2, cy, label, C.text, 11));

        // dashed fan line from fanStart to worker top-centre
        const fan = mkLine(LX + LW / 2, fanStart, wx + WWW / 2, top, C.blue, 1, "3 3");
        fan.setAttribute("opacity", "0.5");
        g.appendChild(fan);

        // solid gold arrow: worker right-edge → kafka partition (horizontal)
        g.appendChild(arrow(wx + WWW + 2, cy, KX - 2, cy));
      });
    };

    // ── drawNodes ───────────────────────────────────────────────────────────
    const drawNodes = () => {
      const g = nodesRef.current!; g.innerHTML = "";
      const ds    = datasetRef.current;
      const mockCY = (LANES[0] + LANES[3]) / 2;
      const mockX  = LX - 46 - 108;

      // Mock node
      g.appendChild(box(mockX, mockCY - 32, 108, 64, fillFor(C.blue), C.blue));
      g.appendChild(txt(mockX + 54, mockCY - 9, "Mock node", C.text, 12));
      g.appendChild(txt(mockX + 54, mockCY + 12, `${ds} tx`, C.mut, 11));

      // Listener group
      drawListenerGroup(g as unknown as SVGGElement);

      // Kafka outer box
      g.appendChild(box(KX, K_TOP, KW, K_H, fillFor(C.amber), C.amber, 14));
      g.appendChild(txt(KX + KW / 2, K_TOP + 16, "Kafka",          C.amber, 13, "600"));
      g.appendChild(txt(KX + KW / 2, K_TOP + 33, "topic raw_txns", C.mut,   10));
      LANES.forEach((y, i) => {
        g.appendChild(box(KX + 10, y - 20, KW - 20, 40, fillFor(C.amber), C.amber, 7));
        g.appendChild(txt(KX + KW / 2, y, "Partition " + i, C.text, 12));
        // kafka right → spark worker left (straight)
        g.appendChild(arrow(KX + KW + 2, y, SX - 2, y));
      });

      // Spark Master
      g.appendChild(box(SX, MASTER_Y, SW, 54, fillFor(C.purple), C.purple, 10));
      g.appendChild(txt(SX + SW / 2, MASTER_Y + 20, "Spark Master",           C.text, 12));
      g.appendChild(txt(SX + SW / 2, MASTER_Y + 37, "coordinator · heartbeat", C.mut,  10));

      // Rerouted assignment arrows (dashed diagonal, drawn before worker boxes)
      assign.forEach((wk, p) => {
        if (wk < 0 || !workers[wk]?.alive || wk === p) return;
        const l = mkLine(KX + KW + 2, LANES[p], SX - 2, LANES[wk], C.gold, 1.4, "4 3");
        l.setAttribute("marker-end", "url(#plsim-ar)"); l.setAttribute("opacity", "0.7");
        g.appendChild(l);
      });

      // Spark worker boxes (clickable)
      workers.forEach((w, i) => {
        const dead   = !w.alive;
        const stroke = dead ? C.red : C.green;
        const fill   = dead ? fillFor(C.red) : (w.pulse > 0.05 ? C.green + "55" : fillFor(C.green));
        const grp    = el("g", { style: "cursor:pointer" });
        grp.addEventListener("click", () => toggleWorker(i));
        const r = box(SX, LANES[i] - 26, SW, 52, fill, stroke, 8);
        if (dead) r.setAttribute("stroke-dasharray", "5 4");
        grp.appendChild(r);
        grp.appendChild(txt(SX + SW / 2, LANES[i] - 7, "Worker " + (i + 1), C.text, 12));
        const parts = assign.map((wk, p) => (wk === i ? p : -1)).filter((p) => p >= 0);
        grp.appendChild(txt(
          SX + SW / 2, LANES[i] + 12,
          dead ? "DOWN — click to revive" : (parts.length ? "part " + parts.join(", ") : "idle"),
          dead ? C.red : C.mut, 10,
        ));
        g.appendChild(grp);
      });

      // Worker → MongoDB fan arrows (spread evenly across MongoDB left edge)
      const alive  = workers.filter((w) => w.alive);
      const nAlive = alive.length;
      const inTop  = MY + MH * 0.15;
      const inBot  = MY + MH * 0.85;
      workers.forEach((w) => {
        if (!w.alive) return;
        const rank = alive.findIndex((a) => a.id === w.id);
        const tgtY = nAlive === 1 ? MY + MH / 2 : inTop + (rank / (nAlive - 1)) * (inBot - inTop);
        g.appendChild(arrow(SX + SW + 2, w.lane, MX - 2, tgtY));
      });

      // MongoDB
      g.appendChild(box(MX, MY, MW, MH, fillFor(C.purple), C.purple, 12));
      g.appendChild(txt(MX + MW / 2, MY + MH / 2 - 10, "MongoDB Atlas",    C.text, 12, "600"));
      g.appendChild(txt(MX + MW / 2, MY + MH / 2 + 10, "foreachPartition", C.mut,  10));

      // Dashboard
      const dFill   = dashFlash > 0 ? C.red + "44" : fillFor(C.purple);
      const dStroke = dashFlash > 0 ? C.red : C.purple;
      g.appendChild(box(DX, DY, DW, DH, dFill, dStroke, 12));
      g.appendChild(txt(DX + DW / 2, DY + 26, "Dashboard",                        C.text, 13, "600"));
      g.appendChild(txt(DX + DW / 2, DY + 46, "SSE real-time",                     C.mut,  11));
      g.appendChild(txt(DX + DW / 2, DY + 68, dashFlash > 0 ? "⚠ ALERT" : "",     C.red,  11));
    };

    // ── Packet waypoints ────────────────────────────────────────────────────
    const waypoints = (p: Packet) => {
      const Li = LANES[p.partition];
      const wk = p.worker != null ? p.worker : assign[p.partition];
      const Lj = wk >= 0 && workers[wk] ? LANES[wk] : Li;

      const alive  = workers.filter((w) => w.alive);
      const nAlive = alive.length;
      const inTop  = MY + MH * 0.15;
      const inBot  = MY + MH * 0.85;
      const rank   = wk >= 0 ? alive.findIndex((a) => a.id === wk) : 0;
      const mTgtY  = nAlive <= 1 ? MY + MH / 2 : inTop + (rank / (nAlive - 1)) * (inBot - inTop);

      const mockCY = (LANES[0] + LANES[3]) / 2;
      const lWX    = LX + LW / 2 - WWW / 2;

      return [
        { x: LX - 46,       y: mockCY },        // mock node right edge
        { x: LX,            y: mockCY },        // listener left edge
        { x: lWX + WWW / 2, y: Li     },        // listener worker centre
        { x: lWX + WWW,     y: Li     },        // listener worker right edge
        { x: KX,            y: Li     },        // kafka left edge
        { x: KX + KW,       y: Li     },        // kafka right edge
        { x: SX,            y: Lj     },        // spark worker left edge
        { x: SX + SW,       y: Lj     },        // spark worker right edge
        { x: MX,            y: mTgtY  },        // mongo left edge (fanned)
        { x: MX + MW / 2,   y: MY + MH / 2 },  // mongo centre
      ];
    };

    const spawn = () => { packets.push({ partition: rrCounter++ % 4, worker: null, seg: 0, t: 0, held: false }); };

    const advance = (p: Packet, dist: number): boolean => {
      const wp = waypoints(p);
      while (dist > 0 && p.seg < wp.length - 1) {
        const a = wp[p.seg], b = wp[p.seg + 1];
        const len = Math.hypot(b.x - a.x, b.y - a.y);
        if (p.seg === 3) {
          const wk = assign[p.partition];
          if (wk < 0 || !workers[wk].alive) { p.held = true; return false; }
          p.worker = wk; p.held = false;
        }
        const remain = (1 - p.t) * len;
        if (dist < remain) { p.t += dist / len; dist = 0; }
        else {
          dist -= remain; p.seg++; p.t = 0;
          if (p.seg === 4) { const w = workers[p.worker!]; if (w) w.pulse = 1; }
        }
      }
      return p.seg >= wp.length - 1;
    };

    const posOf = (p: Packet) => {
      const wp = waypoints(p);
      const a = wp[p.seg], b = wp[Math.min(p.seg + 1, wp.length - 1)];
      return { x: a.x + (b.x - a.x) * p.t, y: a.y + (b.y - a.y) * p.t };
    };

    const renderPackets = () => {
      const layer = packetsRef.current!; layer.innerHTML = "";
      packets.forEach((p) => {
        const pos = posOf(p);
        for (let k = 3; k >= 1; k--)
          layer.appendChild(el("circle", { cx: pos.x, cy: pos.y, r: 3, fill: C.red, opacity: 0.12 * k }));
        layer.appendChild(el("circle", { cx: pos.x, cy: pos.y, r: 5, fill: C.red, filter: "url(#plsim-glow)" }));
      });
    };

    const toggleWorker = (i: number) => workers[i].alive ? killWorker(i) : reviveWorker(i);

    const killWorker = (i: number) => {
      if (!workers[i].alive) return;
      workers[i].alive = false; workers[i].pulse = 0;
      drawNodes(); drawHeartbeats();
      logMsg("err", "⚠ Worker " + (i + 1) + " stopped responding.");
      setT(() => {
        if (workers[i].alive) return;
        flashMaster();
        logMsg("warn", "Master: Worker " + (i + 1) + " missed heartbeat → DOWN.");
        const aliveIdx = workers.filter((w) => w.alive).map((w) => w.id);
        const moved    = assign.map((wk, p) => (wk === i ? p : -1)).filter((p) => p >= 0);
        if (aliveIdx.length === 0) { logMsg("err", "All workers dead — Kafka holding messages."); drawNodes(); return; }
        moved.forEach((p) => {
          const tgt = aliveIdx[rrCounter++ % aliveIdx.length];
          assign[p] = tgt;
          logMsg("ok", "Partition " + p + " → Worker " + (tgt + 1) + " (resuming from checkpoint).");
        });
        drawNodes();
      }, 1300);
    };

    const reviveWorker = (i: number) => {
      if (workers[i].alive) return;
      workers[i].alive = true;
      logMsg("ok", "Worker " + (i + 1) + " back online.");
      if (assign[i] !== i) { assign[i] = i; logMsg("info", "Partition " + i + " returned to Worker " + (i + 1) + "."); }
      drawNodes(); drawHeartbeats();
    };

    const flashMaster = () => {
      const o = el("rect", { x: SX, y: MASTER_Y, width: SW, height: 54, rx: 10, fill: "none", stroke: C.red, "stroke-width": 2 });
      nodesRef.current!.appendChild(o);
      setT(() => o.remove(), 600);
    };

    const logMsg = (type: LogType, msg: string) => {
      const ul = logRef.current!;
      const li = document.createElement("li");
      li.style.cssText = "display:flex;gap:8px;align-items:baseline;animation:plsim-fade .3s";
      const tag   = { info: "›", ok: "✓", warn: "!", err: "✕" }[type];
      const color = { info: C.blue, ok: C.green, warn: C.amber, err: C.red }[type];
      li.innerHTML = `<span style="flex:none;width:14px;text-align:center;color:${color}">${tag}</span><span>${msg}</span>`;
      ul.insertBefore(li, ul.firstChild);
      while (ul.children.length > 8) ul.removeChild(ul.lastChild!);
    };

    const refreshDynamic = () => {
      const dKey = dashFlash > 0 ? 1 : 0;
      const pKey = workers.map((w) => w.alive ? (w.pulse > 0.05 ? "P" : "a") : "d").join("");
      if (dKey !== lastDash || pKey !== lastPulseKey) { lastDash = dKey; lastPulseKey = pKey; drawNodes(); }
    };

    const frame = (ts: number) => {
      if (!last) last = ts;
      const dt = Math.min(0.05, (ts - last) / 1000); last = ts;
      if (runningRef.current) {
        spawnAcc += dt;
        if (spawnAcc >= 0.85) { spawnAcc = 0; spawn(); }
        const dist = 240 * speedRef.current * dt;
        for (let i = packets.length - 1; i >= 0; i--) {
          if (advance(packets[i], dist)) {
            count++; if (cntRef.current) cntRef.current.textContent = String(count);
            dashFlash = 0.5; packets.splice(i, 1);
          }
        }
        workers.forEach((w) => { if (w.pulse > 0) w.pulse = Math.max(0, w.pulse - dt * 2); });
        if (dashFlash > 0) dashFlash = Math.max(0, dashFlash - dt);
      }
      renderPackets(); refreshDynamic();
      raf = requestAnimationFrame(frame);
    };

    const init = () => {
      workers = LANES.map((y, i) => ({ id: i, lane: y, alive: true, pulse: 0 }));
      assign = [0, 1, 2, 3]; packets = []; count = 0; spawnAcc = 0; rrCounter = 0; dashFlash = 0;
      if (cntRef.current)     cntRef.current.textContent = "0";
      if (packetsRef.current) packetsRef.current.innerHTML = "";
      if (logRef.current)     logRef.current.innerHTML = "";
      drawStatic(); drawNodes(); drawHeartbeats();
      logMsg("info", "System started — 4 partitions, 4 workers, master coordinating.");
    };

    apiRef.current = {
      killRandom:  () => { const a = workers.filter((w) => w.alive); if (a.length) killWorker(a[Math.floor(Math.random() * a.length)].id); },
      reviveAll:   () => workers.forEach((w) => { if (!w.alive) reviveWorker(w.id); }),
      reset:       () => init(),
      redrawNodes: () => drawNodes(),
    };

    init();
    raf = requestAnimationFrame(frame);
    return () => { cancelAnimationFrame(raf); timeouts.forEach((id) => clearTimeout(id)); timeouts.clear(); };
  }, []);

  const bench = BENCH[dataset];

  return (
    <div className="space-y-4">
      <style>{`
        @keyframes plsim-hb   { 0%,100%{opacity:.12} 50%{opacity:.65} }
        @keyframes plsim-fade { from{opacity:0;transform:translateY(-4px)} to{opacity:1} }
      `}</style>

      <header>
        <h1 className="text-[22px] font-semibold tracking-tight flex items-center gap-2">
          <Network className="h-5 w-5" style={{ color: C.gold }} />
          Pipeline Simulator
        </h1>
        <p className="mt-1 text-[13px]" style={{ color: C.mut, fontFamily: "monospace" }}>
          Distributed processing &amp; fault tolerance demo · click a worker to kill it · 100% client-side
        </p>
      </header>

      <div className="p-2.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
        <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} role="img" aria-label="Animated distributed pipeline simulator" className="block w-full h-auto">
          <defs>
            <marker id="plsim-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
              <path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
            </marker>
            <filter id="plsim-glow" x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="3" result="b" />
              <feMerge><feMergeNode in="b" /><feMergeNode in="SourceGraphic" /></feMerge>
            </filter>
          </defs>
          <g ref={linksRef} />
          <g ref={hbRef} />
          <g ref={packetsRef} />
          <g ref={nodesRef} />
        </svg>
      </div>

      {/* Controls + log */}
      <div className="flex gap-3.5 flex-wrap">
        <div className="flex-1 min-w-[280px] space-y-3.5">
          <div className="p-3.5 rounded-[14px] flex flex-wrap gap-2.5 items-center"
            style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <button onClick={() => setRunning((r) => !r)} className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium"
              style={running
                ? { color: "#0f1419", background: C.gold, border: `1px solid ${C.gold}` }
                : { color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}>
              <span className="inline-flex items-center gap-1.5">
                {running ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
                {running ? "Pause" : "Play"}
              </span>
            </button>
            <button onClick={() => apiRef.current?.killRandom()} className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium"
              style={{ color: "#ffd2d2", background: "#2a1a1d", border: "1px solid #5a2a2a" }}>
              Kill random worker
            </button>
            <button onClick={() => apiRef.current?.reviveAll()} className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium"
              style={{ color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}>
              Revive all
            </button>
            <button onClick={() => apiRef.current?.reset()} className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium inline-flex items-center gap-1.5"
              style={{ color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}>
              <RotateCcw className="h-3.5 w-3.5" /> Reset
            </button>
            <div className="flex items-center gap-2 text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>
              <Zap className="h-3.5 w-3.5" /> speed
              <input type="range" min={0.4} max={3} step={0.1} value={speed}
                onChange={(e) => setSpeed(parseFloat(e.target.value))}
                style={{ accentColor: C.gold, width: 120 }} />
              <span>{speed.toFixed(1)}×</span>
            </div>
            <div className="w-full text-[12px] mt-0.5" style={{ color: C.mut, fontFamily: "monospace" }}>
              Tip: click any Spark worker box in the diagram to kill or revive it.
            </div>
          </div>

          <div className="p-3.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <div className="flex gap-[18px] items-baseline mb-3">
              <span ref={cntRef} style={{ fontFamily: "monospace", fontSize: 34, fontWeight: 600, color: C.gold }}>0</span>
              <span className="text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>detections written to MongoDB</span>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1.5 text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>
              {([ ["ingest", C.blue], ["kafka broker", C.amber], ["spark worker (alive)", C.green],
                  ["worker down", C.red], ["storage / view", C.purple], ["transaction", C.gold],
              ] as [string, string][]).map(([label, color]) => (
                <span key={label} className="flex items-center gap-1.5">
                  <i className="w-2.5 h-2.5 rounded-[3px]" style={{ background: color }} /> {label}
                </span>
              ))}
            </div>
          </div>
        </div>

        <div className="flex-1 min-w-[280px]">
          <div className="p-3.5 rounded-[14px] h-full min-h-[200px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <h2 className="text-[13px] font-semibold m-0 mb-2.5 uppercase tracking-[0.8px]" style={{ color: C.mut }}>Event log</h2>
            <ul ref={logRef} className="list-none m-0 p-0" style={{ fontFamily: "monospace", fontSize: 12.5, color: C.text }} />
          </div>
        </div>
      </div>

      {/* Benchmark panel */}
      <div className="p-3.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
        <div className="flex items-center justify-between flex-wrap gap-3 mb-3">
          <h2 className="text-[13px] font-semibold m-0 uppercase tracking-[0.8px] flex items-center gap-2" style={{ color: C.mut }}>
            <Activity className="h-4 w-4" /> Ingestion benchmark · listener.py vs listener_mp.py
          </h2>
          <div className="flex gap-1.5">
            {([60, 865] as DatasetKey[]).map((d) => (
              <button key={d} onClick={() => setDataset(d)} className="px-3 py-1.5 rounded-[9px] text-[12px] font-medium"
                style={dataset === d
                  ? { color: "#0f1419", background: C.gold, border: `1px solid ${C.gold}` }
                  : { color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}>
                {d === 60 ? "60-tx dataset" : "865-tx dataset"}
              </button>
            ))}
          </div>
        </div>

        <div className="flex flex-wrap gap-x-6 gap-y-2 mb-3 text-[12px]" style={{ fontFamily: "monospace" }}>
          <span style={{ color: C.mut }}>rows <span style={{ color: C.text }}>{bench.rows}</span></span>
          <span style={{ color: C.mut }}>detected <span style={{ color: C.green }}>{bench.detected}</span></span>
          <span style={{ color: C.mut }}>filtered <span style={{ color: C.amber }}>{bench.filtered}</span></span>
          <span style={{ color: C.mut }}>best <span style={{ color: C.gold }}>{bench.best}</span></span>
          <span className="inline-flex items-center gap-1" style={{ color: C.mut }}>
            <Database className="h-3 w-3" /> offline replay ·{" "}
            <span style={{ color: C.text }}>--offline --benchmark --no-kafka</span>
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left" style={{ fontFamily: "monospace", fontSize: 12.5, borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: C.mut }}>
                <th className="py-2 pr-4 font-normal">workers</th>
                <th className="py-2 pr-4 font-normal">speedup</th>
                <th className="py-2 pr-4 font-normal">throughput (det/s)</th>
                <th className="py-2 pr-4 font-normal">time (s)</th>
                <th className="py-2 pr-4 font-normal">per-worker loads</th>
              </tr>
            </thead>
            <tbody>
              {bench.sweep.map((row) => {
                const isBest = bench.best.startsWith(String(row.workers));
                return (
                  <tr key={row.workers} style={{ borderTop: `1px solid ${C.line}` }}>
                    <td className="py-2 pr-4" style={{ color: C.text }}>{row.workers}</td>
                    <td className="py-2 pr-4" style={{ color: row.speedup >= 1 ? C.green : C.red }}>{row.speedup.toFixed(2)}×</td>
                    <td className="py-2 pr-4" style={{ color: C.text }}>{row.throughput ? row.throughput.toFixed(1) : "—"}</td>
                    <td className="py-2 pr-4" style={{ color: C.text }}>{row.timeS ? row.timeS.toFixed(3) : "—"}</td>
                    <td className="py-2 pr-4" style={{ color: isBest ? C.gold : C.mut }}>{row.loads ? `[${row.loads.join(", ")}]` : "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <p className="mt-3 text-[12px] leading-relaxed" style={{ color: C.mut }}>{bench.takeaway}</p>
        <p className="mt-2 text-[11px] leading-relaxed" style={{ color: C.mut, opacity: 0.8 }}>
          Note: this benchmark measures the <span style={{ color: C.text }}>ingestion</span> layer (listener_mp.py
          multiprocessing, offline CSV replay, zero RPC). The animated workers above are the{" "}
          <span style={{ color: C.text }}>Spark consumer</span> stage — a separate worker pool bounded by the 4 Kafka partitions.
        </p>
      </div>
    </div>
  );
}
