// frontend/src/pages/PipelineSimulatorPage.tsx
// Animated, fully client-side SVG simulator of the real detection pipeline:
//   Mock node -> listener_mp.py (feeder + 4 workers) -> Kafka raw_txns (4 partitions)
//   -> Spark Master + 4 workers -> MongoDB Atlas -> Dashboard (SSE).
//
// Behaviour ported from the project's standalone HTML mock: gold packets flow
// through the pipeline, workers are clickable (kill/revive), the Spark master
// reassigns partitions on worker death (read on from checkpoint/offset), with a
// heartbeat overlay, event log, speed control and a detections counter.
//
// NOTHING here touches real containers — it is a teaching visualization only.
// The 60-tx / 865-tx panels show the ACTUAL offline benchmark numbers we
// measured for the ingestion layer (listener.py vs listener_mp.py).
import { useEffect, useRef, useState } from "react";
import { Play, Pause, RotateCcw, Zap, Database, Activity, Network } from "lucide-react";

// ---- palette (matches the standalone HTML design) ----
const C = {
  blue: "#4a9eff",
  amber: "#f0a93b",
  green: "#5fd07a",
  purple: "#b08cf5",
  red: "#ff5d5d",
  gold: "#ffd24a",
  mut: "#8a96a6",
  line: "#2a3441",
  text: "#e7ebf0",
};
const LANES = [170, 280, 390, 500];

type DatasetKey = 60 | 865;

// ---- REAL measured offline benchmark (listener_mp.py --offline --benchmark) ----
interface SweepRow {
  workers: number;
  speedup: number;
  throughput?: number; // detections/sec
  timeS?: number;
  loads?: number[];
}
interface DatasetBench {
  rows: number;
  detected: number;
  filtered: number;
  best: string;
  takeaway: string;
  sweep: SweepRow[];
}
const BENCH: Record<DatasetKey, DatasetBench> = {
  60: {
    rows: 60,
    detected: 35,
    filtered: 25,
    best: "1 worker",
    takeaway:
      "Too little work to amortize process-spawn cost — multiprocessing is a net loss; single-process wins.",
    sweep: [
      { workers: 1, speedup: 1.0, loads: [60] },
      { workers: 2, speedup: 0.96 },
      { workers: 4, speedup: 0.91, loads: [16, 12, 13, 19] },
      { workers: 8, speedup: 0.84 },
    ],
  },
  865: {
    rows: 865,
    detected: 429,
    filtered: 436,
    best: "2 workers · 2.39×",
    takeaway:
      "Enough total work to amortize spawn overhead — 2–4 workers win; gains taper past 4 as per-worker share shrinks.",
    sweep: [
      { workers: 1, speedup: 1.0, throughput: 636.6, timeS: 0.674, loads: [865] },
      { workers: 2, speedup: 2.39, throughput: 1519.0, timeS: 0.282, loads: [440, 425] },
      { workers: 4, speedup: 2.26, throughput: 1439.6, timeS: 0.298, loads: [227, 207, 222, 209] },
      { workers: 8, speedup: 1.72, throughput: 1094.5, timeS: 0.392, loads: [110, 114, 105, 110, 104, 105, 116, 101] },
    ],
  },
};

interface Worker {
  id: number;
  lane: number;
  alive: boolean;
  pulse: number;
}
interface Packet {
  partition: number;
  worker: number | null;
  seg: number;
  t: number;
  held: boolean;
}
type LogType = "info" | "ok" | "warn" | "err";

export default function PipelineSimulatorPage() {
  const [running, setRunning] = useState(true);
  const [speed, setSpeed] = useState(1);
  const [dataset, setDataset] = useState<DatasetKey>(865);

  // refs the animation loop reads (so the mount effect runs once)
  const runningRef = useRef(running);
  const speedRef = useRef(speed);
  const datasetRef = useRef<DatasetKey>(dataset);

  // SVG group + DOM refs (imperatively managed)
  const linksRef = useRef<SVGGElement>(null);
  const hbRef = useRef<SVGGElement>(null);
  const packetsRef = useRef<SVGGElement>(null);
  const nodesRef = useRef<SVGGElement>(null);
  const cntRef = useRef<HTMLSpanElement>(null);
  const logRef = useRef<HTMLUListElement>(null);

  // imperative control surface exposed by the mount effect
  const apiRef = useRef<{
    killRandom: () => void;
    reviveAll: () => void;
    reset: () => void;
    redrawNodes: () => void;
  } | null>(null);

  useEffect(() => { runningRef.current = running; }, [running]);
  useEffect(() => { speedRef.current = speed; }, [speed]);
  useEffect(() => {
    datasetRef.current = dataset;
    apiRef.current?.redrawNodes();
  }, [dataset]);

  // ---- the simulation (mount once) ----
  useEffect(() => {
    const SVGNS = "http://www.w3.org/2000/svg";
    let workers: Worker[] = [];
    let assign: number[] = [];
    let packets: Packet[] = [];
    let count = 0;
    let spawnAcc = 0;
    let last = 0;
    let rrCounter = 0;
    let dashFlash = 0;
    let lastDash = -1;
    let lastPulseKey = "";
    let raf = 0;
    const timeouts = new Set<ReturnType<typeof setTimeout>>();
    const setT = (fn: () => void, ms: number) => {
      const id = setTimeout(() => { timeouts.delete(id); fn(); }, ms);
      timeouts.add(id);
      return id;
    };

    const el = (tag: string, attrs: Record<string, string | number>) => {
      const e = document.createElementNS(SVGNS, tag);
      for (const k in attrs) e.setAttribute(k, String(attrs[k]));
      return e as SVGElement;
    };
    const txt = (x: number, y: number, s: string, fill: string, size = 13) => {
      const t = el("text", { x, y, "text-anchor": "middle", "dominant-baseline": "central", fill, "font-size": size });
      t.textContent = s;
      return t;
    };
    const box = (x: number, y: number, w: number, h: number, fill: string, stroke: string, rx = 10) =>
      el("rect", { x, y, width: w, height: h, rx, fill, stroke, "stroke-width": 1.2 });
    const fillFor = (hex: string) => hex + "22";

    const drawStatic = () => {
      const g = linksRef.current!;
      g.innerHTML = "";
      const seg = (x1: number, y1: number, x2: number, y2: number) =>
        el("line", { x1, y1, x2, y2, stroke: C.line, "stroke-width": 1.5, "marker-end": "url(#plsim-ar)" });
      g.appendChild(seg(150, 335, 195, 335)); // mock -> listener outer box
      g.appendChild(seg(390, 335, 430, 335)); // listener -> kafka
      g.appendChild(seg(1055, 330, 1055, 373)); // mongo -> dashboard
    };

    const drawHeartbeats = () => {
      const g = hbRef.current!;
      g.innerHTML = "";
      workers.forEach((w) => {
        if (!w.alive) return;
        const l = el("line", {
          x1: 770, y1: 122, x2: 770, y2: w.lane - 34,
          stroke: C.green, "stroke-width": 1, "stroke-dasharray": "2 6", opacity: 0.5,
        });
        (l as SVGLineElement).style.animation = "plsim-hb 1s linear infinite";
        g.appendChild(l);
      });
    };

    // Bug B fix: listener_mp.py shows feeder -> Queue -> worker-0..3 fan layout
    const drawListenerGroup = (g: SVGGElement) => {
      // Outer container for listener_mp.py: x=196, y=200, w=194, h=270
      const lx = 196, ly = 200, lw = 194, lh = 270;
      g.appendChild(box(lx, ly, lw, lh, fillFor(C.blue), C.blue, 12));
      g.appendChild(txt(lx + lw / 2, ly + 16, "listener_mp.py", C.blue, 11));

      // feeder sub-box (top center)
      const fx = lx + 30, fy = ly + 30, fw = 134, fh = 36;
      g.appendChild(box(fx, fy, fw, fh, fillFor(C.blue), C.blue, 7));
      g.appendChild(txt(fx + fw / 2, fy + 18, "feeder", C.text, 12));

      // Queue arrow down from feeder
      const qx = lx + lw / 2;
      const qy1 = fy + fh;
      const qy2 = qy1 + 20;
      g.appendChild(el("line", { x1: qx, y1: qy1, x2: qx, y2: qy2, stroke: C.blue, "stroke-width": 1.2, "marker-end": "url(#plsim-ar)" }));
      g.appendChild(txt(qx, qy1 + 10, "Queue", C.mut, 10));

      // worker sub-boxes fanned out (4 workers, horizontal)
      const wLabels = ["w-0", "w-1", "w-2", "w-3"];
      const wwBoxW = 34, wwBoxH = 30;
      const wwStartX = lx + 10;
      const wwY = qy2 + 14;
      const wwStep = (lw - 20) / 4;
      wLabels.forEach((label, i) => {
        const wx = wwStartX + i * wwStep;
        g.appendChild(box(wx, wwY, wwBoxW, wwBoxH, fillFor(C.blue), C.blue, 5));
        g.appendChild(txt(wx + wwBoxW / 2, wwY + 15, label, C.text, 10));
        // horizontal fan line from queue bottom to each worker
        const fanX = wx + wwBoxW / 2;
        g.appendChild(el("line", { x1: qx, y1: qy2, x2: fanX, y2: wwY, stroke: C.blue, "stroke-width": 1, opacity: 0.5 }));
        // arrow from each worker to Kafka partition i
        const kPartY = LANES[i];
        g.appendChild(el("line", {
          x1: wx + wwBoxW, y1: wwY + wwBoxH / 2,
          x2: 430, y2: kPartY,
          stroke: C.line, "stroke-width": 1.2, "marker-end": "url(#plsim-ar)",
        }));
      });
    };

    const drawNodes = () => {
      const g = nodesRef.current!;
      g.innerHTML = "";
      const ds = datasetRef.current;

      // Mock node
      g.appendChild(box(40, 303, 110, 64, fillFor(C.blue), C.blue));
      g.appendChild(txt(95, 326, "Mock node", C.text));
      g.appendChild(txt(95, 346, `${ds} tx`, C.mut, 12));

      // listener_mp.py group (Bug B fix)
      drawListenerGroup(g as unknown as SVGGElement);

      // Kafka — shifted right to x=432 to accommodate expanded listener
      g.appendChild(box(432, 140, 150, 400, fillFor(C.amber), C.amber, 14));
      g.appendChild(txt(507, 162, "Kafka", C.amber, 13));
      g.appendChild(txt(507, 180, "topic raw_txns", C.mut, 11));
      LANES.forEach((y, i) => {
        g.appendChild(box(450, y - 22, 114, 44, fillFor(C.amber), C.amber, 7));
        g.appendChild(txt(507, y, "Partition " + i, C.text, 12));
      });

      // Spark master — shifted right
      g.appendChild(box(680, 62, 180, 58, fillFor(C.purple), C.purple));
      g.appendChild(txt(770, 84, "Spark Master", C.text));
      g.appendChild(txt(770, 103, "coordinator · heartbeat", C.mut, 11));

      // Workers (clickable) — shifted right
      workers.forEach((w, i) => {
        const dead = !w.alive;
        const stroke = dead ? C.red : C.green;
        const grp = el("g", { style: "cursor:pointer" });
        grp.addEventListener("click", () => toggleWorker(i));
        const fill = dead ? fillFor(C.red) : (w.pulse > 0.05 ? C.green + "55" : fillFor(C.green));
        const r = box(680, w.lane - 32, 180, 64, fill, stroke);
        if (dead) r.setAttribute("stroke-dasharray", "5 4");
        grp.appendChild(r);
        grp.appendChild(txt(770, w.lane - 8, "Worker " + (i + 1), C.text));
        const parts = assign.map((wk, p) => (wk === i ? p : -1)).filter((p) => p >= 0);
        grp.appendChild(txt(770, w.lane + 12, dead ? "DOWN" : (parts.length ? "part " + parts.join(",") : "idle"), dead ? C.red : C.mut, 12));
        g.appendChild(grp);
      });

      // partition -> worker assignment lines
      assign.forEach((wk, p) => {
        if (wk < 0 || !workers[wk]?.alive) return;
        g.appendChild(el("line", { x1: 564, y1: LANES[p], x2: 678, y2: workers[wk].lane, stroke: C.line, "stroke-width": 1.4, "marker-end": "url(#plsim-ar)" }));
      });

      // worker -> mongo
      workers.forEach((w) => {
        if (!w.alive) return;
        g.appendChild(el("line", { x1: 860, y1: w.lane, x2: 968, y2: 275, stroke: C.line, "stroke-width": 1.2, "marker-end": "url(#plsim-ar)" }));
      });

      // Mongo — shifted right
      g.appendChild(box(970, 220, 170, 110, fillFor(C.purple), C.purple, 12));
      g.appendChild(txt(1055, 262, "MongoDB Atlas", C.text));
      g.appendChild(txt(1055, 284, "foreachPartition", C.mut, 11));

      // Dashboard — shifted right
      const dFill = dashFlash > 0 ? C.red + "55" : fillFor(C.purple);
      const dStroke = dashFlash > 0 ? C.red : C.purple;
      g.appendChild(box(970, 373, 170, 110, dFill, dStroke, 12));
      g.appendChild(txt(1055, 415, "Dashboard", C.text));
      g.appendChild(txt(1055, 437, "SSE real-time", C.mut, 11));
      g.appendChild(txt(1055, 458, dashFlash > 0 ? "⚠ ALERT" : "", C.red, 12));
    };

    const waypoints = (p: Packet) => {
      const Li = LANES[p.partition];
      const w = p.worker != null ? p.worker : assign[p.partition];
      const Lj = w >= 0 && workers[w] ? workers[w].lane : Li;
      // Updated x-positions to match shifted layout
      return [
        { x: 95, y: 335 },   // mock node
        { x: 293, y: 335 },  // listener_mp center
        { x: 507, y: Li },   // kafka partition
        { x: 564, y: Li },   // kafka right edge
        { x: 770, y: Lj },   // spark worker
        { x: 860, y: Lj },   // spark worker right edge
        { x: 1055, y: 275 }, // mongo
      ];
    };
    const spawn = () => {
      const partition = rrCounter++ % 4;
      packets.push({ partition, worker: null, seg: 0, t: 0, held: false });
    };
    const advance = (p: Packet, dist: number): boolean => {
      const wp = waypoints(p);
      while (dist > 0 && p.seg < wp.length - 1) {
        const a = wp[p.seg], b = wp[p.seg + 1];
        const len = Math.hypot(b.x - a.x, b.y - a.y);
        if (p.seg === 3) {
          const w = assign[p.partition];
          if (w < 0 || !workers[w].alive) { p.held = true; return false; }
          p.worker = w; p.held = false;
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
      const layer = packetsRef.current!;
      layer.innerHTML = "";
      packets.forEach((p) => {
        const pos = posOf(p);
        for (let k = 3; k >= 1; k--) {
          layer.appendChild(el("circle", { cx: pos.x, cy: pos.y, r: 3, fill: C.gold, opacity: 0.12 * k }));
        }
        layer.appendChild(el("circle", { cx: pos.x, cy: pos.y, r: 5, fill: C.gold, filter: "url(#plsim-glow)" }));
      });
    };

    const toggleWorker = (i: number) => (workers[i].alive ? killWorker(i) : reviveWorker(i));
    const killWorker = (i: number) => {
      if (!workers[i].alive) return;
      workers[i].alive = false; workers[i].pulse = 0;
      drawNodes(); drawHeartbeats();
      // Bug A fix: English strings
      logMsg("err", "⚠ Worker " + (i + 1) + " stopped responding (process died).");
      setT(() => {
        if (workers[i].alive) return;
        flashMaster();
        logMsg("warn", "Master: Worker " + (i + 1) + " missed heartbeat → marking DOWN.");
        const aliveIdx = workers.filter((w) => w.alive).map((w) => w.id);
        const moved = assign.map((wk, p) => (wk === i ? p : -1)).filter((p) => p >= 0);
        if (aliveIdx.length === 0) {
          logMsg("err", "All workers dead — processing paused, Kafka holding messages in queue.");
          drawNodes();
          return;
        }
        moved.forEach((p) => {
          const tgt = aliveIdx[rrCounter++ % aliveIdx.length];
          assign[p] = tgt;
          logMsg("ok", "Partition " + p + " → reassigned to Worker " + (tgt + 1) + " (resuming from checkpoint).");
        });
        drawNodes();
      }, 1300);
    };
    const reviveWorker = (i: number) => {
      if (workers[i].alive) return;
      workers[i].alive = true;
      // Bug A fix: English strings
      logMsg("ok", "Worker " + (i + 1) + " back online.");
      if (assign[i] !== i) {
        assign[i] = i;
        logMsg("info", "Partition " + i + " → returned to Worker " + (i + 1) + " (rebalance).");
      }
      drawNodes(); drawHeartbeats();
    };
    const flashMaster = () => {
      const o = el("rect", { x: 680, y: 62, width: 180, height: 58, rx: 10, fill: "none", stroke: C.red, "stroke-width": 2 });
      nodesRef.current!.appendChild(o);
      setT(() => o.remove(), 600);
    };

    const logMsg = (type: LogType, msg: string) => {
      const ul = logRef.current!;
      const li = document.createElement("li");
      li.className = "plsim-log-" + type;
      li.style.animation = "plsim-fade .3s";
      const tag = { info: "›", ok: "✓", warn: "!", err: "✕" }[type];
      const color = { info: C.blue, ok: C.green, warn: C.amber, err: C.red }[type];
      li.innerHTML =
        '<span style="flex:none;width:14px;text-align:center;color:' + color + '">' + tag + "</span><span>" + msg + "</span>";
      ul.insertBefore(li, ul.firstChild);
      while (ul.children.length > 8) ul.removeChild(ul.lastChild!);
    };

    const refreshDynamic = () => {
      const dKey = dashFlash > 0 ? 1 : 0;
      const pKey = workers.map((w) => (w.alive ? (w.pulse > 0.05 ? "P" : "a") : "d")).join("");
      if (dKey !== lastDash || pKey !== lastPulseKey) {
        lastDash = dKey; lastPulseKey = pKey; drawNodes();
      }
    };

    const frame = (ts: number) => {
      if (!last) last = ts;
      const dt = Math.min(0.05, (ts - last) / 1000);
      last = ts;
      if (runningRef.current) {
        spawnAcc += dt;
        if (spawnAcc >= 0.85) { spawnAcc = 0; spawn(); }
        const dist = 240 * speedRef.current * dt;
        for (let i = packets.length - 1; i >= 0; i--) {
          if (advance(packets[i], dist)) {
            count++;
            if (cntRef.current) cntRef.current.textContent = String(count);
            dashFlash = 0.5;
            packets.splice(i, 1);
          }
        }
        workers.forEach((w) => { if (w.pulse > 0) w.pulse = Math.max(0, w.pulse - dt * 2); });
        if (dashFlash > 0) dashFlash = Math.max(0, dashFlash - dt);
      }
      renderPackets();
      refreshDynamic();
      raf = requestAnimationFrame(frame);
    };

    const init = () => {
      workers = LANES.map((y, i) => ({ id: i, lane: y, alive: true, pulse: 0 }));
      assign = [0, 1, 2, 3];
      packets = []; count = 0; spawnAcc = 0; rrCounter = 0; dashFlash = 0;
      if (cntRef.current) cntRef.current.textContent = "0";
      if (packetsRef.current) packetsRef.current.innerHTML = "";
      if (logRef.current) logRef.current.innerHTML = "";
      drawStatic(); drawNodes(); drawHeartbeats();
      // Bug A fix: English init message
      logMsg("info", "System started: 4 partitions, 4 workers, master coordinating.");
    };

    apiRef.current = {
      killRandom: () => {
        const a = workers.filter((w) => w.alive);
        if (a.length) killWorker(a[Math.floor(Math.random() * a.length)].id);
      },
      reviveAll: () => workers.forEach((w) => { if (!w.alive) reviveWorker(w.id); }),
      reset: () => init(),
      redrawNodes: () => drawNodes(),
    };

    init();
    raf = requestAnimationFrame(frame);

    return () => {
      cancelAnimationFrame(raf);
      timeouts.forEach((id) => clearTimeout(id));
      timeouts.clear();
    };
  }, []);

  const bench = BENCH[dataset];

  return (
    <div className="space-y-4">
      {/* keyframes for heartbeat / packet / log animations */}
      <style>{`
        @keyframes plsim-hb {0%,100%{opacity:.15}50%{opacity:.7}}
        @keyframes plsim-fade {from{opacity:0;transform:translateY(-4px)}to{opacity:1}}
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

      {/* SVG stage — widened viewBox to 1220 to fit expanded listener group */}
      <div className="p-2.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
        <svg viewBox="0 0 1220 590" role="img" aria-label="Animated distributed pipeline simulator" className="block w-full h-auto">
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

      {/* lower: controls + log */}
      <div className="flex gap-3.5 flex-wrap">
        <div className="flex-1 min-w-[280px] space-y-3.5">
          {/* controls */}
          <div className="p-3.5 rounded-[14px] flex flex-wrap gap-2.5 items-center" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <button
              onClick={() => setRunning((r) => !r)}
              className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium transition-colors"
              style={running
                ? { color: "#0f1419", background: C.gold, border: `1px solid ${C.gold}` }
                : { color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}
            >
              <span className="inline-flex items-center gap-1.5">
                {running ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
                {running ? "Pause" : "Play"}
              </span>
            </button>
            <button
              onClick={() => apiRef.current?.killRandom()}
              className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium transition-colors"
              style={{ color: "#ffd2d2", background: "#2a1a1d", border: "1px solid #5a2a2a" }}
            >
              Kill random worker
            </button>
            <button
              onClick={() => apiRef.current?.reviveAll()}
              className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium transition-colors"
              style={{ color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}
            >
              Revive all
            </button>
            <button
              onClick={() => apiRef.current?.reset()}
              className="px-3.5 py-2 rounded-[9px] text-[13px] font-medium transition-colors inline-flex items-center gap-1.5"
              style={{ color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}
            >
              <RotateCcw className="h-3.5 w-3.5" /> Reset
            </button>
            <div className="flex items-center gap-2 text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>
              <Zap className="h-3.5 w-3.5" /> speed
              <input
                type="range" min={0.4} max={3} step={0.1} value={speed}
                onChange={(e) => setSpeed(parseFloat(e.target.value))}
                style={{ accentColor: C.gold, width: 120 }}
              />
              <span>{speed.toFixed(1)}×</span>
            </div>
            <div className="w-full text-[12px] mt-0.5" style={{ color: C.mut, fontFamily: "monospace" }}>
              Tip: click any Spark worker box in the diagram to kill or revive it.
            </div>
          </div>

          {/* counter + legend */}
          <div className="p-3.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <div className="flex gap-[18px] items-baseline mb-3">
              <span ref={cntRef} style={{ fontFamily: "monospace", fontSize: 34, fontWeight: 600, color: C.gold }}>0</span>
              <span className="text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>detections written to MongoDB</span>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1.5 text-[12px]" style={{ color: C.mut, fontFamily: "monospace" }}>
              {[
                ["ingest", C.blue], ["kafka broker", C.amber], ["spark worker (alive)", C.green],
                ["worker down", C.red], ["storage / view", C.purple], ["transaction", C.gold],
              ].map(([label, color]) => (
                <span key={label} className="flex items-center gap-1.5">
                  <i className="w-2.5 h-2.5 rounded-[3px]" style={{ background: color as string }} /> {label}
                </span>
              ))}
            </div>
          </div>
        </div>

        {/* event log */}
        <div className="flex-1 min-w-[280px]">
          <div className="p-3.5 rounded-[14px] h-full min-h-[200px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
            <h2 className="text-[13px] font-semibold m-0 mb-2.5 uppercase tracking-[0.8px]" style={{ color: C.mut }}>Event log</h2>
            <ul ref={logRef} className="list-none m-0 p-0" style={{ fontFamily: "monospace", fontSize: 12.5 }} />
          </div>
        </div>
      </div>

      {/* benchmark panel — REAL measured numbers, ingestion layer */}
      <div className="p-3.5 rounded-[14px]" style={{ background: "#161d27", border: `1px solid ${C.line}` }}>
        <div className="flex items-center justify-between flex-wrap gap-3 mb-3">
          <h2 className="text-[13px] font-semibold m-0 uppercase tracking-[0.8px] flex items-center gap-2" style={{ color: C.mut }}>
            <Activity className="h-4 w-4" /> Ingestion benchmark · listener.py vs listener_mp.py
          </h2>
          <div className="flex gap-1.5">
            {([60, 865] as DatasetKey[]).map((d) => (
              <button
                key={d}
                onClick={() => setDataset(d)}
                className="px-3 py-1.5 rounded-[9px] text-[12px] font-medium transition-colors"
                style={dataset === d
                  ? { color: "#0f1419", background: C.gold, border: `1px solid ${C.gold}` }
                  : { color: C.text, background: "#1c2531", border: `1px solid ${C.line}` }}
              >
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
            <Database className="h-3 w-3" /> offline replay · <span style={{ color: C.text }}>--offline --benchmark --no-kafka</span>
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
                    <td className="py-2 pr-4" style={{ color: row.speedup >= 1 ? C.green : C.red }}>
                      {row.speedup.toFixed(2)}×
                    </td>
                    <td className="py-2 pr-4" style={{ color: C.text }}>{row.throughput ? row.throughput.toFixed(1) : "—"}</td>
                    <td className="py-2 pr-4" style={{ color: C.text }}>{row.timeS ? row.timeS.toFixed(3) : "—"}</td>
                    <td className="py-2 pr-4" style={{ color: isBest ? C.gold : C.mut }}>
                      {row.loads ? `[${row.loads.join(", ")}]` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <p className="mt-3 text-[12px] leading-relaxed" style={{ color: C.mut }}>
          {bench.takeaway}
        </p>
        <p className="mt-2 text-[11px] leading-relaxed" style={{ color: C.mut, opacity: 0.8 }}>
          Note: this benchmark measures the <span style={{ color: C.text }}>ingestion</span> layer (listener_mp.py multiprocessing,
          offline CSV replay, zero RPC). The animated workers above are the <span style={{ color: C.text }}>Spark consumer</span> stage —
          a separate worker pool bounded by the 4 Kafka partitions.
        </p>
      </div>
    </div>
  );
}
