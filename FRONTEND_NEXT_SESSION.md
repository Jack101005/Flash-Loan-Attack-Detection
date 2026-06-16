# Frontend — Next Session Handoff

Read this file at the start of the session. It records what was built,
what is currently broken or wrong, and exactly what to build next.
Cross-reference with `CLAUDE.md` (project overview) and
`DISTRIBUTED_SYSTEM_ANALYSIS.md` (characteristics + demo evidence).

---

## 1. Current frontend state

### Pages that exist

| Route | File | Status |
|---|---|---|
| `/` | `src/pages/HomePage.tsx` | ✅ Working — SSE live table + KPI cards |
| `/pipeline-simulator` | `src/pages/PipelineSimulatorPage.tsx` | ⚠️ Built, has 2 bugs (see §2) |
| `/flow` | `src/pages/TransactionFlowPage.tsx` | ✅ Built |
| `/workers` | `src/pages/WorkerSimulationPage.tsx` | ✅ Built |
| `/decode` | `src/pages/DecodePage.tsx` | ✅ Working — manual tx hash lookup |

### Layout & routing

- `src/components/layout/DashboardLayout.tsx` — persistent `<Outlet/>` layout
  with sticky top NavBar. NavLink active-state highlighting. All 5 pages wired.
- `src/App.tsx` — layout route wraps all 5 page routes.
- `src/lib/config.ts` — central `API_URL` and `SSE_DETECTIONS_URL`
  (reads `VITE_API_URL` env var, falls back to `localhost:8000`).

### Tech stack reminder

React 19, TypeScript (strict: `noUnusedLocals`, `noUnusedParameters`,
`verbatimModuleSyntax`, `erasableSyntaxOnly`), Vite, Tailwind 3.4,
React Router 7, lucide-react, recharts, shadcn-style components.
Custom Tailwind colors `neon-red` and `acid-green` are registered as
static hex values in `tailwind.config.js` (they were previously only
CSS vars, so opacity modifiers didn't work).

---

## 2. Known bugs to fix in PipelineSimulatorPage.tsx

### Bug A — Event log messages are in Vietnamese

All strings passed to `logMsg()` inside the simulation's mount effect are
Vietnamese. They need to be replaced with English. The affected strings are:

```typescript
// Current (wrong):
logMsg("info", "Hệ thống khởi động: 4 partition, 4 worker, master điều phối.");
logMsg("err",  "⚠ Worker " + (i+1) + " ngừng phản hồi (process chết).");
logMsg("warn", "Master: Worker " + (i+1) + " trượt heartbeat → đánh dấu DOWN.");
logMsg("err",  "Tất cả worker đã chết — xử lý tạm dừng, Kafka giữ tin trong hàng đợi.");
logMsg("ok",   "Partition " + p + " → giao lại cho Worker " + (tgt+1) + " (đọc tiếp từ checkpoint).");
logMsg("ok",   "Worker " + (i+1) + " online trở lại.");
logMsg("info", "Partition " + i + " → trả về Worker " + (i+1) + " (rebalance).");

// Target (correct English):
logMsg("info", "System started: 4 partitions, 4 workers, master coordinating.");
logMsg("err",  "⚠ Worker " + (i+1) + " stopped responding (process died).");
logMsg("warn", "Master: Worker " + (i+1) + " missed heartbeat → marking DOWN.");
logMsg("err",  "All workers dead — processing paused, Kafka holding messages in queue.");
logMsg("ok",   "Partition " + p + " → reassigned to Worker " + (tgt+1) + " (resuming from checkpoint).");
logMsg("ok",   "Worker " + (i+1) + " back online.");
logMsg("info", "Partition " + i + " → returned to Worker " + (i+1) + " (rebalance).");
```

### Bug B — Listener box shows wrong architecture

The current SVG shows `listener_mp.py` as a single box between Mock node
and Kafka. This is architecturally incorrect. The real design is:

```
Mock node  →  feeder process  →  Queue  →  worker-0  →  Kafka partition 0
                                        →  worker-1  →  Kafka partition 1
                                        →  worker-2  →  Kafka partition 2
                                        →  worker-3  →  Kafka partition 3
```

The "Listener" box in the SVG must be redesigned to show:
- One **feeder** sub-box (top, labeled `feeder`)
- A **Queue** symbol or arrow between feeder and workers
- N **worker** sub-boxes fanned out below (labeled `worker-0` … `worker-3`),
  each drawing its own arrow to its respective Kafka partition

The feeder and workers should all fit inside a larger container box labeled
`listener_mp.py`. The color stays `C.blue`.

This is an SVG layout change inside `drawNodes()` in the mount effect.
The current single-box coordinates are approximately:
```
box(190, 303, 120, 64, ...)   // old single listener box
txt(250, 324, "listener_mp.py", ...)
txt(250, 344, "decode + filter", ...)
```
These need to be replaced with the feeder + worker fan layout.

The overall SVG `viewBox` may need to widen slightly (currently `0 0 1120 590`)
to accommodate the expanded listener sub-group, or the x-positions of
downstream elements (Kafka, Spark) may need shifting right.

---

## 3. New page to build: `/demo`

### Purpose

A single scrollable page for live presentation of all 6 distributed system
characteristics. The audience follows as you scroll. Each characteristic is
one section with: a one-line explanation, a visual or animation, and a
"Run demo" / "Show evidence" interaction.

Read `DISTRIBUTED_SYSTEM_ANALYSIS.md` for the full evidence behind each
characteristic. What follows here is the UI design spec.

### Page structure

Route: `/demo` — add to `App.tsx` and `DashboardLayout.tsx` nav
(icon suggestion: `Presentation` or `LayoutDashboard` from lucide-react).

The page is a **vertical scroll** with **6 sections**, one per characteristic.
A sticky sub-nav at the top of the page (below the main NavBar) has anchor
links: `Concurrency | Message Passing | Fault Tolerance | Scalability |
Location Transparency | No SPOF`. Clicking an anchor smooth-scrolls to
that section.

Each section follows this card template:
```
┌─────────────────────────────────────────────────────────────┐
│  [Number]  Characteristic Name                    [badge]   │
│  One-line definition in plain English                        │
│  ─────────────────────────────────────────────────────────  │
│  [Visual panel — chart / diagram / code / animation]        │
│  ─────────────────────────────────────────────────────────  │
│  Evidence strip: key numbers or proof in monospace          │
│  [Action button]                                            │
└─────────────────────────────────────────────────────────────┘
```

### Section-by-section spec

#### §1 — Concurrency

**One-liner:** "Multiple computations happen at the same time — not one after another."

**Visual:** Reuse / embed the pipeline SVG from `PipelineSimulatorPage.tsx`
(or a static version of it). The feeder → worker fan layout (Bug B fix above)
makes concurrency visually obvious at the ingestion layer. The Spark master →
4 workers also shows concurrency at the processing layer.

**Evidence strip:**
```
Ingestion:  4 worker processes (PIDs: distinct)  |  sum(loads) == rows
Processing: 4 Spark workers  |  1 partition per worker  |  parallel micro-batches
865-row benchmark:  workers=1 → 637 det/s  |  workers=2 → 1519 det/s (2.39×)
```

**Action:** "Run benchmark" button — plays the benchmark table animation
(the 60-tx / 865-tx toggle already in PipelineSimulatorPage can be
embedded or linked here).

---

#### §2 — Message Passing

**One-liner:** "Every stage communicates through a channel — no component reads another's memory."

**Visual:** A linear "pipeline trace" animation. 5 stage boxes in a horizontal
row (or left-to-right with slight vertical offset for readability):

```
[Listener] ──kafka──► [Spark] ──mongo write──► [FastAPI] ──SSE──► [React]
```

A single glowing dot (use `C.gold`, same as the transaction packets) travels
from left to right, pausing at each box with a label showing what the message
looks like at that stage:

- At Listener: `{ tx_hash: "0xabc...", input: "0xab9c4b5d...", ... }`
- At Kafka: `topic=raw_txns  partition=2  offset=4821`
- At Spark: `decoded: flashLoanSimple  amount_usd=$1.2M  confidence=HIGH`
- At FastAPI: `event: detections  data: [{ tx_hash, confidence, ... }]`
- At React: card shown in the live table

This is client-side animation only. No real data needed.

**Evidence strip:**
```
Channels: Kafka topic raw_txns | MongoDB collection transactions | SSE /stream/detections
No component calls another directly — all communication is through durable channels
```

**Action:** "Trace a transaction" button — replays the dot animation from the beginning.

---

#### §3 — Fault Tolerance

**One-liner:** "When a component fails, the system recovers automatically with no data lost."

**Visual:** Two columns side by side:

```
┌──────────────────────┐   ┌──────────────────────┐
│   Single Process     │   │   Distributed Spark   │
│                      │   │                       │
│  Docs written:  0    │   │  Docs written:  35    │
│  Status: ❌ CRASHED │   │  Status: ✅ RECOVERED │
│  Recovery: manual    │   │  Recovery: automatic  │
└──────────────────────┘   └──────────────────────┘
```

The numbers animate from 0 upward, then the single-process side crashes
(red flash, counter resets to 0), while the Spark side keeps counting.

Below the two columns, show a timeline strip:

```
t=0s   [Kafka broker 2 killed]
t=8s   [Leader election complete — ISR 3→2]
t=12s  [Spark worker 2 killed]
t=18s  [Task reassigned from checkpoint]
t=24s  [Processing resumed — no gap in MongoDB]
```

This is a scripted animation using the actual measured timings from
`DISTRIBUTED_SYSTEM_ANALYSIS.md §4`.

**Evidence strip:**
```
Kafka:  replication_factor=3  |  acks="all"  |  enable.idempotence=True
Spark:  checkpoint at /tmp/spark-checkpoints/  |  resume from last Kafka offset
Result: MongoDB count monotonically increasing through all kills
```

**Action:** "Run fault tolerance demo" button — triggers the scripted timeline animation.
(In a real live demo, the presenter runs `fault_tolerance_demo.py` in the
terminal separately; this button just plays the visualization.)

---

#### §4 — Scalability

**One-liner:** "Adding more workers increases throughput — until the partition ceiling."

**Visual:** A bar chart (use recharts `BarChart`) with:
- X axis: worker count (1, 2, 4, 8)
- Y axis: throughput in detections/sec
- Two series: 60-row dataset (shows degradation) and 865-row dataset (shows speedup)
- A vertical dashed line at `workers=4` labeled "partition ceiling"

Below the chart, a short explanation callout:
> "Gains taper past 2 workers — same spawn overhead, less work per worker.
> At the Spark layer, the ceiling is exactly 4 workers (= 4 Kafka partitions)."

**Evidence strip (actual measured numbers):**
```
865-row offline sweep:
  workers=1  →   637 det/s  (1.00×)
  workers=2  →  1519 det/s  (2.39×)  ← best
  workers=4  →  1440 det/s  (2.26×)
  workers=8  →  1095 det/s  (1.72×)

Spark layer:
  1 worker   →  ~0.85 tx/s
  2 workers  →  ~1.66 tx/s
  4 workers  →  ~2.91 tx/s
  5+ workers →  ~2.91 tx/s  ← plateau (partition-bound)
```

**Action:** A toggle between "Ingestion layer" and "Spark layer" switches
the chart dataset.

---

#### §5 — Location Transparency

**One-liner:** "Components talk to each other by name — not by IP address."

**Visual:** A simple code-block panel showing the key config lines:

```python
# broker/kafka_producer.py
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                             "kafka-1:9092,kafka-2:9092,kafka-3:9092")

# processing/streaming_job.py
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

# backend/Main.py
MONGO_URI = os.getenv("MONGODB_URI")   # atlas DNS name, never an IP
```

Below that, a short proof statement:

```bash
# grep for hardcoded IPs in all Python source:
grep -r "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}" --include="*.py" .
# Result: zero matches
```

**Evidence strip:**
```
Docker bridge network: broker_net (embedded DNS resolves container names)
MongoDB Atlas: cloud endpoint — physical host completely transparent
Result: zero hardcoded IPs anywhere in application source code
```

**Action:** Not applicable — this section is explanation-only. Keep it short.

---

#### §6 — No Single Point of Failure

**One-liner:** "No individual component's failure stops the pipeline."

**Visual:** A "kill map" — the pipeline diagram with 4 interactive kill
buttons, one per vulnerable component:

```
[Kill Kafka broker]   [Kill Spark worker]   [Kill Redis]   [Kill FastAPI]
```

Each button, when clicked, shows a tooltip card:

```
Killed: Kafka broker-2
What happens: ISR drops 3→2, leader election starts
Recovery time: ~10 seconds
Data lost: 0 (replication_factor=3, acks="all")
System status: ✅ PIPELINE CONTINUES
```

The pipeline diagram (static, not the full simulator) flashes the killed
component red, then shows it recovering with a green pulse.

**Evidence strip:**
```
Component         Tolerance          Recovery mechanism
─────────────     ──────────         ──────────────────────────────
Kafka broker      lose 1 of 3        ZooKeeper leader election
Spark worker      lose 1 of 4        checkpoint offset replay
Redis             full loss           static price fallback in price_udf
FastAPI           full loss           EventSource auto-reconnect (browser)
```

**Action:** The kill buttons play the scripted animation described above.
The "grand finale" button kills Kafka + Spark simultaneously and shows
the pipeline surviving both.

---

### Implementation notes for the new chat

- This is a **client-side only** page. Nothing calls real Docker containers.
  All fault-tolerance "demos" are scripted animations using real measured data.
- The recharts `BarChart` is already a dependency (`recharts ^3.8.1`).
- The pipeline SVG from `PipelineSimulatorPage.tsx` can be adapted as a
  static (non-animated) version for §1 and §6.
- Use the same dark palette as the rest of the app:
  `--bg:#0f1419`, `--panel:#161d27`, gold `#ffd24a`, blue `#4a9eff`,
  green `#5fd07a`, red `#ff5d5d`, amber `#f0a93b`.
- Strict TypeScript rules apply — `noUnusedLocals`, `noUnusedParameters`,
  `verbatimModuleSyntax`, `erasableSyntaxOnly`. Import types separately.
- lucide-react version is `^1.8.0` (unusual) — verify icon names exist
  before using them. Safe confirmed icons: `Play`, `Pause`, `RotateCcw`,
  `Zap`, `Database`, `Activity`, `Network`, `Play`, `GitBranch`, `Cpu`,
  `Terminal`, `ShieldAlert`.

---

## 4. Work order for the new session

**Step 1 (quick fixes, ~15 min):**
Fix Bug A (English event log strings) and Bug B (listener architecture SVG)
in `PipelineSimulatorPage.tsx`.

**Step 2 (medium, ~45 min):**
Build `src/pages/DemoPage.tsx` with all 6 sections as described in §3 above.
Add the `/demo` route to `App.tsx` and a nav link to `DashboardLayout.tsx`.

**Step 3 (if time allows):**
Run `npm run build` to catch any TypeScript errors introduced by the new page.
The user will run this locally since the filesystem MCP can't execute commands.

---

## 5. Files the new chat should read before coding

In order:

1. `CLAUDE.md` — full project state and file map
2. `DISTRIBUTED_SYSTEM_ANALYSIS.md` — characteristics, evidence, and demo commands
3. `FRONTEND_NEXT_SESSION.md` — this file
4. `frontend/src/pages/PipelineSimulatorPage.tsx` — to understand the SVG drawing
   system before modifying it (Bug B fix and §1 visual)
5. `frontend/src/components/layout/DashboardLayout.tsx` — to add the `/demo` nav link
6. `frontend/src/App.tsx` — to add the `/demo` route
