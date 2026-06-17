// frontend/src/hooks/useSimulation.ts
// Drives the Transaction Flow simulator: play / pause / step / reset / auto-advance.
// Pure client-side — no backend required (demo mode safe).
import { useCallback, useEffect, useRef, useState } from "react";
import type { StageStatus } from "@/lib/pipeline";
import { PIPELINE_STAGES } from "@/lib/pipeline";
import type { SampleTx } from "@/lib/sampleTransactions";
import { lastReachableIndex } from "@/lib/sampleTransactions";

// Map a real per-stage latency (ms) to a watchable on-screen dwell (ms).
function dwellFor(latencyMs: number, speed: number): number {
  const base = Math.min(1500, Math.max(350, 300 + latencyMs * 3));
  return base / speed;
}

export interface SimulationState {
  progress: number; // index of last completed stage (-1 = not started)
  active: number; // index currently "processing" (-1 = none)
  running: boolean;
  done: boolean;
  lastIndex: number;
  statusFor: (stageId: string, index: number) => StageStatus;
  play: () => void;
  pause: () => void;
  reset: () => void;
  step: () => void;
  setSpeed: (s: number) => void;
  speed: number;
}

interface PlaybackState {
  sampleId: string;
  progress: number;
  running: boolean;
}

type ProgressUpdate = number | ((progress: number) => number);

export function useSimulation(sample: SampleTx): SimulationState {
  const lastIndex = lastReachableIndex(sample);
  const [playback, setPlayback] = useState<PlaybackState>({
    sampleId: sample.id,
    progress: -1,
    running: false,
  });
  const [speed, setSpeed] = useState(1);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearTimer = useCallback(() => {
    if (timer.current) {
      clearTimeout(timer.current);
      timer.current = null;
    }
  }, []);

  const current =
    playback.sampleId === sample.id
      ? playback
      : { sampleId: sample.id, progress: -1, running: false };

  const progress = current.progress;
  const done = progress >= lastIndex;
  const running = current.running && !done;
  const active = running && !done ? progress + 1 : -1;

  const updatePlayback = useCallback(
    (patch: Partial<Omit<PlaybackState, "sampleId">>) => {
      setPlayback((prev) => {
        const base =
          prev.sampleId === sample.id
            ? prev
            : { sampleId: sample.id, progress: -1, running: false };
        return { ...base, ...patch };
      });
    },
    [sample.id],
  );

  const updateProgress = useCallback(
    (next: ProgressUpdate) => {
      setPlayback((prev) => {
        const base =
          prev.sampleId === sample.id
            ? prev
            : { sampleId: sample.id, progress: -1, running: false };
        const progressValue =
          typeof next === "function" ? next(base.progress) : next;
        return { ...base, progress: progressValue };
      });
    },
    [sample.id],
  );

  // Auto-advance loop.
  useEffect(() => {
    if (!running || done) {
      clearTimer();
      return;
    }
    const nextIdx = progress + 1;
    const nextStage = PIPELINE_STAGES[nextIdx];
    const outcome = sample.outcomes[nextStage.id];
    const dwell = dwellFor(outcome?.latencyMs ?? 50, speed);
    timer.current = setTimeout(() => {
      updateProgress((p) => p + 1);
    }, dwell);
    return clearTimer;
  }, [running, progress, done, speed, sample, clearTimer, updateProgress]);

  const play = useCallback(() => {
    updatePlayback({
      progress: progress >= lastIndex ? -1 : progress,
      running: true,
    });
  }, [progress, lastIndex, updatePlayback]);

  const pause = useCallback(() => updatePlayback({ running: false }), [updatePlayback]);

  const reset = useCallback(() => {
    clearTimer();
    updatePlayback({ running: false, progress: -1 });
  }, [clearTimer, updatePlayback]);

  const step = useCallback(() => {
    updatePlayback({ running: false });
    updateProgress((p) => Math.min(p + 1, lastIndex));
  }, [lastIndex, updatePlayback, updateProgress]);

  const statusFor = useCallback(
    (stageId: string, index: number): StageStatus => {
      if (index <= progress) {
        // resolved — use the scripted outcome status
        return sample.outcomes[stageId]?.status ?? "success";
      }
      if (index === active) return "processing";
      return "waiting";
    },
    [progress, active, sample],
  );

  return {
    progress,
    active,
    running,
    done,
    lastIndex,
    statusFor,
    play,
    pause,
    reset,
    step,
    setSpeed,
    speed,
  };
}
