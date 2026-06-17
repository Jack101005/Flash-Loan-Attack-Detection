// frontend/src/pages/HomePage.tsx
import { useState, useEffect } from "react";
import { KpiCards } from "@/components/dashboard/KpiCards";
import { LiveDetectionsTable } from "@/components/dashboard/LiveDetectionsTable";
import { TransactionGraph } from "@/components/dashboard/TransactionGraph";
import type { Detection } from "@/components/dashboard/LiveDetectionsTable";
import { SSE_DETECTIONS_URL } from "@/lib/config";

export default function HomePage() {
  const [detections, setDetections] = useState<Detection[]>([]);
  const [selectedTx, setSelectedTx] = useState<Detection | null>(null);

  useEffect(() => {
    const eventSource = new EventSource(SSE_DETECTIONS_URL);

    eventSource.addEventListener("detections", (event) => {
      const data: Detection[] = JSON.parse(event.data);
      setDetections(data);
    });

    eventSource.onerror = () => {
      console.warn("SSE connection lost, auto-reconnecting...");
    };

    return () => eventSource.close();
  }, []);

  // Auto-select first transaction when detections update
  useEffect(() => {
    if (detections.length > 0 && !selectedTx) {
      setSelectedTx(detections[0]);
    }
  }, [detections, selectedTx]);

  const kpiData = {
    activeAlerts: detections.length,
    criticalAlerts: detections.filter((d) => d.confidence === "HIGH").length,
    mediumAlerts: detections.filter((d) => d.confidence === "MEDIUM").length,
    maxBorrowed: Math.max(...detections.map((d) => d.amount_usd), 0),
  };

  return (
    <>
      <KpiCards data={kpiData} />

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 mt-6">
        <div className="col-span-1 lg:col-span-3 relative">
          <LiveDetectionsTable detections={detections} onSelect={setSelectedTx} />
        </div>
        <div className="col-span-1 lg:col-span-2">
          <TransactionGraph transaction={selectedTx} />
        </div>
      </div>
    </>
  );
}
