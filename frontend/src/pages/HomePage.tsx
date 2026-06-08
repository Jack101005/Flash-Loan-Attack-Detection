// frontend/src/pages/HomePage.tsx
import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/layout/DashboardLayout";
import { KpiCards } from "@/components/dashboard/KpiCards";
import { LiveDetectionsTable } from "@/components/dashboard/LiveDetectionsTable";
import { TransactionGraph } from "@/components/dashboard/TransactionGraph";
import type { Detection } from "@/components/dashboard/LiveDetectionsTable";

// Pull the API URL from environment variables injected by Vite / Docker
const API_URL = "http://localhost:8000";

export default function HomePage() {
  const [detections, setDetections] = useState<Detection[]>([]);
  const [selectedTx, setSelectedTx] = useState<Detection | null>(null);

  useEffect(() => {
    const eventSource = new EventSource(`${API_URL}/stream/detections`);

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
  }, [detections]);

  const kpiData = {
    activeAlerts: detections.length,
    criticalAlerts: detections.filter(d => d.confidence === "HIGH").length,
    mediumAlerts: detections.filter(d => d.confidence === "MEDIUM").length,
    maxBorrowed: Math.max(...detections.map(d => d.amount_usd), 0),
  };

  return (
    <DashboardLayout>
      <KpiCards data={kpiData} />

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 mt-6">
        <div className="col-span-1 lg:col-span-3 relative">
          <LiveDetectionsTable
            detections={detections}
            onSelect={setSelectedTx}
          />
        </div>
        <div className="col-span-1 lg:col-span-2">
          <TransactionGraph transaction={selectedTx} />
        </div>
      </div>
    </DashboardLayout>
  );
}