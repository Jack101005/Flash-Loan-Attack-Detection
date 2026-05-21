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
    const fetchLiveDetections = async () => {
      try {
        const response = await fetch(`${API_URL}/live-detections`);
        if (!response.ok) throw new Error("Network response was not ok");

        const data: Detection[] = await response.json();
        setDetections(data);

        // Auto-select the first transaction if none is selected
        if (data.length > 0 && !selectedTx) {
          setSelectedTx(data[0]);
        }
      } catch (error) {
        console.error("Error fetching live detections:", error);
      }
    };

    // Initial fetch
    fetchLiveDetections();

    // Poll Redis via Backend every 5 seconds
    const interval = setInterval(fetchLiveDetections, 5000);

    return () => clearInterval(interval);
  }, [selectedTx]);

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