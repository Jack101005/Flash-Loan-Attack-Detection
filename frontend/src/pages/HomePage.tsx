import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/layout/DashboardLayout";
import { KpiCards } from "@/components/dashboard/KpiCards";
import { LiveDetectionsTable } from "@/components/dashboard/LiveDetectionsTable";
import { TransactionGraph } from "@/components/dashboard/TransactionGraph";
import type { Detection } from "@/components/dashboard/LiveDetectionsTable";

// MOCK DATA GENERATOR
const generateMockData = (): Detection[] => [
  {
    tx_hash: "0xabc" + Math.random().toString(16).substring(2, 8) + "123",
    is_suspicious: true,
    confidence: "HIGH",
    cycle_path: ["USDT", "WETH", "AAVE", "USDT"],
    profit_estimate: 4100.5,
    price_deviation: 0.503,
    protocol: "aave_v3",
    timestamp: Date.now() / 1000 - 10,
  },
  {
    tx_hash: "0xdef" + Math.random().toString(16).substring(2, 8) + "456",
    is_suspicious: true,
    confidence: "MEDIUM",
    cycle_path: ["DAI", "USDC", "DAI"],
    profit_estimate: 120.0,
    price_deviation: 0.02,
    protocol: "uniswap_v3",
    timestamp: Date.now() / 1000 - 45,
  },
];

export default function HomePage() {
  const [detections, setDetections] = useState<Detection[]>(generateMockData());
  const [selectedTx, setSelectedTx] = useState<Detection | null>(detections[0]);

  // Simulate Polling
  useEffect(() => {
    const interval = setInterval(() => {
      setDetections((prev) => {
        // Randomly add a new detection 30% of the time
        if (Math.random() > 0.7) {
          const isHigh = Math.random() > 0.5;
          const newTx: Detection = {
            tx_hash: "0x" + Math.random().toString(16).substring(2, 40),
            is_suspicious: true,
            confidence: isHigh ? "HIGH" : "MEDIUM",
            cycle_path: isHigh ? ["ETH", "USDC", "YFI", "ETH"] : ["LINK", "WETH", "LINK"],
            profit_estimate: isHigh ? Math.random() * 5000 + 1000 : Math.random() * 500,
            price_deviation: Math.random() * 0.4,
            protocol: isHigh ? "dydx" : "sushiswap",
            timestamp: Date.now() / 1000,
          };
          return [newTx, ...prev].slice(0, 50); // Keep last 50
        }
        return prev;
      });
    }, 5000); // Poll every 5s

    return () => clearInterval(interval);
  }, []);

  const kpiData = {
    activeAlerts: detections.length,
    criticalAlerts: detections.filter(d => d.confidence === "HIGH").length,
    maxProfit: Math.max(...detections.map(d => d.profit_estimate), 0),
    avgDeviation: detections.reduce((acc, curr) => acc + curr.price_deviation, 0) / (detections.length || 1) * 100,
  };

  return (
    <DashboardLayout>
      <KpiCards data={kpiData} />
      
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 mt-6">
        <div className="col-span-1 lg:col-span-2 relative">
           <LiveDetectionsTable 
              detections={detections} 
              onSelect={setSelectedTx} 
           />
        </div>
        <div className="col-span-1 lg:col-span-3">
           <TransactionGraph transaction={selectedTx} />
        </div>
      </div>
    </DashboardLayout>
  );
}
