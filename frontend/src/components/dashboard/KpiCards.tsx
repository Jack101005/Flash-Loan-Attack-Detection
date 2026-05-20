import { AlertTriangle, TrendingUp, Activity, DollarSign } from "lucide-react";

interface KpiData {
  activeAlerts: number;
  criticalAlerts: number;
  maxProfit: number;
  avgDeviation: number;
}

export function KpiCards({ data }: { data: KpiData }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {/* Card 1: Active Alerts */}
      <div className="border border-border/50 bg-background/50 p-4 relative overflow-hidden group hover:border-current transition-colors">
        <div className="absolute top-0 left-0 w-1 h-full bg-border group-hover:bg-current transition-colors" />
        <div className="flex justify-between items-start">
          <div className="space-y-1">
            <p className="text-xs font-mono text-muted-foreground tracking-wider uppercase">Active Alerts</p>
            <p className="text-3xl font-bold tracking-tighter">{data.activeAlerts}</p>
          </div>
          <Activity className="h-5 w-5 text-muted-foreground" />
        </div>
      </div>

      {/* Card 2: Critical Alerts */}
      <div className="border border-neon-red/30 bg-neon-red/5 p-4 relative overflow-hidden group hover:border-neon-red transition-colors">
        <div className="absolute top-0 left-0 w-1 h-full bg-neon-red shadow-[0_0_10px_rgba(239,68,68,0.8)]" />
        <div className="flex justify-between items-start">
          <div className="space-y-1">
            <p className="text-xs font-mono text-neon-red/70 tracking-wider uppercase">High Confidence</p>
            <p className="text-3xl font-bold tracking-tighter text-neon-red">{data.criticalAlerts}</p>
          </div>
          <AlertTriangle className="h-5 w-5 text-neon-red animate-pulse" />
        </div>
      </div>

      {/* Card 3: Max Profit */}
      <div className="border border-acid-green/30 bg-acid-green/5 p-4 relative overflow-hidden group hover:border-acid-green transition-colors">
        <div className="absolute top-0 left-0 w-1 h-full bg-acid-green shadow-[0_0_10px_rgba(132,204,22,0.8)]" />
        <div className="flex justify-between items-start">
          <div className="space-y-1">
            <p className="text-xs font-mono text-acid-green/70 tracking-wider uppercase">Max Profit Found</p>
            <p className="text-3xl font-bold tracking-tighter text-acid-green">
              ${data.maxProfit.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
            </p>
          </div>
          <DollarSign className="h-5 w-5 text-acid-green" />
        </div>
      </div>

      {/* Card 4: Avg Deviation */}
      <div className="border border-border/50 bg-background/50 p-4 relative overflow-hidden group hover:border-current transition-colors">
        <div className="absolute top-0 left-0 w-1 h-full bg-border group-hover:bg-current transition-colors" />
        <div className="flex justify-between items-start">
          <div className="space-y-1">
            <p className="text-xs font-mono text-muted-foreground tracking-wider uppercase">Avg Price Deviation</p>
            <p className="text-3xl font-bold tracking-tighter">{data.avgDeviation.toFixed(1)}%</p>
          </div>
          <TrendingUp className="h-5 w-5 text-muted-foreground" />
        </div>
      </div>
    </div>
  );
}
