import { Terminal } from "lucide-react";

export interface Detection {
  tx_hash: string;
  is_suspicious: boolean;
  confidence: "HIGH" | "MEDIUM" | "LOW";
  cycle_path: string[];
  profit_estimate: number;
  price_deviation: number;
  protocol: string;
  timestamp: number;
}

export function LiveDetectionsTable({ detections, onSelect }: { detections: Detection[], onSelect: (d: Detection) => void }) {
  return (
    <div className="border border-border/50 bg-background/30 backdrop-blur-sm flex flex-col h-[500px]">
      <div className="border-b border-border/50 p-3 flex items-center justify-between bg-zinc-900/50">
        <div className="flex items-center gap-2">
          <Terminal className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-mono tracking-widest uppercase text-muted-foreground">Intercepted Transactions</h3>
        </div>
        <div className="flex items-center gap-2">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-neon-red opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-neon-red"></span>
          </span>
          <span className="text-xs font-mono text-muted-foreground">LIVE STREAM</span>
        </div>
      </div>
      
      <div className="flex-1 overflow-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs font-mono text-muted-foreground uppercase sticky top-0 bg-background border-b border-border/50 z-10">
            <tr>
              <th className="px-4 py-3 font-normal">Time</th>
              <th className="px-4 py-3 font-normal">Protocol</th>
              <th className="px-4 py-3 font-normal">Confidence</th>
              <th className="px-4 py-3 font-normal text-right">Est. Profit</th>
              <th className="px-4 py-3 font-normal">Tx Hash</th>
            </tr>
          </thead>
          <tbody className="font-mono text-sm">
            {detections.map((tx) => {
              const timeStr = new Date(tx.timestamp * 1000).toLocaleTimeString([], { hour12: false });
              const shortHash = `${tx.tx_hash.slice(0, 6)}...${tx.tx_hash.slice(-4)}`;
              
              const isHigh = tx.confidence === "HIGH";
              const isMedium = tx.confidence === "MEDIUM";
              
              return (
                <tr 
                  key={tx.tx_hash} 
                  onClick={() => onSelect(tx)}
                  className="border-b border-border/20 hover:bg-white/5 cursor-pointer transition-colors group"
                >
                  <td className="px-4 py-3 text-muted-foreground/70 group-hover:text-foreground">{timeStr}</td>
                  <td className="px-4 py-3 text-zinc-300">{tx.protocol.replace('_', ' ').toUpperCase()}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-1.5 py-0.5 text-xs font-medium border ${
                      isHigh ? 'border-neon-red/50 text-neon-red bg-neon-red/10' : 
                      isMedium ? 'border-yellow-500/50 text-yellow-500 bg-yellow-500/10' : 
                      'border-border text-muted-foreground bg-white/5'
                    }`}>
                      {tx.confidence}
                    </span>
                  </td>
                  <td className={`px-4 py-3 text-right ${isHigh ? 'text-acid-green' : 'text-foreground'}`}>
                    ${tx.profit_estimate.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </td>
                  <td className="px-4 py-3 text-muted-foreground group-hover:text-blue-400 font-mono text-xs">
                    {shortHash}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
