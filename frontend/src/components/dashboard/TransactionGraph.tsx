import { useMemo } from 'react';
import { Network } from 'lucide-react';
import { 
  ReactFlow, 
  Background, 
  Controls,
  MarkerType,
  BackgroundVariant,
  Position
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import type { Detection } from './LiveDetectionsTable';

export function TransactionGraph({ transaction }: { transaction: Detection | null }) {
  const { nodes, edges } = useMemo(() => {
    if (!transaction || !transaction.cycle_path) return { nodes: [], edges: [] };

    const cycle = transaction.cycle_path;
    const isHigh = transaction.confidence === "HIGH";
    
    // Custom node styling
    const nodeStyle = {
      background: '#09090b',
      color: '#fff',
      border: `1px solid ${isHigh ? '#ef4444' : '#27272a'}`,
      borderRadius: '0px',
      padding: '10px 20px',
      fontSize: '12px',
      fontFamily: 'monospace',
      letterSpacing: '0.05em',
      boxShadow: isHigh ? '0 0 10px rgba(239, 68, 68, 0.2)' : 'none',
    };

    const newNodes = cycle.map((token, index) => ({
      id: `${index}-${token}`,
      data: { label: token },
      position: { x: 50 + index * 180, y: 100 },
      style: nodeStyle,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }));

    const newEdges = [];
    for (let i = 0; i < cycle.length - 1; i++) {
      newEdges.push({
        id: `e${i}-${i+1}`,
        source: `${i}-${cycle[i]}`,
        target: `${i+1}-${cycle[i+1]}`,
        animated: true,
        label: `Step ${i+1}`,
        style: { stroke: isHigh ? '#84cc16' : '#52525b', strokeWidth: 2 },
        labelStyle: { fill: '#a1a1aa', fontWeight: 700, fontSize: 10, fontFamily: 'monospace' },
        labelBgStyle: { fill: '#09090b', fillOpacity: 0.8 },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: isHigh ? '#84cc16' : '#52525b',
        },
      });
    }

    return { nodes: newNodes, edges: newEdges };
  }, [transaction]);

  if (!transaction) {
    return (
      <div className="border border-border/50 bg-background/30 h-[500px] flex items-center justify-center flex-col gap-4 text-muted-foreground">
        <Network className="h-8 w-8 opacity-20" />
        <p className="font-mono text-sm uppercase tracking-widest">Awaiting Transmission...</p>
      </div>
    );
  }

  return (
    <div className="border border-border/50 bg-background/30 backdrop-blur-sm h-[500px] flex flex-col relative overflow-hidden group">
      {/* Decorative corners */}
      <div className="absolute top-0 left-0 w-2 h-2 border-t border-l border-foreground opacity-50 z-10" />
      <div className="absolute top-0 right-0 w-2 h-2 border-t border-r border-foreground opacity-50 z-10" />
      <div className="absolute bottom-0 left-0 w-2 h-2 border-b border-l border-foreground opacity-50 z-10" />
      <div className="absolute bottom-0 right-0 w-2 h-2 border-b border-r border-foreground opacity-50 z-10" />

      <div className="border-b border-border/50 p-3 flex justify-between items-center bg-zinc-900/50 z-10">
        <h3 className="text-sm font-mono tracking-widest uppercase flex items-center gap-2">
          <Network className="h-4 w-4" />
          Topology Analysis
        </h3>
        <div className="font-mono text-xs text-muted-foreground">
          HASH: <span className="text-foreground">{transaction.tx_hash.slice(0, 10)}...</span>
        </div>
      </div>
      
      <div className="flex-1 w-full relative">
        <ReactFlow 
          nodes={nodes} 
          edges={edges}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          proOptions={{ hideAttribution: true }}
          minZoom={0.5}
          maxZoom={2}
        >
          <Background 
             variant={BackgroundVariant.Lines} 
             gap={20} 
             size={1} 
             color="#27272a" 
             style={{ opacity: 0.2 }}
          />
          <Controls 
            showInteractive={false} 
            className="bg-black border border-border/50 [&>button]:border-b [&>button]:border-border/50 [&>button]:bg-transparent hover:[&>button]:bg-white/10 [&>button>svg]:fill-foreground" 
          />
        </ReactFlow>
      </div>
      
      {/* Footer info panel */}
      <div className="absolute bottom-4 left-4 right-4 z-10 pointer-events-none">
         <div className="border border-border/50 bg-black/80 backdrop-blur p-4 grid grid-cols-3 gap-4 pointer-events-auto">
            <div>
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest mb-1">Protocol</p>
              <p className="font-mono text-sm">{transaction.protocol.toUpperCase()}</p>
            </div>
            <div>
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest mb-1">Price Dev</p>
              <p className="font-mono text-sm text-neon-red">{(transaction.price_deviation * 100).toFixed(2)}% WARN</p>
            </div>
            <div className="text-right">
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest mb-1">Target Action</p>
              <button className="border border-foreground/20 hover:border-foreground/80 px-4 py-1 text-xs font-mono transition-colors">
                VIEW RAW JSON
              </button>
            </div>
         </div>
      </div>
    </div>
  );
}
