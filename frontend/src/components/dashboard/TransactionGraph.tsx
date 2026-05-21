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
    if (!transaction) return { nodes: [], edges: [] };

    const isHigh = transaction.confidence === "HIGH";
    const isMedium = transaction.confidence === "MEDIUM";
    const edgeColor = isHigh ? '#ef4444' : isMedium ? '#eab308' : '#52525b';
    const borderColor = isHigh ? '#ef4444' : isMedium ? '#eab308' : '#27272a';

    const baseStyle = {
      background: '#09090b',
      color: '#fff',
      borderRadius: '0px',
      padding: '10px 20px',
      fontSize: '12px',
      fontFamily: 'monospace',
      letterSpacing: '0.05em',
    };

    const senderLabel = transaction.from
      ? `${transaction.from.slice(0, 6)}...${transaction.from.slice(-4)}`
      : 'Sender';
    const poolLabel = transaction.protocol || 'Pool';

    const newNodes = [
      {
        id: 'sender',
        data: { label: senderLabel },
        position: { x: 80, y: 100 },
        style: { ...baseStyle, border: `1px solid ${borderColor}` },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      },
      {
        id: 'pool',
        data: { label: poolLabel },
        position: { x: 380, y: 100 },
        style: { ...baseStyle, border: `1px solid ${borderColor}`, boxShadow: isHigh ? '0 0 10px rgba(239,68,68,0.3)' : isMedium ? '0 0 10px rgba(234,179,8,0.3)' : 'none' },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      },
    ];

    const newEdges = [
      {
        id: 'e-sender-pool',
        source: 'sender',
        target: 'pool',
        animated: true,
        label: `${transaction.token ?? ''} Flash Loan`,
        style: { stroke: edgeColor, strokeWidth: 2 },
        labelStyle: { fill: '#a1a1aa', fontWeight: 700, fontSize: 10, fontFamily: 'monospace' },
        labelBgStyle: { fill: '#09090b', fillOpacity: 0.8 },
        markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor },
      },
    ];

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
            <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest mb-1">Amount Borrowed</p>
            <p className="font-mono text-sm text-acid-green">${transaction.amount_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}</p>
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
