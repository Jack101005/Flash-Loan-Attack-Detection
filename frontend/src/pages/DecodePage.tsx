import { useDecoder } from '@/hooks/useDecoder';
import { DecodeForm } from '@/components/decode/DecodeForm';
import { DecodeResult } from '@/components/decode/DecodeResult';

export default function DecodePage() {
  const { decode, result, loading, error } = useDecoder();

  return (
    <div className="max-w-2xl mx-auto p-8 space-y-8 min-h-screen">
      <div className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Transaction Detail</h1>
        <p className="text-zinc-500">Enter a transaction hash to look up detection results.</p>
      </div>

      <DecodeForm onDecode={decode} loading={loading} />

      <DecodeResult result={result} error={error} />
    </div>
  );
}
