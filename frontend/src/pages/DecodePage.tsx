import { useDecoder } from '@/hooks/useDecoder';
import { DecodeForm } from '@/components/decode/DecodeForm';
import { DecodeResult } from '@/components/decode/DecodeResult';

export default function DecodePage() {
  const { decode, result, loading, error } = useDecoder();

  return (
    <div className="max-w-2xl mx-auto p-8 space-y-8 min-h-screen">
      <div className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Test /decode Endpoint</h1>
        <p className="text-zinc-500">Enter a transaction hash to send a POST request.</p>
      </div>
      
      <DecodeForm onDecode={decode} loading={loading} />
      
      <DecodeResult result={result} error={error} />
    </div>
  );
}
