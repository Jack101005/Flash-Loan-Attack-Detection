import { useState } from 'react';
import { decodeTransaction } from '@/services/decoderService';
import type { DecodeResponse } from '@/services/decoderService';

export function useDecoder() {
  const [result, setResult] = useState<DecodeResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const decode = async (txHash: string) => {
    if (!txHash) return;
    
    setLoading(true);
    setError('');
    setResult(null);
    
    try {
      const data = await decodeTransaction(txHash);
      setResult(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to connect to backend API');
    } finally {
      setLoading(false);
    }
  };

  return { decode, result, loading, error };
}
