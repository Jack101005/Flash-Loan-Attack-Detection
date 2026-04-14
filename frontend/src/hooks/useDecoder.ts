import { useState } from 'react';
import { decodeTransaction } from '@/services/decoderService';

export function useDecoder() {
  const [result, setResult] = useState<any>(null);
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
    } catch (err: any) {
      setError(err.message || 'Failed to connect to backend API');
    } finally {
      setLoading(false);
    }
  };

  return { decode, result, loading, error };
}
