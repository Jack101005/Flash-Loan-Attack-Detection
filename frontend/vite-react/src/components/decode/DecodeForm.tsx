import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface DecodeFormProps {
  onDecode: (txHash: string) => void;
  loading: boolean;
}

export function DecodeForm({ onDecode, loading }: DecodeFormProps) {
  const [txHash, setTxHash] = useState('');

  return (
    <div className="flex gap-4">
      <Input 
        value={txHash}
        onChange={(e) => setTxHash(e.target.value)}
        placeholder="Enter 0x..."
        className="flex-1"
      />
      <Button onClick={() => onDecode(txHash)} disabled={loading || !txHash}>
        {loading ? 'Sending...' : 'Send Request'}
      </Button>
    </div>
  );
}
