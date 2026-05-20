interface DecodeResultProps {
  result: any;
  error: string;
}

export function DecodeResult({ result, error }: DecodeResultProps) {
  if (error) {
    return (
      <div className="p-4 bg-red-50 text-red-700 border border-red-200 rounded-md">
        <strong>Error:</strong> {error}
      </div>
    );
  }

  if (!result) return null;

  return (
    <div className="p-6 border rounded-lg bg-zinc-50 dark:bg-zinc-900 shadow-sm space-y-4">
      <h2 className="text-xl font-semibold">API Response</h2>
      
      <div className="grid gap-2 text-sm">
        <div className="flex flex-col">
          <span className="font-medium text-zinc-500 uppercase text-xs">Transaction Hash</span>
          <span className="break-all font-mono mt-1">{result.tx_hash}</span>
        </div>
        
        <div className="flex flex-col mt-2">
          <span className="font-medium text-zinc-500 uppercase text-xs">Is Flash Loan</span>
          <div className="mt-1">
            <span className={`inline-flex px-2 py-1 rounded text-xs font-semibold ${result.is_flash_loan ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'}`}>
              {result.is_flash_loan ? 'True' : 'False'}
            </span>
          </div>
        </div>
        
        <div className="flex flex-col mt-2">
          <span className="font-medium text-zinc-500 uppercase text-xs">Risk Level</span>
          <span className={`font-semibold mt-1 ${result.risk_level === 'HIGH' ? 'text-red-600' : ''}`}>
            {result.risk_level}
          </span>
        </div>
        
        <div className="flex flex-col mt-2">
          <span className="font-medium text-zinc-500 uppercase text-xs">Summary</span>
          <div className="p-3 mt-1 bg-white border rounded">
            {result.summary}
          </div>
        </div>
      </div>
    </div>
  );
}
