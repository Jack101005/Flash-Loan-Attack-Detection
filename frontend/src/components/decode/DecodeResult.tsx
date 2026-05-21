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

      <div className="grid gap-3 text-sm">
        <div className="flex flex-col">
          <span className="font-medium text-zinc-500 uppercase text-xs">Transaction Hash</span>
          <span className="break-all font-mono mt-1 text-xs">{result.tx_hash}</span>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="flex flex-col">
            <span className="font-medium text-zinc-500 uppercase text-xs">Is Flash Loan</span>
            <div className="mt-1">
              <span className={`inline-flex px-2 py-1 rounded text-xs font-semibold ${result.is_flash_loan ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'}`}>
                {result.is_flash_loan ? 'YES' : 'NO'}
              </span>
            </div>
          </div>

          <div className="flex flex-col">
            <span className="font-medium text-zinc-500 uppercase text-xs">Risk Level</span>
            <span className={`font-semibold mt-1 ${result.risk_level === 'HIGH' ? 'text-red-600' :
                result.risk_level === 'MEDIUM' ? 'text-yellow-600' :
                  result.risk_level === 'UNKNOWN' ? 'text-zinc-400' : 'text-green-600'
              }`}>
              {result.risk_level}
            </span>
          </div>
        </div>

        {result.is_flash_loan && (
          <>
            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Protocol</span>
                <span className="font-mono mt-1">{result.protocol ?? '—'}</span>
              </div>
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Token</span>
                <span className="font-mono mt-1">{result.token ?? '—'}</span>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Amount Borrowed</span>
                <span className="font-mono mt-1 text-green-700 font-semibold">
                  {result.amount_usd != null ? `$${result.amount_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—'}
                </span>
              </div>
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Total by Sender</span>
                <span className="font-mono mt-1 font-semibold">
                  {result.total_usd != null ? `$${result.total_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—'}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Confidence</span>
                <span className={`font-semibold mt-1 ${result.confidence === 'HIGH' ? 'text-red-600' :
                    result.confidence === 'MEDIUM' ? 'text-yellow-600' : 'text-zinc-500'
                  }`}>{result.confidence ?? '—'}</span>
              </div>
              <div className="flex flex-col">
                <span className="font-medium text-zinc-500 uppercase text-xs">Pools Used</span>
                <span className="font-mono mt-1">{result.pools_count ?? '—'}</span>
              </div>
            </div>

            <div className="flex flex-col">
              <span className="font-medium text-zinc-500 uppercase text-xs">Sender</span>
              <span className="font-mono mt-1 text-xs break-all">{result.from_address ?? '—'}</span>
            </div>
          </>
        )}

        <div className="flex flex-col">
          <span className="font-medium text-zinc-500 uppercase text-xs">Summary</span>
          <div className="p-3 mt-1 bg-white dark:bg-zinc-800 border rounded text-xs">
            {result.summary}
          </div>
        </div>
      </div>
    </div>
  );
}
