const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export async function decodeTransaction(txHash: string) {
  const response = await fetch(`${API_URL}/decode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tx_hash: txHash })
  });

  if (!response.ok) {
    throw new Error(`Server error: ${response.status}`);
  }

  return response.json();
}
