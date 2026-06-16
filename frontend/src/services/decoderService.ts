import { API_URL } from "@/lib/config";

export interface DecodeResponse {
  tx_hash: string;
  is_flash_loan: boolean;
  risk_level: "HIGH" | "MEDIUM" | "LOW" | "UNKNOWN" | string;
  protocol?: string | null;
  token?: string | null;
  amount_usd?: number | null;
  total_usd?: number | null;
  confidence?: "HIGH" | "MEDIUM" | "LOW" | string | null;
  pools_count?: number | null;
  from_address?: string | null;
  summary: string;
}

export async function decodeTransaction(txHash: string): Promise<DecodeResponse> {
  const response = await fetch(`${API_URL}/decode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tx_hash: txHash })
  });

  if (!response.ok) {
    throw new Error(`Server error: ${response.status}`);
  }

  return (await response.json()) as DecodeResponse;
}
