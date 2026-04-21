"""
decoder.py — Flash Loan Transaction Decoder (Jack's P3 - Step 1)

Reads detected_flash_loans.csv from Ngan's ingestion layer,
decodes each transaction, and outputs a decoded CSV matching
the verification_expected.csv format.

Usage:
    python3 decoder.py
    python3 decoder.py --input ../ingestion/data/detected_flash_loans.csv
"""

import csv
import json
import argparse

# ──────────────────────────────────────────────────────────────
# Token symbol lookup (contract address → symbol)
# ──────────────────────────────────────────────────────────────
TOKEN_SYMBOLS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": "USDe",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
}

# Token decimals for human-readable amounts
TOKEN_DECIMALS = {
    "USDC": 6,
    "USDT": 6,
    "WETH": 18,
    "WBTC": 8,
    "USDe": 18,
    "DAI":  18,
}

CONTRACT_NAMES = {
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3 Pool",
    "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8": "Uniswap V3 USDC/WETH Pool",
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer V2 Vault",
}


# ──────────────────────────────────────────────────────────────
# Manual ABI decoder (same fix as listener.py)
# ──────────────────────────────────────────────────────────────
def manual_decode_flash_loan(input_hex: str) -> dict:
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        if len(chunks) < 6:
            return None

        receiver    = "0x" + chunks[0][24:]
        on_behalf   = "0x" + chunks[4][24:]
        assets_ptr  = int(chunks[1], 16) // 32
        assets_len  = int(chunks[assets_ptr], 16)
        assets = [
            "0x" + chunks[assets_ptr + 1 + i][24:]
            for i in range(assets_len)
            if assets_ptr + 1 + i < len(chunks)
        ]
        amounts_ptr = int(chunks[2], 16) // 32
        amounts_len = int(chunks[amounts_ptr], 16)
        amounts = [
            str(int(chunks[amounts_ptr + 1 + i], 16))
            for i in range(amounts_len)
            if amounts_ptr + 1 + i < len(chunks)
        ]
        modes_ptr = int(chunks[3], 16) // 32
        modes_len = int(chunks[modes_ptr], 16)
        modes = [
            int(chunks[modes_ptr + 1 + i], 16)
            for i in range(modes_len)
            if modes_ptr + 1 + i < len(chunks)
        ]
        return {"assets": assets, "amounts": amounts, "modes": modes}
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# Helper: format amount to human readable
# ──────────────────────────────────────────────────────────────
def to_human(amount_raw: str, symbol: str) -> str:
    try:
        decimals = TOKEN_DECIMALS.get(symbol, 18)
        val = int(amount_raw) / (10 ** decimals)
        if val >= 1000:
            return f"{val:,.2f}"
        else:
            return f"{val:.4f}"
    except Exception:
        return "0.0000"


def get_symbol(asset_address: str) -> str:
    return TOKEN_SYMBOLS.get(asset_address.lower(), "UNKNOWN")


def check_suspicious(amounts: list, symbol: str) -> str:
    try:
        decimals = TOKEN_DECIMALS.get(symbol, 18)
        for a in amounts:
            val = int(a) / (10 ** decimals)
            if val > 1_000_000_000:
                return "SUSPICIOUS_AMOUNT"
        return "OK"
    except Exception:
        return "OK"


# ──────────────────────────────────────────────────────────────
# Main decoder
# ──────────────────────────────────────────────────────────────
def decode_row(row: dict) -> dict:
    tx_hash     = row.get("tx_hash", "")
    from_addr   = row.get("from", "").lower()
    to_addr     = row.get("to", "").lower()
    protocol    = row.get("protocol", "")
    selector    = row.get("selector", "")
    input_hex   = row.get("input", "")
    decode_ok   = row.get("decode_success", "false") == "true"

    contract_name = CONTRACT_NAMES.get(to_addr, "Unknown Contract")

    # Try using Ngan's already-decoded data first
    if decode_ok and row.get("decoded_assets"):
        try:
            assets  = json.loads(row["decoded_assets"])
            amounts = json.loads(row["decoded_amounts"])
            amounts = [str(a) for a in amounts]
            fn_name = row.get("function_name", "")
        except Exception:
            assets, amounts, fn_name = [], [], ""
    else:
        # Fallback: manual decode from input calldata
        if selector == "0xab9c4b5d":  # flashLoan
            result = manual_decode_flash_loan(input_hex)
            if result:
                assets  = result["assets"]
                amounts = result["amounts"]
                fn_name = "flashLoan"
            else:
                return None  # skip if can't decode
        elif selector == "0x42b0b77c":  # flashLoanSimple
            try:
                raw = input_hex[10:]
                chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
                asset  = "0x" + chunks[1][24:]
                amount = str(int(chunks[2], 16))
                assets  = [asset]
                amounts = [amount]
                fn_name = "flashLoanSimple"
            except Exception:
                return None
        elif selector == "0x490e6cbc":  # Uniswap flash
            try:
                raw = input_hex[10:]
                chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
                amount0 = str(int(chunks[1], 16))
                amount1 = str(int(chunks[2], 16))
                assets  = ["token0", "token1"]
                amounts = [amount0, amount1]
                fn_name = "flash"
            except Exception:
                return None
        else:
            return None

    if not assets or not amounts:
        return None

    # Build symbols and human amounts
    symbols = [get_symbol(a) if not a.startswith("token") else "UNKNOWN" for a in assets]
    amounts_human = [to_human(a, s) for a, s in zip(amounts, symbols)]

    primary_asset  = assets[0]
    primary_symbol = symbols[0]
    primary_amount = amounts[0]
    primary_human  = amounts_human[0]

    is_multi = len(assets) > 1
    quality  = check_suspicious(amounts, primary_symbol)

    return {
        "tx_hash":               tx_hash,
        "from_address":          from_addr,
        "to_address":            to_addr,
        "contract_name":         contract_name,
        "protocol":              protocol,
        "function_name":         fn_name,
        "selector":              selector,
        "assets_json":           json.dumps(assets),
        "amounts_raw_json":      json.dumps(amounts),
        "amounts_human_json":    json.dumps(amounts_human),
        "token_symbols_json":    json.dumps(symbols),
        "primary_asset":         primary_asset,
        "primary_symbol":        primary_symbol,
        "primary_amount_raw":    primary_amount,
        "primary_amount_human":  primary_human,
        "total_usd_borrow_estimate": "",
        "is_multi_asset":        str(is_multi).lower(),
        "data_quality":          quality,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="../ingestion/data/detected_flash_loans.csv")
    parser.add_argument("--output", default="decoded_output.csv")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Flash Loan Decoder — P3 Step 1")
    print(f"{'='*55}\n")

    with open(args.input, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"[decoder] Read {len(rows)} rows from {args.input}")

    decoded = []
    skipped = 0
    for row in rows:
        result = decode_row(row)
        if result:
            decoded.append(result)
        else:
            skipped += 1

    print(f"[decoder] Decoded: {len(decoded)} | Skipped: {skipped}")

    if not decoded:
        print("[decoder] No rows decoded. Check input file.")
        return

    fieldnames = list(decoded[0].keys())
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(decoded)

    print(f"[decoder] Output saved to: {args.output}")
    print(f"\n{'='*55}\n")


if __name__ == "__main__":
    main()