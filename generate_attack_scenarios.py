"""
generate_attack_scenarios.py

Generates a CSV of simulated flash loan attack transactions that mirror
the patterns of famous real-world attacks (Euler, bZx, Cream, Harvest,
Beanstalk, etc.), but encoded as DIRECT calls to Aave V3 / Balancer V2 /
Uniswap V3 so they flow through the listener's two-pass filter.

Each row is structured exactly like a real Ethereum transaction:
  tx_hash, from, to, input (ABI-encoded), value, gas, gas_price, nonce

The CSV can be:
  - Loaded by mock_server.py (rename to test_data.csv) — listener detects all
  - Sent directly to Person 3 to test their graph/cycle/profit logic

Usage:
    python generate_attack_scenarios.py

Output:
    data/attack_scenarios/attack_simulation_data.csv      (15 attack scenarios)
    data/attack_scenarios/attack_scenarios_metadata.csv   (human-readable notes)
"""

import csv
import os
import secrets
from pathlib import Path

from web3 import Web3

PROJECT_ROOT = Path(__file__).parent
OUT_DIR = PROJECT_ROOT / "data" / "attack_scenarios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────
# Real addresses on Ethereum mainnet
# ──────────────────────────────────────────────────────────────
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
UNISWAP_V3   = "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"  # USDC/WETH pool
BALANCER_V2  = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"  # Vault

USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # 6 decimals
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # 6 decimals
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # 18 decimals
DAI  = "0x6B175474E89094C44Da98b954EedeAC495271d0F"  # 18 decimals
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"  # 8 decimals
LINK = "0x514910771AF9Ca656af840dff83E8264EcF986CA"  # 18 decimals
STETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84" # 18 decimals (Lido)

# Decimals reference
DECIMALS = {USDC: 6, USDT: 6, WETH: 18, DAI: 18, WBTC: 8, LINK: 18, STETH: 18}
SYMBOLS  = {USDC: "USDC", USDT: "USDT", WETH: "WETH", DAI: "DAI",
            WBTC: "WBTC", LINK: "LINK", STETH: "stETH"}

# ──────────────────────────────────────────────────────────────
# Minimal ABIs for encoding
# ──────────────────────────────────────────────────────────────
AAVE_ABI = [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "assets", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "interestRateModes", "type": "uint256[]"},
        {"name": "onBehalfOf", "type": "address"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"},
    ], "outputs": []},
    {"name": "flashLoanSimple", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "asset", "type": "address"},
        {"name": "amount", "type": "uint256"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"},
    ], "outputs": []},
]

BALANCER_ABI = [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "tokens", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "userData", "type": "bytes"},
    ], "outputs": []},
]

UNISWAP_ABI = [
    {"name": "flash", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "amount0", "type": "uint256"},
        {"name": "amount1", "type": "uint256"},
        {"name": "data", "type": "bytes"},
    ], "outputs": []},
]

w3 = Web3()
aave_c = w3.eth.contract(abi=AAVE_ABI)
bal_c  = w3.eth.contract(abi=BALANCER_ABI)
uni_c  = w3.eth.contract(abi=UNISWAP_ABI)

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def random_addr():
    return Web3.to_checksum_address("0x" + secrets.token_hex(20))


def random_tx_hash():
    return "0x" + secrets.token_hex(32)


def encode_aave_flash_loan(assets, amounts, attacker):
    return aave_c.encode_abi("flashLoan", [
        Web3.to_checksum_address(attacker),
        [Web3.to_checksum_address(a) for a in assets],
        amounts,
        [0] * len(assets),
        Web3.to_checksum_address(attacker),
        b"\x00" * 32,
        0,
    ])


def encode_aave_flash_loan_simple(asset, amount, attacker):
    return aave_c.encode_abi("flashLoanSimple", [
        Web3.to_checksum_address(attacker),
        Web3.to_checksum_address(asset),
        amount,
        b"\x00" * 32,
        0,
    ])


def encode_balancer_flash_loan(tokens, amounts, attacker):
    return bal_c.encode_abi("flashLoan", [
        Web3.to_checksum_address(attacker),
        [Web3.to_checksum_address(t) for t in tokens],
        amounts,
        b"\x00" * 32,
    ])


def encode_uniswap_flash(amount0, amount1, attacker):
    return uni_c.encode_abi("flash", [
        Web3.to_checksum_address(attacker),
        amount0,
        amount1,
        b"\x00" * 32,
    ])


# ──────────────────────────────────────────────────────────────
# Attack scenarios — inspired by real attacks
# ──────────────────────────────────────────────────────────────
def build_scenarios():
    scenarios = []

    # Scenario 1: Euler-like — Massive single-token DAI flash loan
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan([DAI], [30_000_000 * 10**18], attacker),
        "value": "0x0", "gas": hex(800_000), "gas_price": hex(40_000_000_000),
        "nonce": "0x1",
        "scenario": "Euler-style: 30M DAI flash loan",
        "expected_cycle": "DAI → eDAI mint (Euler) → self-borrow leverage → exploit → DAI",
        "expected_profit_usd": 8_900_000,
        "borrowed_value_usd": 30_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Euler Finance, March 2023, $197M",
    })

    # Scenario 2: bZx Pump-style — ETH flash loan, multi-DEX swap cycle
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(WETH, 10_000 * 10**18, attacker),
        "value": "0x0", "gas": hex(900_000), "gas_price": hex(45_000_000_000),
        "nonce": "0x2",
        "scenario": "bZx Pump-style: 10K WETH, Uniswap WBTC manipulation",
        "expected_cycle": "WETH → WBTC (Uniswap) → ETH (bZx oracle) → WETH",
        "expected_profit_usd": 350_000,
        "borrowed_value_usd": 24_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "bZx Pump Attack, Feb 14 2020, $350K",
    })

    # Scenario 3: bZx Oracle-style — sUSD price manipulation
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(WETH, 7_500 * 10**18, attacker),
        "value": "0x0", "gas": hex(750_000), "gas_price": hex(35_000_000_000),
        "nonce": "0x3",
        "scenario": "bZx Oracle-style: 7,500 WETH, sUSD pump",
        "expected_cycle": "WETH → sUSD (3 DEXes) → bZx oracle → WETH",
        "expected_profit_usd": 620_000,
        "borrowed_value_usd": 18_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "bZx Oracle Attack, Feb 18 2020, $620K",
    })

    # Scenario 4: Cream-style — Multi-asset (DAI + ETH) flash loan
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan(
            [DAI, WETH],
            [500_000_000 * 10**18, 50_000 * 10**18],
            attacker
        ),
        "value": "0x0", "gas": hex(1_200_000), "gas_price": hex(50_000_000_000),
        "nonce": "0x4",
        "scenario": "Cream-style: 500M DAI + 50K WETH, crYUSD mint exploit",
        "expected_cycle": "DAI+WETH → yDAI/yUSD → crYUSD doubling → drain",
        "expected_profit_usd": 130_000_000,
        "borrowed_value_usd": 620_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Cream Finance Hack, Oct 27 2021, $130M",
    })

    # Scenario 5: Harvest-style — Curve y-pool price manipulation
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": BALANCER_V2,
        "input": encode_balancer_flash_loan(
            [USDC, USDT],
            [50_000_000 * 10**6, 50_000_000 * 10**6],
            attacker
        ),
        "value": "0x0", "gas": hex(1_500_000), "gas_price": hex(60_000_000_000),
        "nonce": "0x5",
        "scenario": "Harvest-style: 50M USDC + 50M USDT via Balancer",
        "expected_cycle": "USDC+USDT → Curve y-pool manipulation → fUSDC arb → USDC",
        "expected_profit_usd": 33_800_000,
        "borrowed_value_usd": 100_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Harvest Finance, Oct 26 2020, $33.8M",
    })

    # Scenario 6: Beanstalk-style — Governance attack via massive flash loan
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan(
            [USDC, USDT, DAI],
            [350_000_000 * 10**6, 500_000_000 * 10**6, 150_000_000 * 10**18],
            attacker
        ),
        "value": "0x0", "gas": hex(2_000_000), "gas_price": hex(70_000_000_000),
        "nonce": "0x6",
        "scenario": "Beanstalk-style: $1B governance attack",
        "expected_cycle": "stablecoins → deposit to silo → vote → drain → withdraw",
        "expected_profit_usd": 80_000_000,
        "borrowed_value_usd": 1_000_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Beanstalk Farms, April 17 2022, $182M",
    })

    # Scenario 7: PancakeBunny-style — Price manipulation via repeated flash loans
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(WETH, 2_000 * 10**18, attacker),
        "value": "0x0", "gas": hex(600_000), "gas_price": hex(30_000_000_000),
        "nonce": "0x7",
        "scenario": "PancakeBunny-style: WETH for token price pump",
        "expected_cycle": "WETH → buy victim token → pump price → dump on oracle → WETH",
        "expected_profit_usd": 45_000_000,
        "borrowed_value_usd": 4_800_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "PancakeBunny, May 19 2021, $45M (BSC)",
    })

    # Scenario 8: Mango Markets-style — Oracle manipulation
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(USDC, 5_000_000 * 10**6, attacker),
        "value": "0x0", "gas": hex(700_000), "gas_price": hex(35_000_000_000),
        "nonce": "0x8",
        "scenario": "Mango-style: 5M USDC, perp price manipulation",
        "expected_cycle": "USDC → long perp → pump spot → withdraw against inflated collateral → USDC",
        "expected_profit_usd": 116_000_000,
        "borrowed_value_usd": 5_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Mango Markets, Oct 11 2022, $116M",
    })

    # Scenario 9: Saddle Finance-style — Slippage exploit
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan(
            [USDC, USDT],
            [10_000_000 * 10**6, 10_000_000 * 10**6],
            attacker
        ),
        "value": "0x0", "gas": hex(1_000_000), "gas_price": hex(45_000_000_000),
        "nonce": "0x9",
        "scenario": "Saddle-style: stablecoin pool slippage exploit",
        "expected_cycle": "USDC+USDT → drain stable pool via slippage → USDC",
        "expected_profit_usd": 11_000_000,
        "borrowed_value_usd": 20_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Saddle Finance, April 30 2022, $11M",
    })

    # Scenario 10: Alpha Homora-style — Iron Bank exploit
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan(
            [USDC, WETH],
            [1_000_000_000 * 10**6, 100_000 * 10**18],
            attacker
        ),
        "value": "0x0", "gas": hex(1_500_000), "gas_price": hex(55_000_000_000),
        "nonce": "0xa",
        "scenario": "Alpha Homora-style: cyUSD mint exploit",
        "expected_cycle": "USDC+WETH → cyUSD mint → counterfeit lending → drain",
        "expected_profit_usd": 37_500_000,
        "borrowed_value_usd": 1_300_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Alpha Homora, Feb 13 2021, $37.5M",
    })

    # Scenario 11: Small legitimate arbitrage — Person 3 should mark as LOW/MEDIUM
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(USDC, 100_000 * 10**6, attacker),
        "value": "0x0", "gas": hex(350_000), "gas_price": hex(20_000_000_000),
        "nonce": "0xb",
        "scenario": "Legitimate arb: 100K USDC cross-DEX",
        "expected_cycle": "USDC → WETH (Uniswap) → USDC (Sushiswap) — tiny profit",
        "expected_profit_usd": 250,
        "borrowed_value_usd": 100_000,
        "confidence_target": "LOW",
        "is_real_attack": "false",
        "inspired_by": "Routine arbitrage bot behavior",
    })

    # Scenario 12: Medium arb — Person 3 should mark as MEDIUM
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(WETH, 200 * 10**18, attacker),
        "value": "0x0", "gas": hex(400_000), "gas_price": hex(25_000_000_000),
        "nonce": "0xc",
        "scenario": "MEV arb: 200 WETH triangular",
        "expected_cycle": "WETH → USDC → WBTC → WETH",
        "expected_profit_usd": 1_200,
        "borrowed_value_usd": 480_000,
        "confidence_target": "MEDIUM",
        "is_real_attack": "false",
        "inspired_by": "MEV searcher activity",
    })

    # Scenario 13: Uniswap V3 flash — token0/token1 arbitrage
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": UNISWAP_V3,
        "input": encode_uniswap_flash(
            1_000_000 * 10**6,  # 1M USDC (token0)
            300 * 10**18,        # 300 WETH (token1)
            attacker
        ),
        "value": "0x0", "gas": hex(500_000), "gas_price": hex(30_000_000_000),
        "nonce": "0xd",
        "scenario": "Uniswap V3 flash: USDC/WETH dual-side",
        "expected_cycle": "USDC+WETH → external swap → repay both sides",
        "expected_profit_usd": 8_500,
        "borrowed_value_usd": 1_700_000,
        "confidence_target": "MEDIUM",
        "is_real_attack": "false",
        "inspired_by": "Uniswap V3 native flash pattern",
    })

    # Scenario 14: Balancer V2 — multi-token flash loan
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": BALANCER_V2,
        "input": encode_balancer_flash_loan(
            [WETH, WBTC, DAI],
            [5_000 * 10**18, 100 * 10**8, 10_000_000 * 10**18],
            attacker
        ),
        "value": "0x0", "gas": hex(2_000_000), "gas_price": hex(80_000_000_000),
        "nonce": "0xe",
        "scenario": "Balancer V2 triple-asset attack",
        "expected_cycle": "WETH+WBTC+DAI → complex 3-token arb → repay",
        "expected_profit_usd": 25_000_000,
        "borrowed_value_usd": 35_000_000,
        "confidence_target": "HIGH",
        "is_real_attack": "false",
        "inspired_by": "Balancer flash exploit class",
    })

    # Scenario 15: Massive single-token flash — should trigger HIGH alert on amount alone
    attacker = random_addr()
    scenarios.append({
        "tx_hash": random_tx_hash(),
        "from": attacker,
        "to": AAVE_V3_POOL,
        "input": encode_aave_flash_loan_simple(USDC, 100_000_000 * 10**6, attacker),
        "value": "0x0", "gas": hex(1_000_000), "gas_price": hex(50_000_000_000),
        "nonce": "0xf",
        "scenario": "Whale flash: 100M USDC",
        "expected_cycle": "Unknown — must trigger LOW alert based on size alone",
        "expected_profit_usd": 0,
        "borrowed_value_usd": 100_000_000,
        "confidence_target": "LOW",
        "is_real_attack": "false",
        "inspired_by": "Edge case: huge loan, no obvious arb",
    })

    return scenarios


# ──────────────────────────────────────────────────────────────
# Write output files
# ──────────────────────────────────────────────────────────────
def main():
    scenarios = build_scenarios()

    # File 1: Listener-compatible CSV (drop-in replacement for test_data.csv)
    sim_path = OUT_DIR / "attack_simulation_data.csv"
    with open(sim_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tx_hash", "from", "to", "input",
            "value", "gas", "gas_price", "nonce",
            "protocol_label",
        ])
        writer.writeheader()
        for s in scenarios:
            writer.writerow({
                "tx_hash": s["tx_hash"],
                "from": s["from"],
                "to": s["to"],
                "input": s["input"],
                "value": s["value"],
                "gas": s["gas"],
                "gas_price": s["gas_price"],
                "nonce": s["nonce"],
                "protocol_label": s["scenario"],
            })

    # File 2: Metadata for Person 3 — what to expect after detection
    meta_path = OUT_DIR / "attack_scenarios_metadata.csv"
    with open(meta_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tx_hash", "scenario",
            "expected_cycle", "expected_profit_usd", "borrowed_value_usd",
            "confidence_target", "inspired_by", "is_real_attack",
        ])
        writer.writeheader()
        for s in scenarios:
            writer.writerow({
                "tx_hash": s["tx_hash"],
                "scenario": s["scenario"],
                "expected_cycle": s["expected_cycle"],
                "expected_profit_usd": s["expected_profit_usd"],
                "borrowed_value_usd": s["borrowed_value_usd"],
                "confidence_target": s["confidence_target"],
                "inspired_by": s["inspired_by"],
                "is_real_attack": s["is_real_attack"],
            })

    print(f"\n[done] Generated {len(scenarios)} attack scenarios")
    print(f"  Listener input:    {sim_path}")
    print(f"  Person 3 metadata: {meta_path}")
    print()

    # Summary by confidence
    from collections import Counter
    by_conf = Counter(s["confidence_target"] for s in scenarios)
    print("  Confidence distribution (Person 3's algorithm should produce):")
    for conf in ["HIGH", "MEDIUM", "LOW"]:
        print(f"    {conf:8s}  {by_conf.get(conf, 0)} scenarios")

    print()
    print("  Total simulated economic value:")
    total_borrowed = sum(s["borrowed_value_usd"] for s in scenarios)
    total_profit = sum(s["expected_profit_usd"] for s in scenarios)
    print(f"    Borrowed:  ${total_borrowed:>16,.0f}")
    print(f"    Profit:    ${total_profit:>16,.0f}")


if __name__ == "__main__":
    main()
