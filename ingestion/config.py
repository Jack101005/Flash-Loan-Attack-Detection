# config.py
# Flash-Loan Attack Detection — Ingestion Configuration
#
# WATCHLIST: contract addresses we monitor (to field must match)
# SELECTORS: 4-byte function selectors that indicate flash-loan calls

WATCHLIST = {
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3",
    "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8": "Uniswap V3",
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer V2",
}

SELECTORS = {
    # Aave V3 Pool
    "0xab9c4b5d": "Aave V3 flashLoan",
    "0x42b0b77c": "Aave V3 flashLoanSimple",
    # Balancer V2 Vault
    "0x5c38449e": "Balancer V2 flashLoan",
    # Uniswap V3 — flash() function
    "0x490e6cbc": "Uniswap V3 flash",
}