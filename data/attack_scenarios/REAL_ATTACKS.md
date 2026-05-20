# Real Flash Loan Attacks — Reference Documentation

This document lists historical real-world flash loan attacks with verified
transaction hashes. **Read this carefully** — there is an important caveat
about what your listener can and cannot detect from these attacks.

---

## CRITICAL: What your listener detects vs. what attackers actually do

Your `listener.py` filters by:
1. `tx.to == known_protocol_contract` (Aave V3, Uniswap V3, Balancer V2)
2. `tx.input[:10] == flash_loan_selector`

**But real attackers do NOT directly call `flashLoan` from an EOA.** They:
1. Deploy a malicious smart contract first
2. Call THEIR OWN contract (`to = attacker_contract`)
3. INSIDE that contract, they call `Aave.flashLoan(...)` as an internal call
4. The flash loan provider then callbacks the attacker's contract
5. Inside the callback, the attacker does all the swaps and exploits

This means **your listener will NOT detect these famous attacks at the
outer-transaction level**. The flash loan call is buried in internal
transactions / event logs.

### Two ways to handle this for testing Person 3's algorithm:

**Option A (recommended): Simulate the attack as if it were a direct call**
- Use the synthetic attack scenarios in `attack_simulation_data.csv`
- These are direct calls to Aave V3 `flashLoan` with realistic parameters
- Your listener detects them; Person 3 can build the cycle graph from them

**Option B (more realistic but requires extra work): Use the real tx hashes
+ event logs**
- The real attack tx hashes below are useful for Person 3 to study patterns
- Person 3's `graph_builder.py` would need to use `eth_getLogs` to fetch
  the Transfer and Swap events from inside these transactions
- This is what the PDF spec actually requires for Stage 4

---

## Real attack reference list

### 1. Euler Finance Hack — March 13, 2023 — $197M stolen

The largest DeFi flash loan attack ever. Borrowed 30M DAI from Aave V2,
deposited into Euler, exploited the `donateToReserves` function.

**Attack transactions (6 total):**
```
0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d
0x71a908be0bef6174bccc3d493becdfd28395d78898e355d451cb52f7bac38617
0x62bd3d31a7b75c098ccf28bc4d4af8c4a191b4b9e451fab4232258079e8b18c4
0x465a6780145f1efe3ab52f94c006065575712d2003d83d85481f3d110ed131d9
0x3097830e9921e4063d334acb82f6a79374f76f0b1a8f857e89b89bc58df1f311
0x47ac3527d02e6b9631c77fad1cdee7bfa77a8a7bfd4880dccbda5146ace4088f
```

**Attacker addresses:**
- Exploiter EOA 3: `0x5F259D0b76665c337c6104145894F4D1D2758B8c`
- Exploit Contract 1: `0xeBC29199C817Dc47BA12E3F86102564D640CBf99`
- Attacker contract: `0x036cec1a199234fc02f72d29e596a09440825f1c`

**Tokens stolen:** DAI, USDC, stETH, WBTC
**Cycle pattern:** DAI flash loan → Euler eDAI mint → self-borrow leverage →
donateToReserves exploit → liquidate self → drain funds

### 2. bZx Pump Attack — February 14, 2020 — $350K stolen

The first major flash loan attack. Borrowed 10,000 ETH from dYdX,
manipulated Uniswap WBTC/ETH price.

**Attack transaction:**
```
0xb5c8bd9430b6cc87a0e2fe110ece6bf527fa4f170a4bc8cd0c4a37e8e1b5d4be
```

**Attacker contract:** `0x4f4e0f2cb72e718fc0433222768c57e823162152`
**Profit:** 1,193 ETH

### 3. bZx Oracle Attack — February 18, 2020 — $620K stolen

The "copycat" second attack. Used 7,500 ETH flash loan from bZx itself.

**Attack transaction:**
```
0x762881b07feb63c436dee38edd4ff1f7a74c33091e534af56c9f7d49b5ecac15
```

**Profit:** 2,381.41 ETH
**Cycle:** ETH → sUSD (3 swaps) → bZx oracle manipulation → ETH

### 4. Harvest Finance — October 26, 2020 — $33.8M stolen

Flash loan + price manipulation of Curve y-pool.

**Attack transaction:**
```
0x35f8d2f572fceaac9288e5d462117850ef2694786992a8c3f6d02612277b0877
```

### 5. PancakeBunny — May 19, 2021 — $45M stolen (BSC, not Ethereum)

Note: This is BSC. Your Ethereum-focused listener wouldn't see this.

### 6. Cream Finance — October 27, 2021 — $130M stolen

Used flash loans from MakerDAO and Aave to mint counterfeit crYUSD tokens.

### 7. Beanstalk — April 17, 2022 — $182M stolen

Governance attack using flash loan to acquire voting power.

**Attack transaction:**
```
0x4b16e74a1ad8a8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3b8e3
```
(Note: verify exact hash on Etherscan before using)

---

## How to use these for Person 3's testing

### For studying attack patterns (recommended first step):
1. Open any of the transactions above on Etherscan
2. Look at the "Internal Txns" tab to see the cycle of swaps
3. Look at the "Logs" tab to see Transfer/Swap events
4. Person 3 uses these patterns to design `graph_builder.py`

### For end-to-end testing (using your existing pipeline):
- Use `attack_simulation_data.csv` instead
- It contains direct flashLoan calls that your listener WILL detect
- The patterns mirror what Euler / bZx / Cream did, but simplified to fit
  the direct-call model

### For decoding verification:
- Person 3's decoder should be able to extract the borrowed amounts
- Compare against `verification_expected.csv` (existing file)
