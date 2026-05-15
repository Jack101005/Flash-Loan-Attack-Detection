# Attack Scenarios — Test Data for Person 3

This folder contains realistic flash loan attack data for testing the
detection algorithm end-to-end through the full pipeline:

```
mock_server → listener → (Kafka) → Person 3's algorithm
```

---

## Files in this folder

### `attack_simulation_data.csv`
**Use this as the listener's input** (replaces `data/test_data.csv`).

15 simulated attack transactions, each a direct call to one of:
- Aave V3 Pool (`flashLoan` or `flashLoanSimple`)
- Balancer V2 Vault (`flashLoan`)
- Uniswap V3 Pool (`flash`)

All transactions WILL pass through your listener's two-pass filter and
get fully decoded. Person 3 receives clean structured data with realistic
amounts, tokens, and attack patterns.

### `attack_scenarios_metadata.csv`
**Send this to Person 3 as their answer key.**

For each tx_hash in the simulation data, this file specifies:
- `scenario` — Short description of the attack
- `expected_cycle` — The token flow Person 3 should reconstruct
- `expected_profit_usd` — How much profit Person 3 should compute
- `borrowed_value_usd` — Total flash-loaned value
- `confidence_target` — HIGH / MEDIUM / LOW that Person 3's algorithm
  should output
- `inspired_by` — Which real-world attack this scenario mirrors

### `REAL_ATTACKS.md`
Documentation of actual historical flash loan attacks with verified
transaction hashes. Includes an important caveat: real attackers don't
call `flashLoan` directly from EOAs — they use intermediary contracts,
which means the listener won't detect those raw transactions. The
simulation data above bridges this gap for testing.

---

## How Person 3 uses this data

### Step 1: Generate fresh data
```powershell
cd D:\ThirdYear\second_semester\Flash-Loan-Attack-Detection
python generate_attack_scenarios.py
```
This creates fresh `attack_simulation_data.csv` and `attack_scenarios_metadata.csv`.
Re-run anytime to randomize tx hashes and from-addresses.

### Step 2: Run through the listener
```powershell
# Replace test_data.csv with the attack scenarios
copy data\attack_scenarios\attack_simulation_data.csv data\test_data.csv

# Terminal 1
python ingestion/mock_server.py

# Terminal 2
python ingestion/listener.py
```
All 15 transactions should be detected and decoded.

### Step 3: Person 3's algorithm processes detections
Person 3's code receives:
- The decoded flash loan parameters (assets, amounts, receiver)
- Built graph of token flows
- Computed cycle path
- Estimated profit
- Confidence score (HIGH/MEDIUM/LOW)

### Step 4: Person 3 verifies against the answer key
Person 3 compares their algorithm's output to `attack_scenarios_metadata.csv`:
- Did they detect 9 HIGH-confidence attacks?
- Did they detect 3 MEDIUM scenarios?
- Did they correctly flag 3 LOW scenarios (small arb + whale)?
- Did their `cycle_path` make sense for each scenario?
- Are their `profit_estimate` values within an order of magnitude of `expected_profit_usd`?

---

## Distribution of scenarios

| Confidence | Count | Examples |
|---|---|---|
| HIGH   | 9 | Euler-style $197M, Cream-style $130M, Beanstalk-style $182M |
| MEDIUM | 3 | MEV arb, Uniswap V3 dual-side flash, mid-size triangular |
| LOW    | 3 | Small legitimate arb, whale flash with no clear cycle |

Total simulated borrowed value across all scenarios: ~$3 billion
Total simulated attacker profit: ~$450 million
