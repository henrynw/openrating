# OpenRating Algorithm Spec (v1.1)

## 0. Purpose & Scope
- **Goal:** Provide fair, transparent ratings for head-to-head racket sports (badminton first; tennis, squash, padel, pickleball later).
- **Outputs:** Per-player rating `μ` and uncertainty `σ`; optional pair synergy in doubles.
- **Constraints:** Real-time updates, robust to sparse cross-play, resistant to “rating pocket” inflation, minimal recompute cost for back-dated matches.

---

## 1. Design Principles
- **Probabilistic**: Matches provide evidence about relative skill.
- **Uncertainty-aware**: Move new/unknown players more; shrink σ only with consistent evidence.
- **Asymmetric mismatches**: Upsets matter more than expected wins, via residuals or smooth mismatch multiplier.
- **Sport-agnostic core**: Sport profiles define winner/MOV/tuning; core math is shared.
- **Explainable**: Every update logs inputs and deltas.

---

## 2. Notation
- Player *i*: rating `(μ_i, σ_i)`.
- Team rating:  
  ```
  R_A = Σ μ_i + γ_A
  ```
- Win probability:  
  ```
  p_A = Φ((R_A - R_B) / (√2 * β))
  ```
- Match outcome: `y ∈ {0,1}`, surprise `s = y - p_A`.

---

## 3. Update Rule (per Match)

### 3.1 Team delta
```
Δ_team = K * s * W_mov * W_tier * M(p_A, y)
```

### 3.2 Learning rate K
```
K = clip(
  K0 * sqrt((avg(σA²) + avg(σB²)) / (2σ_ref²)) * RookieMultiplier,
  K_min,
  K_max
)
```

- **RookieMultiplier** = 1.4 for first N=10 matches.
- **σ_ref** = steady-state median σ.

### 3.3 Distribution
- Singles: winner `+Δ_team`, loser `-Δ_team`.
- Doubles: split equally across teammates.

---

## 4. Uncertainty Update (σ)

### 4.1 Evidence shrink
```
σ'² = σ² - η_down * I(p) * σ²
where I(p) = 4p(1-p)
```

### 4.2 Surprise inflation
```
if |s| > τ:
  σ'² = min(σ_max², σ'² + η_up * (|s| - τ) * σ²)
```

### 4.3 Inactivity creep (nightly)
```
σ² = min(σ_idle², σ² * (1 + ρ_idle)^Δt)
```

**Defaults:**
- `η_down=0.05`, `η_up=0.15`, `τ=0.25`
- `ρ_idle=0.02/week`
- Bounds: `[σ_min, σ_max] = [70, 400]`

---

## 5. Margin-of-Victory & Tier Weights
- `W_mov`: profile-specific, capped range.
- `W_tier`: sanctioned 1.15, league 1.05, social 1.0, exhibition 0.9.
- Total multiplier capped ≈ 1.5.

---

## 6. Mismatch Asymmetry
- **Option A (default, smooth):**
  ```
  M(p,y) = 1 + λ * (2p - 1) * (1 - 2y), with λ = 0.35
  ```
- **Option B:** drop `M` and rely on residual `s`.

---

## 7. Doubles/Mixed Synergy
- Range: `γ ∈ [-40, 40]`.
- **Activation threshold:** learn only after ≥3 shared matches.
- **Per-match update:**
  ```
  Δγ = clip(K_syn * s, -δ_max, δ_max)
  ```
  Defaults: `K_syn=6`, `δ_max=2`.
- **Decay:** nightly  
  ```
  γ ← (1 - ρ_γ)^Δt * γ,  with ρ_γ=0.03/week
  ```
- **Regularization:**  
  ```
  γ ← γ - λ_γ * γ, with λ_γ=0.02
  ```

---

## 8. Initialization
- New player: `μ=1500`, `σ=350`.
- Rookie multiplier applied for first 10 matches.
- Provisional until `σ ≤ 120`.
- New pair synergy: `γ=0`.

---

## 9. Stabilization (Nightly Batch)
1. **Region bias correction:**  
   - Fit cross-region residuals only.  
   - Apply bounded affine shifts: `|Δ| ≤ 8 pts/day`.

2. **Graph smoothing:**  
   - One Laplacian step:  
     ```
     μ ← μ - λLμ, with λ ≤ 0.02
     ```
   - Skip provisional players.

3. **Drift control:**  
   - Re-center global mean/variance with affine transform.

---

## 10. Back-Dated Results
- Immutable event log with `start_time`, `ingest_time`.
- Daily snapshots (`snapshot_id = UTC date + param hash`).
- On insert at `t*`:
  1. Choose latest snapshot ≤ `t*`.
  2. Build affected subgraph via BFS from participants, horizon H=180 days.
  3. Recompute deterministically; stop if max |Δμ| < 1.
- Provide **Live** and **Historical** ratings.

---

## 11. Parameters (v1.1 Defaults)
- `μ0=1500`, `σ0=350`, `σ_min=70`, `σ_max=400`, `σ_prov=120`
- Badminton: `β=200`, `K0=32`
- Tennis: `β=230`, `K0=28`
- MOV: rally [0.7,1.3], set [0.85,1.15]
- Tier multipliers: 1.15 / 1.05 / 1.0 / 0.9
- Rookie: N=10, multiplier=1.4
- Synergy: as above

---

## 12. Pseudocode

```pseudo
function PROCESS_MATCH(match):
  Aμ = sum(μ[p] for p in match.sideA) + synergy(match.sideA)
  Bμ = sum(μ[p] for p in match.sideB) + synergy(match.sideB)

  pA = normal_cdf((Aμ - Bμ) / (sqrt(2) * β))
  y  = profile.winnerIsA(match.games, match.meta) ? 1 : 0
  s  = y - pA

  W  = profile.movWeight(match.games, match.meta) * tier_weight(match.tier)
  K  = clip(K0 * sqrt((avg(σA²)+avg(σB²))/(2*σ_ref²)) * rookie_boost, Kmin, Kmax)

  M  = use_mismatch ? (1 + λ*(2*pA - 1)*(1 - 2*y)) : 1
  Δteam = M * K * s * W

  for p in sideA: μ[p] += Δteam / len(sideA)
  for p in sideB: μ[p] -= Δteam / len(sideB)

  I = 4 * pA * (1 - pA)
  for p in all_players:
      σ2 = σ[p]²
      σ2 -= η_down * I * σ2
      if abs(s) > τ:
          σ2 = min(σ_max², σ2 + η_up*(abs(s)-τ)*σ[p]²)
      σ[p] = clip(sqrt(σ2), σ_min, σ_max)

  if is_doubles(match):
      Δγ = clip(K_syn * s, -δ_max, δ_max)
      γ[sideA_pair] = clip(γ[sideA_pair] + Δγ, γ_min, γ_max)
      γ[sideB_pair] = clip(γ[sideB_pair] - Δγ, γ_min, γ_max)

  append_history(match_id, scalars, pre/post ratings)
```

---

## 13. Monitoring
- **Accuracy:** Brier, log-loss, AUC.
- **Calibration:** reliability curves by p-bucket.
- **Stability:** mean |Δμ| per player-week, σ distribution, rank churn.
- **Fairness:** residuals vs tier, MOV, region.
- **Guards:** cap per-day net |Δμ| (e.g., 60 pts), damp repeated opponents.

---