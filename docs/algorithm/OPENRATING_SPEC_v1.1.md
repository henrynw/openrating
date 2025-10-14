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
- Team rating (core updates):  
  ```
  R_A = Σ μ_i + γ_A
  ```
- Win probability:  
  ```
  p_A = Φ((R_A - R_B) / (√2 * β))
  ```
- Match outcome: `y ∈ {0,1}`, surprise `s = y - p_A`.
- Adjusted display / prediction (optional):  
  ```
  R_A^{adj} = Σ (μ_i + θ_{sex(i)}) + γ_A
  ```
- Per-sex offset: `θ_g` for `g ∈ {M, F, X}` with baseline `θ_baseline = 0`. Stored ratings (`μ_raw`) exclude this term; public displays use `μ_display = μ_raw + θ_{sex}`.

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

- **RookieMultiplier** = 1.8 for first N=20 matches.
- `K_max = 60` (and `K_min = 8`).
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
σ² = min(σ_max², σ² * (1 + ρ_idle)^Δt)
```

**Defaults:**
- `η_down = 0.10`, `η_up = 0.09`, `τ = 0.30`
- Inactivity creep: `ρ_idle = 0.0075 / week` applied only after 28 idle days
- Bounds: `[σ_min, σ_max] = [70, 350]`

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

## 8. Sex Offset Calibration
- **Scope:** `θ` is never used inside the core update loop. It is applied only when producing adjusted leaderboards or when an API caller explicitly asks for adjusted predictions.
- Combined ladders maintain per-sex offsets `θ_g` (g ∈ {M, F, X}) that shift each player's display mean when needed.
- Stored ratings remain neutral (`μ_raw`); public APIs expose both `μ_raw` and `μ_display = μ_raw + θ_{sex}` so consumers can audit the adjustment.
- Offset learning is triggered only by inter-sex matches. The update leverages surprise and the difference in headcounts:  
  ```
  θ_g ← clip(θ_g + κ * s * Δcount_g, -θ_max, θ_max)
  ```
  where `Δcount_g = (#g on side A) - (#g on side B)` and `s` is the match surprise.
- Offset updates run independently of the μ/σ updates; they never mutate the stored ratings.
- During nightly stabilization the offsets are re-centered to zero mean and a mild regularization (`λ_θ`) decays them toward `0`.
- Unknown sex (`U`) stays neutral (0) until the player is tagged.
- Defaults: `κ=12`, per-update cap `θ_step=6`, absolute clamp `θ_max=120`, nightly shrink `λ_θ=0.01`.
- Updates pause when the past 90 days contain fewer than θ_min_edges mixed matches or when the current spread exceeds θ_max_ci_width.
- Offsets persist per ladder (keyed by `(ladder_id, sex)`) so backfills and service restarts retain calibration.

---

## 9. Initialization
- New player: `μ=1500`, `σ=300` (pros may initialize at `σ=260`).
- Rookie multiplier applied for first 20 matches.
- Provisional until `σ ≤ 130` or matches played ≥ 20.
- New pair synergy: `γ=0`.

---

## 10. Stabilization (Nightly Batch)
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

4. **Sex offset maintenance:**  
   - For each `g ∈ {M, F, X}`, apply nightly shrink `θ_g ← (1 - λ_θ) · θ_g`.  
   - Recenter offsets to zero mean.
   - If inter-sex edges in the past 90 days fall below 200 or the current spread exceeds 40 points, freeze θ updates and skip further adjustments until confidence is restored.

---

## 11. Back-Dated Results
- Immutable event log with `start_time`, `ingest_time`.
- Daily snapshots (`snapshot_id = UTC date + param hash`).
- On insert at `t*`:
  1. Choose latest snapshot ≤ `t*`.
  2. Build affected subgraph via BFS from participants, horizon H=180 days.
  3. Recompute deterministically; stop if max |Δμ| < 1.
- Provide **Live** and **Historical** ratings.

---

## 12. Parameters (v1.1b Defaults)
- `μ0=1500`, `σ0=300` (pros 260), `σ_min=70`, `σ_max=350`, `σ_prov=130`
- Badminton: `β=185`, `K0=44`, `K_max=60`
- Learning rate bounds: `K_min=8`, `K_max=60`
- MOV: rally [0.7,1.3], set [0.85,1.15]
- Tier multipliers: 1.15 / 1.05 / 1.0 / 0.9
- Rookie: N=20, multiplier=1.8
- Uncertainty: `η_down=0.10`, `η_up=0.09`, `τ=0.30`
- Inactivity: `ρ_idle=0.0075` per week after 28 idle days
- Synergy: as above
- Sex offsets: baseline `θ_M=0`, per-update cap `±6`, clamp `±120`, nightly shrink `λ_θ=0.01`, gating thresholds `θ_min_edges=200`, `θ_max_ci_width=40`.

---

## 13. Pseudocode

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

  if match includes multiple sexes:
      for g in {M, F, X}:
          Δcount = count_players(match.sideA, g) - count_players(match.sideB, g)
          if Δcount ≠ 0:
              θ[g] = clip(θ[g] + κ * s * Δcount, -θ_max, θ_max)

  append_history(match_id, scalars, pre/post ratings)

function ADJUSTED_PREDICTION(sideA, sideB):
  Aμ_adj = sum(μ[p] + θ[sex(p)] for p in sideA) + synergy(sideA)
  Bμ_adj = sum(μ[p] + θ[sex(p)] for p in sideB) + synergy(sideB)
  return normal_cdf((Aμ_adj - Bμ_adj) / (sqrt(2) * β))
```

---

## 14. Monitoring
- **Accuracy:** Brier, log-loss, AUC.
- **Calibration:** reliability curves by p-bucket.
- **Stability:** mean |Δμ| per player-week, σ distribution, rank churn.
- **Fairness:** residuals vs tier, MOV, region.
- **Guards:** cap per-day net |Δμ| (e.g., 60 pts), damp repeated opponents.

---
