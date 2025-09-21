# OpenRating Algorithm Spec (v1)

## 0. Purpose & scope
- **Goal:** Provide fair, transparent ratings for head-to-head racket sports (badminton first; tennis, squash, padel, pickleball later).
- **Outputs:** Per-player rating `μ` and uncertainty `σ`; optional pair synergy in doubles.
- **Constraints:** Real-time updates, robust to sparse cross-play, resistant to “rating pocket” inflation, minimal recompute cost for back-dated matches.

---

## 1. Design principles
- **Probabilistic**: Treat a match as evidence about relative skill.
- **Uncertainty-aware**: Move new/unknown players more.
- **Asymmetric mismatches**: Heavy favorites gain little for expected wins, lose more when upset.
- **Sport-agnostic core**: Sport profiles provide winner/MOV interpretation; core math is shared.
- **Explainable**: Every change is auditable with inputs and deltas.

---

## 2. Notation
- Players \(i, j\) with ratings \( \mu_i, \sigma_i \).
- Team rating \( R_A = \sum_{i \in A} \mu_i + \gamma_A \) (synergy \( \gamma \) only for doubles/mixed).
- Win probability \( p_A = \Phi\left( \frac{R_A - R_B}{\sqrt{2}\,\beta} \right) \), where \( \Phi \) is the normal CDF and \(\beta\) is sport-tunable.
- Match outcome \( y \in \{0,1\} \) (1 if side A wins), surprise \( s = y - p_A \).

---

## 3. Update rule (per match)
**Team delta**
\[
\Delta_{team} = M(p_A,y) \cdot K \cdot s \cdot W_{mov} \cdot W_{tier}
\]

**Distribute to players**
- Singles: winner +\( \Delta_{team} \), loser \(-\Delta_{team}\).
- Doubles: split equally within a side (can later weight by rallies/points if available).

**Uncertainty shrink**
\[
\sigma' = \sqrt{\sigma^2(1-\alpha) + \alpha \cdot \sigma_{\min}^2}
\quad\text{with}\quad \alpha = \alpha_0 \cdot |s| \cdot W_{mov} \cdot W_{tier}
\]

**Learning rate \(K\)**
\[
K = \text{clip}\left(K_0 \cdot \sqrt{\tfrac{\overline{\sigma^2_A} + \overline{\sigma^2_B}}{2\sigma_{\text{ref}}^2}} \cdot \text{RookieMultiplier},\ K_{\min}, K_{\max}\right)
\]

---

## 4. Margin-of-Victory (MOV) and tiers
- **MOV weight \(W_{mov}\)**: sport-profile function mapping scores to \([w_{\min}, w_{\max}]\).  
  - Rally games (badminton/squash): use capped per-game point spreads → average → scale.
  - Set sports (tennis/padel): use set differential with small amplitude and tie-break dampening.
- **Tier weight \(W_{tier}\)**: sanctioned > league > social > exhibition.

---

## 5. Mismatch asymmetry \(M(p_A,y)\)
- If heavy favorite (e.g., \(p_A > 0.97\)):  
  - win: multiply by small factor (e.g., 0.1)  
  - loss: multiply by larger factor (e.g., 2.0)
- Symmetric for the underdog case.

*Rationale:* prevents farming; punishes shock upsets more than expected wins reward.

---

## 6. Doubles/mixed synergy (optional)
- Pair synergy \( \gamma_{(a,b)} \in [\gamma_{\min}, \gamma_{\max}] \) per discipline.
- Small learning rate \(K_{\text{syn}}\); update synergy in direction of match surprise.
- Stored and decays slowly when pair is inactive.

---

## 7. Multi-sport support (profiles)
A **Sport Profile** defines:
- `winnerIsA(games, meta)` → boolean  
- `movWeight(games, meta)` → \([w_{\min}, w_{\max}]\)  
- `tune` → overrides for \(\beta, K_0\)  
- Valid disciplines and team sizes

**Badminton**: rally to 21, MOV via capped point spread, \(\beta \approx 200\).  
**Tennis**: set differential, tie-break dampening, \(\beta \approx 230\), \(K_0\) slightly lower.  
(Profiles live in code at `service/ts/src/sports/`.)

---

## 8. Initialization & cold-start
- New player: \( \mu = \mu_0\) (e.g., 1500), \( \sigma = \sigma_0\) (e.g., 350), **rookie boost** for first N matches.
- New pair synergy: \( \gamma = 0 \).

---

## 9. Stabilization (nightly batch)
- **Region bias correction** using cross-region residuals (regularized).
- **Graph smoothing** (small Laplacian step across recent match graph) to reduce isolated pocket drift.
- **Drift control** to hold long-run global mean/variance around targets.
- All adjustments are small, logged, and attributed (reason=`stabilization`).

---

## 10. Back-dated results & recomputation
- **Immutable event log** with `start_time` and `ingest_time`.
- **Snapshots** of rating state (daily/weekly).
- On back-insert:
  - Recompute from latest snapshot ≤ `start_time`, **only affected subgraph**, with horizon \(H\) (e.g., 180 days) and epsilon stop \( \varepsilon \) (e.g., 1 point).
  - Expose both **Live** (append-only) and **Historical** (point-in-time) ratings.

---

## 11. Parameters (v1 defaults)
- \( \mu_0 = 1500\), \( \sigma_0 = 350\), \( \sigma_{\min} = 70\), \( \sigma_{\max} = 400\)  
- \( \beta = 200\) (badminton), \(K_0 = 32\), \(K_{\min}=8\), \(K_{\max}=48\)  
- Rookie: \(N=10\), multiplier \(1.4\)  
- MOV: \([0.7, 1.3]\), per-game cap 11 (badminton)  
- Mismatch pivot \(0.97\), favorite win multiplier 0.1, upset loss multiplier 2.0  
- Synergy: \(K_{\text{syn}}=8\), \(\gamma \in [-40, 40]\)

---

## 12. Pseudocode (single match)

```pseudo
function PROCESS_MATCH(match):
  A = team_ratings(match.sideA.players) + synergy(A)
  B = team_ratings(match.sideB.players) + synergy(B)
  pA = normal_cdf((A - B) / (sqrt(2) * beta))
  y  = profile.winnerIsA(match.games)
  s  = (y ? 1 : 0) - pA
  W  = profile.movWeight(match.games) * tier_weight(match.tier)
  K  = clip(K0 * sqrt((avg(sigmaA^2)+avg(sigmaB^2))/(2*sigmaRef^2)) * rookie_boost, Kmin, Kmax)
  M  = mismatch_multiplier(pA, y)
  Δteam = M * K * s * W
  distribute Δteam equally across players on each side
  shrink sigma for each player with alpha = alpha0 * |s| * W
  update synergy if doubles
  append rating_history rows
