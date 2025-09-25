# OpenRating — Plain‑English Explainer (v1.1)

> **Goal:** Help players understand what their rating means, why it moves, and how to improve it — without needing a math degree.

---

## TL;DR
- Your rating is a **number for skill (μ)** plus a **volatility (σ)** that shows how sure we are.
- We predict your chance to win each match. **If you do better than expected, your rating goes up.** If you do worse, it goes down.
- **New or inactive players move more** because we’re less sure about them.
- Big wins matter a bit more; top‑tier events matter a bit more. (Both are capped.)
- Doubles pairs can earn a small **“chemistry” bonus** for consistent partners.
- On leaderboards, we sort by a **conservative score**: `μ − 2σ`. Provisional players are shown but not officially ranked yet.

---

## 1) What is my rating?
- **μ (mu):** our current best guess of your skill.
- **σ (sigma):** how uncertain we are — think of it as a “wiggle room.” High σ = we’re less sure.
- Example: `μ = 1530, σ = 110` means your true level is *likely* near 1530, give or take.

**Why two numbers?** Because sports are noisy. With σ, we can be honest about how sure we are, not just how high the number is.

---

## 2) How does a match change my rating?
Before each match we estimate your **win chance** based on both sides’ ratings. After the match:
- If you **beat the prediction** (e.g., upset a favorite), you move **up**.
- If you **match the prediction** (e.g., favorite beats underdog), you move **a little**.
- If you **underperform** (e.g., lose as a heavy favorite), you move **down** more.

Rating change also nudges σ:
- **Consistent results** → σ **shrinks** (we’re more confident).
- **Surprising results** → σ **grows** (maybe you’re much better/worse than we thought).

---

## 3) Why do new players move so much?
- New players start at a reasonable default and a **high σ** (we don’t know them yet).
- For their first ~10 matches, they also get a small **rookie boost** so the system can “find” their level quickly.
- After enough matches, σ drops and their rating stabilizes.

**Tip:** Play a variety of opponents. Beating the same person repeatedly teaches the system less than beating different players.

---

## 4) Do scorelines and event type matter?
A bit — and they’re **capped** so one blowout doesn’t warp things.
- **Margin‑of‑Victory (MOV):** big wins nudge ratings more than tight wins (within limits). Tie‑breakers count, but less.
- **Tier:** sanctioned events count a little more than casual matches.

We publish the caps so everyone knows the maximum effect.

---

## 5) How does doubles work?
Your team’s rating is `μA + μB + γ`.
- `γ` is **pair chemistry**. If a duo consistently outperforms expectations, they earn a small bonus over time.
- `γ` grows slowly, only after a few shared matches, and it fades with inactivity. It’s bounded so it never dominates.

---

## 6) Leaderboards: why am I “Provisional”?
You’ll see a **Provisional** badge until you meet these basics:
- σ drops below a stability threshold (we’re confident enough),
- you’ve played enough matches (e.g., 10), and
- you’ve faced a minimum number of **different** opponents.

We still **show** provisional players, but official rank numbers are reserved for stabilized players. Lists are sorted by a **conservative score**: `μ − 2σ`.

---

## 7) Common “Wait, what?” moments
- **“I won but dropped a little.”** If you were a huge favorite and won narrowly, you may have performed *slightly below* expectation. Movements will be tiny in this case.
- **“I lost but barely moved.”** If the result was expected, the system already priced it in.
- **“My friend jumped a lot after one match!”** They’re likely **new** (high σ), or pulled a big upset.
- **“Why is my doubles team better than our singles ratings?”** Consistent over‑performance builds a modest `γ` chemistry bonus.

---

## 8) How to improve your rating (and stabilize σ)
- Play **regularly** (stops σ from creeping back up).
- Seek **diverse opponents** (the system learns more).
- Enter **tiered events** if you can (slightly higher weight).
- Don’t chase blowouts — **quality wins** against tough, varied opponents teach the system best.

---

## 9) Fairness & anti‑gaming
- MOV and tier effects are **capped**.
- Repeated matches vs the same person have **diminishing impact**.
- Region and pocket biases are checked nightly and gently corrected.
- Back‑dated results are handled with **snapshots** so late entries don’t require re‑computing the whole world.

---

## 10) What we log (for transparency)
For every rated match we store a short **explanation record** (no private data):
- both sides’ pre‑match ratings, predicted win chance,
- result and scoreline,
- rating change and σ change,
- whether MOV/tier/chemistry applied,
- and whether an update came from live play, backfill, or nightly stabilization.

---

## 11) Quick glossary
- **μ (mu):** your estimated skill.
- **σ (sigma):** how uncertain we are about that estimate.
- **MOV:** margin‑of‑victory (capped effect).
- **Tier:** event importance (sanctioned/league/social/exhibition).
- **Chemistry (γ):** small bonus for established doubles pairs.
- **Provisional:** shown on lists but not officially ranked yet.
- **Conservative score:** `μ − 2σ`, used for leaderboard ordering.

---

## 12) For the curious (light math)
- We model win chance with a standard bell‑curve (Normal CDF). If your team rating is higher, your win probability is higher.
- After the match, your rating moves in the direction of **“actual minus expected.”**
- σ shrinks when results are consistent with predictions, and grows when they’re surprising.
- Full technical details live in the **OpenRating Algorithm Spec (v1.1)**.