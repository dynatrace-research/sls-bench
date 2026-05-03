# Post-Mortem to Incident Model Semantic Verification (v3.2)

## Task
You are given:
- a post-mortem incident report in `<postmortem>`,
- a System Description YAML in `<system_description>`,
- a Scenario YAML in `<scenario>`.

Your job is to evaluate **semantic requirements only**: `S1`..`S9`.

Do **not** perform structural validation already handled by algorithmic checks. Assume the YAML is parseable and structurally validated unless a semantic requirement explicitly depends on a field’s meaning.

## Output format (JSON only)
Return **only** raw JSON with exactly these keys: `"S1"` .. `"S9"`.

Each value must be:
```json
{ "score": 0|1, "reason": null|string }
```

Rules:
- `score = 1` -> `reason` must be `null`
- `score = 0` -> `reason` must be a non-empty string
- no extra keys
- no prose
- no markdown fences

Example shape only:
```json
{
  "S1": { "score": 1, "reason": null },
  "S2": { "score": 0, "reason": "Issue: ...; Location: ...; Fix: ..." },
  "S3": { "score": 1, "reason": null },
  "S4": { "score": 1, "reason": null },
  "S5": { "score": 1, "reason": null },
  "S6": { "score": 1, "reason": null },
  "S7": { "score": 0, "reason": "Issue: ...; Location: ...; Fix: ..." },
  "S8": { "score": 1, "reason": null },
  "S9": { "score": 1, "reason": null }
}
```

## Failure reason format (mandatory)
For every failing requirement, `reason` must use this exact plain-text format:

- `Issue: <what fails and why>; Location: <exact YAML path(s) / id(s)>; Fix: <how to fix it>`

If multiple material issues belong under the same requirement, combine them in one string:
- `Issue: 1) ... 2) ...; Location: 1) ... 2) ...; Fix: 1) ... 2) ...`

Prefer the deepest root cause over derivative symptoms. If several observations are consequences of one modeling choice, report only that deepest issue.

The feedback must be:
- specific,
- actionable,
- grounded in concrete YAML paths or IDs,
- high-confidence.

## High-precision evaluation mode
You are being used in a repeated verification setting. Therefore:

1. **Fail only on direct, material problems.**
   - A problem is material if it meaningfully harms fidelity, realism, causal consistency, likely log generation, or diagnostic usefulness.
   - Do not fail because a different plausible design would also have worked.
   - Do not fail for stylistic preferences, minor prose improvements, or weak plausibility concerns.

2. **If multiple plausible interpretations exist and the YAML supports at least one reasonable interpretation, pass.**
   - Do not invent stricter assumptions than the YAML states.
   - Do not infer missing implementation details and then fail the model for lacking them.

3. **Do not fail on mere possibility.**
   - If a field, domain, or path admits an obvious coherent interpretation that a downstream generator could deterministically realize from state / flow / retry / event context, pass.
   - Fail only when contradiction is forced, materially likely in ordinary generation, or central to a modeled distinction with no disambiguating context.

4. **If you cannot point to a concrete contradiction or insufficiency and cite the exact YAML location, pass.**

5. **Report only high-confidence issues.**
   - Do not pad `reason` with speculative concerns.
   - Prefer one or two strong issues over a long list of weak ones.

6. **Prefer the deepest root cause over derivative symptoms.**
   - If several complaints stem from one deeper modeling choice, report only the deepest issue.

## General evaluation rules
- Treat the YAML as authoritative.
- IDs and references are case-sensitive.
- A log reference is `<component_id>.<log_id>`.
- A flow reference is a `flow_id` from `system_description.flows.<state>.req[].id`.

## Key semantics
- `n` = normal, `f` = failure.
- Background logs come from `components[].beh.<state>.emit[]`.
- Flow logs come from `flows.<state>.req[].emit[]`.
- Retry-only logs come from `flows.<state>.req[].retry.emit_per_retry[]`.
- `per_min` and `rpm` are base rates.
- Failure-state `beh.f` and `flows.f` rates are **onset baselines** at `f.start_min`.
- `phases.f.events[]` apply persistent rate multipliers, persistent latency multipliers, and discrete `one_shots`.
- A control set in an event remains active until a later event overrides it.
- `manifestation` fields are **reference lists**, not emission mechanisms.
- Therefore, an important manifestation is useful only if the underlying background log / flow / retry log / one-shot makes it likely to appear.
- `path` is a coarse logical route/context field; `emit` is the chronological emitted-log chain for one request attempt.
- `path` should preserve the main request family and keep the emitted component sequence plausibly traversable in order, but it is **not** a full physical packet trace.
- Do not require explicit final client-return hops unless they matter to the modeled meaning.
- Terminal or post-retry local cleanup/spooling actions may still keep the main downstream in `path` when that remains the clearest way to identify the request family.
- If the incident is specifically about a materially different target/route (wrong endpoint, localhost default, wrong shard, wrong region, etc.), `path` should reflect that difference.
- Logs whose semantics include final status, bytes sent, upstream result, total duration, or response return should appear only after that information exists.
- If a log field carries semantic identity or outcome (feature name, upstream id, endpoint/server, status code, kill count, version/config value, outcome label, etc.), and the flow/event meaning requires a specific value, the model should either make that value fixed **or** make the coherent subset obvious from surrounding state / flow / event context.
- The downstream script may deterministically choose coherent values from allowed domains to keep emissions consistent; do not treat every unused alternative as a contradiction.
- `assumptions` may justify adaptations, simplifications, traffic splits, omitted details, and volume/visibility reasoning.

## Repeated-verification focus points
Because this verifier is run repeatedly and should catch **latent underconstraint**, pay special attention to these high-yield checks:
1. **Semantic-carrier binding** — generic logs used as evidence for specific events/flows should not make contradictory values forced or materially likely.
2. **Failure-onset cleanliness** — late-only or event-activated signals should not already be visibly active in the onset baseline.
3. **Coarse path semantics** — paths should preserve the main request family and emitted order, not act as a full physical execution trace.
4. **Known-at-this-time chronology** — completion/outcome logs should appear only after the outcome is knowable.
5. **Quantitative envelopes** — use rough order-of-magnitude reasoning for latency/backoff/throughput/queue/disk domains, not exact accounting.

## Requirement ownership and de-duplication
Each issue must be reported under exactly **one** requirement: the most specific owner.

Use this ownership map:

- **S7** owns system-description-intrinsic realism and realizability:
  - log template realism,
  - variable-domain realism,
  - flow/path/emit/retry realism,
  - causal consistency of emitted flow chains,
  - rate/counter/throughput alignment inside the system model.

- **S8** owns scenario-dynamic realism and event-control alignment:
  - event story quality,
  - event `event` vs `why`,
  - activation/suppression timing,
  - multiplier trajectories,
  - latency progression,
  - one-shot alignment,
  - event-level manifestations vs moment-specific narrative.

- **S9** owns diagnostic sufficiency and likely visibility across the incident as a whole:
  - whether likely emitted signals would let an operator recognize progression, root cause, and distinguishing clues.

- **S6** owns cross-document completeness only when the problem is a missing causal or observational bridge that is **not already a more specific S7/S8/S9 issue**.

- **S2** owns meaning-level misuse of otherwise valid references:
  - wrong component layer,
  - wrong flow interpretation,
  - wrong log meaning.

- **S3/S4** own document-internal logical structure and motivation.
- **S1** owns overall post-mortem fidelity.
- **S5** owns assumptions completeness and usefulness.

**Tie-break order:** `S8 > S7 > S9 > S4 > S3 > S6 > S2 > S1 > S5`

If an issue could fit multiple requirements, assign it only to the highest-priority owner above.

## Semantic requirements

### S1 — System description and scenario capture the spirit of the post-mortem
Pass if the YAML pair preserves the post-mortem’s core:
- system purpose and relevant architecture or component roles,
- incident trigger and broad timeline shape,
- failure mechanism or causal chain,
- concrete symptoms and impact.

Adequate adaptation is allowed. The match does **not** need to be exact.

Extra guidance:
- If the post-mortem characterizes an important symptom as **extremely rare**, materially inflating it into a regularly occurring symptom is an S1 failure.
- It is acceptable to preserve that rarity and use realistic probe / detector / operator signals to keep the incident diagnosable.

Fail only when the model changes the incident’s causal core or observable character in a material way.

### S2 — Semantic consistency between system description and scenario
Pass if the scenario uses modeled entities in ways consistent with their meaning in the system description:
- components are assigned appropriate responsibilities,
- cited logs actually support the narrative claim being made,
- flow variants are used according to their modeled purpose,
- scenario references do not reinterpret a modeled entity into something fundamentally different,
- trigger / summary / event prose do not attribute a material effect to a component unless the system model gives that component a defined causal role.

If the simplified model intentionally omits a real-world detail from the post-mortem, the scenario should either omit that detail from the causal claim or clearly confine it to background context / assumptions.

Do **not** use S2 for:
- missing coverage -> S6
- event/control mismatch -> S8
- structural reference validity -> handled upstream
- purely stylistic narrative weaknesses -> S4

### S3 — System description has a clear, motivated, and logical structure
Pass if:
- `sys.desc` matches the modeled components, logs, and flows,
- components and edges are motivated and coherent,
- logs and behaviors fit the role of each component,
- normal and failure flows together form a believable system model.

Fail only when the system model itself is muddled, internally unmotivated, or role-incoherent.

### S4 — Scenario has a clear, motivated, and logical structure
Pass if:
- `summary` and `trigger_desc` are coherent and concrete,
- the normal phase is clear and useful,
- the failure phase, events, steady conditions, and feedback loops form a believable progression,
- phase-level references are sensible and focused.

Fail only for genuine scenario-structure problems, not for narrower S8 control issues.

### S5 — All important assumptions and adaptations are recorded
Pass if the assumptions sections materially help a reader or downstream model understand:
- what was inferred or adapted,
- why the simplified system is sufficient,
- how traffic/rate splits were modeled when non-obvious,
- why the volume fits the target range,
- why the important signals are visible enough to be useful.

Both documents should independently support rate/volume feasibility reading.
The scenario assumptions should also make the failure-phase piecewise breakdown understandable.

Fail only when missing assumptions materially hinder evaluation or downstream use.

### S6 — The two documents tell a complete, gap-free story
Pass if the pair gives a complete end-to-end account:
- the system description supports the scenario’s claims,
- the trigger, progression, and steady state connect without unexplained jumps,
- important effects have some visible signal,
- important causes have some plausible mechanism in the pair.

Use S6 only for true missing bridges or missing coverage.
Do **not** use S6 for:
- impossible or unrealistic flows/logs -> S7
- event control mismatches -> S8
- weak incident distinguishability or low visibility -> S9
- meaning-level misuse of a specific cited entity -> S2

### S7 — System description contains realistic logs and flows
Pass if the system description is realistic and internally realizable.

Evaluate the following:

#### 1) Log template realism
- templates are meaningful for the component’s role,
- levels fit the message semantics,
- variables are realistic,
- descriptive logs are acceptable when they are realistically grounded in the component’s role, but implausibly self-diagnosing or root-cause-explaining messages that mainly exist to give away the answer should fail,
- rate/counter-like variables are not grossly inconsistent with modeled rates or volumes.

#### 2) Variable-domain realism
- domains are realistic and not self-defeating,
- state-dependent domains reflect real state-dependent behavior when used,
- retry-related variables are compatible with retry settings,
- throughput / queue / connection / lag-like variables are not obviously incompatible with the modeled traffic.

**Important standard:** domains need only admit a coherent realistic subset. The downstream script may choose coherent values deterministically from the allowed range. Do **not** fail merely because a domain is somewhat broad or includes values that would be contradictory in a different context. Fail when:
- no coherent realistic subset exists for the modeled state / flow / event,
- the YAML gives no obvious context for choosing the right subset for a meaning-carrying field,
- or the broadness is so large that contradictory generation would be materially likely or diagnostically harmful.

Snapshot-style counters/durations (e.g., `waited_s`, `lag_s`, `queue_depth`) may describe the condition observed at that minute rather than the instant it began. Do not fail on that basis unless the scenario prose makes a temporally precise claim that the snapshot would materially contradict.

#### 2b) Semantic-carrier binding
- fields that encode modeled identity or outcome (feature names, upstream ids, server/endpoints, status codes, version/config values, kill counts, outcome labels, txid-scale-like values, etc.) should align with the meaning they are used to carry;
- a broader carrier domain is acceptable when the surrounding state / flow / event context makes the coherent subset obvious for downstream generation;
- fail when the carrier itself is the sole disambiguator and the broader domain would make the modeled meaning ambiguous, forcedly contradictory, or materially likely to be contradictory;
- outcome/status values across a single emitted flow chain should be mutually compatible unless the model explicitly distinguishes multiple variants.

#### 3) Flow realism and causal consistency
- each flow is meaningful and role-appropriate,
- the emitted log chain is a plausible chronology for that request,
- descriptions do not imply an ordering that the `emit` sequence violates,
- failure variants stop in the right place,
- downstream components do not emit logs after the request should already have failed,
- retry-only logs come from the retrying layer, not from downstream components.

**Important standard:** evaluate causal consistency primarily from the emitted log chain and log semantics, not from speculative physical execution details beyond the YAML.

#### 3b) Coarse path semantics
- treat `path` as a coarse logical route/context field, not a full physical execution trace;
- it should preserve the main request family and keep the emitted component sequence plausibly traversable in order;
- do **not** require explicit final client-return hops;
- allow terminal/post-retry local cleanup or spooling flows to retain the main downstream in `path` when that remains the clearest way to identify the request family;
- fail only when `path` materially misidentifies the main dependency/route, makes the emitted order non-traversable, or changes the incident meaning.

#### 3c) Known-at-this-time chronology and retry scope
- completion/outcome logs (status, bytes sent, upstream result, response returned, total duration, request completed, etc.) should appear only after the relevant result is knowable;
- retrying flows should not place once-per-request terminal actions inside the per-attempt `emit` chain unless they truly happen on every attempt.

#### 4) Timing interpretation
`latency_ms` pairs are `[p50, p95]` hints, **not** `[min, max]` bounds.
Do not derive impossibility from them unless the contradiction is explicit elsewhere.

#### 5) Quantitative-envelope realism
- a few relevant auxiliary components are allowed when they are operationally adjacent and emit realistic non-primary logs;
- low-rate background WARN/ERROR noise is allowed in normal and failure when it is realistic and clearly non-primary;
- log domains for latency / duration / timeout / backoff / throughput / queue / disk / connection-pressure values should cover what the rest of the model implies;
- metric-like values should stay on the same rough order as modeled total traffic and emission volume;
- use rough order-of-magnitude sanity, not conservation-law exactness;
- avoid implausibly chatty per-request success logging on infrastructure components at very high RPM unless the modeled system explicitly supports that behavior;
- fail on quantitative issues only when the model materially distorts the traffic picture (for example, duplicating one logical lifecycle as multiple independent full-RPM flows or erasing a major visible source).

Fail S7 only on direct, material realism or realizability problems in the system model.

### S8 — Scenario describes a clear, consistent, and realistic failure phase
Pass if the failure-phase story and controls are aligned.

Evaluate the following:

#### 1) Event narrative quality
- events form a believable progression,
- each `event` says what changed and what it produced,
- each `why` explains the mechanism rather than merely restating the event,
- observability status in `why` is sensible.

Do not fail because `event` contains light connective prose. Fail only if the event/why split is materially confused.

#### 2) Narrative -> controls
If the narrative says a signal or flow:
- first appears,
- disappears,
- materially increases,
- materially decreases,
- becomes slower,
- is restored / completed / eliminated / no longer leaking,
- or a discrete operational action happens at that minute,

then the controls should support that claim via:
- `rate_multipliers`,
- `latency_multipliers`,
- `one_shots`,
- or an explicit normal-to-failure base-rate difference where that interpretation clearly fits.

Mechanism claims remain strict: if the prose says a patch / kill / rollback / failover / fix changed exposure or behavior, the modeled mechanism must support that specific action rather than only a vaguely similar operational direction.

#### 2b) Failure-onset cleanliness
- Signals or flows that the narrative says begin later or remain initially muted should not already be visibly incident-like at `f.start_min` via `beh.f`, `flows.f`, or sharply elevated `state_vars.f`.
- If a later event introduces such a signal, the onset baseline should normally keep it inactive/near-zero or otherwise non-incident-like until that event.

#### 3) Controls -> narrative
Material control changes should be justified by the event narrative.
Use this threshold:
- changes of **20% or less relative to the current active multiplier** are ambient variation and do not require explicit narrative support,
- larger changes usually do require narrative support.

Do not fail on an omitted explanation for a tiny multiplier tweak.

#### 3b) Strength of narrative verbs
- words like **disabled / stopped / completed / restored / no leakage / eliminated / recovered** require contradictory evidence to be suppressed or genuinely negligible after that event;
- if a source remains materially active, the narrative should say **reduced / partially relieved / still present** rather than implying disappearance;
- if a later event materially improves rates, success ratios, or symptom frequency while the scenario still remains in failure, the narrative should usually mention that improvement.

#### 4) Activation / suppression semantics
- “appears / starts / first occurs” implies prior inactivity and later activity,
- “stops / is disabled / disappears” implies suppression to `0.0` unless the narrative clearly says partial continuation,
- “increases / spikes / amplifies” does **not** require prior zero if the source was already active,
- a signal can be “elevated” throughout failure because `f` base rate already exceeds `n` base rate; that does not require a new event-time multiplier.

#### 5) Latency progression
Latency multipliers should match the degradation story and should not silently recover unless the narrative clearly describes recovery or partial relief.

#### 6) One-shots
- one-shots should align with new discrete actions occurring at that minute,
- later events may describe consequences of earlier one-shots without repeating them,
- do not require a duplicate one-shot for a consequence that is merely unfolding later,
- if a one-shot is used as evidence for a specific event-specific action, its variable domains should not allow contradictory action values,
- snapshot-style one-shots may capture an already-accumulated condition at that minute; do not require onset-like values unless the event wording explicitly says the condition began exactly then.

#### 7) Event-level manifestations and flows
These are headline references for that moment.
They need not be exhaustive, but they must fit the moment-specific narrative and should not foreground flows/logs that the same event is suppressing or contradicting.

Fail S8 only for significant event-story/control mismatches or implausible failure dynamics.

### S9 — Emitted signals enable analysis and diagnosis of the incident
Pass if the likely emitted logs and flow changes over time would let a reasonable operator:
- recognize the baseline,
- notice the trigger or first visible degradation,
- follow the main progression,
- identify the root cause or at least the correct causal chain,
- and distinguish this incident from close alternatives.

Key points:
- A manifestation reference alone is not enough; underlying emission support matters.
- Not every mechanism must be directly visible. Partial observability is acceptable if the emitted symptoms and progression still support a strong diagnosis.
- Decisive signals should be likely to appear in a single generated log file, not only “possible in expectation.”
- Do not fail merely because the log stream contains some realistic low-rate background WARN/ERROR noise or auxiliary-component chatter, as long as the primary incident remains diagnosable.
- Use rough expected-count reasoning when needed, but stay conservative. Clearly near-zero expected visibility for a decisive signal is a problem; mere lack of exact probability calculations is not.
- If the post-mortem’s decisive customer-facing symptom is intentionally rare, diagnosability may come from realistic probes, detector logs, operator actions, or corroborating internal signals rather than from inflating that symptom’s frequency.

Fail S9 only when the emitted signal picture would likely leave the incident underdetermined, weakly distinguishable, or missing key progression/root-cause evidence.

## Evaluation procedure
1. Read the post-mortem and both YAMLs.
2. Evaluate `S1`..`S9` using the ownership rules above.
3. For each requirement:
   - pass unless there is a direct, material, high-confidence issue,
   - if failing, cite the exact YAML location(s),
   - provide a concrete repair suggestion.
4. Return raw JSON only.