# Incident Model to Log Script Semantic Verification (v3)

## Task
You are given:
- a System Description YAML in `<system_description>`,
- a Scenario YAML in `<scenario>`,
- a candidate Python script in `<script>`.

Your job is to evaluate **semantic requirements only**: `S1` .. `S5`.

Do **not** execute the script.
Do **not** perform algorithmic validation already handled elsewhere (CSV schema, determinism, imports, row count, etc.).

Treat the YAML inputs as authoritative and reason from the code.

## Output format (JSON only)
Return **only** raw JSON with exactly these keys: `"S1"`, `"S2"`, `"S3"`, `"S4"`, `"S5"`.

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
  "S5": { "score": 0, "reason": "Issue: ...; Location: ...; Fix: ..." }
}
```

## Failure reason format (mandatory)
For every failing requirement, `reason` must use this exact plain-text format:

- `Issue: <what fails and why>; Location: <exact code location(s)>; Fix: <how to fix it>`

If multiple material issues belong under the same requirement, combine them in one string:
- `Issue: 1) ... 2) ...; Location: 1) ... 2) ...; Fix: 1) ... 2) ...`

Requirements for failure reasons:
- specific,
- actionable,
- grounded in concrete code locations (`SYSTEM`, `SCENARIO`, helper function names, variables, branches, etc.),
- high-confidence,
- focused on the deepest root cause rather than derivative symptoms.

## High-precision evaluation mode
You are used in repeated verification. Therefore:

1. **Fail only on direct, material problems.**
   - A problem is material if it meaningfully changes the implemented YAML semantics, emission behavior, retry/trace logic, rough rate picture, or message coherence.
   - Do not fail for stylistic preferences, cosmetic refactors, or alternate but reasonable code structure.

2. **If multiple plausible implementations exist and the script clearly implements at least one reasonable interpretation supported by the YAML, pass.**
   - Do not invent stricter assumptions than the YAML states.
   - Do not require the candidate to implement extra mechanics not encoded in the YAML.

3. **Do not fail on mere possibility.**
   - If the script deterministically chooses a coherent subset from a broad variable domain, pass.
   - If the script uses rough-count scheduling consistent with encoded rates, pass.
   - If a signal is not guaranteed to appear but the script faithfully implements the encoded support for it, pass.

4. **Do not fail because the script is simpler than a real system.**
   - The target is faithful implementation of the incident model, not maximum simulator realism.

5. **Do not fail for comments/style alone.**
   - Helpful comments are encouraged, but lack of section markers or elaborate prose is not a semantic failure by itself.

6. **If you cannot cite a concrete code location and a concrete semantic mismatch, pass.**

7. **Prefer the deepest root cause over derivative symptoms.**
   - If several complaints are consequences of one modeling choice, report only that deepest issue.

## General evaluation rules
- Treat the YAMLs as authoritative.
- IDs and references are case-sensitive.
- A log reference is `<component_id>.<log_id>`.
- A flow reference is a `flow_id` from `system_description.flows.<state>.req[].id`.
- The script may normalize/minimize the YAMLs into `SYSTEM` and `SCENARIO`. It does **not** need to store every prose field or every reference-only field if they are unused.
- The script may derive helper indices/tables from `SYSTEM` / `SCENARIO`; this is fine.

## Key semantics you must use

### Emission mechanisms
Actual emission may come only from:
- background `components[].beh.<state>.emit[]`,
- per-attempt flow `flows.<state>.req[].emit[]`,
- retry-only `flows.<state>.req[].retry.emit_per_retry[]`,
- failure-event `one_shots`.

`manifestation` fields and `phases.*.flows` are **reference lists**, not direct emission instructions and not suppression lists.

### Rough count semantics
- `per_min` and `rpm` are expected intensities.
- Counts in one deterministic run need only be roughly aligned to those intensities and active multipliers.
- Use rough order-of-magnitude reasoning, not exact accounting.

### Background scope semantics
- Background `scope` controls whether `per_min` applies per listed host or once per component.
- Omitted `scope` means `per_host`.
- For `per_host`, allocate across listed hosts only; do not assume an implicit synthetic fallback host when none are listed.
- For `global`, allocate once per component, not once per host.

### Start-time semantics for request chains
- Background emissions are interpreted at their own timestamp.
- A flow instance belongs to the `n` or `f` flow definition chosen by its **start time**.
- A failure-state flow instance may use the active latency/controller state from its **start time** for the whole request chain.
- Logs/retries may spill over phase or event boundaries at sub-minute scale if caused by modeled latency/backoff.

### Path semantics
- `path` is a coarse logical route/context field, not a full physical execution trace.
- It is mainly for plausibility of the emitted component order.
- Do not require explicit final client-return hops.
- Do not require microscopic post-retry local cleanup steps to appear in `path`.
- Do not fail because the script does not reconstruct hidden physical hops beyond the encoded emitted chain.

### Separate flow entries
Multiple flow entries should be implemented according to the relationships already encoded in the YAML (`rpm`, retry, timing, assumptions). Do not expect the script to invent extra hidden coupling or resplit their volumes beyond what the YAML already says.

### Identity columns
- `service` and `host` must be derived from the **emitting component** for each emitted row.
- Different rows in the same request chain may legitimately use different `service` / `host` values when different components emit them.
- Component-local host stickiness within a request chain is a realism preference, not a hard requirement.

### Variable selection
- Broad variable domains are acceptable if the script clearly chooses a coherent subset based on state / flow / attempt / event / earlier logs.
- For request/attempt chains with outcome-bearing or observed-timing fields, the code should keep those fields coherent with the chosen chain meaning and emitted chronology.
- Do not fail because unused alternatives exist in the domain.
- Fail only when the code forces contradiction, makes contradiction materially likely, or leaves a meaning-carrying / observed-timing field effectively inconsistent in execution.

### Retry/backoff
- `expected_attempts` is a rough target, not an exact per-batch equality constraint.
- Retry structure must stay within `[1, max_attempts]`.
- `emit_per_retry` belongs only to attempts `2..A`.
- If messages log backoff-like values, those values should be compatible with the actual retry spacing.
- A lognormal backoff with soft cap around `2–3x p95` is preferred, but a clearly bounded/skewed deterministic sampler consistent with the hints can also pass.

## Repeated-verification focus points
Pay extra attention to these high-yield issues:
1. **Manifestation overreach** — do not fail merely because the script does not guarantee every manifestation appears.
2. **Path over-literalness** — do not treat `path` as a full execution trace.
3. **Per-row identity mapping** — `service` / `host` should map to the emitting component, not remain constant for the whole flow.
4. **Coherent-subset variable choice** — broad domains are fine when code obviously resolves them coherently.
5. **Rough-count reasoning** — do not demand exact volume conservation.
6. **Start-time controller semantics** — spillover across boundaries is acceptable when the code uses start-time logic consistently.
7. **Background scope semantics** — fail only on material per-host/global collapse, fanout, or synthetic-host mistakes.
8. **Chronology/value coupling** — timing fields in messages should be compatible with emitted timestamp gaps, and outcome-bearing fields should align with the chosen attempt/request outcome.
9. **Root-cause reporting** — prefer one strong issue over several derivative observations.

## Requirement ownership and de-duplication
Each issue must be reported under exactly **one** requirement: the most specific owner.

Use this ownership map:
- **S5** owns:
  - flow log ordering,
  - retry mechanics,
  - trace propagation,
  - per-attempt / retry chronology,
  - observed-timing-field coherence,
  - variable-value coherence,
  - per-attempt / per-request semantic binding,
  - known-at-this-time outcome consistency.
- **S4** owns:
  - allowed emission mechanisms,
  - rough rate/intensity realization,
  - background scope semantics,
  - major source omission/invention,
  - identity mapping (`service`, `host`),
  - emission-source semantics.
- **S3** owns:
  - failure event ordering,
  - persistent/override multiplier semantics,
  - suppression/reactivation,
  - one-shot timing/count semantics,
  - correct application of failure controls.
- **S2** owns:
  - phase boundaries,
  - scenario timeline alignment,
  - consistent base-time usage,
  - steady-state interval handling,
  - scenario-semantic representation in the code.
- **S1** owns:
  - faithful executable representation of the system description in code/data structures.

**Tie-break order:** `S5 > S4 > S3 > S2 > S1`

If an issue could fit multiple requirements, assign it only to the highest-priority owner above.

## Semantic requirements

### S1 — Executable system representation
Pass if the script faithfully represents the executable parts of the system description it uses:
- components and identity metadata,
- log templates and variable domains,
- background behaviors,
- tracing config,
- flows, latency hints, retry settings, and tracing flags.

Important:
- `SYSTEM` may be normalized/minimized.
- Unused prose may be omitted.
- Helper tables derived from `SYSTEM` are fine.

Fail only when the script materially omits, miscopies, or changes system semantics in a way that affects generated logs.

### S2 — Scenario representation and timeline alignment
Pass if the script faithfully represents the executable parts of the scenario it uses:
- base time used consistently,
- phase boundaries,
- failure event order/timing,
- steady-state structure,
- relevant scenario controls and one-shots.

Important:
- `SCENARIO` may be normalized/minimized.
- Unused narrative prose may be omitted.
- Unused reference-only fields may be omitted.
- Do not require any specific absolute base timestamp; require only that the script uses some base time consistently.
- Logs/retries may spill over phase/event boundaries at sub-minute scale if the code uses coherent start-time semantics.

Do **not** fail because the script omitted manifestation lists or phase flow reference lists from `SCENARIO` when they are not needed for generation.

Fail only when the script materially changes the encoded timeline/progression or ignores important scenario structure.

### S3 — Failure-event controller correctness
Pass if the script implements failure events correctly:
- event ordering is respected,
- flow rate multipliers apply to failure flows only,
- background-log rate multipliers apply to failure-state background sources only,
- latency multipliers apply to the intended failure flows,
- controls persist until overridden,
- `0.0` suppression remains active until reactivated,
- one-shots occur at the correct event time and are not scaled by rate multipliers.
- if a one-shot provides an explicit host subset, emissions stay within that subset.

Start-time semantics are allowed:
- a flow chain that starts before a later event may continue under its start-time controller state.

Fail only when event/controller logic is materially wrong or partially applied.

### S4 — Emission mechanics, rough rates, and identity mapping
Pass if:
- logs are emitted only through the allowed mechanisms,
- the script realizes baselines and multipliers with rough order-of-magnitude fidelity,
- background `scope` is respected: omitted `scope` behaves as `per_host`, `per_host` is per listed host, and `global` is once per component,
- the script does not invent new emission sources,
- the script does not structurally erase a major modeled source,
- `service` / `host` values map back to the emitting component.

Important:
- manifestations are reference-only; they need not be guaranteed visible,
- counts need only be roughly aligned, not exact,
- fail if the script collapses a per-host background source into one component-wide stream, fans a global source out across hosts, or invents an implicit synthetic host just to realize per-host scope,
- do not require one `service` or one `host` for an entire flow instance,
- do not use `path` as a basis for failing literal hop reconstruction.

Fail only on direct, material emission-source or identity-mapping problems.

### S5 — Flow mechanics, retries, tracing, and coherent values
Pass if:
- logs from `emit[]` appear in encoded order,
- timestamps reflect the intended latency structure reasonably,
- retries stay within bounds and roughly align with `expected_attempts`,
- `emit_per_retry` is used only on retry attempts,
- once-per-request terminal actions are not incorrectly repeated unless encoded that way,
- trace ids are propagated correctly for traced flow instances,
- variable values are sampled from the correct state domains,
- broad domains are resolved coherently from context,
- observed timing fields (for example `duration_ms`, `total_ms`, `waited_ms`, `backoff_ms`) are compatible with the emitted timestamp gaps for the same attempt/request,
- outcome-bearing categorical fields (for example `status`, `upstream_status`, `result`, `error`, `cause`, `action`) are compatible with the chosen attempt/request outcome and known-at-this-time chronology,
- later logs in an attempt/request chain do not materially contradict earlier outcome-bearing logs unless later-attempt recovery is explicitly modeled.

Important:
- scripts do not need a dedicated `bound context` helper, but the code should make timing fields and outcome-bearing fields mutually consistent with emitted chronology and attempt meaning,
- `latency_ms` and `backoff_ms` are hints, not hard exact bounds,
- lognormal-with-softcap backoff is preferred, but an obviously compatible bounded/skewed implementation can pass,
- if code hardcodes a narrower categorical override, fail only when that override materially excludes modeled state-appropriate outcomes or contradicts the chain meaning,
- if code schedules with one elapsed/backoff value and renders a separately clamped or re-sampled timing field, that is a direct contradiction and should fail,
- do not fail on mere possibility from unused domain values.

Fail only on direct, material flow-mechanics or value-coherence contradictions.

## Evaluation procedure
1. Read the YAMLs and the candidate script.
2. Evaluate `S1` .. `S5` using the ownership rules above.
3. For each requirement:
   - pass unless there is a direct, material, high-confidence issue,
   - if several observed symptoms share one helper-level cause, report that shared cause once at the most direct code location,
   - if failing, cite the exact code location(s),
   - provide a concrete repair suggestion.
4. Return raw JSON only.
