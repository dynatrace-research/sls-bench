# Incident Model to Log Script Conversion (v3)

## Task
Convert the provided incident model into a Python script that generates the logs the modeled system would have emitted during the described incident.

You will receive:
- a System Description YAML in `<system_description>`,
- a Scenario YAML in `<scenario>`.

These two YAML documents are an **incident-specific executable specification** produced by the incident-model prompts (v3.1 semantics).

Your job is to produce a **self-contained deterministic Python simulator** that writes a `logs.csv` file representing that incident.

The goal is **faithful implementation of the YAMLs**, not extra realism beyond what the YAML already encodes. Keep the simulator simple, explicit, and stable.

## Output
Output only the complete Python script. No markdown fences. No prose.

The script must:
- generate and save `logs.csv`,
- be deterministic and reproducible,
- embed the input data as Python dictionaries `SYSTEM: Dict[str, Any]` and `SCENARIO: Dict[str, Any]`,
- use only Python standard library modules, `numpy`, and `pandas`.

## What matters most
1. The generated CSV should reflect the incident model’s causal story and emitted signals.
2. The script should implement the YAML exactly as encoded, not reinterpret it into a different model.
3. The simulator should be simple and verifier-stable.
4. Counts/rates are **rough expectations**, not exact accounting targets.

## Operating principles (read carefully)
1. **Implement the YAML exactly as encoded.** Do not “repair” or reinterpret the incident model. If the YAML encodes a particular flow set, retry shape, or rate split, implement that encoding directly.
2. **Do not invent hidden coupling between sibling flows.** Multiple flow entries should be executed according to the relationships already encoded upstream via `rpm`, retry structure, and assumptions. Do not add extra traffic-splitting or hidden mutual exclusivity logic not present in the YAML.
3. **Use the smallest simulator sufficient to realize the YAML.** Do not add extra queues, schedulers, sampling regimes, or behavioral rules unless they are needed to implement the provided fields.
4. **No forced emissions.** `manifestation` lists and `phases.*.flows` are reference fields. They do not emit logs by themselves and must not be used to force visibility.
5. **Prefer low-variance deterministic scheduling.** Use deterministic or low-variance count allocation close to expected counts for each interval/source. Avoid unnecessarily noisy sampling that makes accepted scripts unstable.
6. **Treat counts and rates as rough expectations.** Preserve order-of-magnitude behavior and relative changes. Do not try to satisfy exact conservation-style accounting.
7. **`path` is a coarse logical route/context field.** Use it for plausibility and emitted-order sanity. It is not a full physical execution trace, does not require explicit final client-return hops, and does not require microscopic post-retry local cleanup steps.
8. **The emitted chronology comes from `emit` and retry semantics, not from `path`.** Do not fabricate extra logs for path-only components.
9. **Per-row identity comes from the emitting component.**
   - `service` = emitting component’s `svc`, or `""` if null.
   - `host` = a host from the emitting component’s `hosts`, or `""` if empty.
   - Different rows in the same flow instance may legitimately have different `service` / `host` values when different components emit them.
10. **Prefer component-local host stickiness within a request chain.** If the same component emits multiple logs for one flow instance (including retries), it is usually better to reuse the same chosen host for that component within that chain.
11. **Choose coherent variable values deterministically.** The script may choose an obvious coherent subset from a broader domain based on state, flow, attempt, event, and earlier emitted logs.
12. **Do not fail-safe by adding new semantics.** If a field is broad, choose coherently from what is encoded. Do not create new values, new logs, or new flows that are not supported by the YAML.
13. **Use comments and explanation for non-obvious logic.** Helpful comments are good, but keep the code clean and functional rather than verbose for its own sake.

## Key semantics

### System semantics
- `n` = normal, `f` = failure.
- Background logs come from `components[].beh.<state>.emit[]`.
- Background `scope` controls whether `per_min` applies once per component (`global`) or separately per listed host (`per_host`, default when omitted).
- Flow logs come from `flows.<state>.req[].emit[]` and are emitted **once per attempt**.
- Retry-only logs come from `flows.<state>.req[].retry.emit_per_retry[]` and are emitted once on retry attempts `2..A`.
- `rpm` is entry request rate **before retries**.
- `latency_ms` is a list of `[p50, p95]` hints, one pair per log in `emit`, interpreted as delay since the previous emitted log in the same attempt.
- `backoff_ms` is a list of `[p50, p95]` hints, one pair between consecutive attempts.
- If tracing is enabled and a flow has `trace: true`, one trace id should be created per flow instance and propagated through all logs of that flow instance, including retries.
- `vars` and `state_vars` define message placeholder domains. Use `state_vars.n` / `state_vars.f` when the log’s state requires it.

### Scenario semantics
- There are exactly two phases: normal then failure.
- Time is expressed in integer minutes, but emitted timestamps are real timestamps with milliseconds.
- Use a fixed UTC base time for scenario minute `0`; any consistent base time is acceptable.
- Phase intervals are **start inclusive, end exclusive**.
- Failure events define piecewise control changes:
  - `rate_multipliers` persist until overridden,
  - `latency_multipliers` persist until overridden,
  - `one_shots` are discrete exact-count emissions at the event time and are not scaled by rate multipliers.
- Only these failure-state sources are modulated:
  - failure-state flow `rpm`,
  - failure-state background `per_min` sources.
- `manifestation` and phase `flows` fields are references only.

### Background scope semantics
- Omitted background `scope` means `per_host`.
- For `scope: per_host`, the modeled rate applies separately across the component's listed `hosts`.
- For `scope: global`, the modeled rate applies once for the component as a whole and must not be fanned out across hosts.

### Outcome/timing binding semantics
- Before emitting a request chain, and before each retry attempt when retries exist, bind a coherent per-request / per-attempt context.
- That context should fix meaning-bearing categorical fields (for example `status`, `upstream_status`, `result`, `error`, `cause`, `action`) and any observed timing fields that appear in messages (for example `duration_ms`, `total_ms`, `waited_ms`, `backoff_ms`).
- Message-carried timing values should agree with emitted timestamp gaps and retry spacing.
- Do not schedule with one sampled value and then independently clamp or re-sample a different value into the rendered message.

### Start-time semantics for request chains
Use the following interpretation consistently:
- A **background emission** belongs to the state and active controls at its own timestamp.
- A **flow instance** belongs to the `n` or `f` flow definition selected by its **start time**.
- A failure-state flow instance uses the active rate/latency control state from its **start time**. Later logs in that same request/retry chain may spill across event boundaries or even beyond `f.end_min` at sub-minute scale, but do not need to re-evaluate multipliers mid-chain.
- This spillover is acceptable if it is caused by modeled latency or retry timing and does not break the incident logic.

### Variable-domain semantics
Use these domain meanings:
- `i`: integer in `[min, max]`
- `f`: float in `[min, max]`
- `ch`: deterministic choice from the listed values
- `uuid`: uuid4-like string
- `hex`: lowercase hex string of given length
- `ip`: IP value, optionally constrained by CIDR
- `str`: a generation hint, not a hard regex

Important:
- broad domains are allowed,
- the script may choose a coherent deterministic subset,
- message consistency within a request/attempt chain matters more than blind domain-wide randomness.

### Retry semantics
- `max_attempts` includes the first attempt.
- `expected_attempts` is a rough target for how many attempts request instances should make on average.
- `emit_per_retry` is emitted once on retry attempts `2..A`, in addition to the per-attempt `emit` chain.
- Use a bounded skewed backoff distribution consistent with `backoff_ms`. Prefer a **lognormal with soft cap around 2–3x p95**.
- Do not place extra once-per-request terminal behavior into retries unless the YAML explicitly encodes it.

### Visibility semantics
- Do not force manifestation visibility.
- Do not suppress or accidentally erase emission-backed major signals either.
- If a decisive signal has meaningful modeled support, the simulator should realize that support naturally through counts, interval scheduling, retries, and one-shots.

## Expected script architecture
Use a clear architecture similar to the following:

1. Define `SYSTEM` and `SCENARIO` as Python dictionaries.
   - They may be **normalized/minimized** versions of the YAMLs.
   - Include the fields actually used by the simulator.
   - Unused prose and unused reference-only fields may be omitted.
   - Do not paste raw YAML text or parse YAML at runtime.

2. Build helper indices.
   - component lookup,
   - log-template lookup,
   - flow lookup by state/id,
   - event list and active controller tables,
   - host/service lookups.

3. Build failure intervals.
   - derive piecewise failure intervals from ordered events,
   - compute active rate multipliers and latency multipliers for each interval/source,
   - treat controls as persistent until overridden.

4. Allocate counts deterministically or with very low variance.
   - For each interval/source, compute expected counts from `per_min`, `rpm`, durations, retry expectations, and active multipliers.
   - Choose integer counts close to expected counts using deterministic rounding, fractional carry, seeded Bernoulli on the fractional remainder, or an equivalently stable method.
   - Avoid highly noisy Poisson-style allocation unless it is tightly controlled and does not materially destabilize the output.

5. Schedule timestamps stably.
   - Spread starts/emissions roughly evenly within each interval and add small deterministic jitter.
   - Keep timestamps sorted or sortable.
   - Small sub-second variation is fine.

6. Simulate flow instances.
   - choose a start time,
   - create per-flow context,
   - choose a trace id if required,
   - choose component-local hosts for emitting components,
   - choose an attempt count consistent with `expected_attempts` and `max_attempts`,
   - emit attempt logs in `emit` order using scaled `latency_ms`,
   - emit `emit_per_retry` on retry attempts only,
   - insert backoff between attempts using the chosen bounded skewed sampler.

7. Render messages coherently.
   - sample placeholders from `vars` and `state_vars` for the correct state,
   - preserve repeated semantic carriers where appropriate,
   - keep outcomes/status-like fields coherent across the request chain,
   - use the emitting component’s `lvl`, `svc`, and host mapping.

8. Emit one-shots exactly.
   - emit the exact `count`,
   - at the event minute with small deterministic sub-minute jitter,
   - using only allowed hosts if `hosts` is provided.

9. Build the final DataFrame and save `logs.csv`.
   - columns exactly: `timestamp, level, message, trace_id, service, host`
   - rows sorted ascending by timestamp.

## Detailed implementation guidance

### Count allocation
Use the input rates as **expected intensities**, not exact row-count commands.

Preferred approach:
- For an interval of duration `d` minutes and a source with effective rate `r`, compute `E = r * d`.
- Turn `E` into a nearby integer count with a deterministic or low-variance method.
- For background sources, omitted `scope` means `per_host`.
- For `scope: per_host`, allocate counts separately across the component's listed `hosts`.
- For `scope: global`, allocate once for the whole component; do not fan the rate out across hosts.
- For flows, allocate flow-instance counts from `rpm`.
- Preserve rough relative changes across intervals and multipliers.
- Do not add post-hoc “fixups” just to force the total row count.

### Flow-attempt allocation
For a batch of flow instances with `expected_attempts = e`:
- keep each instance in `[1, max_attempts]`,
- make the batch average roughly `e`,
- use a simple stable construction (for example, a deterministic two-point mixture around `floor(e)` / `ceil(e)` when sufficient),
- do not overcomplicate the attempt-count model.

### Timing and latency
- `latency_ms` pairs are hints, not hard min/max bounds.
- A simple skewed positive sampler or deterministic quantile-like selection around the `[p50, p95]` hints is acceptable.
- For failure flows, scale each latency pair by the active `latency_multipliers` for the flow’s start interval.
- If emitted messages include observed timing fields (for example `duration_ms`, `total_ms`, `waited_ms`), derive those fields from the same chosen chronology or make the chronology follow those chosen values.
- Do not schedule with one sampled elapsed value and then independently clamp or re-sample a different value for the rendered message.
- Apply small jitter if helpful, but keep the chain coherent.

### Backoff
- Prefer a calibrated lognormal sampler consistent with `backoff_ms`.
- Use a soft cap around `2–3 * p95`.
- If retry logs expose a backoff-style field, use the same chosen backoff value for both the emitted content and the retry spacing.
- Small jitter beyond the soft cap is acceptable if still reasonable.
- Keep backoff deterministic via the fixed seed.

### Identity columns
- Every emitted row’s `service` and `host` must map back to the **emitting component**.
- Do not require one `service`/`host` for a whole flow; multi-component flows legitimately emit multiple identities.
- Empty component metadata should become `""`.

### Trace ids
- For `trace: true` flows: one trace id per flow instance, reused across all logs and retries in that instance.
- For `trace: false` flows: use `""`.
- For background logs: use `""`.
- For one-shots: use `""` unless the script is clearly modeling a request-correlated one-shot that belongs to an existing traced chain.

### Value coherence
When rendering message variables:
- use state-appropriate domains,
- before emitting a request chain (and each retry attempt if retries exist), build a deterministic bound context for that chain/attempt:
  - the intended outcome class implied by the flow/state/attempt,
  - any meaning-carrying categorical fields required by that outcome (for example `status`, `upstream_status`, `result`, `error`, `cause`, `action`),
  - any observed timing fields required by emitted messages (for example `duration_ms`, `total_ms`, `waited_ms`, `backoff_ms`),
- choose values coherently across logs in the same request chain,
- if you override or narrow a categorical field for coherence, derive that override from the state-appropriate template domain plus the chain/attempt meaning; do not hard-code a narrower set that excludes modeled state-specific outcomes unless the YAML flow semantics require it,
- if a logged field represents an observed timing quantity, derive it from the emitted timestamps or make the timestamps follow that chosen value,
- do not schedule with one sampled value and then independently clamp or re-sample a different value for the rendered message,
- keep semantic carriers consistent when the chain meaning requires it,
- allow later attempts to differ from earlier attempts when retry/recovery semantics make that appropriate,
- do not let a later log contradict an already-determined outcome in the same attempt unless the YAML explicitly models that.

### Known-at-this-time discipline
Completion- or outcome-bearing messages (status/result/duration/response-return style logs) should only be emitted after that information is knowable according to the encoded `emit` order and retry structure.

### Path discipline
- `path` is a coarse logical route/context field.
- It should help keep the emitted component order plausible.
- It should not cause the script to invent extra logs.
- It should not be treated as a literal full execution trace.

## Required internal construction procedure (do not output these notes)

### Step 1 — Normalize only the executable semantics
Internally create normalized `SYSTEM` and `SCENARIO` dictionaries containing the fields actually needed by the simulator:
- components and identity metadata,
- log templates and variable domains,
- background behaviors,
- tracing config,
- normal/failure flows,
- phase boundaries,
- failure events,
- active control data,
- one-shots.

You may omit unused prose and unused reference-only fields. However, do not omit a field if the code needs it to reproduce the YAML’s behavior.

### Step 2 — Build source indices
Create helper lookups for:
- components by id,
- logs by `component_id.log_id`,
- flows by state and flow id,
- host/service metadata,
- event list and interval boundaries.

### Step 3 — Derive active control state
From `phases.f.events[]`, derive persistent control state:
- active flow rate multipliers,
- active background-log rate multipliers,
- active flow latency multipliers,
- discrete one-shots.

Treat event controls as persistent until overridden.

### Step 4 — Plan counts for each source
For each normal interval and each failure interval:
- compute effective background rates and flow rates,
- compute expected counts,
- convert expected counts to integer counts with a stable low-variance method.

Do not use manifestation/reference fields as emission sources.

### Step 5 — Schedule background emissions
Generate background emissions from `beh.n` / `beh.f`:
- for background sources, omitted `scope` means `per_host`,
- for `scope: per_host`, allocate counts separately across the component’s listed `hosts`,
- for `scope: global`, allocate once for the whole component; do not fan the rate out across hosts,
- use correct state-based variable domains,
- emit with per-row component identity,
- keep timestamps within the intended interval.

### Step 6 — Schedule flow starts
Generate flow instances from `flows.n` / `flows.f`:
- pick start times within the relevant interval,
- keep state determined by the flow instance start time,
- use the active failure controls at start time for failure flows.

### Step 7 — Simulate request chains
For each flow instance:
- determine trace id,
- determine per-component host choices,
- determine attempt count,
- before emitting logs, build the bound per-attempt / per-request context that fixes outcome-bearing and observed-timing fields,
- emit `emit` logs in order for each attempt,
- emit `emit_per_retry` on attempts `2..A`,
- apply latency/backoff timing using the same bound values that drive any logged duration / waited / backoff-style fields,
- preserve message coherence and semantic-carrier consistency.

### Step 8 — Emit one-shots
For each failure event one-shot:
- emit the exact count,
- at the event time,
- with allowed hosts only.

### Step 9 — Final self-audit before outputting code
Internally confirm:
- the simulator emits only through allowed mechanisms,
- flow instances use the correct state and start-time controller semantics,
- service/host map to the emitting component,
- manifestations were not forced,
- broad variable domains are handled coherently,
- terminal status/result/action fields are compatible with the chosen attempt/request outcome,
- any logged duration / total / waited / backoff field is compatible with the actual timestamp gaps,
- no helper independently clamps or re-samples a timing or outcome field after the chain outcome has been fixed,
- retry/backoff logic is bounded and stable,
- the output will be deterministic,
- the total row count should land in the expected rough target range.

## CSV requirements
The script must produce `logs.csv` with exactly these columns:

| Column | Type | Format |
|---|---|---|
| `timestamp` | string | ISO 8601 with milliseconds in UTC: `YYYY-MM-DDTHH:MM:SS.fffZ` |
| `level` | string | `DEBUG` / `INFO` / `WARN` / `ERROR` / `CRITICAL` |
| `message` | string | human-readable rendered message |
| `trace_id` | string | 32 lowercase hex chars or empty string |
| `service` | string | lowercase-with-hyphens or empty string |
| `host` | string | hostname / IPv4 / empty string |

Rows must be sorted ascending by `timestamp`.

## Verification checks

### Algorithmic checks
**A1 — Log volume target**
- `logs.csv` must contain between `20,000` and `100,000` rows inclusive.

**A2 — CSV format correctness**
- exactly six columns in the correct order,
- values conform to the expected formats,
- rows sorted ascending by timestamp.

**A3 — Deterministic output**
- fixed seed and deterministic output.

**A4 — Allowed imports**
- only Python standard library, `numpy`, and `pandas`.

### Semantic checks

**S1 — Executable system representation**
The script must faithfully represent the executable system semantics it uses:
- components and identity metadata,
- log templates and variable domains,
- tracing,
- background behaviors,
- flows, latencies, retries, and tracing flags.

`SYSTEM` may be normalized/minimized. Unused prose may be omitted.

**S2 — Scenario representation and timeline alignment**
The script must faithfully represent:
- phase boundaries,
- consistent base-time usage,
- failure event ordering/timing,
- steady-state interval structure,
- the scenario controls it actually uses.

`SCENARIO` may be normalized/minimized. Unused narrative and unused reference-only fields may be omitted.

Request chains may spill over phase/event boundaries at sub-minute scale if caused by modeled latency/retry timing.

**S3 — Failure-event controller correctness**
The script must implement:
- persistent rate multiplier semantics,
- persistent latency multiplier semantics,
- exact one-shot semantics,
- correct override behavior,
- correct suppression/reactivation behavior.

**S4 — Emission mechanics, rough rates, and identity mapping**
The script must:
- emit only through background / flow / retry-only / one-shot mechanisms,
- keep rough rate behavior aligned to baselines and active multipliers,
- apply background `scope` correctly (`per_host` by default when omitted; `global` once per component),
- not invent or erase major modeled sources,
- map `service` and `host` from the emitting component on each row.

**S5 — Flow mechanics, retries, tracing, and coherent values**
The script must:
- emit flow logs in encoded order,
- apply latency/backoff coherently,
- keep retries bounded and roughly aligned to `expected_attempts`,
- propagate trace ids correctly,
- choose state-appropriate and chain-coherent variable values,
- keep observed timing fields compatible with emitted timestamp gaps,
- keep outcome-bearing fields bound coherently to the attempt/request meaning and known-at-this-time chronology,
- avoid contradictory outcome/status sequences within a request chain.

## Correction loop
If the user provides:
- `<previous_script> ... </previous_script>`
- `<verifier_feedback> ... </verifier_feedback>`
and optionally
- `<runtime_feedback> ... </runtime_feedback>`

then you must **repair** the previous script rather than generate a brand-new unrelated solution.

Use this repair policy:
1. Read the previous script carefully.
2. Parse every failed requirement’s `Issue`, `Location`, and `Fix`.
3. Base the repair on those exact locations and suggested invariants unless they conflict with the YAML or these prompt semantics.
4. Prefer the smallest change set that fixes the issue **and the underlying helper-level cause** without introducing new fragility.
5. If several failures stem from shared logic (state-aware domain selection, semantic-carrier binding, chronology/value coupling, retry timing), repair the shared helper or chain-planning logic rather than only the cited branch.
6. Preserve good helper structure and stable naming where practical.
7. If the current architecture is fundamentally wrong, do a broader rewrite.
8. If verifier feedback suggests a change that conflicts with these prompt semantics or with the YAML, follow the YAML and these semantics while still addressing the true root cause.
9. Output the full replacement script and nothing else.
