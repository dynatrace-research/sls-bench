# Post-Mortem to Incident Model Conversion (v3.2)

## Task
Convert a post-mortem incident report into two YAML documents:

1. **System Description** — a compact but sufficient model of the system, its logs, and its normal/failure behavior.
2. **Scenario** — a time-ordered model of one incident in that system.

The post-mortem will be provided in `<postmortem>` tags.

The YAML pair is an **incident-specific executable specification** used later to:
- generate a Python script that emits realistic logs, and
- generate log-based QA tasks about phase boundaries, behavior changes, and causes.

Your job is **not** to preserve every real-world detail. Your job is to preserve the **incident’s causal core, observability, and diagnostic value** while producing a YAML pair that is structurally valid, semantically stable, and practical for downstream log generation.

## Output format
Return exactly two YAML documents and nothing else:

<system_description>
```yaml
...
```
</system_description>

<scenario>
```yaml
...
```
</scenario>

No prose outside those tags.

## Operating principles (read carefully)
1. **Model one incident-specific system and one incident-specific failure.** Do not design a reusable platform model.
2. **Prefer the smallest incident-sufficient model.** Include only components, logs, flows, events, and mechanisms needed to explain the incident and support downstream log-based analysis.
3. **Important diagnostic signals must be emission-backed and likely to appear.** A manifestation list is only a reference list; it does not emit logs by itself.
4. **Strong grounding, flexible adaptation.** Preserve the post-mortem’s causal core, progression, failure symptoms, and relevant technologies or roles. You may genericize names and surrounding architecture if that keeps the mechanism faithful.
5. **Prefer tight variable domains where they add real clarity.** When a broad domain is not needed, choose a small, realistic one. Favor narrow ranges, short enumerations, or essentially fixed values over vague, expansive domains. But do **not** add extra templates or variants solely to remove harmless possibilities when the downstream script can obviously choose a coherent subset from the current state / flow / event context.
6. **Prefer binding semantic carriers, but do not over-specify.** If a log field carries semantic identity or outcome used to distinguish modeled variants — e.g., upstream/component ids, endpoint/server names, feature names, config/version values, status codes, kill counts, outcome labels — make the intended value either fixed, state-constrained, flow-constrained, or otherwise obvious from context. A broader domain is acceptable when the surrounding state / flow / event semantics make the coherent subset unambiguous for downstream generation.
7. **Treat `path` as a coarse logical route/context field.** It should preserve the main request family and keep the emitted component order plausible, but it does **not** need to encode every physical hop, final client-return hop, or microscopic post-retry local cleanup step. Do not use `path` to claim a materially different primary downstream than the flow actually concerns.
8. **Use rough-count reasoning, not exact accounting.** Volume estimates and subset splits are approximate sanity checks. Avoid clearly duplicating one logical lifecycle as multiple independent full-RPM flows, but do not over-engineer exact traffic conservation.
9. **Prefer fewer moving parts.** Avoid decorative components, decorative background logs, decorative flows, and decorative events. A few relevant auxiliary components that emit realistic adjacent logs are allowed when they add plausible noise without changing the incident's causal core.
10. **Keep dynamics legible.** Persistent failure-state behavior belongs in `flows.f` / `beh.f` base rates only when it is active from `f.start_min` and persists through most of failure. Time-varying changes, late activations, and temporary suppressions belong in `phases.f.events[]`.
11. **Use `one_shots` only for discrete operational actions or markers.** Do not use them for persistent symptoms that should be rate-based.
12. **Event prose must match controls.** Do not narrate a new activation, suppression, latency shift, operational action, elimination, or recovery unless the YAML controls and value domains actually realize it.
13. **Preserve rare symptoms when rarity is part of the incident.** If the post-mortem says a customer-visible symptom is extremely rare, keep it rare. Make the incident diagnosable via targeted probes, operator actions, internal detector logs, or other realistic evidence — not by inflating the rare symptom itself.
14. **Healthy baselines should look mostly healthy.** A small amount of low-rate WARN/ERROR-like background noise is allowed when it is realistic, but signals that are supposed to distinguish the incident should not be frequent in normal unless the post-mortem clearly says that such noise is normal.
15. **Assumptions are part of the model.** Use them to record adaptations, modeling choices, traffic splits, visibility rationale, omitted real-world details, and volume estimates.
16. **Compactness is a feature.** As a soft target, prefer roughly 3–7 components, a small set of important flow families, 2–4 failure events, and 1–3 steady conditions unless the post-mortem clearly needs more.
17. **During repair, prefer local fixes when they are sufficient.** Preserve good structure and only do a broader rewrite when needed.

## Key semantics
- **States**: `n` = normal, `f` = failure.
- **Background logs**: `components[].beh.<state>.emit[]` — emitted independently of request traffic.
- **Flow logs**: `flows.<state>.req[].emit[]` — emitted once per request attempt.
- **Retry-only logs**: `flows.<state>.req[].retry.emit_per_retry[]` — emitted once on each retry attempt (attempts 2..A).
- **Background rate** `per_min`: emissions per minute. `scope` is `per_host` (default) or `global`.
- **Flow rate** `rpm`: entry requests per minute before retries.
- **Failure-state base rates**: `beh.f.emit[].per_min` and `flows.f.req[].rpm` are the onset baselines at `f.start_min`.
- **Events**: `phases.f.events[]` change active rate multipliers, latency multipliers, and one-shots over time. A control stays active until a later event overrides it.
- **Manifestations**: headline references to diagnostically important logs for a phase, event, or steady condition. They do **not** emit logs by themselves.
- **Coarse path vs emitted logs**:
  - `path` is a coarse logical traversal/context field for that flow family, not a full physical packet trace.
  - It should preserve the main request family and make the emitted component sequence traversable in order.
  - You may omit exact final client-return hops when they add no diagnostic value.
  - For post-retry terminal cleanup/spooling or other local after-effects, `path` may stay anchored to the main request family rather than shrinking to only the last microscopic local action.
  - Do not use `path` to claim a materially different primary downstream than the flow actually concerns.
- **Failed flow variants**: when a request can fail mid-way, model that as a distinct flow variant when the emitted logs, stop-point, retry behavior, or timing materially differ.
- **Outcome-bearing / completion-bearing logs**: logs whose semantics include final status, bytes sent, upstream result, total duration, or response-return meaning should appear only after that information is known.
- **Semantic carriers**: fields such as feature names, server addresses, upstream ids, status codes, config/version values, and outcome labels are part of the model’s meaning. If a flow/event needs a specific value, make that value either effectively fixed or obviously selectable from surrounding state / flow / event context. Do not add extra variants purely to remove harmless alternatives.
- **Retries**: model retries on the retrying layer, not on downstream components. Do not put once-per-request terminal actions into a retrying flow’s per-attempt emit chain unless they truly happen on every attempt. If one logical lifecycle is split across multiple flow entries, their `rpm` values should be rough mutually exclusive subsets or stage-specific attempt volumes — not multiple independent full-RPM copies of the same base operation.

## Required internal construction procedure (do not output these notes)
You must follow this construction order internally before writing the final YAML.

### Step 1 — Extract the incident backbone
Internally write a compact backbone for the post-mortem:
- what the system does,
- what external trigger or precondition starts the incident,
- the ordered failure progression beats,
- the final steady broken state,
- the user/business impact,
- which mechanisms are directly observable, partially observable, or require outside knowledge.

If the post-mortem is ambiguous, pick the **simplest plausible** interpretation that preserves the causal core and the observable signals.

### Step 2 — Choose the minimal system
Select only the components needed to support:
- the important normal behavior,
- the incident trigger and propagation path,
- the final symptoms,
- the logs and flows needed for diagnosis.

Do not model surrounding infrastructure unless it contributes directly to emitted logs or causal explanation. When realistic, include a few operationally adjacent auxiliary components and a small amount of low-rate background WARN/ERROR noise so the log stream is not perfectly clean, but keep them clearly non-primary.

### Step 3 — Build a hidden signal ledger
Internally choose **3–8 anchor signals** that make the incident diagnosable. For each anchor signal, track:
- source (`component.log` or flow),
- what it proves,
- whether it is a baseline marker, trigger marker, progression marker, or steady-state marker,
- whether it is directly observable or only partially observable,
- why it is likely to appear in a single generated log file,
- **what semantic carrier values must be fixed** (if any),
- **whether visibility comes from baseline rate, event activation, one-shot, or targeted probe flow**.

At least one anchor signal should support each of:
- the normal baseline,
- the trigger or earliest visible failure change,
- each major progression beat,
- the final steady broken state.

If an important user-visible symptom is intentionally very rare, do **not** inflate it just to satisfy visibility. Instead choose additional realistic anchor signals (e.g., targeted probe, detector, operator action, or internal corroborating symptom) that keep the incident diagnosable.

### Step 4 — Define log templates and variable domains
For each component:
- define only meaningful log templates,
- make `msg` message-only,
- choose realistic levels and realistic placeholder variables,
- avoid contrived self-diagnosing or root-cause-explaining messages; descriptive logs are acceptable when they are realistically grounded in the component's role,
- use `state_vars` only when a variable’s domain materially differs by state,
- prefer small enumerations and narrow numeric ranges,
- avoid overly generic `str` hints unless unavoidable.

**Domain discipline**
- If a log appears only in one state, do not create unnecessary state-dependent domains for it.
- If a variable reflects throughput, counts, queue depth, attempts, latency-like quantities, disk/space pressure, backoff, or similar operational magnitudes, make its domain consistent with modeled rates, retry settings, and event multipliers.
- Domains need only admit a coherent realistic subset; they do not need to enumerate every impossible combination. However, do not make them so broad that realistic generation becomes fragile.
- If the post-mortem relies on a specific version/config/server/upstream/status/outcome value, bind that value tightly enough that generation cannot contradict it.

### Step 4A — Bind semantic carriers and timing roles
For every log template, decide internally:
- its timing role: `entry`, `internal_step`, `retry_marker`, `terminal_success`, `terminal_error`, `response_return`, `periodic_metric`, or `action_marker`;
- whether any variable is a **semantic carrier** whose value matters for diagnosis or event meaning.

Then enforce all of the following:
- outcome-bearing / completion-bearing logs (status, bytes, upstream result, response sent, request completed, total duration, etc.) appear only after the relevant result is known;
- if a flow/event/one-shot needs a specific carrier value, make that value effectively fixed for that use **or** make the intended subset obvious from surrounding state / flow / event context;
- if multiple materially different meanings are needed (e.g., status 200 vs 503, feature A vs feature B, upstream X vs upstream Y), prefer separate log IDs or separate flow variants over one generic log with a loose domain **when the context would otherwise be ambiguous**;
- one-shots used as evidence for a specific action should carry the specific action’s values, unless the event/context already makes the intended value unambiguous.

### Step 5 — Define flows and failure variants
For each important user or system action:
- decide whether one flow is enough or whether normal/failure variants are needed,
- make the `path` structurally plausible for the flow’s **main logical route/context**,
- make the `emit` sequence a causally valid chronological log chain,
- add a separate failed variant when the request terminates early, times out, is rejected, or never reaches a downstream,
- keep retry logs on the retrying layer only.

**Flow design discipline**
- Do not create a new failure-state flow variant unless something material changes: emitted logs, stop-point, retry semantics, timing profile, or main attempted target.
- If the same observable request path simply continues with different rates, latencies, or variable values, avoid unnecessary variant proliferation.
- Repeat a component in `path` when it materially clarifies emitted order or a return hop that matters diagnostically; exact client-return hops are optional.
- `path` should preserve the main request family and emitted component order. It may omit final client-return hops or microscopic terminal local cleanup steps. Do not encode a materially wrong primary downstream.
- If a retrying request performs a once-per-request terminal action only after retries are exhausted (e.g., final abort, client give-up, durable spool, operator-visible terminal response), do **not** place that action inside the per-attempt `emit` chain unless it truly happens on every attempt. If the schema cannot express the exact after-retry action cleanly, prefer a simpler but semantically honest modeling choice.
- If multiple flow entries represent stages or outcomes of one logical lifecycle, ensure their `rpm` values are rough mutually exclusive subsets or stage-specific attempt volumes; do not assign the full original entry RPM to every stage.
- If a log’s semantics require a downstream response to have happened first, include the necessary downstream / return-hop structure where that matters for meaning, or use a different log.

### Step 6 — Set rates, latency hints, and total volume
Choose plausible:
- background `per_min`,
- flow `rpm`,
- retry expectations,
- `latency_ms` hints.

Then check the estimated total log volume over the whole scenario and keep it in `20000..100000`.

**Visibility discipline**
When a log or flow is important for understanding or diagnosing the incident, it must be **likely** to appear in a single generated log file.
Use these rough heuristics:
- a phase baseline marker should usually have expected count comfortably above zero across that phase,
- a decisive progression symptom should usually have several expected appearances across its active interval,
- a discrete operational action should usually be a `one_shot` if you need it to appear exactly when the event occurs.

Do not rely on a low-rate manifestation being “probably there.” If it matters, give it enough modeled support to appear naturally.

**Quantitative-envelope discipline**
Before finalizing, compute a rough min/max envelope for every important quantitative field:
- latency / duration / timeout / backoff,
- throughput / rps / eps / queue depth / lag / disk usage / connection pressure,
- counts or gauges that should reflect modeled traffic or failures.

Then enforce:
- each domain must cover the largest and smallest values plausibly implied anywhere in the scenario for the quantity it claims to represent;
- summary/metric-like logs should stay on the same order of magnitude as the modeled total traffic or emission volume;
- treat these as rough sanity envelopes, not exact conservation calculations;
- do not create healthy normal-phase metrics that already look incident-like unless the post-mortem clearly says they do;
- do not use per-request success logs on infrastructure components at very high request rates unless that logging pattern is itself realistic for the modeled system.

If a decisive customer-facing symptom is canonically rare in the post-mortem, keep that rate faithful and instead make diagnosis possible through realistic probe, detector, or corroborating signals.

### Step 7 — Build failure events from control changes
Construct `phases.f.events[]` by deciding **what control changes at each event time**, then writing narrative that matches those changes.

For each event, internally track:
- which sources become newly active,
- which sources are suppressed,
- which sources materially increase or decrease,
- which flows get latency changes,
- which discrete operational actions happen exactly at that minute,
- which logs/flows are the headline manifestations,
- the **effective before/after counts or levels** for each headline signal.

**Lexical discipline for event prose**
Use words carefully:
- say a signal **appears / starts / first occurs** only if it was previously inactive (effectively multiplier `0.0` or otherwise absent) and becomes active at this event;
- say **increases / spikes / amplifies** only if it was already active and becomes materially larger;
- say **stops / is disabled / disappears** only if the source is driven to `0.0` from this event onward;
- say **remains elevated** when the source is already elevated because of failure-state base rates and no new event-time activation is needed;
- for snapshot-style counters/durations in one-shots (e.g., `waited_s`, `lag_s`, `queue_depth`), avoid exact onset wording like **begins waiting** unless the sampled value will also be onset-like; otherwise use observational wording like **is observed waiting** or **waiting is present**;
- describe a **new operational action at this minute** only if it is backed by a `one_shot` in this event;
- say **recovers / restores / completes / no leakage / eliminated / mostly gone** only if the remaining controls and value domains make contradictory evidence absent or genuinely negligible; otherwise say **reduced / partially relieved / still present but smaller**.

**Use event fields correctly**
- `event`: observational cause → effect only.
- `why`: the mechanism explaining why that cause leads to that effect, plus whether it is directly observable, partially observable, or external.
- `flows` / `manifestation`: the headline items for that moment, not an exhaustive list.

**Failure-onset discipline**
At `f.start_min`, only signals that are truly active from the first minute of failure should already look incident-like in `beh.f` / `flows.f` or in `state_vars.f`.
If the event narrative says a signal is initially muted or begins later, the model must make that true through baseline zeros/inactivity, event activation, or non-elevated initial domains.

### Step 8 — Write assumptions deliberately
Use `assumptions` to record:
- what was inferred or adapted from the post-mortem,
- why the simplified system is sufficient,
- traffic split logic across variants,
- any genericization of technologies or names,
- any important post-mortem detail intentionally omitted from the modeled mechanism and why,
- the normal-phase logs/min estimate,
- the total log count estimate and why it falls in range,
- a short visibility rationale for the most important diagnostic signals.

In the **scenario** assumptions additionally record the failure-phase piecewise breakdown:
- interval durations,
- active effective logs/min after multipliers,
- discrete one-shot counts per event.

### Step 9 — Final self-audit before writing YAML
Before producing the answer, internally confirm all of the following:

**A. Minimality**
- No decorative components, flows, or events.
- Every included item supports generation, diagnosis, or fidelity.
- No contrived self-diagnosing logs that simply explain the answer; realistic descriptive logs remain acceptable.

**B. Flow realizability**
- Every flow’s emitted component sequence is traversable through `path`.
- Failed variants stop where they should.
- Retry logs come from the retrying layer.
- `path` keeps the main request family clear without pretending a materially different primary downstream.

**C. Narrative ↔ controls alignment**
- Every meaningful narrative signal change is backed by a control or by an `n`→`f` base-rate shift.
- Every material control change is justified in narrative.
- No event claims a new action without a matching one-shot.
- Strong claims like “disabled”, “completed”, “recovered”, or “no leakage” are only used when contradictory evidence is suppressed or negligible.

**D. Visibility**
- Every event and steady condition has at least one likely-visible manifestation.
- The decisive diagnostics for root cause and progression are likely to appear in logs without downstream forced emissions.
- Rare user-facing symptoms were not inflated merely to create visibility; extra visibility comes from realistic corroborating signals.

**E. Semantic binding**
- Every one-shot or flow that relies on a specific feature/status/upstream/server/version/outcome is either bound tightly enough or contextualized clearly enough that downstream generation can choose a coherent value.
- Generic logs were split when materially different meanings would otherwise be ambiguous.
- Scenario narrative does not name a component as a cause unless the system model gives it a modeled causal role.

**F. Quantitative envelope**
- Variable domains cover all important implied latencies, durations, backoffs, and metric ranges.
- Metric-like values remain on the same rough order as modeled traffic and volume.
- Healthy normal baselines do not already look incident-like unless the post-mortem requires that.

**G. Failure-onset cleanliness**
- `beh.f` / `flows.f` contain only signals active from failure start and persistent across most of failure.
- Late-only or event-activated signals start inactive and are activated by events.

**H. Assumptions**
- Important adaptations and volume/visibility estimates are recorded.
- The two documents are independently readable.

## System Description specification

### YAML structure
```yaml
sys:
  id: <system_name_snake_case>
  desc: <3-5 sentences: what the system does and why it exists>

states: {n: normal, f: failure}

components:
  - id: <component_id_snake_case>
    name: <human-readable name>
    svc: <service-name-or-null>
    hosts: [<host1>, <host2>, ...]
    desc: <1-5 sentences>

    to:
      - dst: <component_id>
        proto: <https|grpc|kafka|redis|jdbc|tcp|etc.>
        desc: <1-3 sentences>

    logs:
      <log_id>:
        desc: <1-2 sentences>
        lvl: <DEBUG|INFO|WARN|ERROR|CRITICAL>
        msg: "<message template with {vars}; no timestamp/level prefix>"
        vars:
          <var>: {k: <i|f|ch|uuid|hex|ip|str>, v: <domain>}
        state_vars:
          n: {<var>: {k: <i|f|ch|uuid|hex|ip|str>, v: <domain>}, ...}
          f: {<var>: {k: <i|f|ch|uuid|hex|ip|str>, v: <domain>}, ...}

    beh:
      n:
        desc: <1-5 sentences>
        emit:
          - id: <log_id>
            per_min: <float >= 0>
            scope: <per_host|global>   # optional; default per_host
      f:
        desc: <1-5 sentences>
        emit:
          - id: <log_id>
            per_min: <float >= 0>
            scope: <per_host|global>   # optional; default per_host

tracing:
  on: <true|false>
  origins: [<component_id>, ...]
  trace_id: {k: hex, v: 32}

flows:
  n:
    desc: <1-5 sentences>
    req:
      - id: <flow_id_snake_case>
        desc: <1-5 sentences>
        rpm: <float >= 0>
        path: [<component_id>, <component_id>, ...]
        emit: [<component_id>.<log_id>, ...]
        latency_ms: [[<p50>, <p95>], ...]
        retry:
          max_attempts: <int >= 1>
          expected_attempts: <float in [1, max_attempts]>
          emit_per_retry: [<component_id>.<log_id>, ...]
          backoff_ms: [[<p50>, <p95>], ...]
        trace: <true|false>
  f:
    desc: <1-5 sentences>
    req:
      - id: <flow_id_snake_case>
        desc: <1-5 sentences>
        rpm: <float >= 0>
        path: [<component_id>, <component_id>, ...]
        emit: [<component_id>.<log_id>, ...]
        latency_ms: [[<p50>, <p95>], ...]
        retry:
          max_attempts: <int >= 1>
          expected_attempts: <float in [1, max_attempts]>
          emit_per_retry: [<component_id>.<log_id>, ...]
          backoff_ms: [[<p50>, <p95>], ...]
        trace: <true|false>

assumptions:
  - <adaptation / inference / volume / visibility note>
```

### System Description rules
- Up to 10 components. Choose only the relevant ones. A few relevant auxiliary components are allowed when they emit realistic adjacent logs that add background noise without becoming the primary subject of the incident.
- Variable domain formats:
  - `k: i` or `k: f` -> `v: [min, max]`
  - `k: ch` -> `v: [a, b, c]` (non-empty list)
  - `k: uuid` -> `v: null`
  - `k: hex` -> `v: <length>`
  - `k: ip` -> `v: null` or `v: "<cidr>"`
  - `k: str` -> `v: <generation hint>`
- Each component should define only the logs that matter.
- Each log template is defined exactly once on its component.
- Within a given state, a log template must be referenced through exactly one emission mechanism:
  - background,
  - per-attempt,
  - retry-only,
  - or one-shot.
- `msg` is message-only; do not include timestamps, levels, or wrappers.
- `vars` and `state_vars` must cleanly cover all placeholders with no overlap.
- `state_vars` is for variables whose domain materially changes by state. If present, include both `n` and `f` keys and keep the variable type the same across states. If state differences are not important, keep the variable in `vars`.
- Use realistic levels and realistic descriptions. Avoid logs whose semantics are vague or impossible to place in a coherent flow. Avoid contrived self-diagnosing signatures that simply explain what went wrong.
- If a flow emits no logs, `latency_ms` must be `[]`.
- `latency_ms` pairs are `[p50, p95]` hints for realistic generation, not hard bounds.
- `rpm` is entry traffic only; do not inflate it to simulate retries.
- Retry logs should represent retry attempts at the retrying layer only.
- If tracing is enabled, only origin components may start traces.

## Scenario specification

### YAML structure
```yaml
scenario:
  id: <incident_id_snake_case>
  title: <brief incident title>

  states: {n: normal, f: failure}

  summary: |
    <3-4 sentences describing normal purpose, trigger, failure manifestation, and impact>

  trigger_desc: |
    <concrete external action or precondition that starts the incident>

  time:
    total_minutes: <int 2..60>
    phases:
      n: {start_min: 0, end_min: <int>}
      f: {start_min: <int>, end_min: <int>}

  phases:
    n:
      desc: |
        <detailed normal operations narrative>
      flows: [<flow_id>, ...]
      manifestation: [<component_id>.<log_id>, ...]

    f:
      desc: |
        <detailed overall failure narrative>

      events:
        - order: 1
          at_min: <int>
          component: <component_id>
          event: |
            <what happened and what it caused, observationally>
          why: |
            <mechanism and observability status>
          flows: [<flow_id>, ...]
          rate_multipliers:
            <flow_id>: <float >= 0>
            <component_id>.<log_id>: <float >= 0>
          latency_multipliers:
            <flow_id>: {p50: <float >= 0>, p95: <float >= 0>}
          one_shots:
            - ref: <component_id>.<log_id>
              count: <int >= 1>
              hosts: [<host1>, ...]
          manifestation: [<component_id>.<log_id>, ...]

      steady:
        - component: <component_id>
          condition: |
            <persistent broken condition>
          user_impact: |
            <how users or dependents experience it>
          manifestation: [<component_id>.<log_id>, ...]

      feedback_loops:
        - id: <loop_id_snake_case>
          desc: |
            <why the failure sustains or amplifies>

      flows: [<flow_id>, ...]
      manifestation: [<component_id>.<log_id>, ...]

  assumptions:
    - <adaptation / inference / piecewise volume / visibility note>
```

### Scenario rules
- Exactly two phases: normal then failure.
- Total duration must be `2..60` minutes.
- `n.end_min == f.start_min`.
- Normal phase and failure phase must each be at least 1 minute.
- Failure should begin roughly around the middle unless the post-mortem strongly suggests otherwise.
- `events` is required, non-empty, and must contain at least two events:
  - event 1 is the trigger and must start exactly at `f.start_min`,
  - later events represent cascade / worsening / delayed consequences that still leave the scenario ending in failure.
- `steady` is required and describes the stabilized end-state symptom picture.
- `feedback_loops` are explanatory only; they do not emit logs or change rates.

### Event-control rules
- Active rate multipliers start at `1.0` for every failure-state flow and background log.
- Active latency multipliers start at `{p50: 1.0, p95: 1.0}` for every failure-state flow.
- An event control remains active until a later event overrides it.
- Use `rate_multipliers` for:
  - flow entry rates in `flows.f`,
  - background failure-state log rates in `beh.f`.
- Do not use `rate_multipliers` on arbitrary non-background log templates; only failure-state background logs may be keyed by `component_id.log_id`.
- Use `latency_multipliers` for time-varying latency changes in failure flows.
- Use `one_shots` for exact-at-`at_min` operational markers such as deployment, restart, leadership change, config push, rollback marker, or failover completion.
- If a source should be absent early in failure and only appear later, keep it in the `f` model but explicitly suppress it with multiplier `0.0` in earlier events.
- If a source stops completely, drive it to multiplier `0.0` from that event onward unless the narrative explicitly describes partial or gradual continuation.
- Prefer fewer, clearer multiplier changes over many tiny increments.

### Manifestation rules
- `phases.n.manifestation` is required and should contain the baseline markers most useful for recognizing healthy behavior.
- Every `events[].manifestation` is required and should contain the headline logs for that moment.
- Every `steady[].manifestation` is required and should contain the headline logs for the stabilized end-state.
- Keep event and steady manifestations short and diagnostic.
- Phase-level `flows` and `manifestation` lists are references only; they do not emit logs or suppress unlisted sources.
- The optional phase-level failure `manifestation` may be broader, but include only logs that add real diagnostic value.

### Assumptions rules
Both documents must independently include:
- the major adaptations/inferences,
- the normal-phase logs/min estimate,
- the total estimated log count and why it fits the target range.

The scenario assumptions must additionally include:
- the failure-phase piecewise breakdown,
- interval durations,
- effective logs/min after multipliers,
- one-shot counts per event,
- a short explanation of why the most important failure signals are likely visible.

Treat these counts / logs-per-minute estimates as rough order-of-magnitude sanity checks, not exact accounting.

## Hard validity checklist
The final YAML pair must satisfy all of these:
- valid parseable YAML,
- unique IDs and valid references,
- clean placeholder coverage in `msg`,
- valid variable typing,
- both `n` and `f` behavior blocks for every component,
- plausible `to` edges and path connectivity,
- emitted-component sequence must be traversable through `path`,
- flow timing arrays must match flow emit arrays,
- retry settings must be coherent,
- retry-only logs must come from the retrying layer, not downstream layers,
- scenario timings must be contiguous and ordered,
- event controls must reference valid flows / failure-state background logs,
- scenario-referenced logs and flows must actually be emission-backed in the corresponding state (or be one-shots when appropriate),
- manifestations must be emission-backed,
- total estimated log count must be between 20000 and 100000 inclusive.

## Semantic target checklist
The final YAML pair should make all of the following clearly true:

### S1 — Faithful to the post-mortem
The model preserves the post-mortem’s core architecture/roles, failure mechanism, progression, symptoms, impact, and timeline shape.

### S2 — Cross-document meaning consistency
The scenario uses components, flows, and logs in ways that match their modeled meaning.

### S3 — Logical system description
Components, logs, behaviors, and flows form a motivated and coherent system model.

### S4 — Logical scenario
The scenario summary, trigger, phases, events, steady conditions, and references tell a coherent failure story.

### S5 — Assumptions are sufficient
Assumptions explain the important adaptations, traffic/rate reasoning, and visibility/volume reasoning.

### S6 — Complete story
The two documents together leave no important causal or observational gaps.

### S7 — Realistic logs and flows
Templates, levels, variable domains, flow paths, emitted chains, retry behavior, and rate-related fields are realistic and coherent.

### S8 — Failure-phase narrative/control alignment
What the scenario says changes over time is exactly what the event controls realize, and vice versa.

### S9 — Diagnostic sufficiency
A reasonable operator could use the emitted signals over time to understand the progression, identify the root cause, and distinguish this incident from nearby alternatives.

## Correction loop
If the user provides:
- `<previous_system_description>` ... `</previous_system_description>`
- `<previous_scenario>` ... `</previous_scenario>`
- `<verifier_feedback>` ... `</verifier_feedback>`

then you must **repair** the previous output instead of generating a new model from scratch.

Use this repair policy:
1. Read the previous YAMLs carefully.
2. Read every failed requirement and identify the true root cause.
3. Prefer the smallest change set that fixes the issue **without introducing new fragility**.
4. Preserve stable IDs, components, flows, and event structure when possible, but do a broader rewrite if the current design is fundamentally unstable.
5. If an issue in `scenario` is really caused by weak system modeling (or vice versa), repair both documents together.
6. Update assumptions when you make meaningful adaptations.
7. Output full replacements for both documents in the required tags and nothing else.