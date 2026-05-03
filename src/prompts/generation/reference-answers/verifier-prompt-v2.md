# Incident Model to Log Investigation Report Semantic Verification (v8)

## Task
You are given:
- a System Description in `<system_description>`,
- a Scenario in `<scenario>`,
- a generated question in `<question>`,
- a generated answer in `<answer>`.

Your job is to evaluate semantic requirements only: `S1` .. `S9`.

Do **not** perform structural validation already handled elsewhere unless a semantic requirement explicitly depends on the structure.
Assume the YAML is parseable and structurally valid unless a semantic requirement depends on a field’s meaning.

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
  "S7": { "score": 1, "reason": null },
  "S8": { "score": 1, "reason": null },
  "S9": { "score": 1, "reason": null }
}
```

## Failure reason format (mandatory)
For every failing requirement, `reason` must use this exact plain-text format:

- `Issue: <what fails and why>; Location: <exact location(s)>; Fix: <how to fix it>`

If multiple material issues belong under the same requirement, combine them in one string:
- `Issue: 1) ... 2) ...; Location: 1) ... 2) ...; Fix: 1) ... 2) ...`

Prefer the deepest root cause over derivative symptoms. If several observations are consequences of one modeling choice, report only that deepest issue.

The feedback must be:
- specific,
- actionable,
- grounded in concrete locations in `<question>` or `<answer>`,
- high-confidence.

## High-precision evaluation mode
You are being used in a repeated verification setting. Therefore:

1. **Fail only on direct, material problems.**
   - A problem is material if it meaningfully harms question fidelity, log-only plausibility, system reconstruction quality, timeline accuracy, evidence grounding, diagnosis quality, or factual consistency.
   - Do not fail because a different plausible answer would also have worked.
   - Do not fail for stylistic preferences, minor prose improvements, or weak plausibility concerns.

2. **If multiple plausible interpretations exist and the generated text supports at least one reasonable interpretation, pass.**
   - Do not invent stricter assumptions than the incident model states.
   - Do not infer hidden details and then fail the answer for not matching your invented interpretation.

3. **Do not fail on mere possibility.**
   - If an answer statement is plausibly supported by logs plus normal domain knowledge, pass.
   - If a broad variable domain still allows the cited observation to be realistically visible, pass.
   - If a statement is reasonably qualified with uncertainty, prefer passing.

4. **If you cannot point to a concrete contradiction or insufficiency and cite the exact location in `<question>` or `<answer>`, pass.**

5. **Report only high-confidence issues.**
   - Do not pad `reason` with speculative concerns.
   - Prefer one or two strong issues over a long list of weak ones.

6. **Prefer the deepest root cause over derivative symptoms.**
   - If several complaints stem from one deeper modeling or answer-writing mistake, report only that deeper issue.

## General evaluation rules
- Treat the System Description and Scenario as authoritative hidden references.
- Evaluate only the generated `<question>` and `<answer>`.
- IDs and references in the incident model are case-sensitive.
- A log reference is `<component_id>.<log_id>`.
- A flow reference is a `flow_id` from `system_description.flows.<state>.req[].id`.

## Key semantics

### Log-file surface semantics
The downstream agent will analyze a CSV log export. The file has one row per emitted log line with these columns:
- `timestamp`: when the line was emitted,
- `level`: the severity level,
- `message`: the rendered log message text,
- `trace_id`: a request-level correlation id when present,
- `service`: the service that emitted the line,
- `host`: the host that emitted the line.

This schema is hidden reference context for you. The fixed question is intentionally generic and does **not** enumerate these columns. Do not treat that omission as a defect.

### System Description semantics
The System Description gives the hidden ground truth for:
- visible services/components,
- log templates,
- background log emissions,
- request flows,
- retry behavior,
- tracing.

For this task, the important observable consequences are:
- visible log signatures,
- service and host identities,
- request signatures implied by ordered emitted logs,
- retry markers,
- trace correlation.

### Scenario and control semantics
The Scenario gives the hidden ground truth for:
- the normal phase,
- the ordered failure events,
- event timing,
- persistent rate controls,
- persistent latency controls,
- exact-count one-shot markers,
- the stabilized end-state in `steady`.

For this task:
- the answer phases are the baseline plus the ordered failure events only,
- the `steady` section is not a separate answer phase and must be absorbed into the final event section.

#### Rate-control semantics
Failure-state flow rates and background-log rates are **onset baselines**:
- `flows.f.req[].rpm`
- `components[].beh.f.emit[].per_min`

Interpret every failure interval in two layers:
1. the **failure-state baseline layer** — what the source would emit with multiplier `1.0`,
2. the **effective visible layer** — what would actually be visible after applying the currently active multiplier.

Control lifecycle:
- At `scenario.time.phases.f.start_min`, every failure-state source conceptually starts with active multiplier `1.0`.
- Event 1 occurs exactly at `f.start_min`; any `rate_multipliers` it sets replace `1.0` immediately for the interval beginning at `f.start_min`.
- A later event replaces the previously active multiplier only for the sources it names.
- Sources omitted from an event keep their previously active multiplier.
- A source set to `0.0` is fully suppressed from that interval onward until a later event explicitly assigns it a positive multiplier.

Effective visible rate rule:

`effective visible rate in an interval = failure-state baseline × currently active rate multiplier`

Worked examples:
- baseline `rpm = 20`, event 1 sets `0.5` -> that interval is about `10 rpm`
- later event 2 sets `2.0` for the same source -> that later interval is about `40 rpm`, not `20 rpm`
- if event 2 omits the source, it stays about `10 rpm`
- if event 2 sets `0.0` and event 3 later sets `1.5`, the source is absent throughout the event-2 interval and returns at about `30 rpm` in the event-3 interval

When judging whether the answer correctly describes a rate change, compare adjacent intervals by their **effective visible behavior**. Do **not** compare raw hidden multiplier values directly.

#### Latency-control semantics
Failure-state flow latency hints are also **onset baselines**:
- `flows.f.req[].latency_ms`

Interpret every failure interval in two layers:
1. the **failure-state latency baseline** for that flow,
2. the **effective visible end-to-end timing** after applying the currently active latency multiplier pair.

Control lifecycle:
- At `scenario.time.phases.f.start_min`, every failure-state flow conceptually starts with active latency multipliers `p50: 1.0` and `p95: 1.0`.
- Event 1 occurs exactly at `f.start_min`; any `latency_multipliers` it sets replace that default pair immediately for the interval beginning at `f.start_min`.
- A later event replaces the previously active latency pair only for the flows it names.
- A flow omitted from a later event keeps its previously active latency pair.

Effective visible timing rule:

`effective visible end-to-end timing in an interval = failure-state latency hint × currently active latency multiplier pair`

Worked examples:
- failure-state latency hint `[[200, 800], ...]`, event 1 keeps the default pair -> that interval stays at the failure baseline timing
- later event 2 sets `p50: 3.0` and `p95: 2.0` -> that later interval is about three times slower at p50 and about two times slower at p95 relative to the failure-state baseline for that flow
- if a later event omits the flow, those slower timings remain active

When judging whether the answer correctly describes a latency change, compare adjacent intervals by their **effective visible end-to-end behavior**. Do **not** compare raw hidden latency multiplier values directly.

#### One-shot semantics
Event `one_shots` are exact-count markers emitted at the event time. They are discrete evidence items, not persistent rate-based behaviors. They may justify bursty or one-off evidence in an answer, but they do not by themselves justify describing an ongoing interval-wide pattern.

### Phase-interval semantics
Use these answer-phase intervals:
- baseline: `[0, scenario.time.phases.f.start_min)`
- event interval `i`: `[events[i].at_min, events[i+1].at_min)`
- final event interval: `[events[last].at_min, scenario.time.phases.f.end_min)`

The final event interval must absorb the stabilized broken end-state described in `steady`.

### Request-signature semantics
A request signature is the visible ordered sequence of emitted log signatures for one request pattern.

When two flows share the same visible sequence:
- the answer may describe the full sequence once and reference it later,
- it does **not** need to re-list the full path every time,
- later references should note the observable differences such as rate, end-to-end timing, trace usage, or visibly different field values,
- this rule applies across normal/failure variants and also within a single phase if two newly visible flows share the same observable sequence.

Do not penalize the answer for referencing an earlier description of the same visible request signature instead of reprinting it.

### Trace-correlation semantics
If a flow is traced, cross-service correlation through a shared `trace_id` is observable and may be used as evidence, even when the message templates themselves do not contain a trace placeholder. Background logs and non-traced flows do not carry that cross-service trace evidence.

### Reconstruction and diagnosis semantics
The answer may reconstruct:
- system purpose,
- service or component roles,
- interactions between services,
- the single best diagnosis of the incident.

These reconstructions must be:
- plausible from logs plus normal domain knowledge,
- consistent with the System Description and Scenario,
- based primarily on visible service names, host patterns, repeated message signatures, request signatures, and trace-linked interactions,
- explicit about uncertainty when the logs do not fully determine the conclusion.

In `## Reconstructed system` specifically:
- core request/data-path actors should be prioritized over support-plane or incident-response actors,
- monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors should appear as main components only when they are diagnostically central,
- hidden component `name` labels should not be copied unless the same strings are actually visible in service, host, or message text,
- a concrete external dependency should not be materialized from a single opaque token alone,
- inferential roles or dependencies should be phrased with visible hedging rather than as flat fact.

For the final diagnosis:
- `## Primary diagnosis > ### Summary` should contain only the core fault chain,
- amplifiers, mitigation steps, recovery nuance, material alternatives, and unresolved ambiguity belong mainly in `### Reasoning`.

### Certainty-scale semantics
Interpret the required certainty labels exactly as follows:
- `highly likely` — the conclusion is directly visible in the logs and follows from them without extra reasoning about system behavior beyond what the logs explicitly show.
- `moderately supported` — the evidence is only partially visible in the logs, or the conclusion requires ordinary domain knowledge or a bridging inference to connect the observations.
- `speculative` — the evidence is indirect or incomplete, multiple plausible explanations remain, and the answer is choosing the best-supported interpretation.

Evaluate the label on the weakest material inferential step in that `Reasoning` subsection, not on each raw evidence item. When one engineering interpretation is strongly favored but still needs ordinary domain knowledge, prefer `moderately supported` over `speculative`.

## Requirement ownership and de-duplication
Assign each issue to exactly one requirement: the most specific owner.

Use this ownership map:
- **S8** owns factual inconsistencies with the System Description or Scenario, including wrong services, hosts, signatures, request paths, trace usage, retry behavior, timeline facts, visible changes, invented evidence, or materially wrong incident readings.
- **S7** owns unsupported diagnosis, overconfident diagnosis, missing material alternatives or unresolved ambiguity in the final diagnosis, or a diagnosis that does not match the strongest supported interpretation.
- **S6** owns missing or weak grounding, missing evidence linkage, evidence that does not actually support the associated reasoning, missing or inconsistent certainty labels on reasoning claims, and weak explanation of why cited evidence is relevant.
- **S5** owns wrong phase ranges, wrong phase count or order inside the timeline, non-local comparisons, future leakage, failure to absorb the stabilized end-state into the final phase, and other temporal-discipline mistakes.
- **S4** owns weak, implausible, or materially incomplete system reconstruction when the issue is not a raw factual contradiction better owned by S8.
- **S3** owns log-observability and style discipline: hidden-model leakage, internal ids or jargon, raw placeholders, or claims that rely on inaccessible hidden information rather than visible evidence or normal domain knowledge.
- **S2** owns required answer structure and required headings or subsections.
- **S9** owns internal contradictions, misleading summaries, weak narrative flow, or an investigation note that is materially unhelpful despite being otherwise mostly correct.
- **S1** owns question-template fidelity.

**Tie-break order:** `S8 > S7 > S6 > S5 > S4 > S3 > S2 > S9 > S1`

If an issue could fit multiple requirements, assign it only to the highest-priority owner above.

## Semantic requirements

### S1 — Question fidelity
Apply this requirement only to `<question>`.

Pass if the question matches this exact template, character-for-character:

```text
Analyze the provided log file and write an incident investigation report.

Use the logs to reconstruct what system this is, how it normally behaves, when the behavior changes, and the single best diagnosis of the incident. Base the report on evidence visible in the logs plus reasonable domain knowledge. When the logs do not fully determine a conclusion, state the best-supported interpretation and make the uncertainty explicit.

Use exactly this structure:

## Incident synopsis
Give a short bullet list with one bullet per phase in chronological order. Each bullet should name the phase and summarize its main observable state or change in one sentence.

## Reconstructed system
Use exactly these `###` subsections in this order:
- Inferred purpose
- Main services/components
- Main interactions

Describe the system only to the extent the logs support it.
Prioritize the core request or data path. Mention monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors only when they are diagnostically central.
Prefer names and roles that are visible in service names, hostnames, trace-linked interactions, or message text. Do not introduce hidden component labels or concrete external dependencies unless the logs support them.
When a role, dependency, or interaction is inferred rather than explicit, use appropriately hedged language such as `appears to`, `likely`, or `suggests`.

## Timeline
Partition the full log into phases with [inclusive, exclusive) time ranges in minutes from the start of the file. Include the normal baseline, the trigger phase, and each subsequent failure-progression phase. Compare each phase primarily to the immediately preceding phase, but you may also refer explicitly to earlier phases when that helps explain the current one.

Use one `###` heading per phase in chronological order:
- `### Baseline — [start, end)`
- `### Trigger — [start, end)`
- `### Progression 1 — [start, end)`
- `### Progression 2 — [start, end)`
- and so on as needed

Under each phase heading, use exactly these `####` subsections in this order:
- Summary
- Evidence
- Reasoning

For each phase:
- `Summary` should state the main phase-level claim in 2–3 sentences. For non-baseline phases, make the main visible change relative to the immediately preceding phase clear. Do not turn this subsection into an evidence dump.
- `Evidence` should stay anchored to the log surface. Cite the concrete evidence that supports the phase reading: representative log templates with "..." placeholders, request or trace-linked sequences, retry patterns, bursty operational markers, service/host patterns, severity mix, and major appearance/disappearance or rate/timing shifts. For each cited item, say what is directly visible and why it is relevant. Keep `Evidence` close to the log surface rather than turning it into a mini-diagnosis.
- `Reasoning` should explain what the stated evidence means and how it supports the summary. It may use domain knowledge and reasonable speculation, but it must stay tied to stated evidence from the current phase and may also refer explicitly to earlier phases when that materially clarifies the claim. Begin this subsection with exactly one line of the form `Certainty: highly likely`, `Certainty: moderately supported`, or `Certainty: speculative`. Use the certainty label that matches the weakest material inferential step in the subsection. Use `highly likely` when the conclusion is directly visible in the logs and follows from them without extra reasoning beyond what the logs explicitly show. Use `moderately supported` when the evidence is only partially visible or when ordinary domain knowledge or a bridging inference is needed. Use `speculative` when the evidence is indirect or incomplete and multiple plausible explanations remain. In the baseline, use this subsection to infer the normal system structure and behavior. In later phases, use it to explain what most likely changed operationally, how that affects the system, and what it says about the incident progression.

## Primary diagnosis
Use exactly these `###` subsections in this order:
- Summary
- Evidence
- Reasoning

For `## Primary diagnosis`:
- `Summary` should state only the core fault chain in plain terms, preferably in 1–2 sentences. Keep amplifiers, mitigation steps, and recovery detail out of this subsection unless they are part of the core diagnosis itself.
- `Evidence` should cite the main cross-phase evidence that supports that diagnosis. For each cited item, say what is directly visible and why it is relevant.
- `Reasoning` should explain why that evidence makes this diagnosis the best-supported interpretation. Put amplifiers, mitigation nuance, recovery detail, material alternatives, and unresolved ambiguities here when they matter, and begin with exactly one certainty line using the same scale as above. Use the certainty label that matches the weakest material inferential step in this subsection.

Additional requirements:
- When describing log templates, replace variable parts with "..."
- If the same observable request-flow signature appears again later, reference the earlier description and describe only the observable differences
- Use qualitative language for rate and latency changes unless "at least 2x" is warranted
- Write like an engineer producing a handoff-quality investigation note
```

Fail only on concrete deviations from the required wording.

### S2 — Answer structure and required section coverage
Apply this requirement only to `<answer>`.

Pass if all of the following are true:
- the answer uses the exact required top-level headings in this order:
  - `## Incident synopsis`
  - `## Reconstructed system`
  - `## Timeline`
  - `## Primary diagnosis`
- `## Incident synopsis` contains a short bullet list with one bullet per phase in chronological order,
- `## Reconstructed system` contains the exact required `###` subsections in this order:
  - `### Inferred purpose`
  - `### Main services/components`
  - `### Main interactions`
- `## Timeline` contains exactly one phase section for the baseline and one for each failure event, in scenario order,
- each phase heading follows the requested label pattern and includes the phase time range,
- each phase contains the exact required `####` subsections in this order:
  - `#### Summary`
  - `#### Evidence`
  - `#### Reasoning`
- within each phase, `#### Summary` is a short phase-level claim rather than a mere evidence list,
- within each phase, `#### Evidence` stays anchored to direct log-visible support for that interval, says what is directly visible, explains why it is relevant, and does not do the main inferential work reserved for `#### Reasoning`,
- within each phase, `#### Reasoning` explains what the stated evidence means and how it supports the phase summary; in the baseline it infers normal structure and behavior, and in later phases it explains what changed, how it affects the system, and how it advances the incident,
- every `#### Reasoning` subsection begins with exactly one `Certainty:` line using one of `highly likely`, `moderately supported`, or `speculative`,
- `## Primary diagnosis` contains the exact required `###` subsections in this order:
  - `### Summary`
  - `### Evidence`
  - `### Reasoning`
- `## Primary diagnosis > ### Summary` states one best diagnosis in plain terms and stays focused on the core fault chain rather than recapping the whole incident,
- `## Primary diagnosis > ### Evidence` says what is directly visible and why it is relevant,
- `## Primary diagnosis > ### Reasoning` begins with exactly one `Certainty:` line using one of `highly likely`, `moderately supported`, or `speculative`,
- no separate steady-state phase is created.
Do not fail merely because the answer uses prose instead of bullets inside a required subsection, or vice versa, if the required sectioning and coverage are still present.

### S3 — Log-observability, style, and no hidden-model leakage
Apply this requirement only to `<answer>`.

Pass if:
- the answer reads like an investigation report written from logs rather than a YAML explanation,
- it does not mention hidden model constructs such as `manifestation`, `rate multiplier`, `latency multiplier`, `one-shot`, `flow id`, `log id`, `state_vars`, or similar benchmark machinery,
- it does not use internal component ids or other hidden identifiers unless those strings are truly visible in the log file,
- it does not copy hidden component `name` labels unless those same labels are actually visible in service, host, or message text,
- it does not turn an opaque field token or message fragment into a concrete named dependency unless that dependency is visibly supported by the logs,
- quoted log signatures render variable parts as `"..."` rather than raw placeholders like `{status}`,
- concrete placeholder values are mentioned only when explicitly justified by the incident model or highly likely to be visible,
- claims stay within what could reasonably be observed or inferred from logs plus normal domain knowledge.

Fail S3 only on direct observability or style violations, not on general factual errors better owned by S8.

### S4 — System reconstruction quality
Apply this requirement only to `<answer>`.

Pass if:
- `## Reconstructed system` gives a plausible, log-supported account of what the system does,
- the inferred services/components and their roles are useful and consistent with visible evidence,
- the reconstruction prioritizes the core request/data-path actors and interactions rather than inventorying every visible support-plane or incident-response actor,
- monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors appear as main components only when they are diagnostically central,
- the described interactions or request paths are plausible from visible request signatures and trace patterns,
- inferential roles or dependencies are appropriately hedged rather than presented as flat fact,
- the reconstruction avoids deep speculation beyond what the logs could support.

Pass when the answer chooses any reasonable abstraction supported by the logs, even if another phrasing could also have worked.
Fail only when the reconstruction is materially implausible, materially incomplete, over-bloated with peripheral actors, or clearly inconsistent with the visible evidence.

### S5 — Timeline reconstruction and temporal discipline
Apply this requirement only to `<answer>`.

Pass if:
- the baseline and every failure event are covered in correct chronological order,
- each phase heading uses the correct `[inclusive, exclusive)` minute range,
- each phase describes only evidence available within its interval,
- each phase uses the immediately preceding phase as the default comparison baseline, while allowing explicit references to earlier phases when they materially clarify the current phase or diagnosis,
- the answer does not foreshadow later-only evidence or use future information too early,
- the final phase extends through `scenario.time.phases.f.end_min` and captures the stabilized broken state without creating a separate phase,
- described phase-to-phase changes are judged from effective visible interval behavior, not from raw hidden multiplier values.

Past-cause inference is allowed if the answer makes clear that the diagnosis is being made from evidence available by that phase. Explicit references to earlier phases are allowed when they clarify the current phase or diagnosis, but they must not replace the current interval as the main evidentiary anchor. Future-only evidence is not allowed to justify an earlier claim.

### S6 — Argument grounding, evidence quality, and certainty discipline
Apply this requirement only to `<answer>`.

Pass if:
- `#### Evidence` sections and `## Primary diagnosis > ### Evidence` cite the most relevant log signatures, request flows, trace correlations, service/host patterns, retry markers, bursty operational markers, or major rate/latency shifts,
- each cited evidence item or local evidence group makes clear what is directly visible and why it is relevant, rather than merely listing artifacts from the logs,
- `#### Evidence` stays mostly on the log surface and does not do the main inferential work that belongs in `#### Reasoning`,
- `#### Reasoning` sections and `## Primary diagnosis > ### Reasoning` are meaningfully derived from the stated `Evidence`, rather than introducing unsupported new claims,
- `Evidence` and `Reasoning` remain materially distinct rather than repeating the same mini-argument with only superficial rewording,
- every `Reasoning` subsection begins with exactly one explicit `Certainty:` line using one of `highly likely`, `moderately supported`, or `speculative`,
- the reasoning that follows is explicitly tied to stated evidence from the same section, to clearly signposted domain-knowledge inference, or to explicit earlier-phase references that materially clarify the current claim,
- when domain knowledge or reasonable speculation is used, that fact is explicitly signposted rather than smuggled in as direct observation,
- the chosen certainty labels are broadly appropriate to the strength of the stated support, match the dedicated certainty-scale definitions above, and reflect the weakest material inferential step in the subsection,
- when the same observable request signature reappears, referencing the earlier description and naming only the observable differences is treated as sufficient grounding.

Fail S6 only when the answer lacks material evidence, fails to explain why the evidence matters, materially blurs Evidence and Reasoning, makes reasoning leaps that are not tied to any stated evidence or clearly signposted inference, or omits/misuses the required `Certainty:` lines.

### S7 — Diagnosis quality and uncertainty calibration
Apply this requirement only to `<answer>`.

Pass if:
- `## Primary diagnosis > ### Summary` states one single best diagnosis,
- that diagnosis summary stays focused on the core fault chain rather than recapping the whole incident,
- that diagnosis matches the strongest supported reading of the incident,
- `### Evidence` provides the main cross-phase evidence for that diagnosis,
- `### Reasoning` explains why that evidence makes this the best-supported interpretation and carries the amplifiers, mitigation nuance, recovery detail, material alternatives, or unresolved ambiguity when those matter,
- `### Reasoning` begins with exactly one `Certainty:` line using one of `highly likely`, `moderately supported`, or `speculative`,
- `### Reasoning` acknowledges material alternatives or unresolved ambiguity when they matter,
- the answer does not present log-invisible specifics as certain when they are only plausible,
- the overall level of commitment is appropriate: decisive when the logs strongly support one diagnosis, and more qualified when multiple explanations remain plausible; the diagnosis-level certainty line should also match the dedicated certainty-scale definitions above and reflect the weakest material inferential step in the diagnosis reasoning.

Fail S7 when the diagnosis is unsupported, overconfident, evasive, missing the required diagnosis-level `Certainty:` line, overly sprawling in `### Summary`, or missing material alternatives or unresolved ambiguity that should have been acknowledged.

### S8 — Factual consistency with the incident model
Apply this requirement only to `<answer>`.

Pass if:
- services, hosts, log signatures, request signatures, retry behavior, and trace usage are consistent with the System Description,
- phase ranges, visible changes, bursty markers, and major rate or latency shifts are consistent with the Scenario,
- the answer does not invent components, services, hosts, log signatures, failure stages, or evidence,
- the system reconstruction, timeline narrative, and diagnosis are all consistent with the incident model’s causal core,
- same-signature request paths are handled consistently with the System Description’s visible emitted sequences.

Do not fail S8 merely because the answer chooses a different but still reasonable abstraction of a component role or system purpose. Fail only on direct factual contradiction, invented content, or a materially wrong incident reading.

### S9 — Coherence and investigation usefulness
Apply this requirement only to `<answer>`.

Pass if:
- each section summary matches the details that follow,
- there are no internal contradictions within a phase or across adjacent phases,
- the report flows naturally from system reconstruction through timeline to final diagnosis,
- the answer highlights the incident-defining evidence instead of drowning it in irrelevant detail,
- the result reads like a useful handoff-quality investigation note.

Use S9 only for genuine coherence or usefulness failures, not for issues better owned by S2–S8.

## Evaluation procedure
1. Read the System Description, Scenario, `<question>`, and `<answer>`.
2. Evaluate `S1` .. `S9` using the ownership rules above.
3. For each requirement:
   - pass unless there is a direct, material, high-confidence issue,
   - if failing, cite the exact location,
   - provide an actionable fix.
4. Return raw JSON only.
