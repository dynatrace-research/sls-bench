# Incident Model to Log Investigation Report Generation (v8)

## Task
Generate one benchmark problem for evaluating log analytics agents from the provided incident model inputs.

The problem consists of:
1. a fixed question,
2. a ground-truth answer.

The task you are constructing asks the agent to read a log file and produce an **incident investigation report** that:
- reconstructs what system the logs came from,
- infers the main visible services or components and how they interact,
- partitions the timeline into stable phases,
- describes what changes from phase to phase,
- gives one single best diagnosis of the incident,
- supports that diagnosis with evidence and explicit uncertainty.

You will receive:
- a System Description in `<system_description>` tags,
- a Scenario in `<scenario>` tags.

If only these two objects are provided, generate the benchmark problem from scratch.

If the user also provides:
- `<previous_question>` ... `</previous_question>`
- `<previous_answer>` ... `</previous_answer>`
- `<verifier_feedback>` ... `</verifier_feedback>`

then you must repair the previous output instead of generating a new one from scratch.

## Output format
Return exactly these two tagged objects and nothing else:

<question>
...
</question>

<answer>
...
</answer>

The content inside the tags must be raw markdown. Do not use code fences. Do not add prose outside the tags.

## Operating principles
1. **One incident, one benchmark problem.** Generate exactly one coherent question and one answer for the provided incident.
2. **The question text is fixed.** Copy it exactly. Do not add, remove, or paraphrase any wording.
3. **The fixed question must stay generic.** Do not add schema descriptions, system-specific hints, time-boundary hints, hidden benchmark terminology, or any other extra guidance.
4. **Every user-facing instruction must make sense to a log-only agent.** The question is the only instruction the downstream agent will see besides the log file, so every sentence in it must be meaningful from that perspective.
5. **Treat the log file as the observable surface.** The answer must read as if it was derived from the log file alone, plus standard domain knowledge.
6. **System reconstruction is mandatory but bounded.** Infer the system purpose, main services/components, and interactions only to the extent the logs support them. Prioritize the core request or data path, and keep monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors secondary unless they are diagnostically central.
7. **Use only the System Description and Scenario as hidden references.** Internally derive the visible phase ledger, the reconstruction, and the diagnosis from them.
8. **The answer phases are the baseline plus the failure events only.** Do not create a separate steady-state phase. Absorb the stabilized broken end-state into the final event section.
9. **Use the immediately preceding phase as the default comparison baseline.** Do not skip intermediate phases when stating what changed, but you may also refer explicitly to earlier phases when that materially clarifies the current phase or the diagnosis.
10. **Use a clean argument structure.** `Summary` states the phase-level claim, `Evidence` records what is directly visible and why it is relevant, and `Reasoning` explains what that evidence means and how strongly it supports the claim. Keep `Evidence` and `Reasoning` distinct rather than repeating the same mini-argument in both.
11. **Use one single best diagnosis and one consistent uncertainty scale.** The answer should commit to the most defensible overall diagnosis, and every `Reasoning` subsection should begin with exactly one certainty line: `Certainty: highly likely`, `Certainty: moderately supported`, or `Certainty: speculative`. Apply that label to the weakest material inferential step in the subsection.
12. **Reasonable domain knowledge and bounded speculation are allowed.** The answer may interpret the logs, but it must not present hidden facts as certain when the logs do not support them. Any use of domain knowledge or reasonable speculation must be explicitly signposted in `Reasoning` and tied back to stated evidence.
13. **Respect observable-signature identity.** Treat two request flows as the same observable request signature when their visible emitted log sequence is the same. Do not redescribe the same visible path as new merely because an internal flow id changed.
14. **Prefer visible names over hidden ids.** Use service names, hostnames, trace correlation, and quoted log templates rather than internal component ids, flow ids, log ids, or hidden component `name` labels.
15. **Do not materialize hidden dependencies from opaque tokens.** A field value or message fragment may suggest a technology or downstream, but do not turn it into a concrete named dependency unless the logs provide repeated visible support for that interpretation.
16. **Hedge inferred reconstruction claims.** When a role, dependency, or interaction is inferred rather than explicit, phrase it as inferred (`appears to`, `likely`, `suggests`) instead of as flat fact.
17. **Quote templates, not incidental concrete values.** When describing a log signature, replace variable parts with `"..."` unless a concrete value is explicitly justified by the incident model, uniquely determined by the modeled domain, or highly likely to be visible and diagnostically important.
18. **Keep quantitative language honest.** Use qualitative descriptions such as “increases sharply”, “drops”, or “is at least 2x higher”. Do not report raw multipliers or exact hidden counts unless they would truly be inferable from the logs.
19. **Prefer the signals that carry the narrative.** Include the important recurring signatures, representative request flows, retry patterns, bursty operational markers, trace correlations, and major rate or latency shifts, but avoid exhaustive low-value listings.
20. **Flexible formatting inside subsections is allowed.** Inside `Summary`, `Evidence`, `Reasoning`, and the diagnosis subsections, use prose, bullets, or a compact mix as long as the required content is present and clear.
21. **Do not leak hidden modeling machinery.** Never mention YAML fields, internal ids, rate multipliers, latency multipliers, one-shots, or other benchmark-generation terminology in the final output.
22. **During repair, prefer local fixes.** Preserve correct structure, wording, and valid content unless a deeper rewrite is needed.

## Key semantics

### Log-file surface semantics
The downstream agent will analyze a log file that is a CSV export. The file has one row per emitted log line with these columns:
- `timestamp`: when the line was emitted,
- `level`: the severity level,
- `message`: the rendered log message text,
- `trace_id`: a request-level correlation id when present,
- `service`: the service that emitted the line,
- `host`: the host that emitted the line.

This schema is hidden context for you. Do **not** repeat it in the fixed question. The answer must still behave as though it had access only to this observable surface plus ordinary engineering knowledge.

### System Description semantics
The System Description models the static system:
- components and their visible identities,
- log templates and placeholder variables,
- background log emissions in normal and failure states,
- request flows in normal and failure states,
- retry behavior,
- distributed tracing configuration.

Relevant observables for this task are:
- visible log signatures implied by the log templates,
- service and host identities on emitted rows,
- request signatures implied by the visible ordered emitted logs,
- retry markers,
- cross-service trace correlation when a flow is traced.

### Scenario and control semantics
The Scenario models the incident timeline:
- one normal phase,
- one failure phase containing ordered events,
- per-event persistent rate controls,
- per-event persistent latency controls,
- exact-count one-shot markers,
- a stabilized end-state described in `steady`.

For this task:
- the answer phases are the normal phase and the ordered failure events only,
- the `steady` section is reference context and must be absorbed into the final event section,
- the event timeline determines the true phase boundaries,
- the event narrative (`summary`, `desc`, `event`, `why`, `feedback_loops`) determines what is directly visible, partially visible, or mainly inferential.

#### Rate-control semantics
Failure-state flow rates and background-log rates in the System Description are **onset baselines**:
- `flows.f.req[].rpm`
- `components[].beh.f.emit[].per_min`

Think in two layers for every failure interval:
1. the **failure-state baseline layer** — what the source would emit with multiplier `1.0`,
2. the **effective visible layer** — what would actually be visible in that interval after applying the currently active multiplier.

Control lifecycle:
- At `scenario.time.phases.f.start_min`, every failure-state source conceptually starts with active multiplier `1.0`.
- Event 1 happens exactly at `f.start_min`; any `rate_multipliers` it sets replace `1.0` immediately for the interval beginning at `f.start_min`.
- A later event replaces the previously active multiplier only for the sources it names.
- Sources omitted from an event keep their previously active multiplier.
- A source set to `0.0` is fully suppressed from that event interval onward until a later event explicitly assigns it a positive multiplier.

Effective visible rate rule:

`effective visible rate in an interval = failure-state baseline × currently active rate multiplier`

Worked examples:
- baseline `rpm = 20`, event 1 sets `0.5` -> that interval is about `10 rpm`
- later event 2 sets `2.0` for the same source -> that later interval is about `40 rpm`, not `20 rpm`
- if event 2 omits the source, it stays about `10 rpm`
- if event 2 sets `0.0` and event 3 later sets `1.5`, the source is absent throughout the event-2 interval and returns at about `30 rpm` in the event-3 interval

Answer-writing consequence:
- baseline-to-trigger comparisons are between what is actually visible in the baseline interval and what is actually visible in the trigger interval,
- later phase comparisons are always between adjacent event intervals,
- never describe a rate change from raw multiplier deltas alone; derive it from effective visible interval behavior.

#### Latency-control semantics
Failure-state flow latency hints in the System Description are also **onset baselines**:
- `flows.f.req[].latency_ms`

Think in the same two layers:
1. the **failure-state latency baseline** for that flow,
2. the **effective visible end-to-end timing** after applying the currently active latency multiplier pair.

Control lifecycle:
- At `scenario.time.phases.f.start_min`, every failure-state flow conceptually starts with active latency multipliers `p50: 1.0` and `p95: 1.0`.
- Event 1 happens exactly at `f.start_min`; any `latency_multipliers` it sets replace that default pair immediately for the interval beginning at `f.start_min`.
- A later event replaces the previously active latency pair only for the flows it names.
- A flow omitted from a later event keeps its previously active latency pair.

Effective visible timing rule:

`effective visible end-to-end timing in an interval = failure-state latency hint × currently active latency multiplier pair`

Worked examples:
- failure-state latency hint `[[200, 800], ...]`, event 1 keeps the default pair -> that interval stays at the failure baseline timing
- later event 2 sets `p50: 3.0` and `p95: 2.0` -> that later interval is about three times slower at p50 and about two times slower at p95 relative to the failure-state baseline for that flow
- if a later event omits the flow, those slower timings remain active

Answer-writing consequence:
- describe latency changes using the **effective visible end-to-end behavior** of adjacent intervals,
- do not compare raw latency multiplier values directly,
- do not infer concrete numeric latency fields inside messages from these controls alone unless the log content would actually justify them.

#### One-shot semantics
Event `one_shots` are exact-count markers emitted at the event time. They are discrete evidence items, not persistent rate-based behaviors. In the answer they may show up as bursty or one-off operational markers, but they must not be described as ongoing interval-wide behavior unless the logs support that.

### Phase-interval semantics
Derive the answer phases as follows:
- baseline interval: `[0, scenario.time.phases.f.start_min)`
- event interval `i`: `[events[i].at_min, events[i+1].at_min)`
- final event interval: `[events[last].at_min, scenario.time.phases.f.end_min)`

The final event interval must absorb the stabilized broken end-state described in `steady`.

### Observable-signature semantics
A **visible log signature** is a service-visible message template with variable parts abstracted to `"..."`.

A **request signature** is the visible ordered sequence of emitted log signatures for one request pattern, including retry markers when they are part of the visible pattern.

When two flows have the same visible request signature:
- describe the full signature only on its first appearance,
- later references should point back to the earlier description and mention only the observable differences such as rate, end-to-end timing, trace usage, or visible field-value changes,
- this rule applies across normal/failure variants and also within a single phase if two newly visible flows share the same observable sequence.

### Trace-correlation semantics
If a flow is traced, a shared `trace_id` across the emitted rows of that request pattern is observable evidence and may be used in the answer, even when the message templates themselves do not contain a trace placeholder. Background logs and non-traced flows do not provide that cross-service trace evidence.

### Reconstruction semantics
In `## Reconstructed system`:
- prefer visible service names, host patterns, repeated message signatures, and trace-linked interactions as the basis for reconstruction,
- prioritize the core request or data path,
- mention monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors only when they are diagnostically central,
- do not copy hidden component `name` labels unless the same strings are actually visible in service, host, or message text,
- do not materialize a concrete external dependency from a single opaque token alone,
- hedge claims that remain inferential.

### Certainty-scale semantics
Use the three certainty labels exactly as follows:
- `highly likely` — the conclusion is directly visible in the logs and follows from them without needing extra reasoning about system behavior beyond what the logs explicitly show.
- `moderately supported` — the evidence is only partially visible in the logs, or the conclusion requires ordinary domain knowledge or a bridging inference to connect the observations.
- `speculative` — the evidence is indirect or incomplete, multiple plausible explanations remain, and the answer is choosing the best-supported interpretation.

Apply the label to the weakest material inferential step in that `Reasoning` subsection, not to each raw evidence item. When the logs strongly support one engineering interpretation but still require ordinary domain knowledge, prefer `moderately supported` over `speculative`.

## Required internal construction procedure (do not output these notes)

### Step 1 — Extract the incident backbone
Internally write a compact incident backbone:
- what the system appears to do,
- what the healthy baseline looks like in logs,
- what changes at the trigger interval,
- what each later interval adds, removes, or worsens,
- what the final degraded state looks like by the end of the last event,
- which parts of the story are directly visible versus reconstructed or inferred.

### Step 2 — Build a visible-system ledger
From the System Description, internally derive:
- the visible services or components and their likely roles,
- which actors are core request/data-path actors versus supporting or incident-response actors,
- the recurring background log signatures in normal operation,
- the important request signatures in normal and failure states,
- visible retry markers,
- useful service/host patterns,
- useful trace-correlation patterns,
- the visible clues that can justify each reconstructed role or interaction,
- which roles or dependencies remain inferential and therefore need hedged phrasing.

Keep the reconstruction compact and tied to visible evidence. Do not infer architecture that the logs would not realistically support. Do not copy hidden component labels into user-facing prose unless they are actually visible. Do not promote peripheral support-plane actors into the main component list unless they are diagnostically central.

### Step 3 — Build the effective interval ledger
Using the Scenario, derive the answer phases and the effective visible behavior in each interval:
- exact interval boundaries,
- active background behaviors,
- active request signatures,
- effective visible rates,
- effective visible end-to-end timing,
- bursty or operational markers,
- the stabilized end-state content that must be absorbed into the final event interval.

### Step 4 — Build the observable delta ledger
For each phase, internally determine:
- what is newly visible,
- what disappears,
- what clearly increases or decreases,
- what clearly slows down or speeds up,
- which request signatures are continuations of earlier ones,
- which evidence items are the most representative and diagnostically useful.

Use the immediately preceding phase as the default comparison baseline. Additional explicit references to earlier phases are allowed when they materially clarify the current phase or diagnosis.

### Step 5 — Build the reconstruction and diagnosis ledger
Internally derive:
- the best-supported system-purpose reconstruction,
- the best-supported core service/component role reconstruction,
- the best-supported core interaction picture,
- which support-plane actors, if any, are diagnostically central enough to mention,
- which reconstruction claims are directly visible versus inferred and therefore need hedging,
- the single best overall incident diagnosis,
- the core fault chain that belongs in diagnosis `Summary`,
- any amplifiers, mitigation steps, or end-state nuance that belong in diagnosis `Reasoning`,
- the main evidence that supports that diagnosis,
- the major uncertainties that remain if the logs do not fully determine the explanation.

Use scenario `why` and `feedback_loops` only as hidden guidance for what is diagnostically legitimate. Do not expose hidden scenario language that the logs could not support.

### Step 6 — Draft the exact question
Construct the `<question>` by copying the following template exactly and changing nothing.

Use this exact question template:

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

### Step 7 — Draft the answer
Write the `<answer>` as if an expert engineer analyzed the log file directly.

The answer must:
- follow the exact section and subsection structure requested by the question,
- begin with the required incident-synopsis bullet list, with one bullet per phase in chronological order,
- reconstruct the system purpose, main services/components, and interactions only to the extent the logs support that reconstruction, prioritizing the core request/data path and keeping support-plane actors secondary unless they are diagnostically central,
- use visible names and clues rather than hidden component labels or opaque dependency guesses, and hedge reconstruction claims that remain inferential,
- cover the timeline in order: baseline, trigger, progression phases,
- include the exact `[inclusive, exclusive)` minute range in each phase heading,
- describe each phase primarily relative to the immediately preceding phase; additional explicit references to earlier phases are allowed when they materially clarify the current phase or diagnosis,
- make each `#### Summary` a compact phase-level claim rather than a list of evidence,
- make each `#### Evidence` stay anchored to the direct log surface for that interval: representative signatures, request or retry patterns, service or host participation, severity mix, bursty operational markers, and major rate or end-to-end timing behavior,
- make each `#### Evidence` explicitly say what is directly visible and why each cited item is relevant, without doing the main inferential work that belongs in `#### Reasoning`,
- make each `#### Reasoning` explain what the stated evidence means and how it supports the phase summary, using domain knowledge or reasonable speculation only when explicitly signposted and tied back to that evidence; it may also refer explicitly to earlier phases when that materially clarifies the current claim,
- ensure every `#### Reasoning` subsection begins with exactly one certainty line: `Certainty: highly likely`, `Certainty: moderately supported`, or `Certainty: speculative`, chosen to match the weakest material inferential step in that subsection,
- use the same-signature rule for request-signature descriptions,
- commit to one best diagnosis in the final section,
- make `## Primary diagnosis` use the same `Summary` / `Evidence` / `Reasoning` structure, with `### Summary` restricted to the core fault chain and amplifiers, mitigation nuance, recovery detail, alternatives, and residual ambiguity handled in `### Reasoning`,
- use qualitative rate or latency language unless “at least 2x” is warranted,
- avoid internal model terminology and hidden identifiers,
- avoid raw placeholders like `{status}` and instead use `"..."`.

### Step 8 — Final self-audit before writing the answer
Before producing the final output, internally confirm all of the following:

**A. Question fidelity**
- The question matches the exact template above.
- No extra wording, schema explanation, or hidden benchmark terminology was added.

**B. Reconstruction quality**
- The answer reconstructs the system purpose, visible services/components, and main interactions only to the extent the logs support that.
- Core request/data-path actors are prioritized over peripheral support-plane actors.
- Monitoring, probes, operator tooling, deployment or restore tooling, and other support-plane actors are mentioned only when diagnostically central.
- Hidden component labels are not copied into the answer unless they are actually visible.
- Roles or dependencies that remain inferential are phrased with appropriate hedging.

**C. Timeline quality**
- The answer covers the baseline and every failure event in correct order.
- The final event section extends through `scenario.time.phases.f.end_min` and absorbs the stabilized broken end-state.
- Each phase uses the immediately preceding phase as the default comparison baseline, while allowing explicit references to earlier phases when they materially clarify the current phase or diagnosis.

**D. Effective-control reasoning**
- Phase-to-phase rate and latency descriptions are based on effective visible interval behavior, not on raw hidden multipliers.
- Suppressed sources stay absent until later reactivated.
- Unmentioned sources keep their current active behavior.

**E. Evidence and diagnosis**
- Each phase uses the three timeline subsections distinctly: `Summary` states the phase-level claim, `Evidence` gives direct log-surface observations and why they are relevant, and `Reasoning` explains what that evidence means.
- `Evidence` and `Reasoning` are materially distinct rather than repetitive.
- Every `Reasoning` subsection begins with exactly one certainty line — `Certainty: highly likely`, `Certainty: moderately supported`, or `Certainty: speculative` — and that label matches the weakest material inferential step in the subsection.
- The final diagnosis uses the same `Summary` / `Evidence` / `Reasoning` pattern, keeps `### Summary` focused on the core fault chain, handles amplifiers/mitigation/recovery nuance and any material alternatives or unresolved ambiguity inside `### Reasoning`, and commits to one best diagnosis rather than offering an unranked list.

**F. Log-only plausibility**
- The answer reads like it came from the CSV log surface plus domain knowledge.
- No hidden ids, YAML language, or benchmark-generation terms leak into the answer.
- Log templates use `"..."` for variable parts unless a concrete value is clearly justified.

## Correction loop
If the user provides:
- `<previous_question>` ... `</previous_question>`
- `<previous_answer>` ... `</previous_answer>`
- `<verifier_feedback>` ... `</verifier_feedback>`

then you must **repair** the previous output instead of generating a new one from scratch.

Use this repair policy:
1. Read the previous question and answer carefully.
2. Read every failed requirement and identify the true root cause.
3. Preserve the fixed question if it is already correct.
4. Prefer the smallest change set that fixes the issue without introducing new ambiguity.
5. Preserve correct answer structure and valid content unless a deeper rewrite is needed.
6. If a problem in the answer is really caused by a misunderstanding of the question template, repair both together.
7. Output full replacements for both tagged objects and nothing else.
