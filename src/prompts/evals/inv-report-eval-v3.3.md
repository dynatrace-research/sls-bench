You are evaluating a predicted answer (PD) against a ground-truth answer (GT) for the same fixed question.

Inputs:
1) the question
2) the GT answer
3) the PD answer

Goal:
Judge how well the PD recovers the GT's incident story, diagnosis, phase progression, and required scaffold.

Core principles:
- Compare meaning, not wording.
- Use the question only for the required structure, section names, and certainty scale. Use the GT for the target incident story, chronology, diagnosis, and hedge posture.
- Judge only from the question, GT, and PD. Do not use outside information.
- The GT defines the required qualitative story, not a maximum level of detail.
- The overall verdict is a hybrid of GT-faithfulness and handoff usefulness: the PD should recover the same incident understanding as the GT and be at least comparably useful as a recovery of that GT.
- Extra log-surface specificity in the PD is allowed. Do not penalize exact values, IDs, counts, hostnames, timestamps, log template fragments, or similar details just because the GT states them more qualitatively.
- Since you do not see the logs, do not treat omission from the GT by itself as lack of support. A PD exactness claim is acceptable when the PD itself ties it to stated observations and it stays within the GT's qualitative story.
- Judge those specifics under `support_alignment`, not `uncertainty`. Ask whether the PD itself presents concrete support for the specific claim and whether that claim stays within the GT's qualitative story. Penalize only if the specific claim conflicts with the GT, is unsupported even within the PD's own stated evidence, or is used to claim a more specific trigger, key dependency direction, diagnosis, scope, severity, or certainty than the GT supports.
- Treat an issue as material when it changes any of: the primary system under investigation, the trigger, a key dependency direction, a major phase-to-phase change, the diagnosis, the claimed scope/severity, or the hedge posture on a core unresolved issue.
- Keep structure penalties separate from semantic penalties.
- Do not double-penalize one root problem across many dimensions more than necessary.
- Prefer one primary semantic home for a mismatch:
  - If the PD introduces a different causal mechanism, score that mainly under `diagnosis`.
  - If the PD keeps the same fault chain but chooses one GT-hedged initiating option too confidently, score that mainly under `uncertainty`.
  - If the PD keeps the same story but adds more exact numbers or local sequencing, score that mainly under `support_alignment`.
- A local ordering difference inside the same GT phase is usually not material by itself. Treat it as at most `support_alignment = partial_minor` unless it changes the trigger or causal reading.
- A contradiction about the final observable state at the end of the log is material. If the GT says degradation persists through the end and the PD says the system or key components have recovered, score that under `timeline_progression` (and under `diagnosis` only if it changes the core fault chain).

Evaluation workflow:
1) Internally identify from the GT:
   - the primary system under investigation,
   - the baseline,
   - the trigger,
   - the major progression changes,
   - the primary diagnosis,
   - any GT-explicit unresolved core uncertainty about the trigger, key dependency direction, or diagnosis,
   - and the GT's final observable end-state.
2) Score each dimension using the four-level scale below.
3) Record only the deepest independent mismatches and only material extras.
4) Determine the strongest eligible `overall_verdict` using the verdict meanings, hard verdict rules, caps, and exclusions below.
5) Before emitting JSON, verify that the chosen `overall_verdict` satisfies all of its eligibility conditions and none of its exclusion rules. If not, step down to the strongest label that does.

Dimension scale:
Use exactly one verdict per dimension.
- `yes`: matches cleanly.
- `partial_minor`: mostly right; only a local or non-central problem remains.
- `partial_major`: a material but bounded problem; the dimension is only partly recovered.
- `no`: materially wrong, absent, or replaced by a different story.

Dimension definitions:
- `structure`: scaffold compliance only. Judge required headings/subsections, certainty lines, and one synopsis bullet per GT phase. Do not use this dimension for semantic correctness.
  - A single surplus or missing synopsis bullet caused by a GT-preserving phase split/merge is usually `partial_minor`, not `partial_major`.
  - Use `partial_major` only when scaffold noncompliance is broader: multiple required headings/subsections are absent, certainty lines are materially wrong/missing, or the phase scaffold is substantially incomplete.
- `system_reconstruction`: whether the PD recovers the same main system purpose, actors, and interactions.
  - Do not lower this dimension merely because the PD adds plausible product-shape color or concrete host/table names that do not change the main system.
  - Use `partial_major` or `no` if the PD recenters a downstream dependent as the main system, swaps the primary system under investigation, or reverses a key dependency direction in a way that becomes a materially wrong premise for the analysis.
- `timeline_structure`: whether phase count, order, and ranges match closely enough.
  - Phase-boundary drift within ±0.25 minutes is acceptable.
  - GT-preserving merges or splits are usually `partial_minor` or `partial_major`, not `no`.
  - A degenerate or zero-width phase is a minor structure defect.
  - Use `partial_major` only when a distinct GT phase can no longer be cleanly located, or when a merge/split obscures a diagnostically important state transition.
- `timeline_progression`: whether the PD recovers the same baseline, trigger, and major phase-to-phase changes.
  - Slight mistiming or GT-preserving phase merges/splits that keep the same progression are usually `partial_minor`.
  - Missing or materially changing a major progression step is `partial_major` or `no`.
  - Contradicting the GT's end-state (for example, GT says key nodes remain degraded through the end while PD says they recovered) is usually at least `partial_major`.
- `diagnosis`: whether the PD recovers the same core fault chain.
  - A more specific restatement is acceptable only if it stays within the GT's diagnosis and does not pick a different unresolved mechanism as a required new causal step.
  - If the GT leaves the initiating mechanism unresolved but the PD selects one GT-plausible option while preserving the same core chain, keep `diagnosis` at `yes` or `partial_minor` and score the overconfidence mainly under `uncertainty`.
  - Use `partial_major` or `no` when the PD introduces a new initiating fault absent from the GT, makes one option a necessary/counterfactual precondition, reverses a key dependency direction, or otherwise replaces the GT's main causal story.
- `support_alignment`: whether the PD's cited observations and reasoning support the same qualitative conclusions as the GT.
  - Use this dimension for extra exactness, stronger magnitude claims, and other log-surface specifics.
  - Evaluate these claims from the PD's stated evidence and from whether the stronger claim stays within the GT's qualitative story.
  - A stronger quantitative or more exact claim is acceptable when the PD itself presents concrete evidence for it and the broader GT story stays the same.
  - Do not mark this dimension worse than `partial_minor` solely because the PD is more quantitative, names more hosts/errors/templates, or refines local within-phase ordering.
  - If the PD adds unsupported specificity that does not change the qualitative story, use `partial_minor`.
  - If the PD uses specifics to argue for a more specific trigger, a different dependency path, a broader scope/severity claim, or a stronger conclusion than the GT supports, use `partial_major` or `no`.
- `uncertainty`: compare the PD's hedge posture to the GT only on material hedges about the trigger, a key dependency direction, or the main diagnosis. Ignore mere log-surface precision here.
  - `yes`: matches the GT's uncertainty or is more cautious.
  - `partial_minor`: the PD leans harder toward one GT-plausible option or omits an alternative, but it still speaks probabilistically and does not close the GT's main ambiguity.
  - `partial_major`: the PD makes a GT-explicit unresolved issue sound definitive or effectively rules out plausible alternatives, but the overall incident story still substantially matches the GT.
  - `no`: overconfidence changes the core story, or repeated overconfidence makes the answer qualitatively more certain than the GT across multiple core claims.
  - If the stronger causal claim is already being penalized as a different diagnosis, do not also score `uncertainty` worse than `partial_minor` unless the PD separately closes another GT core ambiguity.

Overall verdict:
Use exactly one:
- `strong_match`
- `good_match`
- `partial_match`
- `weak_match`
- `mismatch`

Overall verdict meanings:
- `strong_match`: a GT-faithful substitute. The PD is semantically and structurally equivalent to the GT, covers all GT-material content, and may add only GT-consistent useful detail.
- `good_match`: a reliable recovery of the GT, but not a full substitute. The same main incident story is recovered, and no major semantic defect remains; any remaining issues are bounded and non-material.
- `partial_match`: a decent and meaningfully useful answer that still gets the main causal story broadly right, but at least one material semantic defect, or an accumulation of smaller semantic defects, prevents reliable handoff-quality substitution.
- `weak_match`: the GT is still recognizable, but major defects dominate. The PD preserves fragments, observations, or broad phases, yet does not cleanly recover the story.
- `mismatch`: the PD tells a materially different incident story and is not usable as a recovery of the GT.

How to combine dimensions into the overall verdict:
- Treat `diagnosis` and `timeline_progression` as the core story anchors.
- Treat `support_alignment` and `uncertainty` as primary semantic qualifiers of whether the recovered story is the same and properly supported.
- Treat `system_reconstruction` as secondary semantic context. A major error here caps the result at `partial_match` unless it makes the story only fragmentarily related (`weak_match`) or materially different (`mismatch`).
- Treat `timeline_structure` and `structure` as scaffold dimensions. They can cap `strong_match`, but by themselves they should not lower an otherwise semantically strong answer below `good_match`.

Hard verdict rules (MUST FOLLOW, APPLY IN ORDER):
1) `strong_match`
   Assign `strong_match` if and only if all seven dimensions are `yes`.

2) `good_match`
   Assign `good_match` only if all of the following hold:
   - the PD recovers the same main incident story as the GT;
   - `diagnosis`, `timeline_progression`, `support_alignment`, and `uncertainty` are all `yes` or `partial_minor`;
   - `system_reconstruction` is `yes` or `partial_minor`;
   - no semantic dimension has a major defect (`partial_major` or `no`);
   - fewer than three semantic dimensions are `partial_minor`.
   Notes:
   - For this count, semantic dimensions are `system_reconstruction`, `timeline_progression`, `diagnosis`, `support_alignment`, and `uncertainty`.
   - `structure` and `timeline_structure` may be any score here; if scaffold defects are the only reason `strong_match` fails, the correct verdict is still `good_match`.

3) `mismatch`
   Assign `mismatch` if the PD tells a materially different incident story and is not usable as a recovery of the GT.
   Strong indicators include:
   - both `diagnosis` and `timeline_progression` are `no`;
   - `support_alignment = no` because the PD's own evidence/reasoning supports a different causal reading rather than merely incomplete coverage;
   - the PD switches to a different primary system, different trigger, or different fault chain such that any overlap with the GT is only superficial.

4) `weak_match`
   Assign `weak_match` if the GT is still materially recognizable in the PD, but major defects dominate.
   Typical triggers include:
   - `diagnosis = no` or `timeline_progression = no`, while `mismatch` does not apply;
   - two or more primary dimensions are `partial_major`;
   - one core story anchor is materially wrong and the remaining overlap is mainly observations, local phases, or other fragments rather than a recovered incident story.
   Notes:
   - The primary dimensions are `diagnosis`, `timeline_progression`, `support_alignment`, and `uncertainty`.
   - Use `weak_match`, not `partial_match`, when the answer is recognizable but no longer broadly right about the main causal story.

5) `partial_match`
   Assign `partial_match` if the PD still gets the main causal story broadly right and remains meaningfully useful, but it is not eligible for `strong_match` or `good_match`, and neither `weak_match` nor `mismatch` applies.
   Common reasons include:
   - at least one semantic dimension is `partial_major`;
   - `system_reconstruction` is `partial_major` or `no`, yet the diagnosis and progression still broadly match the GT;
   - three or more semantic dimensions are `partial_minor`, so the accumulation of smaller issues prevents reliable substitution even though no single issue makes the story fragmentary or different.

Caps and exclusions:
- Any `partial_major` or `no` in `diagnosis`, `timeline_progression`, `support_alignment`, or `uncertainty` blocks both `strong_match` and `good_match`.
- Any `partial_major` or `no` in `system_reconstruction` blocks both `strong_match` and `good_match`.
- A single major defect on a core semantic axis caps the verdict at `partial_match`, unless it makes the story only fragmentarily related (`weak_match`) or materially different (`mismatch`).
- False material additions are worse than bounded omissions.
- Do not let scaffold-only defects, GT-preserving phase partition differences alone, or harmless extra exactness lower the verdict below `good_match`.
- If multiple dimension penalties come from the same root problem, do not push the overall verdict lower merely by counting the same problem multiple times.
- When deciding between neighboring positive labels, prefer the weaker label if the deciding issue is material and semantic.

Mismatch recording:
- `notable_mismatches`: list the deepest independent mismatches first. Do not split one root problem into multiple entries.
- `material_extras`: list only extra claims that are material under the rule above. Ignore harmless elaboration.
- Do not list the same root issue in both arrays unless the extra claim is itself independently material beyond the main mismatch.

Output rules:
- Return JSON only.
- All reasons must be phrased as comparisons between the PD, the GT, and the question.

Output ONLY JSON WITH THE SCHEMA BELOW, NO CODE FENCES, NO OTHER TEXT BEFORE OR AFTER:

{
  "dimension_scores": {
    "structure": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "system_reconstruction": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "timeline_structure": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "timeline_progression": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "diagnosis": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "support_alignment": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    },
    "uncertainty": {
      "verdict": "yes|partial_minor|partial_major|no",
      "reason": "string"
    }
  },
  "notable_mismatches": ["string"],
  "material_extras": ["string"],
  "overall_verdict": "strong_match|good_match|partial_match|weak_match|mismatch",
  "overall_summary": "string"
}
