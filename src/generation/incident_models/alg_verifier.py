"""Algorithmic grader for System Description + Scenario YAML pairs.

Checks algorithmic requirements A1, A2, ...
Returns only violated requirements as a list of dicts.
"""

from __future__ import annotations

import re
from typing import Any, TypeGuard

import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
ISO8601_PREFIX_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"  # 2024-01-01T00:00 or 2024-01-01 00:00
)
LEVEL_PREFIX_RE = re.compile(
    r"^(\[?)("
    r"DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|CRITICAL|FATAL|TRACE|SEVERE"
    r")(\]?)[:\s\-\|]",
    re.IGNORECASE,
)
JSON_WRAPPER_RE = re.compile(r'^\s*\{.*"msg"\s*:', re.DOTALL)
CODE_FENCE_RE = re.compile(
    r"```(?P<lang>[^\n`]*)\s*(?P<content>.*?)```",
    re.DOTALL,
)

VALID_VAR_KINDS = {"i", "f", "ch", "uuid", "hex", "ip", "str"}
VALID_SCOPES = {"per_host", "global"}
PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


def _fail(req_id: str, issue: str, location: str) -> dict[str, Any]:
    return {
        "requirement": req_id,
        "score": 0,
        "reason": f"Issue: {issue}; Location: {location}",
    }


def _fail_multi(req_id: str, issues: list[str]) -> dict[str, Any]:
    combined = " ".join(f"{i + 1}) {msg}" for i, msg in enumerate(issues))
    return {
        "requirement": req_id,
        "score": 0,
        "reason": combined,
    }


def _is_number(v: Any) -> TypeGuard[int | float]:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_int(v: Any) -> TypeGuard[int]:
    return isinstance(v, int) and not isinstance(v, bool)


def _safe_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return []


def _safe_dict(v: Any) -> dict:
    if isinstance(v, dict):
        return v
    return {}


def _yaml_candidates(text: Any) -> list[str]:
    if not isinstance(text, str):
        return [str(text or "")]
    candidates: list[str] = [text.strip()]
    matches = list(CODE_FENCE_RE.finditer(text))
    if not matches:
        return candidates

    def _is_yaml_lang(lang: str) -> bool:
        return lang.strip().lower() in {"", "yaml", "yml"}

    yaml_blocks = [
        match.group("content")
        for match in matches
        if _is_yaml_lang(match.group("lang"))
    ]
    if yaml_blocks:
        candidates.extend(block.strip() for block in yaml_blocks)
    else:
        candidates.append(matches[0].group("content").strip())

    seen: set[str] = set()
    unique: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        unique.append(candidate)
    return unique


def _parse_yaml(text: Any, label: str, location: str) -> tuple[dict, dict[str, Any] | None]:
    last_error: yaml.YAMLError | None = None
    for candidate in _yaml_candidates(text):
        try:
            parsed = yaml.safe_load(candidate)
        except yaml.YAMLError as exc:
            last_error = exc
            continue
        if not isinstance(parsed, dict):
            parsed = {}
        return parsed, None

    message = f"{label} YAML parse error"
    if last_error is not None:
        message = f"{label} YAML parse error: {last_error}"
    return {}, _fail("A0", message, location)


def _resolve_log_ref(ref: str, comp_map: dict[str, dict]) -> bool:
    """Check if <component_id>.<log_id> exists."""
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return False
    cid, lid = parts
    comp = comp_map.get(cid)
    if comp is None:
        return False
    logs = _safe_dict(comp.get("logs"))
    return lid in logs


def _get_component_hosts(comp: dict) -> list:
    return _safe_list(comp.get("hosts"))


def _host_count(comp: dict) -> int:
    return max(1, len(_get_component_hosts(comp)))


# ---------------------------------------------------------------------------
# Requirement checks
# ---------------------------------------------------------------------------


def _check_A1(sd: dict, **_: Any) -> list[dict]:
    components = _safe_list(sd.get("components"))
    if len(components) > 10:
        return [_fail("A1", f"{len(components)} components exceed cap of 10", "components")]
    return []


def _check_A2(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    components = _safe_list(sd.get("components"))
    comp_ids = [c.get("id") for c in components if isinstance(c, dict)]
    if len(comp_ids) != len(set(comp_ids)):
        dupes = [cid for cid in comp_ids if comp_ids.count(cid) > 1]
        issues.append(f"Issue: duplicate component ids: {set(dupes)}; Location: components[].id")
    for comp in components:
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        logs = _safe_dict(comp.get("logs"))
        log_keys = list(logs.keys())
        if len(log_keys) != len(set(log_keys)):
            dupes = [k for k in log_keys if log_keys.count(k) > 1]
            issues.append(
                f"Issue: duplicate log ids in component {cid}: {set(dupes)}; "
                f"Location: components[id={cid}].logs"
            )
    if issues:
        return [_fail_multi("A2", issues)]
    return []


def _check_A3(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for edge in _safe_list(comp.get("to")):
            if not isinstance(edge, dict):
                continue
            dst = edge.get("dst")
            if dst not in comp_map:
                issues.append(
                    f"Issue: dst '{dst}' not found; "
                    f"Location: components[id={cid}].to[].dst"
                )
    if issues:
        return [_fail_multi("A3", issues)]
    return []


def _check_A4(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        beh = _safe_dict(comp.get("beh"))
        logs = _safe_dict(comp.get("logs"))
        for state in ("n", "f"):
            state_beh = beh.get(state)
            if not isinstance(state_beh, dict):
                issues.append(
                    f"Issue: missing beh.{state}; Location: components[id={cid}].beh"
                )
                continue
            if "desc" not in state_beh:
                issues.append(
                    f"Issue: missing beh.{state}.desc; "
                    f"Location: components[id={cid}].beh.{state}"
                )
            emit_val = state_beh.get("emit")
            if emit_val is None:
                issues.append(
                    f"Issue: missing beh.{state}.emit; "
                    f"Location: components[id={cid}].beh.{state}"
                )
                continue
            for idx, entry in enumerate(_safe_list(emit_val)):
                if not isinstance(entry, dict):
                    issues.append(
                        f"Issue: emit[{idx}] is not a mapping; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}]"
                    )
                    continue
                eid = entry.get("id")
                if eid is None:
                    issues.append(
                        f"Issue: missing emit[{idx}].id; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}]"
                    )
                elif eid not in logs:
                    issues.append(
                        f"Issue: emit id '{eid}' not in component logs; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}].id"
                    )
                pm = entry.get("per_min")
                if pm is None:
                    issues.append(
                        f"Issue: missing emit[{idx}].per_min; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}]"
                    )
                elif not _is_number(pm) or pm < 0:
                    issues.append(
                        f"Issue: per_min={pm} invalid; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}].per_min"
                    )
                scope = entry.get("scope")
                if scope is not None and scope not in VALID_SCOPES:
                    issues.append(
                        f"Issue: scope='{scope}' invalid; "
                        f"Location: components[id={cid}].beh.{state}.emit[{idx}].scope"
                    )
    if issues:
        return [_fail_multi("A4", issues)]
    return []


def _check_A5(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        state_flows = _safe_dict(flows.get(state))
        for req in _safe_list(state_flows.get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            rpm = req.get("rpm")
            if rpm is None:
                issues.append(
                    f"Issue: missing rpm; Location: flows.{state}.req[id={fid}]"
                )
            elif not _is_number(rpm) or rpm < 0:
                issues.append(
                    f"Issue: rpm={rpm} invalid; Location: flows.{state}.req[id={fid}].rpm"
                )
    if issues:
        return [_fail_multi("A5", issues)]
    return []


def _check_A6(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        state_flows = _safe_dict(flows.get(state))
        for req in _safe_list(state_flows.get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            for ref in _safe_list(req.get("emit")):
                if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                    issues.append(
                        f"Issue: emit ref '{ref}' invalid; "
                        f"Location: flows.{state}.req[id={fid}].emit"
                    )
    if issues:
        return [_fail_multi("A6", issues)]
    return []


def _check_A7(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        state_flows = _safe_dict(flows.get(state))
        for req in _safe_list(state_flows.get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            for cid in _safe_list(req.get("path")):
                if cid not in comp_map:
                    issues.append(
                        f"Issue: path component '{cid}' not found; "
                        f"Location: flows.{state}.req[id={fid}].path"
                    )
    if issues:
        return [_fail_multi("A7", issues)]
    return []


def _check_A8(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        state_flows = _safe_dict(flows.get(state))
        for req in _safe_list(state_flows.get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            path = _safe_list(req.get("path"))
            for i in range(len(path) - 1):
                a, b = path[i], path[i + 1]
                comp_a = comp_map.get(a)
                if comp_a is None:
                    continue  # caught by req 7
                edges = _safe_list(comp_a.get("to"))
                dsts = {e.get("dst") for e in edges if isinstance(e, dict)}
                if b not in dsts:
                    issues.append(
                        f"Issue: no edge {a}->{b}; "
                        f"Location: flows.{state}.req[id={fid}].path"
                    )
    if issues:
        return [_fail_multi("A8", issues)]
    return []


def _check_A9(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for lid, log_def in _safe_dict(comp.get("logs")).items():
            if not isinstance(log_def, dict):
                continue
            msg = log_def.get("msg")
            if not isinstance(msg, str):
                continue
            msg_stripped = msg.strip()
            if ISO8601_PREFIX_RE.search(msg_stripped):
                issues.append(
                    f"Issue: msg contains timestamp prefix; "
                    f"Location: components[id={cid}].logs.{lid}.msg"
                )
            if LEVEL_PREFIX_RE.search(msg_stripped):
                issues.append(
                    f"Issue: msg contains level prefix; "
                    f"Location: components[id={cid}].logs.{lid}.msg"
                )
            if JSON_WRAPPER_RE.search(msg_stripped):
                issues.append(
                    f"Issue: msg contains JSON wrapper; "
                    f"Location: components[id={cid}].logs.{lid}.msg"
                )
    if issues:
        return [_fail_multi("A9", issues)]
    return []


def _check_A10(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for lid, log_def in _safe_dict(comp.get("logs")).items():
            if not isinstance(log_def, dict):
                continue
            msg = log_def.get("msg")
            if not isinstance(msg, str):
                continue
            placeholders = set(PLACEHOLDER_RE.findall(msg))
            vars_keys = set(_safe_dict(log_def.get("vars")).keys())
            state_vars_all = _safe_dict(log_def.get("state_vars"))
            sv_keys: set[str] = set()
            for st in ("n", "f"):
                sv_keys |= set(_safe_dict(state_vars_all.get(st)).keys())

            overlap = vars_keys & sv_keys
            if overlap:
                issues.append(
                    f"Issue: vars and state_vars overlap on {overlap}; "
                    f"Location: components[id={cid}].logs.{lid}"
                )

            all_defined = vars_keys | sv_keys
            missing_in_def = placeholders - all_defined
            if missing_in_def:
                issues.append(
                    f"Issue: placeholders {missing_in_def} not in vars/state_vars; "
                    f"Location: components[id={cid}].logs.{lid}.msg"
                )
            unused = all_defined - placeholders
            if unused:
                issues.append(
                    f"Issue: unused var entries {unused}; "
                    f"Location: components[id={cid}].logs.{lid}"
                )
    if issues:
        return [_fail_multi("A10", issues)]
    return []


def _check_var_domain(k: str, v: Any) -> str | None:
    """Return error string if domain is invalid, else None."""
    if k in ("i", "f"):
        if not (isinstance(v, list) and len(v) == 2 and all(_is_number(x) for x in v)):
            return f"k={k} requires v=[min,max], got {v!r}"
    elif k == "ch":
        if not (isinstance(v, list) and len(v) > 0):
            return f"k=ch requires non-empty list, got {v!r}"
    elif k == "uuid":
        if v is not None:
            return f"k=uuid requires v=null, got {v!r}"
    elif k == "hex":
        if not _is_int(v):
            return f"k=hex requires integer length, got {v!r}"
    elif k == "ip":
        if v is not None and not isinstance(v, str):
            return f"k=ip requires null or CIDR string, got {v!r}"
    elif k == "str":
        if not (isinstance(v, str) and len(v) > 0):
            return f"k=str requires non-empty hint string, got {v!r}"
    else:
        return f"unknown kind k={k!r}"
    return None


def _check_A11(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for lid, log_def in _safe_dict(comp.get("logs")).items():
            if not isinstance(log_def, dict):
                continue
            # Check vars
            for vname, vdef in _safe_dict(log_def.get("vars")).items():
                if not isinstance(vdef, dict):
                    issues.append(
                        f"Issue: var '{vname}' not a mapping; "
                        f"Location: components[id={cid}].logs.{lid}.vars.{vname}"
                    )
                    continue
                k = vdef.get("k")
                v = vdef.get("v")
                if k not in VALID_VAR_KINDS:
                    issues.append(
                        f"Issue: invalid kind k={k!r}; "
                        f"Location: components[id={cid}].logs.{lid}.vars.{vname}"
                    )
                    continue
                err = _check_var_domain(k, v)
                if err:
                    issues.append(
                        f"Issue: {err}; "
                        f"Location: components[id={cid}].logs.{lid}.vars.{vname}"
                    )
            # Check state_vars
            for st in ("n", "f"):
                for vname, vdef in _safe_dict(
                    _safe_dict(log_def.get("state_vars")).get(st)
                ).items():
                    if not isinstance(vdef, dict):
                        issues.append(
                            f"Issue: state_var '{vname}' not a mapping; "
                            f"Location: components[id={cid}].logs.{lid}.state_vars.{st}.{vname}"
                        )
                        continue
                    k = vdef.get("k")
                    v = vdef.get("v")
                    if k not in VALID_VAR_KINDS:
                        issues.append(
                            f"Issue: invalid kind k={k!r}; Location: "
                            f"components[id={cid}].logs.{lid}.state_vars.{st}.{vname}"
                        )
                        continue
                    err = _check_var_domain(k, v)
                    if err:
                        issues.append(
                            f"Issue: {err}; Location: "
                            f"components[id={cid}].logs.{lid}.state_vars.{st}.{vname}"
                        )
    if issues:
        return [_fail_multi("A11", issues)]
    return []


def _check_A12(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for lid, log_def in _safe_dict(comp.get("logs")).items():
            if not isinstance(log_def, dict):
                continue
            sv = log_def.get("state_vars")
            if sv is None:
                continue
            if not isinstance(sv, dict):
                issues.append(
                    f"Issue: state_vars not a mapping; "
                    f"Location: components[id={cid}].logs.{lid}.state_vars"
                )
                continue
            has_n = "n" in sv
            has_f = "f" in sv
            if not (has_n and has_f):
                issues.append(
                    f"Issue: state_vars missing state key(s) "
                    f"(n={'present' if has_n else 'missing'}, "
                    f"f={'present' if has_f else 'missing'}); "
                    f"Location: components[id={cid}].logs.{lid}.state_vars"
                )
                continue
            n_vars = _safe_dict(sv.get("n"))
            f_vars = _safe_dict(sv.get("f"))
            all_keys = set(n_vars.keys()) | set(f_vars.keys())
            for vname in all_keys:
                if vname not in n_vars:
                    issues.append(
                        f"Issue: state_var '{vname}' missing in n; "
                        f"Location: components[id={cid}].logs.{lid}.state_vars"
                    )
                elif vname not in f_vars:
                    issues.append(
                        f"Issue: state_var '{vname}' missing in f; "
                        f"Location: components[id={cid}].logs.{lid}.state_vars"
                    )
                else:
                    n_k = _safe_dict(n_vars[vname]).get("k")
                    f_k = _safe_dict(f_vars[vname]).get("k")
                    if n_k != f_k:
                        issues.append(
                            f"Issue: state_var '{vname}' kind mismatch n.k={n_k} vs f.k={f_k}; "
                            f"Location: components[id={cid}].logs.{lid}.state_vars"
                        )
    if issues:
        return [_fail_multi("A12", issues)]
    return []


def _check_A13(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    tracing = _safe_dict(sd.get("tracing"))
    if not tracing.get("on"):
        return []  # tracing off — skip
    issues: list[str] = []
    origins = tracing.get("origins")
    if not isinstance(origins, list) or len(origins) == 0:
        issues.append("Issue: tracing.origins empty or missing; Location: tracing.origins")
    else:
        for o in origins:
            if o not in comp_map:
                issues.append(
                    f"Issue: tracing origin '{o}' not a component; "
                    f"Location: tracing.origins"
                )
    tid = tracing.get("trace_id")
    if not isinstance(tid, dict) or tid.get("k") != "hex" or tid.get("v") != 32:
        issues.append(
            f"Issue: trace_id must be {{k: hex, v: 32}}, got {tid!r}; "
            f"Location: tracing.trace_id"
        )
    # traced flows: path[0] must be in origins
    origins_set = set(_safe_list(origins))
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue
            if req.get("trace") is True:
                path = _safe_list(req.get("path"))
                if path and path[0] not in origins_set:
                    fid = req.get("id", "?")
                    issues.append(
                        f"Issue: traced flow '{fid}' path[0]={path[0]} not in origins; "
                        f"Location: flows.{state}.req[id={fid}].path[0]"
                    )
    if issues:
        return [_fail_multi("A13", issues)]
    return []


def _check_A14(sd: dict, **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            emit = _safe_list(req.get("emit"))
            lat = req.get("latency_ms")
            if len(emit) > 0:
                if not isinstance(lat, list):
                    issues.append(
                        f"Issue: emit non-empty but latency_ms missing; "
                        f"Location: flows.{state}.req[id={fid}]"
                    )
                elif len(lat) != len(emit):
                    issues.append(
                        f"Issue: len(latency_ms)={len(lat)} != len(emit)={len(emit)}; "
                        f"Location: flows.{state}.req[id={fid}].latency_ms"
                    )
                else:
                    for li, entry in enumerate(lat):
                        if not (
                            isinstance(entry, list)
                            and len(entry) == 2
                            and all(_is_number(x) for x in entry)
                        ):
                            issues.append(
                                f"Issue: latency_ms[{li}] not [p50,p95]; "
                                f"Location: flows.{state}.req[id={fid}].latency_ms[{li}]"
                            )
                        elif entry[0] < 0 or entry[0] > entry[1]:
                            issues.append(
                                f"Issue: latency_ms[{li}] violates 0<=p50<=p95; "
                                f"Location: flows.{state}.req[id={fid}].latency_ms[{li}]"
                            )
            else:
                if lat is not None and lat != []:
                    issues.append(
                        f"Issue: emit empty but latency_ms not empty; "
                        f"Location: flows.{state}.req[id={fid}].latency_ms"
                    )
    if issues:
        return [_fail_multi("A14", issues)]
    return []


def _check_A15(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            retry = req.get("retry")
            if retry is None:
                continue
            if not isinstance(retry, dict):
                issues.append(
                    f"Issue: retry not a mapping; "
                    f"Location: flows.{state}.req[id={fid}].retry"
                )
                continue
            ma = retry.get("max_attempts")
            if not _is_int(ma) or ma < 1:
                issues.append(
                    f"Issue: max_attempts={ma!r} invalid; "
                    f"Location: flows.{state}.req[id={fid}].retry.max_attempts"
                )
                ma = None  # prevent further checks
            ea = retry.get("expected_attempts")
            if ea is not None:
                if not _is_number(ea):
                    issues.append(
                        f"Issue: expected_attempts={ea!r} not a number; "
                        f"Location: flows.{state}.req[id={fid}].retry.expected_attempts"
                    )
                elif ma is not None and (ea < 1 or ea > ma):
                    issues.append(
                        f"Issue: expected_attempts={ea} out of [1,{ma}]; "
                        f"Location: flows.{state}.req[id={fid}].retry.expected_attempts"
                    )
            for ref in _safe_list(retry.get("emit_per_retry")):
                if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                    issues.append(
                        f"Issue: emit_per_retry ref '{ref}' invalid; "
                        f"Location: flows.{state}.req[id={fid}].retry.emit_per_retry"
                    )
            backoff = retry.get("backoff_ms")
            if backoff is not None:
                if not isinstance(backoff, list):
                    issues.append(
                        f"Issue: backoff_ms not a list; "
                        f"Location: flows.{state}.req[id={fid}].retry.backoff_ms"
                    )
                elif ma is not None and len(backoff) != ma - 1:
                    issues.append(
                        f"Issue: len(backoff_ms)={len(backoff)} != max_attempts-1={ma - 1}; "
                        f"Location: flows.{state}.req[id={fid}].retry.backoff_ms"
                    )
                else:
                    for bi, entry in enumerate(backoff if isinstance(backoff, list) else []):
                        if not (
                            isinstance(entry, list)
                            and len(entry) == 2
                            and all(_is_number(x) for x in entry)
                        ):
                            issues.append(
                                f"Issue: backoff_ms[{bi}] not [p50,p95]; "
                                f"Location: flows.{state}.req[id={fid}].retry.backoff_ms[{bi}]"
                            )
                        elif entry[0] < 0 or entry[0] > entry[1]:
                            issues.append(
                                f"Issue: backoff_ms[{bi}] violates 0<=p50<=p95; "
                                f"Location: flows.{state}.req[id={fid}].retry.backoff_ms[{bi}]"
                            )
    if issues:
        return [_fail_multi("A15", issues)]
    return []


# --- Scenario requirements ---


def _check_A16(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    required = ["id", "title", "states", "summary", "time", "phases", "assumptions"]
    missing = [k for k in required if k not in scenario]
    if missing:
        return [_fail("A16", f"missing keys: {missing}", "scenario")]
    return []


def _check_A17(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    issues: list[str] = []
    states = scenario.get("states")
    expected_states = {"n": "normal", "f": "failure"}
    if states != expected_states:
        issues.append(
            f"Issue: states={states!r} != expected {expected_states}; "
            f"Location: scenario.states"
        )
    phases = _safe_dict(scenario.get("phases"))
    extra = set(phases.keys()) - {"n", "f"}
    if extra:
        issues.append(
            f"Issue: extra phase keys {extra}; Location: scenario.phases"
        )
    if issues:
        return [_fail_multi("A17", issues)]
    return []


def _check_A18(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    issues: list[str] = []
    time = _safe_dict(scenario.get("time"))
    tm = time.get("total_minutes")
    if not _is_int(tm):
        issues.append(
            f"Issue: total_minutes={tm!r} not integer; Location: scenario.time.total_minutes"
        )
    phases_time = _safe_dict(time.get("phases"))
    for state in ("n", "f"):
        pt = _safe_dict(phases_time.get(state))
        for field in ("start_min", "end_min"):
            val = pt.get(field)
            if not _is_int(val):
                issues.append(
                    f"Issue: {state}.{field}={val!r} not integer; "
                    f"Location: scenario.time.phases.{state}.{field}"
                )
    # events at_min
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        am = ev.get("at_min")
        order = ev.get("order", "?")
        if not _is_int(am):
            issues.append(
                f"Issue: event order={order} at_min={am!r} not integer; "
                f"Location: scenario.phases.f.events[order={order}].at_min"
            )
    if issues:
        return [_fail_multi("A18", issues)]
    return []


def _check_A19(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    issues: list[str] = []
    time = _safe_dict(scenario.get("time"))
    tm = time.get("total_minutes")
    phases_time = _safe_dict(time.get("phases"))
    n_t = _safe_dict(phases_time.get("n"))
    f_t = _safe_dict(phases_time.get("f"))
    n_start = n_t.get("start_min")
    n_end = n_t.get("end_min")
    f_start = f_t.get("start_min")
    f_end = f_t.get("end_min")
    if n_start != 0:
        issues.append(
            f"Issue: n.start_min={n_start} != 0; Location: scenario.time.phases.n.start_min"
        )
    if _is_int(f_end) and _is_int(tm) and f_end != tm:
        issues.append(
            f"Issue: f.end_min={f_end} != total_minutes={tm}; "
            f"Location: scenario.time.phases.f.end_min"
        )
    if _is_int(n_end) and _is_int(f_start) and n_end != f_start:
        issues.append(
            f"Issue: n.end_min={n_end} != f.start_min={f_start}; "
            f"Location: scenario.time.phases"
        )
    for label, s, e in [("n", n_start, n_end), ("f", f_start, f_end)]:
        if _is_int(s) and _is_int(e):
            if s >= e:
                issues.append(
                    f"Issue: {label} start_min={s} >= end_min={e}; "
                    f"Location: scenario.time.phases.{label}"
                )
    # all mins within [0, total_minutes]
    if _is_int(tm):
        for label, val in [
            ("n.start_min", n_start),
            ("n.end_min", n_end),
            ("f.start_min", f_start),
            ("f.end_min", f_end),
        ]:
            if _is_int(val) and (val < 0 or val > tm):
                issues.append(
                    f"Issue: {label}={val} outside [0,{tm}]; "
                    f"Location: scenario.time.phases.{label}"
                )
    if issues:
        return [_fail_multi("A19", issues)]
    return []


def _check_A20(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    tm = _safe_dict(scenario.get("time")).get("total_minutes")
    if _is_int(tm) and (tm < 2 or tm > 60):
        return [_fail("A20", f"total_minutes={tm} not in [2,60]", "scenario.time.total_minutes")]
    return []


def _check_A21(sc: dict, **_: Any) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    issues: list[str] = []
    phases_time = _safe_dict(_safe_dict(scenario.get("time")).get("phases"))
    for state in ("n", "f"):
        pt = _safe_dict(phases_time.get(state))
        s = pt.get("start_min")
        e = pt.get("end_min")
        if _is_int(s) and _is_int(e) and (e - s) < 1:
            issues.append(
                f"Issue: {state} duration={e - s} < 1; "
                f"Location: scenario.time.phases.{state}"
            )
    if issues:
        return [_fail_multi("A21", issues)]
    return []


def _check_A22(
    sc: dict, comp_map: dict[str, dict], flow_map_f: dict[str, dict], **_: Any
) -> list[dict]:
    scenario = _safe_dict(sc.get("scenario"))
    issues: list[str] = []
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))
    events = f_phase.get("events")
    if not isinstance(events, list) or len(events) == 0:
        return [_fail("A22", "phases.f.events missing or empty", "scenario.phases.f.events")]
    if len(events) < 2:
        issues.append(
            f"Issue: only {len(events)} event(s), need >= 2; "
            f"Location: scenario.phases.f.events"
        )

    # Order checks
    orders = []
    for ev in events:
        if isinstance(ev, dict):
            orders.append(ev.get("order"))
    expected_orders = list(range(1, len(events) + 1))
    if orders != expected_orders:
        issues.append(
            f"Issue: orders={orders} not consecutive 1..{len(events)}; "
            f"Location: scenario.phases.f.events[].order"
        )

    # Timing checks
    time = _safe_dict(scenario.get("time"))
    phases_time = _safe_dict(time.get("phases"))
    f_time = _safe_dict(phases_time.get("f"))
    f_start = f_time.get("start_min")
    f_end = f_time.get("end_min")

    prev_at_min = None
    for ev in events:
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        at_min = ev.get("at_min")

        if order == 1 and _is_int(at_min) and _is_int(f_start) and at_min != f_start:
            issues.append(
                f"Issue: events[0].at_min={at_min} != f.start_min={f_start}; "
                f"Location: scenario.phases.f.events[order=1].at_min"
            )
        if order == 2 and _is_int(at_min) and _is_int(f_start) and at_min > f_start + 10:
            issues.append(
                f"Issue: events[1].at_min={at_min} > f.start_min+10={f_start + 10}; "
                f"Location: scenario.phases.f.events[order=2].at_min"
            )
        if _is_int(at_min):
            if _is_int(f_start) and at_min < f_start:
                issues.append(
                    f"Issue: event order={order} at_min={at_min} < f.start_min={f_start}; "
                    f"Location: scenario.phases.f.events[order={order}].at_min"
                )
            if _is_int(f_end) and at_min > f_end:
                issues.append(
                    f"Issue: event order={order} at_min={at_min} > f.end_min={f_end}; "
                    f"Location: scenario.phases.f.events[order={order}].at_min"
                )
            if prev_at_min is not None and at_min < prev_at_min:
                issues.append(
                    f"Issue: event order={order} at_min={at_min} < prev at_min={prev_at_min}; "
                    f"Location: scenario.phases.f.events[order={order}].at_min"
                )
            prev_at_min = at_min

        # component exists
        comp_ref = ev.get("component")
        if comp_ref is not None and comp_ref not in comp_map:
            issues.append(
                f"Issue: event component '{comp_ref}' not found; "
                f"Location: scenario.phases.f.events[order={order}].component"
            )

        # flows exist in flows.f
        for fid in _safe_list(ev.get("flows")):
            if fid not in flow_map_f:
                issues.append(
                    f"Issue: event flow '{fid}' not in flows.f; "
                    f"Location: scenario.phases.f.events[order={order}].flows"
                )

        # manifestation
        manif = ev.get("manifestation")
        if not isinstance(manif, list) or len(manif) == 0:
            issues.append(
                f"Issue: event manifestation missing/empty; "
                f"Location: scenario.phases.f.events[order={order}].manifestation"
            )
        else:
            for ref in manif:
                if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                    issues.append(
                        f"Issue: manifestation ref '{ref}' invalid; "
                        f"Location: scenario.phases.f.events[order={order}].manifestation"
                    )

        # one_shots
        for os_entry in _safe_list(ev.get("one_shots")):
            if not isinstance(os_entry, dict):
                continue
            ref = os_entry.get("ref")
            if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                issues.append(
                    f"Issue: one_shot ref '{ref}' invalid; "
                    f"Location: scenario.phases.f.events[order={order}].one_shots"
                )
            count = os_entry.get("count")
            if not _is_int(count) or count < 1:
                issues.append(
                    f"Issue: one_shot count={count!r} invalid; "
                    f"Location: scenario.phases.f.events[order={order}].one_shots"
                )
            hosts = os_entry.get("hosts")
            if hosts is not None and isinstance(ref, str):
                parts = ref.split(".", 1)
                if len(parts) == 2:
                    comp = comp_map.get(parts[0])
                    if comp is not None:
                        comp_hosts = set(_get_component_hosts(comp))
                        for h in _safe_list(hosts):
                            if h not in comp_hosts:
                                issues.append(
                                    f"Issue: one_shot host '{h}' not in {parts[0]}.hosts; "
                                    f"Location: scenario.phases.f.events[order={order}].one_shots"
                                )

    if issues:
        return [_fail_multi("A22", issues)]
    return []


def _check_A23(
    sc: dict,
    comp_map: dict[str, dict],
    flow_map_f: dict[str, dict],
    sd: dict,
    **_: Any,
) -> list[dict]:
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))

    # Build beh.f emit map: component_id.log_id -> True
    beh_f_emit_set: set[str] = set()
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        beh_f = _safe_dict(_safe_dict(comp.get("beh")).get("f"))
        for entry in _safe_list(beh_f.get("emit")):
            if isinstance(entry, dict):
                eid = entry.get("id")
                if eid:
                    beh_f_emit_set.add(f"{cid}.{eid}")

    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        rm = _safe_dict(ev.get("rate_multipliers"))

        for key, val in rm.items():
            # Determine if key is flow_id or log_ref
            is_flow = "." not in key
            if is_flow:
                # flow-id key
                if key not in flow_map_f:
                    issues.append(
                        f"Issue: rate_multiplier flow '{key}' not in flows.f; "
                        f"Location: scenario.phases.f.events[order={order}].rate_multipliers"
                    )
                if not _is_number(val) or val < 0:
                    issues.append(
                        f"Issue: rate_multiplier value={val!r} invalid for flow '{key}'; "
                        f"Location: scenario.phases.f.events[order={order}].rate_multipliers"
                    )
            else:
                # log-ref key
                if not _resolve_log_ref(key, comp_map):
                    issues.append(
                        f"Issue: rate_multiplier log ref '{key}' invalid; "
                        f"Location: scenario.phases.f.events[order={order}].rate_multipliers"
                    )
                elif key not in beh_f_emit_set:
                    issues.append(
                        f"Issue: rate_multiplier log '{key}' not in beh.f.emit; "
                        f"Location: scenario.phases.f.events[order={order}].rate_multipliers"
                    )
                if not _is_number(val) or val < 0:
                    issues.append(
                        f"Issue: rate_multiplier value={val!r} invalid for log '{key}'; "
                        f"Location: scenario.phases.f.events[order={order}].rate_multipliers"
                    )
    if issues:
        return [_fail_multi("A23", issues)]
    return []


def _check_A24(sc: dict, flow_map_f: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        lm = _safe_dict(ev.get("latency_multipliers"))
        for key, val in lm.items():
            if key not in flow_map_f:
                issues.append(
                    f"Issue: latency_multiplier flow '{key}' not in flows.f; "
                    f"Location: scenario.phases.f.events[order={order}].latency_multipliers"
                )
            if not isinstance(val, dict):
                issues.append(
                    f"Issue: latency_multiplier value not a mapping for '{key}'; "
                    f"Location: scenario.phases.f.events[order={order}].latency_multipliers"
                )
                continue
            for pf in ("p50", "p95"):
                pv = val.get(pf)
                if not _is_number(pv) or pv < 0:
                    issues.append(
                        f"Issue: latency_multiplier {key}.{pf}={pv!r} invalid; "
                        f"Location: scenario.phases.f.events[order={order}].latency_multipliers.{key}"
                    )
    if issues:
        return [_fail_multi("A24", issues)]
    return []


def _check_A25(
    sc: dict,
    comp_map: dict[str, dict],
    flow_map_n: dict[str, dict],
    flow_map_f: dict[str, dict],
    **_: Any,
) -> list[dict]:
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))

    def _check_manif(manif_list: list, loc: str) -> None:
        for ref in manif_list:
            if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                issues.append(f"Issue: manifestation ref '{ref}' invalid; Location: {loc}")

    # n.manifestation
    n_phase = _safe_dict(phases.get("n"))
    _check_manif(_safe_list(n_phase.get("manifestation")), "scenario.phases.n.manifestation")

    # n.flows
    for fid in _safe_list(n_phase.get("flows")):
        if fid not in flow_map_n:
            issues.append(
                f"Issue: n.flows ref '{fid}' not in flows.n; "
                f"Location: scenario.phases.n.flows"
            )

    # f phase
    f_phase = _safe_dict(phases.get("f"))
    for fid in _safe_list(f_phase.get("flows")):
        if fid not in flow_map_f:
            issues.append(
                f"Issue: f.flows ref '{fid}' not in flows.f; "
                f"Location: scenario.phases.f.flows"
            )

    # f.events
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        for os_entry in _safe_list(ev.get("one_shots")):
            if isinstance(os_entry, dict):
                ref = os_entry.get("ref")
                if not isinstance(ref, str) or not _resolve_log_ref(ref, comp_map):
                    issues.append(
                        f"Issue: one_shot ref '{ref}' invalid; "
                        f"Location: scenario.phases.f.events[order={order}].one_shots"
                    )
        for fid in _safe_list(ev.get("flows")):
            if fid not in flow_map_f:
                issues.append(
                    f"Issue: event flow '{fid}' not in flows.f; "
                    f"Location: scenario.phases.f.events[order={order}].flows"
                )

    # f.manifestation (optional top-level)
    _check_manif(
        _safe_list(f_phase.get("manifestation")), "scenario.phases.f.manifestation"
    )

    # steady manifestation
    for si, st_entry in enumerate(_safe_list(f_phase.get("steady"))):
        if isinstance(st_entry, dict):
            _check_manif(
                _safe_list(st_entry.get("manifestation")),
                f"scenario.phases.f.steady[{si}].manifestation",
            )

    if issues:
        return [_fail_multi("A25", issues)]
    return []


def _check_A26(sc: dict, **_: Any) -> list[dict]:
    """A26: Narrative structure (steady non-empty, condition/user_impact)."""
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))

    steady = f_phase.get("steady")
    if not isinstance(steady, list) or len(steady) == 0:
        issues.append("Issue: phases.f.steady is empty/missing; Location: scenario.phases.f.steady")
    else:
        for si, entry in enumerate(steady):
            if not isinstance(entry, dict):
                continue
            cond = entry.get("condition")
            if not cond or (isinstance(cond, str) and not cond.strip()):
                issues.append(
                    f"Issue: steady[{si}].condition empty; "
                    f"Location: scenario.phases.f.steady[{si}].condition"
                )
            ui = entry.get("user_impact")
            if not ui or (isinstance(ui, str) and not ui.strip()):
                issues.append(
                    f"Issue: steady[{si}].user_impact empty; "
                    f"Location: scenario.phases.f.steady[{si}].user_impact"
                )
    if issues:
        return [_fail_multi("A26", issues)]
    return []


def _check_A27(sc: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))

    # n.manifestation
    n_manif = _safe_dict(phases.get("n")).get("manifestation")
    if not isinstance(n_manif, list) or len(n_manif) == 0:
        issues.append(
            "Issue: phases.n.manifestation missing/empty; "
            "Location: scenario.phases.n.manifestation"
        )

    # f.events[].manifestation
    f_phase = _safe_dict(phases.get("f"))
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        em = ev.get("manifestation")
        if not isinstance(em, list) or len(em) == 0:
            issues.append(
                f"Issue: event manifestation missing/empty; "
                f"Location: scenario.phases.f.events[order={order}].manifestation"
            )

    # steady[].manifestation
    for si, st_entry in enumerate(_safe_list(f_phase.get("steady"))):
        if not isinstance(st_entry, dict):
            continue
        sm = st_entry.get("manifestation")
        if not isinstance(sm, list) or len(sm) == 0:
            issues.append(
                f"Issue: steady[{si}].manifestation missing/empty; "
                f"Location: scenario.phases.f.steady[{si}].manifestation"
            )
    if issues:
        return [_fail_multi("A27", issues)]
    return []


def _check_A28(sd: dict, **_: Any) -> list[dict]:
    """A28: Sufficient detail for log generation (structural)."""
    issues: list[str] = []
    components = _safe_list(sd.get("components"))

    # Each component has >= 1 log
    for comp in components:
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        logs = _safe_dict(comp.get("logs"))
        if len(logs) == 0:
            issues.append(
                f"Issue: component '{cid}' has no log templates; "
                f"Location: components[id={cid}].logs"
            )

    # At least one beh emit across system
    has_beh_emit = False
    for comp in components:
        if not isinstance(comp, dict):
            continue
        beh = _safe_dict(comp.get("beh"))
        for state in ("n", "f"):
            for entry in _safe_list(_safe_dict(beh.get(state)).get("emit")):
                if isinstance(entry, dict) and entry.get("id"):
                    has_beh_emit = True
                    break
            if has_beh_emit:
                break
        if has_beh_emit:
            break
    if not has_beh_emit:
        issues.append(
            "Issue: no beh emit across entire system; Location: components[].beh.*.emit"
        )

    # Each state has >= 1 flow
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        reqs = _safe_list(_safe_dict(flows.get(state)).get("req"))
        if len(reqs) == 0:
            issues.append(
                f"Issue: no request flows in state '{state}'; "
                f"Location: flows.{state}.req"
            )

    # Emission mechanism separation (per state)
    for comp in components:
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        for state in ("n", "f"):
            beh_emit_ids: set[str] = set()
            for entry in _safe_list(_safe_dict(_safe_dict(comp.get("beh")).get(state)).get("emit")):
                if isinstance(entry, dict) and entry.get("id"):
                    beh_emit_ids.add(entry["id"])

            # Collect flow-emitted and retry-emitted log refs for this component in this state
            flow_emit_ids: set[str] = set()
            retry_emit_ids: set[str] = set()
            for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
                if not isinstance(req, dict):
                    continue
                for ref in _safe_list(req.get("emit")):
                    if isinstance(ref, str) and ref.startswith(f"{cid}."):
                        flow_emit_ids.add(ref.split(".", 1)[1])
                retry = _safe_dict(req.get("retry"))
                for ref in _safe_list(retry.get("emit_per_retry")):
                    if isinstance(ref, str) and ref.startswith(f"{cid}."):
                        retry_emit_ids.add(ref.split(".", 1)[1])

            # Check: not both background and flow-emitted
            dual_beh_flow = beh_emit_ids & (flow_emit_ids | retry_emit_ids)
            if dual_beh_flow:
                issues.append(
                    f"Issue: log(s) {dual_beh_flow} used as both beh and flow emit in state {state}; "
                    f"Location: components[id={cid}]"
                )

            # Check: not both per-attempt and retry
            dual_flow_retry = flow_emit_ids & retry_emit_ids
            if dual_flow_retry:
                issues.append(
                    f"Issue: log(s) {dual_flow_retry} in both emit and retry.emit_per_retry in state {state}; "
                    f"Location: components[id={cid}]"
                )

    if issues:
        return [_fail_multi("A28", issues)]
    return []


def _check_A29(
    sd: dict, sc: dict, flow_map_n: dict[str, dict], flow_map_f: dict[str, dict], **_: Any
) -> list[dict]:
    """A29: IDs and naming hygiene + assumptions structure."""
    issues: list[str] = []

    # snake_case IDs
    def _check_snake(val: Any, loc: str) -> None:
        if isinstance(val, str) and not SNAKE_CASE_RE.match(val):
            issues.append(f"Issue: '{val}' not snake_case; Location: {loc}")

    _check_snake(_safe_dict(sd.get("sys")).get("id"), "sys.id")
    for comp in _safe_list(sd.get("components")):
        if isinstance(comp, dict):
            _check_snake(comp.get("id"), f"components[id={comp.get('id', '?')}].id")

    scenario = _safe_dict(sc.get("scenario"))
    _check_snake(scenario.get("id"), "scenario.id")

    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if isinstance(req, dict):
                _check_snake(req.get("id"), f"flows.{state}.req[id={req.get('id', '?')}].id")

    f_phase = _safe_dict(_safe_dict(scenario.get("phases")).get("f"))
    for fl in _safe_list(f_phase.get("feedback_loops")):
        if isinstance(fl, dict):
            _check_snake(fl.get("id"), f"feedback_loops[id={fl.get('id', '?')}].id")

    # assumptions exist and are lists of strings
    sd_assumptions = sd.get("assumptions")
    if not isinstance(sd_assumptions, list):
        issues.append(
            "Issue: system_description.assumptions missing or not a list; "
            "Location: assumptions"
        )
    elif not all(isinstance(a, str) for a in sd_assumptions):
        issues.append(
            "Issue: system_description.assumptions contains non-string; "
            "Location: assumptions"
        )

    sc_assumptions = scenario.get("assumptions")
    if not isinstance(sc_assumptions, list):
        issues.append(
            "Issue: scenario.assumptions missing or not a list; "
            "Location: scenario.assumptions"
        )
    elif not all(isinstance(a, str) for a in sc_assumptions):
        issues.append(
            "Issue: scenario.assumptions contains non-string; "
            "Location: scenario.assumptions"
        )

    if issues:
        return [_fail_multi("A29", issues)]
    return []


def _build_emission_indexes(
    sd: dict,
) -> tuple[dict[str, set[str]], dict[str, dict[str, set[str]]], dict[str, dict[str, set[str]]]]:
    """Build helper indexes for rate/emission checks (shared by A30, A31, A32)."""
    beh_emit: dict[str, set[str]] = {"n": set(), "f": set()}
    for comp in _safe_list(sd.get("components")):
        if not isinstance(comp, dict):
            continue
        cid = comp.get("id", "?")
        beh = _safe_dict(comp.get("beh"))
        for state in ("n", "f"):
            for entry in _safe_list(_safe_dict(beh.get(state)).get("emit")):
                if not isinstance(entry, dict):
                    continue
                eid = entry.get("id")
                pm = entry.get("per_min", 0)
                if eid and _is_number(pm) and pm > 0:
                    beh_emit[state].add(f"{cid}.{eid}")

    flow_emit: dict[str, dict[str, set[str]]] = {"n": {}, "f": {}}
    flow_retry_emit: dict[str, dict[str, set[str]]] = {"n": {}, "f": {}}
    flows = _safe_dict(sd.get("flows"))
    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            rpm = req.get("rpm", 0)
            if _is_number(rpm) and rpm > 0:
                flow_emit[state][fid] = set(_safe_list(req.get("emit")))
                retry = _safe_dict(req.get("retry"))
                flow_retry_emit[state][fid] = set(_safe_list(retry.get("emit_per_retry")))

    return beh_emit, flow_emit, flow_retry_emit


def _all_flow_emitted(
    state: str,
    flow_emit: dict[str, dict[str, set[str]]],
    flow_retry_emit: dict[str, dict[str, set[str]]],
) -> set[str]:
    result: set[str] = set()
    for fid, refs in flow_emit[state].items():
        result |= refs
    for fid, refs in flow_retry_emit[state].items():
        result |= refs
    return result


def _check_A30(
    sd: dict,
    sc: dict,
    comp_map: dict[str, dict],
    flow_map_n: dict[str, dict],
    flow_map_f: dict[str, dict],
    **_: Any,
) -> list[dict]:
    """A30: Scenario and rate consistency."""
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))

    # n phase flows
    n_phase = _safe_dict(phases.get("n"))
    for fid in _safe_list(n_phase.get("flows")):
        if fid not in flow_map_n:
            continue  # caught by A25
        req = flow_map_n[fid]
        rpm = req.get("rpm", 0)
        if not (_is_number(rpm) and rpm > 0):
            issues.append(
                f"Issue: scenario references flow '{fid}' in n but rpm=0; "
                f"Location: flows.n.req[id={fid}].rpm"
            )

    # f phase flows
    f_phase = _safe_dict(phases.get("f"))
    for fid in _safe_list(f_phase.get("flows")):
        if fid not in flow_map_f:
            continue
        req = flow_map_f[fid]
        rpm = req.get("rpm", 0)
        if not (_is_number(rpm) and rpm > 0):
            issues.append(
                f"Issue: scenario references flow '{fid}' in f but rpm=0; "
                f"Location: flows.f.req[id={fid}].rpm"
            )

    # f.events[].flows
    checked_event_flows: set[str] = set()
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        for fid in _safe_list(ev.get("flows")):
            if fid in checked_event_flows:
                continue
            checked_event_flows.add(fid)
            if fid not in flow_map_f:
                continue  # caught by A22/A25
            req = flow_map_f[fid]
            rpm = req.get("rpm", 0)
            if not (_is_number(rpm) and rpm > 0):
                issues.append(
                    f"Issue: event order={order} references flow '{fid}' but rpm=0; "
                    f"Location: flows.f.req[id={fid}].rpm"
                )

    if issues:
        return [_fail_multi("A30", issues)]
    return []


def _check_A31(
    sd: dict,
    sc: dict,
    comp_map: dict[str, dict],
    **_: Any,
) -> list[dict]:
    """A31: Manifestation emission validity."""
    issues: list[str] = []
    scenario = _safe_dict(sc.get("scenario"))
    phases = _safe_dict(scenario.get("phases"))

    beh_emit, flow_emit, flow_retry_emit = _build_emission_indexes(sd)

    # n.manifestation: each log emitted with rate > 0 in n via beh or flow
    n_phase = _safe_dict(phases.get("n"))
    n_all_emitted = beh_emit["n"] | _all_flow_emitted("n", flow_emit, flow_retry_emit)
    for ref in _safe_list(n_phase.get("manifestation")):
        if ref not in n_all_emitted:
            issues.append(
                f"Issue: n.manifestation '{ref}' not emitted with rate>0 in n; "
                f"Location: scenario.phases.n.manifestation"
            )

    # f.steady[].manifestation
    f_phase = _safe_dict(phases.get("f"))
    f_all_emitted = beh_emit["f"] | _all_flow_emitted("f", flow_emit, flow_retry_emit)
    for si, st_entry in enumerate(_safe_list(f_phase.get("steady"))):
        if not isinstance(st_entry, dict):
            continue
        for ref in _safe_list(st_entry.get("manifestation")):
            if ref not in f_all_emitted:
                issues.append(
                    f"Issue: steady[{si}].manifestation '{ref}' not emitted with rate>0 in f; "
                    f"Location: scenario.phases.f.steady[{si}].manifestation"
                )

    # f.events[].manifestation: emitted with rate > 0 in f OR one-shot in same event
    for ev in _safe_list(f_phase.get("events")):
        if not isinstance(ev, dict):
            continue
        order = ev.get("order", "?")
        ev_one_shots: set[str] = set()
        for os_entry in _safe_list(ev.get("one_shots")):
            if isinstance(os_entry, dict):
                ref = os_entry.get("ref")
                if isinstance(ref, str):
                    ev_one_shots.add(ref)
        for ref in _safe_list(ev.get("manifestation")):
            if ref not in f_all_emitted and ref not in ev_one_shots:
                issues.append(
                    f"Issue: event[order={order}].manifestation '{ref}' not emitted in f or one-shot; "
                    f"Location: scenario.phases.f.events[order={order}].manifestation"
                )

    if issues:
        return [_fail_multi("A31", issues)]
    return []


def _check_A32(
    sd: dict,
    sc: dict,
    comp_map: dict[str, dict],
    **_: Any,
) -> list[dict]:
    """A32: Total log volume validity."""
    issues: list[str] = []
    try:
        total_logs = _estimate_total_logs(sd, sc, comp_map)
        if total_logs < 20000 or total_logs > 100000:
            issues.append(
                f"Issue: estimated total_logs={total_logs:.0f} not in [20000,100000]; "
                f"Location: log volume estimation"
            )
    except Exception as e:
        issues.append(
            f"Issue: log volume estimation error: {e}; Location: log volume estimation"
        )

    if issues:
        return [_fail_multi("A32", issues)]
    return []


def _check_A33(sd: dict, **_: Any) -> list[dict]:
    """A33: Emit/path traversal validity."""
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))

    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue

            fid = req.get("id", "?")
            path = _safe_list(req.get("path"))
            emit_refs = _safe_list(req.get("emit"))

            path_pos = 0
            for emit_idx, ref in enumerate(emit_refs):
                if not isinstance(ref, str):
                    continue  # covered by A6

                parts = ref.split(".", 1)
                if len(parts) != 2:
                    continue  # covered by A6
                emit_component = parts[0]

                match_pos = None
                for idx in range(path_pos, len(path)):
                    if path[idx] == emit_component:
                        match_pos = idx
                        break

                if match_pos is None:
                    issues.append(
                        f"Issue: emit component '{emit_component}' at emit[{emit_idx}] cannot be matched in path at/after index {path_pos}; "
                        f"path={path}; Location: flows.{state}.req[id={fid}]"
                    )
                    break

                # Allow repeated emits from the same path component.
                path_pos = match_pos

    if issues:
        return [_fail_multi("A33", issues)]
    return []


def _check_A34(sd: dict, comp_map: dict[str, dict], **_: Any) -> list[dict]:
    """A34: Retry emit_per_retry layer coherence."""
    issues: list[str] = []
    flows = _safe_dict(sd.get("flows"))

    # A component is considered "in front of" the entrypoint if it has an
    # explicit outgoing edge to path[0].
    upstream_by_dst: dict[str, set[str]] = {}
    for src_id, comp in comp_map.items():
        for edge in _safe_list(_safe_dict(comp).get("to")):
            if not isinstance(edge, dict):
                continue
            dst = edge.get("dst")
            if isinstance(dst, str):
                upstream_by_dst.setdefault(dst, set()).add(src_id)

    for state in ("n", "f"):
        for req in _safe_list(_safe_dict(flows.get(state)).get("req")):
            if not isinstance(req, dict):
                continue

            fid = req.get("id", "?")
            retry_raw = req.get("retry")
            if retry_raw is None:
                continue
            if not isinstance(retry_raw, dict):
                continue  # covered by A15

            path = _safe_list(req.get("path"))
            if not path or not isinstance(path[0], str):
                continue  # covered by A10
            entrypoint = path[0]
            upstream_callers = upstream_by_dst.get(entrypoint, set())

            for ref in _safe_list(retry_raw.get("emit_per_retry")):
                if not isinstance(ref, str):
                    continue  # covered by A15

                parts = ref.split(".", 1)
                if len(parts) != 2:
                    continue  # covered by A15
                emit_component = parts[0]

                if emit_component == entrypoint or emit_component in upstream_callers:
                    continue

                if emit_component in path:
                    issues.append(
                        f"Issue: retry.emit_per_retry ref '{ref}' uses downstream path component '{emit_component}' "
                        f"(entrypoint='{entrypoint}'); Location: flows.{state}.req[id={fid}].retry.emit_per_retry"
                    )
                else:
                    upstream_sorted = sorted(upstream_callers)
                    issues.append(
                        f"Issue: retry.emit_per_retry ref '{ref}' component '{emit_component}' is neither entrypoint "
                        f"'{entrypoint}' nor an upstream caller {upstream_sorted}; "
                        f"Location: flows.{state}.req[id={fid}].retry.emit_per_retry"
                    )

    if issues:
        return [_fail_multi("A34", issues)]
    return []


def _estimate_total_logs(
    sd: dict, sc: dict, comp_map: dict[str, dict]
) -> float:
    """Estimate total log volume using the formula from the spec."""
    scenario = _safe_dict(sc.get("scenario"))
    time = _safe_dict(scenario.get("time"))
    phases_time = _safe_dict(time.get("phases"))
    n_time = _safe_dict(phases_time.get("n"))
    f_time = _safe_dict(phases_time.get("f"))

    n_start = n_time.get("start_min", 0)
    n_end = n_time.get("end_min", 0)
    f_start = f_time.get("start_min", 0)
    f_end = f_time.get("end_min", 0)
    n_duration = max(0, n_end - n_start)
    f_duration = max(0, f_end - f_start)

    flows_data = _safe_dict(sd.get("flows"))
    components = _safe_list(sd.get("components"))

    def _logs_per_min_beh(state: str, multipliers: dict[str, float] | None = None) -> float:
        total = 0.0
        for comp in components:
            if not isinstance(comp, dict):
                continue
            cid = comp.get("id", "?")
            hc = _host_count(comp)
            beh = _safe_dict(_safe_dict(comp.get("beh")).get(state))
            for entry in _safe_list(beh.get("emit")):
                if not isinstance(entry, dict):
                    continue
                pm = entry.get("per_min", 0)
                if not _is_number(pm):
                    pm = 0
                scope = entry.get("scope", "per_host")
                eid = entry.get("id", "?")
                ref = f"{cid}.{eid}"
                mult = 1.0
                if multipliers and ref in multipliers:
                    mult = multipliers[ref]
                effective_pm = pm * mult
                if scope == "per_host":
                    total += effective_pm * hc
                else:
                    total += effective_pm
        return total

    def _logs_per_min_flows(state: str, multipliers: dict[str, float] | None = None) -> float:
        total = 0.0
        for req in _safe_list(_safe_dict(flows_data.get(state)).get("req")):
            if not isinstance(req, dict):
                continue
            fid = req.get("id", "?")
            rpm = req.get("rpm", 0)
            if not _is_number(rpm):
                rpm = 0
            mult = 1.0
            if multipliers and fid in multipliers:
                mult = multipliers[fid]
            effective_rpm = rpm * mult
            emit_count = len(_safe_list(req.get("emit")))
            retry = _safe_dict(req.get("retry"))
            ea = retry.get("expected_attempts", 1)
            if not _is_number(ea):
                ea = 1
            retry_emit_count = len(_safe_list(retry.get("emit_per_retry")))
            total += effective_rpm * (ea * emit_count + max(0, ea - 1) * retry_emit_count)
        return total

    # Normal phase
    n_lpm = _logs_per_min_beh("n") + _logs_per_min_flows("n")
    normal_logs = n_lpm * n_duration

    # Failure phase: piecewise by events
    phases = _safe_dict(scenario.get("phases"))
    f_phase = _safe_dict(phases.get("f"))
    events = _safe_list(f_phase.get("events"))

    # Build intervals from events
    # Each event's rate_multipliers apply ONLY for its interval;
    # absent keys default to multiplier 1.0 (non-cumulative).
    intervals: list[tuple[float, float, dict[str, float]]] = []
    sorted_events = sorted(
        [e for e in events if isinstance(e, dict)],
        key=lambda e: e.get("at_min", 0),
    )

    for i, ev in enumerate(sorted_events):
        at_min = ev.get("at_min", f_start)
        if not _is_number(at_min):
            at_min = f_start
        next_at = f_end
        if i + 1 < len(sorted_events):
            next_at = sorted_events[i + 1].get("at_min", f_end)
            if not _is_number(next_at):
                next_at = f_end

        # Only this event's rate_multipliers (default 1.0 for absent)
        event_multipliers: dict[str, float] = {}
        rm = _safe_dict(ev.get("rate_multipliers"))
        for key, val in rm.items():
            if _is_number(val):
                event_multipliers[key] = val

        interval_dur = max(0, next_at - at_min)
        if interval_dur > 0:
            intervals.append((at_min, next_at, event_multipliers))

    failure_logs = 0.0
    for start, end, mults in intervals:
        dur = end - start
        lpm = _logs_per_min_beh("f", mults) + _logs_per_min_flows("f", mults)
        failure_logs += lpm * dur

    # One-shots
    one_shot_total = 0
    for ev in sorted_events:
        for os_entry in _safe_list(ev.get("one_shots")):
            if isinstance(os_entry, dict):
                count = os_entry.get("count", 0)
                if _is_int(count):
                    one_shot_total += count

    failure_logs += one_shot_total

    return normal_logs + failure_logs


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

ALL_ALGORITHMIC_CHECKS = [
    ("A1", _check_A1),
    ("A2", _check_A2),
    ("A3", _check_A3),
    ("A4", _check_A4),
    ("A5", _check_A5),
    ("A6", _check_A6),
    ("A7", _check_A7),
    ("A8", _check_A8),
    ("A9", _check_A9),
    ("A10", _check_A10),
    ("A11", _check_A11),
    ("A12", _check_A12),
    ("A13", _check_A13),
    ("A14", _check_A14),
    ("A15", _check_A15),
    ("A16", _check_A16),
    ("A17", _check_A17),
    ("A18", _check_A18),
    ("A19", _check_A19),
    ("A20", _check_A20),
    ("A21", _check_A21),
    ("A22", _check_A22),
    ("A23", _check_A23),
    ("A24", _check_A24),
    ("A25", _check_A25),
    ("A26", _check_A26),
    ("A27", _check_A27),
    ("A28", _check_A28),
    ("A29", _check_A29),
    ("A30", _check_A30),
    ("A31", _check_A31),
    ("A32", _check_A32),
    ("A33", _check_A33),
    ("A34", _check_A34),
]


def grade(sd_yaml: str, sc_yaml: str) -> list[dict[str, Any]]:
    """Run all algorithmic checks on system description and scenario YAMLs.

    Args:
        sd_yaml: Raw YAML string for the system description.
        sc_yaml: Raw YAML string for the scenario.

    Returns:
        List of violation dicts (only failed requirements).
        Each dict has keys: requirement, score, reason.
    """
    # Parse YAML
    sd, sd_error = _parse_yaml(sd_yaml, "system_description", "<system_description>")
    if sd_error is not None:
        return [sd_error]

    sc, sc_error = _parse_yaml(sc_yaml, "scenario", "<scenario>")
    if sc_error is not None:
        return [sc_error]

    # Build indexes
    comp_map: dict[str, dict] = {}
    for comp in _safe_list(sd.get("components")):
        if isinstance(comp, dict) and comp.get("id"):
            comp_map[comp["id"]] = comp

    flow_map_n: dict[str, dict] = {}
    flow_map_f: dict[str, dict] = {}
    flows = _safe_dict(sd.get("flows"))
    for req in _safe_list(_safe_dict(flows.get("n")).get("req")):
        if isinstance(req, dict) and req.get("id"):
            flow_map_n[req["id"]] = req
    for req in _safe_list(_safe_dict(flows.get("f")).get("req")):
        if isinstance(req, dict) and req.get("id"):
            flow_map_f[req["id"]] = req

    kwargs = {
        "sd": sd,
        "sc": sc,
        "comp_map": comp_map,
        "flow_map_n": flow_map_n,
        "flow_map_f": flow_map_f,
    }

    violations: list[dict[str, Any]] = []
    for req_id, check_fn in ALL_ALGORITHMIC_CHECKS:
        try:
            violations.extend(check_fn(**kwargs))
        except Exception as e:
            violations.append(
                _fail(req_id, f"grader internal error: {e}", req_id)
            )

    return violations
