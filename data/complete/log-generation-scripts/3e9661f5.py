import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "id": "filestore_control_plane",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["filestore_api", "internal_project_manager"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "filestore_api",
            "svc": "filestore-api",
            "hosts": ["fs-api-1", "fs-api-2", "fs-api-3"],
            "logs": {
                "access_list_instances_ok": {
                    "lvl": "INFO",
                    "msg": "request_id={request_id} caller={caller} rpc=ListInstances quota_bucket=read_global status=200 page_size={page_size} latency_ms={latency_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client", "svc_project_manager"]},
                        "page_size": {"k": "i", "v": [20, 1000]},
                        "latency_ms": {"k": "i", "v": [15, 250]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_list_instances_429": {
                    "lvl": "WARN",
                    "msg": "request_id={request_id} caller={caller} rpc=ListInstances quota_bucket=read_global status=429 reason=RESOURCE_EXHAUSTED latency_ms={latency_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client", "svc_project_manager"]},
                        "latency_ms": {"k": "i", "v": [5, 80]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_get_operation_ok": {
                    "lvl": "INFO",
                    "msg": "request_id={request_id} caller={caller} rpc=GetOperation op_id={op_id} quota_bucket=read_global status=200 latency_ms={latency_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client"]},
                        "op_id": {"k": "hex", "v": 16},
                        "latency_ms": {"k": "i", "v": [10, 220]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_get_operation_429": {
                    "lvl": "WARN",
                    "msg": "request_id={request_id} caller={caller} rpc=GetOperation op_id={op_id} quota_bucket=read_global status=429 reason=RESOURCE_EXHAUSTED latency_ms={latency_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client"]},
                        "op_id": {"k": "hex", "v": 16},
                        "latency_ms": {"k": "i", "v": [5, 90]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "create_instance_received": {
                    "lvl": "INFO",
                    "msg": "request_id={request_id} caller={caller} rpc=CreateInstance project={project} inst_id={inst_id} quota_bucket=write_global accepted=true trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client"]},
                        "project": {"k": "ch", "v": ["p-alpha", "p-beta", "p-gamma", "p-delta"]},
                        "inst_id": {"k": "ch", "v": ["fs-1", "fs-2", "fs-3", "fs-4"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_create_instance_accepted": {
                    "lvl": "INFO",
                    "msg": "request_id={request_id} caller={caller} rpc=CreateInstance quota_bucket=write_global status=202 op_id={op_id} latency_ms={latency_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "caller": {"k": "ch", "v": ["console_user", "gcloud_user", "api_client"]},
                        "op_id": {"k": "hex", "v": 16},
                        "latency_ms": {"k": "i", "v": [60, 2500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_server_metrics": {
                    "lvl": "INFO",
                    "msg": "metric=api_server in_flight={in_flight} cpu_pct={cpu_pct} p95_handler_ms={p95_handler_ms}",
                    "vars": {
                        "in_flight": {"k": "i", "v": [0, 450]},
                        "cpu_pct": {"k": "i", "v": [5, 95]},
                        "p95_handler_ms": {"k": "i", "v": [10, 3000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "api_server_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "api_server_metrics", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "global_quota",
            "svc": "filestore-quota",
            "hosts": ["quota-1"],
            "logs": {
                "quota_metrics": {
                    "lvl": "INFO",
                    "msg": "metric=global_quota bucket=read_global limit_rpm={limit_rpm} observed_rpm={observed_rpm} throttled_rpm={throttled_rpm}",
                    "vars": {"limit_rpm": {"k": "i", "v": [800, 800]}},
                    "state_vars": {
                        "n": {
                            "observed_rpm": {"k": "i", "v": [400, 700]},
                            "throttled_rpm": {"k": "i", "v": [0, 30]},
                        },
                        "f": {
                            "observed_rpm": {"k": "i", "v": [2200, 2300]},
                            "throttled_rpm": {"k": "i", "v": [2200, 2300]},
                        },
                    },
                },
                "throttle_gate_open": {
                    "lvl": "WARN",
                    "msg": "global_throttle active=true bucket=read_global limit_rpm={limit_rpm} observed_rpm={observed_rpm}",
                    "vars": {
                        "limit_rpm": {"k": "i", "v": [800, 800]},
                        "observed_rpm": {"k": "i", "v": [2100, 2400]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "quota_metrics", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "quota_metrics", "per_min": 1.0, "scope": "global"},
                        {"id": "throttle_gate_open", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "filestore_ops",
            "svc": "filestore-ops",
            "hosts": ["fs-ops-1", "fs-ops-2"],
            "logs": {
                "op_created": {
                    "lvl": "INFO",
                    "msg": "op_created op_id={op_id} kind={kind} project={project} inst_id={inst_id}",
                    "vars": {
                        "op_id": {"k": "hex", "v": 16},
                        "kind": {"k": "ch", "v": ["CreateInstance", "UpdateInstance", "CreateBackup"]},
                        "project": {"k": "ch", "v": ["p-alpha", "p-beta", "p-gamma", "p-delta"]},
                        "inst_id": {"k": "ch", "v": ["fs-1", "fs-2", "fs-3", "fs-4"]},
                    },
                },
                "op_status_fetched": {
                    "lvl": "DEBUG",
                    "msg": "op_status op_id={op_id} state={state} age_s={age_s}",
                    "vars": {
                        "op_id": {"k": "hex", "v": 16},
                        "state": {"k": "ch", "v": ["PENDING", "RUNNING", "DONE"]},
                        "age_s": {"k": "i", "v": [0, 7200]},
                    },
                },
                "ops_heartbeat": {
                    "lvl": "INFO",
                    "msg": "metric=ops_worker heartbeat=true queue_depth={queue_depth}",
                    "vars": {"queue_depth": {"k": "i", "v": [0, 2000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ops_heartbeat", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "ops_heartbeat", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "internal_project_manager",
            "svc": "project-manager-internal",
            "hosts": ["proj-mgr-1", "proj-mgr-2"],
            "logs": {
                "sweep_cycle": {
                    "lvl": "INFO",
                    "msg": "sweep_cycle job_id={job_id} projects_scanned={projects_scanned} filestore_api_calls={api_calls}",
                    "vars": {"job_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {
                            "projects_scanned": {"k": "i", "v": [80, 250]},
                            "api_calls": {"k": "i", "v": [15, 25]},
                        },
                        "f": {
                            "projects_scanned": {"k": "i", "v": [800, 1800]},
                            "api_calls": {"k": "i", "v": [110, 150]},
                        },
                    },
                },
                "rapid_reconcile_entered": {
                    "lvl": "WARN",
                    "msg": "abnormal_loop entered=true job_id={job_id} reason={reason}",
                    "vars": {
                        "job_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["scheduler_bug", "stuck_retry", "project_inventory_corruption"]},
                    },
                },
                "pause_internal_service": {
                    "lvl": "INFO",
                    "msg": "operator_action action=pause_service service=project-manager-internal requested_by={requested_by} ticket={ticket}",
                    "vars": {
                        "requested_by": {"k": "ch", "v": ["oncall_sre", "control_plane_eng"]},
                        "ticket": {"k": "ch", "v": ["INC-1842", "INC-1843", "INC-1844"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "sweep_cycle", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "sweep_cycle", "per_min": 6.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "control-plane-monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "synthetic_check_ok": {
                    "lvl": "INFO",
                    "msg": "synthetic_check check_id={check_id} rpc=ListInstances status=200 latency_ms={latency_ms}",
                    "vars": {"check_id": {"k": "ch", "v": ["ro_list_instances_global"]}, "latency_ms": {"k": "i", "v": [20, 300]}},
                },
                "synthetic_check_fail_429": {
                    "lvl": "ERROR",
                    "msg": "synthetic_check check_id={check_id} rpc=ListInstances status=429 error=RESOURCE_EXHAUSTED latency_ms={latency_ms}",
                    "vars": {"check_id": {"k": "ch", "v": ["ro_list_instances_global"]}, "latency_ms": {"k": "i", "v": [5, 120]}},
                },
                "alert_throttle_high": {
                    "lvl": "CRITICAL",
                    "msg": "alert name=filestore_read_throttling_high bucket=read_global severity=critical",
                    "vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "synthetic_check_ok", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "synthetic_check_fail_429", "per_min": 1.0, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "list_instances_ok",
                    "rpm": 200.0,
                    "emit": ["filestore_api.access_list_instances_ok"],
                    "latency_ms": [[20, 140]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "get_operation_ok",
                    "rpm": 250.0,
                    "emit": ["filestore_ops.op_status_fetched", "filestore_api.access_get_operation_ok"],
                    "latency_ms": [[5, 25], [15, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "create_instance_mutate",
                    "rpm": 30.0,
                    "emit": [
                        "filestore_api.create_instance_received",
                        "filestore_ops.op_created",
                        "filestore_api.access_create_instance_accepted",
                    ],
                    "latency_ms": [[5, 25], [10, 60], [80, 900]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "internal_project_sync_ok",
                    "rpm": 20.0,
                    "emit": ["filestore_api.access_list_instances_ok"],
                    "latency_ms": [[25, 160]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "list_instances_429",
                    "rpm": 200.0,
                    "emit": ["filestore_api.access_list_instances_429"],
                    "latency_ms": [[8, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "get_operation_429",
                    "rpm": 250.0,
                    "emit": ["filestore_api.access_get_operation_429"],
                    "latency_ms": [[8, 70]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "create_instance_mutate",
                    "rpm": 30.0,
                    "emit": [
                        "filestore_api.create_instance_received",
                        "filestore_ops.op_created",
                        "filestore_api.access_create_instance_accepted",
                    ],
                    "latency_ms": [[8, 40], [15, 90], [120, 1800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "internal_project_sync_429",
                    "rpm": 1500.0,
                    "emit": ["filestore_api.access_list_instances_429"],
                    "latency_ms": [[6, 50]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "filestore_global_read_throttle_2022_09_13",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {},
                    "latency_multipliers": {"create_instance_mutate": {"p50": 1.5, "p95": 2.0}},
                    "one_shots": [{"ref": "internal_project_manager.rapid_reconcile_entered", "count": 1, "hosts": ["proj-mgr-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 27,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "monitoring.alert_throttle_high", "count": 1, "hosts": ["mon-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 45,
                    "rate_multipliers": {
                        "internal_project_sync_429": 0.3,
                        "internal_project_manager.sweep_cycle": 0.3,
                        "global_quota.throttle_gate_open": 0.6,
                    },
                    "latency_multipliers": {"create_instance_mutate": {"p50": 1.2, "p95": 1.5}},
                    "one_shots": [{"ref": "internal_project_manager.pause_internal_service", "count": 1, "hosts": ["proj-mgr-1"]}],
                },
            ]
        }
    },
}

SEED_SALT = "filestore_global_read_throttle_2022_09_13|deterministic|v1"
BASE_TIME = datetime(2022, 9, 13, 0, 0, 0, tzinfo=timezone.utc)
NORM = NormalDist()


def md5_bytes(s: str) -> bytes:
    return hashlib.md5((SEED_SALT + "|" + s).encode("utf-8")).digest()


def md5_hex(s: str) -> str:
    return hashlib.md5((SEED_SALT + "|" + s).encode("utf-8")).hexdigest()


def u01_from_key(key: str) -> float:
    b = md5_bytes(key)
    x = int.from_bytes(b, "big")
    return x / 2**128


def clamp01(u: float) -> float:
    return min(1.0 - 1e-12, max(1e-12, u))


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    u = u01_from_key("round|" + key)
    return base + (1 if u < frac else 0)


def make_hex(n: int, key: str) -> str:
    h = md5_hex("hex|" + key)
    if n <= len(h):
        return h[:n]
    out = h
    i = 1
    while len(out) < n:
        out += md5_hex(f"hex|{key}|{i}")
        i += 1
    return out[:n]


def make_uuid(key: str) -> str:
    b = bytearray(md5_bytes("uuid|" + key))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    hexs = b.hex()
    return f"{hexs[0:8]}-{hexs[8:12]}-{hexs[12:16]}-{hexs[16:20]}-{hexs[20:32]}"


def choose_int(lo: int, hi: int, key: str) -> int:
    if lo == hi:
        return lo
    u = u01_from_key("int|" + key)
    return lo + int(u * (hi - lo + 1))


def choose_float(lo: float, hi: float, key: str) -> float:
    if lo == hi:
        return lo
    u = u01_from_key("float|" + key)
    return lo + u * (hi - lo)


def choose_choice(choices: List[Any], key: str) -> Any:
    if not choices:
        return ""
    u = u01_from_key("ch|" + key)
    idx = int(u * len(choices))
    if idx >= len(choices):
        idx = len(choices) - 1
    return choices[idx]


def dt_to_iso_z(dt: datetime) -> str:
    ms = int(dt.microsecond / 1000)
    dt2 = dt.replace(microsecond=ms * 1000)
    return dt2.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def lognormal_quantile_ms(p50_ms: float, p95_ms: float, u: float) -> float:
    p50_ms = max(1e-6, p50_ms)
    p95_ms = max(p50_ms * 1.000001, p95_ms)
    mu = math.log(p50_ms)
    sigma = (math.log(p95_ms) - mu) / 1.6448536269514722
    z = NORM.inv_cdf(clamp01(u))
    x = math.exp(mu + sigma * z)
    return float(x)


def bounded_lognormal_quantile_ms(p50_ms: float, p95_ms: float, u: float, cap_mult: float = 3.0) -> float:
    # Soft-cap the heavy tail to keep per-step deltas stable and within downstream template domains.
    x = lognormal_quantile_ms(p50_ms, p95_ms, u)
    cap = max(1.0, float(cap_mult) * max(1.0, float(p95_ms)))
    return float(min(x, cap))


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Dict[str, Any]]


def build_indices() -> Tuple[Dict[str, Any], Dict[str, LogTemplate], Dict[str, Dict[str, Any]]]:
    comp_by_id: Dict[str, Any] = {}
    log_by_ref: Dict[str, LogTemplate] = {}
    flow_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}

    for c in SYSTEM["components"]:
        comp_by_id[c["id"]] = c
        for log_id, ld in c.get("logs", {}).items():
            ref = f'{c["id"]}.{log_id}'
            log_by_ref[ref] = LogTemplate(
                component_id=c["id"],
                log_id=log_id,
                level=ld["lvl"],
                msg=ld["msg"],
                vars=ld.get("vars", {}),
                state_vars=ld.get("state_vars", {}),
            )

    for state in ["n", "f"]:
        for f in SYSTEM["flows"][state]["req"]:
            flow_by_state[state][f["id"]] = f

    return comp_by_id, log_by_ref, flow_by_state


COMP_BY_ID, LOG_BY_REF, FLOW_BY_STATE = build_indices()


def get_var_domain(t: LogTemplate, state: str, var_name: str) -> Optional[Dict[str, Any]]:
    if var_name in t.vars:
        return t.vars[var_name]
    if t.state_vars and state in t.state_vars and var_name in t.state_vars[state]:
        return t.state_vars[state][var_name]
    return None


def get_int_domain_bounds(t: LogTemplate, state: str, var_name: str) -> Optional[Tuple[int, int]]:
    dom = get_var_domain(t, state, var_name)
    if not dom or dom.get("k") != "i":
        return None
    lo, hi = dom.get("v", [0, 0])
    return int(lo), int(hi)


def render_log_message(ref: str, state: str, bound: Dict[str, Any], key: str) -> str:
    t = LOG_BY_REF[ref]
    vals: Dict[str, Any] = {}
    all_var_names = set(t.vars.keys())
    if t.state_vars and state in t.state_vars:
        all_var_names |= set(t.state_vars[state].keys())

    for vn in all_var_names:
        if vn in bound:
            vals[vn] = bound[vn]
            continue
        dom = get_var_domain(t, state, vn)
        if not dom:
            vals[vn] = ""
            continue
        k = dom["k"]
        v = dom.get("v", None)
        if k == "uuid":
            vals[vn] = make_uuid(f"{key}|{ref}|{vn}")
        elif k == "hex":
            vals[vn] = make_hex(int(v), f"{key}|{ref}|{vn}")
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            vals[vn] = choose_int(lo, hi, f"{key}|{ref}|{vn}")
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            vals[vn] = choose_float(lo, hi, f"{key}|{ref}|{vn}")
        elif k == "ch":
            vals[vn] = choose_choice(list(v), f"{key}|{ref}|{vn}")
        else:
            vals[vn] = str(v) if v is not None else ""
    return t.msg.format(**vals)


def service_and_hosts(component_id: str) -> Tuple[str, List[str]]:
    c = COMP_BY_ID[component_id]
    return c.get("svc", "") or "", c.get("hosts", []) or []


def choose_host(component_id: str, chain_key: str) -> str:
    _, hosts = service_and_hosts(component_id)
    if not hosts:
        return ""
    return hosts[int(u01_from_key(f"host|{component_id}|{chain_key}") * len(hosts)) % len(hosts)]


def schedule_times(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_s = (end_dt - start_dt).total_seconds()
    if duration_s <= 0:
        return [start_dt for _ in range(count)]
    spacing = duration_s / count
    jitter_max = min(0.15, 0.30 * spacing)
    out: List[datetime] = []
    for i in range(count):
        pos = (i + 0.5) / count
        t = start_dt + timedelta(seconds=pos * duration_s)
        u = u01_from_key(f"jitter|{key}|{i}")
        jitter = (u - 0.5) * 2.0 * jitter_max
        t2 = t + timedelta(seconds=jitter)
        if t2 < start_dt:
            t2 = start_dt
        if t2 >= end_dt:
            t2 = end_dt - timedelta(milliseconds=1)
        out.append(t2)
    return out


@dataclass
class Segment:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_failure_segments() -> List[Segment]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted(set([fstart] + [e["at_min"] for e in events] + [fend]))

    rate_mult: Dict[str, float] = {}
    latency_mult: Dict[str, Dict[str, float]] = {}
    segs: List[Segment] = []

    events_by_at: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_at.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]
        for e in events_by_at.get(start, []):
            for k, v in e.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for flow_id, lm in e.get("latency_multipliers", {}).items():
                latency_mult[flow_id] = {"p50": float(lm["p50"]), "p95": float(lm["p95"])}
        segs.append(Segment(state="f", start_min=int(start), end_min=int(end), rate_mult=dict(rate_mult), latency_mult=dict(latency_mult)))
    return segs


def get_one_shots() -> List[Dict[str, Any]]:
    events = SCENARIO["phases"]["f"]["events"]
    out: List[Dict[str, Any]] = []
    for e in events:
        for os in e.get("one_shots", []):
            out.append({"at_min": int(e["at_min"]), "ref": os["ref"], "count": int(os["count"]), "hosts": os.get("hosts", None)})
    return out


FAILURE_SEGS = build_failure_segments()
ONE_SHOTS = get_one_shots()


def flow_is_read_global(flow: Dict[str, Any]) -> bool:
    for ref in flow.get("emit", []):
        tmpl = LOG_BY_REF.get(ref)
        if tmpl and "quota_bucket=read_global" in tmpl.msg:
            return True
    return False


def modeled_read_rpm_for_segment(seg: Segment) -> float:
    total = 0.0
    for flow in SYSTEM["flows"][seg.state]["req"]:
        if not flow_is_read_global(flow):
            continue
        mult = 1.0
        if seg.state == "f":
            mult = float(seg.rate_mult.get(flow["id"], 1.0))
        total += float(flow["rpm"]) * mult
    return total


def lookahead_total_hi_cap(flow_emit: List[str], state: str, li: int) -> Optional[float]:
    """
    Return a cap on the *total elapsed* (ms) at the end of log index li so that
    future logs that carry a latency_ms domain can still remain within their max.

    We assume each remaining inter-log gap can be >= 1ms, so to satisfy a future
    max hi at position j, after step li we need:
      total_after_li <= hi_j - (j-li)*1ms
    """
    cap: Optional[float] = None
    for j in range(li, len(flow_emit)):
        tmpl = LOG_BY_REF[flow_emit[j]]
        bounds = get_int_domain_bounds(tmpl, state, "latency_ms")
        if bounds is None:
            continue
        _, hi = bounds
        min_add = 1.0 * (j - li)
        cap_j = float(hi) - min_add
        cap = cap_j if cap is None else min(cap, cap_j)
    if cap is None:
        return None
    return max(0.0, cap)


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_dt: datetime,
    seg_latency_mult: Dict[str, Dict[str, float]],
    instance_key: str,
) -> List[Dict[str, Any]]:
    flow_id = flow["id"]
    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow.get("trace", False):
        trace_id = make_hex(32, f"trace|{state}|{flow_id}|{instance_key}")

    request_id = make_uuid(f"req|{state}|{flow_id}|{instance_key}")

    if flow_id.startswith("internal_project_sync"):
        caller = "svc_project_manager"
    else:
        choices = ["console_user", "gcloud_user", "api_client"]
        caller = choose_choice(choices, f"caller|{state}|{flow_id}|{instance_key}")

    op_id = make_hex(16, f"op|{state}|{flow_id}|{instance_key}") if ("operation" in flow_id or "create_instance" in flow_id) else None
    project = choose_choice(["p-alpha", "p-beta", "p-gamma", "p-delta"], f"project|{state}|{flow_id}|{instance_key}")
    inst_id = choose_choice(["fs-1", "fs-2", "fs-3", "fs-4"], f"inst|{state}|{flow_id}|{instance_key}")

    # Bind operation kind coherently with the request meaning.
    if flow_id == "create_instance_mutate":
        kind = "CreateInstance"
    else:
        kind = choose_choice(["CreateInstance", "UpdateInstance", "CreateBackup"], f"kind|{state}|{flow_id}|{instance_key}")

    comp_host: Dict[str, str] = {}

    lm = seg_latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    p50m, p95m = float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0))

    t = start_dt
    cumulative_ms = 0.0  # elapsed since start_dt for this attempt (max_attempts is 1 in this model)
    rows: List[Dict[str, Any]] = []

    for li, ref in enumerate(flow["emit"]):
        tmpl = LOG_BY_REF[ref]
        comp_id = tmpl.component_id
        if comp_id not in comp_host:
            comp_host[comp_id] = choose_host(comp_id, trace_id or request_id or instance_key)
        svc, _ = service_and_hosts(comp_id)

        # Sample the inter-log delay (delta since previous emitted log in this attempt) with a soft cap.
        p50, p95 = flow["latency_ms"][li]
        p50_s = float(p50) * (p50m if state == "f" else 1.0)
        p95_s = float(p95) * (p95m if state == "f" else 1.0)
        u = u01_from_key(f"lat|{state}|{flow_id}|{instance_key}|{li}")
        delta_ms = bounded_lognormal_quantile_ms(p50_s, p95_s, u, cap_mult=3.0)
        delta_ms = max(0.0, float(delta_ms))

        proposed_total = cumulative_ms + delta_ms

        # Lookahead: if a later log carries a latency_ms domain, ensure this step doesn't "spend" so much time
        # that the later access log cannot stay within its max.
        cap_total = lookahead_total_hi_cap(flow["emit"], state, li)
        if cap_total is not None:
            proposed_total = min(proposed_total, cap_total)

        # If this log template carries latency_ms, interpret it as total elapsed since request start.
        # Enforce that total within its own domain by adjusting the timeline (not clamping message).
        lat_bounds = get_int_domain_bounds(tmpl, state, "latency_ms")
        if lat_bounds is not None:
            lo, hi = lat_bounds
            # hi is already considered in lookahead for current log, but apply again for clarity.
            proposed_total = min(proposed_total, float(hi))
            proposed_total = max(proposed_total, float(lo))
            # If a lookahead cap exists and conflicts (rare), prioritize feasibility.
            if cap_total is not None and proposed_total > cap_total:
                proposed_total = cap_total

        delta_ms = proposed_total - cumulative_ms
        if delta_ms < 0.0:
            delta_ms = 0.0

        cumulative_ms += delta_ms
        t = t + timedelta(milliseconds=float(delta_ms))

        bound: Dict[str, Any] = {"request_id": request_id, "caller": caller}
        if trace_id:
            bound["trace_id"] = trace_id
        if op_id is not None:
            bound["op_id"] = op_id
        bound["project"] = project
        bound["inst_id"] = inst_id
        bound["kind"] = kind

        if lat_bounds is not None:
            bound["latency_ms"] = int(round(cumulative_ms))

        msg = render_log_message(ref, state, bound, key=f"flowmsg|{state}|{flow_id}|{instance_key}|{li}")

        rows.append(
            {
                "timestamp_dt": t,
                "level": tmpl.level,
                "message": msg,
                "trace_id": trace_id,
                "service": svc,
                "host": comp_host[comp_id],
            }
        )
    return rows


def emit_background_for_segment(seg: Segment, rows: List[Dict[str, Any]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=seg.start_min)
    end_dt = BASE_TIME + timedelta(minutes=seg.end_min)
    duration_min = seg.end_min - seg.start_min

    seg_modeled_read_rpm = modeled_read_rpm_for_segment(seg)
    seg_modeled_read_rpm_int = int(math.ceil(seg_modeled_read_rpm - 1e-12))

    for comp in SYSTEM["components"]:
        comp_id = comp["id"]
        beh = comp.get("beh", {}).get(seg.state, {})
        emits = beh.get("emit", [])
        for e in emits:
            log_id = e["id"]
            ref = f"{comp_id}.{log_id}"
            tmpl = LOG_BY_REF[ref]
            svc, hosts = service_and_hosts(comp_id)
            scope = e.get("scope", "per_host")
            base_rate = float(e["per_min"])

            mult = 1.0
            if seg.state == "f":
                mult = float(seg.rate_mult.get(ref, 1.0))
            eff_rate = base_rate * mult

            if scope == "global":
                cnt = stable_round(eff_rate * duration_min, key=f"bg|{seg.state}|{ref}|{seg.start_min}")
                times = schedule_times(start_dt, end_dt, cnt, key=f"bgtime|{seg.state}|{ref}|{seg.start_min}")
                host = hosts[0] if hosts else ""
                for i, ts in enumerate(times):
                    bound: Dict[str, Any] = {}
                    if ref == "global_quota.quota_metrics":
                        bound["limit_rpm"] = 800

                        obs_bounds = get_int_domain_bounds(tmpl, seg.state, "observed_rpm")
                        thr_bounds = get_int_domain_bounds(tmpl, seg.state, "throttled_rpm")

                        if seg.state == "f":
                            lo, hi = obs_bounds if obs_bounds else (2200, 2300)
                            obs = choose_int(lo, hi, f"qm_obs|{seg.start_min}|{i}")
                            bound["observed_rpm"] = obs
                            bound["throttled_rpm"] = obs
                        else:
                            lo, hi = obs_bounds if obs_bounds else (400, 700)
                            eff_lo = max(lo, seg_modeled_read_rpm_int)
                            if eff_lo <= hi:
                                obs = choose_int(eff_lo, hi, f"qm_obs|{seg.start_min}|{i}")
                            else:
                                obs = eff_lo
                            bound["observed_rpm"] = obs

                            tlo, thi = thr_bounds if thr_bounds else (0, 30)
                            thr = choose_int(tlo, thi, f"qm_thr|{seg.start_min}|{i}")
                            bound["throttled_rpm"] = min(thr, obs)

                    msg = render_log_message(ref, seg.state, bound, key=f"bgmsg|{seg.state}|{ref}|{seg.start_min}|{i}")
                    rows.append(
                        {
                            "timestamp_dt": ts,
                            "level": tmpl.level,
                            "message": msg,
                            "trace_id": "",
                            "service": svc,
                            "host": host,
                        }
                    )
            else:
                for host in hosts:
                    cnt = stable_round(eff_rate * duration_min, key=f"bg|{seg.state}|{ref}|{host}|{seg.start_min}")
                    times = schedule_times(start_dt, end_dt, cnt, key=f"bgtime|{seg.state}|{ref}|{host}|{seg.start_min}")
                    for i, ts in enumerate(times):
                        msg = render_log_message(ref, seg.state, {}, key=f"bgmsg|{seg.state}|{ref}|{host}|{seg.start_min}|{i}")
                        rows.append(
                            {
                                "timestamp_dt": ts,
                                "level": tmpl.level,
                                "message": msg,
                                "trace_id": "",
                                "service": svc,
                                "host": host,
                            }
                        )


def emit_flows_for_segment(seg: Segment, rows: List[Dict[str, Any]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=seg.start_min)
    end_dt = BASE_TIME + timedelta(minutes=seg.end_min)
    duration_min = seg.end_min - seg.start_min

    for flow in SYSTEM["flows"][seg.state]["req"]:
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if seg.state == "f":
            mult = float(seg.rate_mult.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        inst_cnt = stable_round(eff_rpm * duration_min, key=f"flow|{seg.state}|{flow_id}|{seg.start_min}")
        start_times = schedule_times(start_dt, end_dt, inst_cnt, key=f"flowtime|{seg.state}|{flow_id}|{seg.start_min}")
        for i, st in enumerate(start_times):
            inst_key = f"{seg.start_min}-{seg.end_min}|{flow_id}|{i}"
            rows.extend(simulate_flow_instance(flow, seg.state, st, seg.latency_mult, inst_key))


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    for os in ONE_SHOTS:
        at_min = int(os["at_min"])
        ref = os["ref"]
        cnt = int(os["count"])
        tmpl = LOG_BY_REF[ref]
        comp_id = tmpl.component_id
        svc, hosts = service_and_hosts(comp_id)

        allowed_hosts = os.get("hosts") or hosts
        allowed_hosts = allowed_hosts if allowed_hosts else [""]

        base_dt = BASE_TIME + timedelta(minutes=at_min)
        for i in range(cnt):
            u = u01_from_key(f"oneshot|{ref}|{at_min}|{i}")
            ts = base_dt + timedelta(milliseconds=int(u * 900.0))
            host = allowed_hosts[i % len(allowed_hosts)]
            msg = render_log_message(ref, "f", {}, key=f"oneshotmsg|{ref}|{at_min}|{i}")
            rows.append(
                {
                    "timestamp_dt": ts,
                    "level": tmpl.level,
                    "message": msg,
                    "trace_id": "",
                    "service": svc,
                    "host": host,
                }
            )


def main() -> None:
    random.seed(0)
    np.random.seed(0)

    rows: List[Dict[str, Any]] = []

    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    normal_seg = Segment(state="n", start_min=int(nstart), end_min=int(nend), rate_mult={}, latency_mult={})
    emit_background_for_segment(normal_seg, rows)
    emit_flows_for_segment(normal_seg, rows)

    for seg in FAILURE_SEGS:
        emit_background_for_segment(seg, rows)
        emit_flows_for_segment(seg, rows)

    emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].apply(dt_to_iso_z)
    df = df.drop(columns=["timestamp_dt"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"logs.csv row count out of target range: {nrows}")


if __name__ == "__main__":
    main()
