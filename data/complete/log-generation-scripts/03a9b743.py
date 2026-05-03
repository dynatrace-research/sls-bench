import math
import hashlib
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Deterministic seeds (required by verifier, even though generation uses md5-based determinism)
random.seed(0)
np.random.seed(0)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "dynamodb_membership_us_east"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_frontend"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_frontend",
            "svc": "dynamodb-api",
            "hosts": ["api-01", "api-02"],
            "logs": {
                "req_in": {
                    "lvl": "INFO",
                    "msg": "req {trace_id} op={op} table={table} key_hash={key_hash}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "op": {"k": "ch", "v": ["GetItem", "PutItem", "UpdateItem", "Query"]},
                        "table": {"k": "ch", "v": ["users", "orders", "sessions", "inventory"]},
                        "key_hash": {"k": "hex", "v": 16},
                    },
                },
                "resp_ok": {
                    "lvl": "INFO",
                    "msg": "resp {trace_id} status=200 dur_ms={dur_ms} bytes={bytes} storage={storage}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "dur_ms": {"k": "i", "v": [2, 60]},
                        "bytes": {"k": "i", "v": [200, 8000]},
                        "storage": {"k": "ch", "v": ["stor-01", "stor-02", "stor-03", "stor-04"]},
                    },
                },
                "resp_err": {
                    "lvl": "ERROR",
                    "msg": "resp {trace_id} status={status} err={err} dur_ms={dur_ms} storage={storage}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "status": {"k": "ch", "v": [500, 503, 504]},
                        "err": {"k": "ch", "v": ["StorageUnavailable", "UpstreamTimeout", "InternalError"]},
                        "dur_ms": {"k": "i", "v": [2, 2500]},
                        "storage": {"k": "ch", "v": ["stor-01", "stor-02", "stor-03", "stor-04"]},
                    },
                },
                "access_summary": {
                    "lvl": "INFO",
                    "msg": "access_summary rps={rps} err_pct={err_pct}",
                    "vars": {"rps": {"k": "f", "v": [2.0, 12.0]}},
                    "state_vars": {
                        "n": {"err_pct": {"k": "f", "v": [0.0, 2.0]}},
                        "f": {"err_pct": {"k": "f", "v": [5.0, 80.0]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "access_summary", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "access_summary", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "storage_node",
            "svc": "dynamodb-storage",
            "hosts": ["stor-01", "stor-02", "stor-03", "stor-04"],
            "logs": {
                "serve_ok": {
                    "lvl": "INFO",
                    "msg": "served {trace_id} op={op} table={table} part={part} dur_ms={dur_ms}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "op": {"k": "ch", "v": ["GetItem", "PutItem", "UpdateItem", "Query"]},
                        "table": {"k": "ch", "v": ["users", "orders", "sessions", "inventory"]},
                        "part": {"k": "i", "v": [0, 2047]},
                        "dur_ms": {"k": "i", "v": [1, 40]},
                    },
                },
                "serve_reject": {
                    "lvl": "WARN",
                    "msg": "reject {trace_id} op={op} reason=membership_unavailable",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "op": {"k": "ch", "v": ["GetItem", "PutItem", "UpdateItem", "Query"]},
                    },
                },
                "membership_req": {
                    "lvl": "INFO",
                    "msg": "membership_fetch_start req_id={req_id} reason={reason} meta={meta}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["periodic", "net_recover", "startup"]},
                        "meta": {"k": "ch", "v": ["meta-01", "meta-02"]},
                    },
                },
                "membership_applied": {
                    "lvl": "INFO",
                    "msg": "membership_applied req_id={req_id} partitions={partitions} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "partitions": {"k": "i", "v": [50, 2500]},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                    },
                },
                "membership_retry": {
                    "lvl": "WARN",
                    "msg": "membership_fetch_retry req_id={req_id} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 4]},
                        "backoff_ms": {"k": "i", "v": [50, 1500]},
                    },
                },
                "membership_timeout": {
                    "lvl": "ERROR",
                    "msg": "membership_fetch_timeout req_id={req_id} waited_ms={waited_ms} deadline_ms={deadline_ms} disqualify_for_s={disqualify_for_s}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "waited_ms": {"k": "i", "v": [1500, 2200]},
                        "deadline_ms": {"k": "i", "v": [1500, 1500]},
                        "disqualify_for_s": {"k": "i", "v": [20, 240]},
                    },
                },
                "net_disruption": {
                    "lvl": "WARN",
                    "msg": "network_disruption detected=true link_reset_count={link_reset_count}",
                    "vars": {"link_reset_count": {"k": "i", "v": [1, 20]}},
                },
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat ok=true eligible={eligible}",
                    "vars": {},
                    "state_vars": {
                        "n": {"eligible": {"k": "ch", "v": ["true"]}},
                        "f": {"eligible": {"k": "ch", "v": ["true", "false"]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 0.3, "scope": "per_host"}]},
                "f": {"emit": [{"id": "heartbeat", "per_min": 0.3, "scope": "per_host"}]},
            },
        },
        {
            "id": "metadata_service",
            "svc": "dynamodb-metadata",
            "hosts": ["meta-01", "meta-02"],
            "logs": {
                "queue_metrics": {
                    "lvl": "INFO",
                    "msg": "load queue_depth={queue_depth} inflight={inflight} p95_build_ms={p95_build_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "queue_depth": {"k": "i", "v": [0, 30]},
                            "inflight": {"k": "i", "v": [0, 20]},
                            "p95_build_ms": {"k": "i", "v": [40, 350]},
                        },
                        "f": {
                            "queue_depth": {"k": "i", "v": [5, 300]},
                            "inflight": {"k": "i", "v": [2, 80]},
                            "p95_build_ms": {"k": "i", "v": [200, 3000]},
                        },
                    },
                },
                "membership_resp": {
                    "lvl": "INFO",
                    "msg": "membership_resp req_id={req_id} storage={storage} bytes={bytes} build_ms={build_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "storage": {"k": "ch", "v": ["stor-01", "stor-02", "stor-03", "stor-04"]},
                    },
                    "state_vars": {
                        "n": {"bytes": {"k": "i", "v": [20000, 120000]}, "build_ms": {"k": "i", "v": [40, 300]}},
                        "f": {"bytes": {"k": "i", "v": [120000, 700000]}, "build_ms": {"k": "i", "v": [150, 1600]}},
                    },
                },
                "slow_build": {
                    "lvl": "WARN",
                    "msg": "slow_membership_build req_id={req_id} storage={storage} bytes={bytes} elapsed_ms={elapsed_ms} partitions={partitions}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "storage": {"k": "ch", "v": ["stor-01", "stor-02", "stor-03", "stor-04"]},
                        "bytes": {"k": "i", "v": [200000, 1200000]},
                        "elapsed_ms": {"k": "i", "v": [600, 1500]},
                        "partitions": {"k": "i", "v": [200, 3000]},
                    },
                },
                "admin_api_err": {
                    "lvl": "ERROR",
                    "msg": "admin_api action={action} status={status} reason={reason}",
                    "vars": {
                        "action": {"k": "ch", "v": ["scale_capacity"]},
                        "status": {"k": "ch", "v": [503, 504]},
                        "reason": {"k": "ch", "v": ["overloaded", "timeout"]},
                    },
                },
                "admin_api_ok": {
                    "lvl": "INFO",
                    "msg": "admin_api action={action} status=200 new_capacity={new_capacity}",
                    "vars": {"action": {"k": "ch", "v": ["scale_capacity"]}, "new_capacity": {"k": "i", "v": [6, 16]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "queue_metrics", "per_min": 2.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "queue_metrics", "per_min": 6.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "metadata_kvstore",
            "svc": "dynamodb-metadata-store",
            "hosts": ["meta-db-01"],
            "logs": {
                "latency_metrics": {
                    "lvl": "INFO",
                    "msg": "kv_latency p95_ms={p95_ms} read_qps={read_qps}",
                    "vars": {},
                    "state_vars": {
                        "n": {"p95_ms": {"k": "i", "v": [2, 15]}, "read_qps": {"k": "i", "v": [50, 600]}},
                        "f": {"p95_ms": {"k": "i", "v": [5, 80]}, "read_qps": {"k": "i", "v": [50, 3000]}},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "latency_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "latency_metrics", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "ops_control",
            "svc": "dynamodb-ops",
            "hosts": ["ops-01"],
            "logs": {
                "scale_capacity_cmd": {
                    "lvl": "WARN",
                    "msg": "operator_cmd action=scale_capacity from={from_cap} to={to_cap} req_id={req_id}",
                    "vars": {
                        "from_cap": {"k": "i", "v": [2, 8]},
                        "to_cap": {"k": "i", "v": [6, 16]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "scale_capacity_applied": {
                    "lvl": "INFO",
                    "msg": "operator_done action=scale_capacity from={from_cap} to={to_cap}",
                    "vars": {"from_cap": {"k": "i", "v": [2, 8]}, "to_cap": {"k": "i", "v": [6, 16]}},
                },
                "pause_membership_requests": {
                    "lvl": "WARN",
                    "msg": "operator_action action=pause_membership_fetch duration_min={duration_min}",
                    "vars": {"duration_min": {"k": "i", "v": [5, 60]}},
                },
                "audit": {
                    "lvl": "INFO",
                    "msg": "audit actor=oncall activity={activity}",
                    "vars": {"activity": {"k": "ch", "v": ["review_metrics", "runbook_step", "open_ticket"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "audit", "per_min": 0.1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "audit", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "customer_rw",
                    "rpm": 350.0,
                    "emit": ["api_frontend.req_in", "storage_node.serve_ok", "api_frontend.resp_ok"],
                    "latency_ms": [[1, 2], [4, 15], [1, 2]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "membership_renewal",
                    "rpm": 120.0,
                    "emit": ["storage_node.membership_req", "metadata_service.membership_resp", "storage_node.membership_applied"],
                    "latency_ms": [[1, 2], [80, 250], [5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "customer_rw_ok_f",
                    "rpm": 175.0,
                    "emit": ["api_frontend.req_in", "storage_node.serve_ok", "api_frontend.resp_ok"],
                    "latency_ms": [[1, 2], [6, 35], [1, 2]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "customer_rw_err_f",
                    "rpm": 175.0,
                    "emit": ["api_frontend.req_in", "storage_node.serve_reject", "api_frontend.resp_err"],
                    "latency_ms": [[1, 2], [1, 5], [2, 40]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "membership_renewal_success_f",
                    "rpm": 140.0,
                    "emit": ["storage_node.membership_req", "metadata_service.membership_resp", "storage_node.membership_applied"],
                    "latency_ms": [[1, 2], [250, 1200], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "membership_renewal_timeout_f",
                    "rpm": 220.0,
                    "emit": ["storage_node.membership_req", "metadata_service.slow_build", "storage_node.membership_timeout"],
                    "latency_ms": [[1, 2], [700, 1200], [800, 1000]],
                    "retry": {
                        "max_attempts": 4,
                        "expected_attempts": 2.0,
                        "emit_per_retry": ["storage_node.membership_retry"],
                        "backoff_ms": [[100, 300], [300, 900], [600, 1500]],
                    },
                    "trace": False,
                },
                {
                    "id": "admin_scale_attempt_fail_f",
                    "rpm": 0.2,
                    "emit": ["ops_control.scale_capacity_cmd", "metadata_service.admin_api_err"],
                    "latency_ms": [[5, 20], [100, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "dynamodb_us_east_membership_retry_storm",
        "time": {"total_minutes": 30, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 30}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 15,
                        "rate_multipliers": {"membership_renewal_timeout_f": 1.4, "metadata_service.queue_metrics": 1.3},
                        "latency_multipliers": {"membership_renewal_timeout_f": {"p50": 1.2, "p95": 1.4}},
                        "one_shots": [{"ref": "storage_node.net_disruption", "count": 2, "hosts": ["stor-01", "stor-02"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 21,
                        "rate_multipliers": {
                            "membership_renewal_timeout_f": 2.0,
                            "membership_renewal_success_f": 1.2,
                            "customer_rw_err_f": 1.2,
                            "customer_rw_ok_f": 0.8,
                            "admin_scale_attempt_fail_f": 2.0,
                            "metadata_service.queue_metrics": 2.0,
                        },
                        "latency_multipliers": {
                            "membership_renewal_timeout_f": {"p50": 1.5, "p95": 2.0},
                            "membership_renewal_success_f": {"p50": 1.3, "p95": 1.8},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 26,
                        "rate_multipliers": {
                            "membership_renewal_timeout_f": 0.0,
                            "membership_renewal_success_f": 0.0,
                            "admin_scale_attempt_fail_f": 0.0,
                            "customer_rw_err_f": 0.6,
                            "customer_rw_ok_f": 1.0,
                            "metadata_service.queue_metrics": 0.5,
                        },
                        "latency_multipliers": {
                            "membership_renewal_timeout_f": {"p50": 0.9, "p95": 1.1},
                            "membership_renewal_success_f": {"p50": 0.9, "p95": 1.1},
                        },
                        "one_shots": [
                            {"ref": "ops_control.pause_membership_requests", "count": 1, "hosts": ["ops-01"]},
                            {"ref": "metadata_service.admin_api_ok", "count": 1, "hosts": ["meta-01"]},
                            {"ref": "ops_control.scale_capacity_applied", "count": 1, "hosts": ["ops-01"]},
                        ],
                    },
                ]
            }
        },
    }
}


def md5_int(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big", signed=False)


def u01(s: str) -> float:
    return (md5_int(s) % 10_000_000) / 10_000_000.0


def clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


# Acklam approximation for inverse normal CDF (ppf)
def norm_ppf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def sample_lognormal_ms(p50: float, p95: float, key: str, hard_cap: Optional[float] = None) -> int:
    # Construct lognormal with median=p50 and 95th=p95
    p50 = max(1e-3, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    q = 0.45 + 0.5 * u01(key)  # [0.45, 0.95]
    z = norm_ppf(q)
    x = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    x = min(x, soft_cap)
    if hard_cap is not None:
        x = min(x, hard_cap)
    x = max(1.0, x)
    return int(round(x))


def stable_uuid(key: str) -> str:
    b = bytearray(hashlib.md5(key.encode("utf-8")).digest())
    # set version 4 and RFC 4122 variant
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def stable_hex(key: str, n: int) -> str:
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:n]


def fmt_float_1(x: float) -> str:
    return f"{x:.1f}"


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    comps = {c["id"]: c for c in system["components"]}
    logs = {}
    for cid, c in comps.items():
        for lid, t in c["logs"].items():
            logs[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **t}
    flows = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for f in system["flows"][st]["req"]:
            flows[st][f["id"]] = f
    return comps, logs, flows


def round_expected(expected: float, key: str) -> int:
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    return base + (1 if u01(key) < frac else 0)


def schedule_times(start_sec: float, end_sec: float, n: int, key: str) -> List[float]:
    if n <= 0:
        return []
    dur = max(1e-6, end_sec - start_sec)
    step = dur / n
    out = []
    for i in range(n):
        center = start_sec + (i + 0.5) * step
        jitter = (u01(f"{key}:j:{i}") - 0.5) * min(0.4, 0.15 * step)  # seconds
        t = center + jitter
        t = max(start_sec, min(end_sec - 1e-6, t))
        out.append(t)
    return out


def active_failure_intervals(scenario: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    sc = scenario["scenario"]
    f_start = sc["time"]["phases"]["f"]["start_min"]
    f_end = sc["time"]["phases"]["f"]["end_min"]
    events = sorted(sc["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    one_shots: List[Dict[str, Any]] = []
    # Persistent controls
    rate_flow: Dict[str, float] = {}
    rate_bg: Dict[str, float] = {}
    lat_flow: Dict[str, Dict[str, float]] = {}
    intervals: List[Dict[str, Any]] = []

    # Group events by at_min
    events_by_t: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_t.setdefault(int(e["at_min"]), []).append(e)

    boundaries = sorted(set([f_start] + [int(e["at_min"]) for e in events] + [f_end]))
    cur = f_start

    def apply_events(at_min: int) -> None:
        nonlocal rate_flow, rate_bg, lat_flow, one_shots
        for e in events_by_t.get(at_min, []):
            for k, v in e.get("rate_multipliers", {}).items():
                if "." in k:
                    rate_bg[k] = float(v)
                else:
                    rate_flow[k] = float(v)
            for k, v in e.get("latency_multipliers", {}).items():
                lat_flow[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
            for os in e.get("one_shots", []):
                one_shots.append({"at_min": at_min, **os})

    # Apply events at start boundary if any (e.g., at_min==15)
    apply_events(cur)
    for b in boundaries[1:]:
        if b > cur:
            intervals.append(
                {
                    "start_min": cur,
                    "end_min": b,
                    "rate_flow": dict(rate_flow),
                    "rate_bg": dict(rate_bg),
                    "lat_flow": dict(lat_flow),
                }
            )
        cur = b
        apply_events(cur)

    return intervals, one_shots


def merge_var_specs(tmpl: Dict[str, Any], state: str) -> Dict[str, Any]:
    merged = {}
    for k, v in tmpl.get("vars", {}).items():
        merged[k] = v
    sv = tmpl.get("state_vars", {}).get(state, {})
    for k, v in sv.items():
        merged[k] = v
    return merged


def gen_from_spec(spec: Dict[str, Any], key: str) -> Any:
    k = spec["k"]
    v = spec["v"]
    if k == "hex":
        return stable_hex(key, int(v))
    if k == "uuid":
        return stable_uuid(key)
    if k == "ch":
        arr = list(v)
        idx = md5_int(key) % len(arr)
        return arr[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        r = u01(key)
        return int(lo + math.floor(r * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        r = u01(key)
        return lo + r * (hi - lo)
    # fallback
    return str(v)


def render_message(tmpl: Dict[str, Any], values: Dict[str, Any]) -> str:
    return tmpl["msg"].format(**values)


def iso_utc(base: datetime, sec: float) -> str:
    dt = base + timedelta(seconds=float(sec))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def choose_host(hosts: List[str], key: str) -> str:
    if not hosts:
        return ""
    return hosts[md5_int(key) % len(hosts)]


def interpolate_int(lo: int, hi: int, f: float, key: str) -> int:
    f = clamp(f, 0.0, 1.0)
    base = lo + f * (hi - lo)
    wobble = (u01(key) - 0.5) * 0.08 * (hi - lo)
    return int(round(clamp(base + wobble, lo, hi)))


def interpolate_float(lo: float, hi: float, f: float, key: str) -> float:
    f = clamp(f, 0.0, 1.0)
    base = lo + f * (hi - lo)
    wobble = (u01(key) - 0.5) * 0.06 * (hi - lo)
    return float(clamp(base + wobble, lo, hi))


def make_bg_context(state: str, interval_start_min: int, interval_end_min: int, rate_flow_effects: Dict[str, float]) -> Dict[str, Any]:
    # Coarse load indicator used only to pick coherent values within state ranges.
    if state == "n":
        load = 0.12
    else:
        # Load inferred from whether membership renewal traffic is present and amplified.
        mt = rate_flow_effects.get("membership_renewal_timeout_f", 1.0)
        ms = rate_flow_effects.get("membership_renewal_success_f", 1.0)
        # When paused, multipliers go to 0.0
        if mt <= 0.01 and ms <= 0.01:
            load = 0.30
        elif mt >= 1.9:
            load = 1.00
        elif mt >= 1.2:
            load = 0.78
        else:
            load = 0.65
        # Slightly differentiate intervals by time
        if interval_start_min >= 26:
            load = 0.30
    return {"load_level": load, "start_min": interval_start_min, "end_min": interval_end_min}


def emit_row(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    sec: float,
    tmpl: Dict[str, Any],
    message_vals: Dict[str, Any],
    trace_id_col: str,
    service: str,
    host: str,
) -> None:
    rows.append(
        {
            "timestamp": iso_utc(base_time, sec),
            "level": tmpl["lvl"],
            "message": render_message(tmpl, message_vals),
            "trace_id": trace_id_col,
            "service": service or "",
            "host": host or "",
        }
    )


def simulate_customer_flow(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    flow: Dict[str, Any],
    state: str,
    start_sec: float,
    latency_mult: Optional[Dict[str, float]],
    instance_key: str,
    kind: str,  # "ok" or "err"
) -> None:
    # Host stickiness
    api_host = choose_host(comps["api_frontend"]["hosts"], f"{instance_key}:api")
    # Coherent request context
    trace_id = stable_hex(f"{instance_key}:trace", 32)
    op = gen_from_spec(logs["api_frontend.req_in"]["vars"]["op"], f"{instance_key}:op")
    table = gen_from_spec(logs["api_frontend.req_in"]["vars"]["table"], f"{instance_key}:table")
    key_hash = stable_hex(f"{instance_key}:key", 16)
    storage_host = choose_host(comps["storage_node"]["hosts"], f"{instance_key}:{key_hash}:stor")
    part = int(md5_int(f"{instance_key}:{key_hash}:part") % 2048)

    # Latency multipliers (not typically present for customer flows here; still supported)
    m50 = latency_mult["p50"] if latency_mult else 1.0
    m95 = latency_mult["p95"] if latency_mult else 1.0

    lat_pairs = flow["latency_ms"]
    dt1 = sample_lognormal_ms(lat_pairs[0][0] * m50, lat_pairs[0][1] * m95, f"{instance_key}:l1", hard_cap=20)
    dt2 = sample_lognormal_ms(lat_pairs[1][0] * m50, lat_pairs[1][1] * m95, f"{instance_key}:l2", hard_cap=200)
    dt3 = sample_lognormal_ms(lat_pairs[2][0] * m50, lat_pairs[2][1] * m95, f"{instance_key}:l3", hard_cap=300)

    # timestamps
    t_req = start_sec + dt1 / 1000.0
    t_storage = t_req + dt2 / 1000.0
    t_resp = t_storage + dt3 / 1000.0

    # Derived durations tied to timestamps (and thus to chosen dt's)
    serve_dur_ms = int(round((t_storage - t_req) * 1000.0))
    resp_dur_ms = int(round((t_resp - t_req) * 1000.0))

    # Ensure resp_ok dur_ms stays within domain by scaling dt2+dt3 if needed (rare)
    if kind == "ok":
        dom = logs["api_frontend.resp_ok"]["vars"]["dur_ms"]["v"]
        lo, hi = int(dom[0]), int(dom[1])
        if resp_dur_ms < lo or resp_dur_ms > hi:
            target = int(clamp(resp_dur_ms, lo, hi))
            total = max(1, dt2 + dt3)
            scale = target / total
            dt2 = max(1, int(round(dt2 * scale)))
            dt3 = max(1, int(round(dt3 * scale)))
            t_storage = t_req + dt2 / 1000.0
            t_resp = t_storage + dt3 / 1000.0
            serve_dur_ms = int(round((t_storage - t_req) * 1000.0))
            resp_dur_ms = int(round((t_resp - t_req) * 1000.0))

    # Emit logs in order
    tmpl = logs["api_frontend.req_in"]
    emit_row(
        rows,
        base_time,
        t_req,
        tmpl,
        {"trace_id": trace_id, "op": op, "table": table, "key_hash": key_hash},
        trace_id,
        comps["api_frontend"]["svc"],
        api_host,
    )

    if kind == "ok":
        tmpl = logs["storage_node.serve_ok"]
        ddom = tmpl["vars"]["dur_ms"]["v"]
        serve_dur_ms = int(clamp(serve_dur_ms, int(ddom[0]), int(ddom[1])))
        emit_row(
            rows,
            base_time,
            t_storage,
            tmpl,
            {"trace_id": trace_id, "op": op, "table": table, "part": part, "dur_ms": serve_dur_ms},
            trace_id,
            comps["storage_node"]["svc"],
            storage_host,
        )
        tmpl = logs["api_frontend.resp_ok"]
        bdom = tmpl["vars"]["bytes"]["v"]
        bytes_out = int(clamp(int(gen_from_spec(tmpl["vars"]["bytes"], f"{instance_key}:bytes")), int(bdom[0]), int(bdom[1])))
        emit_row(
            rows,
            base_time,
            t_resp,
            tmpl,
            {"trace_id": trace_id, "dur_ms": resp_dur_ms, "bytes": bytes_out, "storage": storage_host},
            trace_id,
            comps["api_frontend"]["svc"],
            api_host,
        )
    else:
        tmpl = logs["storage_node.serve_reject"]
        emit_row(
            rows,
            base_time,
            t_storage,
            tmpl,
            {"trace_id": trace_id, "op": op},
            trace_id,
            comps["storage_node"]["svc"],
            storage_host,
        )
        tmpl = logs["api_frontend.resp_err"]
        err_choices = tmpl["vars"]["err"]["v"]
        err = err_choices[md5_int(f"{instance_key}:err") % len(err_choices)]
        status = 503 if err == "StorageUnavailable" else 504 if err == "UpstreamTimeout" else 500
        if status not in tmpl["vars"]["status"]["v"]:
            status = int(tmpl["vars"]["status"]["v"][0])
        resp_dom = tmpl["vars"]["dur_ms"]["v"]
        resp_dur_ms = int(clamp(resp_dur_ms, int(resp_dom[0]), int(resp_dom[1])))
        emit_row(
            rows,
            base_time,
            t_resp,
            tmpl,
            {"trace_id": trace_id, "status": status, "err": err, "dur_ms": resp_dur_ms, "storage": storage_host},
            trace_id,
            comps["api_frontend"]["svc"],
            api_host,
        )


def simulate_membership_success_flow(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    flow: Dict[str, Any],
    state: str,
    start_sec: float,
    latency_mult: Optional[Dict[str, float]],
    instance_key: str,
) -> None:
    storage_host = choose_host(comps["storage_node"]["hosts"], f"{instance_key}:stor")
    meta_host = choose_host(comps["metadata_service"]["hosts"], f"{instance_key}:{storage_host}:meta")
    req_id = stable_uuid(f"{instance_key}:req_id")

    m50 = latency_mult["p50"] if latency_mult else 1.0
    m95 = latency_mult["p95"] if latency_mult else 1.0

    lat_pairs = flow["latency_ms"]
    dt1 = sample_lognormal_ms(lat_pairs[0][0] * m50, lat_pairs[0][1] * m95, f"{instance_key}:m:l1", hard_cap=20)

    resp_tmpl = logs["metadata_service.membership_resp"]
    build_spec = resp_tmpl["state_vars"][state]["build_ms"]
    build_lo, build_hi = int(build_spec["v"][0]), int(build_spec["v"][1])
    raw_build = sample_lognormal_ms(lat_pairs[1][0] * m50, lat_pairs[1][1] * m95, f"{instance_key}:m:build")
    build_ms = int(clamp(raw_build, build_lo, build_hi))
    overhead_ms = 8 + (md5_int(f"{instance_key}:m:ovh") % 13)  # 8..20ms
    dt2 = build_ms + overhead_ms

    applied_tmpl = logs["storage_node.membership_applied"]
    apply_spec = applied_tmpl["vars"]["dur_ms"]
    apply_lo, apply_hi = int(apply_spec["v"][0]), int(apply_spec["v"][1])
    raw_apply = sample_lognormal_ms(lat_pairs[2][0] * m50, lat_pairs[2][1] * m95, f"{instance_key}:m:apply")
    apply_ms = int(clamp(raw_apply, apply_lo, apply_hi))
    dt3 = apply_ms

    t_req = start_sec + dt1 / 1000.0
    t_resp = t_req + dt2 / 1000.0
    t_applied = t_resp + dt3 / 1000.0

    tmpl = logs["storage_node.membership_req"]
    reason = "periodic"
    if state == "f" and (start_sec / 60.0) < 16.0:
        reason = "net_recover"
    emit_row(
        rows,
        base_time,
        t_req,
        tmpl,
        {"req_id": req_id, "reason": reason, "meta": meta_host},
        "",
        comps["storage_node"]["svc"],
        storage_host,
    )

    tmpl = logs["metadata_service.membership_resp"]
    bytes_spec = tmpl["state_vars"][state]["bytes"]
    bytes_lo, bytes_hi = int(bytes_spec["v"][0]), int(bytes_spec["v"][1])
    bytes_frac = clamp((build_ms - build_lo) / max(1, (build_hi - build_lo)), 0.0, 1.0)
    bytes_val = interpolate_int(bytes_lo, bytes_hi, bytes_frac, f"{instance_key}:m:bytes")
    emit_row(
        rows,
        base_time,
        t_resp,
        tmpl,
        {"req_id": req_id, "storage": storage_host, "bytes": bytes_val, "build_ms": build_ms},
        "",
        comps["metadata_service"]["svc"],
        meta_host,
    )

    tmpl = logs["storage_node.membership_applied"]
    part_spec = tmpl["vars"]["partitions"]
    part_lo, part_hi = int(part_spec["v"][0]), int(part_spec["v"][1])
    if state == "n":
        part_val = interpolate_int(part_lo, part_hi, 0.25, f"{instance_key}:m:parts")
    else:
        part_val = interpolate_int(part_lo, part_hi, 0.70, f"{instance_key}:m:parts")
    emit_row(
        rows,
        base_time,
        t_applied,
        tmpl,
        {"req_id": req_id, "partitions": part_val, "dur_ms": apply_ms},
        "",
        comps["storage_node"]["svc"],
        storage_host,
    )


def simulate_membership_timeout_flow(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    flow: Dict[str, Any],
    state: str,
    start_sec: float,
    latency_mult: Optional[Dict[str, float]],
    instance_key: str,
) -> None:
    storage_host = choose_host(comps["storage_node"]["hosts"], f"{instance_key}:stor")
    meta_host = choose_host(comps["metadata_service"]["hosts"], f"{instance_key}:{storage_host}:meta")
    req_id = stable_uuid(f"{instance_key}:req_id")

    retry = flow["retry"]
    max_attempts = int(retry["max_attempts"])
    expected_attempts = float(retry["expected_attempts"])
    if abs(expected_attempts - round(expected_attempts)) < 1e-9:
        attempts = int(round(expected_attempts))
    else:
        a = int(math.floor(expected_attempts))
        b = min(max_attempts, a + 1)
        p = expected_attempts - a
        attempts = b if u01(f"{instance_key}:attempts") < p else a
        attempts = max(1, min(max_attempts, attempts))

    m50 = latency_mult["p50"] if latency_mult else 1.0
    m95 = latency_mult["p95"] if latency_mult else 1.0
    lat_pairs = flow["latency_ms"]

    cur_start = start_sec
    for attempt in range(1, attempts + 1):
        if attempt >= 2:
            pair = retry["backoff_ms"][attempt - 2] if (attempt - 2) < len(retry["backoff_ms"]) else retry["backoff_ms"][-1]
            backoff = sample_lognormal_ms(pair[0], pair[1], f"{instance_key}:backoff:{attempt}", hard_cap=3000)
            backoff = int(clamp(backoff, 50, 1500))
            cur_start += backoff / 1000.0
            rtmpl = logs["storage_node.membership_retry"]
            emit_row(
                rows,
                base_time,
                cur_start,
                rtmpl,
                {"req_id": req_id, "attempt": attempt, "backoff_ms": backoff},
                "",
                comps["storage_node"]["svc"],
                storage_host,
            )
            cur_start += (1 + (md5_int(f"{instance_key}:retrygap:{attempt}") % 3)) / 1000.0

        dt1 = sample_lognormal_ms(lat_pairs[0][0] * m50, lat_pairs[0][1] * m95, f"{instance_key}:t:l1:{attempt}", hard_cap=20)
        raw_elapsed = sample_lognormal_ms(lat_pairs[1][0] * m50, lat_pairs[1][1] * m95, f"{instance_key}:t:el:{attempt}")
        raw_rem = sample_lognormal_ms(lat_pairs[2][0] * m50, lat_pairs[2][1] * m95, f"{instance_key}:t:rem:{attempt}")
        raw_waited = raw_elapsed + raw_rem

        waited_lo, waited_hi = logs["storage_node.membership_timeout"]["vars"]["waited_ms"]["v"]
        waited_ms = int(clamp(raw_waited, int(waited_lo), int(waited_hi)))

        el_lo, el_hi = logs["metadata_service.slow_build"]["vars"]["elapsed_ms"]["v"]
        elapsed_ms = int(clamp(raw_elapsed, int(el_lo), int(el_hi)))

        min_remaining = 200
        max_remaining = 1200
        remaining_ms = waited_ms - elapsed_ms

        if remaining_ms < min_remaining:
            elapsed_ms = max(int(el_lo), waited_ms - min_remaining)
            elapsed_ms = min(elapsed_ms, int(el_hi))
            remaining_ms = waited_ms - elapsed_ms
        if remaining_ms > max_remaining:
            desired_remaining = max_remaining
            new_elapsed = waited_ms - desired_remaining
            new_elapsed = int(clamp(new_elapsed, int(el_lo), int(el_hi)))
            elapsed_ms = new_elapsed
            remaining_ms = waited_ms - elapsed_ms
            if remaining_ms > max_remaining:
                remaining_ms = max_remaining
                waited_ms = elapsed_ms + remaining_ms
                waited_ms = int(clamp(waited_ms, int(waited_lo), int(waited_hi)))

        t_req = cur_start + dt1 / 1000.0
        t_slow = t_req + elapsed_ms / 1000.0
        t_to = t_slow + remaining_ms / 1000.0
        cur_start = t_to

        tmpl = logs["storage_node.membership_req"]
        reason = "periodic"
        if state == "f" and (start_sec / 60.0) < 16.0:
            reason = "net_recover"
        emit_row(
            rows,
            base_time,
            t_req,
            tmpl,
            {"req_id": req_id, "reason": reason, "meta": meta_host},
            "",
            comps["storage_node"]["svc"],
            storage_host,
        )

        tmpl = logs["metadata_service.slow_build"]
        minute = start_sec / 60.0
        if minute < 21:
            load_f = 0.70
        elif minute < 26:
            load_f = 0.95
        else:
            load_f = 0.40
        bytes_lo, bytes_hi = tmpl["vars"]["bytes"]["v"]
        parts_lo, parts_hi = tmpl["vars"]["partitions"]["v"]
        bytes_val = interpolate_int(int(bytes_lo), int(bytes_hi), load_f, f"{instance_key}:slow:bytes:{attempt}")
        parts_val = interpolate_int(int(parts_lo), int(parts_hi), load_f, f"{instance_key}:slow:parts:{attempt}")
        emit_row(
            rows,
            base_time,
            t_slow,
            tmpl,
            {"req_id": req_id, "storage": storage_host, "bytes": bytes_val, "elapsed_ms": elapsed_ms, "partitions": parts_val},
            "",
            comps["metadata_service"]["svc"],
            meta_host,
        )

        tmpl = logs["storage_node.membership_timeout"]
        if minute < 21:
            dq = 60 + (md5_int(f"{instance_key}:dq:{attempt}") % 90)  # 60..149
        else:
            dq = 120 + (md5_int(f"{instance_key}:dq:{attempt}") % 121)  # 120..240
        emit_row(
            rows,
            base_time,
            t_to,
            tmpl,
            {"req_id": req_id, "waited_ms": waited_ms, "deadline_ms": 1500, "disqualify_for_s": dq},
            "",
            comps["storage_node"]["svc"],
            storage_host,
        )


def simulate_admin_scale_fail_flow(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    flow: Dict[str, Any],
    start_sec: float,
    instance_key: str,
) -> None:
    ops_host = choose_host(comps["ops_control"]["hosts"], f"{instance_key}:ops")
    meta_host = choose_host(comps["metadata_service"]["hosts"], f"{instance_key}:meta")
    req_id = stable_uuid(f"{instance_key}:req_id")

    dt1 = sample_lognormal_ms(flow["latency_ms"][0][0], flow["latency_ms"][0][1], f"{instance_key}:a:l1", hard_cap=100)
    dt2 = sample_lognormal_ms(flow["latency_ms"][1][0], flow["latency_ms"][1][1], f"{instance_key}:a:l2", hard_cap=2000)
    t_cmd = start_sec + dt1 / 1000.0
    t_err = t_cmd + dt2 / 1000.0

    from_spec = logs["ops_control.scale_capacity_cmd"]["vars"]["from_cap"]["v"]
    to_spec = logs["ops_control.scale_capacity_cmd"]["vars"]["to_cap"]["v"]
    from_lo, from_hi = int(from_spec[0]), int(from_spec[1])
    to_lo, to_hi = int(to_spec[0]), int(to_spec[1])
    from_cap = interpolate_int(from_lo, from_hi, 0.55, f"{instance_key}:cap:from")
    to_cap = interpolate_int(to_lo, to_hi, 0.65, f"{instance_key}:cap:to")
    if to_cap <= from_cap:
        to_cap = min(to_hi, from_cap + 2)

    tmpl = logs["ops_control.scale_capacity_cmd"]
    emit_row(
        rows,
        base_time,
        t_cmd,
        tmpl,
        {"from_cap": from_cap, "to_cap": to_cap, "req_id": req_id},
        "",
        comps["ops_control"]["svc"],
        ops_host,
    )

    tmpl = logs["metadata_service.admin_api_err"]
    status = 503 if (md5_int(f"{instance_key}:status") % 2 == 0) else 504
    reason = "overloaded" if status == 503 else "timeout"
    emit_row(
        rows,
        base_time,
        t_err,
        tmpl,
        {"action": "scale_capacity", "status": status, "reason": reason},
        "",
        comps["metadata_service"]["svc"],
        meta_host,
    )


def simulate_background(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    state: str,
    start_min: int,
    end_min: int,
    rate_mult_bg: Dict[str, float],
    rate_mult_flow: Dict[str, float],
) -> None:
    start_sec = start_min * 60.0
    end_sec = end_min * 60.0
    duration_min = end_min - start_min
    bg_ctx = make_bg_context(state, start_min, end_min, rate_mult_flow)

    if state == "n":
        err_ratio = 0.005
        total_rpm = SYSTEM["flows"]["n"]["req"][0]["rpm"]
    else:
        f_flows = {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}
        ok_rpm = f_flows["customer_rw_ok_f"]["rpm"] * rate_mult_flow.get("customer_rw_ok_f", 1.0)
        err_rpm = f_flows["customer_rw_err_f"]["rpm"] * rate_mult_flow.get("customer_rw_err_f", 1.0)
        total_rpm = ok_rpm + err_rpm
        err_ratio = (err_rpm / total_rpm) if total_rpm > 0 else 0.5

    for cid, comp in comps.items():
        beh = comp.get("beh", {}).get(state, {})
        for e in beh.get("emit", []):
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = float(rate_mult_bg.get(f"{cid}.{log_id}", 1.0))
            eff_per_min = per_min * mult

            if scope == "global":
                n = round_expected(eff_per_min * duration_min, f"bg:{state}:{start_min}-{end_min}:{cid}.{log_id}")
                times = schedule_times(start_sec, end_sec, n, f"bg:{state}:{start_min}-{end_min}:{cid}.{log_id}")
                for i, ts in enumerate(times):
                    tmpl = logs[f"{cid}.{log_id}"]
                    var_specs = merge_var_specs(tmpl, state)
                    values: Dict[str, Any] = {}

                    if cid == "api_frontend" and log_id == "access_summary":
                        rps = clamp((total_rpm / 60.0) / max(1, len(comp["hosts"])), 2.0, 12.0)
                        rps = interpolate_float(2.0, 12.0, (rps - 2.0) / 10.0, f"as:rps:{state}:{start_min}:{i}")
                        err_min, err_max = tmpl["state_vars"][state]["err_pct"]["v"]
                        err_pct = clamp(err_ratio * 100.0, float(err_min), float(err_max))
                        err_pct = interpolate_float(float(err_min), float(err_max), err_pct / float(err_max), f"as:err:{state}:{start_min}:{i}")
                        values["rps"] = fmt_float_1(rps)
                        values["err_pct"] = fmt_float_1(err_pct)
                    elif cid == "metadata_service" and log_id == "queue_metrics":
                        sv = tmpl["state_vars"][state]
                        load = bg_ctx["load_level"]
                        values["queue_depth"] = interpolate_int(int(sv["queue_depth"]["v"][0]), int(sv["queue_depth"]["v"][1]), load, f"qm:qd:{start_min}:{i}")
                        values["inflight"] = interpolate_int(int(sv["inflight"]["v"][0]), int(sv["inflight"]["v"][1]), load, f"qm:in:{start_min}:{i}")
                        values["p95_build_ms"] = interpolate_int(int(sv["p95_build_ms"]["v"][0]), int(sv["p95_build_ms"]["v"][1]), load, f"qm:p95:{start_min}:{i}")
                    elif cid == "metadata_kvstore" and log_id == "latency_metrics":
                        sv = tmpl["state_vars"][state]
                        load = bg_ctx["load_level"]
                        values["p95_ms"] = interpolate_int(int(sv["p95_ms"]["v"][0]), int(sv["p95_ms"]["v"][1]), load, f"kv:p95:{start_min}:{i}")
                        values["read_qps"] = interpolate_int(int(sv["read_qps"]["v"][0]), int(sv["read_qps"]["v"][1]), load, f"kv:qps:{start_min}:{i}")
                    elif cid == "storage_node" and log_id == "heartbeat":
                        sv = tmpl["state_vars"][state]["eligible"]["v"]
                        values["eligible"] = sv[0]
                    else:
                        for vk, spec in var_specs.items():
                            values[vk] = gen_from_spec(spec, f"bg:{state}:{cid}.{log_id}:{start_min}:{i}:{vk}")

                    host = choose_host(comp.get("hosts", []), f"bg:{state}:{cid}.{log_id}:{start_min}:{i}:host")
                    emit_row(rows, base_time, ts, tmpl, values, "", comp.get("svc", ""), host)
            else:
                for h in comp.get("hosts", []):
                    n = round_expected(eff_per_min * duration_min, f"bg:{state}:{start_min}-{end_min}:{cid}.{log_id}:{h}")
                    times = schedule_times(start_sec, end_sec, n, f"bg:{state}:{start_min}-{end_min}:{cid}.{log_id}:{h}")
                    for i, ts in enumerate(times):
                        tmpl = logs[f"{cid}.{log_id}"]
                        var_specs = merge_var_specs(tmpl, state)
                        values: Dict[str, Any] = {}

                        if cid == "api_frontend" and log_id == "access_summary":
                            rps = clamp((total_rpm / 60.0) / max(1, len(comp["hosts"])), 2.0, 12.0)
                            rps = interpolate_float(2.0, 12.0, (rps - 2.0) / 10.0, f"as:rps:{state}:{start_min}:{h}:{i}")
                            err_min, err_max = tmpl["state_vars"][state]["err_pct"]["v"]
                            err_pct = clamp(err_ratio * 100.0, float(err_min), float(err_max))
                            if state == "f" and start_min >= 26:
                                err_pct = max(err_pct, 20.0)
                            err_pct = interpolate_float(float(err_min), float(err_max), err_pct / float(err_max), f"as:err:{state}:{start_min}:{h}:{i}")
                            values["rps"] = fmt_float_1(rps)
                            values["err_pct"] = fmt_float_1(err_pct)
                        elif cid == "metadata_service" and log_id == "queue_metrics":
                            sv = tmpl["state_vars"][state]
                            load = bg_ctx["load_level"]
                            values["queue_depth"] = interpolate_int(int(sv["queue_depth"]["v"][0]), int(sv["queue_depth"]["v"][1]), load, f"qm:qd:{start_min}:{h}:{i}")
                            values["inflight"] = interpolate_int(int(sv["inflight"]["v"][0]), int(sv["inflight"]["v"][1]), load, f"qm:in:{start_min}:{h}:{i}")
                            values["p95_build_ms"] = interpolate_int(int(sv["p95_build_ms"]["v"][0]), int(sv["p95_build_ms"]["v"][1]), load, f"qm:p95:{start_min}:{h}:{i}")
                        elif cid == "metadata_kvstore" and log_id == "latency_metrics":
                            sv = tmpl["state_vars"][state]
                            load = bg_ctx["load_level"]
                            values["p95_ms"] = interpolate_int(int(sv["p95_ms"]["v"][0]), int(sv["p95_ms"]["v"][1]), load, f"kv:p95:{start_min}:{h}:{i}")
                            values["read_qps"] = interpolate_int(int(sv["read_qps"]["v"][0]), int(sv["read_qps"]["v"][1]), load, f"kv:qps:{start_min}:{h}:{i}")
                        elif cid == "storage_node" and log_id == "heartbeat":
                            if state == "n":
                                values["eligible"] = "true"
                            else:
                                load = bg_ctx["load_level"]
                                p_false = clamp(0.10 + 0.55 * load, 0.10, 0.70)
                                values["eligible"] = "false" if u01(f"hb:{start_min}:{h}:{i}") < p_false else "true"
                        else:
                            for vk, spec in var_specs.items():
                                values[vk] = gen_from_spec(spec, f"bg:{state}:{cid}.{log_id}:{start_min}:{h}:{i}:{vk}")

                        emit_row(rows, base_time, ts, tmpl, values, "", comp.get("svc", ""), h)


def simulate_one_shots(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    one_shots: List[Dict[str, Any]],
) -> None:
    for os in one_shots:
        at_min = int(os["at_min"])
        ref = os["ref"]
        count = int(os["count"])
        allowed_hosts = list(os.get("hosts", []))
        cid, lid = parse_ref(ref)
        tmpl = logs[ref]
        var_specs = merge_var_specs(tmpl, "f")
        for i in range(count):
            t0 = at_min * 60.0
            ts = t0 + (u01(f"os:{ref}:{at_min}:{i}") * 0.9)
            values: Dict[str, Any] = {}
            for vk, spec in var_specs.items():
                values[vk] = gen_from_spec(spec, f"os:{ref}:{at_min}:{i}:{vk}")
            if allowed_hosts:
                host = allowed_hosts[i % len(allowed_hosts)]
            else:
                host = choose_host(comps[cid].get("hosts", []), f"os:{ref}:{at_min}:{i}:host")
            emit_row(rows, base_time, ts, tmpl, values, "", comps[cid].get("svc", ""), host)


def simulate_flows_for_interval(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    comps: Dict[str, Any],
    logs: Dict[str, Any],
    flows: Dict[str, Any],
    state: str,
    interval_start_min: int,
    interval_end_min: int,
    rate_mult_flow: Dict[str, float],
    lat_mult_flow: Dict[str, Dict[str, float]],
) -> None:
    start_sec = interval_start_min * 60.0
    end_sec = interval_end_min * 60.0
    duration_min = interval_end_min - interval_start_min

    for fid, flow in flows[state].items():
        rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f":
            mult = float(rate_mult_flow.get(fid, 1.0))
        eff_rpm = rpm * mult
        expected = eff_rpm * duration_min
        n_instances = round_expected(expected, f"flow:{state}:{interval_start_min}-{interval_end_min}:{fid}")
        times = schedule_times(start_sec, end_sec, n_instances, f"flow:{state}:{interval_start_min}-{interval_end_min}:{fid}")

        for i, st in enumerate(times):
            inst_key = f"{state}:{fid}:{interval_start_min}-{interval_end_min}:{i}:{int(st*1000)}"
            lmult = lat_mult_flow.get(fid, None) if state == "f" else None

            if fid in ("customer_rw", "customer_rw_ok_f"):
                simulate_customer_flow(rows, base_time, comps, logs, flow, state, st, lmult, inst_key, kind="ok")
            elif fid == "customer_rw_err_f":
                simulate_customer_flow(rows, base_time, comps, logs, flow, state, st, lmult, inst_key, kind="err")
            elif fid in ("membership_renewal", "membership_renewal_success_f"):
                simulate_membership_success_flow(rows, base_time, comps, logs, flow, state, st, lmult, inst_key)
            elif fid == "membership_renewal_timeout_f":
                simulate_membership_timeout_flow(rows, base_time, comps, logs, flow, state, st, lmult, inst_key)
            elif fid == "admin_scale_attempt_fail_f":
                simulate_admin_scale_fail_flow(rows, base_time, comps, logs, flow, st, inst_key)
            else:
                continue


def main() -> None:
    comps, log_templates, flows = build_indices(SYSTEM)

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    rows: List[Dict[str, Any]] = []

    n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
    simulate_background(rows, base_time, comps, log_templates, "n", n_start, n_end, rate_mult_bg={}, rate_mult_flow={})
    simulate_flows_for_interval(rows, base_time, comps, log_templates, flows, "n", n_start, n_end, rate_mult_flow={}, lat_mult_flow={})

    f_intervals, one_shots = active_failure_intervals(SCENARIO)
    for iv in f_intervals:
        simulate_background(
            rows,
            base_time,
            comps,
            log_templates,
            "f",
            int(iv["start_min"]),
            int(iv["end_min"]),
            rate_mult_bg=iv["rate_bg"],
            rate_mult_flow=iv["rate_flow"],
        )
        simulate_flows_for_interval(
            rows,
            base_time,
            comps,
            log_templates,
            flows,
            "f",
            int(iv["start_min"]),
            int(iv["end_min"]),
            rate_mult_flow=iv["rate_flow"],
            lat_mult_flow=iv["lat_flow"],
        )

    simulate_one_shots(rows, base_time, comps, log_templates, one_shots)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    df["trace_id"] = df["trace_id"].fillna("").astype(str)
    df.loc[~df["trace_id"].str.fullmatch(r"[0-9a-f]{32}|"), "trace_id"] = ""

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
