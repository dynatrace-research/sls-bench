import hashlib
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "aws_lambda_cell_use1"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["lambda_frontend", "event_router", "sts_gateway"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "lambda_frontend": {
            "svc": "lambda-frontend",
            "hosts": ["lf-1", "lf-2", "lf-3", "lf-4"],
            "logs": {
                "invoke_received": {
                    "lvl": "INFO",
                    "msg": "Invoke request accepted trace={trace_id} req_id={req_id} fn={fn} type={invoke_type}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "req_id": {"k": "uuid", "v": None},
                        "fn": {"k": "ch", "v": ["fn-auth", "fn-orders", "fn-metrics"]},
                        "invoke_type": {"k": "ch", "v": ["sync", "async"]},
                    },
                },
                "invoke_retry": {
                    "lvl": "WARN",
                    "msg": "Retrying invoke trace={trace_id} req_id={req_id} attempt={attempt} reason={reason}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "req_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["no_capacity", "upstream_timeout"]},
                    },
                },
                "invoke_success": {
                    "lvl": "INFO",
                    "msg": "Invoke completed trace={trace_id} req_id={req_id} status=200 dur_ms={dur_ms}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [5, 3000]},
                    },
                },
                "async_accepted": {
                    "lvl": "INFO",
                    "msg": "Async invoke accepted trace={trace_id} req_id={req_id} fn={fn} status=202 dur_ms={dur_ms}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "req_id": {"k": "uuid", "v": None},
                        "fn": {"k": "ch", "v": ["fn-auth", "fn-orders", "fn-metrics"]},
                        "dur_ms": {"k": "i", "v": [2, 500]},
                    },
                },
                "invoke_error": {
                    "lvl": "ERROR",
                    "msg": "Invoke failed trace={trace_id} req_id={req_id} status={status} err={err} dur_ms={dur_ms}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "req_id": {"k": "uuid", "v": None},
                        "status": {"k": "ch", "v": [429, 500, 503, 504]},
                        "err": {"k": "ch", "v": ["NoCapacity", "Timeout", "Throttled"]},
                        "dur_ms": {"k": "i", "v": [50, 10000]},
                    },
                },
                "scaling_threshold_crossed": {
                    "lvl": "WARN",
                    "msg": "Frontend scaling crossed cell limit cell={cell_id} desired_hosts={desired_hosts}",
                    "vars": {"cell_id": {"k": "ch", "v": ["use1-cell-07"]}, "desired_hosts": {"k": "i", "v": [800, 1400]}},
                },
                "scale_down_initiated": {
                    "lvl": "INFO",
                    "msg": "Mitigation: scaling down frontend cell={cell_id} new_desired_hosts={desired_hosts}",
                    "vars": {"cell_id": {"k": "ch", "v": ["use1-cell-07"]}, "desired_hosts": {"k": "i", "v": [500, 900]}},
                },
                "frontend_metrics": {
                    "lvl": "INFO",
                    "msg": "Cell metrics cell={cell_id} inv_rps={inv_rps} err_rate={err_rate} p95_ms={p95_ms}",
                    "vars": {"cell_id": {"k": "ch", "v": ["use1-cell-07"]}},
                    "state_vars": {
                        "n": {
                            "inv_rps": {"k": "f", "v": [1.0, 8.0]},
                            "err_rate": {"k": "f", "v": [0.0, 0.02]},
                            "p95_ms": {"k": "i", "v": [40, 250]},
                        },
                        "f": {
                            "inv_rps": {"k": "f", "v": [1.0, 10.0]},
                            "err_rate": {"k": "f", "v": [0.1, 0.7]},
                            "p95_ms": {"k": "i", "v": [400, 4000]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "frontend_metrics", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "frontend_metrics", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "invocation_manager": {
            "svc": "lambda-invocation-manager",
            "hosts": ["im-1", "im-2", "im-3"],
            "logs": {
                "allocate_env": {
                    "lvl": "DEBUG",
                    "msg": "Allocating execution environment trace={trace_id} fn={fn} alloc_id={alloc_id}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "fn": {"k": "ch", "v": ["fn-auth", "fn-orders", "fn-metrics"]},
                        "alloc_id": {"k": "hex", "v": 16},
                    },
                },
                "provision_blocked": {
                    "lvl": "WARN",
                    "msg": "Provisioning blocked trace={trace_id} fn={fn} reason={reason} ready={ready} allocated={allocated}",
                    "vars": {
                        "trace_id": {"k": "hex", "v": 32},
                        "fn": {"k": "ch", "v": ["fn-auth", "fn-orders", "fn-metrics"]},
                        "reason": {"k": "ch", "v": ["no_ready_env", "quota_reached"]},
                        "ready": {"k": "i", "v": [0, 200]},
                        "allocated": {"k": "i", "v": [1000, 10000]},
                    },
                },
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "Env pool stats cell={cell_id} allocated={allocated} ready={ready} in_use={in_use}",
                    "vars": {"cell_id": {"k": "ch", "v": ["use1-cell-07"]}},
                    "state_vars": {
                        "n": {
                            "allocated": {"k": "i", "v": [1500, 2500]},
                            "ready": {"k": "i", "v": [200, 600]},
                            "in_use": {"k": "i", "v": [100, 500]},
                        },
                        "f": {
                            "allocated": {"k": "i", "v": [3000, 9000]},
                            "ready": {"k": "i", "v": [0, 120]},
                            "in_use": {"k": "i", "v": [0, 120]},
                        },
                    },
                },
                "capacity_gap_snapshot": {
                    "lvl": "WARN",
                    "msg": "Capacity below target cell={cell_id} ready={ready} target_ready={target_ready}",
                    "vars": {
                        "cell_id": {"k": "ch", "v": ["use1-cell-07"]},
                        "ready": {"k": "i", "v": [0, 200]},
                        "target_ready": {"k": "i", "v": [200, 800]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_stats", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "pool_stats", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "exec_env_pool": {
            "svc": "lambda-exec-env",
            "hosts": ["envd-1", "envd-2", "envd-3"],
            "logs": {
                "env_ready": {
                    "lvl": "DEBUG",
                    "msg": "Execution environment ready alloc_id={alloc_id} host={host} cold_start_ms={cold_ms}",
                    "vars": {"alloc_id": {"k": "hex", "v": 16}, "host": {"k": "ch", "v": ["envd-1", "envd-2", "envd-3"]}, "cold_ms": {"k": "i", "v": [10, 800]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "event_router": {
            "svc": "event-router",
            "hosts": ["er-1", "er-2"],
            "logs": {
                "delivery_attempt": {
                    "lvl": "INFO",
                    "msg": "Delivering event trace={trace_id} evt_id={evt_id} target={fn} lag_s={lag_s}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}, "evt_id": {"k": "uuid", "v": None}, "fn": {"k": "ch", "v": ["fn-auth", "fn-orders", "fn-metrics"]}},
                    "state_vars": {"n": {"lag_s": {"k": "i", "v": [0, 5]}}, "f": {"lag_s": {"k": "i", "v": [10, 900]}}},
                },
                "delivery_retry": {
                    "lvl": "WARN",
                    "msg": "Retrying event delivery trace={trace_id} evt_id={evt_id} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}, "evt_id": {"k": "uuid", "v": None}, "attempt": {"k": "i", "v": [2, 5]}, "backoff_ms": {"k": "i", "v": [500, 6000]}},
                },
                "delivery_success": {
                    "lvl": "INFO",
                    "msg": "Event delivered evt_id={evt_id} status=202 lag_s={lag_s}",
                    "vars": {"evt_id": {"k": "uuid", "v": None}},
                    "state_vars": {"n": {"lag_s": {"k": "i", "v": [0, 5]}}, "f": {"lag_s": {"k": "i", "v": [10, 900]}}},
                },
                "delivery_deferred": {"lvl": "WARN", "msg": "Event delivery deferred evt_id={evt_id} reason={reason}", "vars": {"evt_id": {"k": "uuid", "v": None}, "reason": {"k": "ch", "v": ["invoke_error", "throttled"]}}},
                "queue_stats": {
                    "lvl": "INFO",
                    "msg": "Queue stats source={source} depth={depth} max_age_s={max_age_s}",
                    "vars": {"source": {"k": "ch", "v": ["eventbridge"]}},
                    "state_vars": {"n": {"depth": {"k": "i", "v": [0, 500]}, "max_age_s": {"k": "i", "v": [0, 10]}}, "f": {"depth": {"k": "i", "v": [2000, 20000]}, "max_age_s": {"k": "i", "v": [60, 900]}}},
                },
                "queue_lag_snapshot": {"lvl": "WARN", "msg": "Observed queue lag source={source} depth={depth} max_age_s={max_age_s}", "vars": {"source": {"k": "ch", "v": ["eventbridge"]}, "depth": {"k": "i", "v": [1000, 25000]}, "max_age_s": {"k": "i", "v": [30, 900]}}},
            },
            "beh": {"n": {"emit": [{"id": "queue_stats", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "queue_stats", "per_min": 1.5, "scope": "global"}]}},
        },
        "sts_gateway": {
            "svc": "sts-gateway",
            "hosts": ["sts-1", "sts-2"],
            "logs": {
                "saml_req": {"lvl": "INFO", "msg": "SAML federation request trace={trace_id} req_id={req_id} idp={idp}", "vars": {"trace_id": {"k": "hex", "v": 32}, "req_id": {"k": "uuid", "v": None}, "idp": {"k": "ch", "v": ["okta", "aadfs"]}}},
                "saml_ok": {"lvl": "INFO", "msg": "SAML federation success req_id={req_id} status=200 dur_ms={dur_ms}", "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [20, 3000]}}},
                "saml_throttled": {"lvl": "ERROR", "msg": "SAML federation failed req_id={req_id} status={status} err={err} dur_ms={dur_ms}", "vars": {"req_id": {"k": "uuid", "v": None}, "status": {"k": "ch", "v": [429, 503]}, "err": {"k": "ch", "v": ["UpstreamLambdaError", "Throttled"]}, "dur_ms": {"k": "i", "v": [100, 8000]}}},
                "throttle_metric": {"lvl": "WARN", "msg": "STS throttling active scope={scope} throttled_rps={thr_rps}", "vars": {"scope": {"k": "ch", "v": ["saml_federation"]}}, "state_vars": {"n": {"thr_rps": {"k": "i", "v": [0, 2]}}, "f": {"thr_rps": {"k": "i", "v": [5, 80]}}}},
            },
            "beh": {"n": {"emit": [{"id": "throttle_metric", "per_min": 0.1, "scope": "per_host"}]}, "f": {"emit": [{"id": "throttle_metric", "per_min": 0.3, "scope": "per_host"}]}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {"id": "sync_invoke_ok_n", "rpm": 180.0, "emit": ["lambda_frontend.invoke_received", "invocation_manager.allocate_env", "exec_env_pool.env_ready", "lambda_frontend.invoke_success"], "latency_ms": [[2, 8], [3, 12], [10, 120], [15, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "async_event_delivery_ok_n", "rpm": 50.0, "emit": ["event_router.delivery_attempt", "lambda_frontend.invoke_received", "lambda_frontend.async_accepted", "event_router.delivery_success"], "latency_ms": [[2, 10], [3, 15], [3, 40], [2, 10]], "retry": {"max_attempts": 2, "expected_attempts": 1.1, "emit_per_retry": ["event_router.delivery_retry"], "backoff_ms": [[200, 800]]}, "trace": True},
                {"id": "sts_saml_federation_ok_n", "rpm": 20.0, "emit": ["sts_gateway.saml_req", "lambda_frontend.invoke_received", "invocation_manager.allocate_env", "lambda_frontend.invoke_success", "sts_gateway.saml_ok"], "latency_ms": [[2, 8], [3, 12], [3, 15], [30, 400], [5, 20]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
        "f": {
            "req": [
                {"id": "sync_invoke_f_err", "rpm": 120.0, "emit": ["lambda_frontend.invoke_received", "invocation_manager.allocate_env", "invocation_manager.provision_blocked", "lambda_frontend.invoke_error"], "latency_ms": [[2, 10], [5, 20], [300, 2500], [500, 8000]], "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["lambda_frontend.invoke_retry"], "backoff_ms": [[100, 500], [200, 1200]]}, "trace": True},
                {"id": "sync_invoke_f_ok", "rpm": 60.0, "emit": ["lambda_frontend.invoke_received", "invocation_manager.allocate_env", "exec_env_pool.env_ready", "lambda_frontend.invoke_success"], "latency_ms": [[2, 10], [5, 20], [50, 600], [150, 1200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "async_event_delivery_f_fail", "rpm": 40.0, "emit": ["event_router.delivery_attempt", "lambda_frontend.invoke_received", "invocation_manager.provision_blocked", "lambda_frontend.invoke_error", "event_router.delivery_deferred"], "latency_ms": [[2, 12], [3, 15], [200, 2000], [300, 5000], [2, 12]], "retry": {"max_attempts": 5, "expected_attempts": 3.0, "emit_per_retry": ["event_router.delivery_retry"], "backoff_ms": [[1000, 5000], [2000, 8000], [4000, 15000], [8000, 30000]]}, "trace": True},
                {"id": "async_event_delivery_f_ok", "rpm": 15.0, "emit": ["event_router.delivery_attempt", "lambda_frontend.invoke_received", "lambda_frontend.async_accepted", "event_router.delivery_success"], "latency_ms": [[2, 12], [3, 15], [3, 80], [2, 12]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "sts_saml_federation_f", "rpm": 15.0, "emit": ["sts_gateway.saml_req", "lambda_frontend.invoke_received", "invocation_manager.provision_blocked", "lambda_frontend.invoke_error", "sts_gateway.saml_throttled"], "latency_ms": [[2, 10], [3, 15], [200, 2500], [300, 6000], [5, 25]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "use1_lambda_frontend_scale_threshold_bug"},
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "async_event_delivery_f_ok": 0.0,
                        "sts_saml_federation_f": 0.0,
                        "invocation_manager.pool_stats": 1.3,
                        "event_router.queue_stats": 1.3,
                        "sts_gateway.throttle_metric": 0.0,
                    },
                    "latency_multipliers": {
                        "sync_invoke_f_err": {"p50": 2.0, "p95": 4.0},
                        "sync_invoke_f_ok": {"p50": 1.6, "p95": 2.6},
                        "async_event_delivery_f_fail": {"p50": 1.2, "p95": 1.5},
                    },
                    "one_shots": [
                        {"ref": "lambda_frontend.scaling_threshold_crossed", "count": 1, "hosts": ["lf-2"]},
                        {"ref": "invocation_manager.capacity_gap_snapshot", "count": 1, "hosts": ["im-1"]},
                        {"ref": "event_router.queue_lag_snapshot", "count": 1, "hosts": ["er-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 35,
                    "rate_multipliers": {
                        "sync_invoke_f_err": 0.5,
                        "sync_invoke_f_ok": 1.3,
                        "async_event_delivery_f_fail": 0.9,
                        "invocation_manager.pool_stats": 1.1,
                        "event_router.queue_stats": 1.2,
                    },
                    "latency_multipliers": {"sync_invoke_f_err": {"p50": 1.3, "p95": 2.0}, "sync_invoke_f_ok": {"p50": 1.2, "p95": 1.6}},
                    "one_shots": [{"ref": "lambda_frontend.scale_down_initiated", "count": 1, "hosts": ["lf-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 45,
                    "rate_multipliers": {
                        "sync_invoke_f_err": 0.2,
                        "sync_invoke_f_ok": 1.5,
                        "async_event_delivery_f_fail": 0.6,
                        "async_event_delivery_f_ok": 1.0,
                        "sts_saml_federation_f": 1.0,
                        "sts_gateway.throttle_metric": 1.0,
                        "event_router.queue_stats": 1.2,
                        "invocation_manager.pool_stats": 1.0,
                    },
                    "latency_multipliers": {"sync_invoke_f_ok": {"p50": 1.0, "p95": 1.2}, "async_event_delivery_f_ok": {"p50": 1.0, "p95": 1.3}},
                    "one_shots": [{"ref": "event_router.queue_lag_snapshot", "count": 2, "hosts": ["er-1", "er-2"]}],
                },
            ]
        }
    },
}

# -----------------------------
# Deterministic helpers
# -----------------------------
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
SEED_TAG = "incident-sim-v3/aws_lambda_cell_use1/use1_lambda_frontend_scale_threshold_bug"

_SEED_INT = int(hashlib.md5(SEED_TAG.encode("utf-8")).hexdigest()[:8], 16)
random.seed(_SEED_INT)
np.random.seed(_SEED_INT)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def md5_int(s: str) -> int:
    return int(md5_hex(s), 16)


def u01(s: str) -> float:
    x = md5_int(s)
    return (x % (2**53)) / float(2**53)


def det_hex(s: str, n: int) -> str:
    return md5_hex(f"{SEED_TAG}|hex|{n}|{s}")[:n]


def det_uuid(s: str) -> str:
    b = hashlib.md5(f"{SEED_TAG}|uuid|{s}".encode("utf-8")).digest()
    u = uuid.UUID(bytes=b)
    return str(u)


def det_choice(s: str, choices: List[Any]) -> Any:
    if not choices:
        return ""
    idx = md5_int(f"{SEED_TAG}|ch|{s}") % len(choices)
    return choices[idx]


def det_int(s: str, lo: int, hi: int) -> int:
    if hi <= lo:
        return int(lo)
    span = hi - lo + 1
    return lo + (md5_int(f"{SEED_TAG}|i|{s}") % span)


def det_float(s: str, lo: float, hi: float) -> float:
    if hi <= lo:
        return float(lo)
    return lo + (hi - lo) * u01(f"{SEED_TAG}|f|{s}")


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 0:
        return n
    if u01(f"{SEED_TAG}|round|{key}") < frac:
        return n + 1
    return n


_NORMAL = NormalDist()


def lognormal_quantile(p50: float, p95: float, q: float) -> float:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    z = _NORMAL.inv_cdf(min(0.999999, max(1e-6, q)))
    return math.exp(mu + sigma * z)


def sample_lognormal_ms(p50: float, p95: float, key: str, cap_mult: float = 3.0) -> int:
    q = 0.05 + 0.90 * u01(f"{SEED_TAG}|lnq|{key}")
    x = lognormal_quantile(p50, p95, q)
    cap = cap_mult * p95
    if x > cap:
        extra = (x - cap) * 0.15
        x = cap + extra
    return max(1, int(round(x)))


def iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def parse_ref(ref: str) -> Tuple[str, str]:
    if "." not in ref:
        raise ValueError(f"Bad ref: {ref}")
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def schedule_even(start: datetime, end: datetime, n: int, key: str, jitter_ms: int = 250) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start for _ in range(n)]
    step = total_s / n
    out = []
    for i in range(n):
        base = (i + 0.5) * step
        j = (u01(f"{SEED_TAG}|jit|{key}|{i}") - 0.5) * 2.0 * (jitter_ms / 1000.0)
        t = start + timedelta(seconds=max(0.0, min(total_s - 1e-6, base + j)))
        out.append(t)
    return out


def format_float(val: float, places: int = 2) -> str:
    return f"{val:.{places}f}"


def merged_vars_spec(comp_id: str, log_id: str, state: str) -> Dict[str, Any]:
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    vars_spec: Dict[str, Any] = dict(tmpl.get("vars", {}))
    state_vars = tmpl.get("state_vars", {})
    if state in state_vars:
        for k, v in state_vars[state].items():
            vars_spec.setdefault(k, v)
    return vars_spec


def get_int_domain(comp_id: str, log_id: str, var_name: str, state: str) -> Optional[Tuple[int, int]]:
    spec = merged_vars_spec(comp_id, log_id, state).get(var_name)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    dom = spec.get("v")
    if not isinstance(dom, (list, tuple)) or len(dom) != 2:
        return None
    return int(dom[0]), int(dom[1])


def clamp_int_to_domain(value: int, dom: Optional[Tuple[int, int]]) -> int:
    if dom is None:
        return int(value)
    lo, hi = dom
    return int(min(hi, max(lo, int(value))))


def intersect_domains(doms: List[Tuple[int, int]]) -> Optional[Tuple[int, int]]:
    if not doms:
        return None
    lo = max(d[0] for d in doms)
    hi = min(d[1] for d in doms)
    if hi < lo:
        # No valid intersection; fall back to the tightest cap around the first domain.
        return doms[0]
    return lo, hi


def find_ref_index(emit_refs: List[str], ref: str) -> Optional[int]:
    try:
        return emit_refs.index(ref)
    except ValueError:
        return None


# -----------------------------
# Template rendering
# -----------------------------
@dataclass(frozen=True)
class Emission:
    timestamp: datetime
    level: str
    message: str
    trace_id: str
    service: str
    host: str


def render_template(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], render_key: str) -> Tuple[str, str]:
    comp = SYSTEM["components"][comp_id]
    tmpl = comp["logs"][log_id]
    msg = tmpl["msg"]
    lvl = tmpl["lvl"]
    vars_spec: Dict[str, Any] = dict(tmpl.get("vars", {}))
    state_vars = tmpl.get("state_vars", {})
    if state in state_vars:
        for k, v in state_vars[state].items():
            vars_spec.setdefault(k, v)

    values: Dict[str, Any] = {}
    for k in vars_spec.keys():
        if k in bound:
            values[k] = bound[k]

    for k, spec in vars_spec.items():
        if k in values:
            continue
        kind = spec["k"]
        dom = spec.get("v", None)
        kkey = f"{render_key}|{comp_id}.{log_id}|{k}"
        if kind == "hex":
            values[k] = det_hex(kkey, int(dom))
        elif kind == "uuid":
            values[k] = det_uuid(kkey)
        elif kind == "ch":
            values[k] = det_choice(kkey, list(dom))
        elif kind == "i":
            lo, hi = int(dom[0]), int(dom[1])
            values[k] = det_int(kkey, lo, hi)
        elif kind == "f":
            lo, hi = float(dom[0]), float(dom[1])
            values[k] = det_float(kkey, lo, hi)
        elif kind == "ip":
            values[k] = "127.0.0.1"
        elif kind == "str":
            values[k] = f"s-{det_hex(kkey, 8)}"
        else:
            values[k] = ""

    if "err_rate" in values:
        values["err_rate"] = format_float(float(values["err_rate"]), 3)
    if "inv_rps" in values:
        values["inv_rps"] = format_float(float(values["inv_rps"]), 2)

    for k, v in list(values.items()):
        if isinstance(v, (np.integer,)):
            values[k] = int(v)
        elif isinstance(v, (np.floating,)):
            values[k] = float(v)

    return lvl, msg.format(**values)


def pick_host_for_component(comp_id: str, key: str, allowed: Optional[List[str]] = None) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts", [])
    if allowed is not None:
        hosts = [h for h in hosts if h in allowed]
    if not hosts:
        return ""
    return det_choice(f"{key}|host|{comp_id}", hosts)


# -----------------------------
# Control state: derive failure intervals with persistent multipliers
# -----------------------------
@dataclass(frozen=True)
class IntervalControl:
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> Tuple[List[IntervalControl], List[Dict[str, Any]]]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = list(SCENARIO["phases"]["f"]["events"])
    events = sorted(events, key=lambda e: (e["at_min"], e.get("order", 0)))

    flow_ids = [f["id"] for f in SYSTEM["flows"]["f"]["req"]]
    flow_rate_mult = {fid: 1.0 for fid in flow_ids}
    flow_latency_mult = {fid: {"p50": 1.0, "p95": 1.0} for fid in flow_ids}

    bg_rate_mult: Dict[str, float] = {}
    for comp_id, comp in SYSTEM["components"].items():
        for src in comp.get("beh", {}).get("f", {}).get("emit", []):
            bg_rate_mult[f"{comp_id}.{src['id']}"] = 1.0

    times = [fstart] + [e["at_min"] for e in events if fstart <= e["at_min"] < fend] + [fend]
    boundaries = []
    for t in times:
        if not boundaries or boundaries[-1] != t:
            boundaries.append(t)
    boundaries = sorted(boundaries)
    if boundaries[0] != fstart:
        boundaries.insert(0, fstart)
    if boundaries[-1] != fend:
        boundaries.append(fend)

    by_time: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_time.setdefault(e["at_min"], []).append(e)

    intervals: List[IntervalControl] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        if s in by_time:
            for ev in by_time[s]:
                for k, v in ev.get("rate_multipliers", {}).items():
                    if "." in k:
                        bg_rate_mult[k] = float(v)
                    else:
                        flow_rate_mult[k] = float(v)
                for fid, mv in ev.get("latency_multipliers", {}).items():
                    flow_latency_mult.setdefault(fid, {"p50": 1.0, "p95": 1.0})
                    flow_latency_mult[fid] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
        intervals.append(
            IntervalControl(
                start_min=s,
                end_min=e,
                flow_rate_mult=dict(flow_rate_mult),
                bg_rate_mult=dict(bg_rate_mult),
                flow_latency_mult={k: dict(v) for k, v in flow_latency_mult.items()},
            )
        )
    return intervals, events


FAIL_INTERVALS, FAIL_EVENTS = build_failure_intervals()

# -----------------------------
# Background emission simulation
# -----------------------------
def emit_background_for_interval(state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]], rows: List[Emission]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f" and bg_mult is not None:
                mult = float(bg_mult.get(mult_key, 1.0))
            effective = per_min * mult
            if effective <= 0:
                continue

            if scope == "global":
                count = deterministic_round(effective * dur_min, f"bg|{state}|{start_min}-{end_min}|{mult_key}|global")
                times = schedule_even(start_dt, end_dt, count, f"bg|{state}|{start_min}-{end_min}|{mult_key}|global", jitter_ms=600)
                for j, t in enumerate(times):
                    host = comp.get("hosts", [])
                    host_val = host[j % len(host)] if host else ""
                    bound = {}
                    if mult_key == "event_router.queue_stats" and state == "f":
                        minute = (t - BASE_TIME).total_seconds() / 60.0
                        frac = min(1.0, max(0.0, (minute - 25.0) / 25.0))
                        depth = int(round(2500 + frac * (18000 - 2500)))
                        max_age = int(round(80 + frac * (850 - 80)))
                        bound = {"depth": depth, "max_age_s": max_age}
                    lvl, msg = render_template(comp_id, log_id, state, bound, f"bg|{mult_key}|{start_min}-{end_min}|{j}")
                    rows.append(Emission(t, lvl, msg, "", comp["svc"], host_val))
            else:
                hosts = comp.get("hosts", [])
                for h in hosts:
                    count = deterministic_round(effective * dur_min, f"bg|{state}|{start_min}-{end_min}|{mult_key}|{h}")
                    times = schedule_even(start_dt, end_dt, count, f"bg|{state}|{start_min}-{end_min}|{mult_key}|{h}", jitter_ms=600)
                    for j, t in enumerate(times):
                        bound = {}
                        if mult_key == "invocation_manager.pool_stats" and state == "f":
                            minute = (t - BASE_TIME).total_seconds() / 60.0
                            frac = min(1.0, max(0.0, (minute - 25.0) / 25.0))
                            allocated = int(round(4000 + frac * (8500 - 4000)))
                            ready = int(round(max(0, 90 - frac * 70)))
                            in_use = int(round(max(0, 80 - frac * 60)))
                            bound = {"allocated": allocated, "ready": ready, "in_use": in_use}
                        if mult_key == "lambda_frontend.frontend_metrics" and state == "f":
                            minute = (t - BASE_TIME).total_seconds() / 60.0
                            frac = min(1.0, max(0.0, (minute - 25.0) / 25.0))
                            err_rate = min(0.70, 0.18 + frac * 0.30)
                            p95_ms = int(round(600 + frac * 2400))
                            bound = {"err_rate": err_rate, "p95_ms": p95_ms}
                        lvl, msg = render_template(comp_id, log_id, state, bound, f"bg|{mult_key}|{start_min}-{end_min}|{h}|{j}")
                        rows.append(Emission(t, lvl, msg, "", comp["svc"], h))


# -----------------------------
# Flow simulation
# -----------------------------
def attempt_count(expected: float, max_attempts: int, key: str) -> int:
    max_attempts = max(1, int(max_attempts))
    expected = float(expected)
    if expected <= 1.0:
        hi = min(max_attempts, 2)
        if hi == 1:
            return 1
        p = max(0.0, min(1.0, expected - 1.0))
        return 2 if u01(f"{SEED_TAG}|att|{key}") < p else 1
    lo = int(math.floor(expected))
    hi = min(max_attempts, lo + 1)
    lo = max(1, min(max_attempts, lo))
    if hi == lo:
        return lo
    p = max(0.0, min(1.0, expected - lo))
    return hi if u01(f"{SEED_TAG}|att|{key}") < p else lo


def flow_latency_scaled(flow_id: str, base_pair: List[float], state: str, lat_mult: Optional[Dict[str, Dict[str, float]]]) -> Tuple[float, float]:
    p50, p95 = float(base_pair[0]), float(base_pair[1])
    if state != "f" or not lat_mult:
        return p50, p95
    m = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    return p50 * float(m.get("p50", 1.0)), p95 * float(m.get("p95", 1.0))


def bind_common_context(flow_id: str, state: str, instance_key: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    trace_id = det_hex(f"{instance_key}|trace", 32) if SYSTEM["tracing"]["on"] else ""
    ctx["trace_id"] = trace_id

    if "async_event_delivery" in flow_id:
        ctx["evt_id"] = det_uuid(f"{instance_key}|evt_id")
        ctx["invoke_type"] = "async"
        ctx["fn"] = det_choice(f"{instance_key}|fn", ["fn-auth", "fn-orders", "fn-metrics"])
        ctx["req_id"] = det_uuid(f"{instance_key}|req_id")
    elif "sts_saml_federation" in flow_id:
        ctx["req_id"] = det_uuid(f"{instance_key}|req_id")
        ctx["idp"] = det_choice(f"{instance_key}|idp", ["okta", "aadfs"])
        ctx["invoke_type"] = "sync"
        ctx["fn"] = "fn-auth"
    else:
        ctx["req_id"] = det_uuid(f"{instance_key}|req_id")
        ctx["invoke_type"] = "sync"
        ctx["fn"] = det_choice(f"{instance_key}|fn", ["fn-auth", "fn-orders", "fn-metrics"])

    if state == "f":
        if flow_id in ("sync_invoke_f_err", "async_event_delivery_f_fail", "sts_saml_federation_f"):
            r = u01(f"{instance_key}|errclass")
            if r < 0.75:
                ctx["_err"] = "NoCapacity"
                ctx["_status"] = 503
                ctx["_invoke_retry_reason"] = "no_capacity"
                ctx["_prov_reason"] = "no_ready_env"
                ctx["_defer_reason"] = "invoke_error"
                ctx["_sts_err"] = "UpstreamLambdaError"
                ctx["_sts_status"] = 503
            elif r < 0.90:
                ctx["_err"] = "Timeout"
                ctx["_status"] = 504
                ctx["_invoke_retry_reason"] = "upstream_timeout"
                ctx["_prov_reason"] = "no_ready_env"
                ctx["_defer_reason"] = "invoke_error"
                ctx["_sts_err"] = "UpstreamLambdaError"
                ctx["_sts_status"] = 503
            else:
                ctx["_err"] = "Throttled"
                ctx["_status"] = 429
                ctx["_invoke_retry_reason"] = "no_capacity"
                ctx["_prov_reason"] = "quota_reached"
                ctx["_defer_reason"] = "throttled"
                ctx["_sts_err"] = "Throttled"
                ctx["_sts_status"] = 429
    return ctx


def emit_subset_for_attempt(flow_id: str, state: str, attempt: int, total_attempts: int, emit_refs: List[str], lat_pairs: List[List[float]]) -> Tuple[List[str], List[List[float]]]:
    # For the normal async flow, retries are rare and represent an earlier delivery attempt that
    # did not reach terminal success; the model lacks explicit failure logs in normal state.
    if state == "n" and flow_id == "async_event_delivery_ok_n" and total_attempts > 1 and attempt < total_attempts:
        return [emit_refs[0]], [lat_pairs[0]]
    return emit_refs, lat_pairs


def apply_duration_caps(state: str, emit_refs: List[str], times: List[datetime]) -> List[datetime]:
    """
    If any emitted log carries a dur_ms field, compress the timeline (keeping the first emitted log fixed)
    so that the implied dur_ms fits within that log template's integer domain.

    IMPORTANT: lambda_frontend.* dur_ms is bound to lambda_frontend.invoke_received time (per-attempt),
    not to the first emitted log (which can be an upstream component like event_router.delivery_attempt).
    """
    if not times:
        return times
    if len(times) != len(emit_refs):
        return times

    t0 = times[0]
    lf_idx = find_ref_index(emit_refs, "lambda_frontend.invoke_received")
    sts_idx = find_ref_index(emit_refs, "sts_gateway.saml_req")

    def base_for(ref: str) -> datetime:
        if ref.startswith("lambda_frontend.") and lf_idx is not None:
            return times[lf_idx]
        if ref.startswith("sts_gateway.") and sts_idx is not None:
            return times[sts_idx]
        return t0

    scale = 1.0
    for j, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        dom = get_int_domain(comp_id, log_id, "dur_ms", state)
        if dom is None:
            continue
        _, hi = dom
        base_t = base_for(ref)
        dur_ms = int(round((times[j] - base_t).total_seconds() * 1000.0))
        if dur_ms > hi and dur_ms > 0:
            scale = min(scale, hi / float(dur_ms))

    if scale >= 0.999999:
        return times

    new_times = [t0]
    for j in range(1, len(times)):
        dt = times[j] - t0
        new_times.append(t0 + timedelta(seconds=dt.total_seconds() * scale))
    return new_times


def simulate_flow_instance(flow_def: Dict[str, Any], state: str, start_time: datetime, rate_lat_mult: Optional[Dict[str, Dict[str, float]]], instance_index: int, rows: List[Emission]) -> None:
    flow_id = flow_def["id"]
    instance_key = f"flow|{state}|{flow_id}|{int(start_time.timestamp()*1000)}|{instance_index}"

    ctx = bind_common_context(flow_id, state, instance_key)
    trace_id_col = ctx["trace_id"] if flow_def.get("trace", False) else ""

    comp_hosts: Dict[str, str] = {}
    for ref in flow_def["emit"] + list(flow_def["retry"].get("emit_per_retry", [])):
        comp_id, _ = parse_ref(ref)
        if comp_id not in comp_hosts:
            comp_hosts[comp_id] = pick_host_for_component(comp_id, f"{instance_key}|stick|{comp_id}")

    max_att = int(flow_def["retry"]["max_attempts"])
    exp_att = float(flow_def["retry"]["expected_attempts"])
    A = attempt_count(exp_att, max_att, f"{instance_key}|attcount")

    base_lag_s = None
    if "async_event_delivery" in flow_id:
        if state == "n":
            base_lag_s = det_int(f"{instance_key}|lag0", 0, 5)
        else:
            minute = (start_time - BASE_TIME).total_seconds() / 60.0
            frac = min(1.0, max(0.0, (minute - 25.0) / 25.0))
            base_lag_s = int(round(30 + frac * 650))
            base_lag_s = max(10, min(900, base_lag_s))

    attempt_start = start_time
    cumulative_elapsed_ms = 0

    for attempt in range(1, A + 1):
        if attempt > 1:
            prev_end = attempt_start  # attempt_start is end of previous attempt after last loop iteration.

            pair = flow_def["retry"]["backoff_ms"][attempt - 2]
            sampled_backoff_ms = sample_lognormal_ms(pair[0], pair[1], f"{instance_key}|backoff|{attempt}", cap_mult=3.0)

            # Enforce template variable-domain coherence for any retry log that carries backoff_ms.
            # Use the SAME clamped value for both the logged backoff_ms and the actual inter-attempt timing.
            backoff_domains: List[Tuple[int, int]] = []
            for retry_ref in flow_def["retry"].get("emit_per_retry", []):
                rcomp, rlog = parse_ref(retry_ref)
                dom = get_int_domain(rcomp, rlog, "backoff_ms", state)
                if dom is not None:
                    backoff_domains.append(dom)
            dom_int = intersect_domains(backoff_domains)
            backoff_ms = clamp_int_to_domain(sampled_backoff_ms, dom_int)

            cumulative_elapsed_ms += backoff_ms

            # Emit retry-only logs once per retry (attempts 2..A). Timestamp near previous attempt end.
            for retry_ref in flow_def["retry"].get("emit_per_retry", []):
                rcomp, rlog = parse_ref(retry_ref)
                bound_retry = dict(ctx)
                if retry_ref == "lambda_frontend.invoke_retry":
                    bound_retry.update(
                        {
                            "attempt": attempt,
                            "reason": ctx.get("_invoke_retry_reason", det_choice(f"{instance_key}|retryreason|{attempt}", ["no_capacity", "upstream_timeout"])),
                        }
                    )
                if retry_ref == "event_router.delivery_retry":
                    bound_retry.update({"attempt": attempt, "backoff_ms": backoff_ms})
                rtime = prev_end + timedelta(milliseconds=1)
                lvl, msg = render_template(rcomp, rlog, state, bound_retry, f"{instance_key}|retryemit|{attempt}|{retry_ref}")
                rows.append(Emission(rtime, lvl, msg, trace_id_col, SYSTEM["components"][rcomp]["svc"], comp_hosts.get(rcomp, "")))

            attempt_start = prev_end + timedelta(milliseconds=backoff_ms)

        emit_refs_full = flow_def["emit"]
        lat_pairs_full = flow_def["latency_ms"]
        assert len(emit_refs_full) == len(lat_pairs_full)

        emit_refs, lat_pairs = emit_subset_for_attempt(flow_id, state, attempt, A, emit_refs_full, lat_pairs_full)

        t = attempt_start
        times: List[datetime] = []
        for j, pair in enumerate(lat_pairs):
            p50, p95 = flow_latency_scaled(flow_id, pair, state, rate_lat_mult)
            dms = sample_lognormal_ms(p50, p95, f"{instance_key}|lat|a{attempt}|j{j}", cap_mult=3.0)
            t = t + timedelta(milliseconds=dms)
            times.append(t)

        times = apply_duration_caps(state, emit_refs, times)

        lf_idx = find_ref_index(emit_refs, "lambda_frontend.invoke_received")
        lf_received_time = times[lf_idx] if (lf_idx is not None and 0 <= lf_idx < len(times)) else None
        sts_idx = find_ref_index(emit_refs, "sts_gateway.saml_req")
        sts_req_time = times[sts_idx] if (sts_idx is not None and 0 <= sts_idx < len(times)) else (times[0] if times else None)

        lag_s_attempt = None
        if base_lag_s is not None:
            lag_s_attempt = int(round(base_lag_s + cumulative_elapsed_ms / 1000.0))
            if state == "n":
                lag_s_attempt = max(0, min(5, lag_s_attempt))
            else:
                lag_s_attempt = max(10, min(900, lag_s_attempt))

        for j, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            bound = dict(ctx)

            if ref == "invocation_manager.allocate_env" or ref == "exec_env_pool.env_ready":
                alloc_id = det_hex(f"{instance_key}|alloc|a{attempt}", 16)
                bound["alloc_id"] = alloc_id
            if ref == "exec_env_pool.env_ready":
                bound["host"] = comp_hosts.get(comp_id, pick_host_for_component(comp_id, f"{instance_key}|envhost|a{attempt}"))
                bound["cold_ms"] = det_int(f"{instance_key}|cold|a{attempt}", 10, 800)

            if ref == "invocation_manager.provision_blocked":
                bound["reason"] = ctx.get("_prov_reason", det_choice(f"{instance_key}|provreason|a{attempt}", ["no_ready_env", "quota_reached"]))
                if state == "f":
                    bound["ready"] = det_int(f"{instance_key}|ready|a{attempt}", 0, 60)
                    bound["allocated"] = det_int(f"{instance_key}|allocated|a{attempt}", 4000, 10000)
                else:
                    bound["ready"] = det_int(f"{instance_key}|ready|a{attempt}", 200, 600)
                    bound["allocated"] = det_int(f"{instance_key}|allocated|a{attempt}", 1500, 2500)

            if ref == "lambda_frontend.invoke_received":
                bound["invoke_type"] = ctx["invoke_type"]
                bound["fn"] = ctx["fn"]

            if ref == "event_router.delivery_attempt" and lag_s_attempt is not None:
                bound["lag_s"] = lag_s_attempt
            if ref == "event_router.delivery_success" and lag_s_attempt is not None:
                bound["lag_s"] = lag_s_attempt

            if ref in ("lambda_frontend.invoke_success", "lambda_frontend.invoke_error", "lambda_frontend.async_accepted"):
                base_t = lf_received_time if lf_received_time is not None else times[0]
                dur_ms = int(round((times[j] - base_t).total_seconds() * 1000.0))
                dur_ms = max(1, dur_ms)
                dom = get_int_domain(comp_id, log_id, "dur_ms", state)
                bound["dur_ms"] = clamp_int_to_domain(dur_ms, dom)

            if ref == "lambda_frontend.invoke_error":
                if state == "f" and flow_id in ("sync_invoke_f_err", "async_event_delivery_f_fail", "sts_saml_federation_f"):
                    bound["status"] = ctx.get("_status", 503)
                    bound["err"] = ctx.get("_err", "NoCapacity")
                else:
                    bound["status"] = det_choice(f"{instance_key}|status|a{attempt}", [429, 500, 503, 504])
                    bound["err"] = det_choice(f"{instance_key}|err|a{attempt}", ["NoCapacity", "Timeout", "Throttled"])

            if ref == "event_router.delivery_deferred":
                if state == "f" and flow_id == "async_event_delivery_f_fail":
                    bound["reason"] = ctx.get("_defer_reason", "invoke_error")

            if ref == "sts_gateway.saml_ok":
                base_t = sts_req_time if sts_req_time is not None else times[0]
                dur_ms = int(round((times[j] - base_t).total_seconds() * 1000.0))
                dom = get_int_domain(comp_id, log_id, "dur_ms", state)
                bound["dur_ms"] = clamp_int_to_domain(max(1, dur_ms), dom)

            if ref == "sts_gateway.saml_throttled":
                base_t = sts_req_time if sts_req_time is not None else times[0]
                dur_ms = int(round((times[j] - base_t).total_seconds() * 1000.0))
                dom = get_int_domain(comp_id, log_id, "dur_ms", state)
                bound["dur_ms"] = clamp_int_to_domain(max(1, dur_ms), dom)
                if state == "f" and flow_id == "sts_saml_federation_f":
                    bound["status"] = ctx.get("_sts_status", 503)
                    bound["err"] = ctx.get("_sts_err", "UpstreamLambdaError")

            lvl, msg = render_template(comp_id, log_id, state, bound, f"{instance_key}|emit|a{attempt}|j{j}|{ref}")
            rows.append(Emission(times[j], lvl, msg, trace_id_col, SYSTEM["components"][comp_id]["svc"], comp_hosts.get(comp_id, "")))

        attempt_dur = int(round((times[-1] - attempt_start).total_seconds() * 1000.0)) if times else 0
        cumulative_elapsed_ms += max(0, attempt_dur)
        attempt_start = times[-1] if times else attempt_start


def simulate_flows_for_interval(state: str, start_min: int, end_min: int, rate_mult: Optional[Dict[str, float]], lat_mult: Optional[Dict[str, Dict[str, float]]], rows: List[Emission]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for flow_def in SYSTEM["flows"][state]["req"]:
        flow_id = flow_def["id"]
        rpm = float(flow_def["rpm"])
        m = 1.0
        if state == "f" and rate_mult is not None:
            m = float(rate_mult.get(flow_id, 1.0))
        eff_rpm = rpm * m
        if eff_rpm <= 0:
            continue
        expected_instances = eff_rpm * dur_min
        n_instances = deterministic_round(expected_instances, f"flow|{state}|{start_min}-{end_min}|{flow_id}")
        starts = schedule_even(start_dt, end_dt, n_instances, f"flow|{state}|{start_min}-{end_min}|{flow_id}", jitter_ms=400)
        for i, st in enumerate(starts):
            simulate_flow_instance(flow_def, state, st, lat_mult, i, rows)


# -----------------------------
# One-shots
# -----------------------------
def emit_one_shots(rows: List[Emission]) -> None:
    for ev in FAIL_EVENTS:
        at_min = int(ev["at_min"])
        t0 = BASE_TIME + timedelta(minutes=at_min)
        one_shots = ev.get("one_shots", [])
        for shot in one_shots:
            ref = shot["ref"]
            comp_id, log_id = parse_ref(ref)
            count = int(shot["count"])
            allowed_hosts = list(shot.get("hosts", [])) if shot.get("hosts") is not None else None
            for k in range(count):
                jms = int(round((u01(f"{SEED_TAG}|oneshot|{at_min}|{ref}|{k}") * 1500.0)))
                ts = t0 + timedelta(milliseconds=jms)
                host = pick_host_for_component(comp_id, f"oneshot|{at_min}|{ref}|{k}", allowed=allowed_hosts)
                lvl, msg = render_template(comp_id, log_id, "f", {}, f"oneshot|{at_min}|{ref}|{k}")
                rows.append(Emission(ts, lvl, msg, "", SYSTEM["components"][comp_id]["svc"], host))


# -----------------------------
# Main simulation
# -----------------------------
def main() -> None:
    rows: List[Emission] = []

    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    emit_background_for_interval("n", nstart, nend, None, rows)
    simulate_flows_for_interval("n", nstart, nend, None, None, rows)

    for ic in FAIL_INTERVALS:
        emit_background_for_interval("f", ic.start_min, ic.end_min, ic.bg_rate_mult, rows)
        simulate_flows_for_interval("f", ic.start_min, ic.end_min, ic.flow_rate_mult, ic.flow_latency_mult, rows)

    emit_one_shots(rows)

    df = pd.DataFrame(
        {
            "timestamp": [iso_z(r.timestamp) for r in rows],
            "level": [r.level for r in rows],
            "message": [r.message for r in rows],
            "trace_id": [r.trace_id for r in rows],
            "service": [r.service for r in rows],
            "host": [r.host for r in rows],
        }
    )

    df["_ts"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values(["_ts", "service", "host", "level", "message"], kind="mergesort").drop(columns=["_ts"]).reset_index(drop=True)

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count out of bounds: {len(df)} (expected 20k-100k)")
    expected_cols = ["timestamp", "level", "message", "trace_id", "service", "host"]
    if list(df.columns) != expected_cols:
        raise RuntimeError(f"Bad columns: {list(df.columns)}")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
