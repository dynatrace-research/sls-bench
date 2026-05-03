import math
import hashlib
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "edge_waf_managed_rules"},
    "states": {"n": "normal", "f": "failure"},
    "components": {
        "edge_proxy": {
            "svc": "edge-proxy",
            "hosts": ["edge-sfo-1", "edge-lhr-1", "edge-sin-1"],
            "logs": {
                "access_2xx": {
                    "lvl": "INFO",
                    "msg": "edge_access status=200 method={method} host={host} uri={uri} dur_ms={dur_ms} bytes={bytes} cf_ray={ray} colo={colo}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "api.example.net"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "bytes": {"k": "i", "v": [200, 20000]},
                        "ray": {"k": "hex", "v": 16},
                        "colo": {"k": "ch", "v": ["SFO", "LHR", "SIN"]},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [10, 300]}},
                        "f": {"dur_ms": {"k": "i", "v": [50, 2500]}},
                    },
                },
                "access_502": {
                    "lvl": "WARN",
                    "msg": "edge_access status=502 method={method} host={host} uri={uri} dur_ms={dur_ms} err={err} cf_ray={ray} colo={colo}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "api.example.net"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [80, 4000]},
                        "err": {"k": "ch", "v": ["waf_timeout", "waf_cpu_overload"]},
                        "ray": {"k": "hex", "v": 16},
                        "colo": {"k": "ch", "v": ["SFO", "LHR", "SIN"]},
                    },
                    "state_vars": {},
                },
                "pop_health": {
                    "lvl": "INFO",
                    "msg": "pop_health colo={colo} req_rpm={req_rpm} err_5xx_pct={err_5xx_pct} worker_busy_pct={busy_pct}",
                    "vars": {
                        "colo": {"k": "ch", "v": ["SFO", "LHR", "SIN"]},
                        "req_rpm": {"k": "i", "v": [50, 2000]},
                        "err_5xx_pct": {"k": "f", "v": [0.0, 95.0]},
                        "busy_pct": {"k": "f", "v": [5.0, 100.0]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pop_health", "per_min": 0.5}]},  # scope omitted => per_host
                "f": {"emit": [{"id": "pop_health", "per_min": 0.5}]},
            },
        },
        "waf_engine": {
            "svc": "waf-engine",
            "hosts": ["waf-sfo-1", "waf-lhr-1", "waf-sin-1"],
            "logs": {
                "managed_rules_loaded": {
                    "lvl": "INFO",
                    "msg": "managed_rules_loaded ruleset_ver={ruleset_ver} mode={mode} top_rule_id={top_rule_id}",
                    "vars": {
                        "ruleset_ver": {"k": "ch", "v": ["mr-2026.04.02-bad"]},
                        "mode": {"k": "ch", "v": ["simulate"]},
                        "top_rule_id": {"k": "ch", "v": ["mr_js_inline_17"]},
                    },
                    "state_vars": {},
                },
                "cpu_sample": {
                    "lvl": "INFO",
                    "msg": "waf_cpu usage_pct={cpu_pct} runq={runq} colo={colo}",
                    "vars": {"colo": {"k": "ch", "v": ["SFO", "LHR", "SIN"]}},
                    "state_vars": {
                        "n": {
                            "cpu_pct": {"k": "f", "v": [10.0, 45.0]},
                            "runq": {"k": "i", "v": [0, 6]},
                        },
                        "f": {
                            "cpu_pct": {"k": "f", "v": [90.0, 100.0]},
                            "runq": {"k": "i", "v": [20, 200]},
                        },
                    },
                },
                "regex_stats": {
                    "lvl": "INFO",
                    "msg": "waf_regex_stats ruleset_ver={ruleset_ver} slow_evals={slow_evals} p95_eval_ms={p95_eval_ms} top_rule_id={top_rule_id} top_regex_id={top_regex_id} colo={colo}",
                    "vars": {"colo": {"k": "ch", "v": ["SFO", "LHR", "SIN"]}},
                    "state_vars": {
                        "n": {
                            "ruleset_ver": {"k": "ch", "v": ["mr-2026.04.02-good"]},
                            "slow_evals": {"k": "i", "v": [0, 20]},
                            "p95_eval_ms": {"k": "i", "v": [1, 10]},
                            "top_rule_id": {"k": "ch", "v": ["mr_common_01", "mr_xss_03"]},
                            "top_regex_id": {"k": "ch", "v": ["re_ok_01", "re_ok_07"]},
                        },
                        "f": {
                            "ruleset_ver": {"k": "ch", "v": ["mr-2026.04.02-bad"]},
                            "slow_evals": {"k": "i", "v": [500, 5000]},
                            "p95_eval_ms": {"k": "i", "v": [80, 1200]},
                            "top_rule_id": {"k": "ch", "v": ["mr_js_inline_17"]},
                            "top_regex_id": {"k": "ch", "v": ["re_inline_js_catastrophic"]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cpu_sample", "per_min": 1.0}, {"id": "regex_stats", "per_min": 1.0}]},
                "f": {"emit": [{"id": "cpu_sample", "per_min": 1.0}, {"id": "regex_stats", "per_min": 1.0}]},
            },
        },
        "ruleset_control_plane": {
            "svc": "waf-managed-control",
            "hosts": ["cp-1"],
            "logs": {
                "deploy_ruleset": {
                    "lvl": "INFO",
                    "msg": "deploy_ruleset product=waf_managed ruleset_ver={ruleset_ver} scope={scope} mode={mode} change_id={change_id}",
                    "vars": {
                        "ruleset_ver": {"k": "ch", "v": ["mr-2026.04.02-bad"]},
                        "scope": {"k": "ch", "v": ["global"]},
                        "mode": {"k": "ch", "v": ["simulate"]},
                        "change_id": {"k": "ch", "v": ["chg-9f3a1c2b"]},
                    },
                    "state_vars": {},
                },
                "termination_requested": {
                    "lvl": "WARN",
                    "msg": "termination_requested product=waf_managed scope=global reason={reason} change_id={change_id}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["cpu_exhaustion", "widespread_502s"]},
                        "change_id": {"k": "ch", "v": ["chg-9f3a1c2b"]},
                    },
                    "state_vars": {},
                },
                "apply_status": {
                    "lvl": "INFO",
                    "msg": "apply_status change_id={change_id} progressed_pct={progress_pct} target={target}",
                    "vars": {
                        "change_id": {"k": "ch", "v": ["chg-9f3a1c2b"]},
                        "progress_pct": {"k": "i", "v": [0, 100]},
                        "target": {"k": "ch", "v": ["waf_workers_global"]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": []},
                "f": {"emit": [{"id": "apply_status", "per_min": 0.5, "scope": "global"}]},
            },
        },
        "monitoring": {
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "monitor_heartbeat ok=true shard={shard}",
                    "vars": {"shard": {"k": "ch", "v": ["a", "b"]}},
                    "state_vars": {},
                },
                "alert_cpu_global": {
                    "lvl": "CRITICAL",
                    "msg": "alert_firing name=GlobalCPUHigh value_pct={value_pct} threshold_pct={threshold_pct}",
                    "vars": {
                        "value_pct": {"k": "f", "v": [70.0, 100.0]},
                        "threshold_pct": {"k": "f", "v": [85.0, 95.0]},
                    },
                    "state_vars": {},
                },
                "alert_502_rate": {
                    "lvl": "CRITICAL",
                    "msg": "alert_firing name=Edge502Rate value_pct={value_pct} threshold_pct={threshold_pct}",
                    "vars": {
                        "value_pct": {"k": "f", "v": [1.0, 95.0]},
                        "threshold_pct": {"k": "f", "v": [2.0, 10.0]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 0.5, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "heartbeat", "per_min": 0.5, "scope": "global"},
                        {"id": "alert_cpu_global", "per_min": 1.0, "scope": "global"},
                        {"id": "alert_502_rate", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
    },
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "req": [
                {
                    "id": "http_request_2xx",
                    "rpm": 1200.0,
                    "emit": ["edge_proxy.access_2xx"],
                    "latency_ms": [[25, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "http_request_2xx_degraded",
                    "rpm": 800.0,
                    "emit": ["edge_proxy.access_2xx"],
                    "latency_ms": [[120, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "http_request_502",
                    "rpm": 400.0,
                    "emit": ["edge_proxy.access_502"],
                    "latency_ms": [[200, 1800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "global_edge_502_from_waf_regex_cpu"},
    "time": {
        "total_minutes": 44,
        "phases": {"n": {"start_min": 0, "end_min": 22}, "f": {"start_min": 22, "end_min": 44}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 22,
                    "rate_multipliers": {
                        "monitoring.alert_cpu_global": 0.0,
                        "monitoring.alert_502_rate": 0.0,
                        "ruleset_control_plane.apply_status": 0.0,
                    },
                    "latency_multipliers": {
                        "http_request_2xx_degraded": {"p50": 1.2, "p95": 1.4},
                        "http_request_502": {"p50": 1.0, "p95": 1.2},
                    },
                    "one_shots": [
                        {"ref": "ruleset_control_plane.deploy_ruleset", "count": 1, "hosts": ["cp-1"]},
                        {
                            "ref": "waf_engine.managed_rules_loaded",
                            "count": 3,
                            "hosts": ["waf-sfo-1", "waf-lhr-1", "waf-sin-1"],
                        },
                    ],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {
                        "http_request_2xx_degraded": 0.4,
                        "http_request_502": 2.0,
                        "monitoring.alert_cpu_global": 1.0,
                        "monitoring.alert_502_rate": 1.0,
                    },
                    "latency_multipliers": {
                        "http_request_2xx_degraded": {"p50": 1.4, "p95": 1.7},
                        "http_request_502": {"p50": 1.3, "p95": 1.6},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 42,
                    "rate_multipliers": {
                        "http_request_2xx_degraded": 0.05,
                        "http_request_502": 0.4,
                        "ruleset_control_plane.apply_status": 1.0,
                    },
                    "latency_multipliers": {
                        "http_request_2xx_degraded": {"p50": 1.5, "p95": 1.8},
                        "http_request_502": {"p50": 1.4, "p95": 1.7},
                    },
                    "one_shots": [
                        {"ref": "ruleset_control_plane.termination_requested", "count": 1, "hosts": ["cp-1"]}
                    ],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------

GLOBAL_SEED = 1337
BASE_TIME = datetime(2026, 4, 3, 0, 0, 0, tzinfo=timezone.utc)


def stable_u01(key: str) -> float:
    h = hashlib.md5(key.encode("utf-8")).digest()
    x = int.from_bytes(h, "big")
    return (x + 1) / (2**128 + 2)


def stable_seed32(key: str) -> int:
    h = hashlib.md5(f"{GLOBAL_SEED}|{key}".encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big")


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    f = math.floor(expected)
    frac = expected - f
    u = stable_u01(f"{key}|{expected:.9f}")
    return int(f + (1 if u < frac else 0))


def isoformat_ms(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_colo_from_host(host: str) -> str:
    h = host.lower()
    if "sfo" in h:
        return "SFO"
    if "lhr" in h:
        return "LHR"
    if "sin" in h:
        return "SIN"
    return "SFO"


def lognormal_from_p50_p95(rng: np.random.Generator, p50: float, p95: float, n: int) -> np.ndarray:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    x = rng.lognormal(mean=mu, sigma=sigma, size=n)
    cap = 3.0 * p95
    x = np.minimum(x, cap)
    x = np.maximum(x, 1.0)
    return x


def pick_choice(rng: np.random.Generator, choices: List[Any]) -> Any:
    idx = int(rng.integers(0, len(choices)))
    return choices[idx]


def gen_hex(key: str, length: int) -> str:
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:length]


def sample_var(rng: np.random.Generator, dom: Dict[str, Any], key: str, var_name: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        return pick_choice(rng, list(v))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        val = lo + (hi - lo) * float(rng.random())
        return f"{val:.1f}"
    if k == "hex":
        ln = int(v)
        return gen_hex(f"{key}|{var_name}", ln)
    if k == "uuid":
        raw = hashlib.md5(f"{key}|{var_name}".encode("utf-8")).hexdigest()
        return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"
    if k == "ip":
        u = stable_u01(f"{key}|{var_name}")
        octet = 1 + int(u * 254)
        return f"192.0.2.{octet}"
    if k == "str":
        return str(v)
    return str(v)


def get_var_domain(comp_id: str, log_id: str, state: str, var_name: str) -> Optional[Dict[str, Any]]:
    log = SYSTEM["components"][comp_id]["logs"][log_id]
    if var_name in log.get("vars", {}):
        return log["vars"][var_name]
    if var_name in log.get("state_vars", {}).get(state, {}):
        return log["state_vars"][state][var_name]
    return None


def clamp_value_to_domain(val: Any, dom: Optional[Dict[str, Any]]) -> Any:
    if dom is None:
        return val
    k = dom.get("k")
    v = dom.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        try:
            ival = int(val)
        except Exception:
            return max(lo, min(hi, int(lo)))
        return max(lo, min(hi, ival))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        try:
            fval = float(val)
        except Exception:
            fval = lo
        fval = max(lo, min(hi, fval))
        return f"{fval:.1f}"
    return val


def render_log_message(
    comp_id: str,
    log_id: str,
    state: str,
    rng: np.random.Generator,
    key: str,
    bindings: Dict[str, Any],
) -> Tuple[str, str]:
    comp = SYSTEM["components"][comp_id]
    log = comp["logs"][log_id]
    template = log["msg"]

    doms: Dict[str, Dict[str, Any]] = {}
    doms.update(log.get("vars", {}))
    doms.update(log.get("state_vars", {}).get(state, {}))

    values: Dict[str, Any] = dict(bindings)

    # Clamp pre-bound values to their configured domains.
    for var_name in list(values.keys()):
        values[var_name] = clamp_value_to_domain(values[var_name], doms.get(var_name))

    for var_name, dom in doms.items():
        if var_name in values:
            continue
        values[var_name] = sample_var(rng, dom, key, var_name)

    msg = template.format(**{k: str(v) for k, v in values.items()})
    return log["lvl"], msg


# ----------------------------
# Timeline / control derivation
# ----------------------------

def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted(set([f_start] + [e["at_min"] for e in events] + [f_end]))

    intervals: List[Dict[str, Any]] = []
    current_rate: Dict[str, float] = {}
    current_lat: Dict[str, Dict[str, float]] = {}

    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]

        for ev in [e for e in events if e["at_min"] == start]:
            for k, v in ev.get("rate_multipliers", {}).items():
                current_rate[k] = float(v)
            for flow_id, mults in ev.get("latency_multipliers", {}).items():
                current_lat[flow_id] = {"p50": float(mults["p50"]), "p95": float(mults["p95"])}

        intervals.append(
            {
                "state": "f",
                "start_min": start,
                "end_min": end,
                "rate_mult": dict(current_rate),
                "lat_mult": {k: dict(v) for k, v in current_lat.items()},
            }
        )

    return intervals


def build_all_intervals() -> List[Dict[str, Any]]:
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    intervals = [{"state": "n", "start_min": n_start, "end_min": n_end, "rate_mult": {}, "lat_mult": {}}]
    intervals.extend(build_failure_intervals())
    return intervals


# ----------------------------
# Emission planning and simulation
# ----------------------------

def schedule_evenly(
    start_epoch_ms: int, end_epoch_ms: int, n: int, rng: np.random.Generator, jitter_ms: int = 200
) -> np.ndarray:
    if n <= 0:
        return np.empty((0,), dtype=np.int64)
    dur = max(1, end_epoch_ms - start_epoch_ms)
    base = start_epoch_ms + (np.arange(n, dtype=np.float64) + 0.5) * (dur / n)
    jit = rng.integers(-jitter_ms, jitter_ms + 1, size=n, dtype=np.int64)
    t = base.astype(np.int64) + jit
    t = np.clip(t, start_epoch_ms, end_epoch_ms - 1)
    return t.astype(np.int64)


def interval_epoch_bounds(interval: Dict[str, Any]) -> Tuple[int, int]:
    start_dt = BASE_TIME + timedelta(minutes=int(interval["start_min"]))
    end_dt = BASE_TIME + timedelta(minutes=int(interval["end_min"]))
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def emit_background_for_interval(
    rows: List[Tuple[int, str, str, str, str, str]],
    interval: Dict[str, Any],
    pop_context: Dict[str, Any],
) -> None:
    state = interval["state"]
    start_ms, end_ms = interval_epoch_bounds(interval)
    dur_min = interval["end_min"] - interval["start_min"]

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")  # omitted => per_host
            rate_key = f"{comp_id}.{log_id}"
            mult = float(interval.get("rate_mult", {}).get(rate_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * dur_min
                n = det_round(expected, f"bg|{state}|{rate_key}|{interval['start_min']}-{interval['end_min']}")
                if n <= 0:
                    continue
                rng = np.random.default_rng(stable_seed32(f"bg|{rate_key}|{interval['start_min']}|global"))
                times = schedule_evenly(start_ms, end_ms, n, rng, jitter_ms=500)
                host = comp["hosts"][0] if comp.get("hosts") else ""
                for i, ts in enumerate(times.tolist()):
                    bindings: Dict[str, Any] = {}
                    if comp_id == "edge_proxy" and log_id == "pop_health":
                        bindings["colo"] = get_colo_from_host(host)
                        rpm_center = pop_context["edge_req_rpm_per_pop"]
                        rpm = int(max(50, min(2000, rpm_center + int(rng.integers(-15, 16)))))
                        bindings["req_rpm"] = rpm
                        bindings["err_5xx_pct"] = f"{pop_context['edge_err_5xx_pct']:.1f}"
                        bindings["busy_pct"] = f"{pop_context['edge_busy_pct']:.1f}"
                    elif comp_id == "waf_engine" and log_id in ("cpu_sample", "regex_stats"):
                        bindings["colo"] = get_colo_from_host(host)
                    elif comp_id == "monitoring" and log_id in ("alert_cpu_global", "alert_502_rate"):
                        # Ensure "alert_firing" messages are internally consistent: value >= threshold.
                        if log_id == "alert_cpu_global":
                            thr = 90.0 + 5.0 * float(rng.random())  # within 85..95 domain after clamp
                            val = thr + (100.0 - thr) * float(rng.random())
                            bindings["threshold_pct"] = f"{thr:.1f}"
                            bindings["value_pct"] = f"{val:.1f}"
                        else:
                            thr = 2.0 + 8.0 * float(rng.random())  # 2..10
                            # Keep value reasonably above threshold, still within 1..95.
                            base = max(thr + 1.0, 15.0)
                            val = base + (95.0 - base) * float(rng.random())
                            bindings["threshold_pct"] = f"{thr:.1f}"
                            bindings["value_pct"] = f"{val:.1f}"
                    lvl, msg = render_log_message(comp_id, log_id, state, rng, f"{rate_key}|{ts}|{i}", bindings)
                    rows.append((ts, lvl, msg, "", comp["svc"] or "", host or ""))
            else:
                for host in comp.get("hosts", []) if comp.get("hosts") else [""]:
                    expected = eff_per_min * dur_min
                    n = det_round(
                        expected, f"bg|{state}|{rate_key}|{host}|{interval['start_min']}-{interval['end_min']}"
                    )
                    if n <= 0:
                        continue
                    rng = np.random.default_rng(
                        stable_seed32(f"bg|{rate_key}|{interval['start_min']}|{host}|per_host")
                    )
                    times = schedule_evenly(start_ms, end_ms, n, rng, jitter_ms=500)
                    for i, ts in enumerate(times.tolist()):
                        bindings = {}
                        if comp_id == "edge_proxy" and log_id == "pop_health":
                            bindings["colo"] = get_colo_from_host(host)
                            rpm_center = pop_context["edge_req_rpm_per_pop"]
                            rpm = int(max(50, min(2000, rpm_center + int(rng.integers(-15, 16)))))
                            bindings["req_rpm"] = rpm
                            err = float(pop_context["edge_err_5xx_pct"]) + float(rng.uniform(-1.0, 1.0))
                            err = min(95.0, max(0.0, err))
                            bindings["err_5xx_pct"] = f"{err:.1f}"
                            busy = float(pop_context["edge_busy_pct"]) + float(rng.uniform(-2.0, 2.0))
                            busy = min(100.0, max(5.0, busy))
                            bindings["busy_pct"] = f"{busy:.1f}"
                        elif comp_id == "waf_engine" and log_id in ("cpu_sample", "regex_stats"):
                            bindings["colo"] = get_colo_from_host(host)
                            if log_id == "cpu_sample" and state == "f":
                                bindings["cpu_pct"] = f"{min(100.0, 96.0 + 4.0 * float(rng.random())):.1f}"
                                bindings["runq"] = int(rng.integers(80, 201))
                            if log_id == "regex_stats" and state == "f":
                                bindings["slow_evals"] = int(rng.integers(1500, 5001))
                                bindings["p95_eval_ms"] = int(rng.integers(250, 1201))
                        elif comp_id == "monitoring" and log_id in ("alert_cpu_global", "alert_502_rate"):
                            if log_id == "alert_cpu_global":
                                bindings["value_pct"] = f"{min(100.0, 92.0 + 8.0 * float(rng.random())):.1f}"
                                bindings["threshold_pct"] = f"{90.0 + 5.0 * float(rng.random()):.1f}"
                            else:
                                bindings["value_pct"] = f"{min(95.0, 55.0 + 40.0 * float(rng.random())):.1f}"
                                bindings["threshold_pct"] = f"{2.0 + 8.0 * float(rng.random()):.1f}"
                        lvl, msg = render_log_message(
                            comp_id, log_id, state, rng, f"{rate_key}|{host}|{ts}|{i}", bindings
                        )
                        rows.append((ts, lvl, msg, "", comp["svc"] or "", host or ""))


def emit_flows_for_interval(
    rows: List[Tuple[int, str, str, str, str, str]],
    interval: Dict[str, Any],
) -> Dict[str, float]:
    state = interval["state"]
    start_ms, end_ms = interval_epoch_bounds(interval)
    dur_min = interval["end_min"] - interval["start_min"]

    flow_defs = {f["id"]: f for f in SYSTEM["flows"][state]["req"]}
    per_flow_effective_rpm: Dict[str, float] = {}

    for flow_id, flow in flow_defs.items():
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f":
            mult = float(interval.get("rate_mult", {}).get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        per_flow_effective_rpm[flow_id] = eff_rpm
        if eff_rpm <= 0:
            continue

        expected = eff_rpm * dur_min
        n = det_round(expected, f"flow|{state}|{flow_id}|{interval['start_min']}-{interval['end_min']}")
        if n <= 0:
            continue

        rng = np.random.default_rng(stable_seed32(f"flow|{flow_id}|{state}|{interval['start_min']}"))
        starts = schedule_evenly(start_ms, end_ms, n, rng, jitter_ms=250)

        emit_ref = flow["emit"][0]
        comp_id, log_id = emit_ref.split(".", 1)
        comp = SYSTEM["components"][comp_id]

        p50, p95 = flow["latency_ms"][0]
        if state == "f":
            lm = interval.get("lat_mult", {}).get(flow_id, {"p50": 1.0, "p95": 1.0})
            p50 = float(p50) * float(lm.get("p50", 1.0))
            p95 = float(p95) * float(lm.get("p95", 1.0))

        durs = lognormal_from_p50_p95(rng, p50, p95, n).astype(np.int64)

        dur_dom = get_var_domain(comp_id, log_id, state, "dur_ms")
        if dur_dom is not None and dur_dom.get("k") == "i":
            lo, hi = int(dur_dom["v"][0]), int(dur_dom["v"][1])
            durs = np.clip(durs, lo, hi).astype(np.int64)

        ts = starts + durs

        edge_hosts = comp.get("hosts", []) if comp.get("hosts") else [""]
        eff_p95_for_err = float(p95)
        if dur_dom is not None and dur_dom.get("k") == "i":
            eff_p95_for_err = min(eff_p95_for_err, float(dur_dom["v"][1]))

        for i in range(n):
            emit_host = edge_hosts[i % len(edge_hosts)] if edge_hosts else ""
            colo = get_colo_from_host(emit_host) if emit_host else ""

            dur_i = int(durs[i])
            bindings: Dict[str, Any] = {"dur_ms": dur_i}
            if "colo" in SYSTEM["components"][comp_id]["logs"][log_id].get("vars", {}):
                bindings["colo"] = colo
            if log_id == "access_2xx":
                bindings["bytes"] = int(rng.integers(200, 20001))
            if log_id == "access_502":
                if dur_i >= int(eff_p95_for_err):
                    bindings["err"] = "waf_timeout"
                else:
                    bindings["err"] = pick_choice(rng, ["waf_timeout", "waf_cpu_overload"])

            bindings["ray"] = gen_hex(f"ray|{state}|{flow_id}|{interval['start_min']}|{i}|{int(ts[i])}", 16)

            lvl, msg = render_log_message(
                comp_id, log_id, state, rng, f"flow|{flow_id}|{int(ts[i])}|{i}", bindings
            )
            rows.append((int(ts[i]), lvl, msg, "", comp["svc"] or "", emit_host or ""))

    return per_flow_effective_rpm


def emit_one_shots(rows: List[Tuple[int, str, str, str, str, str]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ts = BASE_TIME + timedelta(minutes=at_min)
        base_ms = int(base_ts.timestamp() * 1000)
        for shot in ev.get("one_shots", []):
            ref = shot["ref"]
            count = int(shot["count"])
            hosts = list(shot.get("hosts", []))
            comp_id, log_id = ref.split(".", 1)
            comp = SYSTEM["components"][comp_id]
            rng = np.random.default_rng(stable_seed32(f"oneshot|{ref}|{at_min}"))

            for i in range(count):
                ts = base_ms + int(rng.integers(0, 10001))
                if hosts:
                    host = hosts[i % len(hosts)]
                else:
                    host = (comp.get("hosts") or [""])[0]
                bindings = {}
                lvl, msg = render_log_message(comp_id, log_id, "f", rng, f"oneshot|{ref}|{at_min}|{i}|{ts}", bindings)
                rows.append((ts, lvl, msg, "", comp["svc"] or "", host or ""))


# ----------------------------
# Run simulation
# ----------------------------

def main() -> None:
    random.seed(GLOBAL_SEED)
    np.random.seed(GLOBAL_SEED)

    intervals = build_all_intervals()
    rows: List[Tuple[int, str, str, str, str, str]] = []

    emit_one_shots(rows)

    for interval in intervals:
        state = interval["state"]
        eff_rpm = emit_flows_for_interval(rows, interval)

        if state == "n":
            total_rpm = eff_rpm.get("http_request_2xx", 0.0)
            rpm_502 = 0.0
        else:
            total_rpm = eff_rpm.get("http_request_2xx_degraded", 0.0) + eff_rpm.get("http_request_502", 0.0)
            rpm_502 = eff_rpm.get("http_request_502", 0.0)

        per_pop_rpm = total_rpm / 3.0 if total_rpm > 0 else 0.0
        err_pct = (100.0 * rpm_502 / total_rpm) if total_rpm > 0 else 0.0

        if state == "n":
            busy = 15.0 + 15.0 * stable_u01(f"busy|n|{interval['start_min']}")
            err_pct = min(err_pct, 2.0)
        else:
            busy = 88.0 + 12.0 * stable_u01(f"busy|f|{interval['start_min']}")
            err_pct = min(95.0, max(0.0, err_pct))

        pop_context = {
            "edge_req_rpm_per_pop": float(per_pop_rpm),
            "edge_err_5xx_pct": float(err_pct),
            "edge_busy_pct": float(busy),
        }

        emit_background_for_interval(rows, interval, pop_context)

    df = pd.DataFrame(rows, columns=["_ts_ms", "level", "message", "trace_id", "service", "host"])
    df.sort_values("_ts_ms", inplace=True, kind="mergesort")
    df["timestamp"] = df["_ts_ms"].astype(np.int64).apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    for col in ["timestamp", "level", "message", "trace_id", "service", "host"]:
        df[col] = df[col].fillna("").astype(str)

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
