import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "id": "honeycomb_slo_pipeline",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "ops_tool": {
            "svc": "ops-tool",
            "hosts": ["ops-1"],
            "logs": {
                "scale_retriever": {
                    "lvl": "INFO",
                    "msg": "scaled deployment=retriever replicas={replicas} by={user} result=ok",
                    "vars": {"replicas": {"k": "i", "v": [4, 24]}, "user": {"k": "ch", "v": ["oncall", "sre"]}},
                },
                "scale_beagle": {
                    "lvl": "INFO",
                    "msg": "scaled deployment=beagle replicas={replicas} by={user} result=ok",
                    "vars": {"replicas": {"k": "i", "v": [2, 16]}, "user": {"k": "ch", "v": ["oncall", "sre"]}},
                },
                "apply_slo_def_wrong": {
                    "lvl": "INFO",
                    "msg": "applied SLO definition app=beagle stream=retriever_mutation topic=honeycomb-prod.retriever_mutation shard_count={shards} mode=manual by={user}",
                    "vars": {"shards": {"k": "i", "v": [1, 64]}, "user": {"k": "ch", "v": ["oncall", "sre"]}},
                },
                "apply_slo_def_correct": {
                    "lvl": "INFO",
                    "msg": "applied SLO definition app=beagle stream=retriever_mutation topic=honeycomb-prod.retriever-mutation shard_count={shards} mode=manual by={user}",
                    "vars": {"shards": {"k": "i", "v": [1, 64]}, "user": {"k": "ch", "v": ["oncall", "sre"]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
        "retriever": {
            "svc": "retriever",
            "hosts": ["retriever-1", "retriever-2", "retriever-3", "retriever-4"],
            "logs": {
                "mutation_publish_ok": {
                    "lvl": "INFO",
                    "msg": "published mutation event topic={topic} partition={partition} offset={offset} bytes={bytes} dur_ms={dur_ms}",
                    "vars": {
                        "topic": {"k": "ch", "v": ["honeycomb-prod.retriever-mutation"]},
                        "partition": {"k": "i", "v": [0, 47]},
                        "offset": {"k": "i", "v": [100000, 5000000]},
                        "bytes": {"k": "i", "v": [200, 4000]},
                        "dur_ms": {"k": "i", "v": [2, 40]},
                    },
                },
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "healthcheck ok build={build} uptime_s={uptime_s}",
                    "vars": {"build": {"k": "ch", "v": ["r-1.42.0"]}, "uptime_s": {"k": "i", "v": [60, 200000]}},
                },
            },
            "beh": {
                "n": [{"id": "heartbeat", "per_min": 0.2}],  # scope omitted => per_host
                "f": [{"id": "heartbeat", "per_min": 0.2}],
            },
        },
        "kafka_cluster": {
            "svc": "kafka",
            "hosts": ["kafka-1", "kafka-2", "kafka-3"],
            "logs": {
                "broker_stats": {
                    "lvl": "INFO",
                    "msg": "broker={broker_id} net_out_mb_s={net_out} req_q={req_q}",
                    "vars": {
                        "broker_id": {"k": "i", "v": [1, 3]},
                        "net_out": {"k": "i", "v": [10, 200]},
                        "req_q": {"k": "i", "v": [0, 500]},
                    },
                },
                "controller_election": {
                    "lvl": "WARN",
                    "msg": "controller changed to broker={broker_id} reason={reason}",
                    "vars": {
                        "broker_id": {"k": "i", "v": [1, 3]},
                        "reason": {"k": "ch", "v": ["broker_restart", "maintenance", "leader_imbalance"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "broker_stats", "per_min": 0.5}, {"id": "controller_election", "per_min": 0.01, "scope": "global"}],
                "f": [{"id": "broker_stats", "per_min": 0.5}, {"id": "controller_election", "per_min": 0.01, "scope": "global"}],
            },
        },
        "slo_db": {
            "svc": "postgres",
            "hosts": ["pg-1"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete write_mb={write_mb} dur_ms={dur_ms}",
                    "vars": {"write_mb": {"k": "i", "v": [50, 2000]}, "dur_ms": {"k": "i", "v": [200, 15000]}},
                },
                "slow_query": {
                    "lvl": "WARN",
                    "msg": "slow query dur_ms={dur_ms} db={db}",
                    "vars": {"dur_ms": {"k": "i", "v": [500, 20000]}, "db": {"k": "ch", "v": ["beagle_slo"]}},
                },
            },
            "beh": {
                "n": [{"id": "checkpoint_complete", "per_min": 0.15, "scope": "global"}, {"id": "slow_query", "per_min": 0.01, "scope": "global"}],
                "f": [{"id": "checkpoint_complete", "per_min": 0.15, "scope": "global"}, {"id": "slow_query", "per_min": 0.01, "scope": "global"}],
            },
        },
        "beagle": {
            "svc": "beagle",
            "hosts": ["beagle-1", "beagle-2", "beagle-3", "beagle-4"],
            "logs": {
                "process_start": {
                    "lvl": "INFO",
                    "msg": "beagle starting build={build} group={group}",
                    "vars": {"build": {"k": "ch", "v": ["b-2.18.1"]}, "group": {"k": "ch", "v": ["beagle-slo"]}},
                },
                "consumer_rebalance": {
                    "lvl": "WARN",
                    "msg": "consumer group rebalance phase={phase} generation={gen} members={members}",
                    "vars": {"phase": {"k": "ch", "v": ["join", "sync", "stabilize"]}, "gen": {"k": "i", "v": [1, 500]}, "members": {"k": "i", "v": [1, 12]}},
                },
                "lag_metric": {
                    "lvl": "INFO",
                    "msg": "consumer lag topic={topic} p95_lag_s={lag_s} assigned_partitions={parts}",
                    "vars": {"topic": {"k": "ch", "v": ["honeycomb-prod.retriever-mutation"]}, "parts": {"k": "i", "v": [1, 48]}},
                    "state_vars": {"n": {"lag_s": {"k": "i", "v": [0, 30]}}, "f": {"lag_s": {"k": "i", "v": [120, 1800]}}},
                },
                "batch_fetch": {
                    "lvl": "INFO",
                    "msg": "fetched batch topic={topic} partitions={parts} max_bytes={max_bytes} fetch_ms={fetch_ms}",
                    "vars": {"topic": {"k": "ch", "v": ["honeycomb-prod.retriever-mutation"]}, "parts": {"k": "i", "v": [1, 48]}, "max_bytes": {"k": "i", "v": [1048576, 16777216]}, "fetch_ms": {"k": "i", "v": [5, 200]}},
                },
                "batch_processed": {
                    "lvl": "INFO",
                    "msg": "processed batch topic={topic} msgs={msgs} lag_s={lag_s} proc_ms={proc_ms}",
                    "vars": {"topic": {"k": "ch", "v": ["honeycomb-prod.retriever-mutation"]}},
                    "state_vars": {
                        "n": {"msgs": {"k": "i", "v": [10, 40]}, "lag_s": {"k": "i", "v": [0, 30]}, "proc_ms": {"k": "i", "v": [20, 180]}},
                        "f": {"msgs": {"k": "i", "v": [60, 140]}, "lag_s": {"k": "i", "v": [120, 1800]}, "proc_ms": {"k": "i", "v": [120, 1200]}},
                    },
                },
                "slo_write_ok": {"lvl": "INFO", "msg": "wrote SLO points service={service} points={points} write_ms={write_ms}", "vars": {"service": {"k": "ch", "v": ["retriever_mutation"]}, "points": {"k": "i", "v": [100, 20000]}, "write_ms": {"k": "i", "v": [5, 200]}}},
                "consumer_error_unknown_topic": {"lvl": "ERROR", "msg": "kafka consume error topic={topic} err={err}", "vars": {"topic": {"k": "ch", "v": ["honeycomb-prod.retriever_mutation"]}, "err": {"k": "ch", "v": ["UnknownTopicOrPartition"]}}},
                "panic_exit": {"lvl": "CRITICAL", "msg": "exiting due to unrecoverable error reason={reason}", "vars": {"reason": {"k": "ch", "v": ["consumer_start_failed", "panic"]}}},
            },
            "beh": {
                "n": [
                    {"id": "process_start", "per_min": 0.06, "scope": "global"},
                    {"id": "consumer_rebalance", "per_min": 0.05},  # per_host default
                    {"id": "lag_metric", "per_min": 1.0},  # per_host default
                ],
                "f": [
                    {"id": "process_start", "per_min": 7.5, "scope": "global"},
                    {"id": "consumer_rebalance", "per_min": 0.1},
                    {"id": "lag_metric", "per_min": 1.0},
                ],
            },
        },
        "alerting": {
            "svc": "alertmanager",
            "hosts": ["alert-1"],
            "logs": {
                "notifier_ok": {"lvl": "INFO", "msg": "notifier ok integrations={integrations}", "vars": {"integrations": {"k": "ch", "v": ["pagerduty", "slack"]}}},
                "alert_fired": {
                    "lvl": "WARN",
                    "msg": "alert {name} severity={sev} value={value} threshold={thresh} for={for_min}m",
                    "vars": {"name": {"k": "ch", "v": ["beagle_slo_lag_high", "beagle_crashlooping"]}, "sev": {"k": "ch", "v": ["page", "ticket"]}, "value": {"k": "i", "v": [1, 2000]}, "thresh": {"k": "i", "v": [60, 300]}, "for_min": {"k": "i", "v": [1, 15]}},
                },
            },
            "beh": {
                "n": [{"id": "notifier_ok", "per_min": 0.3, "scope": "global"}],
                "f": [{"id": "notifier_ok", "per_min": 0.3, "scope": "global"}, {"id": "alert_fired", "per_min": 0.3, "scope": "global"}],
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "publish_retriever_mutation",
                "rpm": 2000,
                "emit": ["retriever.mutation_publish_ok"],
                "latency_ms": [[5, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "beagle_process_slo_batch",
                "rpm": 80,
                "emit": ["beagle.batch_fetch", "beagle.batch_processed", "beagle.slo_write_ok"],
                "latency_ms": [[15, 60], [40, 150], [10, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "publish_retriever_mutation",
                "rpm": 2000,
                "emit": ["retriever.mutation_publish_ok"],
                "latency_ms": [[5, 25]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "beagle_consume_unknown_topic",
                "rpm": 30,
                "emit": ["beagle.consumer_error_unknown_topic", "beagle.panic_exit"],
                "latency_ms": [[5, 30], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "beagle_process_slo_batch",
                "rpm": 30,
                "emit": ["beagle.batch_fetch", "beagle.batch_processed", "beagle.slo_write_ok"],
                "latency_ms": [[20, 80], [160, 1100], [20, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "beagle_slo_crashloop_due_to_topic_typo",
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 18,
                    "rate_multipliers": {
                        "beagle_process_slo_batch": 0.0,
                        "beagle_consume_unknown_topic": 1.0,
                        "beagle.process_start": 4.0,
                        "beagle.lag_metric": 0.0,
                        "alerting.alert_fired": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_tool.scale_retriever", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "ops_tool.apply_slo_def_wrong", "count": 1, "hosts": ["ops-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 26,
                    "rate_multipliers": {
                        "beagle_consume_unknown_topic": 2.0,
                        "beagle.process_start": 8.0,
                        "beagle.consumer_rebalance": 6.0,
                        "beagle.lag_metric": 0.0,
                        "alerting.alert_fired": 1.5,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "ops_tool.scale_beagle", "count": 1, "hosts": ["ops-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 34,
                    "rate_multipliers": {
                        "beagle_consume_unknown_topic": 0.0,
                        "beagle_process_slo_batch": 1.0,
                        "beagle.process_start": 0.1,
                        "beagle.consumer_rebalance": 2.0,
                        "beagle.lag_metric": 1.0,
                        "alerting.alert_fired": 1.0,
                    },
                    "latency_multipliers": {"beagle_process_slo_batch": {"p50": 1.1, "p95": 1.2}},
                    "one_shots": [{"ref": "ops_tool.apply_slo_def_correct", "count": 1, "hosts": ["ops-1"]}],
                },
            ]
        }
    },
}

# -----------------------------
# Deterministic helpers
# -----------------------------
SEED = "incident-model-v3:honeycomb_slo_pipeline:beagle_topic_typo"

_seed_int = int(hashlib.md5(SEED.encode("utf-8")).hexdigest()[:8], 16)
random.seed(_seed_int)
np.random.seed(_seed_int)


def _md5_bytes(s: str) -> bytes:
    return hashlib.md5((SEED + "|" + s).encode("utf-8")).digest()


def u01(key: str) -> float:
    b = _md5_bytes(key)
    x = int.from_bytes(b[:8], byteorder="big", signed=False)
    return (x % (1 << 53)) / float(1 << 53)


def stable_int(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    return base + (1 if u01(f"stable_int:{key}") < frac else 0)


def choose_from(domain: Dict[str, Any], key: str) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "ch":
        idx = int(u01(f"ch:{key}") * len(v)) if v else 0
        idx = max(0, min(idx, len(v) - 1))
        return v[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return lo + int(u01(f"i:{key}") * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if hi <= lo:
            return lo
        return lo + u01(f"f:{key}") * (hi - lo)
    if k == "hex":
        ln = int(v)
        hx = hashlib.md5((SEED + "|hex|" + key).encode("utf-8")).hexdigest()
        while len(hx) < ln:
            hx += hashlib.md5((hx + key).encode("utf-8")).hexdigest()
        return hx[:ln]
    if k == "uuid":
        hx = hashlib.md5((SEED + "|uuid|" + key).encode("utf-8")).hexdigest()
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "str":
        return str(v)
    if k == "ip":
        return "127.0.0.1"
    return str(v)


_Z95 = NormalDist().inv_cdf(0.95)


def sample_lognormal_ms(p50: float, p95: float, key: str, hard_min: Optional[float] = None, hard_max: Optional[float] = None) -> int:
    """Skewed positive sampler, optionally hard-bounded. Used to schedule delays; if bounded, the returned
    value is also safe to embed directly into message timing fields."""
    p50 = max(1e-3, float(p50))
    p95 = max(p50, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / _Z95 if p95 > p50 else 0.0

    u_raw = u01(f"lat_u:{key}")
    u = 0.5 + 0.5 * (u_raw - 0.5) * 0.6
    u = min(0.999999, max(1e-6, u))
    z = NormalDist().inv_cdf(u)
    x = math.exp(mu + sigma * z) if sigma > 0 else p50

    cap = 3.0 * p95
    if hard_max is not None:
        cap = min(cap, float(hard_max))
    if x > cap:
        x = cap

    if hard_min is not None:
        x = max(float(hard_min), x)

    return max(1, int(round(x)))


def iso_utc_from_base(base_dt: datetime, ms_from_base: int) -> str:
    dt = base_dt + timedelta(milliseconds=int(ms_from_base))
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def even_schedule_ms(start_min: int, end_min: int, count: int, key: str, jitter_max_s: float = 0.2) -> List[int]:
    if count <= 0:
        return []
    start_s = start_min * 60.0
    end_s = end_min * 60.0
    dur_s = max(0.0, end_s - start_s)
    if dur_s <= 0:
        return []
    step = dur_s / float(count)
    jitter = min(jitter_max_s, step * 0.3, 0.015)
    times: List[int] = []
    for i in range(count):
        t = start_s + (i + 0.5) * step
        j = (u01(f"jitter:{key}:{i}") - 0.5) * 2.0 * jitter
        t2 = min(end_s - 1e-6, max(start_s, t + j))
        times.append(int(round(t2 * 1000.0)))
    return times


def parse_ref(ref: str) -> Tuple[str, str]:
    c, l = ref.split(".", 1)
    return c, l


def get_log_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]["logs"][log_id]


def get_int_var_bounds(component_id: str, log_id: str, state: str, var_name: str) -> Optional[Tuple[int, int]]:
    tmpl = get_log_template(component_id, log_id)
    dom = None
    if "vars" in tmpl and var_name in tmpl["vars"]:
        dom = tmpl["vars"][var_name]
    st = tmpl.get("state_vars") or {}
    if state in st and var_name in st[state]:
        dom = st[state][var_name]
    if not dom or dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    if hi < lo:
        hi = lo
    return lo, hi


def choose_host(component_id: str, key: str, allowed_hosts: Optional[List[str]] = None) -> str:
    hosts = SYSTEM["components"][component_id].get("hosts") or []
    if allowed_hosts is not None:
        hosts = [h for h in hosts if h in allowed_hosts]
    if not hosts:
        return ""
    idx = int(u01(f"host:{component_id}:{key}") * len(hosts))
    idx = max(0, min(idx, len(hosts) - 1))
    return hosts[idx]


def render_message(component_id: str, log_id: str, state: str, key: str, bound: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    tmpl = get_log_template(component_id, log_id)
    msg_t = tmpl["msg"]
    vars_dom: Dict[str, Dict[str, Any]] = dict(tmpl.get("vars") or {})
    state_vars = tmpl.get("state_vars") or {}
    if state in state_vars:
        vars_dom.update(state_vars[state])

    values: Dict[str, Any] = {}
    if bound:
        values.update(bound)

    for var, dom in vars_dom.items():
        if var in values:
            continue
        values[var] = choose_from(dom, f"{component_id}.{log_id}.{var}:{key}")

    return tmpl["lvl"], msg_t.format(**values)


# -----------------------------
# Failure interval controls
# -----------------------------
@dataclass(frozen=True)
class IntervalCtl:
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50,p95}


def build_failure_intervals() -> List[IntervalCtl]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    flow_ids_f = {f["id"] for f in SYSTEM["flows"]["f"]}

    current_flow_rate: Dict[str, float] = {}
    current_bg_rate: Dict[str, float] = {}
    current_lat: Dict[str, Dict[str, float]] = {}

    boundaries = [fstart] + [e["at_min"] for e in events if fstart <= e["at_min"] < fend] + [fend]
    boundaries = sorted(set(boundaries))

    intervals: List[IntervalCtl] = []
    for i in range(len(boundaries) - 1):
        b = boundaries[i]
        nb = boundaries[i + 1]
        for e in events:
            if e["at_min"] == b:
                for k, v in (e.get("rate_multipliers") or {}).items():
                    if k in flow_ids_f:
                        current_flow_rate[k] = float(v)
                    else:
                        current_bg_rate[k] = float(v)
                for fk, mult in (e.get("latency_multipliers") or {}).items():
                    current_lat[fk] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}
        intervals.append(
            IntervalCtl(
                start_min=b,
                end_min=nb,
                flow_rate_mult=dict(current_flow_rate),
                bg_rate_mult=dict(current_bg_rate),
                flow_latency_mult=dict(current_lat),
            )
        )
    return intervals


FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Simulation
# -----------------------------
BASE_DT = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def emit_background(state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]], rows: List[Tuple[int, str, str, str, str, str]]) -> None:
    duration_min = max(0, end_min - start_min)
    if duration_min <= 0:
        return
    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult = 1.0
            if state == "f" and bg_mult is not None:
                mult = float(bg_mult.get(f"{comp_id}.{log_id}", 1.0))
            eff_rate = per_min * mult
            if eff_rate <= 0:
                continue

            svc = comp.get("svc") or ""
            hosts = comp.get("hosts") or []

            if scope == "global":
                expected = eff_rate * duration_min
                cnt = stable_int(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                ts_list = even_schedule_ms(start_min, end_min, cnt, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                for j, ts in enumerate(ts_list):
                    host = choose_host(comp_id, f"{start_min}-{end_min}:{log_id}:g:{j}")
                    lvl, msg = render_message(comp_id, log_id, state, key=f"{start_min}-{end_min}:g:{j}")
                    rows.append((ts, lvl, msg, "", svc, host))
            else:
                if not hosts:
                    expected = eff_rate * duration_min
                    cnt = stable_int(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:nohost")
                    ts_list = even_schedule_ms(start_min, end_min, cnt, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:nohost")
                    for j, ts in enumerate(ts_list):
                        lvl, msg = render_message(comp_id, log_id, state, key=f"{start_min}-{end_min}:nh:{j}")
                        rows.append((ts, lvl, msg, "", svc, ""))
                else:
                    for h in hosts:
                        expected = eff_rate * duration_min
                        cnt = stable_int(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{h}")
                        ts_list = even_schedule_ms(start_min, end_min, cnt, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{h}")
                        for j, ts in enumerate(ts_list):
                            lvl, msg = render_message(comp_id, log_id, state, key=f"{start_min}-{end_min}:{h}:{j}")
                            rows.append((ts, lvl, msg, "", svc, h))


def simulate_flow_instances(
    state: str,
    start_min: int,
    end_min: int,
    flow_def: Dict[str, Any],
    rate_mult: float,
    latency_mult: Optional[Dict[str, float]],
    rows: List[Tuple[int, str, str, str, str, str]],
) -> None:
    duration_min = max(0, end_min - start_min)
    if duration_min <= 0:
        return

    flow_id = flow_def["id"]
    rpm = float(flow_def["rpm"])
    eff_rpm = rpm * float(rate_mult)
    if eff_rpm <= 0:
        return

    expected_instances = eff_rpm * duration_min
    n_inst = stable_int(expected_instances, f"flow:{state}:{start_min}-{end_min}:{flow_id}")
    start_times = even_schedule_ms(start_min, end_min, n_inst, f"flow_start:{state}:{start_min}-{end_min}:{flow_id}", jitter_max_s=0.05)

    emit_refs: List[str] = flow_def["emit"]
    latency_pairs: List[List[float]] = flow_def.get("latency_ms") or []

    p50_mult = float(latency_mult.get("p50", 1.0)) if latency_mult else 1.0
    p95_mult = float(latency_mult.get("p95", 1.0)) if latency_mult else 1.0

    # Map logs that carry explicit timing fields, so we can hard-bound sampled deltas to template domains.
    timing_var_by_log: Dict[Tuple[str, str], str] = {
        ("retriever", "mutation_publish_ok"): "dur_ms",
        ("beagle", "batch_fetch"): "fetch_ms",
        ("beagle", "batch_processed"): "proc_ms",
        ("beagle", "slo_write_ok"): "write_ms",
    }

    for i, start_ms in enumerate(start_times):
        comp_host: Dict[str, str] = {}
        chain_key = f"{state}:{flow_id}:{start_min}-{end_min}:inst:{i}"
        bound_common: Dict[str, Any] = {}

        if flow_id == "beagle_process_slo_batch":
            bound_common["topic"] = "honeycomb-prod.retriever-mutation"

        deltas_ms: List[int] = []
        for j, pair in enumerate(latency_pairs):
            p50, p95 = float(pair[0]) * p50_mult, float(pair[1]) * p95_mult

            hard_min = None
            hard_max = None
            if j < len(emit_refs):
                c_id, l_id = parse_ref(emit_refs[j])
                tv = timing_var_by_log.get((c_id, l_id))
                if tv is not None:
                    bounds = get_int_var_bounds(c_id, l_id, state, tv)
                    if bounds is not None:
                        hard_min, hard_max = bounds[0], bounds[1]

            deltas_ms.append(sample_lognormal_ms(p50, p95, key=f"{chain_key}:lat:{j}", hard_min=hard_min, hard_max=hard_max))

        current_ts = start_ms
        for j, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            if comp_id not in comp_host:
                comp_host[comp_id] = choose_host(comp_id, key=f"{chain_key}:comp:{comp_id}")

            delta = deltas_ms[j] if j < len(deltas_ms) else 1
            current_ts += int(delta)

            bound = dict(bound_common)

            tv = timing_var_by_log.get((comp_id, log_id))
            if tv is not None:
                # delta is already bounded to the correct domain if tv exists.
                bound[tv] = int(delta)

            lvl, msg = render_message(comp_id, log_id, state, key=f"{chain_key}:emit:{j}", bound=bound)
            svc = SYSTEM["components"][comp_id].get("svc") or ""
            rows.append((current_ts, lvl, msg, "", svc, comp_host.get(comp_id, "")))


def emit_one_shots(rows: List[Tuple[int, str, str, str, str, str]]) -> None:
    f_events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in f_events:
        at_min = int(e["at_min"])
        base_ms = at_min * 60 * 1000
        shots = e.get("one_shots") or []
        for s_idx, s in enumerate(shots):
            ref = s["ref"]
            count = int(s["count"])
            allowed_hosts = s.get("hosts")
            comp_id, log_id = parse_ref(ref)
            svc = SYSTEM["components"][comp_id].get("svc") or ""
            for k in range(count):
                j = int(u01(f"oneshot:{at_min}:{ref}:{s_idx}:{k}") * 500.0)
                ts = base_ms + j + k
                host = choose_host(comp_id, key=f"oneshot:{at_min}:{ref}:{s_idx}:{k}", allowed_hosts=allowed_hosts)
                lvl, msg = render_message(comp_id, log_id, state="f", key=f"oneshot:{at_min}:{ref}:{s_idx}:{k}")
                rows.append((ts, lvl, msg, "", svc, host))


def main() -> None:
    rows: List[Tuple[int, str, str, str, str, str]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    emit_background("n", n_start, n_end, bg_mult=None, rows=rows)
    for flow in SYSTEM["flows"]["n"]:
        simulate_flow_instances("n", n_start, n_end, flow, rate_mult=1.0, latency_mult=None, rows=rows)

    for interval in FAILURE_INTERVALS:
        emit_background("f", interval.start_min, interval.end_min, bg_mult=interval.bg_rate_mult, rows=rows)
        for flow in SYSTEM["flows"]["f"]:
            flow_id = flow["id"]
            rmult = float(interval.flow_rate_mult.get(flow_id, 1.0))
            lmult = interval.flow_latency_mult.get(flow_id)
            simulate_flow_instances("f", interval.start_min, interval.end_min, flow, rate_mult=rmult, latency_mult=lmult, rows=rows)

    emit_one_shots(rows)

    rows.sort(key=lambda r: r[0])
    df = pd.DataFrame(
        {
            "timestamp": [iso_utc_from_base(BASE_DT, r[0]) for r in rows],
            "level": [r[1] for r in rows],
            "message": [r[2] for r in rows],
            "trace_id": [r[3] for r in rows],
            "service": [r[4] for r in rows],
            "host": [r[5] for r in rows],
        },
        columns=["timestamp", "level", "message", "trace_id", "service", "host"],
    )

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count out of target range: {len(df)}")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
