import math
import random
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "arpanet_imp_routing"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["tip_access", "ncc"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "tip_access": {
            "svc": "tip",
            "hosts": ["tip01", "tip07"],
            "logs": {
                "conn_request": {
                    "lvl": "INFO",
                    "msg": "open connection user={user} dst_host={dst_host} local_imp={local_imp}",
                    "vars": {
                        "user": {"k": "ch", "v": ["alice", "bob", "carol", "dave", "erin", "frank"]},
                        "dst_host": {"k": "str", "v": "remote-hostname"},
                        "local_imp": {"k": "ch", "v": ["imp12", "imp17", "imp33"]},
                    },
                },
                "conn_ok": {
                    "lvl": "INFO",
                    "msg": "connection established dst_host={dst_host} dst_ip={dst_ip}",
                    "vars": {"dst_host": {"k": "str", "v": "remote-hostname"}, "dst_ip": {"k": "ip", "v": None}},
                },
                "net_trouble": {
                    "lvl": "ERROR",
                    "msg": "net trouble: no physical path dst_host={dst_host} local_imp={local_imp}",
                    "vars": {"dst_host": {"k": "str", "v": "remote-hostname"}, "local_imp": {"k": "ch", "v": ["imp12", "imp17", "imp33"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "imp_node": {
            "svc": "imp",
            "hosts": ["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"],
            "logs": {
                "ru_rx_normal": {
                    "lvl": "INFO",
                    "msg": "routing update rx origin={origin_imp} seq={seq} from={from_imp} bytes={bytes}",
                    "vars": {
                        "origin_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"]},
                        "seq": {"k": "i", "v": [0, 63]},
                        "from_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41"]},
                        "bytes": {"k": "i", "v": [48, 96]},
                    },
                },
                "ru_rx_cycle": {
                    "lvl": "INFO",
                    "msg": "routing update rx origin={origin_imp} seq={seq} from={from_imp} bytes={bytes}",
                    "vars": {
                        "origin_imp": {"k": "ch", "v": ["imp50"]},
                        "seq": {"k": "ch", "v": [8, 40, 44]},
                        "from_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41"]},
                        "bytes": {"k": "i", "v": [48, 96]},
                    },
                },
                "ru_accept": {
                    "lvl": "INFO",
                    "msg": "routing update accepted origin={origin_imp} seq={seq} prev_seq={prev_seq}",
                    "vars": {
                        "origin_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"]},
                        "seq": {"k": "i", "v": [0, 63]},
                        "prev_seq": {"k": "i", "v": [0, 63]},
                    },
                },
                "ru_fwd": {
                    "lvl": "INFO",
                    "msg": "routing update fwd origin={origin_imp} seq={seq} to={to_imp} kind={tx_kind}",
                    "vars": {
                        "origin_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"]},
                        "seq": {"k": "i", "v": [0, 63]},
                        "to_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41"]},
                    },
                    "state_vars": {"n": {"tx_kind": {"k": "ch", "v": ["initial"]}}, "f": {"tx_kind": {"k": "ch", "v": ["initial", "retransmit"]}}},
                },
                "ru_enqueue": {
                    "lvl": "WARN",
                    "msg": "routing update queued origin={origin_imp} seq={seq} q_depth={q_depth}",
                    "vars": {"origin_imp": {"k": "ch", "v": ["imp50"]}, "seq": {"k": "ch", "v": [8, 40, 44]}, "q_depth": {"k": "i", "v": [200, 8000]}},
                },
                "ru_qdrop": {
                    "lvl": "ERROR",
                    "msg": "routing update dropped origin={origin_imp} seq={seq} q_depth={q_depth}",
                    "vars": {"origin_imp": {"k": "ch", "v": ["imp50"]}, "seq": {"k": "ch", "v": [8, 40, 44]}, "q_depth": {"k": "i", "v": [2000, 12000]}},
                },
                "cpu_stat": {"lvl": "INFO", "msg": "cpu load cpu_pct={cpu_pct} routing_pct={routing_pct}", "vars": {"cpu_pct": {"k": "i", "v": [5, 99]}, "routing_pct": {"k": "i", "v": [1, 98]}}},
                "cpu_warn": {"lvl": "WARN", "msg": "cpu high cpu_pct={cpu_pct} routing_pct={routing_pct}", "vars": {"cpu_pct": {"k": "i", "v": [85, 100]}, "routing_pct": {"k": "i", "v": [70, 100]}}},
                "buf_stat": {"lvl": "INFO", "msg": "buffers free={buf_free} total={buf_total} waiters={buf_waiters}", "vars": {"buf_free": {"k": "i", "v": [0, 4096]}, "buf_total": {"k": "i", "v": [4096, 4096]}, "buf_waiters": {"k": "i", "v": [0, 900]}}},
                "buffer_low": {"lvl": "ERROR", "msg": "buffer starvation free={buf_free} waiters={buf_waiters} symptom={symptom}", "vars": {"buf_free": {"k": "i", "v": [0, 80]}, "buf_waiters": {"k": "i", "v": [50, 1500]}, "symptom": {"k": "ch", "v": ["rx_no_buffer", "hello_drop", "update_queue_full"]}}},
                "line_state": {"lvl": "WARN", "msg": "line {neighbor_imp} state={state} reason={reason}", "vars": {"neighbor_imp": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"]}, "state": {"k": "ch", "v": ["up", "down"]}, "reason": {"k": "ch", "v": ["hello_timeout", "quality_low", "admin_reset"]}}},
                "session_route_ok": {"lvl": "INFO", "msg": "route ok dst_host={dst_host} next_hop={next_hop} rtt_ms={rtt_ms}", "vars": {"dst_host": {"k": "str", "v": "remote-hostname"}, "next_hop": {"k": "ch", "v": ["imp12", "imp17", "imp29", "imp33", "imp41"]}, "rtt_ms": {"k": "i", "v": [20, 220]}}},
                "session_route_fail": {"lvl": "ERROR", "msg": "route fail dst_host={dst_host} cause={cause}", "vars": {"dst_host": {"k": "str", "v": "remote-hostname"}, "cause": {"k": "ch", "v": ["no_physical_path", "routing_busy", "line_down"]}}},
                "session_reset": {"lvl": "WARN", "msg": "session reset peer={peer} reason={reason}", "vars": {"peer": {"k": "str", "v": "peer-id"}, "reason": {"k": "ch", "v": ["line_flap", "route_change", "timeout"]}}},
                "restart_complete": {"lvl": "INFO", "msg": "restart complete dump_taken={dump_taken} uptime_s={uptime_s}", "vars": {"dump_taken": {"k": "ch", "v": ["yes"]}, "uptime_s": {"k": "i", "v": [5, 120]}}},
                "patch_applied": {"lvl": "INFO", "msg": "patch applied ignore_origin={ignore_origin} patch_id={patch_id}", "vars": {"ignore_origin": {"k": "ch", "v": ["imp50"]}, "patch_id": {"k": "ch", "v": ["temp_ignore_origin"]}}},
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "cpu_stat", "per_min": 1.0},
                        {"id": "buf_stat", "per_min": 1.0},
                        {"id": "cpu_warn", "per_min": 0.05},
                        {"id": "buffer_low", "per_min": 0.01},
                        {"id": "line_state", "per_min": 0.08},
                        {"id": "session_reset", "per_min": 0.03},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "cpu_stat", "per_min": 1.0},
                        {"id": "buf_stat", "per_min": 1.0},
                        {"id": "cpu_warn", "per_min": 0.8},
                        {"id": "buffer_low", "per_min": 0.5},
                        {"id": "line_state", "per_min": 0.2},
                        {"id": "session_reset", "per_min": 0.2},
                    ]
                },
            },
        },
        "ncc": {
            "svc": "ncc",
            "hosts": ["ncc01"],
            "logs": {
                "restart_cmd": {"lvl": "WARN", "msg": "restart issued imp={imp_id} reason={reason}", "vars": {"imp_id": {"k": "ch", "v": ["imp12", "imp17", "imp33", "imp29"]}, "reason": {"k": "ch", "v": ["routing_storm", "diagnostics"]}}},
                "patch_broadcast_start": {"lvl": "WARN", "msg": "patch broadcast started ignore_origin={ignore_origin}", "vars": {"ignore_origin": {"k": "ch", "v": ["imp50"]}}},
                "patch_ack": {"lvl": "INFO", "msg": "patch ack imp={imp_id} status={status} lag_s={lag_s}", "vars": {"imp_id": {"k": "ch", "v": ["imp12", "imp17", "imp33", "imp29", "imp41"]}, "status": {"k": "ch", "v": ["applied", "pending", "unreachable"]}, "lag_s": {"k": "i", "v": [30, 14400]}}},
            },
            "beh": {"n": {"emit": [{"id": "patch_ack", "per_min": 0.0, "scope": "global"}]}, "f": {"emit": [{"id": "patch_ack", "per_min": 0.7, "scope": "global"}]}},
        },
    },
    "flows": {
        "n": [
            {
                "id": "tip_connect_success",
                "rpm": 80.0,
                "emit": ["tip_access.conn_request", "imp_node.session_route_ok", "tip_access.conn_ok"],
                "latency_ms": [[8, 25], [5, 18], [10, 35]],
                "trace": True,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
            {
                "id": "routing_update_processed_n",
                "rpm": 200.0,
                "emit": ["imp_node.ru_rx_normal", "imp_node.ru_accept", "imp_node.ru_fwd"],
                "latency_ms": [[1, 4], [1, 6], [1, 5]],
                "trace": False,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
        ],
        "f": [
            {
                "id": "tip_connect_fail_net_trouble",
                "rpm": 75.0,
                "emit": ["tip_access.conn_request", "imp_node.session_route_fail", "tip_access.net_trouble"],
                "latency_ms": [[10, 35], [50, 250], [5, 20]],
                "trace": True,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
            {
                "id": "tip_connect_success_rare",
                "rpm": 2.0,
                "emit": ["tip_access.conn_request", "imp_node.session_route_ok", "tip_access.conn_ok"],
                "latency_ms": [[10, 40], [40, 220], [10, 45]],
                "trace": True,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
            {
                "id": "routing_update_backlog",
                "rpm": 600.0,
                "emit": ["imp_node.ru_rx_cycle", "imp_node.ru_enqueue"],
                "latency_ms": [[1, 5], [5, 40]],
                "trace": False,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
            {
                "id": "routing_update_processed_f",
                "rpm": 150.0,
                "emit": ["imp_node.ru_rx_cycle", "imp_node.ru_accept", "imp_node.ru_fwd"],
                "latency_ms": [[1, 6], [5, 60], [1, 8]],
                "trace": False,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
            {
                "id": "routing_update_drop",
                "rpm": 50.0,
                "emit": ["imp_node.ru_rx_cycle", "imp_node.ru_qdrop"],
                "latency_ms": [[1, 6], [1, 10]],
                "trace": False,
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "arpanet_1980_routing_update_cycle"},
    "time": {"total_minutes": 30, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 30}}},
    "failure_events": [
        {
            "order": 1,
            "at_min": 15,
            "rate_multipliers": {
                "routing_update_backlog": 2.0,
                "routing_update_processed_f": 1.5,
                "routing_update_drop": 2.0,
                "imp_node.cpu_warn": 2.0,
                "imp_node.buffer_low": 1.2,
                "imp_node.line_state": 0.5,
                "imp_node.session_reset": 0.5,
                "ncc.patch_ack": 0.0,
            },
            "latency_multipliers": {"tip_connect_fail_net_trouble": {"p50": 1.2, "p95": 1.4}},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 20,
            "rate_multipliers": {"imp_node.line_state": 4.0, "imp_node.session_reset": 3.0, "imp_node.buffer_low": 3.0},
            "latency_multipliers": {"routing_update_backlog": {"p50": 1.3, "p95": 2.0}},
            "one_shots": [
                {"ref": "ncc.restart_cmd", "count": 3, "hosts": ["ncc01"]},
                {"ref": "imp_node.restart_complete", "count": 3, "hosts": ["imp12", "imp17", "imp33"]},
            ],
        },
        {
            "order": 3,
            "at_min": 27,
            "rate_multipliers": {
                "routing_update_backlog": 1.0,
                "routing_update_processed_f": 0.8,
                "routing_update_drop": 0.8,
                "imp_node.cpu_warn": 1.2,
                "imp_node.buffer_low": 2.0,
                "imp_node.line_state": 2.5,
                "imp_node.session_reset": 2.0,
                "ncc.patch_ack": 1.0,
            },
            "latency_multipliers": {"tip_connect_fail_net_trouble": {"p50": 1.1, "p95": 1.2}},
            "one_shots": [
                {"ref": "ncc.patch_broadcast_start", "count": 1, "hosts": ["ncc01"]},
                {"ref": "imp_node.patch_applied", "count": 2, "hosts": ["imp12", "imp17"]},
            ],
        },
    ],
}

# -----------------------------
# Deterministic helpers
# -----------------------------

SEED = 1337
random.seed(SEED)  # verifier expects this call
np.random.seed(SEED)  # keep legacy/global numpy deterministic if ever used
rng = np.random.default_rng(SEED)

BASE_TIME = datetime(1980, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
NORM = NormalDist()


def iso_utc_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def crc32_u32(s: str) -> int:
    return zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF


def stable_u01(seed_u32: int, i: int) -> float:
    x = (seed_u32 ^ ((i + 1) * 0x9E3779B1)) & 0xFFFFFFFF
    x = (1103515245 * x + 12345) & 0xFFFFFFFF
    return x / 2**32


def stable_jitter_seconds(key: str, i: int, max_jitter_s: float) -> float:
    u = stable_u01(crc32_u32(key), i)
    return (u - 0.5) * 2.0 * max_jitter_s


def carry_round(expected: float, carry: float) -> Tuple[int, float]:
    carry += expected
    n = int(math.floor(carry + 1e-12))
    carry -= n
    return n, carry


def sample_lognormal_ms(p50_ms: float, p95_ms: float, u: float, soft_cap_ms: float) -> float:
    # Quantile-based mapping with a lognormal calibrated by p50/p95.
    # Use statistics.NormalDist().inv_cdf to avoid non-standard numpy special functions.
    p50 = max(0.1, float(p50_ms))
    p95 = max(p50 * 1.001, float(p95_ms))
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    u = min(1.0 - 1e-9, max(1e-9, u))
    z = NORM.inv_cdf(u)
    x = math.exp(mu + sigma * z)
    return float(min(x, soft_cap_ms))


def gen_hex(n: int, local_rng: np.random.Generator) -> str:
    b = local_rng.integers(0, 256, size=(n // 2), dtype=np.uint8).tobytes()
    return b.hex()[:n]


def gen_ip(i: int, salt: int = 0) -> str:
    a = 10
    b = (salt + (i // 256)) % 256
    c = (salt + i) % 256
    d = (salt + 31 + (i * 7)) % 256
    return f"{a}.{b}.{c}.{d}"


def gen_str(hint: str, i: int, salt: int = 0) -> str:
    if hint == "remote-hostname":
        n = (i + 1000 + salt) % 50000
        return f"host{n}.arpa"
    if hint == "peer-id":
        n = (i + 2000 + salt) % 100000
        return f"peer{n}"
    n = (i + 3000 + salt) % 100000
    return f"{hint}-{n}"


def choose_from_list(vals: List[Any], u: float) -> Any:
    if not vals:
        return ""
    idx = int(math.floor(u * len(vals)))
    if idx >= len(vals):
        idx = len(vals) - 1
    return vals[idx]


# -----------------------------
# Model indices
# -----------------------------

def parse_ref(ref: str) -> Tuple[str, str]:
    if "." not in ref:
        raise ValueError(f"bad ref: {ref}")
    c, l = ref.split(".", 1)
    return c, l


COMP = SYSTEM["components"]

LOG_TPL: Dict[Tuple[str, str], Dict[str, Any]] = {}
for cid, c in COMP.items():
    for lid, t in c["logs"].items():
        LOG_TPL[(cid, lid)] = t

# -----------------------------
# Failure controls: piecewise intervals + one-shots
# -----------------------------

@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[Tuple[str, str], float]
    flow_latency_mult: Dict[str, Tuple[float, float]]  # flow_id -> (p50_mult, p95_mult)


def build_failure_intervals() -> Tuple[List[Interval], List[Dict[str, Any]]]:
    p_f = SCENARIO["time"]["phases"]["f"]
    f_start = int(p_f["start_min"])
    f_end = int(p_f["end_min"])
    events = sorted(SCENARIO["failure_events"], key=lambda e: (e["at_min"], e["order"]))

    active_flow_rate: Dict[str, float] = {}
    active_bg_rate: Dict[Tuple[str, str], float] = {}
    active_flow_lat: Dict[str, Tuple[float, float]] = {}

    intervals: List[Interval] = []
    one_shots: List[Dict[str, Any]] = []

    for idx, ev in enumerate(events):
        at = int(ev["at_min"])
        if at < f_start or at >= f_end:
            continue

        # Gap interval if needed
        if at > (intervals[-1].end_min if intervals else f_start):
            prev_end = intervals[-1].end_min if intervals else f_start
            intervals.append(
                Interval(
                    start_min=prev_end,
                    end_min=at,
                    flow_rate_mult=dict(active_flow_rate),
                    bg_rate_mult=dict(active_bg_rate),
                    flow_latency_mult=dict(active_flow_lat),
                )
            )

        # Apply persistent updates
        for k, v in ev.get("rate_multipliers", {}).items():
            if "." in k:
                cid, lid = parse_ref(k)
                active_bg_rate[(cid, lid)] = float(v)
            else:
                active_flow_rate[k] = float(v)
        for fid, mv in ev.get("latency_multipliers", {}).items():
            active_flow_lat[fid] = (float(mv.get("p50", 1.0)), float(mv.get("p95", 1.0)))
        for os in ev.get("one_shots", []) or []:
            one_shots.append({"at_min": at, **os})

        # Interval until next event or end
        next_at = f_end
        for j in range(idx + 1, len(events)):
            nxt = int(events[j]["at_min"])
            if f_start <= nxt < f_end:
                next_at = nxt
                break
        intervals.append(
            Interval(
                start_min=at,
                end_min=next_at,
                flow_rate_mult=dict(active_flow_rate),
                bg_rate_mult=dict(active_bg_rate),
                flow_latency_mult=dict(active_flow_lat),
            )
        )

    if not intervals:
        intervals.append(Interval(f_start, f_end, {}, {}, {}))
    else:
        last_end = intervals[-1].end_min
        if last_end < f_end:
            intervals.append(
                Interval(
                    start_min=last_end,
                    end_min=f_end,
                    flow_rate_mult=dict(intervals[-1].flow_rate_mult),
                    bg_rate_mult=dict(intervals[-1].bg_rate_mult),
                    flow_latency_mult=dict(intervals[-1].flow_latency_mult),
                )
            )

    # Merge adjacent identical controls
    merged: List[Interval] = []
    for it in intervals:
        if not merged:
            merged.append(it)
            continue
        prev = merged[-1]
        if (
            prev.end_min == it.start_min
            and prev.flow_rate_mult == it.flow_rate_mult
            and prev.bg_rate_mult == it.bg_rate_mult
            and prev.flow_latency_mult == it.flow_latency_mult
        ):
            merged[-1] = Interval(prev.start_min, it.end_min, prev.flow_rate_mult, prev.bg_rate_mult, prev.flow_latency_mult)
        else:
            merged.append(it)

    return merged, one_shots


FAIL_INTERVALS, ONE_SHOTS = build_failure_intervals()

# -----------------------------
# Emission planning / scheduling
# -----------------------------

def schedule_times_evenly(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = max(0.001, (end_dt - start_dt).total_seconds())
    step = dur_s / count
    max_jitter_s = min(0.5, 0.25 * step)
    times: List[datetime] = []
    for i in range(count):
        t_s = (i + 0.5) * step + stable_jitter_seconds(key, i, max_jitter_s)
        t_s = min(max(t_s, 0.0), dur_s - 1e-6)
        times.append(start_dt + timedelta(seconds=t_s))
    return times


def get_bg_multiplier(state: str, component_id: str, log_id: str, interval: Optional[Interval]) -> float:
    if state != "f" or interval is None:
        return 1.0
    return float(interval.bg_rate_mult.get((component_id, log_id), 1.0))


def get_flow_rate_multiplier(state: str, flow_id: str, interval: Optional[Interval]) -> float:
    if state != "f" or interval is None:
        return 1.0
    return float(interval.flow_rate_mult.get(flow_id, 1.0))


def get_flow_latency_multiplier(state: str, flow_id: str, interval: Optional[Interval]) -> Tuple[float, float]:
    if state != "f" or interval is None:
        return (1.0, 1.0)
    return interval.flow_latency_mult.get(flow_id, (1.0, 1.0))


def pick_log_var(component_id: str, log_id: str, var_name: str, state: str, u: float, i: int, salt: int = 0) -> Any:
    tpl = LOG_TPL[(component_id, log_id)]
    dom = None
    if "state_vars" in tpl and tpl["state_vars"] and state in tpl["state_vars"] and var_name in tpl["state_vars"][state]:
        dom = tpl["state_vars"][state][var_name]
    else:
        dom = tpl.get("vars", {}).get(var_name)

    if dom is None:
        return ""

    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        return choose_from_list(list(v), u)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        span = hi - lo + 1
        return lo + int(math.floor(u * span)) % span
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + (hi - lo) * u
    if k == "hex":
        n = int(v)
        seed = crc32_u32(f"{component_id}.{log_id}.{var_name}:{i}:{salt}")
        local = np.random.default_rng(seed)
        return gen_hex(n, local)
    if k == "ip":
        return gen_ip(i, salt=salt)
    if k == "str":
        return gen_str(str(v), i, salt=salt)
    if k == "uuid":
        seed = crc32_u32(f"{component_id}.{log_id}.{var_name}:{i}:{salt}")
        local = np.random.default_rng(seed)
        b = local.integers(0, 256, size=16, dtype=np.uint8).tobytes()
        hx = b.hex()
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:]}"
    return ""


def render_log(component_id: str, log_id: str, state: str, bound_vars: Dict[str, Any], ordinal: int, salt: int = 0) -> Tuple[str, str]:
    tpl = LOG_TPL[(component_id, log_id)]
    msg_tpl = tpl["msg"]

    vars_needed: List[str] = []
    for frag in msg_tpl.split("{"):
        if "}" in frag:
            vars_needed.append(frag.split("}", 1)[0])

    u_seed = crc32_u32(f"render:{component_id}.{log_id}:{state}:{ordinal}:{salt}")
    for vi, var in enumerate(vars_needed):
        if var not in bound_vars:
            u = stable_u01(u_seed, vi)
            bound_vars[var] = pick_log_var(component_id, log_id, var, state, u, ordinal, salt=salt)

    msg = msg_tpl.format(**bound_vars)
    lvl = tpl["lvl"]
    return lvl, msg


# -----------------------------
# Flow context builders
# -----------------------------

TIP_LOCAL_IMPS = ["imp12", "imp17", "imp33"]
IMP_HOSTS = COMP["imp_node"]["hosts"]
TIP_HOSTS = COMP["tip_access"]["hosts"]

CYCLE_SEQS = [8, 40, 44]


def pick_tip_host(i: int) -> str:
    return TIP_HOSTS[i % len(TIP_HOSTS)]


def pick_imp_host_roundrobin(i: int, offset: int = 0) -> str:
    return IMP_HOSTS[(i + offset) % len(IMP_HOSTS)]


def pick_local_imp(i: int) -> str:
    return TIP_LOCAL_IMPS[i % len(TIP_LOCAL_IMPS)]


def build_tip_flow_context(flow_id: str, instance_idx: int) -> Dict[str, Any]:
    user = choose_from_list(["alice", "bob", "carol", "dave", "erin", "frank"], stable_u01(crc32_u32(f"user:{flow_id}"), instance_idx))
    dst_host = gen_str("remote-hostname", instance_idx, salt=crc32_u32(flow_id) & 0xFFFF)
    local_imp = pick_local_imp(instance_idx)
    dst_ip = gen_ip(instance_idx, salt=77)
    next_hop = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], stable_u01(crc32_u32(f"nh:{flow_id}"), instance_idx))
    if next_hop == local_imp:
        next_hop = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], stable_u01(crc32_u32(f"nh2:{flow_id}"), instance_idx))

    rtt_ms = 20 + int((stable_u01(crc32_u32(f"rtt:{flow_id}"), instance_idx) * 200.0))
    rtt_ms = min(rtt_ms, 220)

    u = stable_u01(crc32_u32(f"cause:{flow_id}"), instance_idx)
    if u < 0.6:
        cause = "no_physical_path"
    elif u < 0.85:
        cause = "line_down"
    else:
        cause = "routing_busy"

    return {
        "user": user,
        "dst_host": dst_host,
        "local_imp": local_imp,
        "dst_ip": dst_ip,
        "next_hop": next_hop,
        "rtt_ms": rtt_ms,
        "cause": cause,
    }


def build_ru_context(flow_id: str, instance_idx: int, state: str, recv_host: str) -> Dict[str, Any]:
    seed = crc32_u32(f"ru:{flow_id}:{state}:{recv_host}")
    u0 = stable_u01(seed, instance_idx)
    u1 = stable_u01(seed ^ 0xA5A5A5A5, instance_idx)
    u2 = stable_u01(seed ^ 0x5A5A5A5A, instance_idx)

    if flow_id in {"routing_update_backlog", "routing_update_processed_f", "routing_update_drop"}:
        origin_imp = "imp50"
        seq = int(choose_from_list(CYCLE_SEQS, u0))
        from_imp = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], u1)
        bytes_ = 48 + int(math.floor(u2 * (96 - 48 + 1)))
        prev_seq = int(CYCLE_SEQS[(CYCLE_SEQS.index(seq) + 1) % len(CYCLE_SEQS)])
        q_depth_enqueue = 200 + ((instance_idx * 17 + (seed & 0x3FF)) % (8000 - 200 + 1))
        q_depth_drop = 2000 + ((instance_idx * 31 + (seed & 0x7FF)) % (12000 - 2000 + 1))
    else:
        origin_imp = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"], u0)
        seq = int(math.floor(stable_u01(seed, instance_idx + 77) * 64.0)) % 64
        from_imp = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], u1)
        bytes_ = 48 + int(math.floor(u2 * (96 - 48 + 1)))
        prev_seq = (seq - (1 + (instance_idx % 7))) % 64
        q_depth_enqueue = 0
        q_depth_drop = 0

    to_imp = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], stable_u01(seed ^ 0xCAFEBABE, instance_idx))
    if to_imp == recv_host and recv_host in ["imp12", "imp17", "imp29", "imp33", "imp41"]:
        to_imp = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41"], stable_u01(seed ^ 0xFACE0FF0, instance_idx))

    if state == "n":
        tx_kind = "initial"
    else:
        tx_kind = "retransmit" if ((instance_idx + seq) % 3 == 0) else "initial"

    return {
        "origin_imp": origin_imp,
        "seq": seq,
        "from_imp": from_imp,
        "bytes": bytes_,
        "prev_seq": prev_seq,
        "to_imp": to_imp,
        "tx_kind": tx_kind,
        "q_depth": q_depth_enqueue,
        "q_depth_drop": q_depth_drop,
    }


# -----------------------------
# Simulation
# -----------------------------

rows: List[Dict[str, Any]] = []


def emit_row(ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {
            "_ts": ts,
            "timestamp": "",
            "level": level,
            "message": message,
            "trace_id": trace_id,
            "service": service,
            "host": host,
        }
    )


def simulate_background_interval(state: str, start_min: int, end_min: int, interval_obj: Optional[Interval]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end_min - start_min))

    carry: Dict[str, float] = {}

    for cid in sorted(COMP.keys()):
        beh = COMP[cid]["beh"][state]["emit"]
        if not beh:
            continue

        hosts = COMP[cid]["hosts"]
        for ent in beh:
            lid = ent["id"]
            per_min = float(ent["per_min"])
            scope = ent.get("scope", "per_host")
            mult = get_bg_multiplier(state, cid, lid, interval_obj)
            eff = per_min * mult
            if eff <= 0.0:
                continue

            if scope == "global":
                expected = eff * duration_min
                key = f"bg:{state}:{start_min}-{end_min}:{cid}.{lid}:global"
                c = carry.get(key, 0.0)
                n, c = carry_round(expected, c)
                carry[key] = c
                times = schedule_times_evenly(start_dt, end_dt, n, key)
                for i, ts in enumerate(times):
                    host = hosts[0] if hosts else ""
                    service = COMP[cid]["svc"] or ""
                    bound: Dict[str, Any] = {}

                    if cid == "imp_node" and lid in {"cpu_stat", "cpu_warn"}:
                        if lid == "cpu_stat":
                            if state == "n":
                                cpu = 10 + (i * 7) % 41
                                routing = 1 + (i * 5) % 30
                            else:
                                cpu = 70 + (i * 11) % 30
                                routing = 50 + (i * 13) % 49
                            bound = {"cpu_pct": cpu, "routing_pct": routing}
                        else:
                            cpu = 88 + (i * 3) % 13
                            routing = 75 + (i * 5) % 26
                            bound = {"cpu_pct": cpu, "routing_pct": min(routing, 100)}
                    elif cid == "imp_node" and lid == "buf_stat":
                        if state == "n":
                            free = 2500 + (i * 31) % (4096 - 2500 + 1)
                            waiters = (i * 2) % 21
                        else:
                            free = (i * 47) % 801
                            waiters = (i * 19) % 901
                        bound = {"buf_free": free, "buf_total": 4096, "buf_waiters": waiters}
                    elif cid == "imp_node" and lid == "buffer_low":
                        symptom = "hello_drop" if state == "f" and (i % 3 == 0) else ("update_queue_full" if (i % 3 == 1) else "rx_no_buffer")
                        free = (i * 7) % 81
                        waiters = 50 + (i * 37) % (1500 - 50 + 1)
                        bound = {"buf_free": free, "buf_waiters": waiters, "symptom": symptom}
                    elif cid == "imp_node" and lid == "line_state":
                        if state == "f":
                            st = "down" if (i % 10) < 7 else "up"
                            reason = "hello_timeout" if st == "down" else ("admin_reset" if (i % 2 == 0) else "quality_low")
                        else:
                            st = "up" if (i % 10) < 8 else "down"
                            reason = "quality_low" if st == "up" else "hello_timeout"
                        neighbor = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"], stable_u01(crc32_u32(f"nbr:{key}"), i))
                        bound = {"neighbor_imp": neighbor, "state": st, "reason": reason}
                    elif cid == "imp_node" and lid == "session_reset":
                        bound = {"peer": gen_str("peer-id", i, salt=17), "reason": choose_from_list(["line_flap", "route_change", "timeout"], stable_u01(crc32_u32(f"sr:{key}"), i))}
                    elif cid == "ncc" and lid == "patch_ack":
                        u = stable_u01(crc32_u32(f"ack:{start_min}-{end_min}"), i)
                        status = "unreachable" if u < 0.45 else ("pending" if u < 0.7 else "applied")
                        imp_id = choose_from_list(["imp12", "imp17", "imp33", "imp29", "imp41"], stable_u01(crc32_u32(f"ackimp:{key}"), i))
                        lag_s = int(30 + (stable_u01(crc32_u32(f"lag:{key}"), i) ** 2) * (14400 - 30))
                        bound = {"imp_id": imp_id, "status": status, "lag_s": lag_s}

                    lvl, msg = render_log(cid, lid, state, bound, ordinal=i, salt=start_min)
                    emit_row(ts, lvl, msg, "", service, host)
            else:
                for h_idx, host in enumerate(hosts):
                    expected = eff * duration_min
                    key = f"bg:{state}:{start_min}-{end_min}:{cid}.{lid}:{host}"
                    c = carry.get(key, 0.0)
                    n, c = carry_round(expected, c)
                    carry[key] = c
                    times = schedule_times_evenly(start_dt, end_dt, n, key)
                    service = COMP[cid]["svc"] or ""
                    for i, ts in enumerate(times):
                        bound: Dict[str, Any] = {}
                        if cid == "imp_node" and lid in {"cpu_stat", "cpu_warn"}:
                            if lid == "cpu_stat":
                                if state == "n":
                                    cpu = 10 + ((i + h_idx * 17) * 7) % 41
                                    routing = 1 + ((i + h_idx * 11) * 5) % 30
                                else:
                                    cpu = 70 + ((i + h_idx * 13) * 11) % 30
                                    routing = 50 + ((i + h_idx * 19) * 13) % 49
                                bound = {"cpu_pct": cpu, "routing_pct": routing}
                            else:
                                cpu = 88 + ((i + h_idx * 7) * 3) % 13
                                routing = 75 + ((i + h_idx * 5) * 5) % 26
                                bound = {"cpu_pct": cpu, "routing_pct": min(routing, 100)}
                        elif cid == "imp_node" and lid == "buf_stat":
                            if state == "n":
                                free = 2500 + ((i + h_idx * 23) * 31) % (4096 - 2500 + 1)
                                waiters = ((i + h_idx * 3) * 2) % 21
                            else:
                                free = ((i + h_idx * 7) * 47) % 801
                                waiters = ((i + h_idx * 9) * 19) % 901
                            bound = {"buf_free": free, "buf_total": 4096, "buf_waiters": waiters}
                        elif cid == "imp_node" and lid == "buffer_low":
                            symptom = "hello_drop" if (state == "f" and ((i + h_idx) % 3 == 0)) else ("update_queue_full" if ((i + h_idx) % 3 == 1) else "rx_no_buffer")
                            free = ((i + h_idx * 5) * 7) % 81
                            waiters = 50 + ((i + h_idx * 11) * 37) % (1500 - 50 + 1)
                            bound = {"buf_free": free, "buf_waiters": waiters, "symptom": symptom}
                        elif cid == "imp_node" and lid == "line_state":
                            if state == "f":
                                st = "down" if ((i + h_idx) % 10) < 7 else "up"
                                reason = "hello_timeout" if st == "down" else ("admin_reset" if ((i + h_idx) % 2 == 0) else "quality_low")
                            else:
                                st = "up" if ((i + h_idx) % 10) < 8 else "down"
                                reason = "quality_low" if st == "up" else "hello_timeout"
                            neighbor = choose_from_list(["imp12", "imp17", "imp29", "imp33", "imp41", "imp50"], stable_u01(crc32_u32(f"nbr:{key}"), i))
                            bound = {"neighbor_imp": neighbor, "state": st, "reason": reason}
                        elif cid == "imp_node" and lid == "session_reset":
                            bound = {"peer": gen_str("peer-id", i, salt=17 + h_idx), "reason": choose_from_list(["line_flap", "route_change", "timeout"], stable_u01(crc32_u32(f"sr:{key}"), i))}
                        lvl, msg = render_log(cid, lid, state, bound, ordinal=i, salt=start_min + h_idx)
                        emit_row(ts, lvl, msg, "", service, host)


def simulate_flow_interval(state: str, flow: Dict[str, Any], start_min: int, end_min: int, interval_obj: Optional[Interval]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end_min - start_min))

    flow_id = flow["id"]
    rpm = float(flow["rpm"])
    rm = get_flow_rate_multiplier(state, flow_id, interval_obj)
    eff_rpm = rpm * rm
    if eff_rpm <= 0.0 or duration_min <= 0.0:
        return

    expected_instances = eff_rpm * duration_min

    carry_key = f"flow_instances:{state}:{flow_id}"
    if not hasattr(simulate_flow_interval, "_carry"):
        simulate_flow_interval._carry = {}
    carry_map: Dict[str, float] = simulate_flow_interval._carry  # type: ignore[attr-defined]
    c = carry_map.get(carry_key, 0.0)
    n_instances, c = carry_round(expected_instances, c)
    carry_map[carry_key] = c
    if n_instances <= 0:
        return

    times = schedule_times_evenly(start_dt, end_dt, n_instances, f"flowstart:{state}:{start_min}-{end_min}:{flow_id}")
    p50m, p95m = get_flow_latency_multiplier(state, flow_id, interval_obj)

    for inst_i, t0 in enumerate(times):
        global_instance_idx = (start_min * 1_000_000) + inst_i
        trace_id = ""
        if bool(flow.get("trace", False)):
            local_seed = crc32_u32(f"trace:{state}:{flow_id}:{start_min}:{inst_i}")
            local_rng = np.random.default_rng(local_seed)
            trace_id = gen_hex(32, local_rng)

        component_host: Dict[str, str] = {}

        bound_common: Dict[str, Any] = {}
        if flow_id.startswith("tip_connect_"):
            bound_common = build_tip_flow_context(flow_id, global_instance_idx)
            component_host["tip_access"] = pick_tip_host(global_instance_idx)
            component_host["imp_node"] = str(bound_common["local_imp"])
        else:
            recv_host = pick_imp_host_roundrobin(global_instance_idx, offset=crc32_u32(flow_id) & 0x7)
            component_host["imp_node"] = recv_host
            bound_common = build_ru_context(flow_id, global_instance_idx, state, recv_host)

        t = t0
        lat_pairs = flow["latency_ms"]
        emit_refs = flow["emit"]
        for j, ref in enumerate(emit_refs):
            cid, lid = parse_ref(ref)
            host = component_host.get(cid, (COMP[cid]["hosts"][0] if COMP[cid]["hosts"] else ""))
            service = COMP[cid]["svc"] or ""

            p50, p95 = float(lat_pairs[j][0]), float(lat_pairs[j][1])
            p50_s = p50 * p50m
            p95_s = p95 * p95m
            u = stable_u01(crc32_u32(f"lat:{state}:{flow_id}:{start_min}:{inst_i}"), j)
            delay_ms = sample_lognormal_ms(p50_s, p95_s, u, soft_cap_ms=max(3.0 * p95_s, p95_s + 50.0))
            if j == 0:
                t = t + timedelta(milliseconds=min(delay_ms, 3.0))
            else:
                t = t + timedelta(milliseconds=delay_ms)

            bound: Dict[str, Any] = dict(bound_common)

            if lid == "ru_enqueue":
                bound["q_depth"] = int(bound_common.get("q_depth", 0))
            if lid == "ru_qdrop":
                bound["q_depth"] = int(bound_common.get("q_depth_drop", bound_common.get("q_depth", 2000)))
            if lid == "ru_fwd":
                bound["tx_kind"] = bound_common.get("tx_kind", "initial")

            lvl, msg = render_log(cid, lid, state, bound, ordinal=global_instance_idx, salt=j + start_min)
            emit_row(t, lvl, msg, trace_id if flow.get("trace", False) else "", service, host)


def simulate_one_shots() -> None:
    for os in ONE_SHOTS:
        at_min = int(os["at_min"])
        ref = os["ref"]
        count = int(os["count"])
        hosts = list(os.get("hosts", []))
        cid, lid = parse_ref(ref)
        service = COMP[cid]["svc"] or ""
        base = BASE_TIME + timedelta(minutes=at_min)
        for i in range(count):
            ts = base + timedelta(seconds=stable_jitter_seconds(f"oneshot:{ref}:{at_min}", i, 1.5) + (i * 0.15))
            host = hosts[i % len(hosts)] if hosts else (COMP[cid]["hosts"][0] if COMP[cid]["hosts"] else "")
            bound: Dict[str, Any] = {}

            if cid == "ncc" and lid == "restart_cmd":
                imp_id = choose_from_list(["imp12", "imp17", "imp33", "imp29"], stable_u01(crc32_u32(f"os:{ref}:imp"), i))
                reason = "routing_storm" if (i % 2 == 0) else "diagnostics"
                bound = {"imp_id": imp_id, "reason": reason}
            elif cid == "imp_node" and lid == "restart_complete":
                uptime_s = 5 + int(stable_u01(crc32_u32(f"os:{ref}:up"), i) * (120 - 5))
                bound = {"dump_taken": "yes", "uptime_s": uptime_s}
            elif cid == "ncc" and lid == "patch_broadcast_start":
                bound = {"ignore_origin": "imp50"}
            elif cid == "imp_node" and lid == "patch_applied":
                bound = {"ignore_origin": "imp50", "patch_id": "temp_ignore_origin"}

            lvl, msg = render_log(cid, lid, "f", bound, ordinal=i, salt=at_min)
            emit_row(ts, lvl, msg, "", service, host)


def run() -> None:
    n_start = int(SCENARIO["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["time"]["phases"]["n"]["end_min"])

    simulate_background_interval("n", n_start, n_end, None)
    for flow in SYSTEM["flows"]["n"]:
        simulate_flow_interval("n", flow, n_start, n_end, None)

    for it in FAIL_INTERVALS:
        simulate_background_interval("f", it.start_min, it.end_min, it)
        for flow in SYSTEM["flows"]["f"]:
            simulate_flow_interval("f", flow, it.start_min, it.end_min, it)

    simulate_one_shots()

    df = pd.DataFrame(rows)
    df.sort_values("_ts", inplace=True, kind="mergesort")
    df["timestamp"] = df["_ts"].apply(iso_utc_ms)
    df.drop(columns=["_ts"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    n_rows = len(df)
    if not (20000 <= n_rows <= 100000):
        raise RuntimeError(f"Row count {n_rows} out of required range [20000, 100000].")

    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV columns do not match required schema.")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    run()
