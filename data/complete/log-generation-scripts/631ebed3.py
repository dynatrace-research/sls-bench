import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "knight_smars_rlp_router",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}, "origins": ["order_gateway"]},
    "components": {
        "order_gateway": {
            "svc": "order-gateway",
            "hosts": ["gw1", "gw2"],
            "logs": {
                "rlp_order_recv": {
                    "lvl": "INFO",
                    "msg": "recv parent_order={order_id} sym={sym} qty={qty} broker={broker} rlp_flag={rlp_flag} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "sym": {"k": "ch", "v": ["AAPL", "MSFT", "GE", "F", "BAC", "XOM", "INTC", "CSCO", "AMZN", "JPM", "WMT", "T", "C"]},
                        "qty": {"k": "i", "v": [100, 50000]},
                        "broker": {"k": "ch", "v": ["td_ameritrade", "etrade", "scottrade", "vanguard", "other"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {"n": {"rlp_flag": {"k": "ch", "v": ["off"]}}, "f": {"rlp_flag": {"k": "ch", "v": ["on"]}}},
                },
                "respond_ok": {
                    "lvl": "INFO",
                    "msg": "complete parent_order={order_id} status=FILLED fills={fills} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "fills": {"k": "i", "v": [1, 50000]},
                        "dur_ms": {"k": "i", "v": [5, 60000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "respond_err": {
                    "lvl": "WARN",
                    "msg": "complete parent_order={order_id} status=REJECTED reason={reason} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["downstream_disabled", "timeout", "overloaded"]},
                        "dur_ms": {"k": "i", "v": [1, 2000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "conn_pool_warn": {
                    "lvl": "WARN",
                    "msg": "grpc pool warning dst=smars_router active_conns={active_conns} pending={pending}",
                    "vars": {"active_conns": {"k": "i", "v": [20, 300]}, "pending": {"k": "i", "v": [0, 50]}},
                },
            },
            "beh": {
                "n": [{"id": "conn_pool_warn", "per_min": 0.08, "scope": "global"}],
                "f": [{"id": "conn_pool_warn", "per_min": 0.12, "scope": "global"}],
            },
        },
        "smars_router": {
            "svc": "smars",
            "hosts": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"],
            "logs": {
                "dispatch": {
                    "lvl": "INFO",
                    "msg": "dispatch parent_order={order_id} host={host} build={build} rlp_flag={rlp_flag} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"build": {"k": "ch", "v": ["rlp_v2"]}, "rlp_flag": {"k": "ch", "v": ["off"]}},
                        "f": {"build": {"k": "ch", "v": ["rlp_v2", "legacy_v1"]}, "rlp_flag": {"k": "ch", "v": ["on"]}},
                    },
                },
                "powerpeg_activated": {
                    "lvl": "ERROR",
                    "msg": "PowerPeg active parent_order={order_id} sym={sym} host={host} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "sym": {"k": "ch", "v": ["AAPL", "MSFT", "GE", "F", "BAC", "XOM", "INTC", "CSCO", "AMZN", "JPM", "WMT", "T", "C"]},
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "child_order_burst": {
                    "lvl": "WARN",
                    "msg": "child order burst parent_order={order_id} host={host} child_orders={child_orders} throttle={throttle} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "child_orders": {"k": "i", "v": [50, 250000]},
                        "throttle": {"k": "ch", "v": ["on", "off"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat host={host} build={build} queue_depth={queue_depth} cpu_pct={cpu_pct}",
                    "vars": {
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "queue_depth": {"k": "i", "v": [0, 5000]},
                        "cpu_pct": {"k": "i", "v": [5, 100]},
                    },
                    "state_vars": {"n": {"build": {"k": "ch", "v": ["rlp_v2"]}}, "f": {"build": {"k": "ch", "v": ["rlp_v2", "legacy_v1"]}}},
                },
                "child_order_stats": {
                    "lvl": "INFO",
                    "msg": "stats host={host} child_orders_sent={child_orders_sent} exec_reports_rcvd={exec_reports_rcvd} unmatched_execs={unmatched_execs}",
                    "vars": {
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "child_orders_sent": {"k": "i", "v": [100, 6000000]},
                        "exec_reports_rcvd": {"k": "i", "v": [100, 6000000]},
                        "unmatched_execs": {"k": "i", "v": [0, 2000000]},
                    },
                },
                "shutdown_host": {
                    "lvl": "INFO",
                    "msg": "trading disabled host={host} reason={reason}",
                    "vars": {
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "reason": {"k": "ch", "v": ["incident_mitigation", "operator_request"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "heartbeat", "per_min": 1.0}, {"id": "child_order_stats", "per_min": 0.5}],
                "f": [{"id": "heartbeat", "per_min": 1.0}, {"id": "child_order_stats", "per_min": 0.5}],
            },
        },
        "nyse_venue": {
            "svc": "nyse",
            "hosts": ["nyse1"],
            "logs": {
                "parent_exec_summary": {
                    "lvl": "INFO",
                    "msg": "exec summary parent_order={order_id} sym={sym} side={side} execs={execs} shares={shares} venues={venues} trace_id={trace_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "sym": {"k": "ch", "v": ["AAPL", "MSFT", "GE", "F", "BAC", "XOM", "INTC", "CSCO", "AMZN", "JPM", "WMT", "T", "C"]},
                        "side": {"k": "ch", "v": ["BUY", "SELL"]},
                        "execs": {"k": "i", "v": [1, 50000]},
                        "shares": {"k": "i", "v": [100, 5000000]},
                        "venues": {"k": "i", "v": [1, 20]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "volume_alert": {
                    "lvl": "WARN",
                    "msg": "volume anomaly member={member} symbols_affected={symbols_affected} volume_mult={volume_mult} action={action}",
                    "vars": {
                        "member": {"k": "ch", "v": ["KNIGHT"]},
                        "symbols_affected": {"k": "i", "v": [5, 200]},
                        "volume_mult": {"k": "f", "v": [1.0, 10.0]},
                        "action": {"k": "ch", "v": ["monitoring", "called_member"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "volume_alert", "per_min": 0.01, "scope": "global"}],
                "f": [{"id": "volume_alert", "per_min": 0.05, "scope": "global"}],
            },
        },
        "bnet_alerting": {
            "svc": "bnet",
            "hosts": ["bnet1"],
            "logs": {
                "email_powerpeg_disabled": {
                    "lvl": "INFO",
                    "msg": "email queued to={to_group} subject={subject} host={host} detail={detail}",
                    "vars": {
                        "to_group": {"k": "ch", "v": ["smars-notify"]},
                        "subject": {"k": "ch", "v": ["SMARS: Power Peg disabled"]},
                        "host": {"k": "ch", "v": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"]},
                        "detail": {"k": "ch", "v": ["flag_mismatch", "feature_disabled", "unknown_state"]},
                    },
                }
            },
            "beh": {"n": [], "f": []},
        },
        "pmon": {
            "svc": "pmon",
            "hosts": ["pmon1"],
            "logs": {
                "position_snapshot": {
                    "lvl": "INFO",
                    "msg": "snapshot acct={acct} gross_usd={gross_usd} net_usd={net_usd} symbols={symbols}",
                    "vars": {
                        "acct": {"k": "ch", "v": ["33", "MM_MAIN"]},
                        "gross_usd": {"k": "i", "v": [0, 4000000000]},
                        "net_usd": {"k": "i", "v": [-4000000000, 4000000000]},
                        "symbols": {"k": "i", "v": [0, 200]},
                    },
                },
                "limit_breach": {
                    "lvl": "WARN",
                    "msg": "limit breach acct={acct} gross_usd={gross_usd} limit_usd={limit_usd} action={action}",
                    "vars": {
                        "acct": {"k": "ch", "v": ["33"]},
                        "gross_usd": {"k": "i", "v": [1000000, 4000000000]},
                        "limit_usd": {"k": "i", "v": [2000000, 2000000]},
                        "action": {"k": "ch", "v": ["notify_only"]},
                    },
                },
                "operator_ack": {
                    "lvl": "INFO",
                    "msg": "operator viewed breach acct={acct} ack_by={ack_by}",
                    "vars": {"acct": {"k": "ch", "v": ["33"]}, "ack_by": {"k": "ch", "v": ["cio", "it_lead", "risk_ops"]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "position_snapshot", "per_min": 1.0, "scope": "global"},
                    {"id": "limit_breach", "per_min": 0.01, "scope": "global"},
                ],
                "f": [
                    {"id": "position_snapshot", "per_min": 1.0, "scope": "global"},
                    {"id": "limit_breach", "per_min": 0.2, "scope": "global"},
                ],
            },
        },
        "ops_console": {
            "svc": "ops",
            "hosts": ["ops1"],
            "logs": {
                "rollback_started": {
                    "lvl": "WARN",
                    "msg": "ops started rollback target_build={target_build} reason={reason}",
                    "vars": {"target_build": {"k": "ch", "v": ["legacy_v1"]}, "reason": {"k": "ch", "v": ["suspected_new_code_issue", "incident_response"]}},
                },
                "shutdown_issued": {
                    "lvl": "CRITICAL",
                    "msg": "ops issued SMARS shutdown reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["runaway_orders", "incident_response"]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": {
            "parent_order_normal": {
                "rpm": 180.0,
                "emit": ["order_gateway.rlp_order_recv", "smars_router.dispatch", "nyse_venue.parent_exec_summary", "order_gateway.respond_ok"],
                "latency_ms": [[1, 3], [2, 8], [10, 60], [1, 4]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            }
        },
        "f": {
            "parent_order_ok": {
                "rpm": 140.0,
                "emit": ["order_gateway.rlp_order_recv", "smars_router.dispatch", "nyse_venue.parent_exec_summary", "order_gateway.respond_ok"],
                "latency_ms": [[1, 3], [2, 10], [15, 100], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "parent_order_powerpeg": {
                "rpm": 20.0,
                "emit": [
                    "order_gateway.rlp_order_recv",
                    "smars_router.dispatch",
                    "smars_router.powerpeg_activated",
                    "smars_router.child_order_burst",
                    "nyse_venue.parent_exec_summary",
                    "order_gateway.respond_ok",
                ],
                "latency_ms": [[1, 3], [3, 20], [1, 5], [5, 50], [50, 7000], [1, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "parent_order_rejected": {
                "rpm": 120.0,
                "emit": ["order_gateway.rlp_order_recv", "order_gateway.respond_err"],
                "latency_ms": [[1, 3], [2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "knight_powerpeg_rlp_incident",
    "time": {"total_minutes": 46, "phases": {"n": {"start_min": 0, "end_min": 23}, "f": {"start_min": 23, "end_min": 46}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 23,
                    "rate_multipliers": {
                        "parent_order_rejected": 0.0,
                        "smars_router.child_order_stats": 5.0,
                        "pmon.limit_breach": 0.0,
                        "nyse_venue.volume_alert": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "bnet_alerting.email_powerpeg_disabled", "count": 97, "hosts": ["bnet1"]}],
                },
                {
                    "order": 2,
                    "at_min": 27,
                    "rate_multipliers": {"nyse_venue.volume_alert": 6.0, "pmon.limit_breach": 3.0, "smars_router.child_order_stats": 8.0},
                    "latency_multipliers": {"parent_order_powerpeg": {"p50": 2.0, "p95": 3.0}},
                    "one_shots": [{"ref": "pmon.operator_ack", "count": 1, "hosts": ["pmon1"]}],
                },
                {
                    "order": 3,
                    "at_min": 33,
                    "rate_multipliers": {"parent_order_ok": 0.0, "parent_order_powerpeg": 8.0, "smars_router.child_order_stats": 20.0},
                    "latency_multipliers": {"parent_order_powerpeg": {"p50": 3.0, "p95": 5.0}},
                    "one_shots": [{"ref": "ops_console.rollback_started", "count": 1, "hosts": ["ops1"]}],
                },
                {
                    "order": 4,
                    "at_min": 39,
                    "rate_multipliers": {
                        "parent_order_ok": 0.0,
                        "parent_order_powerpeg": 0.0,
                        "parent_order_rejected": 1.0,
                        "smars_router.heartbeat": 0.0,
                        "smars_router.child_order_stats": 0.0,
                        "nyse_venue.volume_alert": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_console.shutdown_issued", "count": 1, "hosts": ["ops1"]},
                        {
                            "ref": "smars_router.shutdown_host",
                            "count": 8,
                            "hosts": ["smars1", "smars2", "smars3", "smars4", "smars5", "smars6", "smars7", "smars8"],
                        },
                    ],
                },
            ]
        }
    },
}

SEED = "knight-smars-rlp-router-v3.1"
BASE_TIME = datetime(2026, 1, 1, 13, 30, 0, tzinfo=timezone.utc)

# Explicit RNG seeding for verifier reproducibility checks (even though the simulator uses hash-based determinism).
_SEED_INT = int.from_bytes(hashlib.sha256(SEED.encode("utf-8")).digest()[:8], "big", signed=False)
random.seed(_SEED_INT)
np.random.seed(_SEED_INT % (2**32))


def _sha256_bytes(s: str) -> bytes:
    return hashlib.sha256((SEED + "|" + s).encode("utf-8")).digest()


def hfloat(key: str) -> float:
    b = _sha256_bytes(key)[:8]
    x = int.from_bytes(b, "big", signed=False)
    return (x % (2**53)) / float(2**53)


def hint_choice(seq: List[Any], key: str) -> Any:
    if not seq:
        return None
    idx = int(hfloat(key) * len(seq)) % len(seq)
    return seq[idx]


def hint_int(lo: int, hi: int, key: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    span = hi - lo + 1
    return lo + (int(hfloat(key) * span) % span)


def hint_float(lo: float, hi: float, key: str) -> float:
    if hi < lo:
        lo, hi = hi, lo
    return lo + (hi - lo) * hfloat(key)


def det_hex(n: int, key: str) -> str:
    out = hashlib.sha256((SEED + "|hex|" + key).encode("utf-8")).hexdigest()
    if n <= len(out):
        return out[:n]
    while len(out) < n:
        out += hashlib.sha256((SEED + "|hex2|" + out).encode("utf-8")).hexdigest()
    return out[:n]


def det_uuid(key: str) -> str:
    raw = _sha256_bytes("uuid|" + key)[:16]
    b = bytearray(raw)
    b[6] = (b[6] & 0x0F) | 0x40  # version 4
    b[8] = (b[8] & 0x3F) | 0x80  # variant
    hx = b.hex()
    return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def inv_norm_cdf(p: float) -> float:
    if p <= 0.0:
        return -1e9
    if p >= 1.0:
        return 1e9

    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow

    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den
    if phigh < p:
        q = math.sqrt(-2 * math.log(1 - p))
        num = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    return num / den


def sample_lognormal_from_p50_p95(p50: float, p95: float, q: float, soft_cap: Optional[float] = None) -> float:
    p50 = max(1e-9, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.6448536269514722
    z = inv_norm_cdf(min(0.999, max(0.001, q)))
    x = math.exp(mu + sigma * z)
    if soft_cap is None:
        soft_cap = 3.0 * p95
    if x > soft_cap:
        x = soft_cap + (x - soft_cap) * 0.15
    return x


def allocate_count(expected: float, key: str) -> int:
    expected = max(0.0, float(expected))
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    return base + (1 if hfloat("alloc|" + key) < frac else 0)


def evenly_spaced_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_ms = max(1.0, (end - start).total_seconds() * 1000.0)
    step = total_ms / n
    times: List[datetime] = []
    for i in range(n):
        center_ms = (i + 0.5) * step
        jitter_ms = (hfloat(f"{key}|jitter|{i}") - 0.5) * min(250.0, 0.35 * step)
        t = start + timedelta(milliseconds=center_ms + jitter_ms)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        times.append(t)
    times.sort()
    return times


@dataclass(frozen=True)
class IntervalControls:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Tuple[float, float]]


def build_log_lookup() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for cid, c in SYSTEM["components"].items():
        for lid, tpl in c["logs"].items():
            out[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **tpl}
    return out


LOGS = build_log_lookup()


def get_component(cid: str) -> Dict[str, Any]:
    return SYSTEM["components"][cid]


def parse_ref(ref: str) -> Tuple[str, str]:
    cid, lid = ref.split(".", 1)
    return cid, lid


def iso_ms(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


def derive_failure_intervals() -> List[IntervalControls]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e["order"]))

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Tuple[float, float]] = {}

    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] < fend}) + [fend]
    boundaries = sorted(dict.fromkeys(boundaries))

    by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[IntervalControls] = []
    for i in range(len(boundaries) - 1):
        start_m = boundaries[i]
        end_m = boundaries[i + 1]
        for e in sorted(by_min.get(start_m, []), key=lambda x: x["order"]):
            for k, v in e.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fid, mv in e.get("latency_multipliers", {}).items():
                p50m = float(mv.get("p50", 1.0))
                p95m = float(mv.get("p95", 1.0))
                active_lat[fid] = (p50m, p95m)
        intervals.append(IntervalControls(start_m, end_m, dict(active_rate), dict(active_lat)))
    return intervals


FAILURE_INTERVALS = derive_failure_intervals()


def pick_component_host_for_flow(component_id: str, flow_key: str) -> str:
    hosts = get_component(component_id).get("hosts", [])
    if not hosts:
        return ""
    return hosts[int(hfloat(f"host|{component_id}|{flow_key}") * len(hosts)) % len(hosts)]


def gen_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom.get("v", None)
    if k == "uuid":
        return det_uuid(key)
    if k == "hex":
        return det_hex(int(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return hint_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(f"{hint_float(lo, hi, key):.3f}")
    if k == "ch":
        return hint_choice(list(v), key)
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v) if v is not None else f"str_{det_hex(8, key)}"
    return str(v) if v is not None else ""


def render_log_message(log_ref: str, state: str, bound: Dict[str, Any], key: str) -> str:
    tpl = LOGS[log_ref]
    vars_def = tpl.get("vars", {})
    state_vars = tpl.get("state_vars", {}).get(state, {})

    values: Dict[str, Any] = {}
    values.update(bound)

    for nm, dom in state_vars.items():
        if nm not in values:
            values[nm] = gen_from_domain(dom, f"{key}|sv|{log_ref}|{nm}")

    for nm, dom in vars_def.items():
        if nm not in values:
            values[nm] = gen_from_domain(dom, f"{key}|v|{log_ref}|{nm}")

    try:
        return tpl["msg"].format(**values)
    except KeyError as e:
        missing = str(e).strip("'")
        values[missing] = ""
        return tpl["msg"].format(**values)


def add_row(rows: List[Dict[str, Any]], ts: datetime, level: str, msg: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"timestamp": ts, "level": level, "message": msg, "trace_id": trace_id, "service": service, "host": host})


def sample_step_delay_ms(p50: float, p95: float, key: str) -> int:
    q = 0.50 + 0.45 * hfloat("q|" + key)
    x = sample_lognormal_from_p50_p95(p50, p95, q, soft_cap=3.0 * p95)
    return int(max(1.0, x))


def plan_flow_bound_context(flow_id: str, state: str, start_ts: datetime, idx: int, interval: Optional[IntervalControls]) -> Dict[str, Any]:
    flow_key = f"{state}|{flow_id}|{idx}|{int(start_ts.timestamp() * 1000)}"
    order_id = det_uuid("order|" + flow_key)
    trace_id = det_hex(32, "trace|" + flow_key) if SYSTEM["tracing"]["on"] else ""

    sym = hint_choice(SYSTEM["components"]["order_gateway"]["logs"]["rlp_order_recv"]["vars"]["sym"]["v"], "sym|" + flow_key)
    qty = hint_int(100, 50000, "qty|" + flow_key)
    broker = hint_choice(SYSTEM["components"]["order_gateway"]["logs"]["rlp_order_recv"]["vars"]["broker"]["v"], "broker|" + flow_key)
    side = hint_choice(["BUY", "SELL"], "side|" + flow_key)

    smars_host: str
    smars_build: str
    if state == "n":
        smars_host = hint_choice(get_component("smars_router")["hosts"], "smars_host|" + flow_key)
        smars_build = "rlp_v2"
    else:
        minute = int((start_ts - BASE_TIME).total_seconds() // 60)
        if minute >= 33:
            smars_build = "legacy_v1"
            smars_host = hint_choice(get_component("smars_router")["hosts"], "smars_host_postrb|" + flow_key)
        else:
            if flow_id == "parent_order_powerpeg":
                smars_build = "legacy_v1"
                if hfloat("smars8_bias|" + flow_key) < 0.875:
                    smars_host = "smars8"
                else:
                    smars_host = hint_choice(get_component("smars_router")["hosts"], "smars_host_pre_misc|" + flow_key)
            else:
                smars_build = "rlp_v2"
                smars_host = hint_choice(get_component("smars_router")["hosts"][:-1], "smars_host_ok_pre|" + flow_key)

    if flow_id == "parent_order_rejected":
        execs = 0
        shares = 0
        venues = 0
    elif flow_id == "parent_order_powerpeg":
        execs = hint_int(2000, 50000, "execs_pp|" + flow_key)
        shares = hint_int(100000, 5000000, "shares_pp|" + flow_key)
        venues = hint_int(5, 20, "venues_pp|" + flow_key)
    else:
        execs = hint_int(1, 30, "execs_small|" + flow_key)
        shares = max(100, min(5000000, qty * hint_int(1, 10, "shares_mult|" + flow_key)))
        venues = hint_int(1, 4, "venues_small|" + flow_key)

    if flow_id == "parent_order_rejected":
        fills = 0
    else:
        fills = min(50000, max(1, execs))

    gw_host = pick_component_host_for_flow("order_gateway", "gw|" + flow_key)
    nyse_host = "nyse1"

    return {
        "order_id": order_id,
        "trace_id": trace_id,
        "sym": sym,
        "qty": qty,
        "broker": broker,
        "side": side,
        "fills": fills,
        "smars_host": smars_host,
        "smars_build": smars_build,
        "gw_host": gw_host,
        "nyse_host": nyse_host,
        "execs": execs,
        "shares": shares,
        "venues": venues,
    }


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    state: str,
    flow_id: str,
    flow_def: Dict[str, Any],
    start_ts: datetime,
    idx: int,
    interval: Optional[IntervalControls],
) -> None:
    bound = plan_flow_bound_context(flow_id, state, start_ts, idx, interval)
    trace_id = bound["trace_id"] if flow_def.get("trace", False) else ""

    gw_host = bound["gw_host"]
    smars_host = bound["smars_host"]
    nyse_host = bound["nyse_host"]

    p50m = 1.0
    p95m = 1.0
    if state == "f" and interval is not None and flow_id in interval.lat_mult:
        p50m, p95m = interval.lat_mult[flow_id]

    t = start_ts
    step_times: List[datetime] = []
    for si, (p50, p95) in enumerate(flow_def["latency_ms"]):
        eff_p50 = float(p50) * p50m
        eff_p95 = float(p95) * p95m
        dms = sample_step_delay_ms(eff_p50, eff_p95, f"lat|{state}|{flow_id}|{idx}|{si}|{int(start_ts.timestamp()*1000)}")
        t = t + timedelta(milliseconds=dms)
        step_times.append(t)

    first_log_ts = step_times[0] if step_times else start_ts
    last_log_ts = step_times[-1] if step_times else start_ts
    dur_ms = int(max(1.0, (last_log_ts - first_log_ts).total_seconds() * 1000.0))

    if flow_id == "parent_order_rejected":
        bound["reason"] = "downstream_disabled"

    if flow_id == "parent_order_powerpeg":
        bound["throttle"] = "off"
        approx_child = min(250000, max(50, int(bound["execs"] * (1.5 + 2.5 * hfloat(f"child_mult|{state}|{flow_id}|{idx}")))))
        bound["child_orders"] = approx_child

    for si, log_ref in enumerate(flow_def["emit"]):
        tpl = LOGS[log_ref]
        cid = tpl["component_id"]
        service = get_component(cid).get("svc", "") or ""
        if cid == "order_gateway":
            host = gw_host
        elif cid == "smars_router":
            host = smars_host
        elif cid == "nyse_venue":
            host = nyse_host
        else:
            host = pick_component_host_for_flow(cid, f"flow_other|{state}|{flow_id}|{idx}|{si}")

        per_log_bound = dict(bound)
        if cid == "smars_router":
            per_log_bound["host"] = smars_host
            per_log_bound["build"] = bound["smars_build"]
        elif cid == "nyse_venue":
            per_log_bound["host"] = nyse_host
        elif cid == "order_gateway":
            per_log_bound["host"] = gw_host

        if log_ref == "nyse_venue.parent_exec_summary":
            per_log_bound["execs"] = bound["execs"]
            per_log_bound["shares"] = bound["shares"]
            per_log_bound["venues"] = bound["venues"]
            per_log_bound["side"] = bound["side"]
            per_log_bound["sym"] = bound["sym"]

        if log_ref == "order_gateway.respond_ok":
            per_log_bound["dur_ms"] = dur_ms
            per_log_bound["fills"] = bound["fills"]
        if log_ref == "order_gateway.respond_err":
            per_log_bound["dur_ms"] = dur_ms
            per_log_bound["reason"] = bound.get("reason", "downstream_disabled")

        msg = render_log_message(log_ref, state, per_log_bound, f"flowmsg|{state}|{flow_id}|{idx}|{si}")
        add_row(rows, step_times[si], tpl["lvl"], msg, trace_id, service, host)


def smars_build_for_host_at_time(host: str, ts: datetime, state: str) -> str:
    if state == "n":
        return "rlp_v2"
    minute = int((ts - BASE_TIME).total_seconds() // 60)
    if minute >= 33:
        return "legacy_v1"
    return "legacy_v1" if host == "smars8" else "rlp_v2"


def gen_smars_stats_values(host: str, ts: datetime, state: str) -> Tuple[int, int, int]:
    key = f"stats|{host}|{int(ts.timestamp())}|{state}"
    if state == "n":
        child = hint_int(500, 15000, "c|" + key)
        execs = max(100, child - hint_int(0, 500, "eoff|" + key))
        unmatched = hint_int(0, 200, "u|" + key)
        return child, execs, unmatched

    minute = int((ts - BASE_TIME).total_seconds() // 60)
    build = smars_build_for_host_at_time(host, ts, state)

    if minute < 27:
        if host == "smars8" and build == "legacy_v1":
            child = hint_int(200000, 1500000, "c1|" + key)
            execs = max(100, int(child * (0.9 + 0.2 * hfloat("e1|" + key))))
            unmatched = min(2000000, hint_int(10000, 250000, "u1|" + key))
        else:
            child = hint_int(1000, 40000, "c1b|" + key)
            execs = max(100, int(child * (0.8 + 0.4 * hfloat("e1b|" + key))))
            unmatched = hint_int(0, 2000, "u1b|" + key)
        return child, execs, unmatched

    if minute < 33:
        if host == "smars8" and build == "legacy_v1":
            child = hint_int(400000, 3000000, "c2|" + key)
            execs = max(100, int(child * (0.9 + 0.2 * hfloat("e2|" + key))))
            unmatched = min(2000000, hint_int(50000, 600000, "u2|" + key))
        else:
            child = hint_int(2000, 80000, "c2b|" + key)
            execs = max(100, int(child * (0.8 + 0.4 * hfloat("e2b|" + key))))
            unmatched = hint_int(0, 6000, "u2b|" + key)
        return child, execs, unmatched

    child = hint_int(800000, 6000000, "c3|" + key)
    execs = max(100, int(child * (0.92 + 0.15 * hfloat("e3|" + key))))
    unmatched = min(2000000, hint_int(100000, 2000000, "u3|" + key))
    return child, execs, unmatched


def gen_smars_heartbeat_values(host: str, ts: datetime, state: str) -> Tuple[int, int]:
    key = f"hb|{host}|{int(ts.timestamp())}|{state}"
    if state == "n":
        qd = hint_int(0, 120, "qd|" + key)
        cpu = hint_int(5, 35, "cpu|" + key)
        return qd, cpu
    minute = int((ts - BASE_TIME).total_seconds() // 60)
    if minute < 27:
        if host == "smars8":
            qd = hint_int(500, 2000, "qd8|" + key)
            cpu = hint_int(60, 95, "cpu8|" + key)
        else:
            qd = hint_int(0, 250, "qdN|" + key)
            cpu = hint_int(10, 45, "cpuN|" + key)
    elif minute < 33:
        if host == "smars8":
            qd = hint_int(1500, 5000, "qd8b|" + key)
            cpu = hint_int(75, 100, "cpu8b|" + key)
        else:
            qd = hint_int(50, 500, "qdNb|" + key)
            cpu = hint_int(15, 55, "cpuNb|" + key)
    else:
        qd = hint_int(1200, 5000, "qdAll|" + key)
        cpu = hint_int(70, 100, "cpuAll|" + key)
    return qd, cpu


def simulate_background_for_interval(
    rows: List[Dict[str, Any]],
    state: str,
    start_min: int,
    end_min: int,
    controls: Optional[IntervalControls],
) -> None:
    start_ts = BASE_TIME + timedelta(minutes=start_min)
    end_ts = BASE_TIME + timedelta(minutes=end_min)
    duration_min = (end_ts - start_ts).total_seconds() / 60.0

    for cid in sorted(SYSTEM["components"].keys()):
        comp = get_component(cid)
        beh = comp.get("beh", {}).get(state, [])
        if not beh:
            continue
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            ref = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f" and controls is not None:
                mult = float(controls.rate_mult.get(ref, 1.0))
            effective_per_min = per_min * mult

            tpl = LOGS[ref]
            service = comp.get("svc", "") or ""
            hosts = comp.get("hosts", [])
            if scope == "global":
                expected = effective_per_min * duration_min
                cnt = allocate_count(expected, f"bg|{state}|{start_min}-{end_min}|{ref}")
                times = evenly_spaced_times(start_ts, end_ts, cnt, f"bgtime|{state}|{start_min}-{end_min}|{ref}")
                for i, t in enumerate(times):
                    host = hosts[int(hfloat(f"bghost|{ref}|{start_min}-{end_min}|{i}") * len(hosts)) % len(hosts)] if hosts else ""
                    bound: Dict[str, Any] = {}
                    if ref == "smars_router.heartbeat":
                        bound["host"] = host
                        bound["build"] = smars_build_for_host_at_time(host, t, state)
                        qd, cpu = gen_smars_heartbeat_values(host, t, state)
                        bound["queue_depth"] = qd
                        bound["cpu_pct"] = cpu
                    elif ref == "smars_router.child_order_stats":
                        bound["host"] = host
                        child, execs, unmatched = gen_smars_stats_values(host, t, state)
                        bound["child_orders_sent"] = child
                        bound["exec_reports_rcvd"] = execs
                        bound["unmatched_execs"] = unmatched
                    elif ref == "pmon.position_snapshot":
                        minute = int((t - BASE_TIME).total_seconds() // 60)
                        severe = (state == "f" and minute >= 27)
                        acct = "33" if (hfloat(f"pmonacct|{minute}|{i}") < (0.8 if severe else 0.3)) else "MM_MAIN"
                        bound["acct"] = acct
                        if state == "n":
                            gross = hint_int(0, 50000000, f"pmongrossn|{minute}|{i}")
                            net = hint_int(-20000000, 20000000, f"pmonnetn|{minute}|{i}")
                            symbols = hint_int(0, 40, f"pmonsymn|{minute}|{i}")
                        else:
                            if acct == "33":
                                gross = hint_int(500000000, 4000000000, f"pmongrossf33|{minute}|{i}")
                                net = hint_int(-3000000000, 3000000000, f"pmonnetf33|{minute}|{i}")
                                symbols = hint_int(40, 200, f"pmonsymf33|{minute}|{i}")
                            else:
                                gross = hint_int(10000000, 200000000, f"pmongrossfmm|{minute}|{i}")
                                net = hint_int(-100000000, 100000000, f"pmonnetfmm|{minute}|{i}")
                                symbols = hint_int(5, 80, f"pmonsymfmm|{minute}|{i}")
                        bound["gross_usd"] = gross
                        bound["net_usd"] = net
                        bound["symbols"] = symbols
                    elif ref == "pmon.limit_breach":
                        minute = int((t - BASE_TIME).total_seconds() // 60)
                        bound["acct"] = "33"
                        if state == "n":
                            bound["gross_usd"] = hint_int(2100000, 15000000, f"breachn|{minute}|{i}")
                        else:
                            bound["gross_usd"] = hint_int(200000000, 4000000000, f"breachf|{minute}|{i}")
                        bound["limit_usd"] = 2000000
                        bound["action"] = "notify_only"
                    elif ref == "nyse_venue.volume_alert":
                        minute = int((t - BASE_TIME).total_seconds() // 60)
                        bound["member"] = "KNIGHT"
                        bound["symbols_affected"] = hint_int(5, 200, f"nyse_sym|{minute}|{i}")
                        if state == "n":
                            bound["volume_mult"] = float(f"{hint_float(1.0, 2.0, f'nyse_vm_n|{minute}|{i}'):.3f}")
                        else:
                            bound["volume_mult"] = float(f"{hint_float(3.0, 10.0, f'nyse_vm_f|{minute}|{i}'):.3f}")
                        bound["action"] = hint_choice(["monitoring", "called_member"], f"nyse_act|{minute}|{i}")
                    elif ref == "order_gateway.conn_pool_warn":
                        minute = int((t - BASE_TIME).total_seconds() // 60)
                        if state == "n":
                            bound["active_conns"] = hint_int(40, 120, f"gwconnsn|{minute}|{i}")
                            bound["pending"] = hint_int(0, 5, f"gwpendingn|{minute}|{i}")
                        else:
                            bound["active_conns"] = hint_int(80, 260, f"gwconnsf|{minute}|{i}")
                            bound["pending"] = hint_int(0, 25, f"gwpendingf|{minute}|{i}")

                    msg = render_log_message(ref, state, bound, f"bgmsg|{state}|{start_min}-{end_min}|{ref}|{i}")
                    add_row(rows, t, tpl["lvl"], msg, "", service, host)
            else:
                for host in hosts:
                    expected = effective_per_min * duration_min
                    cnt = allocate_count(expected, f"bg|{state}|{start_min}-{end_min}|{ref}|{host}")
                    times = evenly_spaced_times(start_ts, end_ts, cnt, f"bgtime|{state}|{start_min}-{end_min}|{ref}|{host}")
                    for i, t in enumerate(times):
                        bound2: Dict[str, Any] = {}
                        if ref == "smars_router.heartbeat":
                            bound2["host"] = host
                            bound2["build"] = smars_build_for_host_at_time(host, t, state)
                            qd, cpu = gen_smars_heartbeat_values(host, t, state)
                            bound2["queue_depth"] = qd
                            bound2["cpu_pct"] = cpu
                        elif ref == "smars_router.child_order_stats":
                            bound2["host"] = host
                            child, execs, unmatched = gen_smars_stats_values(host, t, state)
                            bound2["child_orders_sent"] = child
                            bound2["exec_reports_rcvd"] = execs
                            bound2["unmatched_execs"] = unmatched

                        msg = render_log_message(ref, state, bound2, f"bgmsg|{state}|{start_min}-{end_min}|{ref}|{host}|{i}")
                        add_row(rows, t, tpl["lvl"], msg, "", service, host)


def simulate_one_shots(rows: List[Dict[str, Any]]) -> None:
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        base_ts = BASE_TIME + timedelta(minutes=at_min)
        for os in e.get("one_shots", []):
            ref = os["ref"]
            cnt = int(os["count"])
            allowed_hosts = list(os.get("hosts", []))
            cid, _ = parse_ref(ref)
            tpl = LOGS[ref]
            comp = get_component(cid)
            service = comp.get("svc", "") or ""
            times = evenly_spaced_times(base_ts, base_ts + timedelta(minutes=1), cnt, f"oneshot|{at_min}|{ref}")
            for i, t in enumerate(times):
                host = ""
                if comp.get("hosts"):
                    if allowed_hosts:
                        host = allowed_hosts[i % len(allowed_hosts)]
                    else:
                        host = comp["hosts"][i % len(comp["hosts"])]
                bound: Dict[str, Any] = {}
                if ref == "bnet_alerting.email_powerpeg_disabled":
                    smars_hosts = get_component("smars_router")["hosts"]
                    if hfloat(f"bnet_smars8|{at_min}|{i}") < 0.7:
                        bound["host"] = "smars8"
                    else:
                        bound["host"] = smars_hosts[int(hfloat(f"bnet_h|{at_min}|{i}") * len(smars_hosts)) % len(smars_hosts)]
                if ref == "smars_router.shutdown_host":
                    bound["host"] = host
                    bound["reason"] = "incident_mitigation"
                msg = render_log_message(ref, "f", bound, f"osmsg|{at_min}|{ref}|{i}")
                add_row(rows, t, tpl["lvl"], msg, "", service, host)


def simulate_flows(rows: List[Dict[str, Any]]) -> None:
    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    for flow_id in sorted(SYSTEM["flows"]["n"].keys()):
        flow_def = SYSTEM["flows"]["n"][flow_id]
        duration = nend - nstart
        expected = flow_def["rpm"] * float(duration)
        cnt = allocate_count(expected, f"flow|n|{flow_id}|{nstart}-{nend}")
        times = evenly_spaced_times(BASE_TIME + timedelta(minutes=nstart), BASE_TIME + timedelta(minutes=nend), cnt, f"flowtime|n|{flow_id}|{nstart}-{nend}")
        for i, t in enumerate(times):
            simulate_flow_instance(rows, "n", flow_id, flow_def, t, i, None)

    for interval in FAILURE_INTERVALS:
        start_ts = BASE_TIME + timedelta(minutes=interval.start_min)
        end_ts = BASE_TIME + timedelta(minutes=interval.end_min)
        duration_min = (end_ts - start_ts).total_seconds() / 60.0
        for flow_id in sorted(SYSTEM["flows"]["f"].keys()):
            flow_def = SYSTEM["flows"]["f"][flow_id]
            mult = float(interval.rate_mult.get(flow_id, 1.0))
            rpm_eff = float(flow_def["rpm"]) * mult
            expected = rpm_eff * duration_min
            cnt = allocate_count(expected, f"flow|f|{flow_id}|{interval.start_min}-{interval.end_min}")
            times = evenly_spaced_times(start_ts, end_ts, cnt, f"flowtime|f|{flow_id}|{interval.start_min}-{interval.end_min}")
            for i, t in enumerate(times):
                simulate_flow_instance(rows, "f", flow_id, flow_def, t, i, interval)


def simulate_background(rows: List[Dict[str, Any]]) -> None:
    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background_for_interval(rows, "n", nstart, nend, None)
    for interval in FAILURE_INTERVALS:
        simulate_background_for_interval(rows, "f", interval.start_min, interval.end_min, interval)


def main() -> None:
    rows: List[Dict[str, Any]] = []
    simulate_background(rows)
    simulate_flows(rows)
    simulate_one_shots(rows)

    df = pd.DataFrame(rows)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp_dt", kind="mergesort").drop(columns=["timestamp_dt"])
    df["timestamp"] = df["timestamp"].apply(iso_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
