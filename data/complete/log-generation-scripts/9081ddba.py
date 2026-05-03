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
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "smars_equities_router"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["broker_gateway"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "broker_gateway": {
            "svc": "broker-gateway",
            "hosts": ["bgw-01", "bgw-02"],
            "logs": {
                "parent_order_recv": {
                    "lvl": "INFO",
                    "msg": "recv parent order id={order_id} acct={acct} sym={sym} qty={qty} rlp={rlp}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "acct": {"k": "ch", "v": ["brkA", "brkB", "brkC", "brkD"]},
                        "sym": {"k": "ch", "v": ["AAPL", "MSFT", "GE", "F", "BAC", "XOM", "CSCO", "INTC"]},
                        "qty": {"k": "i", "v": [100, 50000]},
                        "rlp": {"k": "ch", "v": ["Y", "N"]},
                    },
                },
                "route_to_smars": {
                    "lvl": "INFO",
                    "msg": "routing parent order id={order_id} to smars engine={engine} req={req_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [1, 8]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "smars_retry": {
                    "lvl": "WARN",
                    "msg": "retrying send to smars req={req_id} attempt={attempt} reason={reason}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["timeout", "conn_reset"]},
                    },
                },
                "parent_order_resp_ok": {
                    "lvl": "INFO",
                    "msg": "parent order id={order_id} accepted latency_ms={lat_ms}",
                    "vars": {"order_id": {"k": "uuid", "v": None}, "lat_ms": {"k": "i", "v": [2, 400]}},
                },
                "parent_order_resp_err": {
                    "lvl": "ERROR",
                    "msg": "parent order id={order_id} rejected status={status} detail={detail} latency_ms={lat_ms}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "status": {"k": "ch", "v": [503, 429]},
                        "detail": {"k": "ch", "v": ["router_disabled", "smars_unavailable"]},
                        "lat_ms": {"k": "i", "v": [1, 80]},
                    },
                },
                "healthcheck": {
                    "lvl": "INFO",
                    "msg": "healthcheck smars_cluster={cluster} ok={ok}",
                    "vars": {"cluster": {"k": "ch", "v": ["smars"]}, "ok": {"k": "ch", "v": ["true", "false"]}},
                },
            },
            "beh": {
                "n": [{"id": "healthcheck", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "healthcheck", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        "smars_router": {
            "svc": "smars",
            "hosts": ["smars-01", "smars-02", "smars-03", "smars-04", "smars-05", "smars-06", "smars-07", "smars-08"],
            "logs": {
                "accept_order_rlp": {
                    "lvl": "INFO",
                    "msg": "accepted parent order id={order_id} engine={engine} mode=rlp release={release}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [1, 7]},
                        "release": {"k": "ch", "v": ["smars-rlp-2.1.0"]},
                    },
                },
                "accept_order_powerpeg": {
                    "lvl": "INFO",
                    "msg": "accepted parent order id={order_id} engine={engine} mode=powerpeg release={release}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [8, 8]},
                        "release": {"k": "ch", "v": ["smars-legacy-1.8.3"]},
                    },
                },
                "accept_order_powerpeg_legacy": {
                    "lvl": "INFO",
                    "msg": "accepted parent order id={order_id} engine={engine} mode=powerpeg release={release}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [1, 8]},
                        "release": {"k": "ch", "v": ["smars-legacy-1.8.3"]},
                    },
                },
                "child_plan": {
                    "lvl": "DEBUG",
                    "msg": "planned child orders id={order_id} slices={slices} slice_qty={slice_qty}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "slices": {"k": "i", "v": [1, 60]},
                        "slice_qty": {"k": "i", "v": [10, 3000]},
                    },
                },
                "powerpeg_warn": {
                    "lvl": "WARN",
                    "msg": "powerpeg active id={order_id} engine={engine} sent_child_orders={sent} elapsed_ms={elapsed}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [8, 8]},
                        "sent": {"k": "i", "v": [5000, 250000]},
                        "elapsed": {"k": "i", "v": [50, 5000]},
                    },
                },
                "powerpeg_warn_legacy": {
                    "lvl": "WARN",
                    "msg": "powerpeg active id={order_id} engine={engine} sent_child_orders={sent} elapsed_ms={elapsed}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "engine": {"k": "i", "v": [1, 8]},
                        "sent": {"k": "i", "v": [5000, 250000]},
                        "elapsed": {"k": "i", "v": [50, 5000]},
                    },
                },
                "minute_stats": {
                    "lvl": "INFO",
                    "msg": "minute_stats engine={engine} parent_orders={parents} child_orders_sent={child} cpu_pct={cpu} powerpeg_pct={pp_pct}",
                    "vars": {"engine": {"k": "i", "v": [1, 8]}},
                    "state_vars": {
                        "n": {
                            "parents": {"k": "i", "v": [5, 35]},
                            "child": {"k": "i", "v": [0, 3000]},
                            "cpu": {"k": "i", "v": [10, 70]},
                            "pp_pct": {"k": "i", "v": [0, 0]},
                        },
                        "f": {
                            "parents": {"k": "i", "v": [0, 60]},
                            "child": {"k": "i", "v": [0, 4000000]},
                            "cpu": {"k": "i", "v": [10, 100]},
                            "pp_pct": {"k": "i", "v": [0, 100]},
                        },
                    },
                },
            },
            "beh": {"n": [{"id": "minute_stats", "per_min": 1.0, "scope": "per_host"}], "f": [{"id": "minute_stats", "per_min": 1.0, "scope": "per_host"}]},
        },
        "exchange_gateway": {
            "svc": "exchange-gw",
            "hosts": ["exgw-01", "exgw-02"],
            "logs": {
                "order_new_ack": {
                    "lvl": "INFO",
                    "msg": "exchange ack order id={order_id} venue={venue} exch_id={exch_id}",
                    "vars": {
                        "order_id": {"k": "uuid", "v": None},
                        "venue": {"k": "ch", "v": ["NYSE", "NASDAQ"]},
                        "exch_id": {"k": "hex", "v": 12},
                    },
                },
                "exec_summary_minute": {
                    "lvl": "INFO",
                    "msg": "exec_summary txns={txns} shares={shares} symbols={symbols} venues={venues}",
                    "vars": {"venues": {"k": "ch", "v": ["NYSE+NASDAQ"]}},
                    "state_vars": {
                        "n": {"txns": {"k": "i", "v": [100, 20000]}, "shares": {"k": "i", "v": [10000, 5000000]}, "symbols": {"k": "i", "v": [10, 80]}},
                        "f": {"txns": {"k": "i", "v": [50000, 500000]}, "shares": {"k": "i", "v": [5000000, 200000000]}, "symbols": {"k": "i", "v": [80, 200]}},
                    },
                },
                "exec_summary_spike": {
                    "lvl": "WARN",
                    "msg": "exec_spike txns={txns} shares={shares} top_symbol={sym} price_move_pct={move_pct}",
                    "vars": {"txns": {"k": "i", "v": [50000, 500000]}, "shares": {"k": "i", "v": [5000000, 200000000]}, "sym": {"k": "ch", "v": ["AAPL", "MSFT", "GE", "F", "BAC"]}, "move_pct": {"k": "f", "v": [1.0, 15.0]}},
                },
                "throttling_warn": {"lvl": "WARN", "msg": "exchange backpressure venue={venue} rejects={rejects} queue_depth={qdepth}", "vars": {"venue": {"k": "ch", "v": ["NYSE", "NASDAQ"]}, "rejects": {"k": "i", "v": [0, 50000]}, "qdepth": {"k": "i", "v": [0, 200000]}}},
            },
            "beh": {
                "n": [{"id": "exec_summary_minute", "per_min": 1.0, "scope": "global"}, {"id": "throttling_warn", "per_min": 0.02, "scope": "global"}],
                "f": [{"id": "exec_summary_minute", "per_min": 1.0, "scope": "global"}, {"id": "exec_summary_spike", "per_min": 1.0, "scope": "global"}, {"id": "throttling_warn", "per_min": 0.3, "scope": "global"}],
            },
        },
        "risk_monitor": {
            "svc": "risk-monitor",
            "hosts": ["risk-01"],
            "logs": {
                "exposure_snapshot": {
                    "lvl": "INFO",
                    "msg": "exposure net_long_usd={net_long} net_short_usd={net_short} gross_usd={gross}",
                    "vars": {},
                    "state_vars": {
                        "n": {"net_long": {"k": "i", "v": [0, 200000000]}, "net_short": {"k": "i", "v": [0, 200000000]}, "gross": {"k": "i", "v": [0, 400000000]}},
                        "f": {"net_long": {"k": "i", "v": [200000000, 4000000000]}, "net_short": {"k": "i", "v": [200000000, 4000000000]}, "gross": {"k": "i", "v": [400000000, 8000000000]}},
                    },
                },
                "limit_breach": {
                    "lvl": "ERROR",
                    "msg": "risk limit breach net_long_usd={net_long} net_short_usd={net_short} limit_usd={limit}",
                    "vars": {"net_long": {"k": "i", "v": [500000000, 4000000000]}, "net_short": {"k": "i", "v": [500000000, 4000000000]}, "limit": {"k": "i", "v": [250000000, 250000000]}},
                },
            },
            "beh": {"n": [{"id": "exposure_snapshot", "per_min": 1.0, "scope": "global"}], "f": [{"id": "exposure_snapshot", "per_min": 1.0, "scope": "global"}, {"id": "limit_breach", "per_min": 0.2, "scope": "global"}]},
        },
        "email_notifier": {
            "svc": "mailer",
            "hosts": ["mail-01"],
            "logs": {
                "smars_diag_email": {"lvl": "INFO", "msg": "sent email subject='{subject}' to={to} ref={ref_id}", "vars": {"subject": {"k": "ch", "v": ["SMARS diag: Power Peg disabled"]}, "to": {"k": "ch", "v": ["etg-ops@knight.example", "smars-dev@knight.example"]}, "ref_id": {"k": "hex", "v": 10}}},
                "smtp_queue_warn": {"lvl": "WARN", "msg": "smtp queue depth={depth} oldest_age_s={age_s}", "vars": {"depth": {"k": "i", "v": [0, 5000]}, "age_s": {"k": "i", "v": [0, 600]}}},
            },
            "beh": {
                "n": [{"id": "smars_diag_email", "per_min": 4.8, "scope": "global"}, {"id": "smtp_queue_warn", "per_min": 0.05, "scope": "global"}],
                "f": [{"id": "smars_diag_email", "per_min": 0.5, "scope": "global"}, {"id": "smtp_queue_warn", "per_min": 0.1, "scope": "global"}],
            },
        },
        "config_auditor": {
            "svc": "config-auditor",
            "hosts": ["cfg-01"],
            "logs": {"deploy_audit_partial": {"lvl": "INFO", "msg": "deploy audit smars expected_hosts=8 updated_hosts=7 stale_hosts=1", "vars": {}}, "deploy_audit_rollback": {"lvl": "WARN", "msg": "deploy audit smars expected_hosts=8 updated_hosts=0 stale_hosts=8 (post-rollback)", "vars": {}}},
            "beh": {"n": [{"id": "deploy_audit_partial", "per_min": 0.2, "scope": "global"}], "f": [{"id": "deploy_audit_partial", "per_min": 0.2, "scope": "global"}, {"id": "deploy_audit_rollback", "per_min": 0.2, "scope": "global"}]},
        },
        "ops_console": {
            "svc": "ops-console",
            "hosts": ["ops-01"],
            "logs": {
                "rollback_initiated": {"lvl": "WARN", "msg": "operator initiated rollback of smars release={release} reason={reason}", "vars": {"release": {"k": "ch", "v": ["rlp-2012-08-01"]}, "reason": {"k": "ch", "v": ["unexpected_orders", "volume_anomaly"]}}},
                "disable_routing": {"lvl": "CRITICAL", "msg": "operator disabled order routing at broker gateway reason={reason}", "vars": {"reason": {"k": "ch", "v": ["runaway_orders", "risk_limit_breach"]}}},
                "triage_note": {"lvl": "INFO", "msg": "triage note: {note}", "vars": {"note": {"k": "ch", "v": ["reviewing smars logs for repurposed flag usage", "checking deployment consistency across nodes"]}}},
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": [
            {
                "id": "flow_parent_order_rlp",
                "rpm": 150.0,
                "emit": ["broker_gateway.parent_order_recv", "broker_gateway.route_to_smars", "smars_router.accept_order_rlp", "smars_router.child_plan", "exchange_gateway.order_new_ack", "broker_gateway.parent_order_resp_ok"],
                "latency_ms": [[1, 3], [1, 3], [1, 2], [1, 3], [1, 5], [1, 3]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.05, "emit_per_retry": ["broker_gateway.smars_retry"], "backoff_ms": [[20, 80], [40, 160]]},
                "trace": True,
            }
        ],
        "f": [
            {
                "id": "flow_parent_order_rlp_ok",
                "rpm": 130.0,
                "emit": ["broker_gateway.parent_order_recv", "broker_gateway.route_to_smars", "smars_router.accept_order_rlp", "smars_router.child_plan", "exchange_gateway.order_new_ack", "broker_gateway.parent_order_resp_ok"],
                "latency_ms": [[2, 6], [2, 8], [2, 6], [2, 10], [2, 12], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.6, "emit_per_retry": ["broker_gateway.smars_retry"], "backoff_ms": [[50, 250], [100, 400]]},
                "trace": True,
            },
            {
                "id": "flow_parent_order_powerpeg",
                "rpm": 12.0,
                "emit": ["broker_gateway.parent_order_recv", "broker_gateway.route_to_smars", "smars_router.accept_order_powerpeg", "smars_router.powerpeg_warn", "exchange_gateway.order_new_ack", "broker_gateway.parent_order_resp_ok"],
                "latency_ms": [[2, 6], [2, 10], [3, 12], [3, 20], [2, 10], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.2, "emit_per_retry": ["broker_gateway.smars_retry"], "backoff_ms": [[30, 150], [60, 250]]},
                "trace": True,
            },
            {
                "id": "flow_parent_order_powerpeg_widespread",
                "rpm": 12.0,
                "emit": ["broker_gateway.parent_order_recv", "broker_gateway.route_to_smars", "smars_router.accept_order_powerpeg_legacy", "smars_router.powerpeg_warn_legacy", "exchange_gateway.order_new_ack", "broker_gateway.parent_order_resp_ok"],
                "latency_ms": [[2, 6], [2, 10], [3, 12], [3, 20], [2, 10], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.2, "emit_per_retry": ["broker_gateway.smars_retry"], "backoff_ms": [[30, 150], [60, 250]]},
                "trace": True,
            },
            {
                "id": "flow_parent_order_rejected",
                "rpm": 150.0,
                "emit": ["broker_gateway.parent_order_recv", "broker_gateway.parent_order_resp_err"],
                "latency_ms": [[1, 3], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "smars_powerpeg_deploy_mismatch"},
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "flow_parent_order_rejected": 0.0,
                        "flow_parent_order_powerpeg_widespread": 0.0,
                        "exchange_gateway.exec_summary_spike": 0.0,
                        "risk_monitor.limit_breach": 0.0,
                        "config_auditor.deploy_audit_rollback": 0.0,
                    },
                    "latency_multipliers": {"flow_parent_order_rlp_ok": {"p50": 1.2, "p95": 1.5}, "flow_parent_order_powerpeg": {"p50": 1.2, "p95": 1.6}},
                    "one_shots": [{"ref": "ops_console.triage_note", "count": 1, "hosts": ["ops-01"]}],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {
                        "flow_parent_order_powerpeg": 0.0,
                        "flow_parent_order_powerpeg_widespread": 8.0,
                        "flow_parent_order_rlp_ok": 0.0,
                        "exchange_gateway.exec_summary_spike": 1.0,
                        "exchange_gateway.throttling_warn": 2.5,
                        "risk_monitor.limit_breach": 1.0,
                        "config_auditor.deploy_audit_partial": 0.0,
                        "config_auditor.deploy_audit_rollback": 1.0,
                    },
                    "latency_multipliers": {"flow_parent_order_powerpeg_widespread": {"p50": 2.5, "p95": 3.5}},
                    "one_shots": [{"ref": "ops_console.rollback_initiated", "count": 1, "hosts": ["ops-01"]}],
                },
                {
                    "order": 3,
                    "at_min": 55,
                    "rate_multipliers": {
                        "flow_parent_order_powerpeg": 0.0,
                        "flow_parent_order_powerpeg_widespread": 0.0,
                        "flow_parent_order_rlp_ok": 0.0,
                        "flow_parent_order_rejected": 1.0,
                        "exchange_gateway.exec_summary_spike": 0.0,
                        "exchange_gateway.throttling_warn": 0.5,
                        "email_notifier.smars_diag_email": 0.2,
                    },
                    "latency_multipliers": {"flow_parent_order_rejected": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [{"ref": "ops_console.disable_routing", "count": 1, "hosts": ["ops-01"]}],
                },
            ]
        }
    },
}

# -----------------------------
# Deterministic helpers
# -----------------------------
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

ND = NormalDist()


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def h32(s: str) -> int:
    return int(md5_hex(f"{SEED}|{s}")[:8], 16)


def u01(s: str) -> float:
    return (h32(s) + 1) / 2**32


def iso_ms(dt: datetime) -> str:
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def alloc_int(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    return base + (1 if u01(f"alloc|{key}") < frac else 0)


def norm_ppf(p: float) -> float:
    return ND.inv_cdf(p)


def sample_lognormal_ms(p50: float, p95: float, u: float, cap: Optional[float] = None) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = norm_ppf(clamp(u, 1e-6, 1 - 1e-6))
    x = math.exp(mu + sigma * z)
    if cap is not None:
        x = min(x, cap)
    return x


def choose_from(values: List[Any], key: str) -> Any:
    idx = h32(key) % len(values)
    return values[idx]


def gen_hex(n: int, key: str) -> str:
    return md5_hex(f"hex|{key}")[:n]


def gen_uuid(key: str) -> str:
    b = hashlib.md5(f"uuid|{SEED}|{key}".encode("utf-8")).digest()
    u = uuid.UUID(bytes=b, version=4)
    return str(u)


def gen_int(lo: int, hi: int, key: str) -> int:
    if lo == hi:
        return int(lo)
    r = h32(key) % (hi - lo + 1)
    return int(lo + r)


def gen_float(lo: float, hi: float, key: str, decimals: int = 1) -> float:
    if lo == hi:
        return float(lo)
    x = lo + (hi - lo) * u01(f"f|{key}")
    x = float(clamp(x, lo, hi))
    return round(x, decimals)


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def get_template(ref: str) -> Dict[str, Any]:
    comp, log_id = parse_ref(ref)
    return SYSTEM["components"][comp]["logs"][log_id]


def get_int_domain(ref: str, var_name: str, state: str) -> Optional[Tuple[int, int]]:
    tmpl = get_template(ref)
    spec = None
    if var_name in (tmpl.get("vars") or {}):
        spec = tmpl["vars"][var_name]
    else:
        spec = ((tmpl.get("state_vars") or {}).get(state) or {}).get(var_name)
    if not spec or spec.get("k") != "i":
        return None
    lo, hi = spec["v"]
    return int(lo), int(hi)


def default_host_for_component(comp_id: str) -> str:
    hosts = SYSTEM["components"][comp_id]["hosts"]
    return hosts[0] if hosts else ""


def engine_from_smars_host(host: str) -> int:
    try:
        return int(host.split("-")[-1])
    except Exception:
        return 1


def smars_host_for_engine(engine: int) -> str:
    engine = int(clamp(engine, 1, 8))
    return f"smars-{engine:02d}"


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    start_dt: datetime
    end_dt: datetime
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Dict[str, float]]


# -----------------------------
# Control intervals from scenario
# -----------------------------
BASE_TIME = datetime(2026, 4, 1, 13, 0, 0, tzinfo=timezone.utc)


def dt_at_minute(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(minute))


def build_failure_intervals() -> List[Interval]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))

    boundaries = [f_start]
    for e in events:
        if f_start <= e["at_min"] <= f_end and e["at_min"] not in boundaries:
            boundaries.append(e["at_min"])
    if f_end not in boundaries:
        boundaries.append(f_end)
    boundaries = sorted(boundaries)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    intervals: List[Interval] = []
    event_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        event_by_min.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]

        for e in event_by_min.get(start, []):
            for k, v in e.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fid, mult in e.get("latency_multipliers", {}).items():
                active_lat[fid] = {"p50": float(mult["p50"]), "p95": float(mult["p95"])}

        intervals.append(
            Interval(
                state="f",
                start_min=int(start),
                end_min=int(end),
                start_dt=dt_at_minute(start),
                end_dt=dt_at_minute(end),
                rate_mult=dict(active_rate),
                lat_mult=dict(active_lat),
            )
        )
    return intervals


def build_normal_interval() -> Interval:
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    return Interval(state="n", start_min=int(n_start), end_min=int(n_end), start_dt=dt_at_minute(n_start), end_dt=dt_at_minute(n_end), rate_mult={}, lat_mult={})


NORMAL_INTERVAL = build_normal_interval()
FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Scheduling helpers
# -----------------------------


def evenly_spaced_times(start: datetime, end: datetime, count: int, key: str, max_jitter_ms: int = 200) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end - start).total_seconds()
    if dur_s <= 0:
        return []
    times: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start + timedelta(seconds=dur_s * frac)
        j = (u01(f"jit|{key}|{i}") - 0.5) * 2.0 * max_jitter_ms
        t = t + timedelta(milliseconds=j)
        if t < start:
            t = start + timedelta(milliseconds=(i % 50))
        if t >= end:
            t = end - timedelta(milliseconds=(i % 50) + 1)
        times.append(t)
    return times


def per_minute_times(start_min: int, end_min: int, offset_s: float, key: str, max_jitter_ms: int = 80) -> List[datetime]:
    times: List[datetime] = []
    for m in range(start_min, end_min):
        t = dt_at_minute(m) + timedelta(seconds=float(offset_s))
        j = (u01(f"pmjit|{key}|{m}") - 0.5) * 2.0 * max_jitter_ms
        times.append(t + timedelta(milliseconds=j))
    return times


def effective_multiplier(interval: Interval, source_key: str) -> float:
    if interval.state != "f":
        return 1.0
    return float(interval.rate_mult.get(source_key, 1.0))


def latency_multiplier(interval: Interval, flow_id: str) -> Tuple[float, float]:
    if interval.state != "f":
        return (1.0, 1.0)
    mult = interval.lat_mult.get(flow_id)
    if not mult:
        return (1.0, 1.0)
    return (float(mult.get("p50", 1.0)), float(mult.get("p95", 1.0)))


def attempt_count(expected: float, max_attempts: int, key: str) -> int:
    expected = float(clamp(expected, 1.0, float(max_attempts)))
    lo = int(math.floor(expected))
    hi = int(math.ceil(expected))
    if lo < 1:
        lo = 1
    if hi > max_attempts:
        hi = max_attempts
    if lo == hi:
        return lo
    frac = expected - lo
    return hi if u01(f"attempts|{key}") < frac else lo


# -----------------------------
# Rendering helpers
# -----------------------------


def fill_vars_from_domains(dom: Dict[str, Any], key_prefix: str, state: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    vars_def: Dict[str, Any] = dom.get("vars", {}) or {}
    state_vars_def: Dict[str, Any] = (dom.get("state_vars", {}) or {}).get(state, {}) or {}
    combined: Dict[str, Any] = {}
    combined.update(vars_def)
    combined.update(state_vars_def)

    for name, spec in combined.items():
        k = spec["k"]
        v = spec.get("v", None)
        kval = f"{key_prefix}|{name}"
        if k == "uuid":
            out[name] = gen_uuid(kval)
        elif k == "hex":
            out[name] = gen_hex(int(v), kval)
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            out[name] = gen_int(lo, hi, kval)
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            out[name] = gen_float(lo, hi, kval, decimals=1)
        elif k == "ch":
            out[name] = choose_from(list(v), kval)
        else:
            out[name] = str(v) if v is not None else ""
    return out


def render_message(ref: str, state: str, key_prefix: str, bound: Dict[str, Any]) -> Tuple[str, str]:
    tmpl = get_template(ref)
    vars_generated = fill_vars_from_domains(tmpl, key_prefix, state)
    merged = dict(vars_generated)
    merged.update(bound)
    for k, val in list(merged.items()):
        if isinstance(val, float):
            merged[k] = f"{val:.1f}"
    msg = tmpl["msg"].format(**merged)
    lvl = tmpl["lvl"]
    return lvl, msg


def component_identity(comp_id: str, host: str) -> Tuple[str, str]:
    svc = SYSTEM["components"][comp_id].get("svc") or ""
    return svc, host or ""


# -----------------------------
# Background value shaping
# -----------------------------


def minute_of_dt(dt: datetime) -> int:
    delta = dt - BASE_TIME
    return int(delta.total_seconds() // 60)


def bgw_health_ok(state: str, dt: datetime, host: str) -> str:
    m = minute_of_dt(dt)
    if state == "f" and 30 <= m < 55 and (h32(f"hc|{host}|{m}") % 12 == 0):
        return "false"
    return "true"


def smars_minute_stats(state: str, dt: datetime, engine: int) -> Dict[str, Any]:
    m = minute_of_dt(dt)

    if state == "n":
        parents = int(clamp(10 + (h32(f"p|{engine}|{m}") % 20), 5, 35))
        child = int(clamp(100 + (h32(f"c|{engine}|{m}") % 2000), 0, 3000))
        cpu = int(clamp(20 + (h32(f"cpu|{engine}|{m}") % 40), 10, 70))
        pp_pct = 0
        return {"engine": engine, "parents": parents, "child": child, "cpu": cpu, "pp_pct": pp_pct}

    if 20 <= m < 30:
        if engine == 8:
            parents = int(clamp(18 + (h32(f"p8|{m}") % 30), 0, 60))
            child = int(clamp(1500000 + (h32(f"ch8|{m}") % 2000000), 0, 4000000))
            cpu = int(clamp(85 + (h32(f"cpu8|{m}") % 15), 10, 100))
            pp_pct = int(clamp(80 + (h32(f"pp8|{m}") % 21), 0, 100))
        else:
            parents = int(clamp(8 + (h32(f"pN|{engine}|{m}") % 20), 0, 60))
            child = int(clamp(500 + (h32(f"chN|{engine}|{m}") % 15000), 0, 4000000))
            cpu = int(clamp(40 + (h32(f"cpuN|{engine}|{m}") % 35), 10, 100))
            pp_pct = int(clamp(h32(f"ppN|{engine}|{m}") % 5, 0, 100))
        return {"engine": engine, "parents": parents, "child": child, "cpu": cpu, "pp_pct": pp_pct}

    if 30 <= m < 55:
        parents = int(clamp(6 + (h32(f"pW|{engine}|{m}") % 20), 0, 60))
        child = int(clamp(1000000 + (h32(f"chW|{engine}|{m}") % 3000000), 0, 4000000))
        cpu = int(clamp(80 + (h32(f"cpuW|{engine}|{m}") % 20), 10, 100))
        pp_pct = int(clamp(55 + (h32(f"ppW|{engine}|{m}") % 46), 0, 100))
        return {"engine": engine, "parents": parents, "child": child, "cpu": cpu, "pp_pct": pp_pct}

    parents = int(clamp(h32(f"pD|{engine}|{m}") % 4, 0, 60))
    child = int(clamp(h32(f"chD|{engine}|{m}") % 8000, 0, 4000000))
    cpu = int(clamp(25 + (h32(f"cpuD|{engine}|{m}") % 35), 10, 100))
    pp_pct = int(clamp(h32(f"ppD|{engine}|{m}") % 21, 0, 100))
    return {"engine": engine, "parents": parents, "child": child, "cpu": cpu, "pp_pct": pp_pct}


def exchange_exec_summary(state: str, dt: datetime) -> Dict[str, Any]:
    m = minute_of_dt(dt)
    if state == "n":
        txns = int(clamp(2000 + (h32(f"tx|{m}") % 12000), 100, 20000))
        shares = int(clamp(800000 + (h32(f"sh|{m}") % 3000000), 10000, 5000000))
        symbols = int(clamp(25 + (h32(f"sy|{m}") % 40), 10, 80))
        return {"txns": txns, "shares": shares, "symbols": symbols, "venues": "NYSE+NASDAQ"}

    if 20 <= m < 30:
        txns = int(clamp(90000 + (h32(f"ftx1|{m}") % 120000), 50000, 500000))
        shares = int(clamp(15000000 + (h32(f"fsh1|{m}") % 60000000), 5000000, 200000000))
        symbols = int(clamp(90 + (h32(f"fsy1|{m}") % 60), 80, 200))
    elif 30 <= m < 55:
        txns = int(clamp(250000 + (h32(f"ftx2|{m}") % 200000), 50000, 500000))
        shares = int(clamp(70000000 + (h32(f"fsh2|{m}") % 120000000), 5000000, 200000000))
        symbols = int(clamp(140 + (h32(f"fsy2|{m}") % 61), 80, 200))
    else:
        txns = int(clamp(60000 + (h32(f"ftx3|{m}") % 60000), 50000, 500000))
        shares = int(clamp(9000000 + (h32(f"fsh3|{m}") % 20000000), 5000000, 200000000))
        symbols = int(clamp(85 + (h32(f"fsy3|{m}") % 30), 80, 200))
    return {"txns": txns, "shares": shares, "symbols": symbols, "venues": "NYSE+NASDAQ"}


def exchange_spike(dt: datetime) -> Dict[str, Any]:
    m = minute_of_dt(dt)
    txns = int(clamp(250000 + (h32(f"stx|{m}") % 250000), 50000, 500000))
    shares = int(clamp(80000000 + (h32(f"ssh|{m}") % 120000000), 5000000, 200000000))
    sym = choose_from(["AAPL", "MSFT", "GE", "F", "BAC"], f"ssym|{m}")
    move = gen_float(1.0, 15.0, f"smove|{m}", decimals=1)
    return {"txns": txns, "shares": shares, "sym": sym, "move_pct": move}


def exchange_throttle(dt: datetime, intensity: float) -> Dict[str, Any]:
    m = minute_of_dt(dt)
    venue = choose_from(["NYSE", "NASDAQ"], f"venue|{m}|{intensity}")
    base_rejects = int(500 + (h32(f"rej|{m}|{venue}") % 20000))
    base_q = int(2000 + (h32(f"qd|{m}|{venue}") % 80000))
    rejects = int(clamp(base_rejects * intensity, 0, 50000))
    qdepth = int(clamp(base_q * intensity, 0, 200000))
    return {"venue": venue, "rejects": rejects, "qdepth": qdepth}


def risk_snapshot(state: str, dt: datetime) -> Dict[str, Any]:
    m = minute_of_dt(dt)
    if state == "n":
        net_long = int(clamp((h32(f"nl|{m}") % 200000000), 0, 200000000))
        net_short = int(clamp((h32(f"ns|{m}") % 200000000), 0, 200000000))
        gross = int(clamp(net_long + net_short + (h32(f"gr|{m}") % 50000000), 0, 400000000))
        return {"net_long": net_long, "net_short": net_short, "gross": gross}

    t = clamp((m - 20) / 40.0, 0.0, 1.0)
    nl = int(300_000_000 + t * 3_200_000_000 + (h32(f"fnl|{m}") % 50_000_000))
    ns = int(280_000_000 + t * 3_000_000_000 + (h32(f"fns|{m}") % 50_000_000))
    gross = int(clamp(nl + ns + 400_000_000 + (h32(f"fgr|{m}") % 200_000_000), 400_000_000, 8_000_000_000))
    nl = int(clamp(nl, 200_000_000, 4_000_000_000))
    ns = int(clamp(ns, 200_000_000, 4_000_000_000))
    return {"net_long": nl, "net_short": ns, "gross": gross}


def risk_breach(dt: datetime) -> Dict[str, Any]:
    m = minute_of_dt(dt)
    net_long = int(clamp(800_000_000 + (h32(f"bnl|{m}") % 2_800_000_000), 500_000_000, 4_000_000_000))
    net_short = int(clamp(750_000_000 + (h32(f"bns|{m}") % 2_800_000_000), 500_000_000, 4_000_000_000))
    return {"net_long": net_long, "net_short": net_short, "limit": 250_000_000}


# -----------------------------
# Emission
# -----------------------------
ROWS: List[Dict[str, Any]] = []


def emit_row(dt: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    ROWS.append({"timestamp": iso_ms(dt), "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def emit_background_for_interval(interval: Interval) -> None:
    state = interval.state
    start_min = interval.start_min
    end_min = interval.end_min
    duration_min = max(0, end_min - start_min)

    for comp_id, comp in SYSTEM["components"].items():
        for beh in comp.get("beh", {}).get(state, []):
            log_id = beh["id"]
            per_min = float(beh["per_min"])
            scope = beh.get("scope", "per_host")
            source_key = f"{comp_id}.{log_id}"
            mult = effective_multiplier(interval, source_key)
            rate = per_min * mult
            expected = rate * duration_min

            if scope == "per_host":
                for host in comp.get("hosts", []):
                    count = alloc_int(expected, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{source_key}|{host}")
                    if count <= 0:
                        continue

                    if per_min == 1.0 and mult == 1.0 and count == duration_min and duration_min > 0:
                        offset_s = (h32(f"off|{source_key}|{host}") % 900) / 100.0
                        times = per_minute_times(start_min, end_min, offset_s, f"{source_key}|{host}")
                    else:
                        times = evenly_spaced_times(interval.start_dt, interval.end_dt, count, f"bg|{source_key}|{host}")

                    for i, t in enumerate(times):
                        ref = f"{comp_id}.{log_id}"
                        bound: Dict[str, Any] = {}
                        key_prefix = f"bg|{source_key}|{host}|{interval.start_min}-{interval.end_min}|{i}"

                        if ref == "broker_gateway.healthcheck":
                            bound["cluster"] = "smars"
                            bound["ok"] = bgw_health_ok(state, t, host)
                        elif ref == "smars_router.minute_stats":
                            engine = engine_from_smars_host(host)
                            bound.update(smars_minute_stats(state, t, engine))
                        elif ref == "exchange_gateway.exec_summary_minute":
                            bound.update(exchange_exec_summary(state, t))
                        elif ref == "exchange_gateway.exec_summary_spike":
                            bound.update(exchange_spike(t))
                        elif ref == "exchange_gateway.throttling_warn":
                            intensity = clamp(1.0 * mult, 0.5, 5.0)
                            bound.update(exchange_throttle(t, intensity))
                        elif ref == "risk_monitor.exposure_snapshot":
                            bound.update(risk_snapshot(state, t))
                        elif ref == "risk_monitor.limit_breach":
                            bound.update(risk_breach(t))

                        lvl, msg = render_message(ref, state, key_prefix, bound)
                        svc, _ = component_identity(comp_id, host)
                        emit_row(t, lvl, msg, "", svc, host)
            else:
                count = alloc_int(expected, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{source_key}|global")
                if count <= 0:
                    continue

                host = default_host_for_component(comp_id)

                if per_min == 1.0 and mult == 1.0 and count == duration_min and duration_min > 0:
                    offset_s = (h32(f"off|{source_key}|global") % 900) / 100.0
                    times = per_minute_times(start_min, end_min, offset_s, f"{source_key}|global")
                else:
                    times = evenly_spaced_times(interval.start_dt, interval.end_dt, count, f"bg|{source_key}|global")

                for i, t in enumerate(times):
                    ref = f"{comp_id}.{log_id}"
                    bound: Dict[str, Any] = {}
                    key_prefix = f"bg|{source_key}|global|{interval.start_min}-{interval.end_min}|{i}"

                    if ref == "exchange_gateway.exec_summary_minute":
                        bound.update(exchange_exec_summary(state, t))
                    elif ref == "exchange_gateway.exec_summary_spike":
                        bound.update(exchange_spike(t))
                    elif ref == "exchange_gateway.throttling_warn":
                        intensity = clamp(1.0 * mult, 0.5, 5.0)
                        bound.update(exchange_throttle(t, intensity))
                    elif ref == "risk_monitor.exposure_snapshot":
                        bound.update(risk_snapshot(state, t))
                    elif ref == "risk_monitor.limit_breach":
                        bound.update(risk_breach(t))

                    lvl, msg = render_message(ref, state, key_prefix, bound)
                    svc, _ = component_identity(comp_id, host)
                    emit_row(t, lvl, msg, "", svc, host)


def emit_one_shots() -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    for e in events:
        at = int(e["at_min"])
        base = dt_at_minute(at)
        for ospec in e.get("one_shots", []) or []:
            ref = ospec["ref"]
            count = int(ospec["count"])
            hosts = ospec.get("hosts") or []
            comp_id, _ = parse_ref(ref)
            state = "f"
            for i in range(count):
                host = hosts[i % len(hosts)] if hosts else default_host_for_component(comp_id)
                jitter_s = (h32(f"oneshot|{ref}|{at}|{i}") % 25) + (h32(f"oneshotms|{ref}|{at}|{i}") % 1000) / 1000.0
                t = base + timedelta(seconds=float(jitter_s))
                lvl, msg = render_message(ref, state, f"oneshot|{ref}|{at}|{i}", {})
                svc, _ = component_identity(comp_id, host)
                emit_row(t, lvl, msg, "", svc, host)


def flow_defs_by_state(state: str) -> List[Dict[str, Any]]:
    return SYSTEM["flows"][state]


def effective_flow_rpm(interval: Interval, flow_id: str, base_rpm: float) -> float:
    if interval.state != "f":
        return base_rpm
    return base_rpm * float(interval.rate_mult.get(flow_id, 1.0))


@dataclass
class PlannedEvent:
    ref: str
    comp_id: str
    host: str
    kind: str  # "emit" or "retry"
    attempt: int
    # delay from previous planned event (ms); for i==0 it's from chain start
    delay_ms: float
    # constraints on delay_ms when adjusting end-to-end response latency
    min_delay_ms: float
    max_delay_ms: float
    fixed_delay: bool
    # retry-only bound fields
    retry_attempt: int = 0
    retry_reason: str = ""


def sample_latency_delay_ms(ref: str, p50: float, p95: float, lm50: float, lm95: float, key: str) -> float:
    # Latency hints are in ms. Apply multipliers at the p50/p95 level.
    p50_s = float(p50) * float(lm50)
    p95_s = float(p95) * float(lm95)

    u = u01(f"latU|{key}|{ref}")
    if ref in ("smars_router.powerpeg_warn", "smars_router.powerpeg_warn_legacy"):
        u = 0.97 + 0.025 * u
    else:
        u = 0.55 + 0.35 * u

    cap = 3.0 * p95_s
    d_ms = sample_lognormal_ms(p50_s, p95_s, u, cap=cap)
    return max(1.0, float(d_ms))


def sample_backoff_ms(backoff_pair: List[float], key: str) -> float:
    b50, b95 = float(backoff_pair[0]), float(backoff_pair[1])
    bu = 0.60 + 0.35 * u01(f"boU|{key}")
    bcap = 3.0 * b95
    b_ms = sample_lognormal_ms(b50, b95, bu, cap=bcap)
    return max(1.0, float(b_ms))


def adjust_delays_to_target_sum(delays: List[float], mins: List[float], maxs: List[float], fixed: List[bool], idxs: List[int], target: float, key: str) -> None:
    # Adjust delays at indices in idxs so sum(delays[idxs]) == target, respecting mins/maxs/fixed.
    # Works in-place. Deterministic order derived from key.
    if not idxs:
        return

    min_sum = sum(mins[i] for i in idxs)
    max_sum = sum(maxs[i] for i in idxs)
    target = float(clamp(target, min_sum, max_sum))

    cur = sum(delays[i] for i in idxs)
    if cur <= 0:
        # Fall back to mins and then expand to target.
        for i in idxs:
            if not fixed[i]:
                delays[i] = mins[i]
        cur = sum(delays[i] for i in idxs)

    # Scale non-fixed entries; fixed entries stay as-is.
    if cur > 0:
        scale = target / cur
    else:
        scale = 1.0

    for i in idxs:
        if fixed[i]:
            delays[i] = clamp(delays[i], mins[i], maxs[i])
        else:
            delays[i] = clamp(delays[i] * scale, mins[i], maxs[i])

    # Fix residual diff via bounded redistribution.
    cur2 = sum(delays[i] for i in idxs)
    diff = target - cur2
    if abs(diff) < 1e-6:
        return

    adjustable = [i for i in idxs if not fixed[i]]
    if not adjustable:
        return

    order = sorted(adjustable, key=lambda i: h32(f"adj|{key}|{i}"))

    if diff > 0:
        # Need to add time.
        remaining = diff
        for i in order:
            if remaining <= 1e-6:
                break
            room = maxs[i] - delays[i]
            if room <= 0:
                continue
            inc = min(room, remaining)
            delays[i] += inc
            remaining -= inc
    else:
        # Need to remove time.
        remaining = -diff
        for i in order:
            if remaining <= 1e-6:
                break
            room = delays[i] - mins[i]
            if room <= 0:
                continue
            dec = min(room, remaining)
            delays[i] -= dec
            remaining -= dec


def emit_flows_for_interval(interval: Interval) -> None:
    state = interval.state
    duration_min = max(0, interval.end_min - interval.start_min)
    if duration_min <= 0:
        return

    flows = flow_defs_by_state(state)
    for fdef in flows:
        fid = fdef["id"]
        base_rpm = float(fdef["rpm"])
        rpm_eff = effective_flow_rpm(interval, fid, base_rpm)
        expected = rpm_eff * duration_min
        count = alloc_int(expected, f"flowcount|{state}|{interval.start_min}-{interval.end_min}|{fid}")
        if count <= 0:
            continue

        start_times = evenly_spaced_times(
            interval.start_dt,
            interval.end_dt,
            count,
            f"flowstart|{state}|{interval.start_min}-{interval.end_min}|{fid}",
            max_jitter_ms=600,
        )

        lm50, lm95 = latency_multiplier(interval, fid)

        for idx, start_dt in enumerate(start_times):
            chain_key = f"{fid}|{interval.start_min}-{interval.end_min}|{idx}"

            order_id = gen_uuid(f"order|{chain_key}")
            req_id = gen_hex(16, f"req|{chain_key}")
            acct = choose_from(["brkA", "brkB", "brkC", "brkD"], f"acct|{chain_key}")
            sym = choose_from(["AAPL", "MSFT", "GE", "F", "BAC", "XOM", "CSCO", "INTC"], f"sym|{chain_key}")
            qty = gen_int(100, 50000, f"qty|{chain_key}")

            if fid in ("flow_parent_order_powerpeg", "flow_parent_order_powerpeg_widespread", "flow_parent_order_rlp_ok"):
                rlp = "Y"
            else:
                rlp = "Y" if (h32(f"rlp|{chain_key}") % 5 != 0) else "N"

            if fid in ("flow_parent_order_rlp", "flow_parent_order_rlp_ok"):
                engine = gen_int(1, 7, f"eng|{chain_key}")
            elif fid == "flow_parent_order_powerpeg":
                engine = 8
            elif fid == "flow_parent_order_powerpeg_widespread":
                engine = gen_int(1, 8, f"eng|{chain_key}")
            else:
                engine = 0

            bgw_hosts = SYSTEM["components"]["broker_gateway"]["hosts"]
            exgw_hosts = SYSTEM["components"]["exchange_gateway"]["hosts"]
            bgw_host = bgw_hosts[h32(f"bgwh|{chain_key}") % len(bgw_hosts)]
            exgw_host = exgw_hosts[h32(f"exh|{chain_key}") % len(exgw_hosts)]
            smars_host = smars_host_for_engine(engine) if engine > 0 else ""

            trace_id = gen_hex(32, f"trace|{chain_key}") if fdef.get("trace") else ""

            retry = fdef["retry"]
            A = attempt_count(float(retry["expected_attempts"]), int(retry["max_attempts"]), f"ac|{chain_key}")

            # Build planned event list with sampled delays; later we may compress/expand to fit response latency_ms domain.
            planned: List[PlannedEvent] = []

            backoff_pairs = retry.get("backoff_ms", []) or []
            per_retry_refs = retry.get("emit_per_retry", []) or []

            # Helper: choose host for emitting component
            def host_for_comp(comp_id: str) -> str:
                if comp_id == "broker_gateway":
                    return bgw_host
                if comp_id == "exchange_gateway":
                    return exgw_host
                if comp_id == "smars_router":
                    return smars_host
                return default_host_for_component(comp_id)

            for a in range(1, A + 1):
                is_final_attempt = (a == A)

                emit_refs_all: List[str] = fdef["emit"]
                latency_pairs_all: List[List[float]] = fdef["latency_ms"]

                # Model recv only on attempt 1; later retries are internal send-to-SMARS retries.
                if a == 1:
                    emit_refs_attempt_all = emit_refs_all
                    latency_pairs_attempt_all = latency_pairs_all
                else:
                    emit_refs_attempt_all = emit_refs_all[1:]
                    latency_pairs_attempt_all = latency_pairs_all[1:]

                if is_final_attempt:
                    emit_refs = emit_refs_attempt_all
                    latency_pairs = latency_pairs_attempt_all
                else:
                    # Failed attempt emits only early stage (recv+route on attempt1; route only on later attempts).
                    if a == 1:
                        k = min(2, len(emit_refs_attempt_all))
                        emit_refs = emit_refs_attempt_all[:k]
                        latency_pairs = latency_pairs_attempt_all[:k]
                    else:
                        k = min(1, len(emit_refs_attempt_all))
                        emit_refs = emit_refs_attempt_all[:k]
                        latency_pairs = latency_pairs_attempt_all[:k]

                for li, ref in enumerate(emit_refs):
                    comp_id, _ = parse_ref(ref)
                    host = host_for_comp(comp_id)

                    # Delay from previous planned event:
                    # - First event of attempt 1: latency hint
                    # - First event of attempt >1: backoff + latency hint
                    extra_backoff = 0.0
                    if li == 0 and a > 1:
                        bi = a - 2
                        if bi < len(backoff_pairs):
                            extra_backoff = sample_backoff_ms(backoff_pairs[bi], f"{chain_key}|att{a}|bo{bi}")
                        else:
                            extra_backoff = 50.0

                    p50, p95 = float(latency_pairs[li][0]), float(latency_pairs[li][1])
                    lat_delay = sample_latency_delay_ms(ref, p50, p95, (lm50 if state == "f" else 1.0), (lm95 if state == "f" else 1.0), f"{chain_key}|att{a}|li{li}")
                    d_ms = extra_backoff + lat_delay

                    # Default constraints: keep positive and bounded roughly by (backoff cap + latency cap).
                    # We allow later adjustment to compress/expand but never violate mins/maxs.
                    min_d = 1.0
                    max_d = max(1.0, extra_backoff + (3.0 * p95 * ((lm95 if state == "f" else 1.0))))

                    # Special-case: Power Peg warning carries elapsed_ms domain [50,5000] and we bind it to the accept->warn gap.
                    if ref in ("smars_router.powerpeg_warn", "smars_router.powerpeg_warn_legacy"):
                        edom = get_int_domain(ref, "elapsed", state) or (50, 5000)
                        min_d = float(edom[0])
                        max_d = float(edom[1])
                        d_ms = clamp(d_ms, min_d, max_d)

                    planned.append(PlannedEvent(ref=ref, comp_id=comp_id, host=host, kind="emit", attempt=a, delay_ms=float(d_ms), min_delay_ms=min_d, max_delay_ms=max_d, fixed_delay=False))

                if not is_final_attempt:
                    # Retry marker(s) emitted between attempts; fixed 1ms spacing to preserve order.
                    next_attempt = a + 1
                    for rref in per_retry_refs:
                        rcomp, _ = parse_ref(rref)
                        rhost = host_for_comp(rcomp)
                        reason = "timeout" if (next_attempt % 2 == 0) else "conn_reset"
                        planned.append(
                            PlannedEvent(
                                ref=rref,
                                comp_id=rcomp,
                                host=rhost,
                                kind="retry",
                                attempt=a,
                                delay_ms=1.0,
                                min_delay_ms=1.0,
                                max_delay_ms=1.0,
                                fixed_delay=True,
                                retry_attempt=next_attempt,
                                retry_reason=reason,
                            )
                        )

            # Adjust end-to-end response latency_ms so it stays within the YAML domain, while keeping timestamp gaps consistent.
            recv_idx = next((i for i, pe in enumerate(planned) if pe.ref == "broker_gateway.parent_order_recv"), None)
            resp_idx = None
            resp_ref = None
            for i in range(len(planned) - 1, -1, -1):
                if planned[i].ref in ("broker_gateway.parent_order_resp_ok", "broker_gateway.parent_order_resp_err"):
                    resp_idx = i
                    resp_ref = planned[i].ref
                    break

            if recv_idx is not None and resp_idx is not None and resp_ref is not None and resp_idx > recv_idx:
                ldom = get_int_domain(resp_ref, "lat_ms", state)
                if ldom is not None:
                    lat_min, lat_max = ldom
                    delays = [pe.delay_ms for pe in planned]
                    mins = [pe.min_delay_ms for pe in planned]
                    maxs = [pe.max_delay_ms for pe in planned]
                    fixeds = [pe.fixed_delay for pe in planned]
                    idxs = list(range(recv_idx + 1, resp_idx + 1))
                    current = sum(delays[i] for i in idxs)
                    target_int = int(round(current))
                    target_int = int(clamp(target_int, float(lat_min), float(lat_max)))
                    adjust_delays_to_target_sum(delays, mins, maxs, fixeds, idxs, float(target_int), f"latcap|{chain_key}")
                    for i, pe in enumerate(planned):
                        pe.delay_ms = float(clamp(delays[i], mins[i], maxs[i]))

            # Materialize timestamps from delays.
            times: List[datetime] = []
            cur = start_dt
            for pe in planned:
                cur = cur + timedelta(milliseconds=float(pe.delay_ms))
                times.append(cur)

            # Derive key timestamps for message-bound timing fields.
            recv_time: Optional[datetime] = None
            accept_time: Optional[datetime] = None
            for pe, t in zip(planned, times):
                if pe.ref == "broker_gateway.parent_order_recv" and recv_time is None:
                    recv_time = t
                if pe.ref in ("smars_router.accept_order_rlp", "smars_router.accept_order_powerpeg", "smars_router.accept_order_powerpeg_legacy") and accept_time is None:
                    accept_time = t

            # Emit planned rows.
            for i, (pe, t) in enumerate(zip(planned, times)):
                ref = pe.ref
                bound: Dict[str, Any] = {
                    "order_id": order_id,
                    "acct": acct,
                    "sym": sym,
                    "qty": qty,
                    "rlp": rlp,
                    "req_id": req_id,
                }
                if engine > 0:
                    bound["engine"] = engine

                if pe.kind == "retry":
                    bound = {"req_id": req_id, "attempt": pe.retry_attempt, "reason": pe.retry_reason}
                elif ref == "smars_router.child_plan":
                    slices = int(clamp(int(round(qty / 1000.0)), 1, 60))
                    slice_qty = int(math.ceil(qty / float(slices)))
                    slice_qty = int(clamp(slice_qty, 10, 3000))
                    bound.update({"slices": slices, "slice_qty": slice_qty})
                elif ref == "exchange_gateway.order_new_ack":
                    bound.update({"venue": choose_from(["NYSE", "NASDAQ"], f"venue|{chain_key}|{pe.attempt}"), "exch_id": gen_hex(12, f"exch|{chain_key}|{pe.attempt}")})
                elif ref == "broker_gateway.parent_order_resp_ok":
                    if recv_time is None:
                        recv_time = times[0]
                    lat_ms = int(round((t - recv_time).total_seconds() * 1000.0))
                    dom = get_int_domain(ref, "lat_ms", state) or (2, 400)
                    lat_ms = int(clamp(lat_ms, dom[0], dom[1]))
                    bound.update({"lat_ms": lat_ms})
                elif ref == "broker_gateway.parent_order_resp_err":
                    if recv_time is None:
                        recv_time = times[0]
                    lat_ms = int(round((t - recv_time).total_seconds() * 1000.0))
                    dom = get_int_domain(ref, "lat_ms", state) or (1, 80)
                    lat_ms = int(clamp(lat_ms, dom[0], dom[1]))
                    bound.update({"status": 503, "detail": "router_disabled", "lat_ms": lat_ms})
                elif ref in ("smars_router.powerpeg_warn", "smars_router.powerpeg_warn_legacy"):
                    # Bind elapsed_ms to the timestamp gap since accept_time (and keep it within domain).
                    if accept_time is None:
                        accept_time = t
                    elapsed = int(round((t - accept_time).total_seconds() * 1000.0))
                    edom = get_int_domain(ref, "elapsed", state) or (50, 5000)
                    elapsed = int(clamp(elapsed, edom[0], edom[1]))
                    m = minute_of_dt(t)
                    phase_boost = 1.0 if m < 30 else 1.6
                    sent = int(clamp(5000 + phase_boost * (elapsed * 150) + (h32(f"sent|{chain_key}|{pe.attempt}") % 20000), 5000, 250000))
                    bound.update({"elapsed": elapsed, "sent": sent})

                lvl, msg = render_message(ref, state, f"log|{chain_key}|{i}|{ref}", {k: v for k, v in bound.items() if v is not None})
                svc, _ = component_identity(pe.comp_id, pe.host)
                emit_row(t, lvl, msg, trace_id if pe.kind != "retry" or trace_id else trace_id, svc, pe.host)


def run() -> None:
    emit_background_for_interval(NORMAL_INTERVAL)
    emit_flows_for_interval(NORMAL_INTERVAL)

    for interval in FAILURE_INTERVALS:
        emit_background_for_interval(interval)
        emit_flows_for_interval(interval)

    emit_one_shots()

    df = pd.DataFrame(ROWS, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} out of required range [20000, 100000].")
    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV columns incorrect.")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    run()
