import math
import hashlib
import uuid
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "cdn_backbone_route_leak_incident"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_pop_affected", "edge_pop_unaffected"], "trace_len": 32},
    "components": {
        "edge_pop_affected": {
            "svc": "edge-proxy",
            "hosts": ["edge-a01", "edge-a02", "edge-a03"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start req_id={req_id} pop={pop} method={method} host={host} uri={uri} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "str", "v": "customer-domain"},
                        "uri": {"k": "str", "v": "path"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "route_selected_local": {
                    "lvl": "INFO",
                    "msg": "route selected req_id={req_id} pop={pop} selected=local trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "route_selected_bbone_atl": {
                    "lvl": "INFO",
                    "msg": "route selected req_id={req_id} pop={pop} selected=bbone_atl trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "upstream_fetch_start": {
                    "lvl": "INFO",
                    "msg": "upstream fetch start req_id={req_id} pop={pop} dst=atl attempt={attempt} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "upstream_fetch_fail": {
                    "lvl": "WARN",
                    "msg": "upstream fetch failed req_id={req_id} pop={pop} dst=atl attempt={attempt} err={err} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "err": {"k": "ch", "v": ["timeout", "connect_error", "reset"]},
                        "dur_ms": {"k": "i", "v": [50, 3500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "retrying_upstream": {
                    "lvl": "WARN",
                    "msg": "retrying req_id={req_id} pop={pop} route=bbone_atl attempt={attempt} reason={reason} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["timeout", "connect_error"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_complete_ok": {
                    "lvl": "INFO",
                    "msg": "req complete req_id={req_id} pop={pop} status=200 dur_ms={dur_ms} bytes={bytes} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "dur_ms": {"k": "i", "v": [5, 500]},
                        "bytes": {"k": "i", "v": [200, 250000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_complete_err": {
                    "lvl": "ERROR",
                    "msg": "req complete req_id={req_id} pop={pop} status={status} dur_ms={dur_ms} err={err} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "status": {"k": "ch", "v": [502, 503, 504]},
                        "dur_ms": {"k": "i", "v": [200, 7000]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "connect_reset", "no_route"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "worker_health": {
                    "lvl": "INFO",
                    "msg": "worker health pop={pop} cpu_pct={cpu_pct} conn={conn} drop_pct={drop_pct}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ewr", "ord", "sjc", "lon", "ams"]},
                        "cpu_pct": {"k": "i", "v": [0, 100]},
                        "conn": {"k": "i", "v": [500, 60000]},
                        "drop_pct": {"k": "f", "v": [0.0, 8.0]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "worker_health", "per_min": 0.25, "scope": "per_host"}],
                "f": [{"id": "worker_health", "per_min": 0.25, "scope": "per_host"}],
            },
        },
        "edge_pop_unaffected": {
            "svc": "edge-proxy",
            "hosts": ["edge-u01", "edge-u02", "edge-u03"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start req_id={req_id} pop={pop} method={method} host={host} uri={uri} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["syd"]},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "str", "v": "customer-domain"},
                        "uri": {"k": "str", "v": "path"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "route_selected_local": {
                    "lvl": "INFO",
                    "msg": "route selected req_id={req_id} pop={pop} selected=local trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "pop": {"k": "ch", "v": ["syd"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "req_complete_ok": {
                    "lvl": "INFO",
                    "msg": "req complete req_id={req_id} pop={pop} status=200 dur_ms={dur_ms} bytes={bytes} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["syd"]},
                        "dur_ms": {"k": "i", "v": [5, 500]},
                        "bytes": {"k": "i", "v": [200, 250000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "worker_health": {
                    "lvl": "INFO",
                    "msg": "worker health pop={pop} cpu_pct={cpu_pct} conn={conn} drop_pct={drop_pct}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["syd"]},
                        "cpu_pct": {"k": "i", "v": [0, 100]},
                        "conn": {"k": "i", "v": [500, 60000]},
                        "drop_pct": {"k": "f", "v": [0.0, 8.0]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "worker_health", "per_min": 0.25, "scope": "per_host"}],
                "f": [{"id": "worker_health", "per_min": 0.25, "scope": "per_host"}],
            },
        },
        "backbone_router_atl": {
            "svc": "bbone-atl",
            "hosts": ["atl01"],
            "logs": {
                "config_commit": {
                    "lvl": "INFO",
                    "msg": "commit applied change_id={change_id} user={user} diff_lines={diff_lines}",
                    "vars": {"change_id": {"k": "hex", "v": 8}, "user": {"k": "ch", "v": ["neteng1", "neteng2"]}, "diff_lines": {"k": "i", "v": [1, 30]}},
                },
                "bgp_advertise_summary": {
                    "lvl": "INFO",
                    "msg": "bgp export summary vrf=backbone adv_prefixes={adv_prefixes} local_pref={local_pref} peers={peers}",
                    "vars": {"adv_prefixes": {"k": "i", "v": [500, 80000]}, "local_pref": {"k": "i", "v": [100, 200]}, "peers": {"k": "i", "v": [5, 25]}},
                },
                "cpu_overload": {
                    "lvl": "WARN",
                    "msg": "re cpu high cpu_pct={cpu_pct} reason={reason}",
                    "vars": {"cpu_pct": {"k": "i", "v": [75, 100]}, "reason": {"k": "ch", "v": ["bgp_update_storm", "forwarding_overload"]}},
                },
                "forwarding_drops": {
                    "lvl": "WARN",
                    "msg": "fwd drops if={iface} pps_drop={pps_drop} qlen={qlen}",
                    "vars": {"iface": {"k": "ch", "v": ["xe-0/0/0", "xe-0/0/1"]}, "pps_drop": {"k": "i", "v": [0, 220000]}, "qlen": {"k": "i", "v": [0, 12000]}},
                },
                "router_disabled": {
                    "lvl": "CRITICAL",
                    "msg": "backbone disabled action=shutdown_ibgp drain_s={drain_s}",
                    "vars": {"drain_s": {"k": "i", "v": [10, 180]}},
                },
                "participation_status_disabled": {
                    "lvl": "INFO",
                    "msg": "participation status vrf=backbone admin_state=disabled",
                    "vars": {},
                },
            },
            "beh": {
                "n": [{"id": "bgp_advertise_summary", "per_min": 0.4, "scope": "global"}],
                "f": [
                    {"id": "bgp_advertise_summary", "per_min": 2.5, "scope": "global"},
                    {"id": "cpu_overload", "per_min": 0.5, "scope": "global"},
                    {"id": "forwarding_drops", "per_min": 0.7, "scope": "global"},
                    {"id": "participation_status_disabled", "per_min": 0.4, "scope": "global"},
                ],
            },
        },
        "backbone_router_peer": {
            "svc": "bbone-peers",
            "hosts": ["ord01", "ewr01", "lon01"],
            "logs": {
                "link_congestion_warn": {
                    "lvl": "WARN",
                    "msg": "link congested link={link} util_pct={util_pct}",
                    "vars": {"link": {"k": "ch", "v": ["ewr-ord", "atl-iad"]}, "util_pct": {"k": "i", "v": [70, 99]}},
                },
                "bgp_bestpath_change": {
                    "lvl": "INFO",
                    "msg": "bestpath change prefix={prefix} next_hop={next_hop} local_pref={local_pref}",
                    "vars": {"prefix": {"k": "str", "v": "cidr"}, "next_hop": {"k": "ch", "v": ["atl01", "local"]}, "local_pref": {"k": "i", "v": [100, 200]}},
                },
                "bgp_neighbor_down": {
                    "lvl": "WARN",
                    "msg": "bgp neighbor down peer={peer} reason={reason}",
                    "vars": {"peer": {"k": "ch", "v": ["atl01"]}, "reason": {"k": "ch", "v": ["hold_timer", "admin_down"]}},
                },
                "neighbor_state_idle": {"lvl": "INFO", "msg": "bgp neighbor state peer=atl01 state=idle", "vars": {}},
                "bgp_rib_summary": {
                    "lvl": "INFO",
                    "msg": "rib summary learned_prefixes={learned_prefixes} updates_s={updates_s}",
                    "vars": {"learned_prefixes": {"k": "i", "v": [500, 80000]}, "updates_s": {"k": "i", "v": [0, 6000]}},
                },
            },
            "beh": {
                "n": [{"id": "bgp_rib_summary", "per_min": 0.4, "scope": "global"}, {"id": "link_congestion_warn", "per_min": 0.12, "scope": "global"}],
                "f": [
                    {"id": "bgp_rib_summary", "per_min": 1.2, "scope": "global"},
                    {"id": "bgp_bestpath_change", "per_min": 3.5, "scope": "global"},
                    {"id": "neighbor_state_idle", "per_min": 0.6, "scope": "global"},
                ],
            },
        },
        "traffic_manager": {
            "svc": "tm",
            "hosts": ["tm01"],
            "logs": {
                "pop_sample": {
                    "lvl": "INFO",
                    "msg": "pop sample pop={pop} cpu_pct={cpu_pct} req_rps={req_rps}",
                    "vars": {"pop": {"k": "ch", "v": ["atl", "ewr", "ord", "sjc", "lon", "ams", "syd"]}, "cpu_pct": {"k": "i", "v": [0, 100]}, "req_rps": {"k": "i", "v": [0, 3000]}},
                },
                "anomaly_alert": {
                    "lvl": "WARN",
                    "msg": "anomaly detected region={region} dropped_pct={dropped_pct} top_pop={top_pop}",
                    "vars": {"region": {"k": "ch", "v": ["na", "europe", "apac", "latam"]}, "dropped_pct": {"k": "i", "v": [10, 80]}, "top_pop": {"k": "ch", "v": ["atl", "ord", "sjc", "lon"]}},
                },
                "recovery_notice": {"lvl": "INFO", "msg": "recovery backbone_normalized=true affected_pops_est={affected_pops_est}", "vars": {"affected_pops_est": {"k": "i", "v": [5, 30]}}},
            },
            "beh": {"n": [{"id": "pop_sample", "per_min": 8.0, "scope": "global"}], "f": [{"id": "pop_sample", "per_min": 8.0, "scope": "global"}]},
        },
        "core_observability": {
            "svc": "obs-ingest",
            "hosts": ["obs01", "obs02"],
            "logs": {
                "ingest_metric": {
                    "lvl": "INFO",
                    "msg": "ingest stats queue_lag_s={queue_lag_s} ingest_eps={ingest_eps}",
                    "vars": {"queue_lag_s": {"k": "i", "v": [0, 900]}, "ingest_eps": {"k": "i", "v": [10, 400]}},
                },
                "drop_warn": {"lvl": "WARN", "msg": "ingest backpressure dropped_events={dropped_events} window_s={window_s}", "vars": {"dropped_events": {"k": "i", "v": [50, 20000]}, "window_s": {"k": "i", "v": [10, 60]}}},
            },
            "beh": {"n": [{"id": "ingest_metric", "per_min": 1.0, "scope": "global"}], "f": [{"id": "ingest_metric", "per_min": 1.2, "scope": "global"}, {"id": "drop_warn", "per_min": 0.8, "scope": "global"}]},
        },
    },
    "flows": {
        "n": {
            "http_req_affected_ok_n": {
                "rpm": 200.0,
                "emit": ["edge_pop_affected.req_start", "edge_pop_affected.route_selected_local", "edge_pop_affected.req_complete_ok"],
                "latency_ms": [[2, 6], [1, 4], [15, 140]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "http_req_unaffected_ok_n": {
                "rpm": 200.0,
                "emit": ["edge_pop_unaffected.req_start", "edge_pop_unaffected.route_selected_local", "edge_pop_unaffected.req_complete_ok"],
                "latency_ms": [[2, 6], [1, 4], [15, 140]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        },
        "f": {
            "http_req_affected_ok_f": {
                "rpm": 200.0,
                "emit": ["edge_pop_affected.req_start", "edge_pop_affected.route_selected_local", "edge_pop_affected.req_complete_ok"],
                "latency_ms": [[2, 6], [1, 4], [15, 160]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "http_req_unaffected_ok_f": {
                "rpm": 200.0,
                "emit": ["edge_pop_unaffected.req_start", "edge_pop_unaffected.route_selected_local", "edge_pop_unaffected.req_complete_ok"],
                "latency_ms": [[2, 6], [1, 4], [15, 160]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "http_req_affected_client_err_f": {
                "rpm": 200.0,
                "emit": ["edge_pop_affected.req_start", "edge_pop_affected.route_selected_bbone_atl", "edge_pop_affected.req_complete_err"],
                "latency_ms": [[2, 6], [1, 6], [900, 6500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "http_req_affected_to_atl_fail_f": {
                "rpm": 200.0,
                "emit": ["edge_pop_affected.upstream_fetch_start", "edge_pop_affected.upstream_fetch_fail"],
                "latency_ms": [[1, 3], [600, 3200]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.0, "emit_per_retry": ["edge_pop_affected.retrying_upstream"], "backoff_ms": [[50, 200], [100, 400]]},
                "trace": True,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "atl_backbone_route_leak_overload",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "http_req_affected_ok_f": 0.0,
                            "http_req_unaffected_ok_f": 0.5,
                            "http_req_affected_client_err_f": 0.25,
                            "http_req_affected_to_atl_fail_f": 0.25,
                            "backbone_router_atl.bgp_advertise_summary": 2.0,
                            "backbone_router_atl.cpu_overload": 1.0,
                            "backbone_router_atl.forwarding_drops": 1.0,
                            "backbone_router_peer.bgp_bestpath_change": 2.0,
                            "backbone_router_atl.participation_status_disabled": 0.0,
                            "backbone_router_peer.neighbor_state_idle": 0.0,
                            "core_observability.drop_warn": 0.0,
                        },
                        "latency_multipliers": {"http_req_affected_to_atl_fail_f": {"p50": 1.2, "p95": 1.3}, "http_req_affected_client_err_f": {"p50": 1.1, "p95": 1.2}},
                        "one_shots": [{"ref": "backbone_router_atl.config_commit", "count": 1, "hosts": ["atl01"]}, {"ref": "traffic_manager.anomaly_alert", "count": 1, "hosts": ["tm01"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 35,
                        "rate_multipliers": {
                            "http_req_affected_ok_f": 1.0,
                            "http_req_unaffected_ok_f": 1.0,
                            "http_req_affected_client_err_f": 0.02,
                            "http_req_affected_to_atl_fail_f": 0.02,
                            "backbone_router_atl.cpu_overload": 0.0,
                            "backbone_router_atl.forwarding_drops": 0.0,
                            "backbone_router_atl.bgp_advertise_summary": 0.0,
                            "backbone_router_peer.bgp_bestpath_change": 0.4,
                            "backbone_router_atl.participation_status_disabled": 1.0,
                            "backbone_router_peer.neighbor_state_idle": 1.0,
                        },
                        "latency_multipliers": {"http_req_affected_ok_f": {"p50": 1.0, "p95": 1.0}, "http_req_unaffected_ok_f": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "backbone_router_atl.router_disabled", "count": 1, "hosts": ["atl01"]},
                            {"ref": "backbone_router_peer.bgp_neighbor_down", "count": 3, "hosts": ["ord01", "ewr01", "lon01"]},
                            {"ref": "traffic_manager.recovery_notice", "count": 1, "hosts": ["tm01"]},
                        ],
                    },
                    {"order": 3, "at_min": 40, "rate_multipliers": {"core_observability.ingest_metric": 2.0, "core_observability.drop_warn": 1.0}, "latency_multipliers": {}, "one_shots": []},
                ]
            }
        },
    }
}


# ------------------ Deterministic helpers ------------------

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def hash_unit(s: str) -> float:
    h = int(md5_hex(s), 16)
    return (h % (10**12)) / float(10**12)


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    u = hash_unit(f"round:{key}")
    return base + (1 if u < frac else 0)


def inv_norm_cdf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def sample_lognormal_ms(p50: float, p95: float, key: str, cap_mult: float = 3.0) -> float:
    if p50 <= 0:
        return 0.0
    if p95 <= p50:
        return float(p50)
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    u = hash_unit(f"ln:{key}")
    z = inv_norm_cdf(u)
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    return float(min(x, cap))


def sample_choice(options: List[Any], key: str) -> Any:
    if not options:
        return ""
    idx = int(hash_unit(f"ch:{key}") * len(options))
    idx = min(max(idx, 0), len(options) - 1)
    return options[idx]


def deterministic_hex(n: int, key: str) -> str:
    h = md5_hex(f"hex:{key}")
    while len(h) < n:
        h += md5_hex(h)
    return h[:n]


def deterministic_uuid(key: str) -> str:
    b = hashlib.md5(f"uuid:{key}".encode("utf-8")).digest()
    u = uuid.UUID(bytes=b)
    return str(u)


def deterministic_int(lo: int, hi: int, key: str) -> int:
    if hi <= lo:
        return int(lo)
    u = hash_unit(f"i:{key}")
    return int(lo + math.floor(u * (hi - lo + 1)))


def deterministic_float(lo: float, hi: float, key: str) -> float:
    if hi <= lo:
        return float(lo)
    u = hash_unit(f"f:{key}")
    return float(lo + u * (hi - lo))


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def schedule_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start for _ in range(count)]
    jitter_s = min(0.2, max(0.001, (total_s / count) * 0.2))
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start + timedelta(seconds=frac * total_s)
        j = (hash_unit(f"jit:{key}:{i}") - 0.5) * 2.0 * jitter_s
        t = t + timedelta(seconds=j)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def minutes_since(base: datetime, t: datetime) -> float:
    return (t - base).total_seconds() / 60.0


# ------------------ Control intervals ------------------

@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50, p95}


def build_failure_intervals() -> Tuple[List[Interval], List[Dict[str, Any]]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    f_end = f_phase["end_min"]
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}
    intervals: List[Interval] = []
    for idx, ev in enumerate(events):
        start = ev["at_min"]
        end = events[idx + 1]["at_min"] if idx + 1 < len(events) else f_end
        for k, v in ev.get("rate_multipliers", {}).items():
            rate_mult[k] = float(v)
        for k, v in ev.get("latency_multipliers", {}).items():
            lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        intervals.append(Interval(start_min=start, end_min=end, rate_mult=dict(rate_mult), latency_mult=dict(lat_mult)))
    return intervals, events


FAILURE_INTERVALS, FAILURE_EVENTS = build_failure_intervals()


# ------------------ Log emission ------------------

class Emitter:
    def __init__(self, base_time: datetime):
        self.base_time = base_time
        self.rows: List[Dict[str, Any]] = []
        self.seq = 0

    def add(self, t: datetime, level: str, message: str, trace_id: str, service: str, host: str):
        self.rows.append(
            {
                "timestamp_dt": t,
                "level": level,
                "message": message,
                "trace_id": trace_id,
                "service": service,
                "host": host,
                "_seq": self.seq,
            }
        )
        self.seq += 1


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def comp_meta(comp_id: str) -> Tuple[str, List[str]]:
    c = SYSTEM["components"][comp_id]
    return c.get("svc", "") or "", c.get("hosts", []) or []


def log_tmpl(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def get_int_var_range(comp_id: str, log_id: str, var_name: str) -> Optional[Tuple[int, int]]:
    tmpl = log_tmpl(comp_id, log_id)
    spec = (tmpl.get("vars") or {}).get(var_name)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    v = spec.get("v") or [0, 0]
    return int(v[0]), int(v[1])


def choose_component_host_for_flow(comp_id: str, flow_id: str, inst_key: str) -> str:
    _, hosts = comp_meta(comp_id)
    if not hosts:
        return ""
    idx = int(hash_unit(f"hostsel:{comp_id}:{flow_id}:{inst_key}") * len(hosts))
    idx = min(max(idx, 0), len(hosts) - 1)
    return hosts[idx]


def render_str_hint(hint: str, key: str) -> str:
    u = int(hash_unit(f"str:{hint}:{key}") * 1000000)
    if hint == "customer-domain":
        return f"www.customer{u % 5000}.example"
    if hint == "path":
        a = u % 1000
        b = (u // 1000) % 1000
        ext = sample_choice([".png", ".js", ".css", ".html", ".json"], f"ext:{key}")
        return f"/content/{a}/item/{b}{ext}"
    if hint == "cidr":
        a = (u % 200) + 1
        b = ((u // 200) % 254) + 1
        return f"10.{a}.{b}.0/24"
    return f"{hint}-{u % 100000}"


def build_vars_generic(comp_id: str, log_id: str, state: str, t: datetime, key: str, preset: Dict[str, Any]) -> Dict[str, Any]:
    tmpl = log_tmpl(comp_id, log_id)
    vars_spec: Dict[str, Any] = tmpl.get("vars", {}) or {}
    out = dict(preset)
    for name, spec in vars_spec.items():
        if name in out:
            continue
        k = spec["k"]
        v = spec.get("v")
        vkey = f"{key}:{comp_id}.{log_id}:{name}:{fmt_ts(t)}"
        if k == "uuid":
            out[name] = deterministic_uuid(vkey)
        elif k == "hex":
            out[name] = deterministic_hex(int(v), vkey)
        elif k == "ch":
            out[name] = sample_choice(list(v), vkey)
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            out[name] = deterministic_int(lo, hi, vkey)
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            out[name] = round(deterministic_float(lo, hi, vkey), 2)
        elif k == "str":
            out[name] = render_str_hint(str(v), vkey)
        else:
            out[name] = ""
    return out


def build_vars_special(comp_id: str, log_id: str, state: str, t: datetime, key: str, preset: Dict[str, Any], base_time: datetime) -> Dict[str, Any]:
    m = minutes_since(base_time, t)
    out = dict(preset)

    if comp_id in ("edge_pop_affected", "edge_pop_unaffected") and log_id == "worker_health":
        if "pop" not in out:
            out["pop"] = sample_choice(SYSTEM["components"][comp_id]["logs"][log_id]["vars"]["pop"]["v"], f"{key}:pop:{m:.3f}")
        pop = out["pop"]
        if comp_id == "edge_pop_affected" and state == "f" and 25 <= m < 35:
            cpu_lo, cpu_hi = 55, 97
            conn_lo, conn_hi = 15000, 60000
            drop_lo, drop_hi = 1.5, 8.0
        elif comp_id == "edge_pop_affected" and state == "f" and m >= 35:
            cpu_lo, cpu_hi = 15, 70
            conn_lo, conn_hi = 8000, 45000
            drop_lo, drop_hi = 0.0, 2.5
        else:
            cpu_lo, cpu_hi = 10, 65
            conn_lo, conn_hi = 5000, 45000
            drop_lo, drop_hi = 0.0, 1.5
        out["cpu_pct"] = deterministic_int(cpu_lo, cpu_hi, f"{key}:{pop}:cpu:{m:.3f}")
        out["conn"] = deterministic_int(conn_lo, conn_hi, f"{key}:{pop}:conn:{m:.3f}")
        out["drop_pct"] = round(deterministic_float(drop_lo, drop_hi, f"{key}:{pop}:drop:{m:.3f}"), 2)

    elif comp_id == "traffic_manager" and log_id == "pop_sample":
        pops = SYSTEM["components"][comp_id]["logs"][log_id]["vars"]["pop"]["v"]
        idx = deterministic_int(0, len(pops) - 1, f"{key}:pop_sample_idx:{m:.3f}")
        pop = pops[idx]
        out["pop"] = pop
        if 25 <= m < 35:
            if pop == "atl":
                cpu_lo, cpu_hi = 80, 100
                rps_lo, rps_hi = 1500, 3000
            elif pop in ("ewr", "ord", "sjc", "lon", "ams"):
                cpu_lo, cpu_hi = 20, 85
                rps_lo, rps_hi = 50, 700
            else:
                cpu_lo, cpu_hi = 10, 60
                rps_lo, rps_hi = 400, 1200
        else:
            if pop == "atl":
                cpu_lo, cpu_hi = 10, 40
                rps_lo, rps_hi = 100, 900
            elif pop in ("ewr", "ord", "sjc", "lon", "ams"):
                cpu_lo, cpu_hi = 10, 65
                rps_lo, rps_hi = 500, 1800
            else:
                cpu_lo, cpu_hi = 10, 60
                rps_lo, rps_hi = 500, 1700
        out["cpu_pct"] = deterministic_int(cpu_lo, cpu_hi, f"{key}:{pop}:cpu:{m:.3f}")
        out["req_rps"] = deterministic_int(rps_lo, rps_hi, f"{key}:{pop}:rps:{m:.3f}")

    elif comp_id == "core_observability" and log_id == "ingest_metric":
        if m < 25:
            lag_lo, lag_hi = 0, 10
            eps_lo, eps_hi = 150, 350
        elif 25 <= m < 40:
            lag_lo, lag_hi = 5, 60
            eps_lo, eps_hi = 120, 360
        else:
            lag_lo, lag_hi = 120, 900
            eps_lo, eps_hi = 80, 260
        out["queue_lag_s"] = deterministic_int(lag_lo, lag_hi, f"{key}:lag:{m:.3f}")
        out["ingest_eps"] = deterministic_int(eps_lo, eps_hi, f"{key}:eps:{m:.3f}")

    elif comp_id == "core_observability" and log_id == "drop_warn":
        if m < 40:
            dropped_lo, dropped_hi = 50, 500
        else:
            dropped_lo, dropped_hi = 500, 20000
        out["dropped_events"] = deterministic_int(dropped_lo, dropped_hi, f"{key}:dropped:{m:.3f}")
        out["window_s"] = deterministic_int(10, 60, f"{key}:window:{m:.3f}")

    elif comp_id == "backbone_router_atl" and log_id == "bgp_advertise_summary":
        if 25 <= m < 35:
            out["adv_prefixes"] = deterministic_int(40000, 80000, f"{key}:adv:{m:.3f}")
            out["local_pref"] = deterministic_int(170, 200, f"{key}:lp:{m:.3f}")
            out["peers"] = deterministic_int(10, 25, f"{key}:peers:{m:.3f}")
        else:
            out["adv_prefixes"] = deterministic_int(500, 15000, f"{key}:adv:{m:.3f}")
            out["local_pref"] = deterministic_int(100, 130, f"{key}:lp:{m:.3f}")
            out["peers"] = deterministic_int(5, 20, f"{key}:peers:{m:.3f}")

    elif comp_id == "backbone_router_atl" and log_id == "cpu_overload":
        out["cpu_pct"] = deterministic_int(85, 100, f"{key}:cpu:{m:.3f}")
        out["reason"] = sample_choice(["bgp_update_storm", "forwarding_overload"], f"{key}:reason:{m:.3f}")

    elif comp_id == "backbone_router_atl" and log_id == "forwarding_drops":
        out["iface"] = sample_choice(["xe-0/0/0", "xe-0/0/1"], f"{key}:iface:{m:.3f}")
        if 25 <= m < 35:
            out["pps_drop"] = deterministic_int(20000, 220000, f"{key}:pps:{m:.3f}")
            out["qlen"] = deterministic_int(2000, 12000, f"{key}:qlen:{m:.3f}")
        else:
            out["pps_drop"] = deterministic_int(0, 5000, f"{key}:pps:{m:.3f}")
            out["qlen"] = deterministic_int(0, 2000, f"{key}:qlen:{m:.3f}")

    elif comp_id == "backbone_router_peer" and log_id == "bgp_bestpath_change":
        if 25 <= m < 35:
            out["next_hop"] = "atl01"
            out["local_pref"] = deterministic_int(170, 200, f"{key}:lp:{m:.3f}")
        else:
            out["next_hop"] = "local"
            out["local_pref"] = deterministic_int(100, 130, f"{key}:lp:{m:.3f}")
        out["prefix"] = render_str_hint("cidr", f"{key}:pfx:{m:.3f}")

    elif comp_id == "backbone_router_peer" and log_id == "bgp_rib_summary":
        if 25 <= m < 35:
            out["learned_prefixes"] = deterministic_int(40000, 80000, f"{key}:learn:{m:.3f}")
            out["updates_s"] = deterministic_int(2500, 6000, f"{key}:upd:{m:.3f}")
        else:
            out["learned_prefixes"] = deterministic_int(5000, 40000, f"{key}:learn:{m:.3f}")
            out["updates_s"] = deterministic_int(0, 1200, f"{key}:upd:{m:.3f}")

    out = build_vars_generic(comp_id, log_id, state, t, key, out)
    return out


def emit_log(em: Emitter, comp_id: str, log_id: str, state: str, t: datetime, trace_id: str, host: str, key: str, preset: Dict[str, Any]):
    svc, _ = comp_meta(comp_id)
    tmpl = log_tmpl(comp_id, log_id)
    vars_filled = build_vars_special(comp_id, log_id, state, t, key, preset, em.base_time)
    if "trace_id" in (tmpl.get("vars") or {}) and "trace_id" not in vars_filled:
        vars_filled["trace_id"] = trace_id
    if "trace_id" in vars_filled and trace_id and vars_filled["trace_id"] != trace_id:
        vars_filled["trace_id"] = trace_id
    msg = tmpl["msg"]
    if tmpl.get("vars"):
        msg = msg.format(**vars_filled)
    level = tmpl["lvl"]
    em.add(t, level, msg, trace_id, svc, host)


def flow_latency_multiplier(interval: Optional[Interval], flow_id: str) -> Tuple[float, float]:
    if interval is None:
        return (1.0, 1.0)
    m = interval.latency_mult.get(flow_id)
    if not m:
        return (1.0, 1.0)
    return (float(m.get("p50", 1.0)), float(m.get("p95", 1.0)))


def rate_multiplier(interval: Optional[Interval], source_key: str) -> float:
    if interval is None:
        return 1.0
    return float(interval.rate_mult.get(source_key, 1.0))


def choose_attempt_count(flow_id: str, inst_key: str, expected: float, max_attempts: int) -> int:
    if max_attempts <= 1:
        return 1
    if expected <= 1.01:
        return 1
    if abs(expected - 2.0) < 1e-6 and max_attempts == 3:
        u = hash_unit(f"att:{flow_id}:{inst_key}")
        if u < 0.10:
            return 1
        if u < 0.20:
            return 3
        return 2
    a = int(round(expected))
    return max(1, min(max_attempts, a))


def _scale_segment_to_sum(delays_ms: List[int], seg_start: int, seg_end: int, target_sum: int, key: str):
    seg = delays_ms[seg_start : seg_end + 1]
    cur = sum(seg)
    if target_sum <= 0:
        for i in range(seg_start, seg_end + 1):
            delays_ms[i] = 0
        return
    if cur <= 0:
        for i in range(seg_start, seg_end):
            delays_ms[i] = 0
        delays_ms[seg_end] = target_sum
        return

    scaled_f = [x * (target_sum / cur) for x in seg]
    floors = [int(math.floor(x)) for x in scaled_f]
    rem = target_sum - sum(floors)
    fracs = [(scaled_f[i] - floors[i], i) for i in range(len(seg))]
    fracs.sort(key=lambda t: (-t[0], t[1]))
    add = [0] * len(seg)
    for k in range(rem):
        add[fracs[k % len(fracs)][1]] += 1
    for i in range(len(seg)):
        delays_ms[seg_start + i] = max(0, floors[i] + add[i])


def _clamp_duration_by_adjusting_delays(delays_ms: List[int], idx: int, lo: int, hi: int, key: str) -> int:
    if idx <= 0:
        return 0
    cur = sum(delays_ms[1 : idx + 1])
    target = min(max(cur, lo), hi)
    if target == cur:
        return target

    delta = target - cur
    new_last = delays_ms[idx] + delta
    if new_last >= 0:
        delays_ms[idx] = new_last
        return target

    _scale_segment_to_sum(delays_ms, 1, idx, target, key)
    return target


def _sample_delay_ms(p50: float, p95: float, key: str, p50_mult: float, p95_mult: float) -> int:
    p50s = float(p50) * p50_mult
    p95s = float(p95) * p95_mult
    d = sample_lognormal_ms(p50s, p95s, key, cap_mult=3.0)
    return max(0, int(round(d)))


def retry_reason_from_err(err: Optional[str]) -> str:
    # retrying_upstream.reason domain is [timeout, connect_error], while upstream_fetch_fail.err includes reset.
    # Map reset -> connect_error to keep semantic carrier coherence across attempts.
    if err == "timeout":
        return "timeout"
    if err in ("connect_error", "reset"):
        return "connect_error"
    return "timeout"


def simulate_flow_instance(em: Emitter, state: str, flow_id: str, flow: Dict[str, Any], start_t: datetime, interval: Optional[Interval], inst_idx: int):
    trace_id = ""
    inst_key = f"{state}:{flow_id}:{inst_idx}:{fmt_ts(start_t)}"
    if SYSTEM["tracing"]["on"] and flow.get("trace", False):
        trace_id = deterministic_hex(SYSTEM["tracing"]["trace_len"], f"trace:{inst_key}")

    comp_hosts: Dict[str, str] = {}
    for ref in flow["emit"] + flow.get("retry", {}).get("emit_per_retry", []):
        comp_id, _ = parse_ref(ref)
        if comp_id not in comp_hosts:
            comp_hosts[comp_id] = choose_component_host_for_flow(comp_id, flow_id, inst_key)

    req_id = deterministic_uuid(f"req:{inst_key}")
    method = sample_choice(["GET", "POST"], f"method:{inst_key}")
    customer_host = render_str_hint("customer-domain", f"host:{inst_key}")
    uri = render_str_hint("path", f"uri:{inst_key}")

    first_comp, first_log = parse_ref(flow["emit"][0])
    pop_domain = (SYSTEM["components"][first_comp]["logs"][first_log]["vars"].get("pop", {}).get("v") if SYSTEM["components"][first_comp]["logs"][first_log].get("vars") else None) or []
    pop = sample_choice(list(pop_domain), f"pop:{inst_key}")

    retry = flow.get("retry", {}) or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    attempts = choose_attempt_count(flow_id, inst_key, expected_attempts, max_attempts)

    lat_p50_mult, lat_p95_mult = flow_latency_multiplier(interval if state == "f" else None, flow_id)

    emit_refs = list(flow["emit"])
    latency_hints = list(flow.get("latency_ms", []))
    backoff_hints = list(retry.get("backoff_ms", []))
    retry_refs = list(retry.get("emit_per_retry", []))

    t_attempt = start_t
    prev_attempt_err: Optional[str] = None

    for a in range(1, attempts + 1):
        # Retry-only markers emitted at attempt start for retries (attempts 2..A).
        if a >= 2 and retry_refs:
            for rref in retry_refs:
                rc, rl = parse_ref(rref)
                preset = {
                    "req_id": req_id,
                    "pop": pop,
                    "attempt": a,
                    "reason": retry_reason_from_err(prev_attempt_err),
                    "trace_id": trace_id,
                }
                emit_log(em, rc, rl, state, t_attempt, trace_id, comp_hosts.get(rc, ""), f"{inst_key}:retry:{a}", preset)
                t_attempt = t_attempt + timedelta(milliseconds=1)

        delays_ms: List[int] = []
        for j in range(len(emit_refs)):
            if j < len(latency_hints):
                p50, p95 = latency_hints[j]
            else:
                p50, p95 = (0.0, 0.0)
            delays_ms.append(_sample_delay_ms(p50, p95, f"{inst_key}:lat:{a}:{j}", lat_p50_mult if state == "f" else 1.0, lat_p95_mult if state == "f" else 1.0))

        for j, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            if (log_tmpl(comp_id, log_id).get("vars") or {}).get("dur_ms") is not None:
                rng = get_int_var_range(comp_id, log_id, "dur_ms")
                if rng:
                    lo, hi = rng
                    _clamp_duration_by_adjusting_delays(delays_ms, j, lo, hi, key=f"{inst_key}:durclamp:{a}:{j}")

        times: List[datetime] = []
        t = t_attempt + timedelta(milliseconds=delays_ms[0] if delays_ms else 0)
        times.append(t)
        for j in range(1, len(emit_refs)):
            t = t + timedelta(milliseconds=delays_ms[j])
            times.append(t)

        attempt_err: Optional[str] = None

        for j, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            t_j = times[j] if j < len(times) else t_attempt

            preset: Dict[str, Any] = {}
            tmpl_vars = log_tmpl(comp_id, log_id).get("vars", {}) or {}
            if "req_id" in tmpl_vars:
                preset["req_id"] = req_id
            if "pop" in tmpl_vars:
                preset["pop"] = pop
            if "trace_id" in tmpl_vars:
                preset["trace_id"] = trace_id
            if log_id == "req_start":
                preset["method"] = method
                preset["host"] = customer_host
                preset["uri"] = uri

            if log_id in ("upstream_fetch_start", "upstream_fetch_fail", "retrying_upstream"):
                preset["attempt"] = a

            if "dur_ms" in tmpl_vars:
                dur_ms = int(sum(delays_ms[1 : j + 1])) if j >= 1 else 0
                preset["dur_ms"] = dur_ms

            if log_id == "upstream_fetch_fail":
                dur_ms = int(preset.get("dur_ms", 0))
                if dur_ms >= 1800:
                    attempt_err = "timeout"
                else:
                    attempt_err = "connect_error" if hash_unit(f"{inst_key}:err:{a}:{dur_ms}") < 0.85 else "reset"
                preset["err"] = attempt_err

            if log_id == "req_complete_ok":
                dur_ms = int(preset.get("dur_ms", 0))
                bytes_out = 200 + (int(hash_unit(f"bytes:{inst_key}:{uri}") * 249800))
                preset["dur_ms"] = dur_ms
                preset["bytes"] = bytes_out

            if log_id == "req_complete_err":
                dur_ms = int(preset.get("dur_ms", 0))
                if dur_ms >= 4500:
                    status = 504
                    err = "upstream_timeout"
                elif dur_ms >= 2500:
                    status = 503
                    err = "upstream_timeout"
                else:
                    status = 502
                    err = "connect_reset" if hash_unit(f"{inst_key}:cerr:{dur_ms}") < 0.9 else "no_route"
                preset["dur_ms"] = dur_ms
                preset["status"] = status
                preset["err"] = err

            emit_log(em, comp_id, log_id, state, t_j, trace_id, comp_hosts.get(comp_id, ""), f"{inst_key}:emit:{a}:{j}", preset)

        prev_attempt_err = attempt_err

        if a < attempts:
            if (a - 1) < len(backoff_hints):
                b50, b95 = backoff_hints[a - 1]
                backoff_ms = sample_lognormal_ms(float(b50), float(b95), f"{inst_key}:backoff:{a}", cap_mult=3.0)
            else:
                backoff_ms = sample_lognormal_ms(50.0, 200.0, f"{inst_key}:backoff:{a}", cap_mult=3.0)
            t_attempt = times[-1] + timedelta(milliseconds=int(round(backoff_ms)))
        else:
            t_attempt = times[-1]


def simulate_background(em: Emitter, state: str, interval_start: datetime, interval_end: datetime, interval: Optional[Interval], key_prefix: str):
    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, []) or []
        _, hosts = comp_meta(comp_id)
        for emit_spec in beh:
            log_id = emit_spec["id"]
            per_min = float(emit_spec["per_min"])
            scope = emit_spec.get("scope", "per_host")
            src_key = f"{comp_id}.{log_id}"
            mult = rate_multiplier(interval if state == "f" else None, src_key)
            eff_per_min = per_min * mult
            minutes = (interval_end - interval_start).total_seconds() / 60.0
            if eff_per_min <= 0 or minutes <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * minutes
                count = stable_round(expected, f"{key_prefix}:bg:{state}:{src_key}:{interval_start.isoformat()}:{interval_end.isoformat()}")
                times = schedule_times(interval_start, interval_end, count, f"{key_prefix}:bg_sched:{state}:{src_key}")
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    emit_log(em, comp_id, log_id, state, t, "", host, f"{key_prefix}:bg:{src_key}:{i}", preset={})
            else:
                for h in hosts:
                    expected = eff_per_min * minutes
                    count = stable_round(expected, f"{key_prefix}:bg:{state}:{src_key}:{h}:{interval_start.isoformat()}:{interval_end.isoformat()}")
                    times = schedule_times(interval_start, interval_end, count, f"{key_prefix}:bg_sched:{state}:{src_key}:{h}")
                    for i, t in enumerate(times):
                        emit_log(em, comp_id, log_id, state, t, "", h, f"{key_prefix}:bg:{src_key}:{h}:{i}", preset={})


def simulate_flows(em: Emitter, state: str, interval_start: datetime, interval_end: datetime, interval: Optional[Interval], key_prefix: str):
    flows = SYSTEM["flows"][state]
    minutes = (interval_end - interval_start).total_seconds() / 60.0
    for flow_id, flow in flows.items():
        base_rpm = float(flow["rpm"])
        mult = rate_multiplier(interval if state == "f" else None, flow_id)
        eff_rpm = base_rpm * mult
        expected = eff_rpm * minutes
        count = stable_round(expected, f"{key_prefix}:flow:{state}:{flow_id}:{interval_start.isoformat()}:{interval_end.isoformat()}")
        if count <= 0:
            continue
        starts = schedule_times(interval_start, interval_end, count, f"{key_prefix}:flow_sched:{state}:{flow_id}")
        for i, st in enumerate(starts):
            simulate_flow_instance(em, state, flow_id, flow, st, interval, i)


def emit_one_shots(em: Emitter, base_time: datetime):
    for ev in FAILURE_EVENTS:
        at_min = int(ev["at_min"])
        event_t = base_time + timedelta(minutes=at_min)
        shots = ev.get("one_shots", []) or []
        for sidx, shot in enumerate(shots):
            ref = shot["ref"]
            count = int(shot["count"])
            allowed_hosts = shot.get("hosts", None)
            comp_id, log_id = parse_ref(ref)
            _, comp_hosts_list = comp_meta(comp_id)
            hosts = allowed_hosts if allowed_hosts is not None else comp_hosts_list
            if not hosts:
                hosts = [""]

            for i in range(count):
                host = hosts[i % len(hosts)]
                offset_ms = int(5 + (hash_unit(f"oneshot:{ref}:{at_min}:{sidx}:{i}") * 240))
                t = event_t + timedelta(milliseconds=offset_ms)
                preset: Dict[str, Any] = {}
                if ref == "traffic_manager.anomaly_alert":
                    preset["region"] = "na"
                    preset["dropped_pct"] = deterministic_int(35, 75, f"anom:drop:{at_min}")
                    preset["top_pop"] = "atl"
                if ref == "traffic_manager.recovery_notice":
                    preset["affected_pops_est"] = deterministic_int(8, 25, f"recov:aff:{at_min}")
                if ref == "backbone_router_peer.bgp_neighbor_down":
                    preset["peer"] = "atl01"
                    preset["reason"] = "admin_down"
                emit_log(em, comp_id, log_id, "f", t, "", host, f"oneshot:{ref}:{at_min}:{sidx}:{i}", preset=preset)


def main():
    random.seed(0)

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    em = Emitter(base_time=base_time)

    n_phase = SCENARIO["scenario"]["time"]["phases"]["n"]
    n_start = base_time + timedelta(minutes=int(n_phase["start_min"]))
    n_end = base_time + timedelta(minutes=int(n_phase["end_min"]))
    simulate_background(em, "n", n_start, n_end, None, key_prefix="n0")
    simulate_flows(em, "n", n_start, n_end, None, key_prefix="n0")

    for k, interval in enumerate(FAILURE_INTERVALS):
        i_start = base_time + timedelta(minutes=interval.start_min)
        i_end = base_time + timedelta(minutes=interval.end_min)
        simulate_background(em, "f", i_start, i_end, interval, key_prefix=f"f{k}")
        simulate_flows(em, "f", i_start, i_end, interval, key_prefix=f"f{k}")

    emit_one_shots(em, base_time)

    df = pd.DataFrame(em.rows)
    df.sort_values(["timestamp_dt", "_seq"], inplace=True)
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()
    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
