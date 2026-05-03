import math
import re
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

random.seed(0)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "event_api_rds_cache_refresh"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_gateway"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_gateway",
            "svc": "edge-gateway",
            "hosts": ["gw-1", "gw-2"],
            "logs": {
                "gw_in": {
                    "lvl": "INFO",
                    "msg": "upstream start {method} {route} upstream={upstream} attempt={attempt} trace={trace_id} client_ip={client_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "route": {"k": "ch", "v": ["/v1/bounce"]},
                        "upstream": {"k": "ch", "v": ["api_server"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "trace_id": {"k": "hex", "v": 32},
                        "client_ip": {"k": "ip", "v": None},
                    },
                },
                "gw_out": {
                    "lvl": "INFO",
                    "msg": "upstream done {method} {route} upstream={upstream} attempt={attempt} status={status} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "route": {"k": "ch", "v": ["/v1/bounce"]},
                        "upstream": {"k": "ch", "v": ["api_server"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": ["200"]}, "dur_ms": {"k": "i", "v": [8, 250]}},
                        "f": {
                            "status": {"k": "ch", "v": ["200", "502", "503", "504"]},
                            "dur_ms": {"k": "i", "v": [30, 180000]},
                        },
                    },
                },
                "gw_retry": {
                    "lvl": "INFO",
                    "msg": "retry upstream={upstream} attempt={attempt} backoff_ms={backoff_ms} reason={reason} trace={trace_id}",
                    "vars": {
                        "upstream": {"k": "ch", "v": ["api_server"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [80, 2500]},
                        "reason": {"k": "ch", "v": ["upstream_timeout", "upstream_5xx"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "api_server",
            "svc": "api",
            "hosts": ["api-1", "api-2", "api-3", "api-4"],
            "logs": {
                "api_req_start": {
                    "lvl": "INFO",
                    "msg": "start {method} {route} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "route": {"k": "ch", "v": ["/v1/bounce"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_cache_hit": {
                    "lvl": "DEBUG",
                    "msg": "cache hit key={cache_key} ttl_s={ttl_s} trace={trace_id}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "bounce:{team_id}"},
                        "ttl_s": {"k": "i", "v": [1, 900]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_cache_miss": {
                    "lvl": "INFO",
                    "msg": "cache miss key={cache_key} trace={trace_id}",
                    "vars": {"cache_key": {"k": "str", "v": "bounce:{team_id}"}, "trace_id": {"k": "hex", "v": 32}},
                },
                "api_cache_refresh_start": {
                    "lvl": "WARN",
                    "msg": "refreshing key={cache_key} refreshers_inflight={refreshers_inflight} trace={trace_id}",
                    "vars": {"cache_key": {"k": "str", "v": "bounce:{team_id}"}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"refreshers_inflight": {"k": "i", "v": [1, 6]}},
                        "f": {"refreshers_inflight": {"k": "i", "v": [20, 800]}},
                    },
                },
                "api_db_query": {
                    "lvl": "INFO",
                    "msg": "db query name={query} dur_ms={dur_ms} rows={rows} trace={trace_id}",
                    "vars": {
                        "query": {"k": "ch", "v": ["bounce_rate_limit", "bounce_sampling", "bounce_blacklist"]},
                        "rows": {"k": "i", "v": [0, 200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [3, 220]}},
                        "f": {"dur_ms": {"k": "i", "v": [80, 180000]}},
                    },
                },
                "api_resp_ok": {
                    "lvl": "INFO",
                    "msg": "completed status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [8, 300]}},
                        "f": {"dur_ms": {"k": "i", "v": [40, 190000]}},
                    },
                },
                "api_resp_err": {
                    "lvl": "ERROR",
                    "msg": "completed status={status} dur_ms={dur_ms} err={err} trace={trace_id}",
                    "vars": {
                        "status": {"k": "ch", "v": ["502", "503", "504"]},
                        "err": {"k": "ch", "v": ["context deadline exceeded", "db timeout", "upstream overloaded"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [50, 1500]}},
                        "f": {"dur_ms": {"k": "i", "v": [2000, 140000]}},
                    },
                },
                "api_mem_stats": {
                    "lvl": "INFO",
                    "msg": "runtime mem rss_mb={rss_mb} heap_mb={heap_mb} goroutines={goroutines}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "rss_mb": {"k": "i", "v": [220, 850]},
                            "heap_mb": {"k": "i", "v": [140, 650]},
                            "goroutines": {"k": "i", "v": [120, 800]},
                        },
                        "f": {
                            "rss_mb": {"k": "i", "v": [650, 3200]},
                            "heap_mb": {"k": "i", "v": [450, 2500]},
                            "goroutines": {"k": "i", "v": [1500, 18000]},
                        },
                    },
                },
                "api_oom": {
                    "lvl": "CRITICAL",
                    "msg": "runtime out of memory; rss_mb={rss_mb} goroutines={goroutines}",
                    "vars": {"rss_mb": {"k": "i", "v": [1400, 3400]}, "goroutines": {"k": "i", "v": [8000, 20000]}},
                },
                "api_startup": {
                    "lvl": "INFO",
                    "msg": "server started version={version} build={build} warm_cache={warm_cache}",
                    "vars": {
                        "version": {"k": "ch", "v": ["1.42.0"]},
                        "build": {"k": "ch", "v": ["2024-10-04.1"]},
                        "warm_cache": {"k": "ch", "v": ["true", "false"]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "api_mem_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "api_startup", "per_min": 0.02, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "api_mem_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "api_oom", "per_min": 0.25, "scope": "per_host"},
                        {"id": "api_startup", "per_min": 0.25, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "rds_mysql",
            "svc": "rds-mysql",
            "hosts": ["rds-1"],
            "logs": {
                "rds_metrics": {
                    "lvl": "INFO",
                    "msg": "rds metrics cpu_pct={cpu_pct} conns={conns} qps={qps}",
                    "vars": {},
                    "state_vars": {
                        "n": {"cpu_pct": {"k": "i", "v": [25, 60]}, "conns": {"k": "i", "v": [40, 200]}, "qps": {"k": "i", "v": [1, 10]}},
                        "f": {"cpu_pct": {"k": "i", "v": [55, 85]}, "conns": {"k": "i", "v": [120, 900]}, "qps": {"k": "i", "v": [3, 25]}},
                    },
                },
                "rds_metrics_hot": {
                    "lvl": "WARN",
                    "msg": "rds hot cpu_pct={cpu_pct} conns={conns} qps={qps}",
                    "vars": {"cpu_pct": {"k": "i", "v": [85, 95]}, "conns": {"k": "i", "v": [500, 1400]}, "qps": {"k": "i", "v": [4, 18]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rds_metrics", "per_min": 1.0, "scope": "global"}, {"id": "rds_metrics_hot", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "rds_metrics", "per_min": 1.0, "scope": "global"}, {"id": "rds_metrics_hot", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "disk_usage_worker",
            "svc": "disk-usage-worker",
            "hosts": ["du-1"],
            "logs": {
                "du_write_stmt": {
                    "lvl": "INFO",
                    "msg": "disk-usage write dataset_id={dataset_id} bytes={bytes} dur_ms={dur_ms} result={result}",
                    "vars": {"dataset_id": {"k": "i", "v": [1, 4000]}, "bytes": {"k": "i", "v": [1000000, 50000000]}, "result": {"k": "ch", "v": ["ok", "error"]}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [10, 220]}}, "f": {"dur_ms": {"k": "i", "v": [80, 2500]}}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "autoscaler_controller",
            "svc": "autoscaler",
            "hosts": ["asgctl-1"],
            "logs": {
                "asg_status": {
                    "lvl": "INFO",
                    "msg": "asg status desired={desired} in_service={in_service}",
                    "vars": {"desired": {"k": "i", "v": [2, 10]}, "in_service": {"k": "i", "v": [1, 10]}},
                },
                "asg_scale_action": {
                    "lvl": "WARN",
                    "msg": "set desired capacity from {from} to {to} reason={reason}",
                    "vars": {"from": {"k": "i", "v": [6, 10]}, "to": {"k": "i", "v": [2, 5]}, "reason": {"k": "ch", "v": ["reduce_retry_pressure", "shed_load"]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "asg_status", "per_min": 0.5, "scope": "global"}]}, "f": {"emit": [{"id": "asg_status", "per_min": 0.5, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "api_get_bounce_cache_hit",
                    "rpm": 140,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_hit", "api_server.api_resp_ok", "api_gateway.gw_out"],
                    "latency_ms": [[1, 3], [0, 2], [0, 2], [6, 25], [1, 3]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_get_bounce_cache_miss",
                    "rpm": 30,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_miss", "api_server.api_cache_refresh_start", "api_server.api_db_query", "api_server.api_resp_ok", "api_gateway.gw_out"],
                    "latency_ms": [[1, 3], [0, 2], [0, 2], [0, 3], [30, 160], [6, 35], [1, 3]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "disk_usage_write_stmt",
                    "rpm": 2,
                    "emit": ["disk_usage_worker.du_write_stmt"],
                    "latency_ms": [[15, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "api_get_bounce_cache_hit_f",
                    "rpm": 130,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_hit", "api_server.api_resp_ok", "api_gateway.gw_out"],
                    "latency_ms": [[1, 5], [0, 4], [0, 3], [40, 2500], [1, 6]],
                    "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["api_gateway.gw_retry"], "backoff_ms": [[80, 300]]},
                    "trace": True,
                },
                {
                    "id": "api_get_bounce_cache_miss_f",
                    "rpm": 70,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_miss", "api_server.api_cache_refresh_start", "api_server.api_db_query", "api_server.api_resp_ok", "api_gateway.gw_out"],
                    "latency_ms": [[1, 5], [0, 4], [0, 4], [0, 8], [1500, 20000], [50, 2500], [1, 8]],
                    "retry": {"max_attempts": 3, "expected_attempts": 2.0, "emit_per_retry": ["api_gateway.gw_retry"], "backoff_ms": [[120, 500], [250, 900]]},
                    "trace": True,
                },
                {
                    "id": "api_get_bounce_refresh_storm_f",
                    "rpm": 40,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_miss", "api_server.api_cache_refresh_start", "api_server.api_db_query", "api_server.api_resp_ok", "api_gateway.gw_out"],
                    "latency_ms": [[1, 6], [0, 5], [0, 5], [0, 10], [5000, 60000], [80, 6000], [1, 10]],
                    "retry": {"max_attempts": 3, "expected_attempts": 2.1, "emit_per_retry": ["api_gateway.gw_retry"], "backoff_ms": [[150, 650], [320, 1200]]},
                    "trace": True,
                },
                {
                    "id": "api_get_bounce_db_timeout_f",
                    "rpm": 25,
                    "emit": ["api_gateway.gw_in", "api_server.api_req_start", "api_server.api_cache_miss", "api_server.api_cache_refresh_start", "api_server.api_db_query", "api_server.api_resp_err", "api_gateway.gw_out"],
                    "latency_ms": [[1, 6], [0, 5], [0, 5], [0, 12], [20000, 120000], [10, 120], [1, 12]],
                    "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["api_gateway.gw_retry"], "backoff_ms": [[200, 900], [450, 2000]]},
                    "trace": True,
                },
                {
                    "id": "disk_usage_write_stmt_f",
                    "rpm": 2,
                    "emit": ["disk_usage_worker.du_write_stmt"],
                    "latency_ms": [[120, 2200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "rds_clogs_cache_refresh_crash_loops",
        "time": {"total_minutes": 44, "phases": {"n": {"start_min": 0, "end_min": 22}, "f": {"start_min": 22, "end_min": 44}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 22,
                        "rate_multipliers": {
                            "disk_usage_write_stmt_f": 30.0,
                            "api_get_bounce_refresh_storm_f": 0.0,
                            "api_get_bounce_db_timeout_f": 0.0,
                            "rds_mysql.rds_metrics_hot": 0.0,
                            "api_server.api_oom": 0.0,
                            "api_server.api_startup": 0.0,
                        },
                        "latency_multipliers": {"api_get_bounce_cache_miss_f": {"p50": 1.3, "p95": 1.4}},
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 27,
                        "rate_multipliers": {
                            "disk_usage_write_stmt_f": 1.0,
                            "api_get_bounce_refresh_storm_f": 1.0,
                            "api_get_bounce_db_timeout_f": 1.0,
                            "rds_mysql.rds_metrics_hot": 1.0,
                            "api_get_bounce_cache_hit_f": 0.9,
                            "api_get_bounce_cache_miss_f": 1.5,
                        },
                        "latency_multipliers": {
                            "api_get_bounce_cache_miss_f": {"p50": 2.4, "p95": 3.0},
                            "api_get_bounce_refresh_storm_f": {"p50": 2.8, "p95": 3.2},
                            "api_get_bounce_db_timeout_f": {"p50": 1.2, "p95": 1.1},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 32,
                        "rate_multipliers": {
                            "api_server.api_oom": 1.0,
                            "api_server.api_startup": 1.0,
                            "api_get_bounce_cache_hit_f": 0.7,
                            "api_get_bounce_cache_miss_f": 1.8,
                            "api_get_bounce_db_timeout_f": 1.9,
                            "api_get_bounce_refresh_storm_f": 1.4,
                        },
                        "latency_multipliers": {
                            "api_get_bounce_cache_miss_f": {"p50": 2.6, "p95": 3.3},
                            "api_get_bounce_refresh_storm_f": {"p50": 3.1, "p95": 3.6},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 4,
                        "at_min": 38,
                        "rate_multipliers": {
                            "api_get_bounce_cache_hit_f": 0.75,
                            "api_get_bounce_cache_miss_f": 0.75,
                            "api_get_bounce_refresh_storm_f": 0.65,
                            "api_get_bounce_db_timeout_f": 0.75,
                        },
                        "latency_multipliers": {
                            "api_get_bounce_cache_miss_f": {"p50": 0.85, "p95": 0.9},
                            "api_get_bounce_refresh_storm_f": {"p50": 0.85, "p95": 0.9},
                        },
                        "one_shots": [{"ref": "autoscaler_controller.asg_scale_action", "count": 1, "hosts": ["asgctl-1"]}],
                    },
                ]
            }
        },
    }
}

TOKEN_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def hash_to_unit(s: str) -> float:
    h = hashlib.md5(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big")
    return (x + 0.5) / (2**64)


def iso_utc_ms(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def det_jitter_seconds(key: str, max_abs: float) -> float:
    if max_abs <= 0:
        return 0.0
    u = hash_to_unit(key)
    return (u - 0.5) * 2.0 * max_abs


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    p50 = max(0.0, float(p50))
    p95 = max(p50 + 1e-9, float(p95))
    if p50 <= 0:
        p50 = min(1e-6, p95 * 1e-3)
    mu = math.log(p50)
    z95 = 1.6448536269514722
    sigma = max(1e-6, math.log(p95 / p50) / z95)
    z = NormalDist().inv_cdf(min(1.0 - 1e-12, max(1e-12, u)))
    return math.exp(mu + sigma * z)


def sample_lognormal_ms(
    p50: float,
    p95: float,
    key: str,
    cap_mult: float = 3.0,
    hard_min: Optional[int] = None,
    hard_max: Optional[int] = None,
    u_override: Optional[float] = None,
) -> int:
    if float(p50) == 0.0 and float(p95) == 0.0:
        x = 0.0
    else:
        u = hash_to_unit(key) if u_override is None else float(u_override)
        x = lognormal_from_p50_p95(p50, p95, u)
        soft_cap = max(1.0, cap_mult * float(p95))
        x = min(x, soft_cap)
    v = int(round(x))
    if hard_min is not None:
        v = max(int(hard_min), v)
    if hard_max is not None:
        v = min(int(hard_max), v)
    return max(0, v)


def choose_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        arr = list(v)
        idx = int(hash_to_unit(key) * len(arr)) % len(arr)
        return arr[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        span = hi - lo + 1
        return lo + (int(hash_to_unit(key) * span) % span)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + hash_to_unit(key) * (hi - lo)
    if k == "uuid":
        hx = md5_hex("uuid:" + key)
        return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "hex":
        n = int(v)
        return md5_hex("hex:" + key)[:n]
    if k == "ip":
        x = int(hash_to_unit(key) * 254) + 1
        return f"198.51.100.{x}"
    if k == "str":
        if isinstance(v, str):
            return v
        return str(v)
    return ""


def weighted_choice(options: List[Tuple[str, float]], u: float) -> str:
    tot = sum(max(0.0, w) for _, w in options)
    if tot <= 0:
        return options[0][0]
    x = u * tot
    acc = 0.0
    for val, w in options:
        w = max(0.0, w)
        acc += w
        if x <= acc:
            return val
    return options[-1][0]


@dataclass
class Template:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Dict[str, Any]]
    state_vars: Dict[str, Dict[str, Dict[str, Any]]]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Template], Dict[str, Dict[str, Any]]]:
    comp_by_id: Dict[str, Any] = {}
    tpl_by_ref: Dict[str, Template] = {}
    hostsvc_by_comp: Dict[str, Dict[str, Any]] = {}
    for c in system["components"]:
        cid = c["id"]
        comp_by_id[cid] = c
        hostsvc_by_comp[cid] = {"svc": c.get("svc", "") or "", "hosts": list(c.get("hosts", []) or [])}
        for lid, ldef in c.get("logs", {}).items():
            ref = f"{cid}.{lid}"
            tpl_by_ref[ref] = Template(
                component_id=cid,
                log_id=lid,
                level=ldef["lvl"],
                msg=ldef["msg"],
                vars=dict(ldef.get("vars", {}) or {}),
                state_vars=dict(ldef.get("state_vars", {}) or {}),
            )
    return comp_by_id, tpl_by_ref, hostsvc_by_comp


COMP_BY_ID, TPL_BY_REF, HOSTSVC = build_indices(SYSTEM)

FLOW_BY_STATE_ID: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st in ("n", "f"):
    for fdef in SYSTEM["flows"][st]["req"]:
        FLOW_BY_STATE_ID[(st, fdef["id"])] = fdef


def get_int_domain_bounds(ref: str, var: str, state: str) -> Tuple[Optional[int], Optional[int]]:
    tpl = TPL_BY_REF.get(ref)
    if tpl is None:
        return None, None
    dom = None
    if var in (tpl.vars or {}):
        dom = tpl.vars[var]
    if tpl.state_vars and state in tpl.state_vars and var in (tpl.state_vars[state] or {}):
        dom = tpl.state_vars[state][var]
    if not dom or dom.get("k") != "i":
        return None, None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    return lo, hi


def render_message(ref: str, state: str, bound: Dict[str, Any], key_prefix: str) -> Tuple[str, str]:
    tpl = TPL_BY_REF[ref]
    needed = set(TOKEN_RE.findall(tpl.msg))
    vals: Dict[str, Any] = {}

    domains: Dict[str, Dict[str, Any]] = {}
    domains.update(tpl.vars or {})
    if tpl.state_vars and state in tpl.state_vars:
        domains.update(tpl.state_vars[state] or {})

    for name in needed:
        if name in bound:
            vals[name] = bound[name]
            continue
        if name in domains:
            vals[name] = choose_from_domain(domains[name], f"{key_prefix}:{ref}:{name}")
        else:
            vals[name] = ""

    for k, v in list(vals.items()):
        if isinstance(v, str) and "{team_id}" in v:
            team_id = bound.get("team_id")
            if team_id is None:
                team_id = 1000 + (int(hash_to_unit(f"{key_prefix}:team") * 9000) % 9000)
            vals[k] = v.replace("{team_id}", str(team_id))

    return tpl.level, tpl.msg.format(**vals)


def schedule_times_within_minute(base: datetime, minute: int, count: int, key: str, host_offset: float = 0.0) -> List[datetime]:
    if count <= 0:
        return []
    start = base + timedelta(minutes=minute)
    dt = 60.0 / count
    times: List[datetime] = []
    for i in range(count):
        center = (i + 0.5) * dt + host_offset
        jit = det_jitter_seconds(f"{key}:m{minute}:i{i}", max_abs=min(0.15 * dt, 0.45))
        t = start + timedelta(seconds=center + jit)
        times.append(t)
    return times


def stable_count(expected: float, key: str, carry: Dict[str, float]) -> int:
    c = carry.get(key, 0.0)
    x = expected + c
    n = int(math.floor(x + 1e-12))
    carry[key] = x - n
    return n


def pick_component_host(component_id: str, chain_key: str) -> str:
    hosts = HOSTSVC[component_id]["hosts"]
    if not hosts:
        return ""
    idx = int(hash_to_unit(f"host:{component_id}:{chain_key}") * len(hosts)) % len(hosts)
    return hosts[idx]


def build_failure_intervals(scenario: Dict[str, Any]) -> List[Dict[str, Any]]:
    ph = scenario["scenario"]["time"]["phases"]
    f_start = ph["f"]["start_min"]
    f_end = ph["f"]["end_min"]
    events = list(scenario["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    intervals: List[Dict[str, Any]] = []
    cur_rate: Dict[str, float] = {}
    cur_lat: Dict[str, Dict[str, float]] = {}

    for ev in events:
        at = ev["at_min"]
        if at < f_start or at >= f_end:
            continue
        if intervals and intervals[-1]["end_min"] is None:
            intervals[-1]["end_min"] = at

        for k, mult in (ev.get("rate_multipliers", {}) or {}).items():
            cur_rate[k] = float(mult)
        for k, multpair in (ev.get("latency_multipliers", {}) or {}).items():
            cur_lat[k] = {"p50": float(multpair["p50"]), "p95": float(multpair["p95"])}

        intervals.append({"start_min": at, "end_min": None, "rate": dict(cur_rate), "lat": dict(cur_lat), "one_shots": list(ev.get("one_shots", []) or [])})

    if intervals:
        intervals[-1]["end_min"] = f_end
    else:
        intervals.append({"start_min": f_start, "end_min": f_end, "rate": {}, "lat": {}, "one_shots": []})

    if intervals[0]["start_min"] > f_start:
        intervals.insert(0, {"start_min": f_start, "end_min": intervals[0]["start_min"], "rate": {}, "lat": {}, "one_shots": []})

    return intervals


FAIL_INTERVALS = build_failure_intervals(SCENARIO)


def active_failure_controls(minute: int) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    for it in FAIL_INTERVALS:
        if it["start_min"] <= minute < it["end_min"]:
            return it["rate"], it["lat"]
    return {}, {}


def state_for_minute(minute: int) -> str:
    ph = SCENARIO["scenario"]["time"]["phases"]
    if ph["n"]["start_min"] <= minute < ph["n"]["end_min"]:
        return "n"
    return "f"


def compute_attempts(expected_attempts: float, max_attempts: int, key: str) -> int:
    max_attempts = max(1, int(max_attempts))
    e = max(1.0, float(expected_attempts))
    e = min(e, float(max_attempts))
    a0 = int(math.floor(e))
    a1 = int(math.ceil(e))
    if a0 == a1:
        return a0
    if a1 > max_attempts:
        a1 = max_attempts
        a0 = max(1, a1 - 1)
        if a0 == a1:
            return a0
    p = e - a0
    u = hash_to_unit(f"{key}:attempts")
    return a1 if u < p else a0


def flow_latency_multiplier(flow_id: str, lat_ctrl: Dict[str, Dict[str, float]]) -> Tuple[float, float]:
    mp = lat_ctrl.get(flow_id)
    if not mp:
        return 1.0, 1.0
    return float(mp.get("p50", 1.0)), float(mp.get("p95", 1.0))


def build_flow_bound_context(flow: Dict[str, Any], state: str, minute: int, instance_key: str) -> Dict[str, Any]:
    bound: Dict[str, Any] = {}
    bound["method"] = "GET"
    bound["route"] = "/v1/bounce"
    bound["upstream"] = "api_server"
    bound["req_id"] = choose_from_domain({"k": "uuid", "v": None}, f"{instance_key}:reqid")
    bound["team_id"] = 1000 + (int(hash_to_unit(f"{instance_key}:team") * 40000) % 40000)
    bound["cache_key"] = f"bounce:{bound['team_id']}"
    bound["query"] = choose_from_domain({"k": "ch", "v": ["bounce_rate_limit", "bounce_sampling", "bounce_blacklist"]}, f"{instance_key}:query")
    bound["rows"] = choose_from_domain({"k": "i", "v": [0, 200]}, f"{instance_key}:rows")
    bound["minute"] = minute
    bound["state"] = state
    return bound


def choose_refreshers_inflight(state: str, flow_id: str, instance_key: str) -> int:
    tpl = TPL_BY_REF["api_server.api_cache_refresh_start"]
    dom = (tpl.state_vars or {}).get(state, {}).get("refreshers_inflight", {"k": "i", "v": [1, 1]})
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    u = hash_to_unit(f"{instance_key}:refreshers")
    if flow_id == "api_get_bounce_refresh_storm_f":
        u = min(1.0, 0.55 + 0.45 * u)
    elif flow_id in ("api_get_bounce_cache_miss_f", "api_get_bounce_db_timeout_f"):
        u = min(1.0, 0.25 + 0.75 * u)
    else:
        u = min(1.0, 0.10 + 0.90 * u)
    return lo + int(u * (hi - lo))


def flow_emits_api_ok(flow: Dict[str, Any]) -> bool:
    return any(ref == "api_server.api_resp_ok" for ref in flow.get("emit", []))


def flow_emits_api_err(flow: Dict[str, Any]) -> bool:
    return any(ref == "api_server.api_resp_err" for ref in flow.get("emit", []))


def choose_gateway_status_for_attempt(flow: Dict[str, Any], state: str, attempt: int, planned_attempts: int, instance_key: str) -> str:
    # Retry semantics: gateway retries only after a failed upstream attempt. We therefore make
    # attempts 1..planned_attempts-1 "fail" and the final attempt succeed iff the flow's emit chain
    # models success (api_resp_ok). This keeps retries conditional on failure while preserving
    # expected_attempts in the batch.
    if state == "n":
        return "200"

    if flow_emits_api_err(flow):
        u = hash_to_unit(f"{instance_key}:gwstatus:{attempt}")
        return weighted_choice([("504", 0.60), ("503", 0.25), ("502", 0.15)], u)

    # api_resp_ok flows: if we are going to retry, earlier attempts must be failures.
    if attempt < planned_attempts:
        return "504"
    return "200"


def choose_api_err_for_status(status: str, instance_key: str) -> str:
    if status == "504":
        return "context deadline exceeded" if hash_to_unit(f"{instance_key}:errpick") < 0.5 else "db timeout"
    if status in ("502", "503"):
        return "upstream overloaded"
    return "db timeout"


def enforce_duration_bounds_on_delays(
    delays_ms: List[int],
    emit_refs: List[str],
    state: str,
    gw_in_idx: Optional[int],
    gw_out_idx: Optional[int],
    api_req_idx: Optional[int],
    api_resp_idx: Optional[int],
) -> List[int]:
    delays = list(int(x) for x in delays_ms)

    def affecting_indices(i0: Optional[int], i1: Optional[int]) -> List[int]:
        if i0 is None or i1 is None:
            return []
        if i1 <= i0:
            return []
        return list(range(i0 + 1, i1 + 1))

    min_per_delay = [0 for _ in delays]
    for i, ref in enumerate(emit_refs):
        if ref in ("api_server.api_db_query", "disk_usage_worker.du_write_stmt"):
            lo, _hi = get_int_domain_bounds(ref, "dur_ms", state)
            if lo is not None:
                min_per_delay[i] = int(lo)

    def duration_sum(i0: Optional[int], i1: Optional[int]) -> Optional[int]:
        if i0 is None or i1 is None or i1 <= i0:
            return None
        return int(sum(delays[i0 + 1 : i1 + 1]))

    def reduce_duration_to_max(i0: Optional[int], i1: Optional[int], target_max: Optional[int], pref: List[int]) -> bool:
        if target_max is None:
            return False
        cur = duration_sum(i0, i1)
        if cur is None or cur <= target_max:
            return False
        over = cur - int(target_max)
        changed = False
        for idx in pref:
            if over <= 0:
                break
            if idx < 0 or idx >= len(delays):
                continue
            if i0 is None or i1 is None or not (i0 < idx <= i1):
                continue
            reducible = max(0, delays[idx] - min_per_delay[idx])
            take = min(over, reducible)
            if take > 0:
                delays[idx] -= int(take)
                over -= int(take)
                changed = True
        return changed

    def raise_duration_to_min(i0: Optional[int], i1: Optional[int], target_min: Optional[int], add_idx: Optional[int]) -> bool:
        if target_min is None:
            return False
        cur = duration_sum(i0, i1)
        if cur is None or cur >= target_min:
            return False
        under = int(target_min) - cur
        if under <= 0:
            return False
        aff = affecting_indices(i0, i1)
        if not aff:
            return False
        idx = add_idx if (add_idx is not None and add_idx in aff) else aff[-1]
        delays[idx] += int(under)
        return True

    idx_db = None
    for i, ref in enumerate(emit_refs):
        if ref == "api_server.api_db_query":
            idx_db = i
            break

    api_aff = affecting_indices(api_req_idx, api_resp_idx)
    gw_aff = affecting_indices(gw_in_idx, gw_out_idx)

    api_pref: List[int] = []
    if idx_db is not None and idx_db in api_aff:
        api_pref.append(idx_db)
    if api_resp_idx is not None and api_resp_idx in api_aff:
        api_pref.append(api_resp_idx)
    api_pref.extend([i for i in reversed(api_aff) if i not in api_pref])

    post_api: List[int] = []
    overlap: List[int] = []
    if gw_aff:
        for i in gw_aff:
            if api_resp_idx is not None and i > api_resp_idx:
                post_api.append(i)
            else:
                overlap.append(i)
    gw_pref: List[int] = list(reversed(post_api)) + list(reversed(overlap))

    api_lo = api_hi = None
    if api_resp_idx is not None and api_req_idx is not None:
        resp_ref = emit_refs[api_resp_idx]
        api_lo, api_hi = get_int_domain_bounds(resp_ref, "dur_ms", state)

    gw_lo = gw_hi = None
    if gw_in_idx is not None and gw_out_idx is not None:
        gw_lo, gw_hi = get_int_domain_bounds("api_gateway.gw_out", "dur_ms", state)

    for _ in range(6):
        changed = False
        changed |= reduce_duration_to_max(api_req_idx, api_resp_idx, api_hi, api_pref)
        changed |= reduce_duration_to_max(gw_in_idx, gw_out_idx, gw_hi, gw_pref)
        changed |= raise_duration_to_min(api_req_idx, api_resp_idx, api_lo, api_resp_idx)
        changed |= raise_duration_to_min(gw_in_idx, gw_out_idx, gw_lo, gw_out_idx)
        if not changed:
            break

    return delays


def compute_attempts_for_flow(flow: Dict[str, Any], instance_key: str) -> int:
    retry_cfg = flow.get("retry", {}) or {}
    max_attempts = int(retry_cfg.get("max_attempts", 1))
    expected_attempts = float(retry_cfg.get("expected_attempts", 1.0))
    return compute_attempts(expected_attempts, max_attempts, instance_key)


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_time: datetime,
    minute: int,
    lat_ctrl: Dict[str, Dict[str, float]],
    instance_id: int,
    rows: List[Dict[str, str]],
) -> None:
    flow_id = flow["id"]
    instance_key = f"{flow_id}:{state}:m{minute}:inst{instance_id}"

    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow.get("trace", False):
        trace_id = md5_hex(f"trace:{instance_key}")[:32]

    emit_refs: List[str] = list(flow["emit"])
    comp_hosts: Dict[str, str] = {}
    for ref in emit_refs + list(flow.get("retry", {}).get("emit_per_retry", []) or []):
        cid = ref.split(".", 1)[0]
        if cid not in comp_hosts:
            comp_hosts[cid] = pick_component_host(cid, instance_key)

    lm50, lm95 = (1.0, 1.0)
    if state == "f":
        lm50, lm95 = flow_latency_multiplier(flow_id, lat_ctrl)

    bound_req = build_flow_bound_context(flow, state, minute, instance_key)
    bound_req["trace_id"] = trace_id

    retry_cfg = flow.get("retry", {}) or {}
    planned_attempts = compute_attempts_for_flow(flow, instance_key)
    backoff_pairs = list(retry_cfg.get("backoff_ms", []) or [])
    retry_emit = list(retry_cfg.get("emit_per_retry", []) or [])

    gw_in_idx = None
    gw_out_idx = None
    api_req_idx = None
    api_dbq_idx = None
    api_resp_idx = None
    for i, ref in enumerate(emit_refs):
        if ref == "api_gateway.gw_in":
            gw_in_idx = i
        elif ref == "api_gateway.gw_out":
            gw_out_idx = i
        elif ref == "api_server.api_req_start":
            api_req_idx = i
        elif ref == "api_server.api_db_query":
            api_dbq_idx = i
        elif ref in ("api_server.api_resp_ok", "api_server.api_resp_err"):
            api_resp_idx = i

    def sum_between_from_delays(delays: List[int], i0: Optional[int], i1: Optional[int]) -> int:
        if i0 is None or i1 is None or i1 <= i0:
            return 0
        return int(sum(delays[i0 + 1 : i1 + 1]))

    attempt_start = start_time
    prev_gw_out_time: Optional[datetime] = None

    for a in range(1, planned_attempts + 1):
        attempt_key = f"{instance_key}:att{a}"
        gw_status = choose_gateway_status_for_attempt(flow, state, a, planned_attempts, instance_key)
        fail_attempt = (state == "f" and gw_status != "200")

        delays_ms: List[int] = []
        for li, (p50, p95) in enumerate(flow["latency_ms"]):
            sp50 = float(p50) * lm50
            sp95 = float(p95) * lm95

            hard_min = None
            hard_max = None
            ref = emit_refs[li]
            if ref in ("api_server.api_db_query", "disk_usage_worker.du_write_stmt"):
                lo, hi = get_int_domain_bounds(ref, "dur_ms", state)
                hard_min, hard_max = lo, hi

            u_override = None
            if fail_attempt and (sp95 > 0.0 or sp50 > 0.0):
                u0 = hash_to_unit(f"{attempt_key}:lat{li}")
                u_override = 0.85 + 0.15 * u0

            d = sample_lognormal_ms(
                sp50,
                sp95,
                f"{attempt_key}:lat{li}",
                cap_mult=3.0,
                hard_min=hard_min,
                hard_max=hard_max,
                u_override=u_override,
            )
            delays_ms.append(d)

        delays_ms = enforce_duration_bounds_on_delays(delays_ms, emit_refs, state, gw_in_idx, gw_out_idx, api_req_idx, api_resp_idx)

        # Coherence tweak for retry-causing failures:
        # If gateway status indicates timeout (504), ensure the gateway-observed duration is not trivially tiny
        # and is at least as long as the API handler duration (e.g., response lost / not observed by gateway).
        if state == "f" and gw_status == "504" and gw_in_idx is not None and gw_out_idx is not None:
            api_dur = sum_between_from_delays(delays_ms, api_req_idx, api_resp_idx)
            gw_dur = sum_between_from_delays(delays_ms, gw_in_idx, gw_out_idx)
            desired_min = max(2500, api_dur + 50)
            gw_lo, gw_hi = get_int_domain_bounds("api_gateway.gw_out", "dur_ms", state)
            if gw_hi is None:
                gw_hi = 10**9
            desired_min = min(int(gw_hi), int(desired_min))
            if gw_dur < desired_min:
                add = desired_min - gw_dur
                delays_ms[gw_out_idx] += int(add)
                # Clamp if we overshot gw_hi by this adjustment (rare).
                gw_dur2 = sum_between_from_delays(delays_ms, gw_in_idx, gw_out_idx)
                if gw_dur2 > gw_hi:
                    delays_ms[gw_out_idx] -= int(gw_dur2 - gw_hi)

        ts_list: List[datetime] = []
        t = attempt_start
        for dms in delays_ms:
            t = t + timedelta(milliseconds=int(dms))
            ts_list.append(t)

        gw_dur_ms = sum_between_from_delays(delays_ms, gw_in_idx, gw_out_idx)
        api_dur_ms = sum_between_from_delays(delays_ms, api_req_idx, api_resp_idx)
        db_dur_ms = int(delays_ms[api_dbq_idx]) if api_dbq_idx is not None else None

        bound_attempt = dict(bound_req)
        bound_attempt["attempt"] = a
        bound_attempt["status"] = gw_status
        bound_attempt["dur_ms"] = gw_dur_ms
        bound_attempt["client_ip"] = choose_from_domain({"k": "ip", "v": None}, f"{attempt_key}:clientip")
        bound_attempt["ttl_s"] = choose_from_domain({"k": "i", "v": [1, 900]}, f"{attempt_key}:ttl")
        bound_attempt["refreshers_inflight"] = choose_refreshers_inflight(state, flow_id, attempt_key)

        for i, ref in enumerate(emit_refs):
            cid, _ = ref.split(".", 1)
            svc = HOSTSVC[cid]["svc"]
            host = comp_hosts.get(cid, "")

            b = dict(bound_attempt)

            if ref == "api_server.api_db_query":
                b["dur_ms"] = int(db_dur_ms if db_dur_ms is not None else 0)
            elif ref == "disk_usage_worker.du_write_stmt":
                b["dur_ms"] = int(delays_ms[i])
            elif ref in ("api_server.api_resp_ok", "api_server.api_resp_err"):
                b["dur_ms"] = int(api_dur_ms)

            if ref == "api_gateway.gw_out":
                b["dur_ms"] = int(gw_dur_ms)
                b["status"] = gw_status

            if ref == "api_server.api_resp_err":
                b["status"] = gw_status if gw_status in ("502", "503", "504") else "504"
                b["err"] = choose_api_err_for_status(b["status"], attempt_key)

            if ref in ("api_server.api_cache_miss", "api_server.api_cache_hit", "api_server.api_cache_refresh_start"):
                b["cache_key"] = bound_req["cache_key"]

            if ref == "api_server.api_db_query":
                b["query"] = bound_req["query"]
                b["rows"] = choose_from_domain({"k": "i", "v": [0, 200]}, f"{attempt_key}:rows")

            level, msg = render_message(ref, state, b, attempt_key)
            rows.append(
                {
                    "timestamp": iso_utc_ms(ts_list[i]),
                    "level": level,
                    "message": msg,
                    "trace_id": trace_id if flow.get("trace", False) else "",
                    "service": svc,
                    "host": host,
                }
            )

        prev_gw_out_time = ts_list[gw_out_idx] if gw_out_idx is not None else ts_list[-1]

        # Retry only after a failed attempt.
        if a < planned_attempts:
            if gw_status == "200":
                break

            if a - 1 < len(backoff_pairs):
                bp50, bp95 = backoff_pairs[a - 1]
            else:
                bp50, bp95 = (200, 900)

            bo_lo, bo_hi = get_int_domain_bounds("api_gateway.gw_retry", "backoff_ms", state)
            backoff_ms = sample_lognormal_ms(float(bp50), float(bp95), f"{attempt_key}:backoff", cap_mult=3.0, hard_min=bo_lo, hard_max=bo_hi)

            reason = "upstream_timeout" if gw_status == "504" else "upstream_5xx"

            for rr in retry_emit:
                rcid, _ = rr.split(".", 1)
                svc = HOSTSVC[rcid]["svc"]
                host = comp_hosts.get(rcid, "")
                retry_time = prev_gw_out_time + timedelta(milliseconds=1)

                b = dict(bound_req)
                b["attempt"] = a + 1
                b["backoff_ms"] = int(backoff_ms)
                b["reason"] = reason
                b["trace_id"] = trace_id

                level, msg = render_message(rr, state, b, attempt_key)
                rows.append(
                    {
                        "timestamp": iso_utc_ms(retry_time),
                        "level": level,
                        "message": msg,
                        "trace_id": trace_id if flow.get("trace", False) else "",
                        "service": svc,
                        "host": host,
                    }
                )

            attempt_start = prev_gw_out_time + timedelta(milliseconds=int(backoff_ms))


def simulate_background_minute(base: datetime, minute: int, state: str, rate_ctrl: Dict[str, float], carry: Dict[str, float], rows: List[Dict[str, str]]) -> None:
    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = (comp.get("beh", {}) or {}).get(state, {}) or {}
        emits = list(beh.get("emit", []) or [])
        for e in emits:
            log_id = e["id"]
            per_min = float(e.get("per_min", 0.0))
            scope = e.get("scope", "per_host") or "per_host"
            ref = f"{cid}.{log_id}"

            mult = 1.0
            if state == "f":
                mult = float(rate_ctrl.get(ref, 1.0))
            eff = per_min * mult
            if eff <= 0.0:
                continue

            hosts = HOSTSVC[cid]["hosts"]
            svc = HOSTSVC[cid]["svc"]

            if scope == "global":
                key = f"bg:{ref}:global"
                cnt = stable_count(eff, key, carry)
                times = schedule_times_within_minute(base, minute, cnt, key)
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    bound: Dict[str, Any] = {}
                    if ref == "autoscaler_controller.asg_status":
                        desired = choose_from_domain(TPL_BY_REF[ref].vars["desired"], f"{key}:m{minute}:i{i}:desired")
                        in_service = min(int(desired), choose_from_domain(TPL_BY_REF[ref].vars["in_service"], f"{key}:m{minute}:i{i}:in_service"))
                        bound["desired"] = desired
                        bound["in_service"] = in_service
                    elif ref == "api_server.api_startup":
                        bound["warm_cache"] = "true" if state == "n" else "false"
                    else:
                        if ref == "api_server.api_mem_stats":
                            sv = TPL_BY_REF[ref].state_vars.get(state, {})
                            rss_lo, rss_hi = sv["rss_mb"]["v"]
                            heap_lo, heap_hi = sv["heap_mb"]["v"]
                            g_lo, g_hi = sv["goroutines"]["v"]
                            frac = 0.25
                            if state == "f":
                                frac = min(0.95, 0.45 + 0.02 * (minute - SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]))
                            rss = int(rss_lo + frac * (rss_hi - rss_lo))
                            heap = int(heap_lo + frac * (heap_hi - heap_lo))
                            heap = min(heap, rss)
                            gor = int(g_lo + frac * (g_hi - g_lo))
                            bound.update({"rss_mb": rss, "heap_mb": heap, "goroutines": gor})
                        if ref == "rds_mysql.rds_metrics":
                            sv = TPL_BY_REF[ref].state_vars.get(state, {})
                            frac = 0.5 if state == "n" else min(0.95, 0.65 + 0.015 * (minute - SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]))
                            cpu = int(sv["cpu_pct"]["v"][0] + frac * (sv["cpu_pct"]["v"][1] - sv["cpu_pct"]["v"][0]))
                            conns = int(sv["conns"]["v"][0] + frac * (sv["conns"]["v"][1] - sv["conns"]["v"][0]))
                            qps = int(sv["qps"]["v"][0] + (0.4 + 0.3 * frac) * (sv["qps"]["v"][1] - sv["qps"]["v"][0]))
                            bound.update({"cpu_pct": cpu, "conns": conns, "qps": qps})

                    level, msg = render_message(ref, state, bound, f"{key}:m{minute}:i{i}")
                    rows.append({"timestamp": iso_utc_ms(t), "level": level, "message": msg, "trace_id": "", "service": svc, "host": host})
            else:
                for h_idx, host in enumerate(hosts if hosts else [""]):
                    key = f"bg:{ref}:host:{host}"
                    cnt = stable_count(eff, key, carry)
                    host_offset = (h_idx / max(1, len(hosts))) * 0.75
                    times = schedule_times_within_minute(base, minute, cnt, key, host_offset=host_offset)
                    for i, t in enumerate(times):
                        bound: Dict[str, Any] = {}
                        if ref == "api_server.api_startup":
                            bound["warm_cache"] = "true" if state == "n" else "false"
                        if ref == "api_server.api_mem_stats":
                            sv = TPL_BY_REF[ref].state_vars.get(state, {})
                            rss_lo, rss_hi = sv["rss_mb"]["v"]
                            heap_lo, heap_hi = sv["heap_mb"]["v"]
                            g_lo, g_hi = sv["goroutines"]["v"]
                            frac = 0.25
                            if state == "f":
                                frac = min(0.97, 0.50 + 0.02 * (minute - SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]))
                            frac = min(0.99, max(0.05, frac + (hash_to_unit(f"{key}:m{minute}:i{i}:var") - 0.5) * 0.06))
                            rss = int(rss_lo + frac * (rss_hi - rss_lo))
                            heap = int(heap_lo + frac * (heap_hi - heap_lo))
                            heap = min(heap, rss)
                            gor = int(g_lo + frac * (g_hi - g_lo))
                            bound.update({"rss_mb": rss, "heap_mb": heap, "goroutines": gor})

                        level, msg = render_message(ref, state, bound, f"{key}:m{minute}:i{i}")
                        rows.append({"timestamp": iso_utc_ms(t), "level": level, "message": msg, "trace_id": "", "service": svc, "host": host})


def emit_one_shots(base: datetime, rows: List[Dict[str, str]]) -> None:
    for it in FAIL_INTERVALS:
        start_min = it["start_min"]
        for os in it.get("one_shots", []) or []:
            ref = os["ref"]
            count = int(os["count"])
            hosts = list(os.get("hosts", []) or [])
            cid = ref.split(".", 1)[0]
            svc = HOSTSVC[cid]["svc"]
            if not hosts:
                hosts = HOSTSVC[cid]["hosts"] or [""]

            for i in range(count):
                t0 = base + timedelta(minutes=start_min)
                jit = det_jitter_seconds(f"oneshot:{ref}:m{start_min}:i{i}", max_abs=0.9)
                t = t0 + timedelta(seconds=0.2 + jit)
                host = hosts[i % len(hosts)]

                bound: Dict[str, Any] = {}
                if ref == "autoscaler_controller.asg_scale_action":
                    from_dom = TPL_BY_REF[ref].vars["from"]
                    to_dom = TPL_BY_REF[ref].vars["to"]
                    frm = int(choose_from_domain(from_dom, f"oneshot:{ref}:m{start_min}:i{i}:from"))
                    to = int(choose_from_domain(to_dom, f"oneshot:{ref}:m{start_min}:i{i}:to"))
                    if to >= frm:
                        to = max(2, min(frm - 1, to))
                    bound["from"] = frm
                    bound["to"] = to

                level, msg = render_message(ref, "f", bound, f"oneshot:{ref}:m{start_min}:i{i}")
                rows.append({"timestamp": iso_utc_ms(t), "level": level, "message": msg, "trace_id": "", "service": svc, "host": host})


def main() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    total_minutes = int(SCENARIO["scenario"]["time"]["total_minutes"])

    rows: List[Dict[str, str]] = []
    carry: Dict[str, float] = {}
    flow_instances: List[Tuple[datetime, str, int, str, Dict[str, Dict[str, float]]]] = []

    for m in range(total_minutes):
        st = state_for_minute(m)
        rate_ctrl: Dict[str, float] = {}
        lat_ctrl: Dict[str, Dict[str, float]] = {}
        if st == "f":
            rate_ctrl, lat_ctrl = active_failure_controls(m)

        simulate_background_minute(base, m, st, rate_ctrl, carry, rows)

        flows = SYSTEM["flows"][st]["req"]
        for fdef in flows:
            flow_id = fdef["id"]
            rpm = float(fdef["rpm"])
            mult = 1.0
            if st == "f":
                mult = float(rate_ctrl.get(flow_id, 1.0))
            eff_rpm = rpm * mult
            if eff_rpm <= 0.0:
                continue
            cnt = stable_count(eff_rpm, f"flow:{st}:{flow_id}", carry)
            start_times = schedule_times_within_minute(base, m, cnt, f"flowstart:{st}:{flow_id}")
            for t in start_times:
                flow_instances.append((t, st, m, flow_id, lat_ctrl))

    flow_instances.sort(key=lambda x: x[0])

    for inst_id, (t0, st, m, flow_id, lat_ctrl) in enumerate(flow_instances, start=1):
        fdef = FLOW_BY_STATE_ID[(st, flow_id)]
        simulate_flow_instance(fdef, st, t0, m, lat_ctrl, inst_id, rows)

    emit_one_shots(base, rows)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values(by="timestamp", kind="mergesort").reset_index(drop=True)
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
