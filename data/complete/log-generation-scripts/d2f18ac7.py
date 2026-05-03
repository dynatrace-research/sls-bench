import math
import hashlib
import uuid
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "payments_api_dashboard_stack"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_app", "monitoring"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_lb",
            "svc": "nginx",
            "hosts": ["lb1", "lb2"],
            "logs": {
                "lb_stats": {
                    "lvl": "INFO",
                    "msg": "lb stats active_conns={active_conns} upstream_5xx={up_5xx} req_rate_rps={rps}",
                    "vars": {
                        "active_conns": {"k": "i", "v": [50, 900]},
                        "up_5xx": {"k": "i", "v": [0, 80]},
                        "rps": {"k": "i", "v": [5, 25]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "lb_stats", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "lb_stats", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "api_app",
            "svc": "rails-app",
            "hosts": ["app1", "app2", "app3"],
            "logs": {
                "req_done_2xx": {
                    "lvl": "INFO",
                    "msg": "req {method} {route} completed status=200 dur_ms={dur_ms} db_ms={db_ms} request_id={request_id} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/api/payments", "/api/mandates", "/dashboard"]},
                        "request_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [20, 220]}, "db_ms": {"k": "i", "v": [2, 45]}},
                        "f": {"dur_ms": {"k": "i", "v": [40, 900]}, "db_ms": {"k": "i", "v": [4, 260]}},
                    },
                },
                "req_done_503": {
                    "lvl": "WARN",
                    "msg": "req {method} {route} completed status=503 dur_ms={dur_ms} err=DBUnavailable request_id={request_id} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/api/payments", "/api/mandates", "/dashboard"]},
                        "dur_ms": {"k": "i", "v": [600, 4500]},
                        "request_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "db_connect_err_vip": {
                    "lvl": "ERROR",
                    "msg": "db connect failed host=10.0.0.10:5432 err={err} timeout_ms={timeout_ms} request_id={request_id} trace_id={trace_id}",
                    "vars": {
                        "err": {"k": "ch", "v": ["timeout", "connection_refused", "no_route"]},
                        "timeout_ms": {"k": "i", "v": [800, 1600]},
                        "request_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "db_connect_err_direct": {
                    "lvl": "ERROR",
                    "msg": "db connect failed host=10.0.0.22:5432 err={err} timeout_ms={timeout_ms} request_id={request_id} trace_id={trace_id}",
                    "vars": {
                        "err": {"k": "ch", "v": ["timeout", "too_many_connections"]},
                        "timeout_ms": {"k": "i", "v": [600, 1600]},
                        "request_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "config_db_host_override": {
                    "lvl": "WARN",
                    "msg": "set DB_HOST override to 10.0.0.22:5432 reason={reason} change_id={change_id}",
                    "vars": {"reason": {"k": "ch", "v": ["manual_failover"]}, "change_id": {"k": "hex", "v": 12}},
                },
                "housekeeping_tick": {
                    "lvl": "DEBUG",
                    "msg": "housekeeping tick job={job} dur_ms={dur_ms}",
                    "vars": {"job": {"k": "ch", "v": ["cache_warm", "metrics_flush"]}, "dur_ms": {"k": "i", "v": [5, 80]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "housekeeping_tick", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": []},
            },
        },
        {
            "id": "pacemaker",
            "svc": "pacemaker",
            "hosts": ["db1", "db2", "db3"],
            "logs": {
                "cluster_status": {
                    "lvl": "INFO",
                    "msg": "cluster status: dc={dc} quorum={quorum} primary_res={primary_res} vip_postgres={vip_postgres} vip_backup={vip_backup} vip_backup_node={vip_node}",
                    "vars": {
                        "dc": {"k": "ch", "v": ["db1", "db2", "db3"]},
                        "quorum": {"k": "ch", "v": ["yes"]},
                        "primary_res": {"k": "ch", "v": ["pg_primary"]},
                        "vip_postgres": {"k": "ch", "v": ["vip_postgres"]},
                        "vip_backup": {"k": "ch", "v": ["vip_backup"]},
                    },
                    "state_vars": {
                        "n": {"vip_node": {"k": "ch", "v": ["db2", "db3"]}},
                        "f": {"vip_node": {"k": "ch", "v": ["db2"]}},
                    },
                },
                "promote_attempt": {
                    "lvl": "WARN",
                    "msg": "attempting promote resource={resource} from={from_node} to={to_node} result=pending",
                    "vars": {
                        "resource": {"k": "ch", "v": ["pg_primary"]},
                        "from_node": {"k": "ch", "v": ["db1"]},
                        "to_node": {"k": "ch", "v": ["db2"]},
                    },
                },
                "promote_failed_constraints": {
                    "lvl": "ERROR",
                    "msg": "promote failed resource={resource} candidate={candidate} reason=constraints colocation_rule={coloc_rule} stickiness={stickiness}",
                    "vars": {
                        "resource": {"k": "ch", "v": ["pg_primary"]},
                        "candidate": {"k": "ch", "v": ["db2"]},
                        "coloc_rule": {"k": "ch", "v": ["vip_backup -INF with pg_primary"]},
                        "stickiness": {"k": "i", "v": [100, 200]},
                    },
                },
                "vip_not_managed": {
                    "lvl": "WARN",
                    "msg": "maintenance-mode=true; skipping resource management for VIPs and Postgres",
                    "vars": {},
                },
                "crm_resource_cleanup": {
                    "lvl": "INFO",
                    "msg": "crm resource cleanup invoked resource={resource} by={user}",
                    "vars": {"resource": {"k": "ch", "v": ["pg_primary", "vip_postgres"]}, "user": {"k": "ch", "v": ["sre"]}},
                },
                "maintenance_mode_enabled": {"lvl": "WARN", "msg": "crm property maintenance-mode=true by={user}", "vars": {"user": {"k": "ch", "v": ["sre"]}}},
                "maintenance_mode_disabled": {"lvl": "WARN", "msg": "crm property maintenance-mode=false by={user}", "vars": {"user": {"k": "ch", "v": ["sre"]}}},
                "node_fenced": {"lvl": "WARN", "msg": "node {node} fenced action={action} result=success", "vars": {"node": {"k": "ch", "v": ["db1"]}, "action": {"k": "ch", "v": ["poweroff"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_status", "per_min": 0.35, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "cluster_status", "per_min": 0.35, "scope": "global"},
                        {"id": "promote_attempt", "per_min": 3.0, "scope": "global"},
                        {"id": "promote_failed_constraints", "per_min": 3.0, "scope": "global"},
                        {"id": "vip_not_managed", "per_min": 0.8, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "postgres_primary",
            "svc": "postgres",
            "hosts": ["db1"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete wrote={buffers} buffers dur_ms={dur_ms}",
                    "vars": {"buffers": {"k": "i", "v": [200, 2000]}, "dur_ms": {"k": "i", "v": [30, 250]}},
                },
                "raid_disk_loss": {
                    "lvl": "CRITICAL",
                    "msg": "raid controller: simultaneous loss of {disk_count} disks; array degraded",
                    "vars": {"disk_count": {"k": "i", "v": [3, 3]}},
                },
                "raid_io_error": {
                    "lvl": "CRITICAL",
                    "msg": "raid I/O error op={op} sector={sector} errno={errno}",
                    "vars": {"op": {"k": "ch", "v": ["read", "write"]}, "sector": {"k": "i", "v": [100000, 900000]}, "errno": {"k": "ch", "v": ["EIO"]}},
                },
                "fs_remount_ro": {
                    "lvl": "CRITICAL",
                    "msg": "filesystem error; remounting read-only device={dev}",
                    "vars": {"dev": {"k": "ch", "v": ["/dev/sda1"]}},
                },
                "postgres_fatal_io": {
                    "lvl": "CRITICAL",
                    "msg": "could not write to file \"pg_wal/{wal_seg}\": {err}",
                    "vars": {"wal_seg": {"k": "str", "v": "wal_segment_name"}, "err": {"k": "ch", "v": ["Read-only file system", "Input/output error"]}},
                },
                "postgres_stopped": {"lvl": "CRITICAL", "msg": "postmaster terminated due to unrecoverable I/O errors", "vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.25, "scope": "per_host"}]},
                "f": {"emit": [{"id": "raid_io_error", "per_min": 10.0, "scope": "per_host"}, {"id": "fs_remount_ro", "per_min": 0.4, "scope": "per_host"}]},
            },
        },
        {
            "id": "postgres_sync",
            "svc": "postgres",
            "hosts": ["db2"],
            "logs": {
                "replication_receiver": {
                    "lvl": "INFO",
                    "msg": "walreceiver status streaming={streaming} replay_lag_ms={lag_ms}",
                    "vars": {"streaming": {"k": "ch", "v": ["yes"]}, "lag_ms": {"k": "i", "v": [0, 30]}},
                },
                "postmaster_child_crash": {
                    "lvl": "ERROR",
                    "msg": "child process {pid} terminated by signal {sig}; restarting",
                    "vars": {"pid": {"k": "i", "v": [12000, 18000]}, "sig": {"k": "ch", "v": ["SIGSEGV", "SIGABRT"]}},
                },
                "postmaster_restart": {"lvl": "WARN", "msg": "database system was interrupted; starting up in standby mode", "vars": {}},
                "restore_invalid_wal": {
                    "lvl": "WARN",
                    "msg": "restore_command failed for WAL {wal_seg}: invalid WAL record; file discarded",
                    "vars": {"wal_seg": {"k": "str", "v": "wal_segment_name"}},
                },
                "recovery_conf_restored": {"lvl": "WARN", "msg": "standby config restored; starting as replica under cluster control", "vars": {}},
                "manual_promote": {"lvl": "WARN", "msg": "promoted to primary by operator; timeline={tli} listening=10.0.0.22:5432", "vars": {"tli": {"k": "i", "v": [2, 3]}}},
            },
            "beh": {"n": {"emit": [{"id": "replication_receiver", "per_min": 0.25, "scope": "per_host"}]}, "f": {"emit": []}},
        },
        {
            "id": "postgres_async",
            "svc": "postgres",
            "hosts": ["db3"],
            "logs": {
                "replication_lag": {"lvl": "INFO", "msg": "replication status replay_lag_ms={lag_ms}", "vars": {"lag_ms": {"k": "i", "v": [0, 80]}}},
                "standby_conn_lost": {"lvl": "ERROR", "msg": "could not connect to primary for streaming replication: {err}", "vars": {"err": {"k": "ch", "v": ["connection refused", "no route to host"]}}},
            },
            "beh": {"n": {"emit": [{"id": "replication_lag", "per_min": 0.25, "scope": "per_host"}]}, "f": {"emit": [{"id": "standby_conn_lost", "per_min": 0.7, "scope": "per_host"}]}},
        },
        {
            "id": "monitoring",
            "svc": "prometheus",
            "hosts": ["mon1"],
            "logs": {
                "probe_ok": {"lvl": "INFO", "msg": "probe ok target={target} status=200 latency_ms={lat_ms}", "vars": {"target": {"k": "ch", "v": ["api", "dashboard"]}, "lat_ms": {"k": "i", "v": [20, 180]}}},
                "probe_fail": {"lvl": "ERROR", "msg": "probe failed target={target} status={status} latency_ms={lat_ms}", "vars": {"target": {"k": "ch", "v": ["api", "dashboard"]}, "status": {"k": "ch", "v": ["503", "504"]}, "lat_ms": {"k": "i", "v": [900, 3200]}}},
                "alert_api_outage": {"lvl": "CRITICAL", "msg": "api/dashboard outage alert fired err_rate_pct={err_rate_pct} window_s={window_s}", "vars": {"err_rate_pct": {"k": "i", "v": [95, 100]}, "window_s": {"k": "i", "v": [60, 180]}}},
            },
            "beh": {"n": {"emit": [{"id": "probe_ok", "per_min": 4.0, "scope": "global"}]}, "f": {"emit": [{"id": "probe_fail", "per_min": 4.0, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": [
            {
                "id": "web_request_ok",
                "rpm": 650.0,
                "emit": ["api_app.req_done_2xx"],
                "latency_ms": [[60, 180]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            }
        ],
        "f": [
            {
                "id": "web_request_db_down_vip",
                "rpm": 650.0,
                "emit": ["api_app.db_connect_err_vip", "api_app.req_done_503"],
                "latency_ms": [[950, 1600], [1100, 3800]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "web_request_db_direct_fail",
                "rpm": 250.0,
                "emit": ["api_app.db_connect_err_direct", "api_app.req_done_503"],
                "latency_ms": [[800, 1600], [950, 3600]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "api_dashboard_outage_pacemaker_no_promotion_20171010_compressed"},
    "time": {"total_minutes": 54, "phases": {"n": {"start_min": 0, "end_min": 27}, "f": {"start_min": 27, "end_min": 54}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 27,
                    "rate_multipliers": {"web_request_db_direct_fail": 0.0, "pacemaker.vip_not_managed": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "postgres_primary.raid_disk_loss", "count": 1, "hosts": ["db1"]},
                        {"ref": "postgres_primary.postgres_fatal_io", "count": 1, "hosts": ["db1"]},
                        {"ref": "postgres_primary.postgres_stopped", "count": 1, "hosts": ["db1"]},
                        {"ref": "postgres_sync.postmaster_child_crash", "count": 1, "hosts": ["db2"]},
                        {"ref": "postgres_sync.postmaster_restart", "count": 1, "hosts": ["db2"]},
                        {"ref": "postgres_sync.restore_invalid_wal", "count": 1, "hosts": ["db2"]},
                        {"ref": "monitoring.alert_api_outage", "count": 1, "hosts": ["mon1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {"postgres_primary.raid_io_error": 0.0, "postgres_primary.fs_remount_ro": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "pacemaker.node_fenced", "count": 1, "hosts": ["db2"]}],
                },
                {
                    "order": 3,
                    "at_min": 31,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "pacemaker.crm_resource_cleanup", "count": 1, "hosts": ["db2"]}],
                },
                {
                    "order": 4,
                    "at_min": 44,
                    "rate_multipliers": {"pacemaker.promote_attempt": 1.5},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "pacemaker.maintenance_mode_enabled", "count": 1, "hosts": ["db2"]},
                        {"ref": "pacemaker.maintenance_mode_disabled", "count": 1, "hosts": ["db2"]},
                        {"ref": "postgres_sync.recovery_conf_restored", "count": 2, "hosts": ["db2"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 49,
                    "rate_multipliers": {
                        "web_request_db_down_vip": 0.6,
                        "web_request_db_direct_fail": 1.0,
                        "pacemaker.promote_attempt": 0.0,
                        "pacemaker.promote_failed_constraints": 0.0,
                        "pacemaker.vip_not_managed": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "pacemaker.maintenance_mode_enabled", "count": 1, "hosts": ["db2"]},
                        {"ref": "postgres_sync.manual_promote", "count": 1, "hosts": ["db2"]},
                        {"ref": "api_app.config_db_host_override", "count": 3, "hosts": ["app1", "app2", "app3"]},
                    ],
                },
            ]
        }
    },
}


class DeterministicSampler:
    def __init__(self, seed: str):
        self.seed = seed

    def _md5(self, s: str) -> bytes:
        return hashlib.md5((self.seed + "|" + s).encode("utf-8")).digest()

    def u01(self, key: str) -> float:
        b = self._md5(key)
        x = int.from_bytes(b[:8], "big", signed=False)
        return (x % (10**12)) / float(10**12)

    def randint_inclusive(self, lo: int, hi: int, key: str) -> int:
        if hi <= lo:
            return lo
        u = self.u01(key)
        return lo + int(math.floor(u * (hi - lo + 1)))

    def choice(self, items: List[Any], key: str) -> Any:
        if not items:
            return None
        u = self.u01(key)
        idx = int(math.floor(u * len(items)))
        if idx >= len(items):
            idx = len(items) - 1
        return items[idx]

    def hex(self, n: int, key: str) -> str:
        if n <= 0:
            return ""
        out = b""
        ctr = 0
        while len(out) * 2 < n:
            out += hashlib.md5((self.seed + "|" + key + f"|{ctr}").encode("utf-8")).digest()
            ctr += 1
        return out.hex()[:n]

    def uuid_str(self, key: str) -> str:
        d = self._md5(key)
        u = uuid.UUID(bytes=d)
        return str(u)


def parse_ref(ref: str) -> Tuple[str, str]:
    parts = ref.split(".")
    if len(parts) != 2:
        raise ValueError(f"Bad ref: {ref}")
    return parts[0], parts[1]


def domain_for_var(tpl: Dict[str, Any], var: str, state: str) -> Optional[Dict[str, Any]]:
    if "vars" in tpl and var in tpl["vars"]:
        return tpl["vars"][var]
    if "state_vars" in tpl and state in tpl["state_vars"] and var in tpl["state_vars"][state]:
        return tpl["state_vars"][state][var]
    return None


def int_domain_bounds(tpl: Dict[str, Any], var: str, state: str) -> Optional[Tuple[int, int]]:
    dom = domain_for_var(tpl, var, state)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def gen_value(domain: Dict[str, Any], sampler: DeterministicSampler, key: str) -> Any:
    k = domain["k"]
    v = domain.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return sampler.randint_inclusive(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = sampler.u01(key)
        return lo + u * (hi - lo)
    if k == "ch":
        return sampler.choice(list(v), key)
    if k == "uuid":
        return sampler.uuid_str(key)
    if k == "hex":
        n = int(v)
        return sampler.hex(n, key)
    if k == "str":
        # YAML 'str' domains are treated as fixed string hints/values, not synthetic generators.
        return "" if v is None else str(v)
    if k == "ip":
        return "10.0.0.1"
    return ""


def norm_ppf(u: float) -> float:
    u = min(max(u, 1e-12), 1 - 1e-12)

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

    if u < plow:
        q = math.sqrt(-2.0 * math.log(u))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den

    if u > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - u))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)

    q = u - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_from_p50_p95(p50: float, p95: float, u: float, soft_cap: float) -> float:
    z95 = 1.6448536269514722
    mu = math.log(max(p50, 1e-9))
    sigma = (math.log(max(p95, 1e-9)) - mu) / z95 if p95 > 0 else 0.0
    z = norm_ppf(u)
    x = math.exp(mu + sigma * z)
    return min(x, soft_cap)


def stable_int_count(expected: float, carry: float) -> Tuple[int, float]:
    x = expected + carry
    c = int(math.floor(x))
    new_carry = x - c
    return max(0, c), new_carry


def format_ts_ms(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


def schedule_times(interval_start_s: float, interval_end_s: float, n: int, sampler: DeterministicSampler, key_prefix: str, jitter_s: float) -> List[float]:
    if n <= 0:
        return []
    dur = max(0.0, interval_end_s - interval_start_s)
    if dur <= 0:
        return [interval_start_s] * n
    out: List[float] = []
    for i in range(n):
        frac = (i + 0.5) / n
        base = interval_start_s + frac * dur
        u = sampler.u01(f"{key_prefix}|jitter|{i}")
        t = base + (u - 0.5) * jitter_s
        t = max(interval_start_s, min(t, interval_end_s - 1e-3))
        out.append(t)
    return out


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    components_by_id = {c["id"]: c for c in system["components"]}
    templates_by_ref: Dict[str, Any] = {}
    for cid, comp in components_by_id.items():
        for lid, tpl in comp.get("logs", {}).items():
            templates_by_ref[f"{cid}.{lid}"] = tpl
    flows_by_state = system["flows"]
    return components_by_id, templates_by_ref, flows_by_state


def render_message(ref: str, tpl: Dict[str, Any], state: str, bound: Dict[str, Any], sampler: DeterministicSampler, render_key: str) -> str:
    msg = tpl["msg"]
    required: List[str] = []
    cur = ""
    in_brace = False
    for ch in msg:
        if ch == "{":
            in_brace = True
            cur = ""
        elif ch == "}" and in_brace:
            in_brace = False
            if cur:
                required.append(cur)
        elif in_brace:
            cur += ch

    vals: Dict[str, Any] = {}
    for var in required:
        if var in bound:
            vals[var] = bound[var]
            continue
        dom = domain_for_var(tpl, var, state)
        if dom is None:
            vals[var] = ""
        else:
            vals[var] = gen_value(dom, sampler, f"{render_key}|{ref}|{var}")

    try:
        return msg.format(**vals)
    except Exception:
        return msg


def choose_host_for_component(comp: Dict[str, Any], state: str, sampler: DeterministicSampler, key: str) -> str:
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    if comp["id"] == "pacemaker" and state == "f":
        return "db2" if "db2" in hosts else hosts[0]
    return sampler.choice(hosts, key) or hosts[0]


def adjust_delays_with_bounds(
    delays_ms: List[int],
    mins_ms: List[int],
    maxs_ms: List[int],
    target_total_ms: int,
    fixed_mask: Optional[List[bool]] = None,
) -> List[int]:
    if not delays_ms:
        return []
    n = len(delays_ms)
    mins = [max(1, int(x)) for x in mins_ms]
    maxs = [int(x) for x in maxs_ms]
    for i in range(n):
        if maxs[i] < mins[i]:
            maxs[i] = mins[i]

    cur = [clamp_int(int(delays_ms[i]), mins[i], maxs[i]) for i in range(n)]

    min_sum = sum(mins)
    max_sum = sum(maxs)
    tgt = int(target_total_ms)
    if tgt < min_sum:
        tgt = min_sum
    if tgt > max_sum:
        tgt = max_sum

    delta = tgt - sum(cur)
    if delta == 0:
        return cur

    fixed = fixed_mask or [False] * n
    order = [i for i in range(n) if not fixed[i]] + [i for i in range(n) if fixed[i]]

    if delta > 0:
        for i in order:
            if delta <= 0:
                break
            room = maxs[i] - cur[i]
            if room <= 0:
                continue
            add = min(room, delta)
            cur[i] += add
            delta -= add
    else:
        need = -delta
        for i in order:
            if need <= 0:
                break
            room = cur[i] - mins[i]
            if room <= 0:
                continue
            sub = min(room, need)
            cur[i] -= sub
            need -= sub

    return cur


def generate_background_logs(
    rows: List[Dict[str, Any]],
    state: str,
    interval_start_s: float,
    interval_end_s: float,
    rate_mult: Dict[str, float],
    components_by_id: Dict[str, Any],
    templates_by_ref: Dict[str, Any],
    sampler: DeterministicSampler,
    base_epoch_ms: int,
    carry_map: Dict[str, float],
) -> None:
    for comp in SYSTEM["components"]:
        comp_id = comp["id"]
        comp = components_by_id[comp_id]
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for emit in beh:
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            ref = f"{comp_id}.{log_id}"
            mult = float(rate_mult.get(ref, 1.0)) if state == "f" else 1.0
            eff = per_min * mult
            dur_min = (interval_end_s - interval_start_s) / 60.0
            if eff <= 0.0 or dur_min <= 0:
                continue

            tpl = templates_by_ref[ref]
            svc = comp.get("svc") or ""
            hosts = comp.get("hosts") or []

            if scope == "global":
                key = f"bg|{state}|{ref}|{interval_start_s:.3f}"
                carry = carry_map.get(key, 0.0)
                expected = eff * dur_min
                n, new_carry = stable_int_count(expected, carry)
                carry_map[key] = new_carry
                times = schedule_times(interval_start_s, interval_end_s, n, sampler, key, jitter_s=0.25)
                for i, t in enumerate(times):
                    host = choose_host_for_component(comp, state, sampler, f"{key}|host|{i}")
                    bound = {}
                    if ref == "pacemaker.cluster_status":
                        bound["dc"] = host
                    msg = render_message(ref, tpl, state, bound, sampler, f"{key}|{i}")
                    epoch_ms = base_epoch_ms + int(round(t * 1000.0))
                    rows.append({"timestamp_ms": epoch_ms, "level": tpl["lvl"], "message": msg, "trace_id": "", "service": svc, "host": host})
            else:
                for host in hosts:
                    key = f"bg|{state}|{ref}|{host}|{interval_start_s:.3f}"
                    carry = carry_map.get(key, 0.0)
                    expected = eff * dur_min
                    n, new_carry = stable_int_count(expected, carry)
                    carry_map[key] = new_carry
                    times = schedule_times(interval_start_s, interval_end_s, n, sampler, key, jitter_s=0.25)
                    for i, t in enumerate(times):
                        bound = {}
                        if ref == "pacemaker.cluster_status":
                            bound["dc"] = host
                        msg = render_message(ref, tpl, state, bound, sampler, f"{key}|{i}")
                        epoch_ms = base_epoch_ms + int(round(t * 1000.0))
                        rows.append({"timestamp_ms": epoch_ms, "level": tpl["lvl"], "message": msg, "trace_id": "", "service": svc, "host": host})


def generate_flow_logs(
    rows: List[Dict[str, Any]],
    state: str,
    interval_start_s: float,
    interval_end_s: float,
    rate_mult: Dict[str, float],
    latency_mult: Dict[str, float],
    components_by_id: Dict[str, Any],
    templates_by_ref: Dict[str, Any],
    flows: List[Dict[str, Any]],
    sampler: DeterministicSampler,
    base_epoch_ms: int,
    carry_map: Dict[str, float],
) -> None:
    dur_min = (interval_end_s - interval_start_s) / 60.0
    if dur_min <= 0:
        return

    for flow in flows:
        fid = flow["id"]
        base_rpm = float(flow["rpm"])
        mult = float(rate_mult.get(fid, 1.0)) if state == "f" else 1.0
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0:
            continue

        key = f"flow|{state}|{fid}|{interval_start_s:.3f}"
        carry = carry_map.get(key, 0.0)
        expected = eff_rpm * dur_min
        n, new_carry = stable_int_count(expected, carry)
        carry_map[key] = new_carry

        start_times = schedule_times(interval_start_s, interval_end_s, n, sampler, key, jitter_s=0.35)

        lat_mult = float(latency_mult.get(fid, 1.0)) if state == "f" else 1.0
        emit_refs = flow["emit"]
        lat_pairs = flow.get("latency_ms", [])
        trace_on = bool(SYSTEM.get("tracing", {}).get("on", False)) and bool(flow.get("trace", False))

        api_comp = components_by_id["api_app"]
        api_hosts = api_comp.get("hosts") or []

        for i, st in enumerate(start_times):
            inst_key = f"{key}|inst|{i}"

            method = sampler.choice(["GET", "POST"], f"{inst_key}|method")
            route = sampler.choice(["/api/payments", "/api/mandates", "/dashboard"], f"{inst_key}|route")
            request_id = sampler.uuid_str(f"{inst_key}|request_id")
            trace_id = sampler.hex(32, f"{inst_key}|trace") if trace_on else ""
            app_host = api_hosts[i % len(api_hosts)] if api_hosts else ""

            delays_ms: List[int] = []
            mins_ms: List[int] = []
            maxs_ms: List[int] = []
            fixed_mask: List[bool] = []

            for li, pair in enumerate(lat_pairs):
                p50, p95 = float(pair[0]) * lat_mult, float(pair[1]) * lat_mult
                u = sampler.u01(f"{inst_key}|lat_u|{li}")
                soft_cap = 2.5 * p95
                d = lognormal_from_p50_p95(p50, p95, u, soft_cap)
                d_ms = int(round(max(1.0, d)))

                ref = emit_refs[li] if li < len(emit_refs) else ""
                tpl = templates_by_ref.get(ref, {})
                to_bounds = int_domain_bounds(tpl, "timeout_ms", state)
                if to_bounds is not None:
                    to_lo, to_hi = to_bounds
                    d_ms = clamp_int(d_ms, to_lo, to_hi)
                    mins_ms.append(to_lo)
                    maxs_ms.append(to_hi)
                    fixed_mask.append(True)
                else:
                    mins_ms.append(1)
                    maxs_ms.append(10**9)
                    fixed_mask.append(False)

                delays_ms.append(d_ms)

            total_ms = int(sum(delays_ms))
            if emit_refs:
                final_ref = emit_refs[-1]
                final_tpl = templates_by_ref[final_ref]
                dur_bounds = int_domain_bounds(final_tpl, "dur_ms", state)
                if dur_bounds is not None:
                    dur_lo, dur_hi = dur_bounds
                    desired_total = clamp_int(total_ms, dur_lo, dur_hi)
                    if desired_total != total_ms:
                        delays_ms = adjust_delays_with_bounds(
                            delays_ms=delays_ms,
                            mins_ms=mins_ms,
                            maxs_ms=maxs_ms,
                            target_total_ms=desired_total,
                            fixed_mask=fixed_mask,
                        )
                        total_ms = int(sum(delays_ms))

            tcur = st
            for li, ref in enumerate(emit_refs):
                comp_id, _ = parse_ref(ref)
                comp = components_by_id[comp_id]
                tpl = templates_by_ref[ref]
                svc = comp.get("svc") or ""
                host = app_host if comp_id == "api_app" else choose_host_for_component(comp, state, sampler, f"{inst_key}|{ref}|host")

                tcur = tcur + (delays_ms[li] / 1000.0 if li < len(delays_ms) else 0.0)
                epoch_ms = base_epoch_ms + int(round(tcur * 1000.0))

                bound: Dict[str, Any] = {}
                if comp_id == "api_app":
                    bound["method"] = method
                    bound["route"] = route
                    bound["request_id"] = request_id
                    bound["trace_id"] = trace_id

                if ref == "api_app.req_done_2xx":
                    dur_bounds2 = int_domain_bounds(tpl, "dur_ms", state)
                    bound["dur_ms"] = clamp_int(total_ms, dur_bounds2[0], dur_bounds2[1]) if dur_bounds2 else total_ms

                    db_bounds = int_domain_bounds(tpl, "db_ms", state)
                    if db_bounds:
                        db_lo, db_hi = db_bounds
                    else:
                        db_lo, db_hi = (0, max(0, bound["dur_ms"]))
                    db_hi_eff = min(db_hi, int(bound["dur_ms"]))
                    if db_hi_eff < db_lo:
                        db_hi_eff = db_lo
                    udb = sampler.u01(f"{inst_key}|db_ms")
                    candidate = int(round(db_lo + udb * (db_hi_eff - db_lo))) if db_hi_eff >= db_lo else db_lo
                    candidate = min(candidate, int(bound["dur_ms"]))
                    bound["db_ms"] = clamp_int(candidate, db_lo, db_hi_eff)

                if ref == "api_app.db_connect_err_vip":
                    bound["timeout_ms"] = int(delays_ms[li])
                    err = "timeout" if sampler.u01(f"{inst_key}|vip_err") < 0.75 else sampler.choice(tpl["vars"]["err"]["v"], f"{inst_key}|vip_err_choice")
                    bound["err"] = err

                if ref == "api_app.db_connect_err_direct":
                    bound["timeout_ms"] = int(delays_ms[li])
                    bound["err"] = "timeout" if sampler.u01(f"{inst_key}|dir_err") < 0.6 else "too_many_connections"

                if ref == "api_app.req_done_503":
                    dur_bounds3 = int_domain_bounds(tpl, "dur_ms", state)
                    bound["dur_ms"] = clamp_int(total_ms, dur_bounds3[0], dur_bounds3[1]) if dur_bounds3 else total_ms

                msg = render_message(ref, tpl, state, bound, sampler, f"{inst_key}|emit|{li}")
                rows.append({"timestamp_ms": epoch_ms, "level": tpl["lvl"], "message": msg, "trace_id": trace_id if trace_on else "", "service": svc, "host": host})


def generate_one_shots(
    rows: List[Dict[str, Any]],
    event_at_min: int,
    one_shots: List[Dict[str, Any]],
    components_by_id: Dict[str, Any],
    templates_by_ref: Dict[str, Any],
    sampler: DeterministicSampler,
    base_epoch_ms: int,
) -> None:
    event_start_s = float(event_at_min * 60)
    for j, shot in enumerate(one_shots):
        ref = shot["ref"]
        count = int(shot["count"])
        allowed_hosts = list(shot.get("hosts") or [])
        comp_id, _ = parse_ref(ref)
        comp = components_by_id[comp_id]
        tpl = templates_by_ref[ref]
        svc = comp.get("svc") or ""
        for k in range(count):
            key = f"oneshot|{event_at_min}|{ref}|{j}|{k}"
            u = sampler.u01(f"{key}|jit")
            t = event_start_s + (u - 0.5) * 0.8
            epoch_ms = base_epoch_ms + int(round(t * 1000.0))
            if allowed_hosts:
                host = allowed_hosts[k % len(allowed_hosts)]
            else:
                host = choose_host_for_component(comp, "f", sampler, f"{key}|host")
            msg = render_message(ref, tpl, "f", {}, sampler, key)
            rows.append({"timestamp_ms": epoch_ms, "level": tpl["lvl"], "message": msg, "trace_id": "", "service": svc, "host": host})


def build_failure_intervals_and_controls() -> List[Dict[str, Any]]:
    f_start = int(SCENARIO["time"]["phases"]["f"]["start_min"])
    f_end = int(SCENARIO["time"]["phases"]["f"]["end_min"])
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    boundaries = sorted(set([f_start] + [int(e["at_min"]) for e in events] + [f_end]))
    intervals: List[Dict[str, Any]] = []
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, float] = {}

    idx = 0
    for b_i in range(len(boundaries) - 1):
        start_min = boundaries[b_i]
        end_min = boundaries[b_i + 1]
        while idx < len(events) and int(events[idx]["at_min"]) == start_min:
            rm = events[idx].get("rate_multipliers") or {}
            lm = events[idx].get("latency_multipliers") or {}
            for k, v in rm.items():
                active_rate[k] = float(v)
            for k, v in lm.items():
                active_lat[k] = float(v)
            idx += 1
        intervals.append({"start_min": start_min, "end_min": end_min, "rate_mult": dict(active_rate), "lat_mult": dict(active_lat)})
    return intervals


def main() -> None:
    seed_int = 13371337
    random.seed(seed_int)
    np.random.seed(seed_int)

    sampler = DeterministicSampler(seed="payments_api_dashboard_stack|api_dashboard_outage|v3")

    components_by_id, templates_by_ref, flows_by_state = build_indices(SYSTEM)

    base_dt = datetime(2017, 10, 10, 0, 0, 0, tzinfo=timezone.utc)
    base_epoch_ms = int(base_dt.timestamp() * 1000)

    rows: List[Dict[str, Any]] = []
    carry_map: Dict[str, float] = {}

    n_start = int(SCENARIO["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["time"]["phases"]["n"]["end_min"])
    n_interval_start_s = float(n_start * 60)
    n_interval_end_s = float(n_end * 60)

    generate_background_logs(
        rows=rows,
        state="n",
        interval_start_s=n_interval_start_s,
        interval_end_s=n_interval_end_s,
        rate_mult={},
        components_by_id=components_by_id,
        templates_by_ref=templates_by_ref,
        sampler=sampler,
        base_epoch_ms=base_epoch_ms,
        carry_map=carry_map,
    )
    generate_flow_logs(
        rows=rows,
        state="n",
        interval_start_s=n_interval_start_s,
        interval_end_s=n_interval_end_s,
        rate_mult={},
        latency_mult={},
        components_by_id=components_by_id,
        templates_by_ref=templates_by_ref,
        flows=flows_by_state["n"],
        sampler=sampler,
        base_epoch_ms=base_epoch_ms,
        carry_map=carry_map,
    )

    failure_intervals = build_failure_intervals_and_controls()

    for ev in sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0))):
        generate_one_shots(
            rows=rows,
            event_at_min=int(ev["at_min"]),
            one_shots=list(ev.get("one_shots") or []),
            components_by_id=components_by_id,
            templates_by_ref=templates_by_ref,
            sampler=sampler,
            base_epoch_ms=base_epoch_ms,
        )

    for iv in failure_intervals:
        s_min, e_min = int(iv["start_min"]), int(iv["end_min"])
        s_s, e_s = float(s_min * 60), float(e_min * 60)
        rate_mult = iv["rate_mult"]
        lat_mult = iv["lat_mult"]
        generate_background_logs(
            rows=rows,
            state="f",
            interval_start_s=s_s,
            interval_end_s=e_s,
            rate_mult=rate_mult,
            components_by_id=components_by_id,
            templates_by_ref=templates_by_ref,
            sampler=sampler,
            base_epoch_ms=base_epoch_ms,
            carry_map=carry_map,
        )
        generate_flow_logs(
            rows=rows,
            state="f",
            interval_start_s=s_s,
            interval_end_s=e_s,
            rate_mult=rate_mult,
            latency_mult=lat_mult,
            components_by_id=components_by_id,
            templates_by_ref=templates_by_ref,
            flows=flows_by_state["f"],
            sampler=sampler,
            base_epoch_ms=base_epoch_ms,
            carry_map=carry_map,
        )

    df = pd.DataFrame(rows)
    df = df.sort_values(["timestamp_ms", "service", "host", "level"], kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_ms"].apply(format_ts_ms)

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    if not (20000 <= len(out) <= 100000):
        raise RuntimeError(f"log volume out of bounds: {len(out)} rows")
    if not out["timestamp"].is_monotonic_increasing:
        raise RuntimeError("timestamps are not sorted")

    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
