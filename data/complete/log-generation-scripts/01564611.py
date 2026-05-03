import math
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

# ----------------------------
# Embedded executable specs
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "fintech_api_rds_multiaz"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_service"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_service",
            "svc": "api",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "http_access_ok": {
                    "lvl": "INFO",
                    "msg": "req {req_id} {method} {route} status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/v1/txns", "/v1/balance", "/v1/payments"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [5, 250]}},
                        "f": {"dur_ms": {"k": "i", "v": [10, 1200]}},
                    },
                },
                "http_access_5xx": {
                    "lvl": "ERROR",
                    "msg": "req {req_id} {method} {route} status=502 err={err} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/v1/txns", "/v1/balance", "/v1/payments"]},
                        "err": {"k": "ch", "v": ["DB_CONN"]},
                        "dur_ms": {"k": "i", "v": [200, 5000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "db_query": {
                    "lvl": "DEBUG",
                    "msg": "sql {op} db_role={db_role} host={db_host} db_dur_ms={db_dur_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["SELECT", "INSERT"]},
                        "db_role": {"k": "ch", "v": ["replica", "master"]},
                        "db_host": {
                            "k": "ch",
                            "v": [
                                "db-primary-1",
                                "db-replica-1",
                                "db-replica-2",
                                "db-replica-3",
                                "db-replica-4",
                                "db-replica-5",
                            ],
                        },
                    },
                    "state_vars": {
                        "n": {"db_dur_ms": {"k": "i", "v": [2, 120]}},
                        "f": {"db_dur_ms": {"k": "i", "v": [5, 650]}},
                    },
                },
                "db_connect_error": {
                    "lvl": "ERROR",
                    "msg": "db connect failed host={db_host} err={db_err} waited_ms={waited_ms}",
                    "vars": {
                        "db_host": {"k": "ch", "v": ["db-primary-1"]},
                        "db_err": {"k": "ch", "v": ["timeout", "connection_refused", "host_unreachable"]},
                        "waited_ms": {"k": "i", "v": [200, 3500]},
                    },
                    "state_vars": {},
                },
                "trace_txn_commit": {
                    "lvl": "INFO",
                    "msg": "txn commit txn_id={txn_id} table={table} pk={pk} amount_cents={amount_cents}",
                    "vars": {
                        "txn_id": {"k": "uuid", "v": None},
                        "table": {"k": "ch", "v": ["transactions"]},
                        "pk": {"k": "i", "v": [1000000, 1010000]},
                        "amount_cents": {"k": "i", "v": [100, 500000]},
                    },
                    "state_vars": {},
                },
                "health": {
                    "lvl": "INFO",
                    "msg": "health ok build={build}",
                    "vars": {"build": {"k": "ch", "v": ["api-2019.12.14"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "health", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "health", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "rds_primary",
            "svc": "rds-mysql",
            "hosts": ["db-primary-1"],
            "logs": {
                "metric_cpu": {
                    "lvl": "INFO",
                    "msg": "metric cpu_pct={cpu_pct} connections={connections}",
                    "vars": {},
                    "state_vars": {
                        "n": {"cpu_pct": {"k": "i", "v": [15, 35]}, "connections": {"k": "i", "v": [50, 300]}},
                        "f": {"cpu_pct": {"k": "i", "v": [25, 85]}, "connections": {"k": "i", "v": [80, 650]}},
                    },
                },
                "mysql_aborted_conn": {
                    "lvl": "WARN",
                    "msg": "Aborted connection {conn_id} from {client_ip} (Got an error reading communication packets)",
                    "vars": {"conn_id": {"k": "i", "v": [1000, 9000]}, "client_ip": {"k": "ip", "v": "10.0.0.0/16"}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "metric_cpu", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "metric_cpu", "per_min": 1.0, "scope": "per_host"},
                        {"id": "mysql_aborted_conn", "per_min": 15.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "rds_replicas",
            "svc": "rds-mysql",
            "hosts": ["db-replica-1", "db-replica-2", "db-replica-3", "db-replica-4", "db-replica-5"],
            "logs": {
                "replication_status": {
                    "lvl": "INFO",
                    "msg": "replica {replica_id} lag_s={lag_s} io_running={io_running} sql_running={sql_running}",
                    "vars": {
                        "replica_id": {"k": "ch", "v": ["r1", "r2", "r3", "r4", "r5"]},
                        "io_running": {"k": "ch", "v": ["Yes"]},
                    },
                    "state_vars": {
                        "n": {"lag_s": {"k": "i", "v": [0, 2]}, "sql_running": {"k": "ch", "v": ["Yes"]}},
                        "f": {"lag_s": {"k": "i", "v": [30, 900]}, "sql_running": {"k": "ch", "v": ["No"]}},
                    },
                },
                "replication_error": {
                    "lvl": "ERROR",
                    "msg": "replica {replica_id} SQL thread stopped: Duplicate entry {pk} for key 'PRIMARY' exec_pos={exec_pos}",
                    "vars": {
                        "replica_id": {"k": "ch", "v": ["r1", "r2", "r3", "r4", "r5"]},
                        "pk": {"k": "i", "v": [1000000, 1010000]},
                        "exec_pos": {"k": "i", "v": [100000, 250000]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "replication_status", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "replication_status", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "rds_controlplane",
            "svc": "rds-control",
            "hosts": ["rds-ctrl-1"],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "rds-control heartbeat ok region={region}",
                    "vars": {"region": {"k": "ch", "v": ["us-east-1"]}},
                    "state_vars": {},
                },
                "failover_start": {
                    "lvl": "WARN",
                    "msg": "Multi-AZ failover started db_id={db_id} reason={reason}",
                    "vars": {
                        "db_id": {"k": "ch", "v": ["api-mysql"]},
                        "reason": {"k": "ch", "v": ["instance_unreachable", "storage_issue", "network_issue"]},
                    },
                    "state_vars": {},
                },
                "failover_complete": {
                    "lvl": "INFO",
                    "msg": "Failover complete db_id={db_id} new_primary_host={new_primary_host}",
                    "vars": {"db_id": {"k": "ch", "v": ["api-mysql"]}, "new_primary_host": {"k": "ch", "v": ["db-primary-1"]}},
                    "state_vars": {},
                },
                "snapshot_start": {
                    "lvl": "INFO",
                    "msg": "Snapshot started snap_id={snap_id} db_id={db_id} size_gb={size_gb}",
                    "vars": {"snap_id": {"k": "hex", "v": 12}, "db_id": {"k": "ch", "v": ["api-mysql"]}, "size_gb": {"k": "i", "v": [200, 900]}},
                    "state_vars": {},
                },
                "snapshot_progress": {
                    "lvl": "INFO",
                    "msg": "Snapshot progress snap_id={snap_id} pct={pct} elapsed_min={elapsed_min}",
                    "vars": {"snap_id": {"k": "hex", "v": 12}, "pct": {"k": "i", "v": [0, 99]}, "elapsed_min": {"k": "i", "v": [1, 360]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "global"}]},
                "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "global"}, {"id": "snapshot_progress", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "monitor",
            "hosts": ["mon-1"],
            "logs": {
                "heartbeat": {"lvl": "INFO", "msg": "monitor heartbeat ok", "vars": {}, "state_vars": {}},
                "alert_5xx": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT api 5xx_rate_per_min={rate_per_min} threshold_per_min={threshold_per_min}",
                    "vars": {"rate_per_min": {"k": "i", "v": [50, 800]}, "threshold_per_min": {"k": "i", "v": [50, 200]}},
                    "state_vars": {},
                },
                "alert_replication": {"lvl": "CRITICAL", "msg": "ALERT db replicas replication_failed={failed_count}/5", "vars": {"failed_count": {"k": "i", "v": [1, 5]}}, "state_vars": {}},
                "alert_cpu": {"lvl": "WARN", "msg": "ALERT db_primary cpu_pct={cpu_pct} threshold={threshold}", "vars": {"cpu_pct": {"k": "i", "v": [70, 95]}, "threshold": {"k": "i", "v": [70, 85]}}, "state_vars": {}},
            },
            "beh": {"n": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "global"}]}},
        },
        {
            "id": "sre_console",
            "svc": "ops",
            "hosts": ["sre-1"],
            "logs": {
                "action_shift_reads": {
                    "lvl": "INFO",
                    "msg": "ops changed read routing from={from} to={to} reason={reason}",
                    "vars": {"from": {"k": "ch", "v": ["replicas"]}, "to": {"k": "ch", "v": ["master"]}, "reason": {"k": "ch", "v": ["replication_failed"]}},
                    "state_vars": {},
                },
                "action_start_rebuild": {"lvl": "INFO", "msg": "ops started replica rebuild replicas={replicas} snap_id={snap_id}", "vars": {"replicas": {"k": "i", "v": [5, 5]}, "snap_id": {"k": "hex", "v": 12}}, "state_vars": {}},
                "action_open_support": {"lvl": "INFO", "msg": "ops opened aws support case_id={case_id} sev={sev}", "vars": {"case_id": {"k": "str", "v": "case-[0-9]{7}"}, "sev": {"k": "ch", "v": ["high"]}}, "state_vars": {}},
                "action_support_note": {"lvl": "INFO", "msg": "aws support note: snapshot_type={snap_type} due_to={due_to} est_hours={est_hours}", "vars": {"snap_type": {"k": "ch", "v": ["full"]}, "due_to": {"k": "ch", "v": ["host_replacement_after_failover"]}, "est_hours": {"k": "i", "v": [4, 8]}}, "state_vars": {}},
                "action_check_variables": {"lvl": "INFO", "msg": "checked mysql vars innodb_flush_log_at_trx_commit={innodb_flush} sync_binlog={sync_binlog}", "vars": {"innodb_flush": {"k": "i", "v": [2, 2]}, "sync_binlog": {"k": "i", "v": [1, 1]}}, "state_vars": {}},
                "action_set_variable": {"lvl": "WARN", "msg": "set mysql var innodb_flush_log_at_trx_commit new={new_val} old={old_val}", "vars": {"new_val": {"k": "i", "v": [1, 1]}, "old_val": {"k": "i", "v": [2, 2]}}, "state_vars": {}},
                "action_data_gap": {"lvl": "ERROR", "msg": "data reconciliation gap window_s={gap_s} txn_count={txn_count} source={source}", "vars": {"gap_s": {"k": "i", "v": [5, 5]}, "txn_count": {"k": "i", "v": [1, 50]}, "source": {"k": "ch", "v": ["binlog_vs_traces"]}}, "state_vars": {}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "api_read_replicas_n",
                    "rpm": 360.0,
                    "emit": ["api_service.db_query", "api_service.http_access_ok"],
                    "latency_ms": [[8, 40], [4, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_write_master_n",
                    "rpm": 90.0,
                    "emit": ["api_service.db_query", "api_service.trace_txn_commit", "api_service.http_access_ok"],
                    "latency_ms": [[10, 60], [1, 5], [5, 25]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "api_db_conn_fail_f",
                    "rpm": 225.0,
                    "emit": ["api_service.db_connect_error", "api_service.http_access_5xx"],
                    "latency_ms": [[300, 2200], [5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_read_replicas_f",
                    "rpm": 180.0,
                    "emit": ["api_service.db_query", "api_service.http_access_ok"],
                    "latency_ms": [[10, 70], [5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_read_master_f",
                    "rpm": 180.0,
                    "emit": ["api_service.db_query", "api_service.http_access_ok"],
                    "latency_ms": [[20, 140], [5, 35]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_write_master_f",
                    "rpm": 45.0,
                    "emit": ["api_service.db_query", "api_service.trace_txn_commit", "api_service.http_access_ok"],
                    "latency_ms": [[18, 120], [1, 6], [8, 55]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "rds_multiaz_failover_replication_crash",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 15,
                        "rate_multipliers": {
                            "api_db_conn_fail_f": 2.0,
                            "api_read_replicas_f": 0.0,
                            "api_read_master_f": 0.0,
                            "api_write_master_f": 0.0,
                            "rds_replicas.replication_status": 0.0,
                            "rds_controlplane.snapshot_progress": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "rds_controlplane.failover_start", "count": 1, "hosts": ["rds-ctrl-1"]},
                            {"ref": "monitoring.alert_5xx", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 17,
                        "rate_multipliers": {
                            "api_db_conn_fail_f": 0.0,
                            "api_read_replicas_f": 2.0,
                            "api_read_master_f": 0.0,
                            "api_write_master_f": 2.0,
                            "rds_primary.mysql_aborted_conn": 0.0,
                            "rds_replicas.replication_status": 1.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "rds_controlplane.failover_complete", "count": 1, "hosts": ["rds-ctrl-1"]},
                            {
                                "ref": "rds_replicas.replication_error",
                                "count": 5,
                                "hosts": ["db-replica-1", "db-replica-2", "db-replica-3", "db-replica-4", "db-replica-5"],
                            },
                            {"ref": "monitoring.alert_replication", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 18,
                        "rate_multipliers": {"api_read_replicas_f": 0.0, "api_read_master_f": 2.0, "rds_controlplane.snapshot_progress": 1.0},
                        "latency_multipliers": {"api_read_master_f": {"p50": 1.6, "p95": 2.0}, "api_write_master_f": {"p50": 1.3, "p95": 1.7}},
                        "one_shots": [
                            {"ref": "sre_console.action_shift_reads", "count": 1, "hosts": ["sre-1"]},
                            {"ref": "sre_console.action_start_rebuild", "count": 1, "hosts": ["sre-1"]},
                            {"ref": "rds_controlplane.snapshot_start", "count": 1, "hosts": ["rds-ctrl-1"]},
                            {"ref": "monitoring.alert_cpu", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {"order": 4, "at_min": 35, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "sre_console.action_open_support", "count": 1, "hosts": ["sre-1"]}, {"ref": "sre_console.action_support_note", "count": 1, "hosts": ["sre-1"]}]},
                    {"order": 5, "at_min": 50, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "sre_console.action_data_gap", "count": 1, "hosts": ["sre-1"]}, {"ref": "sre_console.action_check_variables", "count": 1, "hosts": ["sre-1"]}, {"ref": "sre_console.action_set_variable", "count": 1, "hosts": ["sre-1"]}]},
                ]
            }
        },
    }
}

# ----------------------------
# Deterministic helpers
# ----------------------------

SEED = "fixed-seed-2026-04-03"
SEED_INT = int.from_bytes(hashlib.sha256(SEED.encode("utf-8")).digest()[:8], "big", signed=False) % (2**32)
random.seed(SEED_INT)
np.random.seed(SEED_INT)

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

def h64(s: str) -> int:
    b = (SEED + "|" + s).encode("utf-8")
    return int.from_bytes(hashlib.sha256(b).digest()[:8], "big", signed=False)

def u01(s: str) -> float:
    return (h64(s) % 10**12) / 10**12

def hex_of(s: str, n: int) -> str:
    d = hashlib.md5((SEED + "|" + s).encode("utf-8")).hexdigest()
    if len(d) >= n:
        return d[:n]
    return (d * (n // len(d) + 1))[:n]

def uuid_of(s: str) -> str:
    d = hashlib.md5((SEED + "|uuid|" + s).encode("utf-8")).hexdigest()
    return f"{d[:8]}-{d[8:12]}-{d[12:16]}-{d[16:20]}-{d[20:32]}"

def ip_in_cidr(cidr: str, s: str) -> str:
    net, pref = cidr.split("/")
    pref = int(pref)
    parts = [int(x) for x in net.split(".")]
    base = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]
    host_bits = 32 - pref
    size = 1 << host_bits
    off = h64("ip|" + cidr + "|" + s) % size
    ip = (base & (~(size - 1))) | off
    return f"{(ip >> 24) & 255}.{(ip >> 16) & 255}.{(ip >> 8) & 255}.{ip & 255}"

def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"

def clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x

def sample_lognormish_between(p50: float, p95: float, key: str, mult_p50: float = 1.0, mult_p95: float = 1.0) -> int:
    u = u01(key)
    p50s = p50 * mult_p50
    p95s = p95 * mult_p95
    val = p50s + (p95s - p50s) * (u * u)
    val = max(1.0, val)
    return int(round(val))

def schedule_times(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur = (end_dt - start_dt).total_seconds()
    if dur <= 0:
        return []
    step = dur / count
    max_jitter = min(0.2, step * 0.2)
    times: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) * step
        jitter = (u01(f"{key}:j:{i}") - 0.5) * 2.0 * max_jitter
        sec = base + jitter
        if sec < 0:
            sec = 0
        if sec > dur - 0.001:
            sec = dur - 0.001
        t = start_dt + timedelta(seconds=sec)
        t += timedelta(milliseconds=int(u01(f"{key}:ms:{i}") * 10))
        times.append(t)
    return times

_CARRY: Dict[str, float] = {}

def alloc_count(expected: float, key: str) -> int:
    carry = _CARRY.get(key, 0.0)
    total = expected + carry
    n = int(math.floor(total + 1e-12))
    _CARRY[key] = total - n
    return n

def domain_value(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        choices = list(v)
        return choices[h64("ch|" + key) % len(choices)]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return lo + (h64("i|" + key) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + u01("f|" + key) * (hi - lo)
    if k == "hex":
        n = int(v)
        return hex_of("hex|" + key, n)
    if k == "uuid":
        return uuid_of(key)
    if k == "ip":
        return ip_in_cidr(str(v), key)
    if k == "str":
        hint = str(v)
        if hint == "case-[0-9]{7}":
            digits = f"{h64('case|' + key) % 10_000_000:07d}"
            return f"case-{digits}"
        return "s-" + hex_of("str|" + key, 10)
    return str(v)

# ----------------------------
# Indices
# ----------------------------

COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOGT: Dict[Tuple[str, str], Dict[str, Any]] = {}
for cid, c in COMP.items():
    for lid, tmpl in c["logs"].items():
        LOGT[(cid, lid)] = tmpl

FLOWS: Dict[str, Dict[str, Any]] = {
    "n": {f["id"]: f for f in SYSTEM["flows"]["n"]["req"]},
    "f": {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]},
}

REPLICA_ID_BY_HOST = {
    "db-replica-1": "r1",
    "db-replica-2": "r2",
    "db-replica-3": "r3",
    "db-replica-4": "r4",
    "db-replica-5": "r5",
}

REPLICA_HOSTS = COMP["rds_replicas"]["hosts"]

SNAP_ID: Optional[str] = None
SNAP_START_DT: Optional[datetime] = None
GLOBAL_PK_COUNTER = 0

# ----------------------------
# Control intervals (failure phase)
# ----------------------------

def build_failure_intervals() -> Tuple[List[Dict[str, Any]], Dict[int, List[Dict[str, Any]]]]:
    scen = SCENARIO["scenario"]
    fstart = scen["time"]["phases"]["f"]["start_min"]
    fend = scen["time"]["phases"]["f"]["end_min"]
    events = list(scen["phases"]["f"]["events"])
    events.sort(key=lambda e: (e.get("at_min", 0), e.get("order", 0)))

    oneshots_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        oneshots_by_min.setdefault(int(e["at_min"]), []).extend(e.get("one_shots", []))

    boundaries = [fstart] + [int(e["at_min"]) for e in events] + [fend]
    b2: List[int] = []
    for b in boundaries:
        if not b2 or b2[-1] != b:
            b2.append(b)

    cur_rate: Dict[str, float] = {}
    cur_lat: Dict[str, Dict[str, float]] = {}
    i = 0
    intervals: List[Dict[str, Any]] = []
    for bi in range(len(b2) - 1):
        t = b2[bi]
        while i < len(events) and int(events[i]["at_min"]) == t:
            ev = events[i]
            for k, v in ev.get("rate_multipliers", {}).items():
                cur_rate[k] = float(v)
            for fk, fv in ev.get("latency_multipliers", {}).items():
                cur_lat[fk] = {"p50": float(fv.get("p50", 1.0)), "p95": float(fv.get("p95", 1.0))}
            i += 1
        intervals.append({"start_min": t, "end_min": b2[bi + 1], "rate_mult": dict(cur_rate), "lat_mult": dict(cur_lat)})
    return intervals, oneshots_by_min

FAIL_INTERVALS, ONESHOTS_BY_MIN = build_failure_intervals()

# ----------------------------
# Emission
# ----------------------------

LOG_ROWS: List[Dict[str, Any]] = []

def emit_row(ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    LOG_ROWS.append(
        {
            "timestamp_dt": ts,
            "timestamp": fmt_ts(ts),
            "level": level,
            "message": message,
            "trace_id": trace_id,
            "service": service,
            "host": host,
        }
    )

def render_log(component_id: str, log_id: str, state: str, key: str, overrides: Dict[str, Any]) -> Tuple[str, str]:
    tmpl = LOGT[(component_id, log_id)]
    values: Dict[str, Any] = {}
    for vn, dom in tmpl.get("vars", {}).items():
        values[vn] = domain_value(dom, f"{key}:{component_id}.{log_id}:{vn}")
    for vn, dom in tmpl.get("state_vars", {}).get(state, {}).items():
        values[vn] = domain_value(dom, f"{key}:{component_id}.{log_id}:{vn}:{state}")
    values.update(overrides)
    msg = tmpl["msg"].format(**values)
    lvl = tmpl["lvl"]
    return lvl, msg

def choose_api_host(instance_idx: int) -> str:
    hosts = COMP["api_service"]["hosts"]
    return hosts[instance_idx % len(hosts)]

def choose_replica_db_host(instance_idx: int) -> str:
    return REPLICA_HOSTS[instance_idx % len(REPLICA_HOSTS)]

def maybe_init_snap(at_min: int) -> None:
    global SNAP_ID, SNAP_START_DT
    if SNAP_ID is None:
        SNAP_ID = hex_of(f"snap-init:{at_min}", 12)
    if SNAP_START_DT is None:
        SNAP_START_DT = BASE_TIME + timedelta(minutes=at_min)

def special_overrides_background(component_id: str, log_id: str, state: str, ts: datetime, host: str, key: str) -> Dict[str, Any]:
    if component_id == "rds_replicas" and log_id == "replication_status":
        ov: Dict[str, Any] = {"replica_id": REPLICA_ID_BY_HOST.get(host, "r1"), "io_running": "Yes"}
        if state == "n":
            lag = int(round(u01(f"{key}:lag") * 2))
            ov["lag_s"] = lag
            ov["sql_running"] = "Yes"
        else:
            minutes_since_17 = max(0.0, (ts - (BASE_TIME + timedelta(minutes=17))).total_seconds() / 60.0)
            base_lag = 30 + int(minutes_since_17 * 18) + (h64(f"{key}:lh") % 15)
            ov["lag_s"] = clamp_int(base_lag, 30, 900)
            ov["sql_running"] = "No"
        return ov

    if component_id == "rds_controlplane" and log_id == "snapshot_progress":
        if SNAP_ID is None or SNAP_START_DT is None:
            maybe_init_snap(18)
        elapsed_min = int(max(1.0, (ts - SNAP_START_DT).total_seconds() / 60.0))
        elapsed_min = clamp_int(elapsed_min, 1, 360)
        pct = clamp_int(int(elapsed_min / 4), 0, 99)
        return {"snap_id": SNAP_ID, "elapsed_min": elapsed_min, "pct": pct}

    if component_id == "rds_primary" and log_id == "metric_cpu":
        if state == "n":
            cpu = 15 + (h64(f"{key}:cpu") % 21)
            con = 50 + (h64(f"{key}:con") % 251)
        else:
            minutes_since_18 = max(0.0, (ts - (BASE_TIME + timedelta(minutes=18))).total_seconds() / 60.0)
            cpu = 25 + int(min(60, minutes_since_18 * 0.9)) + (h64(f"{key}:cpu2") % 6)
            cpu = clamp_int(cpu, 25, 85)
            con = 120 + int(min(500, minutes_since_18 * 4.5)) + (h64(f"{key}:con2") % 25)
            con = clamp_int(con, 80, 650)
        return {"cpu_pct": cpu, "connections": con}

    return {}

def emit_background_interval(state: str, start_min: int, end_min: int, rate_mult: Optional[Dict[str, float]] = None) -> None:
    rate_mult = rate_mult or {}
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, end_min - start_min)

    for cid, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {})
        for em in beh.get("emit", []):
            log_id = em["id"]
            per_min = float(em["per_min"])
            scope = em.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = float(rate_mult.get(f"{cid}.{log_id}", 1.0))
            rate = per_min * mult
            if rate <= 0.0:
                continue

            if scope == "global":
                expected = rate * duration_min
                cnt = alloc_count(expected, f"bg:{state}:{cid}.{log_id}:global")
                host = comp["hosts"][0] if comp.get("hosts") else ""
                times = schedule_times(start_dt, end_dt, cnt, f"bg:{state}:{cid}.{log_id}:global:{start_min}-{end_min}")
                for i, ts in enumerate(times):
                    key = f"bg:{state}:{cid}.{log_id}:global:{start_min}-{end_min}:{i}"
                    overrides = special_overrides_background(cid, log_id, state, ts, host, key)
                    lvl, msg = render_log(cid, log_id, state, key, overrides)
                    emit_row(ts, lvl, msg, "", comp.get("svc", "") or "", host or "")
            else:
                for host in comp.get("hosts", []):
                    expected = rate * duration_min
                    cnt = alloc_count(expected, f"bg:{state}:{cid}.{log_id}:{host}")
                    times = schedule_times(start_dt, end_dt, cnt, f"bg:{state}:{cid}.{log_id}:{host}:{start_min}-{end_min}")
                    for i, ts in enumerate(times):
                        key = f"bg:{state}:{cid}.{log_id}:{host}:{start_min}-{end_min}:{i}"
                        overrides = special_overrides_background(cid, log_id, state, ts, host, key)
                        lvl, msg = render_log(cid, log_id, state, key, overrides)
                        emit_row(ts, lvl, msg, "", comp.get("svc", "") or "", host or "")

def simulate_flow_instance(flow: Dict[str, Any], state: str, start_ts: datetime, instance_idx: int, lat_mult: Optional[Dict[str, Any]]) -> None:
    global GLOBAL_PK_COUNTER

    flow_id = flow["id"]
    trace_on = bool(SYSTEM["tracing"]["on"]) and bool(flow.get("trace", False))
    trace_id = hex_of(f"trace:{flow_id}:{instance_idx}", 32) if trace_on else ""

    req_id = uuid_of(f"req:{flow_id}:{instance_idx}")
    is_write = "write" in flow_id
    is_conn_fail = "conn_fail" in flow_id or "db_conn_fail" in flow_id

    if is_conn_fail:
        method = "GET" if (h64(f"m:{flow_id}:{instance_idx}") % 2 == 0) else "POST"
    else:
        method = "POST" if is_write else "GET"

    if is_write:
        route = "/v1/txns" if (h64(f"r:{flow_id}:{instance_idx}") % 2 == 0) else "/v1/payments"
    else:
        routes = ["/v1/txns", "/v1/balance", "/v1/payments"]
        route = routes[h64(f"r:{flow_id}:{instance_idx}") % len(routes)]

    api_host = choose_api_host(instance_idx)

    if is_conn_fail:
        db_role = "master"
        db_host = "db-primary-1"
        op = None
    else:
        if "read_replicas" in flow_id:
            db_role = "replica"
            db_host = choose_replica_db_host(instance_idx)
            op = "SELECT"
        elif "read_master" in flow_id:
            db_role = "master"
            db_host = "db-primary-1"
            op = "SELECT"
        elif is_write:
            db_role = "master"
            db_host = "db-primary-1"
            op = "INSERT"
        else:
            db_role = "master"
            db_host = "db-primary-1"
            op = "SELECT"

    txn_id = uuid_of(f"txn:{flow_id}:{instance_idx}") if is_write else None
    pk = None
    amount_cents = None
    if is_write:
        pk = 1000000 + (GLOBAL_PK_COUNTER % 10001)
        GLOBAL_PK_COUNTER += 1
        amount_cents = 100 + (h64(f"amt:{flow_id}:{instance_idx}") % (500000 - 100 + 1))

    db_err = None
    if is_conn_fail:
        db_err = ["timeout", "connection_refused", "host_unreachable"][h64(f"e:{flow_id}:{instance_idx}") % 3]

    m50 = 1.0
    m95 = 1.0
    if lat_mult and flow_id in lat_mult:
        m50 = float(lat_mult[flow_id].get("p50", 1.0))
        m95 = float(lat_mult[flow_id].get("p95", 1.0))

    delays_ms: List[int] = []
    for j, (p50, p95) in enumerate(flow["latency_ms"]):
        delays_ms.append(sample_lognormish_between(float(p50), float(p95), f"lat:{flow_id}:{instance_idx}:{j}", mult_p50=m50, mult_p95=m95))

    cur = start_ts
    elapsed_ms = 0
    first_delay_ms = delays_ms[0] if delays_ms else 0

    for j, ref in enumerate(flow["emit"]):
        comp_id, log_id = ref.split(".")
        cur += timedelta(milliseconds=delays_ms[j])
        elapsed_ms += delays_ms[j]

        overrides: Dict[str, Any] = {}
        key = f"flow:{state}:{flow_id}:{instance_idx}:{j}"

        if comp_id == "api_service" and log_id in ("http_access_ok", "http_access_5xx"):
            overrides.update({"req_id": req_id, "method": method, "route": route, "trace_id": trace_id, "dur_ms": int(elapsed_ms)})
            if log_id == "http_access_5xx":
                overrides["err"] = "DB_CONN"

        if comp_id == "api_service" and log_id == "db_query":
            overrides.update({"op": op or "SELECT", "db_role": db_role, "db_host": db_host, "db_dur_ms": int(first_delay_ms)})

        if comp_id == "api_service" and log_id == "db_connect_error":
            overrides.update({"db_host": db_host, "db_err": db_err or "timeout", "waited_ms": int(first_delay_ms)})

        if comp_id == "api_service" and log_id == "trace_txn_commit":
            overrides.update({"txn_id": txn_id, "table": "transactions", "pk": pk, "amount_cents": amount_cents})

        lvl, msg = render_log(comp_id, log_id, state, key, overrides)
        emit_row(
            cur,
            lvl,
            msg,
            trace_id,
            COMP[comp_id].get("svc", "") or "",
            api_host if comp_id == "api_service" else (COMP[comp_id]["hosts"][0] if COMP[comp_id].get("hosts") else ""),
        )

def emit_flows_normal() -> None:
    scen = SCENARIO["scenario"]
    start_min = scen["time"]["phases"]["n"]["start_min"]
    end_min = scen["time"]["phases"]["n"]["end_min"]
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = end_min - start_min

    instance_base_idx = 0
    for fid, flow in FLOWS["n"].items():
        expected = float(flow["rpm"]) * duration_min
        cnt = alloc_count(expected, f"flow:n:{fid}")
        starts = schedule_times(start_dt, end_dt, cnt, f"flow:n:{fid}:{start_min}-{end_min}")
        for i, st in enumerate(starts):
            simulate_flow_instance(flow, "n", st, instance_base_idx + i, lat_mult=None)
        instance_base_idx += max(1, cnt)

def emit_flows_failure() -> None:
    instance_base_idx = 100000
    for interval in FAIL_INTERVALS:
        smin = int(interval["start_min"])
        emin = int(interval["end_min"])
        if emin <= smin:
            continue
        start_dt = BASE_TIME + timedelta(minutes=smin)
        end_dt = BASE_TIME + timedelta(minutes=emin)
        duration_min = emin - smin

        rate_mult = interval["rate_mult"]
        lat_mult = interval["lat_mult"]

        for fid, flow in FLOWS["f"].items():
            mult = float(rate_mult.get(fid, 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            if rpm_eff <= 0.0:
                continue
            expected = rpm_eff * duration_min
            cnt = alloc_count(expected, f"flow:f:{fid}:{smin}-{emin}")
            safe_end_dt = end_dt - timedelta(seconds=1.5)
            if safe_end_dt <= start_dt:
                safe_end_dt = end_dt
            starts = schedule_times(start_dt, safe_end_dt, cnt, f"flow:f:{fid}:{smin}-{emin}")
            for i, st in enumerate(starts):
                simulate_flow_instance(flow, "f", st, instance_base_idx + i, lat_mult=lat_mult)
            instance_base_idx += max(1, cnt)

def emit_one_shots() -> None:
    global SNAP_ID, SNAP_START_DT, GLOBAL_PK_COUNTER

    for at_min, lst in sorted(ONESHOTS_BY_MIN.items(), key=lambda kv: kv[0]):
        base_dt = BASE_TIME + timedelta(minutes=int(at_min))
        for os in lst:
            ref = os["ref"]
            cid, lid = ref.split(".")
            count = int(os["count"])
            hosts = os.get("hosts") or []
            if not hosts:
                hosts = COMP[cid].get("hosts", []) or [""]

            if ref in ("rds_controlplane.snapshot_start", "sre_console.action_start_rebuild", "rds_controlplane.snapshot_progress"):
                maybe_init_snap(int(at_min))

            for i in range(count):
                ts = base_dt + timedelta(seconds=(u01(f"os:{at_min}:{ref}:{i}") * 1.2) + (i / max(1, count)) * 0.2)
                host = hosts[i % len(hosts)]
                key = f"oneshot:{at_min}:{ref}:{i}"

                overrides: Dict[str, Any] = {}

                if ref == "rds_replicas.replication_error":
                    overrides["replica_id"] = REPLICA_ID_BY_HOST.get(host, "r1")
                    overrides["pk"] = 1000000 + ((GLOBAL_PK_COUNTER + i * 37) % 10001)
                    overrides["exec_pos"] = 100000 + (h64(f"{key}:pos") % (250000 - 100000 + 1))

                if ref == "monitoring.alert_5xx":
                    threshold = 100 + (h64(f"{key}:thr") % 51)
                    rate = threshold + 150 + (h64(f"{key}:rate") % 400)
                    overrides["threshold_per_min"] = threshold
                    overrides["rate_per_min"] = rate

                if ref == "monitoring.alert_replication":
                    overrides["failed_count"] = 5

                if ref == "monitoring.alert_cpu":
                    overrides["threshold"] = 80
                    overrides["cpu_pct"] = 88 + (h64(f"{key}:cpu") % 7)

                if ref == "rds_controlplane.snapshot_start":
                    overrides["snap_id"] = SNAP_ID
                    overrides["db_id"] = "api-mysql"
                    overrides["size_gb"] = 650

                if ref == "sre_console.action_start_rebuild":
                    overrides["snap_id"] = SNAP_ID
                    overrides["replicas"] = 5

                if ref == "rds_controlplane.snapshot_progress":
                    if SNAP_ID is None or SNAP_START_DT is None:
                        maybe_init_snap(int(at_min))
                    elapsed_min = int(max(1.0, (ts - SNAP_START_DT).total_seconds() / 60.0))
                    overrides["snap_id"] = SNAP_ID
                    overrides["elapsed_min"] = clamp_int(elapsed_min, 1, 360)
                    overrides["pct"] = clamp_int(int(overrides["elapsed_min"] / 4), 0, 99)

                lvl, msg = render_log(cid, lid, "f", key, overrides)
                emit_row(ts, lvl, msg, "", COMP[cid].get("svc", "") or "", host or "")

# ----------------------------
# Run simulation
# ----------------------------

def main() -> None:
    scen = SCENARIO["scenario"]
    nstart, nend = scen["time"]["phases"]["n"]["start_min"], scen["time"]["phases"]["n"]["end_min"]

    emit_background_interval("n", nstart, nend, rate_mult=None)
    emit_flows_normal()

    for interval in FAIL_INTERVALS:
        emit_background_interval("f", int(interval["start_min"]), int(interval["end_min"]), rate_mult=interval["rate_mult"])
    emit_flows_failure()

    emit_one_shots()

    df = pd.DataFrame(LOG_ROWS)
    df = df.sort_values(["timestamp_dt", "service", "host", "level"], kind="mergesort").reset_index(drop=True)

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    nrows = len(out)
    if nrows < 20000 or nrows > 100000:
        raise RuntimeError(f"Row count {nrows} outside required [20000, 100000].")

    out.to_csv("logs.csv", index=False)

if __name__ == "__main__":
    main()
