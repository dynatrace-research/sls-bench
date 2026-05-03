import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "manta_object_storage_incident_20150727"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "manta_api",
            "svc": "manta-api",
            "hosts": ["api1", "api2", "api3"],
            "logs": {
                "pg_query_ok": {
                    "lvl": "INFO",
                    "msg": "metadata query ok shard={shard} op={op} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["a", "b"]},
                        "op": {"k": "ch", "v": ["get_object", "put_object", "del_object", "list_dir"]},
                        "dur_ms": {"k": "i", "v": [5, 800]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "pg_query_timeout": {
                    "lvl": "WARN",
                    "msg": "metadata query timeout shard={shard} op={op} timeout_ms={timeout_ms} waited_ms={waited_ms} trace={trace_id}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["a"]},
                        "op": {"k": "ch", "v": ["get_object", "put_object", "del_object", "list_dir"]},
                        "timeout_ms": {"k": "i", "v": [20000, 20000]},
                        "waited_ms": {"k": "i", "v": [20000, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_2xx": {
                    "lvl": "INFO",
                    "msg": "resp {method} {ns_path} shard={shard} status={status} dur_ms={dur_ms} bytes={bytes} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "PUT", "DELETE"]},
                        "ns_path": {"k": "str", "v": "manta namespace path (e.g., /$user/stor/...)"},
                        "shard": {"k": "ch", "v": ["a", "b"]},
                        "status": {"k": "i", "v": [200, 204]},
                        "dur_ms": {"k": "i", "v": [10, 2500]},
                        "bytes": {"k": "i", "v": [0, 10485760]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_5xx": {
                    "lvl": "ERROR",
                    "msg": "resp {method} {ns_path} shard={shard} status=500 dur_ms={dur_ms} err={err} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "PUT", "DELETE"]},
                        "ns_path": {"k": "str", "v": "manta namespace path (e.g., /$user/stor/...)"},
                        "shard": {"k": "ch", "v": ["a"]},
                        "dur_ms": {"k": "i", "v": [20000, 20500]},
                        "err": {"k": "ch", "v": ["pg_timeout", "pg_lock_wait"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_metrics": {
                    "lvl": "INFO",
                    "msg": "api_metrics rps={rps} err_rate={err_rate} p95_ms={p95_ms}",
                    "vars": {"rps": {"k": "f", "v": [0.8, 2.5]}},
                    "state_vars": {
                        "n": {"err_rate": {"k": "f", "v": [0.0, 0.02]}, "p95_ms": {"k": "i", "v": [40, 250]}},
                        "f": {"err_rate": {"k": "f", "v": [0.0, 0.30]}, "p95_ms": {"k": "i", "v": [40, 32000]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "api_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "api_metrics", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "pg_shard_a",
            "svc": "postgres",
            "hosts": ["pg-a1"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete wrote_mb={wrote_mb} sync_ms={sync_ms}",
                    "vars": {"wrote_mb": {"k": "i", "v": [20, 600]}, "sync_ms": {"k": "i", "v": [10, 3000]}},
                },
                "autovacuum_start_wraparound": {
                    "lvl": "WARN",
                    "msg": "autovacuum started rel={rel} reason=wraparound xid_age={xid_age}",
                    "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "xid_age": {"k": "i", "v": [150000000, 260000000]}},
                },
                "autovacuum_progress": {
                    "lvl": "INFO",
                    "msg": "autovacuum progress rel={rel} phase={phase} blk_read={blk_read} sleep_ms={sleep_ms}",
                    "vars": {
                        "rel": {"k": "ch", "v": ["manta_metadata"]},
                        "phase": {"k": "ch", "v": ["heap_scan", "index_scan", "freeze", "cleanup"]},
                        "blk_read": {"k": "i", "v": [500, 120000]},
                        "sleep_ms": {"k": "i", "v": [0, 1500]},
                    },
                },
                "autovacuum_complete": {
                    "lvl": "INFO",
                    "msg": "autovacuum complete rel={rel} tuples_frozen={tuples_frozen}",
                    "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "tuples_frozen": {"k": "i", "v": [1000000, 80000000]}},
                },
                "query_lock_wait": {
                    "lvl": "WARN",
                    "msg": "statement waiting lock={lock} rel={rel} wait_s={wait_s}",
                    "vars": {"lock": {"k": "ch", "v": ["AccessShareLock"]}, "rel": {"k": "ch", "v": ["manta_metadata"]}, "wait_s": {"k": "i", "v": [1, 60]}},
                },
                "ddl_exec_trigger": {
                    "lvl": "INFO",
                    "msg": "statement executed ddl=drop_create_trigger rel={rel} dur_ms={dur_ms}",
                    "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "dur_ms": {"k": "i", "v": [50, 1200]}},
                },
                "ddl_drop_trigger_enqueued": {
                    "lvl": "WARN",
                    "msg": "ddl enqueued ddl=DROP TRIGGER rel={rel} pid={pid}",
                    "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "pid": {"k": "i", "v": [1000, 50000]}},
                },
                "ddl_lock_wait_drop_trigger": {
                    "lvl": "WARN",
                    "msg": "statement waiting lock=AccessExclusiveLock rel={rel} wait_s={wait_s} ddl=DROP TRIGGER pid={pid}",
                    "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "wait_s": {"k": "i", "v": [30, 30000]}, "pid": {"k": "i", "v": [1000, 50000]}},
                },
                "shutdown_requested": {
                    "lvl": "WARN",
                    "msg": "received fast shutdown request from {by}",
                    "vars": {"by": {"k": "ch", "v": ["admin"]}},
                },
                "startup_complete": {"lvl": "INFO", "msg": "database system is ready to accept connections", "vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.2}]},
                "f": {
                    "emit": [
                        {"id": "checkpoint_complete", "per_min": 0.2},
                        {"id": "query_lock_wait", "per_min": 20.0},
                        {"id": "autovacuum_progress", "per_min": 1.0},
                        {"id": "ddl_lock_wait_drop_trigger", "per_min": 0.3},
                    ]
                },
            },
        },
        {
            "id": "pg_shard_b",
            "svc": "postgres",
            "hosts": ["pg-b1"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete wrote_mb={wrote_mb} sync_ms={sync_ms}",
                    "vars": {"wrote_mb": {"k": "i", "v": [20, 600]}, "sync_ms": {"k": "i", "v": [10, 3000]}},
                }
            },
            "beh": {"n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.2}]}, "f": {"emit": [{"id": "checkpoint_complete", "per_min": 0.2}]}},
        },
        {
            "id": "pg_monitor",
            "svc": "pg-monitor",
            "hosts": ["mon1"],
            "logs": {
                "pg_stats_ok": {
                    "lvl": "INFO",
                    "msg": "pg_stats shard={shard} active={active} waiting={waiting} exclusive_pending={exclusive_pending} autovacuum={autovacuum} oldest_wait_s={oldest_wait_s}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["a", "b"]},
                        "active": {"k": "i", "v": [5, 90]},
                        "waiting": {"k": "i", "v": [0, 3]},
                        "exclusive_pending": {"k": "i", "v": [0, 0]},
                        "autovacuum": {"k": "ch", "v": ["none", "regular"]},
                        "oldest_wait_s": {"k": "i", "v": [0, 8]},
                    },
                },
                "pg_stats_bad": {
                    "lvl": "WARN",
                    "msg": "pg_stats shard=a active={active} waiting={waiting} exclusive_pending=1 autovacuum=wraparound oldest_wait_s={oldest_wait_s}",
                    "vars": {"active": {"k": "i", "v": [20, 180]}, "waiting": {"k": "i", "v": [50, 450]}, "oldest_wait_s": {"k": "i", "v": [30, 2500]}},
                },
                "pg_alert": {
                    "lvl": "ERROR",
                    "msg": "ALERT metadata shard a lock waits elevated waiting={waiting} oldest_wait_s={oldest_wait_s}",
                    "vars": {"waiting": {"k": "i", "v": [50, 450]}, "oldest_wait_s": {"k": "i", "v": [30, 2500]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pg_stats_ok", "per_min": 2.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "pg_stats_bad", "per_min": 4.0, "scope": "global"},
                        {"id": "pg_stats_ok", "per_min": 4.0, "scope": "global"},
                        {"id": "pg_alert", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "trigger_mgr",
            "svc": "trigger-mgr",
            "hosts": ["trig1"],
            "logs": {
                "ddl_start": {"lvl": "INFO", "msg": "ensure_trigger start shard=a rel={rel}", "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}}},
                "ddl_done": {"lvl": "INFO", "msg": "ensure_trigger done shard=a result=ok dur_ms={dur_ms}", "vars": {"dur_ms": {"k": "i", "v": [50, 2000]}}},
                "ddl_client_timeout": {"lvl": "ERROR", "msg": "ensure_trigger timeout shard=a waited_ms={waited_ms}", "vars": {"waited_ms": {"k": "i", "v": [10000, 120000]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ops_tool",
            "svc": "ops",
            "hosts": ["ops1"],
            "logs": {
                "pg_lock_snapshot_saved": {"lvl": "INFO", "msg": "saved pg lock snapshot shard=a file={file}", "vars": {"file": {"k": "str", "v": "lock_snapshot_*.json"}}},
                "pg_restart_cmd": {"lvl": "WARN", "msg": "executed postgres restart shard=a method=svc_restart", "vars": {}},
                "zfs_prefetch": {"lvl": "INFO", "msg": "prefetched table files rel={rel} bytes={bytes}", "vars": {"rel": {"k": "ch", "v": ["manta_metadata"]}, "bytes": {"k": "i", "v": [1073741824, 17179869184]}}},
                "patch_vacuum_delay": {"lvl": "WARN", "msg": "patched vacuum delay in running autovacuum process pid={pid}", "vars": {"pid": {"k": "i", "v": [1000, 50000]}}},
                "pg_autovacuum_threshold_tuned": {
                    "lvl": "WARN",
                    "msg": "tuned autovacuum threshold shard=a param={param} old={old} new={new} action={action}",
                    "vars": {"param": {"k": "ch", "v": ["vacuum_freeze_table_age"]}, "old": {"k": "i", "v": [150000000, 250000000]}, "new": {"k": "i", "v": [500000000, 1000000000]}, "action": {"k": "ch", "v": ["restart"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "object_op_shard_a_success",
                    "rpm": 70.0,
                    "emit": ["manta_api.pg_query_ok", "manta_api.http_2xx"],
                    "latency_ms": [[25, 90], [30, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "object_op_shard_b_success",
                    "rpm": 230.0,
                    "emit": ["manta_api.pg_query_ok", "manta_api.http_2xx"],
                    "latency_ms": [[20, 70], [25, 110]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ensure_trigger_success",
                    "rpm": 0.1,
                    "emit": ["trigger_mgr.ddl_start", "pg_shard_a.ddl_exec_trigger", "trigger_mgr.ddl_done"],
                    "latency_ms": [[5, 20], [80, 300], [90, 400]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "object_op_shard_a_timeout",
                    "rpm": 60.0,
                    "emit": ["manta_api.pg_query_timeout", "manta_api.http_5xx"],
                    "latency_ms": [[20000, 20050], [20, 300]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "object_op_shard_a_success",
                    "rpm": 10.0,
                    "emit": ["manta_api.pg_query_ok", "manta_api.http_2xx"],
                    "latency_ms": [[120, 900], [150, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "object_op_shard_b_success",
                    "rpm": 230.0,
                    "emit": ["manta_api.pg_query_ok", "manta_api.http_2xx"],
                    "latency_ms": [[20, 75], [25, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ensure_trigger_ok",
                    "rpm": 0.2,
                    "emit": ["trigger_mgr.ddl_start", "pg_shard_a.ddl_exec_trigger", "trigger_mgr.ddl_done"],
                    "latency_ms": [[5, 25], [80, 500], [90, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "manta_outage_wraparound_autovacuum_lock_20150727"},
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "ensure_trigger_ok": 0.0,
                        "pg_monitor.pg_stats_ok": 0.0,
                        "pg_monitor.pg_stats_bad": 1.0,
                        "pg_monitor.pg_alert": 1.0,
                        "pg_shard_a.query_lock_wait": 1.0,
                        "pg_shard_a.autovacuum_progress": 1.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 1.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "pg_shard_a.autovacuum_start_wraparound", "count": 1, "hosts": ["pg-a1"]},
                        {"ref": "pg_shard_a.ddl_drop_trigger_enqueued", "count": 1, "hosts": ["pg-a1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 35,
                    "rate_multipliers": {
                        "object_op_shard_a_timeout": 0.0,
                        "object_op_shard_a_success": 6.0,
                        "ensure_trigger_ok": 1.0,
                        "pg_monitor.pg_stats_bad": 0.0,
                        "pg_monitor.pg_stats_ok": 1.0,
                        "pg_monitor.pg_alert": 0.0,
                        "pg_shard_a.query_lock_wait": 0.0,
                        "pg_shard_a.autovacuum_progress": 0.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 0.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 0.2, "p95": 0.1}},
                    "one_shots": [
                        {"ref": "ops_tool.pg_lock_snapshot_saved", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.pg_restart_cmd", "count": 1, "hosts": ["ops1"]},
                        {"ref": "pg_shard_a.shutdown_requested", "count": 1, "hosts": ["pg-a1"]},
                        {"ref": "pg_shard_a.startup_complete", "count": 1, "hosts": ["pg-a1"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 41,
                    "rate_multipliers": {
                        "object_op_shard_a_timeout": 1.0,
                        "object_op_shard_a_success": 1.0,
                        "ensure_trigger_ok": 0.0,
                        "pg_monitor.pg_stats_ok": 0.0,
                        "pg_monitor.pg_stats_bad": 1.0,
                        "pg_monitor.pg_alert": 1.0,
                        "pg_shard_a.query_lock_wait": 1.0,
                        "pg_shard_a.autovacuum_progress": 1.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 1.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "pg_shard_a.autovacuum_start_wraparound", "count": 1, "hosts": ["pg-a1"]},
                        {"ref": "pg_shard_a.ddl_drop_trigger_enqueued", "count": 1, "hosts": ["pg-a1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 47,
                    "rate_multipliers": {"pg_shard_a.autovacuum_progress": 4.0},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_tool.zfs_prefetch", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.patch_vacuum_delay", "count": 1, "hosts": ["ops1"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 55,
                    "rate_multipliers": {
                        "object_op_shard_a_timeout": 0.0,
                        "object_op_shard_a_success": 7.0,
                        "ensure_trigger_ok": 1.0,
                        "pg_shard_a.autovacuum_progress": 0.0,
                        "pg_shard_a.query_lock_wait": 0.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 0.0,
                        "pg_monitor.pg_stats_bad": 0.0,
                        "pg_monitor.pg_stats_ok": 1.0,
                        "pg_monitor.pg_alert": 0.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 0.2, "p95": 0.1}},
                    "one_shots": [{"ref": "pg_shard_a.autovacuum_complete", "count": 1, "hosts": ["pg-a1"]}],
                },
                {
                    "order": 6,
                    "at_min": 58,
                    "rate_multipliers": {
                        "ensure_trigger_ok": 0.0,
                        "object_op_shard_a_timeout": 1.0,
                        "object_op_shard_a_success": 1.0,
                        "pg_monitor.pg_stats_ok": 0.0,
                        "pg_monitor.pg_stats_bad": 1.0,
                        "pg_monitor.pg_alert": 1.0,
                        "pg_shard_a.query_lock_wait": 1.0,
                        "pg_shard_a.autovacuum_progress": 2.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 1.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "pg_shard_a.autovacuum_start_wraparound", "count": 1, "hosts": ["pg-a1"]},
                        {"ref": "pg_shard_a.ddl_drop_trigger_enqueued", "count": 1, "hosts": ["pg-a1"]},
                    ],
                },
                {
                    "order": 7,
                    "at_min": 59,
                    "rate_multipliers": {
                        "object_op_shard_a_timeout": 0.0,
                        "object_op_shard_a_success": 7.0,
                        "ensure_trigger_ok": 1.0,
                        "pg_shard_a.query_lock_wait": 0.0,
                        "pg_shard_a.autovacuum_progress": 0.0,
                        "pg_shard_a.ddl_lock_wait_drop_trigger": 0.0,
                        "pg_monitor.pg_stats_bad": 0.0,
                        "pg_monitor.pg_stats_ok": 1.0,
                        "pg_monitor.pg_alert": 0.0,
                    },
                    "latency_multipliers": {"object_op_shard_a_success": {"p50": 0.2, "p95": 0.1}},
                    "one_shots": [
                        {"ref": "ops_tool.pg_autovacuum_threshold_tuned", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.pg_restart_cmd", "count": 1, "hosts": ["ops1"]},
                        {"ref": "pg_shard_a.shutdown_requested", "count": 1, "hosts": ["pg-a1"]},
                        {"ref": "pg_shard_a.startup_complete", "count": 1, "hosts": ["pg-a1"]},
                    ],
                },
            ]
        }
    },
}


BASE_DT = datetime(2015, 7, 27, 0, 0, 0, tzinfo=timezone.utc)
BASE_EPOCH_MS = int(BASE_DT.timestamp() * 1000)
SEED_TAG = "manta_object_storage_incident_20150727|manta_outage_wraparound_autovacuum_lock_20150727"


def _h64(*parts: Any) -> int:
    s = "|".join(str(p) for p in (SEED_TAG,) + parts).encode("utf-8")
    return int.from_bytes(hashlib.blake2b(s, digest_size=8).digest(), "big", signed=False)


def _u01(*parts: Any) -> float:
    # 53-bit mantissa uniform in [0,1)
    x = _h64(*parts) >> 11
    return x / float(1 << 53)


def _choice(seq: List[Any], *parts: Any) -> Any:
    if not seq:
        raise ValueError("empty choice sequence")
    return seq[_h64(*parts) % len(seq)]


def _gen_hex(n: int, *parts: Any) -> str:
    s = "|".join(str(p) for p in (SEED_TAG,) + parts).encode("utf-8")
    return hashlib.blake2b(s, digest_size=max(16, (n + 1) // 2)).hexdigest()[:n]


def _sample_int(lo: int, hi: int, *parts: Any) -> int:
    if lo == hi:
        return int(lo)
    u = _u01(*parts)
    return int(lo + math.floor(u * (hi - lo + 1)))


def _sample_float(lo: float, hi: float, *parts: Any) -> float:
    if lo == hi:
        return float(lo)
    u = _u01(*parts)
    return lo + u * (hi - lo)


def _fmt_float(x: float) -> str:
    return f"{x:.3f}"


def _alloc_count(expected: float, key: str, carry: Dict[str, float]) -> int:
    total = expected + carry.get(key, 0.0)
    n = int(math.floor(total + 1e-12))
    carry[key] = total - n
    return n


def _schedule_even(start_ms: int, end_ms: int, n: int, key: str) -> List[int]:
    if n <= 0:
        return []
    dur = max(1, end_ms - start_ms)
    step = dur / float(n)
    jitter_amp = int(min(500.0, max(50.0, step * 0.15)))
    out = []
    for i in range(n):
        base = start_ms + int((i + 0.5) * step)
        jit = int(((_u01(key, i, "jit") - 0.5) * 2.0) * jitter_amp)
        t = base + jit
        if t < start_ms:
            t = start_ms
        if t >= end_ms:
            t = end_ms - 1
        out.append(t)
    return out


def _sample_latency_ms(p50: float, p95: float, *parts: Any) -> int:
    if p95 < p50:
        p50, p95 = p95, p50
    if p50 == p95:
        return int(round(p50))
    u = _u01(*parts)
    x = p50 + (p95 - p50) * (u ** 3)
    return int(max(0, round(x)))


@dataclass(frozen=True)
class LogTemplate:
    key: str
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Dict[str, Any]]
    svc: str
    hosts: List[str]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, LogTemplate], Dict[Tuple[str, str], Any]]:
    comp_by_id: Dict[str, Any] = {c["id"]: c for c in system["components"]}
    tmpl_by_key: Dict[str, LogTemplate] = {}
    flow_by_state_id: Dict[Tuple[str, str], Any] = {}
    for c in system["components"]:
        for log_id, ld in c.get("logs", {}).items():
            key = f"{c['id']}.{log_id}"
            tmpl_by_key[key] = LogTemplate(
                key=key,
                component_id=c["id"],
                log_id=log_id,
                level=ld["lvl"],
                msg=ld["msg"],
                vars=ld.get("vars", {}) or {},
                state_vars=ld.get("state_vars", {}) or {},
                svc=c.get("svc", "") or "",
                hosts=c.get("hosts", []) or [],
            )
    for st, fs in system["flows"].items():
        for f in fs.get("req", []):
            flow_by_state_id[(st, f["id"])] = f
    return comp_by_id, tmpl_by_key, flow_by_state_id


def _gen_ns_path(shard: str, op: str, instance_tag: str) -> str:
    u = _h64("ns", shard, op, instance_tag)
    user = 100 + (u % 900)
    obj = (u // 1000) % 500000
    if op == "list_dir":
        d = (u // 10000000) % 5000
        return f"/$user{user}/stor/dir{d}/"
    else:
        return f"/$user{user}/stor/obj{obj}"


def _op_to_method(op: str) -> str:
    if op in ("get_object", "list_dir"):
        return "GET"
    if op == "put_object":
        return "PUT"
    if op == "del_object":
        return "DELETE"
    return "GET"


def _pick_shard_for_flow(flow_id: str) -> Optional[str]:
    if "_shard_a_" in flow_id:
        return "a"
    if "_shard_b_" in flow_id:
        return "b"
    if "ensure_trigger" in flow_id:
        return "a"
    return None


def _render_template(
    tmpl: LogTemplate,
    state: str,
    bind: Dict[str, Any],
    render_tag: str,
) -> str:
    values: Dict[str, Any] = {}

    for k, dom in tmpl.vars.items():
        kind = dom["k"]
        v = dom.get("v")
        if kind == "ch":
            values[k] = _choice(list(v), render_tag, tmpl.key, k)
        elif kind == "i":
            values[k] = _sample_int(int(v[0]), int(v[1]), render_tag, tmpl.key, k)
        elif kind == "f":
            values[k] = _fmt_float(_sample_float(float(v[0]), float(v[1]), render_tag, tmpl.key, k))
        elif kind == "hex":
            values[k] = _gen_hex(int(v), render_tag, tmpl.key, k)
        elif kind == "str":
            values[k] = f"{v}"
        else:
            values[k] = str(v)

    sv = tmpl.state_vars.get(state, {})
    for k, dom in sv.items():
        kind = dom["k"]
        v = dom.get("v")
        if kind == "ch":
            values[k] = _choice(list(v), render_tag, tmpl.key, state, k)
        elif kind == "i":
            values[k] = _sample_int(int(v[0]), int(v[1]), render_tag, tmpl.key, state, k)
        elif kind == "f":
            values[k] = _fmt_float(_sample_float(float(v[0]), float(v[1]), render_tag, tmpl.key, state, k))
        elif kind == "hex":
            values[k] = _gen_hex(int(v), render_tag, tmpl.key, state, k)
        else:
            values[k] = str(v)

    for k, v in bind.items():
        if isinstance(v, float):
            values[k] = _fmt_float(v)
        else:
            values[k] = v

    return tmpl.msg.format(**values)


def build_failure_intervals(
    scenario: Dict[str, Any],
    system: Dict[str, Any],
) -> List[Dict[str, Any]]:
    fstart = scenario["time"]["phases"]["f"]["start_min"]
    fend = scenario["time"]["phases"]["f"]["end_min"]
    events = sorted(scenario["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] < fend}) + [fend]
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Tuple[float, float]] = {}

    idx_by_at: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        idx_by_at.setdefault(e["at_min"], []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]
        for e in idx_by_at.get(start, []):
            for k, v in (e.get("rate_multipliers", {}) or {}).items():
                active_rate[k] = float(v)
            for fid, mm in (e.get("latency_multipliers", {}) or {}).items():
                active_lat[fid] = (float(mm.get("p50", 1.0)), float(mm.get("p95", 1.0)))

        degraded = active_rate.get("object_op_shard_a_timeout", 1.0) > 0.0
        intervals.append(
            {
                "start_min": start,
                "end_min": end,
                "rate_multipliers": dict(active_rate),
                "latency_multipliers": dict(active_lat),
                "degraded": degraded,
            }
        )
    return intervals


def generate_background_logs(
    comp_by_id: Dict[str, Any],
    tmpl_by_key: Dict[str, LogTemplate],
    phase_state: str,
    start_min: int,
    end_min: int,
    failure_intervals: Optional[List[Dict[str, Any]]] = None,
) -> List[Tuple[int, str, str, str, str, str]]:
    rows: List[Tuple[int, str, str, str, str, str]] = []
    carry: Dict[str, float] = {}

    def emit_one(template_key: str, state: str, ts_ms: int, host: str, bind: Dict[str, Any], render_tag: str):
        tmpl = tmpl_by_key[template_key]
        msg = _render_template(tmpl, state, bind, render_tag)
        rows.append((ts_ms, tmpl.level, msg, "", tmpl.svc, host))

    if phase_state == "n":
        istart_ms = BASE_EPOCH_MS + start_min * 60_000
        iend_ms = BASE_EPOCH_MS + end_min * 60_000
        for comp in comp_by_id.values():
            beh = comp.get("beh", {}).get("n", {}).get("emit", []) or []
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                template_key = f"{comp['id']}.{log_id}"
                hosts = comp.get("hosts", []) or [""]
                dur_min = float(end_min - start_min)

                if scope == "global":
                    key = f"bg|n|{template_key}|global"
                    n = _alloc_count(per_min * dur_min, key, carry)
                    tss = _schedule_even(istart_ms, iend_ms, n, key)
                    for j, t in enumerate(tss):
                        host = _choice(hosts, key, "host", j)
                        bind = {}
                        if template_key == "pg_monitor.pg_stats_ok":
                            bind["shard"] = _choice(["a", "b"], key, "shard", j)
                        emit_one(template_key, "n", t, host, bind, f"{key}|{j}")
                else:
                    for h in hosts:
                        key = f"bg|n|{template_key}|{h}"
                        n = _alloc_count(per_min * dur_min, key, carry)
                        tss = _schedule_even(istart_ms, iend_ms, n, key)
                        for j, t in enumerate(tss):
                            bind = {}
                            if template_key == "pg_monitor.pg_stats_ok":
                                bind["shard"] = _choice(["a", "b"], key, "shard", j)
                            emit_one(template_key, "n", t, h, bind, f"{key}|{j}")
        return rows

    assert failure_intervals is not None
    for interval in failure_intervals:
        istart_min = interval["start_min"]
        iend_min = interval["end_min"]
        istart_ms = BASE_EPOCH_MS + istart_min * 60_000
        iend_ms = BASE_EPOCH_MS + iend_min * 60_000
        dur_min = float(iend_min - istart_min)
        rate_mults = interval["rate_multipliers"]
        degraded = interval["degraded"]

        for comp in comp_by_id.values():
            beh = comp.get("beh", {}).get("f", {}).get("emit", []) or []
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                template_key = f"{comp['id']}.{log_id}"
                mult = float(rate_mults.get(template_key, 1.0))
                eff = per_min * mult
                hosts = comp.get("hosts", []) or [""]

                if eff <= 0.0 or dur_min <= 0.0:
                    continue

                if scope == "global":
                    key = f"bg|f|{template_key}|global|{istart_min}-{iend_min}"
                    n = _alloc_count(eff * dur_min, key, carry)
                    tss = _schedule_even(istart_ms, iend_ms, n, key)
                    for j, t in enumerate(tss):
                        host = _choice(hosts, key, "host", j)
                        bind: Dict[str, Any] = {}
                        if template_key == "manta_api.api_metrics":
                            rps = _sample_float(0.8, 2.5, key, "rps", j)
                            bind["rps"] = float(_fmt_float(rps))
                            if degraded:
                                bind["err_rate"] = _fmt_float(_sample_float(0.10, 0.30, key, "err", j))
                                bind["p95_ms"] = _sample_int(10000, 32000, key, "p95", j)
                            else:
                                bind["err_rate"] = _fmt_float(_sample_float(0.0, 0.02, key, "err", j))
                                bind["p95_ms"] = _sample_int(40, 250, key, "p95", j)
                        elif template_key == "pg_monitor.pg_stats_ok":
                            shard = "a" if _u01(key, "shard", j) < 0.7 else "b"
                            bind["shard"] = shard
                            bind["autovacuum"] = "none" if (not degraded and shard == "a") else _choice(["none", "regular"], key, "av", j)
                        elif template_key == "pg_shard_a.autovacuum_progress":
                            if mult > 1.0:
                                bind["sleep_ms"] = _sample_int(0, 200, key, "sleep", j)
                            bind["phase"] = ["heap_scan", "index_scan", "freeze", "cleanup"][j % 4]
                            base_blk = 500 + int((j + 1) * (119500 / max(1, n)))
                            bind["blk_read"] = int(min(120000, max(500, base_blk)))
                        emit_one(template_key, "f", t, host, bind, f"{key}|{j}")
                else:
                    for h in hosts:
                        key = f"bg|f|{template_key}|{h}|{istart_min}-{iend_min}"
                        n = _alloc_count(eff * dur_min, key, carry)
                        tss = _schedule_even(istart_ms, iend_ms, n, key)
                        for j, t in enumerate(tss):
                            bind = {}
                            if template_key == "manta_api.api_metrics":
                                rps = _sample_float(0.8, 2.5, key, "rps", j)
                                bind["rps"] = float(_fmt_float(rps))
                                if degraded:
                                    bind["err_rate"] = _fmt_float(_sample_float(0.10, 0.30, key, "err", j))
                                    bind["p95_ms"] = _sample_int(10000, 32000, key, "p95", j)
                                else:
                                    bind["err_rate"] = _fmt_float(_sample_float(0.0, 0.02, key, "err", j))
                                    bind["p95_ms"] = _sample_int(40, 250, key, "p95", j)
                            elif template_key == "pg_shard_a.autovacuum_progress":
                                if mult > 1.0:
                                    bind["sleep_ms"] = _sample_int(0, 200, key, "sleep", j)
                                bind["phase"] = ["heap_scan", "index_scan", "freeze", "cleanup"][j % 4]
                                base_blk = 500 + int((j + 1) * (119500 / max(1, n)))
                                bind["blk_read"] = int(min(120000, max(500, base_blk)))
                            emit_one(template_key, "f", t, h, bind, f"{key}|{j}")

    return rows


def simulate_flows_for_interval(
    flows: List[Dict[str, Any]],
    tmpl_by_key: Dict[str, LogTemplate],
    state: str,
    start_min: int,
    end_min: int,
    rate_multipliers: Optional[Dict[str, float]] = None,
    latency_multipliers: Optional[Dict[str, Tuple[float, float]]] = None,
) -> List[Tuple[int, str, str, str, str, str]]:
    rows: List[Tuple[int, str, str, str, str, str]] = []
    carry: Dict[str, float] = {}

    start_ms = BASE_EPOCH_MS + start_min * 60_000
    end_ms = BASE_EPOCH_MS + end_min * 60_000
    dur_min = float(end_min - start_min)
    if dur_min <= 0:
        return rows

    rate_multipliers = rate_multipliers or {}
    latency_multipliers = latency_multipliers or {}

    scheduled_instances: List[Tuple[str, Dict[str, Any], int]] = []
    for f in flows:
        fid = f["id"]
        rpm = float(f["rpm"])
        mult = float(rate_multipliers.get(fid, 1.0))
        eff_rpm = rpm * mult
        if eff_rpm <= 0.0:
            continue
        exp = (eff_rpm * dur_min)
        key_base = f"flow|{state}|{fid}|{start_min}-{end_min}"
        n = _alloc_count(exp, key_base, carry)
        tss = _schedule_even(start_ms, end_ms, n, key_base)
        for t in tss:
            scheduled_instances.append((fid, f, t))

    scheduled_instances.sort(key=lambda x: (x[2], x[0]))

    for inst_idx, (fid, fdef, inst_start) in enumerate(scheduled_instances):
        trace_id = _gen_hex(32, "trace", state, fid, inst_start, inst_idx) if fdef.get("trace", False) else ""
        shard = _pick_shard_for_flow(fid)

        op = _choice(["get_object", "put_object", "del_object", "list_dir"], "op", state, fid, inst_start)
        method = _op_to_method(op)
        ns_path = _gen_ns_path(shard or "a", op, f"{fid}|{inst_start}|{inst_idx}")

        component_host: Dict[str, str] = {}

        def pick_host(component_id: str) -> str:
            if component_id in component_host:
                return component_host[component_id]
            tmpl_any = None
            for t in tmpl_by_key.values():
                if t.component_id == component_id:
                    tmpl_any = t
                    break
            hosts = (tmpl_any.hosts if tmpl_any else [""])
            h = _choice(hosts if hosts else [""], "host", component_id, trace_id or f"{inst_start}", fid)
            component_host[component_id] = h
            return h

        p50m, p95m = latency_multipliers.get(fid, (1.0, 1.0))

        emit_keys: List[str] = list(fdef["emit"])
        lat_pairs: List[List[int]] = list(fdef["latency_ms"])
        cum_ms = 0

        err = "pg_timeout" if fid == "object_op_shard_a_timeout" else ""

        for step_idx, template_key in enumerate(emit_keys):
            tmpl = tmpl_by_key[template_key]
            base_p50, base_p95 = float(lat_pairs[step_idx][0]), float(lat_pairs[step_idx][1])

            eff_p50 = base_p50 * (p50m if state == "f" else 1.0)
            eff_p95 = base_p95 * (p95m if state == "f" else 1.0)

            if template_key == "manta_api.pg_query_timeout":
                delay = 20000
            else:
                delay = _sample_latency_ms(eff_p50, eff_p95, "lat", state, fid, inst_start, step_idx)

            if template_key == "manta_api.pg_query_ok":
                delay = int(min(delay, 800))

            cum_ms += delay
            ts_ms = inst_start + cum_ms

            bind: Dict[str, Any] = {}
            if "trace_id" in tmpl.vars or "{trace_id}" in tmpl.msg:
                bind["trace_id"] = trace_id if trace_id else ""

            if template_key in ("manta_api.pg_query_ok", "manta_api.pg_query_timeout"):
                if shard is not None:
                    bind["shard"] = shard
                bind["op"] = op
                if template_key == "manta_api.pg_query_ok":
                    bind["dur_ms"] = int(delay)
                else:
                    bind["timeout_ms"] = 20000
                    bind["waited_ms"] = 20000

            elif template_key == "manta_api.http_2xx":
                bind["method"] = method
                bind["ns_path"] = ns_path
                if shard is not None:
                    bind["shard"] = shard
                if method == "DELETE":
                    status = 204
                elif method == "PUT":
                    status = 204 if _u01("status", trace_id, fid) < 0.7 else 200
                else:
                    status = 200
                bind["status"] = status
                bind["dur_ms"] = int(cum_ms)
                if method == "DELETE":
                    b = 0
                else:
                    b = _sample_int(0, 10485760, "bytes", trace_id, fid, inst_start)
                bind["bytes"] = b

            elif template_key == "manta_api.http_5xx":
                bind["method"] = method
                bind["ns_path"] = ns_path
                bind["shard"] = "a"
                bind["dur_ms"] = int(min(20500, max(20000, cum_ms)))
                bind["err"] = err if err else _choice(["pg_timeout", "pg_lock_wait"], "err", trace_id, fid)

            elif template_key == "trigger_mgr.ddl_start":
                bind["rel"] = "manta_metadata"
            elif template_key == "pg_shard_a.ddl_exec_trigger":
                bind["rel"] = "manta_metadata"
                bind["dur_ms"] = int(min(1200, max(50, delay)))
            elif template_key == "trigger_mgr.ddl_done":
                bind["dur_ms"] = int(min(2000, max(50, cum_ms)))

            host = pick_host(tmpl.component_id)
            msg = _render_template(tmpl, state, bind, f"flow|{state}|{fid}|{inst_start}|{inst_idx}|{step_idx}")
            rows.append((ts_ms, tmpl.level, msg, trace_id if fdef.get("trace", False) else "", tmpl.svc, host))

    return rows


def generate_flow_logs(
    system: Dict[str, Any],
    scenario: Dict[str, Any],
    tmpl_by_key: Dict[str, LogTemplate],
    failure_intervals: List[Dict[str, Any]],
) -> List[Tuple[int, str, str, str, str, str]]:
    rows: List[Tuple[int, str, str, str, str, str]] = []
    nstart, nend = scenario["time"]["phases"]["n"]["start_min"], scenario["time"]["phases"]["n"]["end_min"]
    fstart, fend = scenario["time"]["phases"]["f"]["start_min"], scenario["time"]["phases"]["f"]["end_min"]

    rows.extend(simulate_flows_for_interval(system["flows"]["n"]["req"], tmpl_by_key, "n", nstart, nend))

    for interval in failure_intervals:
        st = interval["start_min"]
        en = interval["end_min"]
        rows.extend(
            simulate_flows_for_interval(
                system["flows"]["f"]["req"],
                tmpl_by_key,
                "f",
                st,
                en,
                rate_multipliers=interval["rate_multipliers"],
                latency_multipliers=interval["latency_multipliers"],
            )
        )
    return rows


def generate_one_shots(
    scenario: Dict[str, Any],
    tmpl_by_key: Dict[str, LogTemplate],
) -> List[Tuple[int, str, str, str, str, str]]:
    rows: List[Tuple[int, str, str, str, str, str]] = []
    events = sorted(scenario["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    for e in events:
        at_min = int(e["at_min"])
        base_ms = BASE_EPOCH_MS + at_min * 60_000
        shots = e.get("one_shots", []) or []
        seq = 0
        for s in shots:
            ref = s["ref"]
            count = int(s["count"])
            hosts = list(s.get("hosts", []) or [])
            tmpl = tmpl_by_key[ref]
            allowed_hosts = hosts if hosts else (tmpl.hosts if tmpl.hosts else [""])
            for k in range(count):
                jitter = int(_u01("oneshot", ref, at_min, k) * 4000)
                ts_ms = base_ms + 1000 + seq * 120 + jitter
                bind: Dict[str, Any] = {}
                if ref == "ops_tool.pg_lock_snapshot_saved":
                    bind["file"] = f"lock_snapshot_{BASE_DT.strftime('%Y%m%d')}T{at_min:02d}m_{k}.json"
                msg = _render_template(tmpl, "f", bind, f"oneshot|{at_min}|{ref}|{k}")
                host = allowed_hosts[_h64("oneshot", ref, at_min, k, "host") % len(allowed_hosts)]
                rows.append((ts_ms, tmpl.level, msg, "", tmpl.svc, host))
                seq += 1
    return rows


def main() -> None:
    # Deterministic seeding for verifier expectations (even though this simulator uses hash-based determinism).
    seed32 = _h64("seed") % (2**32)
    random.seed(seed32)
    np.random.seed(seed32)

    comp_by_id, tmpl_by_key, _flow_by_state_id = build_indices(SYSTEM)

    nstart, nend = SCENARIO["time"]["phases"]["n"]["start_min"], SCENARIO["time"]["phases"]["n"]["end_min"]
    fstart, fend = SCENARIO["time"]["phases"]["f"]["start_min"], SCENARIO["time"]["phases"]["f"]["end_min"]

    failure_intervals = build_failure_intervals(SCENARIO, SYSTEM)

    rows: List[Tuple[int, str, str, str, str, str]] = []
    rows.extend(generate_background_logs(comp_by_id, tmpl_by_key, "n", nstart, nend))
    rows.extend(generate_background_logs(comp_by_id, tmpl_by_key, "f", fstart, fend, failure_intervals=failure_intervals))
    rows.extend(generate_flow_logs(SYSTEM, SCENARIO, tmpl_by_key, failure_intervals))
    rows.extend(generate_one_shots(SCENARIO, tmpl_by_key))

    df = pd.DataFrame(rows, columns=["_ts_ms", "level", "message", "trace_id", "service", "host"])
    df["_seq"] = np.arange(len(df), dtype=np.int64)
    df.sort_values(["_ts_ms", "_seq"], inplace=True, kind="mergesort")

    ts = pd.to_datetime(df["_ts_ms"].astype("int64"), unit="ms", utc=True)
    df["timestamp"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(0, 23) + "Z"

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    if not out["timestamp"].is_monotonic_increasing:
        raise RuntimeError("timestamps not sorted increasing")

    nrows = len(out)
    if not (20_000 <= nrows <= 100_000):
        raise RuntimeError(f"log volume out of target range: {nrows}")

    bad_trace = out["trace_id"].apply(lambda x: (x != "" and (len(x) != 32 or any(c not in "0123456789abcdef" for c in x))))
    if bool(bad_trace.any()):
        raise RuntimeError("trace_id format violation")

    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
