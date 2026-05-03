import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from string import Formatter
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "telemetry_observability_platform"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_gateway",
            "svc": "edge-gw",
            "hosts": ["gw-1", "gw-2"],
            "logs": {
                "access_ingest_202": {
                    "lvl": "INFO",
                    "msg": "ingest accepted status=202 req_id={req_id} dur_ms={dur_ms} client_ip={client_ip} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [5, 400]},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_query_200": {
                    "lvl": "INFO",
                    "msg": "query ok status=200 req_id={req_id} dur_ms={dur_ms} client_ip={client_ip} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 1200]},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_upstream_500": {
                    "lvl": "WARN",
                    "msg": "request failed status=500 route={route} upstream={upstream} req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "route": {"k": "ch", "v": ["ingest", "query"]},
                        "upstream": {"k": "ch", "v": ["shepherd", "retriever"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [50, 15000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_circuit_503": {
                    "lvl": "WARN",
                    "msg": "ingest rejected status=503 circuit_breaker=open req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [1, 40]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "shepherd_ingest",
            "svc": "shepherd",
            "hosts": ["shepherd-1", "shepherd-2", "shepherd-3"],
            "logs": {
                "ingest_accepted": {
                    "lvl": "INFO",
                    "msg": "ingest accepted req_id={req_id} dataset={dataset} events={events} schema_cache={schema_cache} schema_attempts={schema_attempts} schema_lookup_ms={schema_lookup_ms} total_ms={total_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dataset": {"k": "ch", "v": ["payments", "checkout", "auth"]},
                        "events": {"k": "i", "v": [1, 500]},
                        "schema_cache": {"k": "ch", "v": ["hit", "miss"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {
                            "schema_attempts": {"k": "i", "v": [1, 1]},
                            "schema_lookup_ms": {"k": "i", "v": [2, 60]},
                            "total_ms": {"k": "i", "v": [10, 250]},
                        },
                        "f": {
                            "schema_attempts": {"k": "i", "v": [1, 3]},
                            "schema_lookup_ms": {"k": "i", "v": [50, 8000]},
                            "total_ms": {"k": "i", "v": [150, 15000]},
                        },
                    },
                },
                "ingest_failed_db": {
                    "lvl": "ERROR",
                    "msg": "ingest failed req_id={req_id} dataset={dataset} err={err} schema_attempts={schema_attempts} waited_ms={waited_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dataset": {"k": "ch", "v": ["payments", "checkout", "auth"]},
                        "err": {"k": "ch", "v": ["db_conn_exhausted", "lock_wait_timeout", "db_unavailable"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"schema_attempts": {"k": "i", "v": [1, 1]}, "waited_ms": {"k": "i", "v": [0, 50]}},
                        "f": {"schema_attempts": {"k": "i", "v": [1, 3]}, "waited_ms": {"k": "i", "v": [200, 15000]}},
                    },
                },
                "slo_burn": {
                    "lvl": "INFO",
                    "msg": "ingest slo burn_rate_1h={burn_rate_1h} p95_ms={p95_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {"burn_rate_1h": {"k": "f", "v": [0.0, 0.4]}, "p95_ms": {"k": "i", "v": [60, 250]}},
                        "f": {"burn_rate_1h": {"k": "f", "v": [2.0, 25.0]}, "p95_ms": {"k": "i", "v": [500, 15000]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "slo_burn", "per_min": 0.5, "scope": "global"}]},
                "f": {"emit": [{"id": "slo_burn", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "retriever_engine",
            "svc": "retriever",
            "hosts": ["retriever-1", "retriever-2", "retriever-3", "retriever-4"],
            "logs": {
                "query_ok": {
                    "lvl": "INFO",
                    "msg": "query ok req_id={req_id} dataset={dataset} rows={rows} dur_ms={dur_ms} freshness_age_s={freshness_age_s} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dataset": {"k": "ch", "v": ["payments", "checkout", "auth"]},
                        "rows": {"k": "i", "v": [0, 5000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [20, 900]}, "freshness_age_s": {"k": "i", "v": [0, 900]}},
                        "f": {"dur_ms": {"k": "i", "v": [30, 3000]}, "freshness_age_s": {"k": "i", "v": [0, 60]}},
                    },
                },
                "query_fail_db": {
                    "lvl": "ERROR",
                    "msg": "query failed req_id={req_id} dataset={dataset} err={err} db_attempts={db_attempts} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dataset": {"k": "ch", "v": ["payments", "checkout", "auth"]},
                        "err": {"k": "ch", "v": ["db_conn_exhausted", "lock_wait_timeout", "db_unavailable"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"db_attempts": {"k": "i", "v": [1, 1]}, "dur_ms": {"k": "i", "v": [30, 800]}},
                        "f": {"db_attempts": {"k": "i", "v": [1, 3]}, "dur_ms": {"k": "i", "v": [200, 15000]}},
                    },
                },
                "query_fail_unready": {
                    "lvl": "ERROR",
                    "msg": "query failed req_id={req_id} dataset={dataset} err={err} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dataset": {"k": "ch", "v": ["payments", "checkout", "auth"]},
                        "err": {"k": "ch", "v": ["startup_checks_failed", "dependency_check_failed"]},
                        "dur_ms": {"k": "i", "v": [30, 2000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "stamp_writer_metrics_running": {
                    "lvl": "INFO",
                    "msg": "stamp writer running target={target_cluster} writes_ok_1m={writes_ok_1m} writes_err_1m={writes_err_1m} loop_ms={loop_ms}",
                    "vars": {
                        "target_cluster": {"k": "ch", "v": ["cluster_a", "cluster_b"]},
                        "writes_ok_1m": {"k": "i", "v": [60, 220]},
                        "writes_err_1m": {"k": "i", "v": [0, 5]},
                        "loop_ms": {"k": "i", "v": [10, 120]},
                    },
                },
                "stamp_writer_metrics_stopped": {
                    "lvl": "WARN",
                    "msg": "stamp writer stopped target={target_cluster} writes_ok_1m={writes_ok_1m} writes_err_1m={writes_err_1m} loop_ms={loop_ms}",
                    "vars": {
                        "target_cluster": {"k": "ch", "v": ["cluster_a", "cluster_b"]},
                        "writes_ok_1m": {"k": "i", "v": [0, 3]},
                        "writes_err_1m": {"k": "i", "v": [0, 10]},
                        "loop_ms": {"k": "i", "v": [10, 300]},
                    },
                },
                "host_restart": {
                    "lvl": "WARN",
                    "msg": "host restart requested reason={reason} by={by}",
                    "vars": {"reason": {"k": "ch", "v": ["stamp_writer_recovery", "post_failover_checks"]}, "by": {"k": "ch", "v": ["oncall"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "stamp_writer_metrics_running", "per_min": 1.0}]},
                "f": {"emit": [{"id": "stamp_writer_metrics_running", "per_min": 1.0}, {"id": "stamp_writer_metrics_stopped", "per_min": 1.0}]},
            },
        },
        {
            "id": "schema_cache",
            "svc": "schema-cache",
            "hosts": ["cache-1", "cache-2"],
            "logs": {
                "cache_stats_warm": {
                    "lvl": "INFO",
                    "msg": "schema cache stats state=warm hit_ratio={hit_ratio} warm_schemas={warm_schemas} misses_1m={misses_1m} refresh_q={refresh_q}",
                    "vars": {
                        "hit_ratio": {"k": "f", "v": [0.90, 0.99]},
                        "warm_schemas": {"k": "i", "v": [5000, 12000]},
                        "misses_1m": {"k": "i", "v": [0, 250]},
                        "refresh_q": {"k": "i", "v": [0, 60]},
                    },
                },
                "cache_stats_cold": {
                    "lvl": "INFO",
                    "msg": "schema cache stats state=cold hit_ratio={hit_ratio} warm_schemas={warm_schemas} misses_1m={misses_1m} refresh_q={refresh_q}",
                    "vars": {
                        "hit_ratio": {"k": "f", "v": [0.05, 0.40]},
                        "warm_schemas": {"k": "i", "v": [300, 3000]},
                        "misses_1m": {"k": "i", "v": [800, 9000]},
                        "refresh_q": {"k": "i", "v": [200, 4000]},
                    },
                },
                "refresh_error": {
                    "lvl": "WARN",
                    "msg": "schema cache refresh error err={err} retry_in_ms={retry_in_ms}",
                    "vars": {"err": {"k": "ch", "v": ["db_timeout", "db_unavailable"]}, "retry_in_ms": {"k": "i", "v": [200, 5000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cache_stats_warm", "per_min": 1.0, "scope": "global"}, {"id": "refresh_error", "per_min": 0.02, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "cache_stats_warm", "per_min": 1.0, "scope": "global"},
                        {"id": "cache_stats_cold", "per_min": 1.0, "scope": "global"},
                        {"id": "refresh_error", "per_min": 0.2, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "mysql_main_db",
            "svc": "mysql-main",
            "hosts": ["mysql-primary-1", "mysql-primary-2"],
            "logs": {
                "db_health_ok": {
                    "lvl": "INFO",
                    "msg": "db health state=ok conns_in_use={conns_in_use} threads_running={threads_running} lock_waits_1m={lock_waits_1m} cpu_pct={cpu_pct}",
                    "vars": {
                        "conns_in_use": {"k": "i", "v": [50, 250]},
                        "threads_running": {"k": "i", "v": [5, 60]},
                        "lock_waits_1m": {"k": "i", "v": [0, 5]},
                        "cpu_pct": {"k": "i", "v": [5, 70]},
                    },
                },
                "db_health_overloaded": {
                    "lvl": "INFO",
                    "msg": "db health state=overloaded conns_in_use={conns_in_use} threads_running={threads_running} lock_waits_1m={lock_waits_1m} cpu_pct={cpu_pct}",
                    "vars": {
                        "conns_in_use": {"k": "i", "v": [700, 1000]},
                        "threads_running": {"k": "i", "v": [50, 400]},
                        "lock_waits_1m": {"k": "i", "v": [10, 600]},
                        "cpu_pct": {"k": "i", "v": [60, 100]},
                    },
                },
                "innodb_lock_wait": {
                    "lvl": "WARN",
                    "msg": "innodb lock wait wait_ms={wait_ms} trx_id={trx_id}",
                    "vars": {"wait_ms": {"k": "i", "v": [50, 20000]}, "trx_id": {"k": "hex", "v": 16}},
                },
                "too_many_connections": {
                    "lvl": "ERROR",
                    "msg": "too many connections conns_in_use={conns_in_use} max_connections={max_connections}",
                    "vars": {"conns_in_use": {"k": "i", "v": [995, 1000]}, "max_connections": {"k": "i", "v": [1000, 1000]}},
                },
                "innodb_internal_deadlock": {
                    "lvl": "CRITICAL",
                    "msg": "innodb internal deadlock mutex={mutex} victim_thread={victim_thread}",
                    "vars": {"mutex": {"k": "str", "v": "innodb_mutex_name"}, "victim_thread": {"k": "i", "v": [1, 512]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "db_health_ok", "per_min": 1.0, "scope": "global"}, {"id": "innodb_lock_wait", "per_min": 0.02, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "db_health_ok", "per_min": 1.0, "scope": "global"},
                        {"id": "db_health_overloaded", "per_min": 1.0, "scope": "global"},
                        {"id": "innodb_lock_wait", "per_min": 0.2, "scope": "global"},
                        {"id": "too_many_connections", "per_min": 0.15, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "ops_console",
            "svc": None,
            "hosts": ["oncall-1"],
            "logs": {
                "enable_ingest_circuit_breaker": {
                    "lvl": "WARN",
                    "msg": "set ingest circuit breaker state={state} reason={reason} by={by}",
                    "vars": {"state": {"k": "ch", "v": ["open"]}, "reason": {"k": "ch", "v": ["protect_mysql"]}, "by": {"k": "ch", "v": ["oncall"]}},
                },
                "disable_ingest_circuit_breaker": {
                    "lvl": "INFO",
                    "msg": "set ingest circuit breaker state={state} reason={reason} by={by}",
                    "vars": {"state": {"k": "ch", "v": ["closed"]}, "reason": {"k": "ch", "v": ["restore_ingest"]}, "by": {"k": "ch", "v": ["oncall"]}},
                },
                "initiate_db_failover": {
                    "lvl": "CRITICAL",
                    "msg": "initiate db failover target={target} by={by}",
                    "vars": {"target": {"k": "ch", "v": ["replica_promote"]}, "by": {"k": "ch", "v": ["oncall"]}},
                },
                "db_failover_complete": {"lvl": "INFO", "msg": "db failover complete new_primary={new_primary}", "vars": {"new_primary": {"k": "ch", "v": ["mysql-primary-2"]}}},
                "bulk_schema_stamp_update": {
                    "lvl": "WARN",
                    "msg": "bulk schema stamp update scope={scope} set_to={set_to} affected_schemas={affected_schemas} by={by}",
                    "vars": {
                        "scope": {"k": "ch", "v": ["active_last_24h"]},
                        "set_to": {"k": "ch", "v": ["now"]},
                        "affected_schemas": {"k": "i", "v": [1000, 20000]},
                        "by": {"k": "ch", "v": ["oncall"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "ingest_event",
                    "rpm": 180.0,
                    "emit": ["shepherd_ingest.ingest_accepted", "api_gateway.access_ingest_202"],
                    "latency_ms": [[12, 220], [15, 260]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "query_request",
                    "rpm": 60.0,
                    "emit": ["retriever_engine.query_ok", "api_gateway.access_query_200"],
                    "latency_ms": [[25, 750], [30, 900]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "ingest_event_fail_500",
                    "rpm": 180.0,
                    "emit": ["shepherd_ingest.ingest_failed_db", "api_gateway.access_upstream_500"],
                    "latency_ms": [[220, 14500], [260, 15000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ingest_event_blocked_503",
                    "rpm": 180.0,
                    "emit": ["api_gateway.access_circuit_503"],
                    "latency_ms": [[2, 15]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ingest_event_recovered_202",
                    "rpm": 180.0,
                    "emit": ["shepherd_ingest.ingest_accepted", "api_gateway.access_ingest_202"],
                    "latency_ms": [[25, 500], [30, 650]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "query_request_fail_500",
                    "rpm": 60.0,
                    "emit": ["retriever_engine.query_fail_db", "api_gateway.access_upstream_500"],
                    "latency_ms": [[220, 14500], [260, 15000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "query_request_fail_500_unready",
                    "rpm": 60.0,
                    "emit": ["retriever_engine.query_fail_unready", "api_gateway.access_upstream_500"],
                    "latency_ms": [[45, 900], [55, 1400]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "query_request_recovered_200",
                    "rpm": 60.0,
                    "emit": ["retriever_engine.query_ok", "api_gateway.access_query_200"],
                    "latency_ms": [[40, 1050], [45, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "honeycomb_total_outage_stamp_cache_mysql_lockup_recovery_with_freshness_skew",
        "time": {"total_minutes": 56, "phases": {"n": {"start_min": 0, "end_min": 28}, "f": {"start_min": 28, "end_min": 56}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 28,
                        "rate_multipliers": {
                            "ingest_event_fail_500": 1.0,
                            "ingest_event_blocked_503": 0.0,
                            "ingest_event_recovered_202": 0.0,
                            "query_request_fail_500": 1.0,
                            "query_request_fail_500_unready": 0.0,
                            "query_request_recovered_200": 0.0,
                            "retriever_engine.stamp_writer_metrics_running": 0.0,
                            "retriever_engine.stamp_writer_metrics_stopped": 1.0,
                            "schema_cache.cache_stats_warm": 0.0,
                            "schema_cache.cache_stats_cold": 1.0,
                            "mysql_main_db.db_health_ok": 0.0,
                            "mysql_main_db.db_health_overloaded": 1.0,
                            "mysql_main_db.too_many_connections": 6.0,
                            "mysql_main_db.innodb_lock_wait": 6.0,
                        },
                        "latency_multipliers": {
                            "ingest_event_fail_500": {"p50": 1.0, "p95": 1.0},
                            "query_request_fail_500": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [{"ref": "mysql_main_db.innodb_internal_deadlock", "count": 1, "hosts": ["mysql-primary-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 33,
                        "rate_multipliers": {
                            "ingest_event_fail_500": 0.0,
                            "ingest_event_blocked_503": 1.0,
                            "ingest_event_recovered_202": 0.0,
                            "mysql_main_db.too_many_connections": 3.0,
                            "mysql_main_db.innodb_lock_wait": 4.0,
                        },
                        "latency_multipliers": {
                            "ingest_event_blocked_503": {"p50": 1.0, "p95": 1.0},
                            "query_request_fail_500": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [{"ref": "ops_console.enable_ingest_circuit_breaker", "count": 1, "hosts": ["oncall-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "rate_multipliers": {
                            "query_request_fail_500": 0.0,
                            "query_request_fail_500_unready": 1.0,
                            "mysql_main_db.db_health_ok": 1.0,
                            "mysql_main_db.db_health_overloaded": 0.0,
                            "mysql_main_db.too_many_connections": 0.2,
                            "mysql_main_db.innodb_lock_wait": 0.4,
                        },
                        "latency_multipliers": {"query_request_fail_500_unready": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "ops_console.initiate_db_failover", "count": 1, "hosts": ["oncall-1"]},
                            {"ref": "ops_console.db_failover_complete", "count": 1, "hosts": ["oncall-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 45,
                        "rate_multipliers": {
                            "schema_cache.cache_stats_warm": 1.0,
                            "schema_cache.cache_stats_cold": 0.0,
                            "retriever_engine.stamp_writer_metrics_running": 1.0,
                            "retriever_engine.stamp_writer_metrics_stopped": 0.0,
                        },
                        "latency_multipliers": {"query_request_fail_500_unready": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "ops_console.bulk_schema_stamp_update", "count": 1, "hosts": ["oncall-1"]},
                            {"ref": "retriever_engine.host_restart", "count": 4, "hosts": ["retriever-1", "retriever-2", "retriever-3", "retriever-4"]},
                        ],
                    },
                    {
                        "order": 5,
                        "at_min": 50,
                        "rate_multipliers": {
                            "ingest_event_blocked_503": 0.0,
                            "ingest_event_recovered_202": 1.0,
                            "ingest_event_fail_500": 0.0,
                            "query_request_fail_500_unready": 0.0,
                            "query_request_recovered_200": 1.0,
                        },
                        "latency_multipliers": {
                            "ingest_event_recovered_202": {"p50": 1.0, "p95": 1.0},
                            "query_request_recovered_200": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [
                            {"ref": "ops_console.disable_ingest_circuit_breaker", "count": 1, "hosts": ["oncall-1"]},
                            {"ref": "retriever_engine.host_restart", "count": 2, "hosts": ["retriever-2", "retriever-3"]},
                        ],
                    },
                ]
            }
        },
    }
}


# ------------------------- Helpers -------------------------


def iso8601_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def stable_hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:8], 16)


def parse_cidr(cidr: str) -> Tuple[int, int]:
    ip, prefix = cidr.split("/")
    prefix = int(prefix)
    parts = [int(p) for p in ip.split(".")]
    base = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]
    size = 1 << (32 - prefix)
    mask = (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF
    base &= mask
    return base, size


def ip_from_int(x: int) -> str:
    return ".".join(str((x >> shift) & 255) for shift in (24, 16, 8, 0))


def fields_in_msg(msg: str) -> List[str]:
    out = []
    for _, field_name, _, _ in Formatter().parse(msg):
        if field_name:
            out.append(field_name)
    return out


@dataclass
class Template:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]
    fields: List[str]


class IdGen:
    def __init__(self) -> None:
        self.req_counter = 0
        self.trace_counter = 0
        self.hex_counter = 0

    def req_id(self) -> str:
        self.req_counter += 1
        h = hashlib.md5(f"req-{self.req_counter}".encode()).hexdigest()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

    def trace_id(self) -> str:
        self.trace_counter += 1
        return hashlib.md5(f"trace-{self.trace_counter}".encode()).hexdigest()

    def hex(self, n: int, salt: str = "") -> str:
        self.hex_counter += 1
        h = hashlib.md5(f"hex-{salt}-{self.hex_counter}".encode()).hexdigest()
        if n <= 32:
            return h[:n]
        pieces = [h]
        while len("".join(pieces)) < n:
            pieces.append(hashlib.md5(("".join(pieces)).encode()).hexdigest())
        return ("".join(pieces))[:n]


class ResidualAllocator:
    def __init__(self) -> None:
        self.res: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        x = expected + self.res.get(key, 0.0)
        if x < 0:
            x = 0.0
        c = int(math.floor(x + 1e-12))
        self.res[key] = x - c
        return c


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    if p50 <= 0:
        return 0.0
    if p95 <= p50:
        return p50

    z95 = 1.6448536269514722
    sigma = math.log(p95 / p50) / z95
    mu = math.log(p50)

    def inv_norm(p: float) -> float:
        p = min(max(p, 1e-12), 1.0 - 1e-12)
        a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
        b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
        c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
        d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
        plow = 0.02425
        phigh = 1 - plow
        if p < plow:
            q = math.sqrt(-2 * math.log(p))
            return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
            )
        if p > phigh:
            q = math.sqrt(-2 * math.log(1 - p))
            return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
            )
        q = p - 0.5
        r = q * q
        return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
            ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1
        )

    z = inv_norm(u)
    return math.exp(mu + sigma * z)


def schedule_evenly(start: datetime, end: datetime, n: int, rng: np.random.Generator, jitter_frac: float = 0.2) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start] * n
    step = total_s / n
    jitter = step * jitter_frac
    out = []
    for i in range(n):
        t = (i + 0.5) * step
        j = (rng.random() * 2 - 1) * jitter
        tt = start + timedelta(seconds=clamp(t + j, 0.0, total_s - 1e-6))
        out.append(tt)
    return out


# ------------------------- Build indices -------------------------


COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

TEMPLATES: Dict[str, Template] = {}
for c in SYSTEM["components"]:
    for log_id, t in c.get("logs", {}).items():
        key = f"{c['id']}.{log_id}"
        TEMPLATES[key] = Template(
            component_id=c["id"],
            log_id=log_id,
            lvl=t["lvl"],
            msg=t["msg"],
            vars=t.get("vars", {}),
            state_vars=t.get("state_vars", {}),
            fields=fields_in_msg(t["msg"]),
        )

FLOWS: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for state in ("n", "f"):
    for fd in SYSTEM["flows"][state]["req"]:
        FLOWS[state][fd["id"]] = fd

FAIL_EVENTS = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

N_START = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
N_END = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
F_START = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
F_END = SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"]

BASE_TIME = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

active_rate: Dict[str, float] = {}
active_lat: Dict[str, Dict[str, float]] = {}
events_by_min: Dict[int, List[Dict[str, Any]]] = {}
for ev in FAIL_EVENTS:
    events_by_min.setdefault(ev["at_min"], []).append(ev)

rate_by_min: Dict[int, Dict[str, float]] = {}
lat_by_min: Dict[int, Dict[str, Dict[str, float]]] = {}
one_shots: List[Dict[str, Any]] = []

for m in range(F_START, F_END):
    for ev in events_by_min.get(m, []):
        for k, v in ev.get("rate_multipliers", {}).items():
            active_rate[k] = float(v)
        for k, vv in ev.get("latency_multipliers", {}).items():
            active_lat[k] = {"p50": float(vv.get("p50", 1.0)), "p95": float(vv.get("p95", 1.0))}
        for ospec in ev.get("one_shots", []):
            one_shots.append({"at_min": ev["at_min"], **ospec})
    rate_by_min[m] = dict(active_rate)
    lat_by_min[m] = dict(active_lat)


# ------------------------- Variable generation/rendering -------------------------


def get_domain(tpl: Template, state: str, var: str) -> Optional[Dict[str, Any]]:
    if tpl.state_vars and state in tpl.state_vars and var in tpl.state_vars[state]:
        return tpl.state_vars[state][var]
    if var in tpl.vars:
        return tpl.vars[var]
    return None


def get_i_range(tpl: Template, state: str, var: str) -> Optional[Tuple[int, int]]:
    dom = get_domain(tpl, state, var)
    if dom and dom.get("k") == "i":
        lo, hi = int(dom["v"][0]), int(dom["v"][1])
        return lo, hi
    return None


def gen_value(dom: Dict[str, Any], rng: np.random.Generator, ids: IdGen, salt: str = "") -> Any:
    k = dom["k"]
    v = dom.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        x = float(lo + (hi - lo) * rng.random())
        return f"{x:.3f}"
    if k == "ch":
        choices = list(v)
        idx = int(rng.integers(0, len(choices)))
        return choices[idx]
    if k == "uuid":
        return ids.req_id()
    if k == "hex":
        n = int(v)
        return ids.hex(n, salt=salt)
    if k == "ip":
        base, size = parse_cidr(str(v))
        off = int(rng.integers(1, max(2, size)))
        return ip_from_int(base + (off % size))
    if k == "str":
        return str(v)
    return str(v)


def render_log(tpl: Template, state: str, rng: np.random.Generator, ids: IdGen, bound: Dict[str, Any]) -> str:
    vals: Dict[str, Any] = {}
    for f in tpl.fields:
        if f in bound:
            vals[f] = bound[f]
            continue
        dom = get_domain(tpl, state, f)
        if dom is None:
            vals[f] = ""
        else:
            vals[f] = gen_value(dom, rng, ids, salt=f"{tpl.component_id}.{tpl.log_id}.{f}")
    try:
        return tpl.msg.format(**vals)
    except Exception:
        return tpl.msg


def pick_host(component_id: str, rng: np.random.Generator, prefer_idx: int = 0) -> str:
    hosts = COMP[component_id].get("hosts") or []
    if not hosts:
        return ""
    idx = (prefer_idx + int(rng.integers(0, 1_000_000))) % len(hosts)
    return hosts[idx]


def component_service(component_id: str) -> str:
    svc = COMP[component_id].get("svc")
    return "" if svc is None else str(svc)


def elapsed_primary_field(tpl: Template) -> Optional[str]:
    # Choose the primary observed elapsed timing field in this message (if any).
    if "total_ms" in tpl.fields:
        return "total_ms"
    if "waited_ms" in tpl.fields:
        return "waited_ms"
    if "dur_ms" in tpl.fields:
        return "dur_ms"
    return None


def adjust_delays_for_timing_domains(flow_state: str, emit_refs: List[str], delays_ms: List[float]) -> List[int]:
    """
    Fix verifier S5: ensure message-carried observed timing fields are in-domain AND timestamp-coherent.
    For this model, flows emit 1-2 logs; we interpret:
      - first log's primary timing field == elapsed to first log (delay[0])
      - last log's primary timing field == total elapsed to last log (sum(delay))
    We adjust delays (not just message values) so the timeline and logged fields agree.
    """
    n = len(delays_ms)
    if n <= 0:
        return []

    # Round to integer ms for exact coherence between timestamps and emitted *_ms values.
    d = [max(1, int(round(x))) for x in delays_ms]

    if n == 1:
        tpl0 = TEMPLATES[emit_refs[0]]
        f0 = elapsed_primary_field(tpl0)
        if f0:
            dom0 = get_i_range(tpl0, flow_state, f0)
            if dom0:
                d[0] = int(clamp(d[0], dom0[0], dom0[1]))
        return d

    if n == 2:
        tpl0 = TEMPLATES[emit_refs[0]]
        tpl1 = TEMPLATES[emit_refs[1]]

        f0 = elapsed_primary_field(tpl0)
        dom0 = get_i_range(tpl0, flow_state, f0) if f0 else None

        f1 = elapsed_primary_field(tpl1)
        dom1 = get_i_range(tpl1, flow_state, f1) if f1 else None

        d0, d1 = d[0], d[1]
        min_d1 = 1

        if dom0:
            d0 = int(clamp(d0, dom0[0], dom0[1]))

        if dom1:
            loT, hiT = dom1

            # Ensure first segment doesn't make it impossible to keep total within hiT.
            max_d0_allowed = hiT - min_d1
            if dom0:
                if max_d0_allowed < dom0[0]:
                    # Should not occur with provided YAML, but keep deterministic behavior.
                    d0 = max(1, max_d0_allowed)
                else:
                    d0 = min(d0, max_d0_allowed)
            else:
                d0 = min(d0, max_d0_allowed)

            d1_min = max(min_d1, loT - d0)
            d1_max = max(min_d1, hiT - d0)
            if d1_max < d1_min:
                d1 = d1_max
            else:
                d1 = int(clamp(d1, d1_min, d1_max))
        else:
            d1 = max(min_d1, d1)

        return [d0, d1]

    # Not expected for this incident model; keep as rounded nonzero ms.
    return d


# ------------------------- Simulation -------------------------


SEED = 1337
random.seed(SEED)
rng = np.random.default_rng(SEED)
ids = IdGen()
alloc = ResidualAllocator()

rows: List[Tuple[datetime, str, str, str, str, str]] = []


def add_row(dt: datetime, tpl: Template, msg: str, trace_id: str, host: str) -> None:
    rows.append((dt, tpl.lvl, msg, trace_id, component_service(tpl.component_id), host))


def simulate_background_minute(state: str, minute: int) -> None:
    interval_start = BASE_TIME + timedelta(minutes=minute)
    interval_end = interval_start + timedelta(minutes=1)

    active = rate_by_min.get(minute, {}) if state == "f" else {}

    for comp in SYSTEM["components"]:
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", [])
        for emit_spec in emits:
            log_id = emit_spec["id"]
            per_min = float(emit_spec["per_min"])
            scope = emit_spec.get("scope", "per_host")
            ref = f"{comp['id']}.{log_id}"
            tpl = TEMPLATES[ref]

            mult = 1.0
            if state == "f":
                mult = float(active.get(ref, 1.0))
            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                key = f"bg|{state}|{ref}"
                n = alloc.alloc(key, eff)
                if n <= 0:
                    continue
                times = schedule_evenly(interval_start, interval_end, n, rng, jitter_frac=0.3)
                hosts = comp.get("hosts") or []
                if hosts:
                    idx = (minute + stable_hash_int(ref)) % len(hosts)
                    host = hosts[idx]
                else:
                    host = ""
                for t in times:
                    msg = render_log(tpl, state, rng, ids, bound={})
                    add_row(t, tpl, msg, "", host)
            else:
                hosts = comp.get("hosts") or []
                if not hosts:
                    key = f"bg|{state}|{ref}|nohost"
                    n = alloc.alloc(key, eff)
                    times = schedule_evenly(interval_start, interval_end, n, rng, jitter_frac=0.3)
                    for t in times:
                        msg = render_log(tpl, state, rng, ids, bound={})
                        add_row(t, tpl, msg, "", "")
                else:
                    for host in hosts:
                        key = f"bg|{state}|{ref}|{host}"
                        n = alloc.alloc(key, eff)
                        if n <= 0:
                            continue
                        times = schedule_evenly(interval_start, interval_end, n, rng, jitter_frac=0.3)
                        for t in times:
                            msg = render_log(tpl, state, rng, ids, bound={})
                            add_row(t, tpl, msg, "", host)


def simulate_flow_instance(flow_state: str, flow_id: str, start: datetime, lat_mult: Optional[Dict[str, float]] = None, instance_idx: int = 0) -> None:
    fd = FLOWS[flow_state][flow_id]
    emit_refs = fd["emit"]
    lat_pairs = fd["latency_ms"]

    trace_id = ids.trace_id() if (SYSTEM["tracing"]["on"] and fd.get("trace", False)) else ""
    req_id = ids.req_id()
    dataset = ["payments", "checkout", "auth"][instance_idx % 3]

    lm = lat_mult or {"p50": 1.0, "p95": 1.0}
    raw_delays: List[float] = []
    for (p50, p95) in lat_pairs:
        p50s = float(p50) * float(lm.get("p50", 1.0))
        p95s = float(p95) * float(lm.get("p95", 1.0))
        u = float(rng.random())
        d = lognormal_from_p50_p95(p50s, p95s, u)
        # soft cap around 3x p95 (as before); final domain coherence is enforced below.
        d = clamp(d, 0.5, 3.0 * p95s)
        raw_delays.append(d)

    delays_ms = adjust_delays_for_timing_domains(flow_state, emit_refs, raw_delays)

    comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        comp_id = ref.split(".", 1)[0]
        if comp_id not in comp_host:
            comp_host[comp_id] = pick_host(comp_id, rng, prefer_idx=instance_idx)

    client_ip = gen_value({"k": "ip", "v": "203.0.113.0/24"}, rng, ids, salt=f"client_ip|{req_id}")

    base_bound = {"req_id": req_id, "trace_id": trace_id, "dataset": dataset}

    if flow_id.startswith("ingest_"):
        base_bound["route"] = "ingest"
        base_bound["upstream"] = "shepherd"
    elif flow_id.startswith("query_"):
        base_bound["route"] = "query"
        base_bound["upstream"] = "retriever"

    t = start
    total_ms = int(sum(delays_ms))

    for idx, ref in enumerate(emit_refs):
        tpl = TEMPLATES[ref]
        comp_id = tpl.component_id

        t = t + timedelta(milliseconds=float(delays_ms[idx]))
        bound = dict(base_bound)

        if ref == "api_gateway.access_ingest_202":
            bound["dur_ms"] = total_ms
            bound["client_ip"] = client_ip
        elif ref == "api_gateway.access_query_200":
            bound["dur_ms"] = total_ms
            bound["client_ip"] = client_ip
        elif ref == "api_gateway.access_upstream_500":
            bound["dur_ms"] = total_ms
        elif ref == "api_gateway.access_circuit_503":
            bound["dur_ms"] = total_ms

        elif ref == "shepherd_ingest.ingest_accepted":
            first_ms = int(delays_ms[idx])
            bound["total_ms"] = first_ms

            # Keep schema_lookup_ms within its state-specific domain and <= total_ms.
            dom = get_i_range(tpl, flow_state, "schema_lookup_ms")
            if dom:
                lo, hi = dom
                hi_eff = min(hi, max(lo, first_ms))
                approx = int(round(first_ms * 0.6))
                bound["schema_lookup_ms"] = int(clamp(approx, lo, hi_eff))

            if flow_state == "n":
                bound["schema_cache"] = "hit" if rng.random() < 0.90 else "miss"
            else:
                if flow_id == "ingest_event_fail_500":
                    bound["schema_cache"] = "miss"
                else:
                    bound["schema_cache"] = "hit" if rng.random() < 0.80 else "miss"

        elif ref == "shepherd_ingest.ingest_failed_db":
            first_ms = int(delays_ms[idx])
            bound["waited_ms"] = first_ms
            bound["schema_cache"] = "miss"

        elif ref == "retriever_engine.query_ok":
            first_ms = int(delays_ms[idx])
            bound["dur_ms"] = first_ms

        elif ref == "retriever_engine.query_fail_db":
            first_ms = int(delays_ms[idx])
            bound["dur_ms"] = first_ms

        elif ref == "retriever_engine.query_fail_unready":
            first_ms = int(delays_ms[idx])
            bound["dur_ms"] = first_ms

        msg = render_log(tpl, flow_state, rng, ids, bound=bound)
        add_row(t, tpl, msg, trace_id, comp_host.get(comp_id, ""))


def simulate_flows_minute(state: str, minute: int) -> None:
    interval_start = BASE_TIME + timedelta(minutes=minute)
    interval_end = interval_start + timedelta(minutes=1)

    if state == "n":
        for flow_id, fd in FLOWS["n"].items():
            expected = float(fd["rpm"])
            n_inst = alloc.alloc(f"flow|n|{flow_id}", expected)
            if n_inst <= 0:
                continue
            starts = schedule_evenly(interval_start, interval_end, n_inst, rng, jitter_frac=0.15)
            for i, st in enumerate(starts):
                simulate_flow_instance("n", flow_id, st, lat_mult={"p50": 1.0, "p95": 1.0}, instance_idx=(minute * 1000 + i))
    else:
        active = rate_by_min.get(minute, {})
        lat_active = lat_by_min.get(minute, {})
        for flow_id, fd in FLOWS["f"].items():
            mult = float(active.get(flow_id, 1.0))
            expected = float(fd["rpm"]) * mult
            n_inst = alloc.alloc(f"flow|f|{flow_id}", expected)
            if n_inst <= 0:
                continue
            starts = schedule_evenly(interval_start, interval_end, n_inst, rng, jitter_frac=0.15)
            lm = lat_active.get(flow_id, {"p50": 1.0, "p95": 1.0})
            for i, st in enumerate(starts):
                simulate_flow_instance("f", flow_id, st, lat_mult=lm, instance_idx=(minute * 1000 + i))


def emit_one_shots() -> None:
    for ospec in one_shots:
        at_min = int(ospec["at_min"])
        ref = str(ospec["ref"])
        count = int(ospec["count"])
        hosts = list(ospec.get("hosts") or [])
        tpl = TEMPLATES[ref]
        comp_id = tpl.component_id
        event_time = BASE_TIME + timedelta(minutes=at_min)
        times = schedule_evenly(event_time, event_time + timedelta(seconds=1), count, rng, jitter_frac=0.45)
        for i, t in enumerate(times):
            if hosts:
                host = hosts[i % len(hosts)]
            else:
                ch = COMP[comp_id].get("hosts") or []
                host = ch[i % len(ch)] if ch else ""
            bound: Dict[str, Any] = {}
            if ref == "retriever_engine.host_restart":
                bound["by"] = "oncall"
                bound["reason"] = "stamp_writer_recovery" if at_min == 45 else "post_failover_checks"
            msg = render_log(tpl, "f", rng, ids, bound=bound)
            add_row(t, tpl, msg, "", host)


# ------------------------- Run simulation -------------------------

for m in range(N_START, N_END):
    simulate_background_minute("n", m)
    simulate_flows_minute("n", m)

for m in range(F_START, F_END):
    simulate_background_minute("f", m)
    simulate_flows_minute("f", m)

emit_one_shots()

# ------------------------- Write logs.csv -------------------------

rows.sort(key=lambda r: r[0])
df = pd.DataFrame(
    {
        "timestamp": [iso8601_ms(r[0]) for r in rows],
        "level": [r[1] for r in rows],
        "message": [r[2] for r in rows],
        "trace_id": [r[3] for r in rows],
        "service": [r[4] for r in rows],
        "host": [r[5] for r in rows],
    }
)
df.to_csv("logs.csv", index=False)
