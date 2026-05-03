import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import string


SYSTEM: Dict[str, Any] = {
    "id": "gitlab_com_db_data_loss_incident",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["rails_web"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "rails_web": {
            "svc": "gitlab-rails",
            "hosts": ["web01", "web02"],
            "logs": {
                "http_access": {
                    "lvl": "INFO",
                    "msg": "request {method} {route} status={status} duration_ms={duration_ms} db_ms={db_ms} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/users/sign_in", "/api/v4/snippets", "/api/v4/projects"]},
                        "status": {"k": "ch", "v": ["200", "201", "302"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"duration_ms": {"k": "i", "v": [20, 500]}, "db_ms": {"k": "i", "v": [5, 180]}},
                        "f": {"duration_ms": {"k": "i", "v": [40, 4000]}, "db_ms": {"k": "i", "v": [10, 2500]}},
                    },
                },
                "http_500": {
                    "lvl": "ERROR",
                    "msg": "request {method} {route} status=500 duration_ms={duration_ms} error={error} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/api/v4/snippets", "/users/sign_in"]},
                        "error": {"k": "ch", "v": ["ActiveRecord::LockWaitTimeout", "PG::LockNotAvailable", "PG::QueryCanceled"]},
                        "duration_ms": {"k": "i", "v": [200, 6000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "http_503": {
                    "lvl": "ERROR",
                    "msg": "request {method} {route} status=503 duration_ms={duration_ms} error=DBUnavailable trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/users/sign_in", "/api/v4/snippets", "/api/v4/projects"]},
                        "duration_ms": {"k": "i", "v": [10, 800]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "http_403_blocked": {
                    "lvl": "WARN",
                    "msg": "request {method} {route} status=403 ip={ip} rule_id={rule_id} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/api/v4/snippets", "/users/sign_in"]},
                        "ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "rule_id": {"k": "ch", "v": ["abuse_snippet_spam", "abuse_signin_flood"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "db_pool_saturated": {
                    "lvl": "WARN",
                    "msg": "db pool saturated active={active} waiting={waiting} timeout_ms={timeout_ms}",
                    "vars": {
                        "active": {"k": "i", "v": [50, 400]},
                        "waiting": {"k": "i", "v": [0, 300]},
                        "timeout_ms": {"k": "i", "v": [100, 5000]},
                    },
                    "state_vars": {},
                },
                "app_heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat rss_mb={rss_mb} threads_busy={threads_busy}",
                    "vars": {"rss_mb": {"k": "i", "v": [600, 2400]}, "threads_busy": {"k": "i", "v": [5, 64]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "app_heartbeat", "per_min": 0.5, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "app_heartbeat", "per_min": 0.5, "scope": "per_host"},
                        {"id": "db_pool_saturated", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "pg_primary_db1": {
            "svc": "postgresql",
            "hosts": ["db1"],
            "logs": {
                "sql_query_ok": {
                    "lvl": "INFO",
                    "msg": "sql ok user={db_user} app=rails query_ms={query_ms} rows={rows} trace_id={trace_id}",
                    "vars": {
                        "db_user": {"k": "ch", "v": ["gitlab"]},
                        "rows": {"k": "i", "v": [0, 200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {"n": {"query_ms": {"k": "i", "v": [2, 80]}}, "f": {"query_ms": {"k": "i", "v": [5, 800]}}},
                },
                "sql_tx_commit": {
                    "lvl": "INFO",
                    "msg": "tx commit user={db_user} app=rails query_ms={query_ms} lock_wait_ms={lock_wait_ms} rows={rows} trace_id={trace_id}",
                    "vars": {
                        "db_user": {"k": "ch", "v": ["gitlab"]},
                        "rows": {"k": "i", "v": [1, 50]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"query_ms": {"k": "i", "v": [5, 120]}, "lock_wait_ms": {"k": "i", "v": [0, 30]}},
                        "f": {"query_ms": {"k": "i", "v": [20, 1500]}, "lock_wait_ms": {"k": "i", "v": [0, 4000]}},
                    },
                },
                "sql_lock_timeout": {
                    "lvl": "ERROR",
                    "msg": "canceling statement due to lock timeout pid={pid} relation={relation} waited_ms={waited_ms} trace_id={trace_id}",
                    "vars": {
                        "pid": {"k": "i", "v": [1000, 65000]},
                        "relation": {"k": "ch", "v": ["snippets", "users", "sessions", "projects"]},
                        "waited_ms": {"k": "i", "v": [200, 5000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "wal_sender_limit": {
                    "lvl": "ERROR",
                    "msg": "WAL sender connection rejected max_wal_senders={max_wal_senders} current_senders={current_senders} client={client}",
                    "vars": {
                        "max_wal_senders": {"k": "i", "v": [4, 32]},
                        "current_senders": {"k": "i", "v": [0, 32]},
                        "client": {"k": "ch", "v": ["db2"]},
                    },
                    "state_vars": {},
                },
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete buffers={buffers} write_ms={write_ms}",
                    "vars": {"buffers": {"k": "i", "v": [5000, 80000]}, "write_ms": {"k": "i", "v": [50, 3000]}},
                    "state_vars": {},
                },
                "postgres_startup_failed_semaphores": {
                    "lvl": "CRITICAL",
                    "msg": "startup failed: semaphore allocation error max_connections={max_connections} max_wal_senders={max_wal_senders}",
                    "vars": {"max_connections": {"k": "i", "v": [2000, 8000]}, "max_wal_senders": {"k": "i", "v": [4, 32]}},
                    "state_vars": {},
                },
                "postgres_started": {
                    "lvl": "INFO",
                    "msg": "postmaster started port=5432 max_connections={max_connections} max_wal_senders={max_wal_senders}",
                    "vars": {"max_connections": {"k": "i", "v": [2000, 8000]}, "max_wal_senders": {"k": "i", "v": [4, 32]}},
                    "state_vars": {},
                },
                "data_dir_missing": {
                    "lvl": "CRITICAL",
                    "msg": "data directory {data_dir} is missing or empty",
                    "vars": {"data_dir": {"k": "ch", "v": ["/var/opt/gitlab/postgresql/data"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.2, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "checkpoint_complete", "per_min": 0.2, "scope": "per_host"},
                        {"id": "wal_sender_limit", "per_min": 0.5, "scope": "per_host"},
                    ]
                },
            },
        },
        "pg_replica_db2": {
            "svc": "postgresql",
            "hosts": ["db2"],
            "logs": {
                "basebackup_start": {
                    "lvl": "INFO",
                    "msg": "pg_basebackup started primary={primary} target_dir={target_dir}",
                    "vars": {"primary": {"k": "ch", "v": ["db1"]}, "target_dir": {"k": "ch", "v": ["/var/opt/gitlab/postgresql/data"]}},
                    "state_vars": {},
                },
                "basebackup_waiting": {
                    "lvl": "WARN",
                    "msg": "pg_basebackup waiting for WAL start seconds_waiting={seconds_waiting} primary={primary}",
                    "vars": {"seconds_waiting": {"k": "i", "v": [10, 600]}, "primary": {"k": "ch", "v": ["db1"]}},
                    "state_vars": {},
                },
                "primary_conn_failed": {
                    "lvl": "ERROR",
                    "msg": "could not connect to primary {primary} reason={reason}",
                    "vars": {"primary": {"k": "ch", "v": ["db1"]}, "reason": {"k": "ch", "v": ["connection_refused", "timeout", "no_route"]}},
                    "state_vars": {},
                },
                "standby_heartbeat": {
                    "lvl": "INFO",
                    "msg": "standby heartbeat receiver_status={status}",
                    "vars": {"status": {"k": "ch", "v": ["streaming", "catching_up", "disconnected"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "standby_heartbeat", "per_min": 0.2, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "standby_heartbeat", "per_min": 0.2, "scope": "per_host"},
                        {"id": "basebackup_waiting", "per_min": 1.0, "scope": "per_host"},
                        {"id": "primary_conn_failed", "per_min": 0.8, "scope": "per_host"},
                    ]
                },
            },
        },
        "repl_monitor": {
            "svc": "monitoring",
            "hosts": ["mon01"],
            "logs": {
                "repl_lag_ok": {
                    "lvl": "INFO",
                    "msg": "replication lag primary=db1 replica=db2 lag_bytes={lag_bytes} lag_seconds={lag_seconds}",
                    "vars": {"lag_bytes": {"k": "i", "v": [0, 50000000]}, "lag_seconds": {"k": "i", "v": [0, 60]}},
                    "state_vars": {},
                },
                "repl_lag_critical": {
                    "lvl": "ERROR",
                    "msg": "replication stalled primary=db1 replica=db2 lag_bytes={lag_bytes} lag_seconds={lag_seconds} action=page_oncall",
                    "vars": {"lag_bytes": {"k": "i", "v": [500000000, 6000000000]}, "lag_seconds": {"k": "i", "v": [600, 21600]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "repl_lag_ok", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "repl_lag_ok", "per_min": 1.0, "scope": "global"},
                        {"id": "repl_lag_critical", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        "backup_worker": {
            "svc": "backup",
            "hosts": ["backup01"],
            "logs": {
                "backup_job_summary": {
                    "lvl": "INFO",
                    "msg": "backup finished db={db} pg_bin={pg_bin} size_bytes={size_bytes} s3_put={s3_put} exit_code={exit_code}",
                    "vars": {
                        "db": {"k": "ch", "v": ["gitlabhq_production"]},
                        "pg_bin": {"k": "ch", "v": ["9.2"]},
                        "size_bytes": {"k": "i", "v": [0, 8192]},
                        "s3_put": {"k": "ch", "v": ["skipped", "failed"]},
                        "exit_code": {"k": "i", "v": [0, 1]},
                    },
                    "state_vars": {},
                },
                "s3_bucket_list": {
                    "lvl": "WARN",
                    "msg": "s3 list bucket={bucket} objects={objects}",
                    "vars": {"bucket": {"k": "ch", "v": ["gitlab-prod-db-backups"]}, "objects": {"k": "i", "v": [0, 5]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "backup_job_summary", "per_min": 0.1, "scope": "global"}]},
                "f": {"emit": [{"id": "backup_job_summary", "per_min": 0.1, "scope": "global"}]},
            },
        },
        "ops_console": {
            "svc": "ops",
            "hosts": ["ops01"],
            "logs": {
                "wipe_replica_datadir": {
                    "lvl": "WARN",
                    "msg": "run cmd=rm_rf_datadir host=db2 path={path} user={user} rc={rc}",
                    "vars": {
                        "path": {"k": "ch", "v": ["/var/opt/gitlab/postgresql/data"]},
                        "user": {"k": "ch", "v": ["team_member_1"]},
                        "rc": {"k": "i", "v": [0, 1]},
                    },
                    "state_vars": {},
                },
                "set_max_wal_senders": {
                    "lvl": "INFO",
                    "msg": "config change host=db1 param=max_wal_senders old={old} new={new} user={user}",
                    "vars": {"old": {"k": "i", "v": [4, 16]}, "new": {"k": "i", "v": [32, 32]}, "user": {"k": "ch", "v": ["team_member_1"]}},
                    "state_vars": {},
                },
                "set_max_connections": {
                    "lvl": "INFO",
                    "msg": "config change host=db1 param=max_connections old={old} new={new} user={user}",
                    "vars": {"old": {"k": "i", "v": [8000, 8000]}, "new": {"k": "i", "v": [2000, 2000]}, "user": {"k": "ch", "v": ["team_member_1"]}},
                    "state_vars": {},
                },
                "restart_postgres": {
                    "lvl": "INFO",
                    "msg": "service restart host=db1 service=postgresql user={user}",
                    "vars": {"user": {"k": "ch", "v": ["team_member_1"]}},
                    "state_vars": {},
                },
                "rmrf_on_primary_datadir": {
                    "lvl": "CRITICAL",
                    "msg": "rm -rf issued host=db1 path={path} user={user} terminated_after_s={terminated_after_s} bytes_remaining_gb={bytes_remaining_gb}",
                    "vars": {
                        "path": {"k": "ch", "v": ["/var/opt/gitlab/postgresql/data"]},
                        "user": {"k": "ch", "v": ["team_member_1"]},
                        "terminated_after_s": {"k": "i", "v": [1, 10]},
                        "bytes_remaining_gb": {"k": "f", "v": [2.0, 10.0]},
                    },
                    "state_vars": {},
                },
                "lvm_snapshot_found": {
                    "lvl": "INFO",
                    "msg": "snapshot selected name={name} age_min={age_min} size_gb={size_gb} source={source}",
                    "vars": {
                        "name": {"k": "ch", "v": ["staging-db-snap"]},
                        "age_min": {"k": "i", "v": [330, 390]},
                        "size_gb": {"k": "i", "v": [250, 350]},
                        "source": {"k": "ch", "v": ["db1_staging"]},
                    },
                    "state_vars": {},
                },
                "restore_from_snapshot": {
                    "lvl": "WARN",
                    "msg": "restore started dest=db1 source={source} snapshot={snapshot} age_min={age_min}",
                    "vars": {"source": {"k": "ch", "v": ["db1_staging"]}, "snapshot": {"k": "ch", "v": ["staging-db-snap"]}, "age_min": {"k": "i", "v": [330, 390]}},
                    "state_vars": {},
                },
                "start_basebackup": {
                    "lvl": "INFO",
                    "msg": "run cmd=pg_basebackup host=db2 primary=db1 user={user}",
                    "vars": {"user": {"k": "ch", "v": ["team_member_1"]}},
                    "state_vars": {},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "read_web_n",
                    "rpm": 260.0,
                    "emit": ["pg_primary_db1.sql_query_ok", "rails_web.http_access"],
                    "latency_ms": [[10, 70], [60, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "write_snippet_n",
                    "rpm": 120.0,
                    "emit": ["pg_primary_db1.sql_tx_commit", "rails_web.http_access"],
                    "latency_ms": [[20, 150], [120, 500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "read_web_f",
                    "rpm": 240.0,
                    "emit": ["pg_primary_db1.sql_query_ok", "rails_web.http_access"],
                    "latency_ms": [[20, 250], [90, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "write_snippet_ok_f",
                    "rpm": 140.0,
                    "emit": ["pg_primary_db1.sql_tx_commit", "rails_web.http_access"],
                    "latency_ms": [[60, 1200], [200, 2000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "write_snippet_lock_timeout_f",
                    "rpm": 60.0,
                    "emit": ["pg_primary_db1.sql_lock_timeout", "rails_web.http_500"],
                    "latency_ms": [[800, 5000], [800, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "abuse_blocked_request_f",
                    "rpm": 40.0,
                    "emit": ["rails_web.http_403_blocked"],
                    "latency_ms": [[5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "web_request_db_unavailable_f",
                    "rpm": 400.0,
                    "emit": ["rails_web.http_503"],
                    "latency_ms": [[20, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "gitlab_com_db_data_loss_jan2017_compressed",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "read_web_f": 1.1,
                        "write_snippet_ok_f": 1.6,
                        "write_snippet_lock_timeout_f": 3.0,
                        "abuse_blocked_request_f": 0.0,
                        "web_request_db_unavailable_f": 0.0,
                        "repl_monitor.repl_lag_ok": 1.0,
                        "repl_monitor.repl_lag_critical": 0.0,
                        "pg_replica_db2.basebackup_waiting": 0.0,
                        "pg_replica_db2.primary_conn_failed": 0.0,
                        "pg_primary_db1.wal_sender_limit": 0.0,
                    },
                    "latency_multipliers": {
                        "read_web_f": {"p50": 1.2, "p95": 1.8},
                        "write_snippet_ok_f": {"p50": 1.5, "p95": 2.0},
                        "write_snippet_lock_timeout_f": {"p50": 1.2, "p95": 1.2},
                    },
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 33,
                    "rate_multipliers": {
                        "read_web_f": 1.0,
                        "write_snippet_ok_f": 0.9,
                        "write_snippet_lock_timeout_f": 1.2,
                        "abuse_blocked_request_f": 1.0,
                        "web_request_db_unavailable_f": 0.0,
                        "repl_monitor.repl_lag_ok": 0.0,
                        "repl_monitor.repl_lag_critical": 1.0,
                        "pg_replica_db2.basebackup_waiting": 1.0,
                        "pg_replica_db2.primary_conn_failed": 0.0,
                        "pg_primary_db1.wal_sender_limit": 1.0,
                    },
                    "latency_multipliers": {
                        "read_web_f": {"p50": 1.1, "p95": 1.6},
                        "write_snippet_ok_f": {"p50": 1.4, "p95": 1.8},
                        "write_snippet_lock_timeout_f": {"p50": 1.2, "p95": 1.2},
                    },
                    "one_shots": [
                        {"ref": "ops_console.wipe_replica_datadir", "count": 1, "hosts": ["ops01"]},
                        {"ref": "ops_console.start_basebackup", "count": 1, "hosts": ["ops01"]},
                        {"ref": "pg_replica_db2.basebackup_start", "count": 1, "hosts": ["db2"]},
                        {"ref": "ops_console.set_max_wal_senders", "count": 1, "hosts": ["ops01"]},
                        {"ref": "ops_console.restart_postgres", "count": 1, "hosts": ["ops01"]},
                        {"ref": "pg_primary_db1.postgres_startup_failed_semaphores", "count": 1, "hosts": ["db1"]},
                        {"ref": "ops_console.set_max_connections", "count": 1, "hosts": ["ops01"]},
                        {"ref": "pg_primary_db1.postgres_started", "count": 1, "hosts": ["db1"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {
                        "read_web_f": 0.0,
                        "write_snippet_ok_f": 0.0,
                        "write_snippet_lock_timeout_f": 0.0,
                        "abuse_blocked_request_f": 0.0,
                        "web_request_db_unavailable_f": 1.0,
                        "repl_monitor.repl_lag_critical": 1.0,
                        "pg_replica_db2.primary_conn_failed": 1.0,
                        "pg_replica_db2.basebackup_waiting": 0.3,
                        "pg_primary_db1.wal_sender_limit": 0.0,
                    },
                    "latency_multipliers": {"web_request_db_unavailable_f": {"p50": 1.0, "p95": 1.2}},
                    "one_shots": [
                        {"ref": "ops_console.rmrf_on_primary_datadir", "count": 1, "hosts": ["ops01"]},
                        {"ref": "pg_primary_db1.data_dir_missing", "count": 2, "hosts": ["db1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 44,
                    "rate_multipliers": {
                        "read_web_f": 0.7,
                        "write_snippet_ok_f": 0.8,
                        "write_snippet_lock_timeout_f": 0.4,
                        "abuse_blocked_request_f": 0.5,
                        "web_request_db_unavailable_f": 0.0,
                        "repl_monitor.repl_lag_critical": 1.0,
                        "pg_replica_db2.primary_conn_failed": 0.0,
                        "pg_replica_db2.basebackup_waiting": 0.8,
                        "pg_primary_db1.wal_sender_limit": 0.6,
                    },
                    "latency_multipliers": {
                        "read_web_f": {"p50": 1.1, "p95": 1.3},
                        "write_snippet_ok_f": {"p50": 1.2, "p95": 1.5},
                        "write_snippet_lock_timeout_f": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [
                        {"ref": "ops_console.lvm_snapshot_found", "count": 1, "hosts": ["ops01"]},
                        {"ref": "ops_console.restore_from_snapshot", "count": 1, "hosts": ["ops01"]},
                        {"ref": "pg_primary_db1.postgres_started", "count": 1, "hosts": ["db1"]},
                        {"ref": "backup_worker.s3_bucket_list", "count": 1, "hosts": ["backup01"]},
                    ],
                },
            ]
        }
    },
}

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def u01(*parts: Any) -> float:
    s = "|".join(str(p) for p in parts)
    b = _md5_bytes(s)
    n = int.from_bytes(b, "big")
    return n / float(1 << 128)


def hex_lower(n_chars: int, *parts: Any) -> str:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    if n_chars <= 32:
        return h[:n_chars]
    out = h
    i = 0
    while len(out) < n_chars:
        out += hashlib.md5((s + f"|{i}").encode("utf-8")).hexdigest()
        i += 1
    return out[:n_chars]


def ip_from_key(*parts: Any) -> str:
    x = int.from_bytes(_md5_bytes("|".join(str(p) for p in parts)), "big")
    return ".".join(str((x >> (8 * k)) & 0xFF) for k in [3, 2, 1, 0])


def sample_int(lo: int, hi: int, *key: Any) -> int:
    if hi <= lo:
        return lo
    u = u01(*key)
    return lo + int(u * (hi - lo + 1))


def sample_float(lo: float, hi: float, *key: Any) -> float:
    if hi <= lo:
        return lo
    u = u01(*key)
    return lo + (hi - lo) * u


def choose(seq: List[Any], *key: Any) -> Any:
    if not seq:
        return ""
    idx = int(u01(*key) * len(seq))
    if idx >= len(seq):
        idx = len(seq) - 1
    return seq[idx]


def beta_2_6(*key: Any) -> float:
    us_a = [max(1e-12, u01(*key, "a", i)) for i in range(2)]
    us_b = [max(1e-12, u01(*key, "b", i)) for i in range(6)]
    ga = sum(-math.log(u) for u in us_a)
    gb = sum(-math.log(u) for u in us_b)
    return ga / (ga + gb + 1e-12)


def sample_latency_ms(p50: float, p95: float, mult_p50: float, mult_p95: float, *key: Any) -> int:
    sp50 = max(1.0, p50 * mult_p50)
    sp95 = max(sp50, p95 * mult_p95)
    b = beta_2_6(*key, "beta")
    base = sp50 + b * (sp95 - sp50)
    jit = 0.92 + 0.16 * u01(*key, "jit")
    val = base * jit
    cap = 1.2 * sp95
    val = min(val, cap)
    return max(1, int(round(val)))


def expected_to_count(expected: float, *key: Any) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    return n + (1 if u01(*key, "frac") < frac else 0)


def ms_to_iso(ms: int) -> str:
    dt = BASE_TIME + timedelta(milliseconds=ms)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


_formatter = string.Formatter()


def template_fields(msg: str) -> List[str]:
    fields = []
    for _, field_name, _, _ in _formatter.parse(msg):
        if field_name:
            fields.append(field_name)
    return fields


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def get_template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def merged_domain(template: Dict[str, Any], state: str) -> Dict[str, Any]:
    d = dict(template.get("vars", {}))
    state_vars = template.get("state_vars", {})
    if state in state_vars:
        d.update(state_vars[state])
    return d


def get_int_domain_range(comp_id: str, log_id: str, state: str, field: str) -> Optional[Tuple[int, int]]:
    tpl = get_template(comp_id, log_id)
    doms = merged_domain(tpl, state)
    spec = doms.get(field)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    lo, hi = int(spec["v"][0]), int(spec["v"][1])
    return lo, hi


def gen_value(domain: Dict[str, Any], *key: Any) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "ch":
        return choose(list(v), *key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return sample_int(lo, hi, *key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return round(sample_float(lo, hi, *key), 1)
    if k == "hex":
        return hex_lower(int(v), *key)
    if k == "ip":
        return ip_from_key(*key)
    if k == "uuid":
        return hex_lower(32, *key)
    if k == "str":
        return str(v)
    return ""


def render_message(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], *key: Any) -> str:
    tpl = get_template(comp_id, log_id)
    msg = tpl["msg"]
    domains = merged_domain(tpl, state)
    vals: Dict[str, Any] = {}

    for field in template_fields(msg):
        if field in bound:
            vals[field] = bound[field]
        elif field in domains:
            vals[field] = gen_value(domains[field], *key, comp_id, log_id, field)
        else:
            vals[field] = ""

    return msg.format(**vals)


@dataclass
class FailureInterval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Tuple[float, float]]


def build_failure_intervals() -> List[FailureInterval]:
    f_phase = SCENARIO["time"]["phases"]["f"]
    start = int(f_phase["start_min"])
    end = int(f_phase["end_min"])
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    flow_ids = [f["id"] for f in SYSTEM["flows"]["f"]["req"]]
    bg_keys = []
    for cid, comp in SYSTEM["components"].items():
        for be in comp["beh"]["f"]["emit"]:
            bg_keys.append(f"{cid}.{be['id']}")

    rate_mult: Dict[str, float] = {fid: 1.0 for fid in flow_ids}
    for k in bg_keys:
        rate_mult[k] = 1.0

    latency_mult: Dict[str, Tuple[float, float]] = {fid: (1.0, 1.0) for fid in flow_ids}

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for ev in events:
        events_by_min.setdefault(int(ev["at_min"]), []).append(ev)

    boundaries = [start] + sorted([int(ev["at_min"]) for ev in events if int(ev["at_min"]) != start]) + [end]
    boundaries = sorted(set(boundaries))
    if boundaries[0] != start:
        boundaries = [start] + boundaries
    if boundaries[-1] != end:
        boundaries = boundaries + [end]

    intervals: List[FailureInterval] = []
    for i in range(len(boundaries) - 1):
        b0, b1 = boundaries[i], boundaries[i + 1]
        for ev in events_by_min.get(b0, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for fid, mults in ev.get("latency_multipliers", {}).items():
                latency_mult[fid] = (float(mults["p50"]), float(mults["p95"]))
        intervals.append(FailureInterval(start_min=b0, end_min=b1, rate_mult=dict(rate_mult), latency_mult=dict(latency_mult)))
    return intervals


def schedule_evenly(start_ms: int, end_ms: int, count: int, *key: Any, max_jitter_ms: int = 400) -> List[int]:
    if count <= 0:
        return []
    dur = max(1, end_ms - start_ms)
    out: List[int] = []
    base_spacing = dur / count
    jitter_bound = int(min(max_jitter_ms, max(0, base_spacing * 0.15)))
    for i in range(count):
        frac = (i + 0.5) / count
        t = start_ms + int(frac * dur)
        j = int((u01(*key, "j", i) - 0.5) * 2.0 * jitter_bound) if jitter_bound > 0 else 0
        t2 = t + j
        if t2 < start_ms:
            t2 = start_ms
        if t2 >= end_ms:
            t2 = end_ms - 1
        out.append(int(t2))
    return out


def pick_host_for_component(comp_id: str, chain_key: str) -> str:
    hosts = SYSTEM["components"][comp_id]["hosts"]
    if not hosts:
        return ""
    return choose(hosts, "host", comp_id, chain_key)


def emit_row(rows: List[Tuple[int, str, str, str, str, str]], ts_ms: int, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append((int(ts_ms), level, message, trace_id, service, host))


def adjust_delays_for_domains(state: str, emit_refs: List[str], delays: List[int]) -> List[int]:
    """
    Adjust inter-log delays so that message-carried timing fields we bind from these delays
    remain within their modeled domains and consistent with timestamp gaps.
    """
    out = list(delays)

    # DB-side duration fields bound from the DB-step delay.
    for i, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        if comp_id == "pg_primary_db1" and log_id == "sql_query_ok":
            r = get_int_domain_range(comp_id, log_id, state, "query_ms")
            if r:
                lo, hi = r
                out[i] = max(lo, min(out[i], hi))
        elif comp_id == "pg_primary_db1" and log_id == "sql_lock_timeout":
            r = get_int_domain_range(comp_id, log_id, state, "waited_ms")
            if r:
                lo, hi = r
                out[i] = max(lo, min(out[i], hi))
        elif comp_id == "pg_primary_db1" and log_id == "sql_tx_commit":
            rq = get_int_domain_range(comp_id, log_id, state, "query_ms")
            rl = get_int_domain_range(comp_id, log_id, state, "lock_wait_ms")
            if rq and rl:
                qlo, qhi = rq
                llo, lhi = rl
                out[i] = max(qlo + llo, min(out[i], qhi + lhi))

    # IMPORTANT: rails_web.http_access carries db_ms, which in these flows is bound from the preceding DB leg.
    # Ensure the DB-leg delay feeding db_ms stays within the rails db_ms domain for the state, preserving
    # coherence between timestamps and the rendered db_ms value.
    for i, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        if comp_id == "rails_web" and log_id == "http_access" and i - 1 >= 0:
            r = get_int_domain_range("rails_web", "http_access", state, "db_ms")
            if r:
                lo, hi = r
                out[i - 1] = max(lo, min(out[i - 1], hi))

    # Web-side duration_ms fields where duration_ms represents total elapsed (cumulative) in the attempt.
    for i, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        if comp_id == "rails_web" and log_id in ("http_access", "http_500"):
            r = get_int_domain_range(comp_id, log_id, state, "duration_ms")
            if r:
                _, dhi = r
                cum = sum(out[: i + 1])
                if cum > dhi:
                    overflow = cum - dhi
                    out[i] = max(1, out[i] - overflow)
        elif comp_id == "rails_web" and log_id == "http_503":
            r = get_int_domain_range(comp_id, log_id, state, "duration_ms")
            if r:
                lo, hi = r
                out[i] = max(lo, min(out[i], hi))

    return out


def simulate_flow_instance(
    rows: List[Tuple[int, str, str, str, str, str]],
    flow: Dict[str, Any],
    state: str,
    start_ms: int,
    lat_mult: Tuple[float, float],
    instance_id: int,
) -> None:
    flow_id = flow["id"]
    chain_key = f"{state}:{flow_id}:{instance_id}"
    trace_id = hex_lower(32, "trace", chain_key) if flow.get("trace", False) and SYSTEM["tracing"]["on"] else ""

    comp_host: Dict[str, str] = {}

    def host_for(comp_id: str) -> str:
        if comp_id not in comp_host:
            comp_host[comp_id] = pick_host_for_component(comp_id, chain_key)
        return comp_host[comp_id]

    p50m, p95m = lat_mult
    emit_refs = flow["emit"]

    delays: List[int] = []
    for j, (p50, p95) in enumerate(flow["latency_ms"]):
        delays.append(sample_latency_ms(p50, p95, p50m, p95m, "lat", chain_key, j))

    delays = adjust_delays_for_domains(state, emit_refs, delays)

    bound_common = {"trace_id": trace_id}

    if flow_id.startswith("read_web"):
        method = "GET"
        route = choose(["/", "/api/v4/projects", "/users/sign_in"], "route", chain_key)
        status = choose(["200", "302"], "status", chain_key)
    elif flow_id in ("write_snippet_n", "write_snippet_ok_f", "write_snippet_lock_timeout_f", "abuse_blocked_request_f"):
        method = "POST"
        route = choose(["/api/v4/snippets", "/users/sign_in"], "route", chain_key)
        status = choose(["201", "302"], "status", chain_key)
    else:
        method = choose(["GET", "POST"], "method", chain_key)
        route = choose(["/", "/users/sign_in", "/api/v4/snippets", "/api/v4/projects"], "route", chain_key)
        status = choose(["200", "201", "302"], "status", chain_key)

    ts = start_ms

    for j, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        tpl = get_template(comp_id, log_id)
        level = tpl["lvl"]
        service = SYSTEM["components"][comp_id]["svc"] or ""
        host = host_for(comp_id) if SYSTEM["components"][comp_id]["hosts"] else ""

        ts += delays[j]

        bound = dict(bound_common)

        if comp_id == "pg_primary_db1" and log_id == "sql_query_ok":
            bound["query_ms"] = delays[j]

        elif comp_id == "pg_primary_db1" and log_id == "sql_tx_commit":
            total = delays[j]
            rq = get_int_domain_range(comp_id, log_id, state, "query_ms") or (0, total)
            rl = get_int_domain_range(comp_id, log_id, state, "lock_wait_ms") or (0, total)
            qlo, qhi = rq
            llo, lhi = rl

            lock_pref = sample_int(llo, lhi, "lockpref", chain_key)
            query_ms = total - lock_pref
            query_ms = max(qlo, min(query_ms, qhi))
            lock_wait = total - query_ms

            if lock_wait < llo:
                lock_wait = llo
                query_ms = total - lock_wait
            if lock_wait > lhi:
                lock_wait = lhi
                query_ms = total - lock_wait

            query_ms = max(qlo, min(query_ms, qhi))
            lock_wait = max(llo, min(total - query_ms, lhi))
            query_ms = total - lock_wait

            if state == "n":
                query_ms = min(query_ms, 120)
                lock_wait = total - query_ms
                lock_wait = max(0, min(lock_wait, 30))
                query_ms = total - lock_wait
                query_ms = max(5, min(query_ms, 120))

            bound["query_ms"] = int(query_ms)
            bound["lock_wait_ms"] = int(lock_wait)

        elif comp_id == "pg_primary_db1" and log_id == "sql_lock_timeout":
            bound["waited_ms"] = delays[j]

        elif comp_id == "rails_web" and log_id == "http_access":
            db_ms = delays[j - 1] if j > 0 else delays[j]
            dur_ms = sum(delays[: j + 1])
            bound.update({"method": method, "route": route, "status": status, "duration_ms": dur_ms, "db_ms": db_ms})

        elif comp_id == "rails_web" and log_id == "http_500":
            dur_ms = sum(delays[: j + 1])
            bound.update({"method": method, "route": route, "duration_ms": dur_ms})

        elif comp_id == "rails_web" and log_id == "http_503":
            bound.update({"method": method, "route": route, "duration_ms": delays[j]})

        elif comp_id == "rails_web" and log_id == "http_403_blocked":
            bound.update({"method": "POST", "route": route})

        msg = render_message(comp_id, log_id, state, bound, "flow", chain_key, j)
        emit_row(rows, ts, level, msg, trace_id, service, host)


def background_context(comp_id: str, log_id: str, ts_ms: int, state: str, rate_mult: Optional[Dict[str, float]]) -> Dict[str, Any]:
    minute = ts_ms // 60_000
    ctx: Dict[str, Any] = {}

    if comp_id == "repl_monitor" and log_id == "repl_lag_critical":
        base = 600 + max(0, minute - 33) * 600
        ctx["lag_seconds"] = int(min(21600, base + 60 * u01("lagsec", minute, ts_ms) * 5))
        b = 500_000_000 + max(0, minute - 33) * 250_000_000
        ctx["lag_bytes"] = int(min(6_000_000_000, b + 100_000_000 * u01("lagbytes", minute, ts_ms)))
    elif comp_id == "repl_monitor" and log_id == "repl_lag_ok":
        ctx["lag_seconds"] = int(min(60, 1 + 10 * u01("oklag", minute, ts_ms)))
        ctx["lag_bytes"] = int(min(50_000_000, 10_000 + 2_000_000 * u01("okbytes", minute, ts_ms)))
    elif comp_id == "pg_replica_db2" and log_id == "basebackup_waiting":
        base = 20 + max(0, minute - 33) * 30
        ctx["seconds_waiting"] = int(min(600, base + 30 * u01("bbw", minute, ts_ms)))
    elif comp_id == "pg_replica_db2" and log_id == "standby_heartbeat":
        if rate_mult is not None and rate_mult.get("pg_replica_db2.primary_conn_failed", 0.0) > 0.0:
            ctx["status"] = choose(["disconnected", "disconnected", "catching_up"], "hb", minute, ts_ms)
        else:
            ctx["status"] = choose(["streaming", "catching_up"], "hb", minute, ts_ms)
    elif comp_id == "pg_primary_db1" and log_id == "wal_sender_limit":
        max_ws = sample_int(4, 32, "maxws", minute, ts_ms)
        cur = int(min(max_ws, max(0, max_ws - sample_int(0, 2, "delta", minute, ts_ms))))
        ctx["max_wal_senders"] = max_ws
        ctx["current_senders"] = cur
        ctx["client"] = "db2"

    return ctx


def simulate_background(
    rows: List[Tuple[int, str, str, str, str, str]],
    state: str,
    interval_start_min: int,
    interval_end_min: int,
    rate_mult: Optional[Dict[str, float]] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> None:
    start_ms = interval_start_min * 60_000
    end_ms = interval_end_min * 60_000
    duration_min = max(0.0, interval_end_min - interval_start_min)

    for comp_id, comp in sorted(SYSTEM["components"].items()):
        for be in comp["beh"][state]["emit"]:
            log_id = be["id"]
            per_min = float(be["per_min"])
            scope = be.get("scope", "per_host")

            mult_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if rate_mult is not None and mult_key in rate_mult:
                mult = float(rate_mult[mult_key])

            eff_per_min = per_min * mult
            if eff_per_min <= 0.0 or duration_min <= 0:
                continue

            tpl = get_template(comp_id, log_id)
            service = comp["svc"] or ""
            level = tpl["lvl"]
            trace_id = ""

            if scope == "global":
                expected = eff_per_min * duration_min
                count = expected_to_count(expected, "bg", state, mult_key, interval_start_min, interval_end_min)
                ts_list = schedule_evenly(start_ms, end_ms, count, "bg", state, mult_key, interval_start_min, max_jitter_ms=700)
                host = comp["hosts"][0] if comp["hosts"] else ""
                for i, ts in enumerate(ts_list):
                    bound: Dict[str, Any] = {}
                    if extra_context:
                        bound.update(extra_context)
                    bound.update(background_context(comp_id, log_id, ts, state, rate_mult))
                    msg = render_message(comp_id, log_id, state, bound, "bg", state, mult_key, interval_start_min, i)
                    emit_row(rows, ts, level, msg, trace_id, service, host)
            else:
                for host in comp["hosts"]:
                    expected = eff_per_min * duration_min
                    count = expected_to_count(expected, "bg", state, mult_key, host, interval_start_min, interval_end_min)
                    ts_list = schedule_evenly(start_ms, end_ms, count, "bg", state, mult_key, host, interval_start_min, max_jitter_ms=700)
                    for i, ts in enumerate(ts_list):
                        bound = {}
                        if extra_context:
                            bound.update(extra_context)
                        bound.update(background_context(comp_id, log_id, ts, state, rate_mult))
                        msg = render_message(comp_id, log_id, state, bound, "bg", state, mult_key, host, interval_start_min, i)
                        emit_row(rows, ts, level, msg, trace_id, service, host)


def simulate_one_shots(rows: List[Tuple[int, str, str, str, str, str]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ms = at_min * 60_000
        for os_idx, ospec in enumerate(ev.get("one_shots", [])):
            ref = ospec["ref"]
            comp_id, log_id = parse_ref(ref)
            tpl = get_template(comp_id, log_id)
            service = SYSTEM["components"][comp_id]["svc"] or ""
            level = tpl["lvl"]
            trace_id = ""

            count = int(ospec["count"])
            allowed_hosts = ospec.get("hosts") or SYSTEM["components"][comp_id]["hosts"] or [""]

            for j in range(count):
                jitter = int((u01("oneshot", ref, at_min, os_idx, j) - 0.5) * 1200)  # +/- 600ms
                ts = base_ms + jitter
                if ts < 0:
                    ts = 0
                host = allowed_hosts[j % len(allowed_hosts)] if allowed_hosts else ""
                msg = render_message(comp_id, log_id, "f", {}, "oneshot", ref, at_min, os_idx, j)
                emit_row(rows, ts, level, msg, trace_id, service, host)


def simulate() -> pd.DataFrame:
    rows: List[Tuple[int, str, str, str, str, str]] = []

    n_phase = SCENARIO["time"]["phases"]["n"]
    f_phase = SCENARIO["time"]["phases"]["f"]
    n_start, n_end = int(n_phase["start_min"]), int(n_phase["end_min"])
    f_start, f_end = int(f_phase["start_min"]), int(f_phase["end_min"])
    _ = (f_start, f_end)

    simulate_background(rows, "n", n_start, n_end, rate_mult=None)

    normal_flows = SYSTEM["flows"]["n"]["req"]
    for flow_idx, flow in enumerate(normal_flows):
        duration_min = n_end - n_start
        expected_instances = float(flow["rpm"]) * duration_min
        count = expected_to_count(expected_instances, "flowcount", "n", flow["id"], n_start, n_end)
        start_ms = n_start * 60_000
        end_ms = n_end * 60_000
        starts = schedule_evenly(start_ms, end_ms, count, "flowstart", "n", flow["id"], n_start, max_jitter_ms=350)
        for inst_id, t0 in enumerate(starts):
            simulate_flow_instance(rows, flow, "n", t0, (1.0, 1.0), instance_id=(flow_idx * 1_000_000 + inst_id))

    failure_intervals = build_failure_intervals()
    for fi in failure_intervals:
        simulate_background(rows, "f", fi.start_min, fi.end_min, rate_mult=fi.rate_mult)

    failure_flows = {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}
    flow_id_order = [f["id"] for f in SYSTEM["flows"]["f"]["req"]]
    inst_counter = 0
    for fi in failure_intervals:
        duration_min = fi.end_min - fi.start_min
        if duration_min <= 0:
            continue
        start_ms = fi.start_min * 60_000
        end_ms = fi.end_min * 60_000
        for flow_id in flow_id_order:
            flow = failure_flows[flow_id]
            mult = float(fi.rate_mult.get(flow_id, 1.0))
            if mult <= 0.0:
                continue
            expected_instances = float(flow["rpm"]) * mult * duration_min
            count = expected_to_count(expected_instances, "flowcount", "f", flow_id, fi.start_min, fi.end_min)
            starts = schedule_evenly(start_ms, end_ms, count, "flowstart", "f", flow_id, fi.start_min, fi.end_min, max_jitter_ms=350)
            lat_mult = fi.latency_mult.get(flow_id, (1.0, 1.0))
            for t0 in starts:
                simulate_flow_instance(rows, flow, "f", t0, lat_mult, instance_id=inst_counter)
                inst_counter += 1

    simulate_one_shots(rows)

    rows.sort(key=lambda r: r[0])
    df = pd.DataFrame(rows, columns=["_ts_ms", "level", "message", "trace_id", "service", "host"])
    df["timestamp"] = df["_ts_ms"].astype(int).map(ms_to_iso)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    random.seed(0)
    np.random.seed(0)
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
