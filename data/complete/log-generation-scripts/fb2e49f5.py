import hashlib
import ipaddress
import math
import random
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
    "sys": {"id": "github_metadata_platform_oct21"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge", "api"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "dc_link",
            "svc": "wan-link",
            "hosts": ["linkmon-1"],
            "logs": {
                "link_health": {
                    "lvl": "INFO",
                    "msg": "link health east_hub->east_dc rtt_ms={rtt_ms} loss_pct={loss_pct}",
                    "vars": {"rtt_ms": {"k": "i", "v": [2, 120]}},
                    "state_vars": {
                        "n": {"loss_pct": {"k": "f", "v": [0.0, 1.0]}},
                        "f": {"loss_pct": {"k": "f", "v": [0.0, 100.0]}},
                    },
                },
                "link_down": {
                    "lvl": "CRITICAL",
                    "msg": "wan link down east_hub->east_dc loss_pct={loss_pct}",
                    "vars": {"loss_pct": {"k": "f", "v": [90.0, 100.0]}},
                },
                "link_up": {
                    "lvl": "INFO",
                    "msg": "wan link restored east_hub->east_dc after {outage_s}s",
                    "vars": {"outage_s": {"k": "i", "v": [20, 120]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "link_health", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "link_health", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "edge",
            "svc": "edge",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "req_received": {
                    "lvl": "INFO",
                    "msg": "recv {method} {route} trace={trace_id} src={src_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PATCH"]},
                        "route": {"k": "ch", "v": ["/graphql", "/login", "/repos/issues", "/repos/pulls", "/repos/push"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "src_ip": {"k": "ip", "v": "203.0.113.0/24"},
                    },
                },
                "tls_handshake_fail": {
                    "lvl": "WARN",
                    "msg": "tls handshake failed src={src_ip} err={err}",
                    "vars": {
                        "src_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "err": {"k": "ch", "v": ["timeout", "bad_record_mac", "unknown_ca"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "tls_handshake_fail", "per_min": 0.2}]},
                "f": {"emit": [{"id": "tls_handshake_fail", "per_min": 0.5}]},
            },
        },
        {
            "id": "api",
            "svc": "api",
            "hosts": ["api-1", "api-2", "api-3"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "start {method} {route} rid={req_id} trace={trace_id} user={user_tier}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PATCH"]},
                        "route": {"k": "ch", "v": ["/graphql", "/login", "/repos/issues", "/repos/pulls", "/repos/push"]},
                        "req_id": {"k": "hex", "v": 16},
                        "trace_id": {"k": "hex", "v": 32},
                        "user_tier": {"k": "ch", "v": ["anon", "authed"]},
                    },
                },
                "req_end_ok": {
                    "lvl": "INFO",
                    "msg": "end {method} {route} rid={req_id} status={status} dur_ms={dur_ms} db_dc={db_dc} db_role={db_role}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PATCH"]},
                        "route": {"k": "ch", "v": ["/graphql", "/login", "/repos/issues", "/repos/pulls", "/repos/push"]},
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "i", "v": [200, 302]},
                        "db_dc": {"k": "ch", "v": ["east", "west"]},
                        "db_role": {"k": "ch", "v": ["primary", "replica"]},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [20, 800]}},
                        "f": {"dur_ms": {"k": "i", "v": [50, 20000]}},
                    },
                },
                "req_end_err": {
                    "lvl": "ERROR",
                    "msg": "end {method} {route} rid={req_id} status=503 dur_ms={dur_ms} db_dc={db_dc} db_role={db_role} err={err} attempts={attempts}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PATCH"]},
                        "route": {"k": "ch", "v": ["/graphql", "/login", "/repos/issues", "/repos/pulls", "/repos/push"]},
                        "req_id": {"k": "hex", "v": 16},
                        "db_dc": {"k": "ch", "v": ["east", "west"]},
                        "db_role": {"k": "ch", "v": ["primary", "replica"]},
                        "err": {"k": "ch", "v": ["timeout", "connect_timeout", "txn_deadlock"]},
                        "attempts": {"k": "i", "v": [1, 3]},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [80, 3000]}},
                        "f": {"dur_ms": {"k": "i", "v": [500, 30000]}},
                    },
                },
                "req_end_stale": {
                    "lvl": "WARN",
                    "msg": "end {method} {route} rid={req_id} status=200 dur_ms={dur_ms} replica_lag_s={replica_lag_s} served_from=east_replica",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "route": {"k": "ch", "v": ["/graphql", "/repos/issues", "/repos/pulls"]},
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [20, 5000]},
                        "replica_lag_s": {"k": "i", "v": [300, 20000]},
                    },
                },
                "db_retry": {
                    "lvl": "WARN",
                    "msg": "db retry rid={req_id} attempt={attempt} backoff_ms={backoff_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [50, 800]},
                        "err": {"k": "ch", "v": ["timeout", "connect_timeout"]},
                    },
                },
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "db pool stats dc={dc} in_use={in_use} idle={idle} wait_ms_p95={wait_ms_p95}",
                    "vars": {
                        "dc": {"k": "ch", "v": ["east", "west"]},
                        "in_use": {"k": "i", "v": [0, 200]},
                        "idle": {"k": "i", "v": [0, 200]},
                        "wait_ms_p95": {"k": "i", "v": [1, 5000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_stats", "per_min": 0.5}]},
                "f": {"emit": [{"id": "pool_stats", "per_min": 1.0}]},
            },
        },
        {
            "id": "orchestrator",
            "svc": "orchestrator",
            "hosts": ["orch-e1", "orch-w1", "orch-c1"],
            "logs": {
                "raft_tick": {
                    "lvl": "DEBUG",
                    "msg": "raft tick node={node} leader={leader} term={term} commit_idx={commit_idx}",
                    "vars": {
                        "node": {"k": "ch", "v": ["orch-e1", "orch-w1", "orch-c1"]},
                        "leader": {"k": "ch", "v": ["orch-e1", "orch-w1", "orch-c1"]},
                        "term": {"k": "i", "v": [1, 50]},
                        "commit_idx": {"k": "i", "v": [1000, 100000]},
                    },
                },
                "raft_leader_change": {
                    "lvl": "WARN",
                    "msg": "raft leader change old={old_leader} new={new_leader} term={term} reason={reason}",
                    "vars": {
                        "old_leader": {"k": "ch", "v": ["orch-e1", "orch-w1", "orch-c1", "none"]},
                        "new_leader": {"k": "ch", "v": ["orch-e1", "orch-w1", "orch-c1"]},
                        "term": {"k": "i", "v": [1, 50]},
                        "reason": {"k": "ch", "v": ["leader_deselected", "election_timeout"]},
                    },
                },
                "promotion_start": {
                    "lvl": "WARN",
                    "msg": "cluster failover cluster={cluster} from_dc=east to_dc=west new_primary={new_primary}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "new_primary": {"k": "ch", "v": ["mysql-w1", "mysql-w2"]},
                    },
                },
                "api_get_topology": {
                    "lvl": "INFO",
                    "msg": "api get-topology cluster={cluster} servers={servers} dc_set={dc_set}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "servers": {"k": "i", "v": [2, 20]},
                        "dc_set": {"k": "ch", "v": ["west_only", "mixed"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "raft_tick", "per_min": 1.0}]},
                "f": {"emit": [{"id": "raft_tick", "per_min": 1.0}]},
            },
        },
        {
            "id": "mysql_east",
            "svc": "mysql",
            "hosts": ["mysql-e1", "mysql-e2"],
            "logs": {
                "repl_lag_metric": {
                    "lvl": "INFO",
                    "msg": "repl lag cluster={cluster} source_dc={source_dc} lag_s={lag_s} relay_mb={relay_mb}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "source_dc": {"k": "ch", "v": ["west"]},
                        "relay_mb": {"k": "i", "v": [0, 50000]},
                    },
                    "state_vars": {
                        "n": {"lag_s": {"k": "i", "v": [0, 2]}},
                        "f": {"lag_s": {"k": "i", "v": [300, 20000]}},
                    },
                },
                "repl_error": {
                    "lvl": "ERROR",
                    "msg": "repl error cluster={cluster} last_error={last_error}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "last_error": {
                            "k": "ch",
                            "v": ["errant_transactions", "duplicate_key", "relay_log_corrupt", "cannot_connect_source"],
                        },
                    },
                },
                "restore_progress": {
                    "lvl": "INFO",
                    "msg": "restore cluster={cluster} stage={stage} pct={pct}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "stage": {"k": "ch", "v": ["download", "decompress", "prepare", "load"]},
                        "pct": {"k": "f", "v": [0.0, 100.0]},
                    },
                },
                "restore_started": {
                    "lvl": "INFO",
                    "msg": "restore started cluster={cluster} source={source}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "source": {"k": "ch", "v": ["cloud_backup", "west_stream"]},
                    },
                },
                "role_change": {
                    "lvl": "INFO",
                    "msg": "mysql role change cluster={cluster} role={role} dc=east",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "role": {"k": "ch", "v": ["primary", "replica"]},
                    },
                },
                "replica_provisioned": {
                    "lvl": "INFO",
                    "msg": "read replica provisioned cluster={cluster} location={location} count={count}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "location": {"k": "ch", "v": ["east_dc", "east_cloud"]},
                        "count": {"k": "i", "v": [1, 20]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "repl_lag_metric", "per_min": 1.0}]},
                "f": {
                    "emit": [
                        {"id": "repl_error", "per_min": 0.3},
                        {"id": "restore_progress", "per_min": 0.6},
                        {"id": "repl_lag_metric", "per_min": 1.0},
                    ]
                },
            },
        },
        {
            "id": "mysql_west",
            "svc": "mysql",
            "hosts": ["mysql-w1", "mysql-w2"],
            "logs": {
                "primary_status": {
                    "lvl": "INFO",
                    "msg": "mysql primary status cluster={cluster} dc=west qps={qps} repl_clients={repl_clients}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["meta", "auth", "issues"]},
                        "qps": {"k": "i", "v": [200, 5000]},
                        "repl_clients": {"k": "i", "v": [0, 50]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "primary_status", "per_min": 1.0}]},
                "f": {"emit": [{"id": "primary_status", "per_min": 1.0}]},
            },
        },
        {
            "id": "worker",
            "svc": "jobs",
            "hosts": ["worker-1", "worker-2"],
            "logs": {
                "queue_depth": {
                    "lvl": "INFO",
                    "msg": "queue depth kind={kind} depth={depth} oldest_age_s={oldest_age_s}",
                    "vars": {"kind": {"k": "ch", "v": ["webhook", "pages"]}},
                    "state_vars": {
                        "n": {"depth": {"k": "i", "v": [0, 10000]}, "oldest_age_s": {"k": "i", "v": [0, 300]}},
                        "f": {
                            "depth": {"k": "i", "v": [0, 6000000]},
                            "oldest_age_s": {"k": "i", "v": [0, 200000]},
                        },
                    },
                },
                "jobs_paused": {
                    "lvl": "WARN",
                    "msg": "jobs paused kind={kind} reason={reason} queue_depth={queue_depth}",
                    "vars": {
                        "kind": {"k": "ch", "v": ["webhook", "pages"]},
                        "reason": {"k": "ch", "v": ["db_consistency", "incident_mitigation"]},
                        "queue_depth": {"k": "i", "v": [10000, 6000000]},
                    },
                },
                "jobs_resumed": {
                    "lvl": "INFO",
                    "msg": "jobs resumed kind={kind} concurrency={concurrency}",
                    "vars": {"kind": {"k": "ch", "v": ["webhook", "pages"]}, "concurrency": {"k": "i", "v": [10, 200]}},
                },
                "job_processed": {
                    "lvl": "INFO",
                    "msg": "job processed kind={kind} outcome={outcome} dur_ms={dur_ms}",
                    "vars": {
                        "kind": {"k": "ch", "v": ["webhook", "pages"]},
                        "outcome": {"k": "ch", "v": ["sent", "built"]},
                        "dur_ms": {"k": "i", "v": [50, 20000]},
                    },
                },
                "job_dropped_ttl": {
                    "lvl": "WARN",
                    "msg": "job dropped kind=webhook reason=ttl_expired age_s={age_s}",
                    "vars": {"age_s": {"k": "i", "v": [3600, 200000]}},
                },
                "ttl_config_update": {
                    "lvl": "INFO",
                    "msg": "hook ttl updated old_s={old_s} new_s={new_s}",
                    "vars": {"old_s": {"k": "i", "v": [3600, 7200]}, "new_s": {"k": "i", "v": [7200, 43200]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "queue_depth", "per_min": 1.0}]},
                "f": {"emit": [{"id": "queue_depth", "per_min": 2.0}]},
            },
        },
        {
            "id": "sre_control_plane",
            "svc": "sre-ctl",
            "hosts": ["sre-ctl-1"],
            "logs": {
                "status_heartbeat": {
                    "lvl": "INFO",
                    "msg": "status heartbeat current={status}",
                    "vars": {"status": {"k": "ch", "v": ["green", "yellow", "red"]}},
                },
                "alert_flood": {
                    "lvl": "WARN",
                    "msg": "alerts firing service={service} count_5m={count_5m}",
                    "vars": {"service": {"k": "ch", "v": ["api", "mysql", "worker", "network"]}, "count_5m": {"k": "i", "v": [20, 800]}},
                },
                "deploy_lock": {
                    "lvl": "INFO",
                    "msg": "deployments locked by {actor} reason={reason}",
                    "vars": {"actor": {"k": "ch", "v": ["first_responder", "incident_coordinator"]}, "reason": {"k": "ch", "v": ["stability", "change_freeze"]}},
                },
                "status_change": {
                    "lvl": "INFO",
                    "msg": "status set to {status} by {actor}",
                    "vars": {"status": {"k": "ch", "v": ["yellow", "red"]}, "actor": {"k": "ch", "v": ["first_responder", "incident_coordinator"]}},
                },
                "orch_query": {
                    "lvl": "INFO",
                    "msg": "query orchestrator topology cluster={cluster} result={result}",
                    "vars": {"cluster": {"k": "ch", "v": ["meta", "auth", "issues"]}, "result": {"k": "ch", "v": ["ok", "timeout"]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "status_heartbeat", "per_min": 1.0, "scope": "global"},
                        {"id": "alert_flood", "per_min": 0.2, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "status_heartbeat", "per_min": 1.0, "scope": "global"},
                        {"id": "alert_flood", "per_min": 6.0, "scope": "global"},
                    ]
                },
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "user_read_east",
                    "rpm": 280.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [30, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "user_write_east",
                    "rpm": 120.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [60, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "webhook_delivery",
                    "rpm": 60.0,
                    "emit": ["worker.job_processed"],
                    "latency_ms": [[200, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "pages_build",
                    "rpm": 10.0,
                    "emit": ["worker.job_processed"],
                    "latency_ms": [[1000, 7000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "user_read_west",
                    "rpm": 280.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [40, 150]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "user_read_east_replica_lagged",
                    "rpm": 280.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_stale"],
                    "latency_ms": [[2, 6], [1, 4], [35, 140]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "user_read_east_replica_fresh",
                    "rpm": 280.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [30, 110]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "user_write_west_ok",
                    "rpm": 90.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [80, 300]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "user_write_west_timeout",
                    "rpm": 30.0,
                    "emit": ["api.req_start", "api.req_end_err"],
                    "latency_ms": [[1, 5], [1000, 7000]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.5,
                        "emit_per_retry": ["api.db_retry"],
                        "backoff_ms": [[100, 400], [200, 800]],
                    },
                    "trace": True,
                },
                {
                    "id": "user_write_east_ok",
                    "rpm": 120.0,
                    "emit": ["edge.req_received", "api.req_start", "api.req_end_ok"],
                    "latency_ms": [[2, 6], [1, 4], [60, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "webhook_delivery",
                    "rpm": 60.0,
                    "emit": ["worker.job_processed"],
                    "latency_ms": [[300, 2000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "pages_build",
                    "rpm": 10.0,
                    "emit": ["worker.job_processed"],
                    "latency_ms": [[1500, 12000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "webhook_ttl_drop",
                    "rpm": 20.0,
                    "emit": ["worker.job_dropped_ttl"],
                    "latency_ms": [[5, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "orch_api_get_topology",
                    "rpm": 2.0,
                    "emit": ["sre_control_plane.orch_query", "orchestrator.api_get_topology"],
                    "latency_ms": [[5, 30], [20, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "oct21_cross_region_mysql_failover_compressed",
        "time": {
            "total_minutes": 48,
            "phases": {"n": {"start_min": 0, "end_min": 24}, "f": {"start_min": 24, "end_min": 48}},
        },
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 24,
                        "rate_multipliers": {
                            "user_read_east_replica_lagged": 0.0,
                            "user_read_east_replica_fresh": 0.0,
                            "user_write_east_ok": 0.0,
                            "orch_api_get_topology": 0.0,
                            "webhook_ttl_drop": 0.0,
                            "mysql_east.restore_progress": 0.0,
                            "mysql_east.repl_lag_metric": 0.0,
                            "sre_control_plane.alert_flood": 0.0,
                        },
                        "latency_multipliers": {
                            "user_read_west": {"p50": 4.0, "p95": 5.0},
                            "user_write_west_ok": {"p50": 4.0, "p95": 5.0},
                            "user_write_west_timeout": {"p50": 3.0, "p95": 4.0},
                        },
                        "one_shots": [
                            {"ref": "dc_link.link_down", "count": 1, "hosts": ["linkmon-1"]},
                            {"ref": "dc_link.link_up", "count": 1, "hosts": ["linkmon-1"]},
                            {"ref": "orchestrator.raft_leader_change", "count": 1, "hosts": ["orch-w1"]},
                            {"ref": "orchestrator.promotion_start", "count": 3, "hosts": ["orch-w1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 26,
                        "rate_multipliers": {"sre_control_plane.alert_flood": 1.0, "orch_api_get_topology": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "sre_control_plane.deploy_lock", "count": 1, "hosts": ["sre-ctl-1"]},
                            {"ref": "sre_control_plane.status_change", "count": 2, "hosts": ["sre-ctl-1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 30,
                        "rate_multipliers": {"webhook_delivery": 0.0, "pages_build": 0.0, "mysql_east.restore_progress": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "worker.jobs_paused", "count": 2, "hosts": ["worker-1"]},
                            {"ref": "mysql_east.restore_started", "count": 3, "hosts": ["mysql-e1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 38,
                        "rate_multipliers": {
                            "user_write_east_ok": 0.95,
                            "user_write_west_ok": 0.05,
                            "user_write_west_timeout": 0.05,
                            "user_read_west": 0.4,
                            "user_read_east_replica_lagged": 0.6,
                            "user_read_east_replica_fresh": 0.0,
                            "mysql_east.repl_lag_metric": 1.0,
                            "mysql_east.repl_error": 0.5,
                            "mysql_east.restore_progress": 0.0,
                        },
                        "latency_multipliers": {
                            "user_write_west_ok": {"p50": 4.0, "p95": 5.0},
                            "user_write_west_timeout": {"p50": 3.0, "p95": 4.0},
                            "user_read_west": {"p50": 4.0, "p95": 5.0},
                        },
                        "one_shots": [{"ref": "mysql_east.role_change", "count": 3, "hosts": ["mysql-e1"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 44,
                        "rate_multipliers": {
                            "user_read_west": 0.2,
                            "user_read_east_replica_lagged": 0.2,
                            "user_read_east_replica_fresh": 0.6,
                            "webhook_delivery": 5.0,
                            "pages_build": 5.0,
                            "webhook_ttl_drop": 1.0,
                            "mysql_east.repl_error": 0.2,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "mysql_east.replica_provisioned", "count": 3, "hosts": ["mysql-e1"]},
                            {"ref": "worker.jobs_resumed", "count": 2, "hosts": ["worker-1"]},
                            {"ref": "worker.ttl_config_update", "count": 1, "hosts": ["worker-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Deterministic helpers
# -----------------------------


def stable_hash_int(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def rng_for(*parts: Any) -> np.random.RandomState:
    return np.random.RandomState(stable_hash_int(*parts) & 0xFFFFFFFF)


def hex_token(n_hex: int, *parts: Any) -> str:
    r = rng_for("hex", n_hex, *parts)
    n_bytes = (n_hex + 1) // 2
    b = r.bytes(n_bytes)
    hx = b.hex()
    return hx[:n_hex]


def sample_domain(dom: Dict[str, Any], *seed_parts: Any) -> Any:
    k = dom["k"]
    v = dom["v"]
    r = rng_for("dom", k, str(v), *seed_parts)
    if k == "ch":
        return v[int(r.randint(0, len(v)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(r.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(f"{(lo + (hi - lo) * r.rand()):.1f}")
    if k == "hex":
        return hex_token(int(v), *seed_parts)
    if k == "ip":
        net = ipaddress.ip_network(v, strict=False)
        n = net.num_addresses
        if n <= 2:
            return str(net.network_address)
        offset = int(r.randint(1, n - 1))
        return str(ipaddress.ip_address(int(net.network_address) + offset))
    if k == "uuid":
        hx = hex_token(32, *seed_parts)
        return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "str":
        return str(v)
    raise ValueError(f"Unknown domain kind: {k}")


def deterministic_round(expected: float, *seed_parts: Any) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    r = rng_for("round", *seed_parts)
    return base + (1 if r.rand() < frac else 0)


_NORMAL = NormalDist()


def lognormal_sample_ms(p50_ms: float, p95_ms: float, u: float, soft_cap_mult: float = 3.0) -> float:
    p50_ms = max(0.1, float(p50_ms))
    p95_ms = max(p50_ms, float(p95_ms))
    if p95_ms == p50_ms:
        x = p50_ms
    else:
        sigma = math.log(p95_ms / p50_ms) / 1.6448536269514722
        mu = math.log(p50_ms)
        z = _NORMAL.inv_cdf(min(0.999, max(0.001, u)))
        x = math.exp(mu + sigma * z)
    cap = soft_cap_mult * p95_ms
    if x > cap:
        x = cap
    return x


def schedule_even_times(count: int, start: datetime, end: datetime, *seed_parts: Any, jitter_ms: int = 300) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = max(0.001, (end - start).total_seconds())
    out: List[datetime] = []
    for i in range(count):
        base_off = (i + 0.5) * dur_s / count
        r = rng_for("sched", *seed_parts, i)
        jitter = (r.rand() * 2 - 1) * (jitter_ms / 1000.0)
        t = start + timedelta(seconds=base_off + jitter)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def parse_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


# -----------------------------
# Indices
# -----------------------------


@dataclass(frozen=True)
class Comp:
    id: str
    svc: str
    hosts: List[str]
    logs: Dict[str, Dict[str, Any]]
    beh: Dict[str, Dict[str, Any]]


COMP: Dict[str, Comp] = {}
LOG: Dict[str, Dict[str, Any]] = {}
FLOW: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}

for c in SYSTEM["components"]:
    comp = Comp(id=c["id"], svc=c.get("svc", "") or "", hosts=c.get("hosts", []) or [], logs=c.get("logs", {}), beh=c.get("beh", {}))
    COMP[comp.id] = comp
    for log_id, tmpl in comp.logs.items():
        LOG[f"{comp.id}.{log_id}"] = tmpl

for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]["req"]:
        FLOW[st][f["id"]] = f

# -----------------------------
# Scenario controls
# -----------------------------


def build_failure_intervals() -> List[Dict[str, Any]]:
    scen = SCENARIO["scenario"]
    fstart = scen["time"]["phases"]["f"]["start_min"]
    fend = scen["time"]["phases"]["f"]["end_min"]
    events = sorted(scen["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    bounds = [fstart] + [e["at_min"] for e in events] + [fend]
    dedup: List[int] = []
    for b in bounds:
        if not dedup or dedup[-1] != b:
            dedup.append(b)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(dedup) - 1):
        a = dedup[i]
        b = dedup[i + 1]
        if a in ev_by_min:
            for e in sorted(ev_by_min[a], key=lambda x: x["order"]):
                for k, v in e.get("rate_multipliers", {}).items():
                    active_rate[k] = float(v)
                for k, v in e.get("latency_multipliers", {}).items():
                    active_lat[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        intervals.append({"start_min": a, "end_min": b, "rate": dict(active_rate), "lat": dict(active_lat)})
    return intervals


FAIL_INTERVALS = build_failure_intervals()

# -----------------------------
# Message rendering
# -----------------------------


def vars_for_log(comp_id: str, log_id: str, state: str) -> Dict[str, Dict[str, Any]]:
    tmpl = COMP[comp_id].logs[log_id]
    out = dict(tmpl.get("vars", {}))
    sv = tmpl.get("state_vars", {}).get(state, {})
    out.update(sv)
    return out


def render_message(comp_id: str, log_id: str, state: str, bindings: Dict[str, Any], *seed_parts: Any) -> Tuple[str, str]:
    tmpl = COMP[comp_id].logs[log_id]
    msg_t = tmpl["msg"]
    doms = vars_for_log(comp_id, log_id, state)

    needed: List[str] = []
    i = 0
    while i < len(msg_t):
        if msg_t[i] == "{":
            j = msg_t.find("}", i + 1)
            if j != -1:
                needed.append(msg_t[i + 1 : j])
                i = j + 1
                continue
        i += 1

    vals: Dict[str, Any] = {}
    for k in needed:
        if k in bindings:
            vals[k] = bindings[k]
        elif k in doms:
            vals[k] = sample_domain(doms[k], comp_id, log_id, state, k, *seed_parts)
        else:
            vals[k] = ""
    return tmpl["lvl"], msg_t.format(**vals)


# -----------------------------
# Background generation
# -----------------------------


def choose_host_sticky(comp_id: str, key: str) -> str:
    hosts = COMP[comp_id].hosts
    if not hosts:
        return ""
    r = rng_for("host", comp_id, key)
    return hosts[int(r.randint(0, len(hosts)))]


def background_bindings(comp_id: str, log_id: str, state: str, t: datetime) -> Dict[str, Any]:
    b: Dict[str, Any] = {}
    minute = int((t - BASE_TIME).total_seconds() // 60)

    if comp_id == "sre_control_plane" and log_id == "status_heartbeat":
        if state == "n":
            b["status"] = "green"
        else:
            if minute < 26:
                b["status"] = "green"
            elif minute < 27:
                b["status"] = "yellow"
            else:
                b["status"] = "red"

    if comp_id == "orchestrator" and log_id == "raft_tick":
        if state == "n":
            b["leader"] = "orch-e1"
        else:
            b["leader"] = "orch-w1"
        t0 = 10 if state == "n" else 20
        b["term"] = max(1, min(50, t0 + (minute // 5)))
        b["commit_idx"] = max(1000, min(100000, 20000 + minute * 50))

    if comp_id == "dc_link" and log_id == "link_health":
        if state == "n":
            b["rtt_ms"] = 3 + (minute % 3)
            b["loss_pct"] = float(f"{0.1 * (minute % 5):.1f}")
        else:
            if 24 <= minute <= 25:
                b["rtt_ms"] = 80 + (minute - 24) * 15
                b["loss_pct"] = float(f"{95.0 + (minute - 24) * 2.0:.1f}")
            else:
                b["rtt_ms"] = 20 + (minute % 20)
                b["loss_pct"] = float(f"{min(30.0, 1.0 + (minute % 10) * 0.7):.1f}")

    if comp_id == "api" and log_id == "pool_stats":
        if state == "n":
            b["dc"] = "east"
            b["in_use"] = 80 + (minute % 30)
            b["idle"] = 60 + (minute % 20)
            b["wait_ms_p95"] = 10 + (minute % 40)
        else:
            if minute < 38:
                b["dc"] = "west"
                b["in_use"] = 160 + (minute % 30)
                b["idle"] = 20 + (minute % 15)
                b["wait_ms_p95"] = 800 + (minute % 800)
            else:
                b["dc"] = "east" if (minute % 2 == 0) else "west"
                b["in_use"] = 140 + (minute % 40)
                b["idle"] = 30 + (minute % 25)
                b["wait_ms_p95"] = 400 + (minute % 700)

    if comp_id == "worker" and log_id == "queue_depth":
        if state == "n":
            b["depth"] = 200 + (minute % 100)
            b["oldest_age_s"] = 10 + (minute % 30)
        else:
            if minute < 30:
                depth = 2000 + (minute - 24) * 500
                age = 60 + (minute - 24) * 30
            elif minute < 44:
                depth = 15000 + (minute - 30) * 25000
                age = 600 + (minute - 30) * 900
            else:
                depth = 350000 + max(0, 48 - minute) * 20000
                age = 25000 + (minute - 44) * 2000
            b["depth"] = int(max(0, min(6000000, depth)))
            b["oldest_age_s"] = int(max(0, min(200000, age)))

    if comp_id == "mysql_east" and log_id == "repl_lag_metric":
        if state == "n":
            b["lag_s"] = minute % 3
            b["relay_mb"] = 10 + (minute % 50)
        else:
            if minute < 38:
                b["lag_s"] = 0
                b["relay_mb"] = 0
            else:
                lag = 1000 + (minute - 38) * 1200
                b["lag_s"] = int(max(300, min(20000, lag)))
                b["relay_mb"] = int(max(0, min(50000, 5000 + (minute - 38) * 3500)))

    if comp_id == "mysql_west" and log_id == "primary_status":
        if state == "n":
            b["qps"] = 800 + (minute % 300)
            b["repl_clients"] = 3 + (minute % 5)
        else:
            b["qps"] = 2500 + (minute % 1000)
            b["repl_clients"] = 10 + (minute % 20)

    return b


# -----------------------------
# Flow simulation
# -----------------------------


def flow_semantic_defaults(flow_id: str) -> Dict[str, Any]:
    if flow_id in ("user_read_east", "user_read_east_replica_lagged", "user_read_east_replica_fresh"):
        return {"db_dc": "east", "db_role": "replica", "kind": None}
    if flow_id == "user_read_west":
        return {"db_dc": "west", "db_role": "replica", "kind": None}
    if flow_id in ("user_write_east", "user_write_east_ok"):
        return {"db_dc": "east", "db_role": "primary", "kind": None}
    if flow_id in ("user_write_west_ok", "user_write_west_timeout"):
        return {"db_dc": "west", "db_role": "primary", "kind": None}
    if flow_id == "webhook_delivery":
        return {"kind": "webhook"}
    if flow_id == "pages_build":
        return {"kind": "pages"}
    if flow_id == "webhook_ttl_drop":
        return {"kind": "webhook"}
    if flow_id == "orch_api_get_topology":
        return {}
    return {}


def choose_method_route(flow_id: str, *seed_parts: Any) -> Tuple[str, str]:
    r = rng_for("mr", flow_id, *seed_parts)
    if flow_id.startswith("user_read"):
        method = "GET"
        routes = ["/graphql", "/repos/issues", "/repos/pulls"]
        route = routes[int(r.randint(0, len(routes)))]
        return method, route
    if flow_id.startswith("user_write"):
        methods = ["POST", "PATCH"]
        method = methods[int(r.randint(0, len(methods)))]
        routes = ["/repos/push", "/repos/issues", "/repos/pulls", "/login"]
        route = routes[int(r.randint(0, len(routes)))]
        return method, route
    return "GET", "/graphql"


def choose_attempts(flow_spec: Dict[str, Any], *seed_parts: Any) -> int:
    retry = flow_spec["retry"]
    m = int(retry["max_attempts"])
    e = float(retry["expected_attempts"])
    if m <= 1:
        return 1
    lo = max(1, min(m, int(math.floor(e))))
    hi = max(1, min(m, int(math.ceil(e))))
    if lo == hi:
        return lo
    frac = e - lo
    r = rng_for("attempts", flow_spec["id"], *seed_parts)
    return hi if r.rand() < frac else lo


def sample_latency_pair(pair: List[float], mult: Dict[str, float], *seed_parts: Any) -> float:
    p50, p95 = float(pair[0]), float(pair[1])
    mp50 = mult.get("p50", 1.0)
    mp95 = mult.get("p95", 1.0)
    p50 *= mp50
    p95 *= mp95
    h = stable_hash_int("u", *seed_parts) % 1000
    # Keep u away from extreme tails for stability and to avoid unbounded durations.
    u = 0.55 + 0.35 * (h / 1000.0)
    return lognormal_sample_ms(p50, p95, u=u, soft_cap_mult=3.0)


def simulate_flow_instance(
    records: List[Dict[str, Any]],
    state: str,
    flow_id: str,
    flow_spec: Dict[str, Any],
    start_time: datetime,
    latency_mult: Dict[str, float],
    instance_idx: int,
) -> None:
    trace_id = hex_token(32, "trace", state, flow_id, instance_idx) if (SYSTEM["tracing"]["on"] and flow_spec.get("trace", False)) else ""
    req_id = hex_token(16, "req", state, flow_id, instance_idx)

    semantic = flow_semantic_defaults(flow_id)
    method, route = choose_method_route(flow_id, state, instance_idx)
    user_tier = "authed" if (rng_for("user_tier", flow_id, instance_idx).rand() < 0.55) else "anon"

    orch_cluster = ["meta", "auth", "issues"][instance_idx % 3]
    minute = int((start_time - BASE_TIME).total_seconds() // 60)
    orch_dc_set = "west_only" if minute < 38 else "mixed"

    attempts = choose_attempts(flow_spec, state, instance_idx)

    comp_host: Dict[str, str] = {}

    def host_for(comp_id: str) -> str:
        if comp_id not in comp_host:
            key = trace_id or req_id or f"{flow_id}-{instance_idx}"
            comp_host[comp_id] = choose_host_sticky(comp_id, key)
        return comp_host[comp_id]

    cursor = start_time
    backoff_pairs = flow_spec["retry"].get("backoff_ms", [])
    emit_per_retry = flow_spec["retry"].get("emit_per_retry", [])

    err_choice = None
    if flow_id == "user_write_west_timeout":
        err_choice = "timeout" if (rng_for("err", flow_id, instance_idx).rand() < 0.75) else "connect_timeout"

    # Special-case: user_write_west_timeout models an API request with internal DB retries.
    # Emit req_start once, emit per-retry markers on retries, and emit req_end_err once with attempts=total.
    if flow_id == "user_write_west_timeout" and int(flow_spec["retry"]["max_attempts"]) > 1:
        # Emit req_start
        d0_ms = sample_latency_pair(flow_spec["latency_ms"][0], latency_mult, flow_id, instance_idx, "lat0")
        cursor = cursor + timedelta(milliseconds=d0_ms)
        c_id, l_id = parse_ref("api.req_start")
        bindings = {"method": method, "route": route, "req_id": req_id, "trace_id": trace_id, "user_tier": user_tier}
        lvl, msg = render_message(c_id, l_id, state, bindings, flow_id, instance_idx, "emit", 1, 0, "api.req_start")
        records.append(
            {
                "timestamp": cursor,
                "level": lvl,
                "message": msg,
                "trace_id": trace_id,
                "service": COMP[c_id].svc,
                "host": host_for(c_id),
            }
        )
        t_req_start = cursor

        # Sample a total work time consistent with the modeled (req_start -> req_end_err) latency pair
        # and distribute it across attempts so total duration stays coherent and bounded.
        total_work_ms = sample_latency_pair(flow_spec["latency_ms"][1], latency_mult, flow_id, instance_idx, "work_total")
        weights = list(range(1, attempts + 1))
        wsum = float(sum(weights))
        work_slices = [total_work_ms * (w / wsum) for w in weights]

        # Retry loop (internal attempts); emit retry markers only on attempts 2..A.
        for a in range(1, attempts + 1):
            cursor = cursor + timedelta(milliseconds=work_slices[a - 1])

            if a < attempts:
                pair = backoff_pairs[a - 1] if (a - 1) < len(backoff_pairs) else backoff_pairs[-1]
                bo_ms = sample_latency_pair(pair, {"p50": 1.0, "p95": 1.0}, flow_id, instance_idx, "backoff", a + 1)
                cursor = cursor + timedelta(milliseconds=bo_ms)

                for ref in emit_per_retry:
                    c2, l2 = parse_ref(ref)
                    bindings2: Dict[str, Any] = {}
                    if ref == "api.db_retry":
                        bindings2 = {
                            "req_id": req_id,
                            "attempt": a + 1,
                            "backoff_ms": int(round(bo_ms)),
                            "err": err_choice or "timeout",
                        }
                    lvl2, msg2 = render_message(c2, l2, state, bindings2, flow_id, instance_idx, "retry", a + 1, ref)
                    records.append(
                        {
                            "timestamp": cursor,
                            "level": lvl2,
                            "message": msg2,
                            "trace_id": trace_id,
                            "service": COMP[c2].svc,
                            "host": host_for(c2),
                        }
                    )

        # Emit req_end_err once at the end; dur_ms must match the actual elapsed time since req_start.
        c3, l3 = parse_ref("api.req_end_err")
        dur_ms_total = int(round((cursor - t_req_start).total_seconds() * 1000.0))
        bindings3 = {
            "method": method,
            "route": route,
            "req_id": req_id,
            "dur_ms": dur_ms_total,
            "db_dc": semantic.get("db_dc", "west"),
            "db_role": semantic.get("db_role", "primary"),
            "err": err_choice or sample_domain(LOG["api.req_end_err"]["vars"]["err"], flow_id, instance_idx, "err"),
            "attempts": attempts,
        }
        lvl3, msg3 = render_message(c3, l3, state, bindings3, flow_id, instance_idx, "emit_final", attempts, "api.req_end_err")
        records.append(
            {
                "timestamp": cursor,
                "level": lvl3,
                "message": msg3,
                "trace_id": trace_id,
                "service": COMP[c3].svc,
                "host": host_for(c3),
            }
        )
        return

    # Default behavior: per-attempt emit chain as encoded (max_attempts=1 for all other flows in this model).
    for a in range(1, attempts + 1):
        if a >= 2:
            pair = backoff_pairs[a - 2] if (a - 2) < len(backoff_pairs) else backoff_pairs[-1]
            bo = sample_latency_pair(pair, {"p50": 1.0, "p95": 1.0}, flow_id, instance_idx, "backoff", a)
            cursor = cursor + timedelta(milliseconds=bo)

            for ref in emit_per_retry:
                c_id, l_id = parse_ref(ref)
                bindings = {}
                if ref == "api.db_retry":
                    bindings = {
                        "req_id": req_id,
                        "attempt": a,
                        "backoff_ms": int(round(bo)),
                        "err": err_choice or "timeout",
                    }
                lvl, msg = render_message(c_id, l_id, state, bindings, flow_id, instance_idx, "retry", a, ref)
                records.append(
                    {
                        "timestamp": cursor,
                        "level": lvl,
                        "message": msg,
                        "trace_id": trace_id if flow_spec.get("trace", False) else "",
                        "service": COMP[c_id].svc,
                        "host": host_for(c_id),
                    }
                )

        for li, ref in enumerate(flow_spec["emit"]):
            c_id, l_id = parse_ref(ref)
            d_ms = sample_latency_pair(flow_spec["latency_ms"][li], latency_mult, flow_id, instance_idx, "lat", a, li, ref)
            cursor = cursor + timedelta(milliseconds=d_ms)

            bindings2: Dict[str, Any] = {}

            if ref == "edge.req_received":
                bindings2 = {
                    "method": method,
                    "route": route,
                    "trace_id": trace_id,
                    "src_ip": sample_domain(LOG[ref]["vars"]["src_ip"], "src", flow_id, instance_idx),
                }
            elif ref == "api.req_start":
                bindings2 = {"method": method, "route": route, "req_id": req_id, "trace_id": trace_id, "user_tier": user_tier}
            elif ref == "api.req_end_ok":
                status = 302 if (route == "/login" and method in ("POST", "PATCH")) else 200
                bindings2 = {
                    "method": method,
                    "route": route,
                    "req_id": req_id,
                    "status": status,
                    "dur_ms": int(round(d_ms)),
                    "db_dc": semantic.get("db_dc", "east"),
                    "db_role": semantic.get("db_role", "replica"),
                }
            elif ref == "api.req_end_stale":
                bindings2 = {
                    "method": "GET",
                    "route": route if route in ["/graphql", "/repos/issues", "/repos/pulls"] else "/graphql",
                    "req_id": req_id,
                    "dur_ms": int(round(d_ms)),
                    "replica_lag_s": sample_domain(LOG[ref]["vars"]["replica_lag_s"], flow_id, instance_idx, "lag", minute),
                }
            elif ref == "api.req_end_err":
                bindings2 = {
                    "method": method,
                    "route": route,
                    "req_id": req_id,
                    "dur_ms": int(round(d_ms)),
                    "db_dc": semantic.get("db_dc", "west"),
                    "db_role": semantic.get("db_role", "primary"),
                    "err": err_choice or sample_domain(LOG[ref]["vars"]["err"], flow_id, instance_idx, "err"),
                    "attempts": a,
                }
            elif ref == "worker.job_processed":
                kind = semantic.get("kind", "webhook")
                outcome = "sent" if kind == "webhook" else "built"
                bindings2 = {"kind": kind, "outcome": outcome, "dur_ms": int(round(d_ms))}
            elif ref == "worker.job_dropped_ttl":
                bindings2 = {"age_s": sample_domain(LOG[ref]["vars"]["age_s"], flow_id, instance_idx, "age", minute)}
            elif ref == "sre_control_plane.orch_query":
                result = "timeout" if (rng_for("orch_q", flow_id, instance_idx).rand() < 0.08) else "ok"
                bindings2 = {"cluster": orch_cluster, "result": result}
            elif ref == "orchestrator.api_get_topology":
                servers = 6 if orch_dc_set == "west_only" else 12
                bindings2 = {"cluster": orch_cluster, "servers": servers, "dc_set": orch_dc_set}

            lvl, msg = render_message(c_id, l_id, state, bindings2, flow_id, instance_idx, "emit", a, li, ref)
            records.append(
                {
                    "timestamp": cursor,
                    "level": lvl,
                    "message": msg,
                    "trace_id": trace_id if flow_spec.get("trace", False) else "",
                    "service": COMP[c_id].svc,
                    "host": host_for(c_id),
                }
            )


# -----------------------------
# One-shots
# -----------------------------


def emit_one_shots(records: List[Dict[str, Any]]) -> None:
    scen = SCENARIO["scenario"]
    events = sorted(scen["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    for e in events:
        at_min = int(e["at_min"])
        event_time = BASE_TIME + timedelta(minutes=at_min)

        down_time: Optional[datetime] = None
        outage_s_for_up: Optional[int] = None

        for idx, ospec in enumerate(e.get("one_shots", [])):
            ref = ospec["ref"]
            comp_id, log_id = parse_ref(ref)
            count = int(ospec["count"])
            hosts = ospec.get("hosts", [])
            allowed_hosts = hosts if hosts else COMP[comp_id].hosts

            if ref == "dc_link.link_down":
                down_time = event_time + timedelta(seconds=1)
                t_list = [down_time] * count
            elif ref == "dc_link.link_up" and down_time is not None:
                outage_s_for_up = sample_domain(COMP["dc_link"].logs["link_up"]["vars"]["outage_s"], "outage", at_min)
                up_time = down_time + timedelta(seconds=int(outage_s_for_up))
                t_list = [up_time] * count
            else:
                t_list = schedule_even_times(count, event_time, event_time + timedelta(seconds=2), "oneshot", at_min, ref, idx, jitter_ms=250)

            for i, t in enumerate(t_list):
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else ""
                bindings: Dict[str, Any] = {}

                if ref == "orchestrator.raft_leader_change":
                    bindings = {"old_leader": "orch-e1", "new_leader": "orch-w1", "term": 21, "reason": "election_timeout"}
                elif ref == "orchestrator.promotion_start":
                    cluster = ["meta", "auth", "issues"][i % 3]
                    new_primary = ["mysql-w1", "mysql-w2"][i % 2]
                    bindings = {"cluster": cluster, "new_primary": new_primary}
                elif ref == "sre_control_plane.status_change":
                    status = "yellow" if i == 0 else "red"
                    bindings = {"status": status, "actor": "incident_coordinator"}
                elif ref == "sre_control_plane.deploy_lock":
                    bindings = {"actor": "first_responder", "reason": "stability"}
                elif ref == "worker.jobs_paused":
                    kind = "webhook" if i == 0 else "pages"
                    bindings = {"kind": kind, "reason": "db_consistency", "queue_depth": 250000}
                elif ref == "mysql_east.restore_started":
                    cluster = ["meta", "auth", "issues"][i % 3]
                    bindings = {"cluster": cluster, "source": "cloud_backup"}
                elif ref == "mysql_east.role_change":
                    cluster = ["meta", "auth", "issues"][i % 3]
                    bindings = {"cluster": cluster, "role": "primary"}
                elif ref == "mysql_east.replica_provisioned":
                    cluster = ["meta", "auth", "issues"][i % 3]
                    bindings = {"cluster": cluster, "location": "east_cloud", "count": 6}
                elif ref == "worker.jobs_resumed":
                    kind = "webhook" if i == 0 else "pages"
                    bindings = {"kind": kind, "concurrency": 120}
                elif ref == "worker.ttl_config_update":
                    bindings = {"old_s": 7200, "new_s": 43200}
                elif ref == "dc_link.link_up" and outage_s_for_up is not None:
                    bindings = {"outage_s": int(outage_s_for_up)}

                lvl, msg = render_message(comp_id, log_id, "f", bindings, "oneshot", at_min, ref, i)
                records.append(
                    {
                        "timestamp": t,
                        "level": lvl,
                        "message": msg,
                        "trace_id": "",
                        "service": COMP[comp_id].svc,
                        "host": host,
                    }
                )


# -----------------------------
# Main simulation
# -----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def gen_background(records: List[Dict[str, Any]], state: str, start_min: int, end_min: int, rate_mult: Optional[Dict[str, float]] = None) -> None:
    start = BASE_TIME + timedelta(minutes=start_min)
    end = BASE_TIME + timedelta(minutes=end_min)
    duration_min = end_min - start_min
    rate_mult = rate_mult or {}

    for comp_id, comp in COMP.items():
        beh = comp.beh.get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            src_key = f"{comp_id}.{log_id}"

            eff = per_min
            if state == "f":
                eff *= float(rate_mult.get(src_key, 1.0))

            if eff <= 0:
                continue

            if scope == "global":
                cnt = deterministic_round(eff * duration_min, "bg", state, src_key, start_min, end_min)
                times = schedule_even_times(cnt, start, end, "bg", state, src_key, start_min, end_min, jitter_ms=700)
                for i, t in enumerate(times):
                    host = comp.hosts[0] if comp.hosts else ""
                    bindings = background_bindings(comp_id, log_id, state, t)
                    if comp_id == "orchestrator" and log_id == "raft_tick":
                        bindings = dict(bindings)
                        bindings["node"] = host
                    lvl, msg = render_message(comp_id, log_id, state, bindings, "bg", state, src_key, start_min, i)
                    records.append({"timestamp": t, "level": lvl, "message": msg, "trace_id": "", "service": comp.svc, "host": host})
            else:
                for h in comp.hosts:
                    cnt = deterministic_round(eff * duration_min, "bg", state, src_key, h, start_min, end_min)
                    times = schedule_even_times(cnt, start, end, "bg", state, src_key, h, start_min, end_min, jitter_ms=700)
                    for i, t in enumerate(times):
                        bindings = background_bindings(comp_id, log_id, state, t)
                        if comp_id == "orchestrator" and log_id == "raft_tick":
                            bindings = dict(bindings)
                            bindings["node"] = h
                        lvl, msg = render_message(comp_id, log_id, state, bindings, "bg", state, src_key, h, start_min, i)
                        records.append({"timestamp": t, "level": lvl, "message": msg, "trace_id": "", "service": comp.svc, "host": h})


def gen_flows(
    records: List[Dict[str, Any]],
    state: str,
    start_min: int,
    end_min: int,
    rate_mult: Optional[Dict[str, float]] = None,
    lat_mult: Optional[Dict[str, Dict[str, float]]] = None,
) -> None:
    start = BASE_TIME + timedelta(minutes=start_min)
    end = BASE_TIME + timedelta(minutes=end_min)
    duration_min = end_min - start_min
    rate_mult = rate_mult or {}
    lat_mult = lat_mult or {}

    for flow_id, spec in FLOW[state].items():
        rpm = float(spec["rpm"])
        eff_rpm = rpm
        if state == "f":
            eff_rpm *= float(rate_mult.get(flow_id, 1.0))
        if eff_rpm <= 0:
            continue

        n_inst = deterministic_round(eff_rpm * duration_min, "flowcnt", state, flow_id, start_min, end_min)
        if n_inst <= 0:
            continue

        times = schedule_even_times(n_inst, start, end, "flowstart", state, flow_id, start_min, end_min, jitter_ms=950)
        lm = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
        for idx, t0 in enumerate(times):
            simulate_flow_instance(records, state, flow_id, spec, t0, lm, instance_idx=(start_min * 100000 + idx))


def main() -> None:
    random.seed(0)
    np.random.seed(0)

    records: List[Dict[str, Any]] = []

    scen = SCENARIO["scenario"]
    nstart = scen["time"]["phases"]["n"]["start_min"]
    nend = scen["time"]["phases"]["n"]["end_min"]
    fstart = scen["time"]["phases"]["f"]["start_min"]
    fend = scen["time"]["phases"]["f"]["end_min"]

    gen_background(records, "n", nstart, nend, rate_mult=None)
    gen_flows(records, "n", nstart, nend, rate_mult=None, lat_mult=None)

    for itv in FAIL_INTERVALS:
        a = int(itv["start_min"])
        b = int(itv["end_min"])
        gen_background(records, "f", a, b, rate_mult=itv["rate"])
        gen_flows(records, "f", a, b, rate_mult=itv["rate"], lat_mult=itv["lat"])

    emit_one_shots(records)

    df = pd.DataFrame(records)
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    df["timestamp"] = df["timestamp"].apply(fmt_ts)
    df["trace_id"] = df["trace_id"].fillna("").astype(str)
    df["service"] = df["service"].fillna("").astype(str)
    df["host"] = df["host"].fillna("").astype(str)
    df["level"] = df["level"].astype(str)
    df["message"] = df["message"].astype(str)

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count {nrows} outside target [20000, 100000]")


if __name__ == "__main__":
    main()
