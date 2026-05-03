import math
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "id": "gitlab_consul_tls_expiry_maintenance",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["gitlab_frontend"], "trace_id_len": 32},
    "components": [
        {
            "id": "gitlab_frontend",
            "svc": "gitlab-frontend",
            "hosts": ["web-01", "web-02", "web-03", "web-04"],
            "logs": {
                "http_access_ok": {
                    "lvl": "INFO",
                    "msg": "req {method} {route} status=200 dur_ms={dur_ms} req_id={req_id} actor={actor}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/users/sign_in", "/api/v4/projects", "/api/v4/jobs/request"]},
                        "dur_ms": {"k": "i", "v": [20, 800]},
                        "req_id": {"k": "hex", "v": 16},
                        "actor": {"k": "ch", "v": ["anon", "auth_user", "ci_runner"]},
                    },
                },
                "http_access_500": {
                    "lvl": "ERROR",
                    "msg": "req {method} {route} status=500 dur_ms={dur_ms} req_id={req_id} err={err}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/users/sign_in", "/api/v4/projects", "/api/v4/jobs/request"]},
                        "dur_ms": {"k": "i", "v": [200, 4000]},
                        "req_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["db_connect_failed", "db_timeout", "service_discovery_error"]},
                    },
                },
                "worker_stats": {
                    "lvl": "INFO",
                    "msg": "workers busy={busy} queued={queued} db_pool_wait_ms_p95={db_wait_p95}",
                    "vars": {
                        "busy": {"k": "i", "v": [10, 80]},
                        "queued": {"k": "i", "v": [0, 50]},
                        "db_wait_p95": {"k": "i", "v": [0, 250]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_stats", "per_min": 0.5}]},
                "f": {"emit": [{"id": "worker_stats", "per_min": 0.5}]},
            },
        },
        {
            "id": "pgbouncer_pool",
            "svc": "pgbouncer",
            "hosts": ["pgb-01", "pgb-02", "pgb-03"],
            "logs": {
                "txn_ok": {
                    "lvl": "INFO",
                    "msg": "txn ok db={db} backend={backend} dur_ms={dur_ms} client={client}",
                    "vars": {
                        "db": {"k": "ch", "v": ["gitlabhq_production"]},
                        "backend": {"k": "ch", "v": ["db-01", "db-02", "db-03"]},
                        "dur_ms": {"k": "i", "v": [5, 200]},
                        "client": {"k": "ch", "v": ["web", "api", "sidekiq"]},
                    },
                },
                "txn_fail": {
                    "lvl": "WARN",
                    "msg": "txn fail db={db} err={err} dur_ms={dur_ms} client={client}",
                    "vars": {
                        "db": {"k": "ch", "v": ["gitlabhq_production"]},
                        "err": {"k": "ch", "v": ["backend_conn_refused", "backend_timeout", "no_primary", "consul_lookup_failed"]},
                        "dur_ms": {"k": "i", "v": [50, 5000]},
                        "client": {"k": "ch", "v": ["web", "api", "sidekiq"]},
                    },
                },
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "stats conns_active={conns} conns_waiting={waiting} max_client_conn={max}",
                    "vars": {
                        "conns": {"k": "i", "v": [50, 500]},
                        "waiting": {"k": "i", "v": [0, 80]},
                        "max": {"k": "i", "v": [500, 500]},
                    },
                },
                "consul_lock_ok": {
                    "lvl": "INFO",
                    "msg": "healthcheck ok lock={lock} consul={consul} dur_ms={dur_ms}",
                    "vars": {
                        "lock": {"k": "ch", "v": ["pgbouncer/primary"]},
                        "consul": {"k": "ch", "v": ["consul.service"]},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                    },
                },
                "consul_lock_fail": {
                    "lvl": "WARN",
                    "msg": "healthcheck fail lock={lock} err={err} next_retry_ms={next_ms}",
                    "vars": {
                        "lock": {"k": "ch", "v": ["pgbouncer/primary"]},
                        "err": {"k": "ch", "v": ["connection_refused", "no_leader", "tls_handshake_failed"]},
                        "next_ms": {"k": "i", "v": [200, 3000]},
                    },
                },
                "healthcheck_giveup": {
                    "lvl": "ERROR",
                    "msg": "healthcheck disabled after {failures} failures; last_err={err}",
                    "vars": {
                        "failures": {"k": "i", "v": [3, 8]},
                        "err": {"k": "ch", "v": ["no_leader", "connection_refused", "tls_handshake_failed"]},
                    },
                },
                "manual_healthcheck_restart": {
                    "lvl": "INFO",
                    "msg": "healthcheck process restarted; mode={mode}",
                    "vars": {"mode": {"k": "ch", "v": ["manual"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_stats", "per_min": 1.0}, {"id": "consul_lock_ok", "per_min": 2.0}]},
                "f": {"emit": [{"id": "pool_stats", "per_min": 1.0}, {"id": "consul_lock_ok", "per_min": 2.0}]},
            },
        },
        {
            "id": "postgres_db",
            "svc": "postgres",
            "hosts": ["db-01", "db-02", "db-03"],
            "logs": {
                "pg_stat": {
                    "lvl": "INFO",
                    "msg": "pgstat conns={conns} xact_commits={commits} repl_lag_s={lag_s}",
                    "vars": {
                        "conns": {"k": "i", "v": [200, 950]},
                        "commits": {"k": "i", "v": [5000, 28000]},
                        "lag_s": {"k": "f", "v": [0.0, 2.0]},
                    },
                },
                "too_many_connections_warn": {
                    "lvl": "WARN",
                    "msg": "connection limit reached conns={conns} max={max}",
                    "vars": {
                        "conns": {"k": "i", "v": [900, 1300]},
                        "max": {"k": "i", "v": [1000, 1000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pg_stat", "per_min": 1.0}, {"id": "too_many_connections_warn", "per_min": 0.05}]},
                "f": {"emit": [{"id": "pg_stat", "per_min": 1.0}, {"id": "too_many_connections_warn", "per_min": 0.2}]},
            },
        },
        {
            "id": "patroni_ha",
            "svc": "patroni",
            "hosts": ["db-01", "db-02", "db-03"],
            "logs": {
                "patroni_loop_ok": {
                    "lvl": "INFO",
                    "msg": "patroni loop role={role} leader={leader} consul={consul_state}",
                    "vars": {
                        "role": {"k": "ch", "v": ["leader", "replica"]},
                        "leader": {"k": "ch", "v": ["db-01", "db-02", "db-03"]},
                        "consul_state": {"k": "ch", "v": ["reachable"]},
                    },
                },
                "patroni_consul_error": {
                    "lvl": "ERROR",
                    "msg": "consul session error err={err} waited_ms={waited_ms}",
                    "vars": {
                        "err": {"k": "ch", "v": ["x509_expired", "connection_refused", "no_leader", "timeout"]},
                        "waited_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
                "patroni_paused": {
                    "lvl": "WARN",
                    "msg": "patroni pause requested by {who}",
                    "vars": {"who": {"k": "ch", "v": ["infra_maint"]}},
                },
                "patroni_resumed": {
                    "lvl": "INFO",
                    "msg": "patroni resume requested by {who}",
                    "vars": {"who": {"k": "ch", "v": ["infra_maint"]}},
                },
                "patroni_sync_complete": {
                    "lvl": "INFO",
                    "msg": "consul session re-established; synced={synced}",
                    "vars": {"synced": {"k": "ch", "v": ["true"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "patroni_loop_ok", "per_min": 6.0, "scope": "global"}]},
                "f": {"emit": [{"id": "patroni_loop_ok", "per_min": 6.0, "scope": "global"}]},
            },
        },
        {
            "id": "consul_cluster",
            "svc": "consul",
            "hosts": ["consul-01", "consul-02", "consul-03", "consul-04", "consul-05"],
            "logs": {
                "tls_handshake_error": {
                    "lvl": "WARN",
                    "msg": "TLS handshake error from {peer} err=\"{err}\"",
                    "vars": {
                        "peer": {"k": "ch", "v": ["db-01", "db-02", "db-03", "pgb-01", "pgb-02", "pgb-03", "web-01", "web-02"]},
                        "err": {"k": "ch", "v": ["x509: certificate has expired or is not yet valid"]},
                    },
                },
                "http_api_error_500": {
                    "lvl": "ERROR",
                    "msg": "HTTP {method} {endpoint} returned 500 detail={detail}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "PUT"]},
                        "endpoint": {"k": "ch", "v": ["/v1/agent/health/service/name/pgbouncer", "/v1/kv/patroni/leader"]},
                        "detail": {"k": "ch", "v": ["connection refused", "transport shutdown"]},
                    },
                },
                "raft_no_leader": {
                    "lvl": "WARN",
                    "msg": "raft: no leader found term={term}",
                    "vars": {"term": {"k": "i", "v": [100, 130]}},
                },
                "raft_peers": {
                    "lvl": "INFO",
                    "msg": "raft peers leader={leader} voters={voters} healthy={healthy}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["consul-01", "consul-02", "consul-03", "consul-04", "consul-05", "none"]},
                        "voters": {"k": "i", "v": [3, 5]},
                        "healthy": {"k": "i", "v": [0, 5]},
                    },
                },
                "raft_leader_elected": {
                    "lvl": "INFO",
                    "msg": "raft: elected leader {leader} term={term}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["consul-01", "consul-02", "consul-03", "consul-04", "consul-05"]},
                        "term": {"k": "i", "v": [100, 130]},
                    },
                },
                "agent_restart": {
                    "lvl": "INFO",
                    "msg": "consul agent restarted reason={reason} verify_date={verify_date}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["scheduled_maintenance"]},
                        "verify_date": {"k": "ch", "v": ["false"]},
                    },
                },
                "runtime_tls_config": {
                    "lvl": "WARN",
                    "msg": "tls config verify_date={verify_date} verify_incoming={verify_incoming} verify_outgoing={verify_outgoing}",
                    "vars": {
                        "verify_date": {"k": "ch", "v": ["false"]},
                        "verify_incoming": {"k": "ch", "v": ["true"]},
                        "verify_outgoing": {"k": "ch", "v": ["true"]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "tls_handshake_error", "per_min": 0.0, "scope": "global"},
                        {"id": "raft_peers", "per_min": 0.2, "scope": "global"},
                        {"id": "http_api_error_500", "per_min": 0.0, "scope": "global"},
                        {"id": "raft_no_leader", "per_min": 0.0, "scope": "global"},
                        {"id": "runtime_tls_config", "per_min": 0.0, "scope": "global"},
                    ]
                },
                "f": {"emit": [{"id": "tls_handshake_error", "per_min": 8.0, "scope": "global"}]},
            },
        },
        {
            "id": "ops_audit",
            "svc": "ops-audit",
            "hosts": ["ops-01"],
            "logs": {
                "maintenance_start": {
                    "lvl": "INFO",
                    "msg": "maintenance start change_id={change_id} window_utc={window}",
                    "vars": {"change_id": {"k": "hex", "v": 8}, "window": {"k": "ch", "v": ["02:10-02:40"]}},
                },
                "config_staged": {
                    "lvl": "INFO",
                    "msg": "staged consul config verify_date=false on {nodes} nodes; chef_client={chef_state}",
                    "vars": {"nodes": {"k": "i", "v": [240, 260]}, "chef_state": {"k": "ch", "v": ["stopped"]}},
                },
                "at_scheduled": {
                    "lvl": "INFO",
                    "msg": "scheduled at job cmd=\"{cmd}\" when_utc={when_utc} nodes={nodes}",
                    "vars": {
                        "cmd": {"k": "ch", "v": ["sudo systemctl restart consul.service"]},
                        "when_utc": {"k": "ch", "v": ["02:20"]},
                        "nodes": {"k": "i", "v": [240, 260]},
                    },
                },
                "patroni_pause_recorded": {
                    "lvl": "WARN",
                    "msg": "recorded patroni pause for change_id={change_id}",
                    "vars": {"change_id": {"k": "hex", "v": 8}},
                },
                "patroni_resume_recorded": {
                    "lvl": "INFO",
                    "msg": "recorded patroni resume for change_id={change_id}",
                    "vars": {"change_id": {"k": "hex", "v": 8}},
                },
                "pgbouncer_node_drained": {
                    "lvl": "INFO",
                    "msg": "drained pgbouncer node {node} from load balancer pool",
                    "vars": {"node": {"k": "ch", "v": ["pgb-03"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": [
            {
                "id": "user_http_ok_n",
                "rpm": 800.0,
                "emit": ["pgbouncer_pool.txn_ok", "gitlab_frontend.http_access_ok"],
                "latency_ms": [[15, 90], [120, 450]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "user_http_500_n",
                "rpm": 0.2,
                "emit": ["pgbouncer_pool.txn_fail", "gitlab_frontend.http_access_500"],
                "latency_ms": [[200, 1400], [800, 3000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "user_http_ok_f",
                "rpm": 800.0,
                "emit": ["pgbouncer_pool.txn_ok", "gitlab_frontend.http_access_ok"],
                "latency_ms": [[20, 140], [140, 650]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "user_http_500_f",
                "rpm": 0.2,
                "emit": ["pgbouncer_pool.txn_fail", "gitlab_frontend.http_access_500"],
                "latency_ms": [[200, 1400], [800, 3000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "consul_raft_peers_probe_f",
                "rpm": 6.0,
                "emit": ["consul_cluster.raft_peers"],
                "latency_ms": [[10, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "consul_http_500_probe_f",
                "rpm": 30.0,
                "emit": ["consul_cluster.http_api_error_500"],
                "latency_ms": [[5, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "consul_raft_no_leader_probe_f",
                "rpm": 12.0,
                "emit": ["consul_cluster.raft_no_leader"],
                "latency_ms": [[5, 30]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "consul_runtime_tls_config_poll_f",
                "rpm": 1.0,
                "emit": ["consul_cluster.runtime_tls_config"],
                "latency_ms": [[5, 25]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "pgbouncer_consul_lock_fail_probe_f",
                "rpm": 3.0,
                "emit": ["pgbouncer_pool.consul_lock_fail"],
                "latency_ms": [[30, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "patroni_consul_session_error_probe_f",
                "rpm": 2.0,
                "emit": ["patroni_ha.patroni_consul_error"],
                "latency_ms": [[50, 800]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "consul_tls_expiry_synchronized_restart",
    "time": {
        "total_minutes": 40,
        "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "consul_raft_peers_probe_f": 0.0,
                        "consul_http_500_probe_f": 0.0,
                        "consul_raft_no_leader_probe_f": 0.0,
                        "consul_runtime_tls_config_poll_f": 0.0,
                        "pgbouncer_consul_lock_fail_probe_f": 0.0,
                        "patroni_consul_session_error_probe_f": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "ops_audit.maintenance_start", "count": 1, "hosts": ["ops-01"]}],
                },
                {
                    "order": 2,
                    "at_min": 24,
                    "rate_multipliers": {"consul_raft_peers_probe_f": 1.0},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "patroni_ha.patroni_paused", "count": 1, "hosts": ["db-01"]},
                        {"ref": "ops_audit.patroni_pause_recorded", "count": 1, "hosts": ["ops-01"]},
                        {"ref": "ops_audit.config_staged", "count": 1, "hosts": ["ops-01"]},
                        {"ref": "ops_audit.at_scheduled", "count": 1, "hosts": ["ops-01"]},
                        {"ref": "ops_audit.pgbouncer_node_drained", "count": 1, "hosts": ["ops-01"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 30,
                    "rate_multipliers": {
                        "consul_http_500_probe_f": 1.0,
                        "consul_raft_no_leader_probe_f": 1.0,
                        "pgbouncer_consul_lock_fail_probe_f": 2.0,
                        "patroni_consul_session_error_probe_f": 3.0,
                        "consul_cluster.tls_handshake_error": 0.0,
                        "consul_runtime_tls_config_poll_f": 1.0,
                        "pgbouncer_pool.consul_lock_ok": 0.1,
                    },
                    "latency_multipliers": {"user_http_ok_f": {"p50": 1.1, "p95": 1.3}},
                    "one_shots": [
                        {
                            "ref": "consul_cluster.agent_restart",
                            "count": 5,
                            "hosts": ["consul-01", "consul-02", "consul-03", "consul-04", "consul-05"],
                        }
                    ],
                },
                {
                    "order": 4,
                    "at_min": 31,
                    "rate_multipliers": {
                        "consul_http_500_probe_f": 0.0,
                        "consul_raft_no_leader_probe_f": 0.0,
                        "pgbouncer_consul_lock_fail_probe_f": 0.0,
                        "pgbouncer_pool.consul_lock_ok": 0.0,
                        "patroni_consul_session_error_probe_f": 1.5,
                        "consul_runtime_tls_config_poll_f": 1.0,
                    },
                    "latency_multipliers": {"user_http_ok_f": {"p50": 1.02, "p95": 1.1}},
                    "one_shots": [
                        {"ref": "consul_cluster.raft_leader_elected", "count": 1, "hosts": ["consul-04"]},
                        {"ref": "pgbouncer_pool.healthcheck_giveup", "count": 2, "hosts": ["pgb-01", "pgb-02"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 33,
                    "rate_multipliers": {
                        "pgbouncer_consul_lock_fail_probe_f": 0.2,
                        "pgbouncer_pool.consul_lock_ok": 1.0,
                        "patroni_consul_session_error_probe_f": 0.1,
                        "consul_raft_peers_probe_f": 0.2,
                        "consul_runtime_tls_config_poll_f": 1.0,
                    },
                    "latency_multipliers": {"user_http_ok_f": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "patroni_ha.patroni_sync_complete", "count": 1, "hosts": ["db-01"]},
                        {"ref": "patroni_ha.patroni_resumed", "count": 1, "hosts": ["db-01"]},
                        {"ref": "ops_audit.patroni_resume_recorded", "count": 1, "hosts": ["ops-01"]},
                        {"ref": "pgbouncer_pool.manual_healthcheck_restart", "count": 2, "hosts": ["pgb-01", "pgb-02"]},
                    ],
                },
            ]
        }
    },
}

# -----------------------------
# Deterministic helpers
# -----------------------------
SEED = 1337
random.seed(SEED)  # required for reproducibility checks even if random isn't heavily used
rng = np.random.RandomState(SEED)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def clamp_dt(dt: datetime, start: datetime, end: datetime) -> datetime:
    # Clamp to [start, end), leaving 1ms margin if needed
    if dt < start:
        return start
    if dt >= end:
        # end exclusive; place at end - 1ms
        return end - timedelta(milliseconds=1)
    return dt


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def stable_count(expected: float) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 0:
        return n
    return n + (1 if rng.rand() < frac else 0)


def schedule_times(start: datetime, end: datetime, count: int, key: str, jitter_ms: int = 200) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end - start).total_seconds()
    step = dur_s / count
    out = []
    for i in range(count):
        base = start + timedelta(seconds=(i + 0.5) * step)
        j = int(rng.randint(-jitter_ms, jitter_ms + 1))
        dt = base + timedelta(milliseconds=j)
        out.append(clamp_dt(dt, start, end))
    return out


def pick_host(comp: Dict[str, Any], salt: str) -> str:
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    idx = int(md5_hex(salt)[:8], 16) % len(hosts)
    return hosts[idx]


def gen_from_domain(dom: Dict[str, Any], salt: Optional[str] = None) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        if salt is None:
            return v[int(rng.randint(0, len(v)))]
        idx = int(md5_hex(salt)[:8], 16) % len(v)
        if len(v) > 1:
            idx = (idx + int(rng.randint(0, len(v)))) % len(v)
        return v[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(rng.uniform(lo, hi))
    if k == "hex":
        length = int(v)
        if salt is None:
            return md5_hex(rng.bytes(16).hex())[:length]
        return md5_hex(salt)[:length]
    if k == "uuid":
        h = md5_hex(salt or rng.bytes(16).hex())
        return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-{h[16:20]}-{h[20:32]}"
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v)
    return ""


def sample_delay_ms(p50: float, p95: float, q_max: float) -> float:
    u = float(rng.rand())
    q = (u * u) * q_max
    return max(1.0, p50 + q * (p95 - p50))


# -----------------------------
# Build indices
# -----------------------------
COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOG_TPL: Dict[str, Dict[str, Any]] = {}
for comp_id, comp in COMP.items():
    for log_id, tpl in comp["logs"].items():
        LOG_TPL[f"{comp_id}.{log_id}"] = tpl

FLOWS: Dict[str, Dict[str, Any]] = {}
for st in ("n", "f"):
    for f in SYSTEM["flows"][st]:
        FLOWS[f"{st}.{f['id']}"] = f


def emit_log(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    dt: datetime,
    comp_id: str,
    log_id: str,
    msg_vars: Dict[str, Any],
    trace_id: str,
) -> None:
    comp = COMP[comp_id]
    tpl = comp["logs"][log_id]
    level = tpl["lvl"]
    for k, vv in list(msg_vars.items()):
        if isinstance(vv, float):
            msg_vars[k] = f"{vv:.2f}".rstrip("0").rstrip(".") if "." in f"{vv:.2f}" else f"{vv:.2f}"
    message = tpl["msg"].format(**msg_vars)
    service = comp.get("svc") or ""
    host = msg_vars.pop("_host", None)
    if host is None:
        host = ""
    rows.append((dt, level, message, trace_id, service, host))


# -----------------------------
# Failure control timeline derivation
# -----------------------------
def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start <= e["at_min"] < f_end}) + [f_end]

    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(int(e["at_min"]), []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        cur_min = int(boundaries[i])
        nxt_min = int(boundaries[i + 1])

        for e in ev_by_min.get(cur_min, []):
            for k, v in e.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for k, v in e.get("latency_multipliers", {}).items():
                lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}

        intervals.append({"start_min": cur_min, "end_min": nxt_min, "rate_mult": dict(rate_mult), "lat_mult": dict(lat_mult)})
    return intervals


FAIL_INTERVALS = build_failure_intervals()

# -----------------------------
# Simulation: background + flows + one-shots
# -----------------------------
BASE_TIME = datetime(2026, 1, 1, 2, 0, 0, tzinfo=timezone.utc)


def minute_to_dt(m: int) -> datetime:
    return BASE_TIME + timedelta(minutes=int(m))


def gen_background(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    rate_mult: Optional[Dict[str, float]] = None,
) -> None:
    rate_mult = rate_mult or {}
    duration_min = (end_dt - start_dt).total_seconds() / 60.0

    comp_ids = sorted(COMP.keys())
    for comp_id in comp_ids:
        comp = COMP[comp_id]
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src.get("per_min", 0.0))
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            mult = float(rate_mult.get(mult_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            tpl = comp["logs"][log_id]
            if scope == "global":
                expected = eff_per_min * duration_min
                count = stable_count(expected)
                times = schedule_times(start_dt, end_dt, count, key=f"bg:{state}:{mult_key}")
                hosts = comp.get("hosts") or []
                for j, dt in enumerate(times):
                    msg_vars = {vn: gen_from_domain(dom, salt=f"{mult_key}:{vn}:{state}:{j}") for vn, dom in tpl.get("vars", {}).items()}
                    host = hosts[j % len(hosts)] if hosts else ""
                    msg_vars["_host"] = host
                    emit_log(rows, dt, comp_id, log_id, msg_vars, trace_id="")
            else:
                hosts = comp.get("hosts") or []
                for host in hosts:
                    expected = eff_per_min * duration_min
                    count = stable_count(expected)
                    times = schedule_times(start_dt, end_dt, count, key=f"bg:{state}:{mult_key}:{host}")
                    for j, dt in enumerate(times):
                        msg_vars = {vn: gen_from_domain(dom, salt=f"{mult_key}:{vn}:{state}:{host}:{j}") for vn, dom in tpl.get("vars", {}).items()}
                        msg_vars["_host"] = host
                        emit_log(rows, dt, comp_id, log_id, msg_vars, trace_id="")


def bind_user_request_context(flow_id: str, seq: int) -> Dict[str, Any]:
    u = float(rng.rand())
    if u < 0.45:
        route = "/"
    elif u < 0.65:
        route = "/users/sign_in"
    elif u < 0.88:
        route = "/api/v4/projects"
    else:
        route = "/api/v4/jobs/request"

    method = "GET" if route in ("/", "/api/v4/projects") else "POST"
    actor = "anon"
    if route == "/users/sign_in":
        actor = "anon" if rng.rand() < 0.6 else "auth_user"
    elif route == "/api/v4/projects":
        actor = "auth_user"
    elif route == "/api/v4/jobs/request":
        actor = "ci_runner"

    if route.startswith("/api/v4/") and route != "/api/v4/jobs/request":
        client = "api"
    elif route == "/api/v4/jobs/request":
        client = "sidekiq"
    else:
        client = "web"

    req_id = md5_hex(f"{flow_id}:req:{seq}")[:16]
    return {"route": route, "method": method, "actor": actor, "client": client, "req_id": req_id}


def simulate_flow_instance(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    state: str,
    flow: Dict[str, Any],
    start_dt: datetime,
    rate_mult: Optional[Dict[str, float]] = None,
    lat_mult: Optional[Dict[str, Dict[str, float]]] = None,
    seq: int = 0,
) -> None:
    rate_mult = rate_mult or {}
    lat_mult = lat_mult or {}

    trace_id = ""
    if SYSTEM["tracing"]["on"] and bool(flow.get("trace", False)):
        trace_id = md5_hex(f"trace:{state}:{flow['id']}:{seq}")[: SYSTEM["tracing"]["trace_id_len"]]

    lm = lat_mult.get(flow["id"], {"p50": 1.0, "p95": 1.0})
    lm50 = float(lm.get("p50", 1.0))
    lm95 = float(lm.get("p95", 1.0))

    host_for_comp: Dict[str, str] = {}
    for ref in flow["emit"]:
        comp_id, _log_id = ref.split(".", 1)
        if comp_id not in host_for_comp:
            host_for_comp[comp_id] = pick_host(
                COMP[comp_id],
                salt=f"{trace_id}:{flow['id']}:{seq}:{comp_id}" if trace_id else f"{flow['id']}:{seq}:{comp_id}",
            )

    req_ctx: Dict[str, Any] = {}
    if flow["id"].startswith("user_http_"):
        req_ctx = bind_user_request_context(flow["id"], seq)

    q_max = 1.0
    if flow["id"].startswith("user_http_ok_"):
        q_max = 0.7
    elif flow["id"].startswith("user_http_500_"):
        q_max = 0.85

    current = start_dt
    delays: List[float] = []
    for pair in flow["latency_ms"]:
        p50, p95 = float(pair[0]) * lm50, float(pair[1]) * lm95
        delays.append(sample_delay_ms(p50, p95, q_max=q_max))

    frontend_err = None
    pgb_err = None
    if flow["id"].startswith("user_http_500_"):
        r = float(rng.rand())
        if r < 0.45:
            frontend_err = "db_timeout"
            pgb_err = "backend_timeout"
        elif r < 0.75:
            frontend_err = "service_discovery_error"
            pgb_err = "consul_lookup_failed"
        else:
            frontend_err = "db_connect_failed"
            pgb_err = "backend_conn_refused"

    for li, ref in enumerate(flow["emit"]):
        comp_id, log_id = ref.split(".", 1)
        tpl = LOG_TPL[ref]

        current = current + timedelta(milliseconds=delays[li])

        msg_vars: Dict[str, Any] = {vn: gen_from_domain(dom, salt=f"{state}:{flow['id']}:{seq}:{ref}:{vn}:{li}") for vn, dom in tpl.get("vars", {}).items()}
        msg_vars["_host"] = host_for_comp.get(comp_id, "")

        if ref == "pgbouncer_pool.txn_ok":
            msg_vars["db"] = "gitlabhq_production"
            msg_vars["backend"] = gen_from_domain(COMP["pgbouncer_pool"]["logs"]["txn_ok"]["vars"]["backend"], salt=f"backend:{seq}:{trace_id}")
            if req_ctx:
                msg_vars["client"] = req_ctx["client"]
            msg_vars["dur_ms"] = int(round(delays[li]))

        if ref == "gitlab_frontend.http_access_ok":
            if req_ctx:
                msg_vars["method"] = req_ctx["method"]
                msg_vars["route"] = req_ctx["route"]
                msg_vars["actor"] = req_ctx["actor"]
                msg_vars["req_id"] = req_ctx["req_id"]
            total_ms = int(round((current - start_dt).total_seconds() * 1000.0))
            msg_vars["dur_ms"] = max(20, min(800, total_ms)) if flow["id"].startswith("user_http_ok_") else total_ms

        if ref == "pgbouncer_pool.txn_fail":
            msg_vars["db"] = "gitlabhq_production"
            if req_ctx:
                msg_vars["client"] = req_ctx["client"]
            msg_vars["err"] = pgb_err or msg_vars["err"]
            msg_vars["dur_ms"] = int(round(delays[li]))

        if ref == "gitlab_frontend.http_access_500":
            if req_ctx:
                msg_vars["method"] = req_ctx["method"]
                msg_vars["route"] = req_ctx["route"]
                msg_vars["req_id"] = req_ctx["req_id"]
            msg_vars["err"] = frontend_err or msg_vars["err"]
            total_ms = int(round((current - start_dt).total_seconds() * 1000.0))
            msg_vars["dur_ms"] = max(200, min(4000, total_ms))

        if ref == "pgbouncer_pool.consul_lock_fail":
            minute = int((start_dt - BASE_TIME).total_seconds() // 60)
            if 30 <= minute < 31:
                msg_vars["err"] = "no_leader"
            elif minute >= 33:
                msg_vars["err"] = "connection_refused" if rng.rand() < 0.5 else "tls_handshake_failed"
            else:
                msg_vars["err"] = "tls_handshake_failed"
            msg_vars["next_ms"] = int(gen_from_domain(COMP["pgbouncer_pool"]["logs"]["consul_lock_fail"]["vars"]["next_ms"], salt=f"next:{state}:{flow['id']}:{seq}"))

        if ref == "patroni_ha.patroni_consul_error":
            minute = int((start_dt - BASE_TIME).total_seconds() // 60)
            if 30 <= minute < 31:
                msg_vars["err"] = "no_leader"
            elif 31 <= minute < 33:
                msg_vars["err"] = "timeout"
            else:
                msg_vars["err"] = "connection_refused" if rng.rand() < 0.4 else "timeout"
            msg_vars["waited_ms"] = int(round(delays[li]))

        if ref == "consul_cluster.raft_peers":
            msg_vars["leader"] = "consul-04" if rng.rand() < 0.6 else gen_from_domain(COMP["consul_cluster"]["logs"]["raft_peers"]["vars"]["leader"], salt=f"leader:{seq}")
            msg_vars["voters"] = 5
            minute = int((start_dt - BASE_TIME).total_seconds() // 60)
            msg_vars["healthy"] = 4 if minute >= 33 and rng.rand() < 0.3 else 5
            if msg_vars["leader"] == "none":
                msg_vars["leader"] = "consul-04"

        if ref == "consul_cluster.raft_no_leader":
            msg_vars["term"] = int(115 + (seq % 10))

        if ref == "consul_cluster.http_api_error_500":
            msg_vars["method"] = "GET" if rng.rand() < 0.8 else "PUT"
            msg_vars["endpoint"] = "/v1/kv/patroni/leader" if rng.rand() < 0.55 else "/v1/agent/health/service/name/pgbouncer"
            msg_vars["detail"] = "transport shutdown" if rng.rand() < 0.6 else "connection refused"

        emit_log(rows, current, comp_id, log_id, msg_vars, trace_id=trace_id)


def gen_flows_normal(rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    start_min = SCENARIO["time"]["phases"]["n"]["start_min"]
    end_min = SCENARIO["time"]["phases"]["n"]["end_min"]
    start_dt, end_dt = minute_to_dt(start_min), minute_to_dt(end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0

    flow_seq: Dict[str, int] = {}
    for flow in SYSTEM["flows"]["n"]:
        expected = float(flow["rpm"]) * duration_min
        count = stable_count(expected)
        times = schedule_times(start_dt, end_dt, count, key=f"flow:n:{flow['id']}", jitter_ms=350)
        flow_seq.setdefault(flow["id"], 0)
        for dt in times:
            s = flow_seq[flow["id"]]
            flow_seq[flow["id"]] += 1
            simulate_flow_instance(rows, "n", flow, start_dt=dt, seq=s)


def gen_flows_failure(rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    flow_seq: Dict[str, int] = {f["id"]: 0 for f in SYSTEM["flows"]["f"]}

    for interval in FAIL_INTERVALS:
        start_dt = minute_to_dt(interval["start_min"])
        end_dt = minute_to_dt(interval["end_min"])
        duration_min = (end_dt - start_dt).total_seconds() / 60.0
        rate_mult = interval["rate_mult"]
        lat_mult = interval["lat_mult"]

        for flow in SYSTEM["flows"]["f"]:
            mult = float(rate_mult.get(flow["id"], 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            if rpm_eff <= 0:
                continue
            expected = rpm_eff * duration_min
            count = stable_count(expected)
            times = schedule_times(start_dt, end_dt, count, key=f"flow:f:{flow['id']}:{interval['start_min']}", jitter_ms=350)
            for dt in times:
                s = flow_seq[flow["id"]]
                flow_seq[flow["id"]] += 1
                simulate_flow_instance(rows, "f", flow, start_dt=dt, rate_mult=rate_mult, lat_mult=lat_mult, seq=s)


def gen_one_shots(rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        event_dt = minute_to_dt(int(e["at_min"]))
        for os in (e.get("one_shots", []) or []):
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            comp = COMP[comp_id]
            tpl = comp["logs"][log_id]
            count = int(os.get("count", 1))
            allowed_hosts = os.get("hosts", None)

            spread_ms = 1500 if count > 1 else 200
            for i in range(count):
                dt = event_dt + timedelta(milliseconds=int((i + 1) * (spread_ms / (count + 1))))
                dt = dt + timedelta(milliseconds=int(rng.randint(0, 25)))
                msg_vars: Dict[str, Any] = {vn: gen_from_domain(dom, salt=f"oneshot:{e['order']}:{e['at_min']}:{ref}:{vn}") for vn, dom in tpl.get("vars", {}).items()}
                if allowed_hosts is not None:
                    host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else ""
                else:
                    host = pick_host(comp, salt=f"oneshot:{e['order']}:{ref}:{i}")
                msg_vars["_host"] = host
                emit_log(rows, dt, comp_id, log_id, msg_vars, trace_id="")


def main() -> None:
    rows: List[Tuple[datetime, str, str, str, str, str]] = []

    n_start = minute_to_dt(SCENARIO["time"]["phases"]["n"]["start_min"])
    n_end = minute_to_dt(SCENARIO["time"]["phases"]["n"]["end_min"])
    gen_background(rows, "n", n_start, n_end, rate_mult=None)
    gen_flows_normal(rows)

    for interval in FAIL_INTERVALS:
        f_start = minute_to_dt(interval["start_min"])
        f_end = minute_to_dt(interval["end_min"])
        gen_background(rows, "f", f_start, f_end, rate_mult=interval["rate_mult"])

    gen_flows_failure(rows)
    gen_one_shots(rows)

    rows.sort(key=lambda x: x[0])

    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r[0]) for r in rows],
            "level": [r[1] for r in rows],
            "message": [r[2] for r in rows],
            "trace_id": [r[3] for r in rows],
            "service": [r[4] for r in rows],
            "host": [r[5] for r in rows],
        },
        columns=["timestamp", "level", "message", "trace_id", "service", "host"],
    )
    df.to_csv("logs.csv", index=False)

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Log volume {len(df)} outside required envelope [20000, 100000].")
    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV columns incorrect.")


if __name__ == "__main__":
    main()
