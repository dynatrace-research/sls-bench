import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Deterministic seeding (simulator primarily uses hashing; seed still set for safety)
random.seed(0)
np.random.seed(0)

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "gitlab_com_db_primary_wipe_20170131",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["gitlab_web"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "gitlab_web": {
            "svc": "gitlab-web",
            "hosts": ["web-01.gitlab.com", "web-02.gitlab.com"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "Started {method} {route} for {client_ip} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/projects", "/api/v4/projects", "/api/v4/notes"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "req_done_200": {
                    "lvl": "INFO",
                    "msg": "Completed 200 in {duration_ms}ms req_id={req_id} db_ms={db_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {"duration_ms": {"k": "i", "v": [30, 900]}, "db_ms": {"k": "i", "v": [10, 700]}},
                        "f": {
                            "duration_ms": {"k": "i", "v": [80, 12000]},
                            "db_ms": {"k": "i", "v": [40, 11000]},
                        },
                    },
                },
                "req_done_500": {
                    "lvl": "INFO",
                    "msg": "Completed {status} in {duration_ms}ms req_id={req_id}",
                    "vars": {"status": {"k": "ch", "v": ["500", "503"]}, "req_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {"duration_ms": {"k": "i", "v": [80, 2000]}},
                        "f": {"duration_ms": {"k": "i", "v": [200, 20000]}},
                    },
                },
                "db_error": {
                    "lvl": "ERROR",
                    "msg": "DB error req_id={req_id} error={error}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "error": {
                            "k": "ch",
                            "v": [
                                "connection refused",
                                "could not connect to server",
                                "statement timeout",
                                "unexpected EOF on client connection",
                            ],
                        },
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "sidekiq_worker": {
            "svc": "gitlab-sidekiq",
            "hosts": ["worker-01.gitlab.com", "worker-02.gitlab.com"],
            "logs": {
                "job_start": {
                    "lvl": "INFO",
                    "msg": "Job started job={job} jid={jid}",
                    "vars": {
                        "job": {"k": "ch", "v": ["UserDestroyJob", "AbuseReportProcessJob", "ProjectCacheWarmJob"]},
                        "jid": {"k": "hex", "v": 16},
                    },
                },
                "job_done": {
                    "lvl": "INFO",
                    "msg": "Job done job={job} jid={jid} status={status} duration_ms={duration_ms}",
                    "vars": {
                        "job": {"k": "ch", "v": ["UserDestroyJob", "AbuseReportProcessJob", "ProjectCacheWarmJob"]},
                        "jid": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": ["ok", "error"]},
                    },
                    "state_vars": {
                        "n": {"duration_ms": {"k": "i", "v": [200, 5000]}},
                        "f": {"duration_ms": {"k": "i", "v": [500, 90000]}},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "postgres_primary": {
            "svc": "postgres",
            "hosts": ["db1.cluster.gitlab.com"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete write={write_mb}MB sync={sync_mb}MB",
                    "vars": {"write_mb": {"k": "i", "v": [50, 800]}, "sync_mb": {"k": "i", "v": [10, 300]}},
                },
                "wal_removed": {
                    "lvl": "WARN",
                    "msg": "removed WAL segment {wal_segment} before standby replay",
                    "vars": {"wal_segment": {"k": "hex", "v": 24}},
                },
                "startup_panic": {
                    "lvl": "CRITICAL",
                    "msg": "could not open file \"{file}\" (No such file or directory)",
                    "vars": {
                        "file": {
                            "k": "ch",
                            "v": [
                                "/var/opt/gitlab/postgresql/data/global/pg_control",
                                "/var/opt/gitlab/postgresql/data/base/16384/2609",
                            ],
                        }
                    },
                },
                "config_apply_raise_wal": {
                    "lvl": "INFO",
                    "msg": "restarting postgres apply max_wal_senders=32 max_connections=8000",
                    "vars": {},
                },
                "restart_failed_semaphores": {
                    "lvl": "ERROR",
                    "msg": "could not create semaphores: exceeded system limit",
                    "vars": {},
                },
                "config_apply_reduce_conn": {
                    "lvl": "INFO",
                    "msg": "restarting postgres apply max_wal_senders=32 max_connections=2000",
                    "vars": {},
                },
                "server_ready": {"lvl": "INFO", "msg": "database system is ready to accept connections", "vars": {}},
                "repl_conn_limit": {
                    "lvl": "ERROR",
                    "msg": "replication connection refused: too many wal senders",
                    "vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.25, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "checkpoint_complete", "per_min": 0.25, "scope": "per_host"},
                        {"id": "wal_removed", "per_min": 1.5, "scope": "per_host"},
                        {"id": "startup_panic", "per_min": 5.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "postgres_standby": {
            "svc": "postgres",
            "hosts": ["db2.cluster.gitlab.com"],
            "logs": {
                "repl_status": {
                    "lvl": "INFO",
                    "msg": "streaming from {primary_host} replay_lag={lag_s}s",
                    "vars": {"primary_host": {"k": "ch", "v": ["db1.cluster.gitlab.com"]}},
                    "state_vars": {
                        "n": {"lag_s": {"k": "i", "v": [0, 5]}},
                        "f": {"lag_s": {"k": "i", "v": [60, 20000]}},
                    },
                },
                "wal_missing": {
                    "lvl": "ERROR",
                    "msg": "requested WAL segment {wal_segment} has already been removed",
                    "vars": {"wal_segment": {"k": "hex", "v": 24}},
                },
                "basebackup_start": {
                    "lvl": "INFO",
                    "msg": "pg_basebackup started from {primary_host} dest={dest}",
                    "vars": {
                        "primary_host": {"k": "ch", "v": ["db1.cluster.gitlab.com"]},
                        "dest": {"k": "ch", "v": ["/var/opt/gitlab/postgresql/data"]},
                    },
                },
                "basebackup_wait": {"lvl": "INFO", "msg": "pg_basebackup: waiting for replication data...", "vars": {}},
                "basebackup_error": {
                    "lvl": "ERROR",
                    "msg": "pg_basebackup failed: {error}",
                    "vars": {
                        "error": {
                            "k": "ch",
                            "v": ["too many replication connections", "timeout while waiting for WAL", "connection reset by peer"],
                        }
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "repl_status", "per_min": 0.8, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "repl_status", "per_min": 0.8, "scope": "per_host"},
                        {"id": "wal_missing", "per_min": 0.7, "scope": "per_host"},
                    ]
                },
            },
        },
        "backup_runner": {
            "svc": "gitlab-backup",
            "hosts": ["app-backup-01.gitlab.com"],
            "logs": {
                "pg_dump_start": {
                    "lvl": "INFO",
                    "msg": "Starting pg_dump backup db_host={db_host} pg_dump_ver={pg_dump_ver}",
                    "vars": {"db_host": {"k": "ch", "v": ["db1.cluster.gitlab.com"]}, "pg_dump_ver": {"k": "ch", "v": ["9.2"]}},
                },
                "pg_dump_error": {
                    "lvl": "ERROR",
                    "msg": "pg_dump failed: server version {server_ver}; pg_dump version {pg_dump_ver}",
                    "vars": {"server_ver": {"k": "ch", "v": ["9.6"]}, "pg_dump_ver": {"k": "ch", "v": ["9.2"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "mail_relay": {
            "svc": "smtp-relay",
            "hosts": ["mail-01.gitlab.com"],
            "logs": {
                "dmarc_reject": {
                    "lvl": "WARN",
                    "msg": "rejected outbound mail from={from_addr} reason=DMARC_POLICY",
                    "vars": {
                        "from_addr": {"k": "ch", "v": ["cron@app-backup-01.gitlab.com", "gitlab-backup@app-backup-01.gitlab.com"]}
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "dmarc_reject", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "dmarc_reject", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        "ops_bastion": {
            "svc": "bastion",
            "hosts": ["bastion-01.gitlab.com"],
            "logs": {
                "cmd_exec": {
                    "lvl": "WARN",
                    "msg": "audit user={user} target={target} cmd=\"{cmd}\"",
                    "vars": {
                        "user": {"k": "ch", "v": ["eng1", "eng2", "eng3"]},
                        "target": {"k": "ch", "v": ["db1.cluster.gitlab.com", "db2.cluster.gitlab.com"]},
                        "cmd": {"k": "ch", "v": ["systemctl restart postgresql", "pg_basebackup --verbose -D /var/opt/gitlab/postgresql/data", "strace -p 12345"]},
                    },
                },
                "cmd_exec_rm_primary": {
                    "lvl": "CRITICAL",
                    "msg": "audit user={user} target=db1.cluster.gitlab.com cmd=\"rm -rf /var/opt/gitlab/postgresql/data\"",
                    "vars": {"user": {"k": "ch", "v": ["eng2"]}},
                },
                "s3_backup_check_empty": {
                    "lvl": "ERROR",
                    "msg": "backup check: no recent pg_dump files found in S3 bucket={bucket}",
                    "vars": {"bucket": {"k": "ch", "v": ["gitlab-com-db-backups"]}},
                },
                "rsync_start": {
                    "lvl": "INFO",
                    "msg": "restore copy started src=staging-db dest=db1 size_gb={size_gb}",
                    "vars": {"size_gb": {"k": "i", "v": [400, 1200]}},
                },
                "rsync_progress": {
                    "lvl": "INFO",
                    "msg": "restore copy progress pct={pct} rate_mbps={rate_mbps}",
                    "vars": {"pct": {"k": "i", "v": [1, 99]}, "rate_mbps": {"k": "i", "v": [40, 80]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": [{"id": "rsync_progress", "per_min": 4.0, "scope": "per_host"}]}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "web_read_ok_n",
                    "rpm": 600.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.req_done_200"],
                    "latency_ms": [[1, 5], [80, 350]],
                    "trace": True,
                },
                {
                    "id": "web_post_comment_ok_n",
                    "rpm": 120.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.req_done_200"],
                    "latency_ms": [[1, 5], [120, 700]],
                    "trace": True,
                },
                {
                    "id": "sidekiq_misc_job_n",
                    "rpm": 8.0,
                    "emit": ["sidekiq_worker.job_start", "sidekiq_worker.job_done"],
                    "latency_ms": [[1, 5], [500, 4000]],
                    "trace": False,
                },
                {
                    "id": "pg_dump_backup_n",
                    "rpm": 0.2,
                    "emit": ["backup_runner.pg_dump_start", "backup_runner.pg_dump_error"],
                    "latency_ms": [[1, 5], [800, 3000]],
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "web_read_ok_f",
                    "rpm": 420.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.req_done_200"],
                    "latency_ms": [[1, 5], [200, 1400]],
                    "trace": True,
                },
                {
                    "id": "web_read_5xx_f",
                    "rpm": 180.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.db_error", "gitlab_web.req_done_500"],
                    "latency_ms": [[1, 5], [50, 400], [300, 5000]],
                    "trace": True,
                },
                {
                    "id": "web_post_comment_ok_f",
                    "rpm": 35.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.req_done_200"],
                    "latency_ms": [[1, 5], [400, 2500]],
                    "trace": True,
                },
                {
                    "id": "web_post_comment_5xx_f",
                    "rpm": 65.0,
                    "emit": ["gitlab_web.req_start", "gitlab_web.db_error", "gitlab_web.req_done_500"],
                    "latency_ms": [[1, 5], [80, 600], [600, 12000]],
                    "trace": True,
                },
                {
                    "id": "sidekiq_user_destroy_f",
                    "rpm": 1.0,
                    "emit": ["sidekiq_worker.job_start", "sidekiq_worker.job_done"],
                    "latency_ms": [[1, 5], [5000, 90000]],
                    "trace": False,
                },
                {
                    "id": "standby_basebackup_attempt_f",
                    "rpm": 0.35,
                    "emit": ["postgres_standby.basebackup_start", "postgres_standby.basebackup_wait", "postgres_standby.basebackup_error"],
                    "latency_ms": [[1, 10], [30000, 240000], [10, 100]],
                    "trace": False,
                },
                {
                    "id": "pg_dump_backup_f",
                    "rpm": 0.2,
                    "emit": ["backup_runner.pg_dump_start", "backup_runner.pg_dump_error"],
                    "latency_ms": [[1, 5], [800, 4000]],
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "gitlab_com_primary_db_data_loss_20170131",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "web_read_ok_f": 1.0,
                        "web_read_5xx_f": 1.2,
                        "web_post_comment_ok_f": 0.9,
                        "web_post_comment_5xx_f": 1.4,
                        "sidekiq_user_destroy_f": 2.0,
                        "standby_basebackup_attempt_f": 0.0,
                        "postgres_primary.wal_removed": 0.0,
                        "postgres_standby.wal_missing": 0.0,
                        "postgres_primary.startup_panic": 0.0,
                        "ops_bastion.rsync_progress": 0.0,
                    },
                    "latency_multipliers": {
                        "web_read_ok_f": {"p50": 2.0, "p95": 2.8},
                        "web_post_comment_ok_f": {"p50": 2.3, "p95": 3.2},
                        "web_post_comment_5xx_f": {"p50": 1.8, "p95": 2.5},
                    },
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 33,
                    "rate_multipliers": {"standby_basebackup_attempt_f": 3.0, "postgres_primary.wal_removed": 1.0, "postgres_standby.wal_missing": 1.0},
                    "latency_multipliers": {"standby_basebackup_attempt_f": {"p50": 1.2, "p95": 1.8}},
                    "one_shots": [{"ref": "ops_bastion.cmd_exec", "count": 2, "hosts": ["bastion-01.gitlab.com"]}],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {
                        "web_read_ok_f": 0.85,
                        "web_read_5xx_f": 1.6,
                        "web_post_comment_ok_f": 0.7,
                        "web_post_comment_5xx_f": 1.9,
                    },
                    "latency_multipliers": {"web_read_5xx_f": {"p50": 1.3, "p95": 1.7}, "web_post_comment_5xx_f": {"p50": 1.3, "p95": 1.9}},
                    "one_shots": [
                        {"ref": "postgres_primary.config_apply_raise_wal", "count": 1, "hosts": ["db1.cluster.gitlab.com"]},
                        {"ref": "postgres_primary.restart_failed_semaphores", "count": 1, "hosts": ["db1.cluster.gitlab.com"]},
                        {"ref": "postgres_primary.config_apply_reduce_conn", "count": 1, "hosts": ["db1.cluster.gitlab.com"]},
                        {"ref": "postgres_primary.server_ready", "count": 1, "hosts": ["db1.cluster.gitlab.com"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 43,
                    "rate_multipliers": {
                        "web_read_ok_f": 0.0,
                        "web_read_5xx_f": 2.2,
                        "web_post_comment_ok_f": 0.0,
                        "web_post_comment_5xx_f": 1.8,
                        "pg_dump_backup_f": 0.0,
                        "postgres_primary.startup_panic": 1.0,
                        "postgres_primary.wal_removed": 0.0,
                        "postgres_primary.checkpoint_complete": 0.0,
                        "ops_bastion.rsync_progress": 1.0,
                    },
                    "latency_multipliers": {"web_read_5xx_f": {"p50": 1.8, "p95": 2.4}, "web_post_comment_5xx_f": {"p50": 1.6, "p95": 2.3}},
                    "one_shots": [
                        {"ref": "ops_bastion.cmd_exec_rm_primary", "count": 1, "hosts": ["bastion-01.gitlab.com"]},
                        {"ref": "ops_bastion.s3_backup_check_empty", "count": 1, "hosts": ["bastion-01.gitlab.com"]},
                        {"ref": "ops_bastion.rsync_start", "count": 1, "hosts": ["bastion-01.gitlab.com"]},
                    ],
                },
            ]
        }
    },
}

# -----------------------------
# Helpers
# -----------------------------

BASE_TIME = datetime(2017, 1, 31, 0, 0, 0, tzinfo=timezone.utc)
BASE_EPOCH_MS = int(BASE_TIME.timestamp() * 1000)


def _h_int(key: str) -> int:
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


def _h_unit(key: str) -> float:
    return (_h_int(key) % 1_000_000) / 1_000_000.0


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    f = math.floor(expected)
    frac = expected - f
    return int(f + (1 if _h_unit(key) < frac else 0))


def gen_hex(length: int, key: str) -> str:
    hx = hashlib.md5(key.encode("utf-8")).hexdigest()
    out = (hx * ((length + len(hx) - 1) // len(hx)))[:length]
    return out.lower()


def gen_trace_id(key: str) -> str:
    return gen_hex(32, "trace:" + key)


def gen_uuid_like(key: str) -> str:
    hx = gen_hex(32, "uuid:" + key)
    # uuid4-like shape
    hx = hx[:12] + "4" + hx[13:]
    hx = hx[:16] + "8" + hx[17:]
    return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def choose(lst: List[Any], key: str) -> Any:
    if not lst:
        return ""
    return lst[_h_int(key) % len(lst)]


def parse_cidr_ip(cidr: str, key: str) -> str:
    # only handles /24 in this model
    base, prefix = cidr.split("/")
    prefix = int(prefix)
    if prefix != 24:
        return base
    parts = base.split(".")
    octet = 1 + (_h_int(key) % 254)
    return f"{parts[0]}.{parts[1]}.{parts[2]}.{octet}"


def sample_latency_ms(p50: float, p95: float, key: str) -> int:
    # Deterministic skewed positive sample consistent with p50/p95.
    # Soft cap reduced to avoid runaway durations; final durations are additionally bounded by template state_vars.
    if p50 <= 0 and p95 <= 0:
        return 0
    p50 = max(0.1, float(p50))
    p95 = max(p50, float(p95))
    u = _h_unit("lat:" + key)
    ratio = p95 / p50
    x = p50 * (ratio ** (u / 0.95 if 0.95 > 0 else 1.0))
    cap = 2.0 * p95
    return int(min(x, cap))


def fmt_ts(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:23] + "Z"


def parse_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def schedule_times_ms(start_min: int, end_min: int, count: int, key: str) -> List[int]:
    if count <= 0:
        return []
    start = int(start_min * 60_000)
    end = int(end_min * 60_000)
    dur = max(1, end - start)
    times = []
    for i in range(count):
        pos = (i + 0.5) / count
        t = start + int(pos * dur)
        j = int((_h_unit(f"{key}:j:{i}") - 0.5) * 800.0)  # +/- 400ms
        t2 = t + j
        if t2 < start:
            t2 = start
        if t2 >= end:
            t2 = end - 1
        times.append(t2)
    return times


def domain_value(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return choose(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        return lo + (_h_int(key) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = _h_unit(key)
        return lo + u * (hi - lo)
    if k == "uuid":
        return gen_uuid_like(key)
    if k == "hex":
        return gen_hex(int(v), key)
    if k == "ip":
        return parse_cidr_ip(str(v), key)
    if k == "str":
        return f"{str(v)}-{gen_hex(8, key)}"
    return ""


def render_log(component_id: str, log_id: str, state: str, key: str, overrides: Dict[str, Any]) -> Tuple[str, str]:
    comp = SYSTEM["components"][component_id]
    tpl = comp["logs"][log_id]
    ctx: Dict[str, Any] = {}
    for var, dom in tpl.get("vars", {}).items():
        if var in overrides:
            ctx[var] = overrides[var]
        else:
            ctx[var] = domain_value(dom, f"{key}:{component_id}.{log_id}:{var}")
    for var, dom in tpl.get("state_vars", {}).get(state, {}).items():
        if var in overrides:
            ctx[var] = overrides[var]
        else:
            ctx[var] = domain_value(dom, f"{key}:{component_id}.{log_id}:sv:{var}")
    msg = tpl["msg"].format_map(ctx)
    lvl = tpl["lvl"]
    return lvl, msg


def get_state_i_range(component_id: str, log_id: str, state: str, var: str) -> Optional[Tuple[int, int]]:
    tpl = SYSTEM["components"][component_id]["logs"][log_id]
    sv = tpl.get("state_vars", {}).get(state, {}).get(var)
    if not sv:
        return None
    if sv.get("k") != "i":
        return None
    lo, hi = int(sv["v"][0]), int(sv["v"][1])
    if hi < lo:
        lo, hi = hi, lo
    return lo, hi


def clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def scale_subset_to_sum(delays: List[int], start_idx_inclusive: int, desired_sum: int, key: str, min_each: int = 1) -> List[int]:
    """
    Scale delays[start_idx_inclusive:] so they sum to desired_sum, preserving proportions.
    Ensures each delay in the subset is >= min_each when subset is non-empty and desired_sum permits it.
    """
    if start_idx_inclusive >= len(delays):
        return delays
    subset = delays[start_idx_inclusive:]
    n = len(subset)
    if n == 0:
        return delays

    min_total = min_each * n
    if desired_sum < min_total:
        # If desired sum is too small, allow zeros but keep at least 0.
        min_each_eff = 0
        min_total = 0
    else:
        min_each_eff = min_each

    raw_sum = sum(max(0, d) for d in subset)
    if raw_sum <= 0:
        # Allocate evenly
        base = desired_sum // n
        rem = desired_sum - base * n
        new_subset = [base] * n
        for i in range(rem):
            new_subset[i] += 1
        # Enforce min_each_eff
        if min_each_eff > 0:
            new_subset = [max(min_each_eff, d) for d in new_subset]
            # fix sum if bumped
            bump = sum(new_subset) - desired_sum
            i = 0
            while bump > 0 and i < n:
                can_dec = new_subset[i] - min_each_eff
                dec = min(can_dec, bump)
                new_subset[i] -= dec
                bump -= dec
                i += 1
        return delays[:start_idx_inclusive] + new_subset

    # First pass: proportional scaling
    target = desired_sum
    scaled = []
    for i, d in enumerate(subset):
        val = int(round((max(0, d) / raw_sum) * target))
        scaled.append(val)

    # Fix rounding drift
    drift = target - sum(scaled)
    if drift != 0:
        # deterministic distribution of drift by ranking fractional priorities via hash
        order = list(range(n))
        order.sort(key=lambda i: _h_int(f"{key}:drift:{i}"))
        if drift > 0:
            for k in range(drift):
                scaled[order[k % n]] += 1
        else:
            drift_abs = -drift
            for k in range(drift_abs):
                i = order[k % n]
                if scaled[i] > 0:
                    scaled[i] -= 1

    # Enforce min_each_eff if requested and possible
    if min_each_eff > 0:
        scaled = [max(min_each_eff, v) for v in scaled]
        bump = sum(scaled) - target
        if bump > 0:
            # Remove bump from those above min_each_eff deterministically
            order = list(range(n))
            order.sort(key=lambda i: _h_int(f"{key}:debump:{i}"))
            for i in order:
                if bump <= 0:
                    break
                can_dec = scaled[i] - min_each_eff
                if can_dec <= 0:
                    continue
                dec = min(can_dec, bump)
                scaled[i] -= dec
                bump -= dec
        # If we couldn't remove enough due to min constraints, accept slight overage by trimming last element.
        if sum(scaled) != target:
            diff = sum(scaled) - target
            if diff > 0:
                scaled[-1] = max(min_each_eff, scaled[-1] - diff)
            elif diff < 0:
                scaled[-1] += -diff

    return delays[:start_idx_inclusive] + scaled


# -----------------------------
# Control state for failure phase
# -----------------------------


@dataclass
class CtrlSnapshot:
    flow_rate: Dict[str, float]
    bg_rate: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50, p95}


def build_failure_intervals() -> List[Tuple[int, int, CtrlSnapshot, List[Dict[str, Any]]]]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] <= fend}) + [fend]
    b2 = []
    for b in boundaries:
        if not b2 or b2[-1] != b:
            b2.append(b)
    boundaries = b2

    flow_ids_f = {f["id"] for f in SYSTEM["flows"]["f"]["req"]}
    cur_flow_rate: Dict[str, float] = {fid: 1.0 for fid in flow_ids_f}
    cur_bg_rate: Dict[str, float] = {}
    cur_latency: Dict[str, Dict[str, float]] = {fid: {"p50": 1.0, "p95": 1.0} for fid in flow_ids_f}

    def apply_event(ev: Dict[str, Any]) -> None:
        rm = ev.get("rate_multipliers", {}) or {}
        for k, v in rm.items():
            if k in flow_ids_f:
                cur_flow_rate[k] = float(v)
            else:
                cur_bg_rate[k] = float(v)
        lm = ev.get("latency_multipliers", {}) or {}
        for fid, mults in lm.items():
            if fid in flow_ids_f:
                cur_latency[fid] = {"p50": float(mults.get("p50", 1.0)), "p95": float(mults.get("p95", 1.0))}

    intervals: List[Tuple[int, int, CtrlSnapshot, List[Dict[str, Any]]]] = []
    ev_idx = 0
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        while ev_idx < len(events) and events[ev_idx]["at_min"] <= s:
            apply_event(events[ev_idx])
            ev_idx += 1
        oneshots_now = [ev for ev in events if ev["at_min"] == s]
        snap = CtrlSnapshot(flow_rate=dict(cur_flow_rate), bg_rate=dict(cur_bg_rate), latency_mult=dict(cur_latency))
        intervals.append((s, e, snap, oneshots_now))
    return intervals


# -----------------------------
# Simulation
# -----------------------------


def simulate() -> pd.DataFrame:
    rows: List[Tuple[int, str, str, str, str, str]] = []

    def emit_row(epoch_ms: int, level: str, message: str, trace_id: str, service: str, host: str) -> None:
        rows.append((epoch_ms, level, message, trace_id, service, host))

    def component_identity(component_id: str, chain_key: Optional[str] = None) -> Tuple[str, str]:
        comp = SYSTEM["components"][component_id]
        svc = comp.get("svc") or ""
        hosts = comp.get("hosts") or []
        if not hosts:
            return svc, ""
        if chain_key is None:
            return svc, hosts[0]
        return svc, choose(hosts, f"{chain_key}:host:{component_id}")

    def simulate_background(state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]] = None) -> None:
        duration = end_min - start_min
        for comp_id, comp in SYSTEM["components"].items():
            beh = comp.get("beh", {}).get(state, {}).get("emit", [])
            if not beh:
                continue
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                mult_key = f"{comp_id}.{log_id}"
                mult = 1.0
                if state == "f" and bg_mult is not None:
                    mult = float(bg_mult.get(mult_key, 1.0))
                eff = per_min * mult
                if eff <= 0:
                    continue

                if scope == "global":
                    n = deterministic_round(eff * duration, f"bg:{state}:{mult_key}:{start_min}-{end_min}")
                    times = schedule_times_ms(start_min, end_min, n, f"bg:{state}:{mult_key}:{start_min}-{end_min}")
                    for j, t_rel in enumerate(times):
                        epoch_ms = BASE_EPOCH_MS + t_rel
                        overrides: Dict[str, Any] = {}
                        if comp_id == "postgres_standby" and log_id == "repl_status":
                            ts_min = t_rel / 60_000.0
                            u = _h_unit(f"lag:{epoch_ms}:{j}")
                            if state == "n":
                                overrides["lag_s"] = int(u * 5)
                            else:
                                if ts_min < 33:
                                    overrides["lag_s"] = 60 + int(u * 300)
                                else:
                                    overrides["lag_s"] = 500 + int(u * 18000)
                        svc, host = component_identity(comp_id, chain_key=None)
                        level, msg = render_log(comp_id, log_id, state, f"bg:{mult_key}:{epoch_ms}:{j}", overrides)
                        emit_row(epoch_ms, level, msg, "", svc, host)
                else:
                    hosts = comp.get("hosts") or [""]
                    for host in hosts:
                        n = deterministic_round(eff * duration, f"bg:{state}:{mult_key}:{host}:{start_min}-{end_min}")
                        times = schedule_times_ms(start_min, end_min, n, f"bg:{state}:{mult_key}:{host}:{start_min}-{end_min}")
                        for j, t_rel in enumerate(times):
                            epoch_ms = BASE_EPOCH_MS + t_rel
                            overrides = {}
                            if comp_id == "postgres_standby" and log_id == "repl_status":
                                ts_min = t_rel / 60_000.0
                                u = _h_unit(f"lag:{epoch_ms}:{j}")
                                if state == "n":
                                    overrides["lag_s"] = int(u * 5)
                                else:
                                    if ts_min < 33:
                                        overrides["lag_s"] = 60 + int(u * 300)
                                    else:
                                        overrides["lag_s"] = 500 + int(u * 18000)
                            svc = comp.get("svc") or ""
                            level, msg = render_log(comp_id, log_id, state, f"bg:{mult_key}:{host}:{epoch_ms}:{j}", overrides)
                            emit_row(epoch_ms, level, msg, "", svc, host)

    def simulate_one_shots(at_min: int, shots: List[Dict[str, Any]]) -> None:
        if not shots:
            return
        for ev in shots:
            for shot in ev.get("one_shots", []) or []:
                ref = shot["ref"]
                cnt = int(shot["count"])
                hosts = shot.get("hosts", [])
                comp_id, log_id = parse_ref(ref)
                comp = SYSTEM["components"][comp_id]
                svc = comp.get("svc") or ""
                for k in range(cnt):
                    t_rel = int(at_min * 60_000) + int(_h_unit(f"oneshot:{ref}:{at_min}:{k}") * 2000.0)
                    epoch_ms = BASE_EPOCH_MS + t_rel
                    host = (
                        choose(hosts, f"oneshot:{ref}:{at_min}:{k}:host")
                        if hosts
                        else choose(comp.get("hosts", [""]), f"oneshot:{ref}:{at_min}:{k}:host")
                    )
                    level, msg = render_log(comp_id, log_id, "f", f"oneshot:{ref}:{at_min}:{k}", {})
                    emit_row(epoch_ms, level, msg, "", svc, host)

    def plan_step_delays(flow: Dict[str, Any], state: str, ctrl: Optional[CtrlSnapshot], chain_key: str) -> Tuple[List[int], Optional[int]]:
        """
        Returns:
          delays_ms: list with one delay per emit step (delay since previous log in the same attempt)
          bound_duration_ms: if the completion log has a duration_ms state_var, binds it and ensures delays match it
        """
        flow_id = flow["id"]
        lat_mult = {"p50": 1.0, "p95": 1.0}
        if state == "f" and ctrl is not None:
            lat_mult = ctrl.latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})

        # sample raw delays per step from hints (+ multipliers)
        delays: List[int] = []
        for step_i, (p50, p95) in enumerate(flow["latency_ms"]):
            p50_eff = float(p50)
            p95_eff = float(p95)
            if state == "f" and ctrl is not None:
                p50_eff *= float(lat_mult.get("p50", 1.0))
                p95_eff *= float(lat_mult.get("p95", 1.0))
            d = sample_latency_ms(p50_eff, p95_eff, f"{chain_key}:step:{step_i}")
            delays.append(max(0, int(d)))

        # If the final log template has a duration_ms domain, bind duration_ms and scale delays accordingly.
        last_ref = flow["emit"][-1]
        last_comp, last_log = parse_ref(last_ref)
        dur_rng = get_state_i_range(last_comp, last_log, state, "duration_ms")

        if dur_rng is None:
            return delays, None

        lo, hi = dur_rng

        # Our messages measure duration between the first emitted "start" log and the final completion log.
        # With this simulator's "delay before each log" convention, that duration is sum(delays[1:]).
        raw_duration = sum(delays[1:]) if len(delays) > 1 else 0

        # Clamp into allowed template range.
        desired_duration = clamp_int(int(raw_duration), lo, hi)

        # If raw duration is outside range, scale delays[1:] so sum matches desired_duration.
        # Even if already within range, scaling may be a no-op.
        if len(delays) > 1:
            delays = scale_subset_to_sum(delays, 1, desired_duration, key=f"{chain_key}:scale_duration", min_each=1)

        # After scaling, ensure exact duration sum for coherence
        actual = sum(delays[1:]) if len(delays) > 1 else 0
        if len(delays) > 1 and actual != desired_duration:
            # deterministic correction to last step
            delta = desired_duration - actual
            delays[-1] = max(0, delays[-1] + delta)

        return delays, desired_duration

    def simulate_flow_instances(state: str, start_min: int, end_min: int, ctrl: Optional[CtrlSnapshot]) -> None:
        flows = SYSTEM["flows"][state]["req"]
        duration = end_min - start_min
        flows = sorted(flows, key=lambda f: f["id"])
        for flow in flows:
            flow_id = flow["id"]
            base_rpm = float(flow["rpm"])
            mult = 1.0
            if state == "f" and ctrl is not None:
                mult = float(ctrl.flow_rate.get(flow_id, 1.0))
            eff_rpm = base_rpm * mult
            if eff_rpm <= 0:
                continue

            n_inst = deterministic_round(eff_rpm * duration, f"flow:{state}:{flow_id}:{start_min}-{end_min}")
            starts = schedule_times_ms(start_min, end_min, n_inst, f"flow:{state}:{flow_id}:{start_min}-{end_min}")

            for idx, start_rel_ms in enumerate(starts):
                chain_key = f"{state}:{flow_id}:{start_min}-{end_min}:{idx}"
                trace_id = gen_trace_id(chain_key) if (SYSTEM["tracing"]["on"] and flow.get("trace", False)) else ""

                # Bind semantic context for this request/job
                web_req_id = ""
                method = ""
                route = ""
                client_ip = ""
                status_5xx = ""
                db_error = ""

                if flow_id.startswith("web_"):
                    web_req_id = gen_uuid_like(f"{chain_key}:req_id")
                    is_post = "post_comment" in flow_id
                    is_5xx = "5xx" in flow_id
                    method = "POST" if is_post else "GET"
                    route = "/api/v4/notes" if is_post else choose(["/", "/projects", "/api/v4/projects"], f"{chain_key}:route")
                    client_ip = parse_cidr_ip("198.51.100.0/24", f"{chain_key}:ip")
                    if is_5xx:
                        status_5xx = "503" if (_h_unit(f"{chain_key}:status") < 0.55) else "500"
                        start_min_f = start_rel_ms / 60_000.0
                        if start_min_f >= 43:
                            db_error = choose(
                                ["connection refused", "could not connect to server", "unexpected EOF on client connection"],
                                f"{chain_key}:db_err",
                            )
                        else:
                            db_error = choose(
                                ["statement timeout", "unexpected EOF on client connection", "could not connect to server", "connection refused"],
                                f"{chain_key}:db_err",
                            )

                job = ""
                jid = ""
                job_status = ""
                if flow_id.startswith("sidekiq_"):
                    jid = gen_hex(16, f"{chain_key}:jid")
                    if flow_id == "sidekiq_user_destroy_f":
                        job = "UserDestroyJob"
                        job_status = "error" if (_h_unit(f"{chain_key}:job_status") < 0.35) else "ok"
                    else:
                        job = choose(["AbuseReportProcessJob", "ProjectCacheWarmJob", "UserDestroyJob"], f"{chain_key}:job")
                        job_status = "error" if (_h_unit(f"{chain_key}:job_status") < (0.08 if state == "n" else 0.18)) else "ok"

                # Plan per-step delays; bind duration_ms when completion log template requires it.
                step_delays, bound_duration_ms = plan_step_delays(flow, state, ctrl, chain_key)

                t_ms = BASE_EPOCH_MS + start_rel_ms
                req_start_ts_ms: Optional[int] = None
                job_start_ts_ms: Optional[int] = None

                # For duration-bearing completion logs, duration is between "start" log and completion; with our delay convention that's sum(step_delays[1:]).
                # We'll use bound_duration_ms (if not None) to populate message fields and keep it consistent with timestamps.
                for step_i, ref in enumerate(flow["emit"]):
                    comp_id, log_id = parse_ref(ref)
                    delay = step_delays[step_i] if step_i < len(step_delays) else 0
                    t_ms += int(delay)

                    overrides: Dict[str, Any] = {}

                    if comp_id == "gitlab_web" and log_id == "req_start":
                        overrides.update({"method": method, "route": route, "client_ip": client_ip, "req_id": web_req_id})
                        req_start_ts_ms = t_ms

                    elif comp_id == "gitlab_web" and log_id == "db_error":
                        overrides.update(
                            {
                                "req_id": web_req_id,
                                "error": db_error
                                or choose(SYSTEM["components"]["gitlab_web"]["logs"]["db_error"]["vars"]["error"]["v"], f"{chain_key}:db_err2"),
                            }
                        )

                    elif comp_id == "gitlab_web" and log_id == "req_done_200":
                        overrides["req_id"] = web_req_id
                        if req_start_ts_ms is None:
                            req_start_ts_ms = t_ms

                        # Bind duration from timestamps and clamp to template state_vars (already ensured by plan_step_delays).
                        duration_ms = int(t_ms - req_start_ts_ms)
                        dur_rng = get_state_i_range("gitlab_web", "req_done_200", state, "duration_ms")
                        if dur_rng is not None:
                            duration_ms = clamp_int(duration_ms, dur_rng[0], dur_rng[1])
                        overrides["duration_ms"] = duration_ms

                        # Bind db_ms within template range and <= duration
                        db_rng = get_state_i_range("gitlab_web", "req_done_200", state, "db_ms")
                        db_min, db_max = (0, duration_ms) if db_rng is None else db_rng
                        db_ms = int(max(1, duration_ms * (0.65 + 0.2 * _h_unit(f"{chain_key}:dbfrac"))))
                        db_ms = min(db_ms, duration_ms)
                        db_ms = clamp_int(db_ms, db_min, min(db_max, duration_ms))
                        overrides["db_ms"] = db_ms

                    elif comp_id == "gitlab_web" and log_id == "req_done_500":
                        overrides["req_id"] = web_req_id
                        overrides["status"] = status_5xx or choose(["500", "503"], f"{chain_key}:status2")
                        if req_start_ts_ms is None:
                            req_start_ts_ms = t_ms
                        duration_ms = int(t_ms - req_start_ts_ms)
                        dur_rng = get_state_i_range("gitlab_web", "req_done_500", state, "duration_ms")
                        if dur_rng is not None:
                            duration_ms = clamp_int(duration_ms, dur_rng[0], dur_rng[1])
                        overrides["duration_ms"] = duration_ms

                    elif comp_id == "sidekiq_worker" and log_id == "job_start":
                        overrides.update({"job": job, "jid": jid})
                        job_start_ts_ms = t_ms

                    elif comp_id == "sidekiq_worker" and log_id == "job_done":
                        overrides.update({"job": job, "jid": jid, "status": job_status})
                        if job_start_ts_ms is None:
                            job_start_ts_ms = t_ms
                        duration_ms = int(t_ms - job_start_ts_ms)
                        dur_rng = get_state_i_range("sidekiq_worker", "job_done", state, "duration_ms")
                        if dur_rng is not None:
                            duration_ms = clamp_int(duration_ms, dur_rng[0], dur_rng[1])
                        overrides["duration_ms"] = duration_ms

                    svc, host = component_identity(comp_id, chain_key=chain_key)
                    level, msg = render_log(comp_id, log_id, state, f"{chain_key}:{comp_id}.{log_id}:{t_ms}", overrides)
                    emit_row(t_ms, level, msg, trace_id, svc, host)

                # Extra coherence check (no emission): ensure bound duration matches if it was bound
                if bound_duration_ms is not None and req_start_ts_ms is not None and flow_id.startswith("web_"):
                    # No action needed; plan_step_delays already aligned delays to duration range.
                    pass
                if bound_duration_ms is not None and job_start_ts_ms is not None and flow_id.startswith("sidekiq_"):
                    pass

    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background("n", nstart, nend, None)
    simulate_flow_instances("n", nstart, nend, None)

    intervals = build_failure_intervals()
    for s, e, ctrl, oneshots_at_s in intervals:
        simulate_one_shots(s, oneshots_at_s)
        simulate_background("f", s, e, ctrl.bg_rate)
        simulate_flow_instances("f", s, e, ctrl)

    rows.sort(key=lambda r: r[0])
    df = pd.DataFrame(rows, columns=["_ts_ms", "level", "message", "trace_id", "service", "host"])
    df["timestamp"] = df["_ts_ms"].map(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
