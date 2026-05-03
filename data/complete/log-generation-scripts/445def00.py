import math
import random
import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "mandrill_mailer"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "mandrill_api": {
            "svc": "mandrill-api",
            "hosts": ["api-1", "api-2", "api-3"],
            "logs": {
                "api_req_other": {
                    "lvl": "INFO",
                    "msg": "send job start msg_id={msg_id} account={account} shard={shard}",
                    "vars": {
                        "msg_id": {"k": "uuid", "v": None},
                        "account": {"k": "ch", "v": ["acct_01", "acct_02", "acct_03", "acct_04", "acct_05"]},
                        "shard": {"k": "ch", "v": ["shard1", "shard2", "shard3", "shard5"]},
                    },
                },
                "api_req_shard4": {
                    "lvl": "INFO",
                    "msg": "send job start msg_id={msg_id} account={account} shard=shard4",
                    "vars": {
                        "msg_id": {"k": "uuid", "v": None},
                        "account": {"k": "ch", "v": ["acct_01", "acct_02", "acct_03", "acct_04", "acct_05"]},
                    },
                },
                "kv_write_ok_other": {
                    "lvl": "INFO",
                    "msg": "kv write ok shard={shard} key={key} txn_ms={txn_ms}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard1", "shard2", "shard3", "shard5"]},
                        "key": {"k": "str", "v": "msg:{uuid}|job:{uuid}"},
                    },
                    "state_vars": {
                        "n": {"txn_ms": {"k": "i", "v": [2, 25]}},
                        "f": {"txn_ms": {"k": "i", "v": [3, 60]}},
                    },
                },
                "kv_write_ok_shard4": {
                    "lvl": "INFO",
                    "msg": "kv write ok shard=shard4 key={key} txn_ms={txn_ms}",
                    "vars": {"key": {"k": "str", "v": "msg:{uuid}|job:{uuid}"}},
                    "state_vars": {
                        "n": {"txn_ms": {"k": "i", "v": [3, 40]}},
                        "f": {"txn_ms": {"k": "i", "v": [3, 80]}},
                    },
                },
                "kv_write_err_shard4": {
                    "lvl": "ERROR",
                    "msg": "kv write failed shard=shard4 sqlstate={sqlstate} err=\"{err}\" txn_ms={txn_ms}",
                    "vars": {
                        "sqlstate": {"k": "ch", "v": ["57P01", "55000"]},
                        "err": {
                            "k": "ch",
                            "v": [
                                "database is not accepting commands to avoid wraparound data loss",
                                "terminating connection due to administrator command",
                                "could not connect to server: Connection refused",
                            ],
                        },
                    },
                    "state_vars": {
                        "n": {"txn_ms": {"k": "i", "v": [5, 80]}},
                        "f": {"txn_ms": {"k": "i", "v": [30, 900]}},
                    },
                },
                "retry_write": {
                    "lvl": "WARN",
                    "msg": "retrying kv write shard=shard4 attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {"attempt": {"k": "i", "v": [2, 3]}, "backoff_ms": {"k": "i", "v": [50, 1500]}},
                },
                "job_spooled": {
                    "lvl": "WARN",
                    "msg": "spooled job to disk msg_id={msg_id} spool_path={spool_path} reason={reason}",
                    "vars": {
                        "msg_id": {"k": "uuid", "v": None},
                        "spool_path": {"k": "str", "v": "/var/spool/mandrill/spool-*.json"},
                        "reason": {"k": "ch", "v": ["db_shutdown", "db_timeout"]},
                    },
                },
                "healthcheck": {
                    "lvl": "INFO",
                    "msg": "health ok reqs_1m={reqs_1m} errs_1m={errs_1m}",
                    "vars": {},
                    "state_vars": {
                        "n": {"reqs_1m": {"k": "i", "v": [110, 170]}, "errs_1m": {"k": "i", "v": [0, 3]}},
                        "f": {"reqs_1m": {"k": "i", "v": [90, 170]}, "errs_1m": {"k": "i", "v": [25, 140]}},
                    },
                },
                "disk_usage_metric": {
                    "lvl": "INFO",
                    "msg": "spool disk usage pct={disk_pct} spool_q={spool_q}",
                    "vars": {},
                    "state_vars": {
                        "n": {"disk_pct": {"k": "i", "v": [35, 60]}, "spool_q": {"k": "i", "v": [0, 400]}},
                        "f": {"disk_pct": {"k": "i", "v": [60, 95]}, "spool_q": {"k": "i", "v": [2000, 20000]}},
                    },
                },
                "disk_low": {
                    "lvl": "WARN",
                    "msg": "disk space low pct={disk_pct} free_gb={free_gb}",
                    "vars": {"disk_pct": {"k": "i", "v": [85, 98]}, "free_gb": {"k": "i", "v": [1, 20]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "healthcheck", "per_min": 1.0, "scope": "per_host"},
                        {"id": "disk_usage_metric", "per_min": 0.3, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "healthcheck", "per_min": 1.0, "scope": "per_host"},
                        {"id": "disk_usage_metric", "per_min": 1.0, "scope": "per_host"},
                        {"id": "disk_low", "per_min": 0.8, "scope": "per_host"},
                    ]
                },
            },
        },
        "mail_worker": {
            "svc": "mandrill-worker",
            "hosts": ["worker-1", "worker-2"],
            "logs": {
                "smtp_sent": {
                    "lvl": "INFO",
                    "msg": "smtp delivered msg_id={msg_id} provider={provider} dur_ms={dur_ms}",
                    "vars": {
                        "msg_id": {"k": "uuid", "v": None},
                        "provider": {"k": "ch", "v": ["smtp_relay_a", "smtp_relay_b"]},
                        "dur_ms": {"k": "i", "v": [50, 800]},
                    },
                },
                "queue_depth": {
                    "lvl": "INFO",
                    "msg": "queue depth main={main_q} spool={spool_q}",
                    "vars": {},
                    "state_vars": {
                        "n": {"main_q": {"k": "i", "v": [200, 2500]}, "spool_q": {"k": "i", "v": [0, 300]}},
                        "f": {"main_q": {"k": "i", "v": [3000, 50000]}, "spool_q": {"k": "i", "v": [2000, 25000]}},
                    },
                },
                "spool_backlog_warn": {
                    "lvl": "WARN",
                    "msg": "spool backlog elevated spool_q={spool_q} disk_pct={disk_pct}",
                    "vars": {"spool_q": {"k": "i", "v": [5000, 30000]}, "disk_pct": {"k": "i", "v": [75, 98]}},
                },
                "backlog_drain": {
                    "lvl": "INFO",
                    "msg": "backlog draining drained_1m={drained_1m} main_q={main_q} spool_q={spool_q}",
                    "vars": {
                        "drained_1m": {"k": "i", "v": [100, 2500]},
                        "main_q": {"k": "i", "v": [1000, 30000]},
                        "spool_q": {"k": "i", "v": [200, 12000]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "queue_depth", "per_min": 1.0, "scope": "per_host"},
                        {"id": "backlog_drain", "per_min": 0.0, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "queue_depth", "per_min": 2.0, "scope": "per_host"},
                        {"id": "spool_backlog_warn", "per_min": 0.6, "scope": "per_host"},
                        {"id": "backlog_drain", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "postgres_shard4": {
            "svc": "postgres",
            "hosts": ["pg-shard4-1"],
            "logs": {
                "xid_metric": {
                    "lvl": "INFO",
                    "msg": "xid age snapshot age={xid_age} freeze_max_age={freeze_max_age}",
                    "vars": {"freeze_max_age": {"k": "i", "v": [2000000000, 2000000000]}},
                    "state_vars": {
                        "n": {"xid_age": {"k": "i", "v": [1100000000, 1700000000]}},
                        "f": {"xid_age": {"k": "i", "v": [2050000000, 2120000000]}},
                    },
                },
                "xid_metric_post": {
                    "lvl": "INFO",
                    "msg": "xid age snapshot(post-maint) age={xid_age} freeze_max_age={freeze_max_age}",
                    "vars": {
                        "xid_age": {"k": "i", "v": [50000000, 300000000]},
                        "freeze_max_age": {"k": "i", "v": [2000000000, 2000000000]},
                    },
                },
                "conn_reject": {
                    "lvl": "ERROR",
                    "msg": "connection rejected: {reason}",
                    "vars": {"reason": {"k": "ch", "v": ["wraparound_protection", "maintenance_mode"]}},
                },
                "wraparound_shutdown": {
                    "lvl": "CRITICAL",
                    "msg": "database shutdown to avoid wraparound; xid_age={xid_age}",
                    "vars": {"xid_age": {"k": "i", "v": [2080000000, 2140000000]}},
                },
                "standalone_mode": {"lvl": "INFO", "msg": "started in standalone maintenance mode", "vars": {}},
                "vacuum_started": {"lvl": "INFO", "msg": "VACUUM started", "vars": {}},
                "vacuum_setting": {
                    "lvl": "INFO",
                    "msg": "vacuum settings updated cost_delay_ms={cost_delay_ms} maintenance_work_mem_mb={mw_mem_mb}",
                    "vars": {"cost_delay_ms": {"k": "i", "v": [0, 20]}, "mw_mem_mb": {"k": "i", "v": [1024, 8192]}},
                },
                "vacuum_progress": {
                    "lvl": "INFO",
                    "msg": "VACUUM progress phase={phase} rel={rel} scanned_pct={scanned_pct} remaining_h={remaining_h}",
                    "vars": {
                        "phase": {"k": "ch", "v": ["scanning", "vacuuming", "freezing"]},
                        "rel": {"k": "ch", "v": ["kv_store", "job_state", "search", "url"]},
                        "scanned_pct": {"k": "f", "v": [0.0, 99.9]},
                        "remaining_h": {"k": "f", "v": [1.0, 240.0]},
                    },
                },
                "truncate_search": {"lvl": "WARN", "msg": "truncate table search executed", "vars": {}},
                "truncate_url": {"lvl": "WARN", "msg": "truncate table url executed", "vars": {}},
                "vacuum_completed": {
                    "lvl": "INFO",
                    "msg": "VACUUM completed xid_age_after={xid_age_after}",
                    "vars": {"xid_age_after": {"k": "i", "v": [50000000, 300000000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "xid_metric", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "xid_metric", "per_min": 1.0, "scope": "per_host"},
                        {"id": "xid_metric_post", "per_min": 1.0, "scope": "per_host"},
                        {"id": "conn_reject", "per_min": 30.0, "scope": "per_host"},
                        {"id": "vacuum_progress", "per_min": 2.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "postgres_shards": {
            "svc": "postgres",
            "hosts": ["pg-shard1-1", "pg-shard2-1", "pg-shard3-1", "pg-shard5-1"],
            "logs": {
                "checkpoint": {
                    "lvl": "INFO",
                    "msg": "checkpoint complete write_ms={write_ms} buffers={buffers}",
                    "vars": {"write_ms": {"k": "i", "v": [50, 1200]}, "buffers": {"k": "i", "v": [5000, 80000]}},
                }
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "checkpoint", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        "log_aggregator": {
            "svc": "log-agg",
            "hosts": ["log-1"],
            "logs": {
                "ingest_summary": {
                    "lvl": "INFO",
                    "msg": "ingest summary errs_1m={errs_1m} top_service={top_service} backlog={backlog}",
                    "vars": {"top_service": {"k": "ch", "v": ["mandrill-api", "billing-api", "auth-service", "edge-proxy"]}},
                    "state_vars": {
                        "n": {"errs_1m": {"k": "i", "v": [0, 50]}, "backlog": {"k": "i", "v": [0, 100]}},
                        "f": {"errs_1m": {"k": "i", "v": [500, 5000]}, "backlog": {"k": "i", "v": [1000, 20000]}},
                    },
                },
                "alert_eval": {
                    "lvl": "WARN",
                    "msg": "alert evaluation delayed backlog={backlog} dropped={dropped}",
                    "vars": {"backlog": {"k": "i", "v": [1000, 30000]}, "dropped": {"k": "i", "v": [0, 8000]}},
                },
                "incident_opened": {
                    "lvl": "INFO",
                    "msg": "incident opened id={inc_id} title=\"{title}\"",
                    "vars": {"inc_id": {"k": "hex", "v": 8}, "title": {"k": "ch", "v": ["shard4 write failures", "email send degradation", "database incident"]}},
                },
                "alert_rule_added": {
                    "lvl": "INFO",
                    "msg": "alert rule added name={name} threshold={threshold}",
                    "vars": {"name": {"k": "ch", "v": ["pg_xid_wraparound"]}, "threshold": {"k": "i", "v": [1900000000, 2100000000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_summary", "per_min": 2.0, "scope": "global"}]},
                "f": {"emit": [{"id": "ingest_summary", "per_min": 2.0, "scope": "global"}, {"id": "alert_eval", "per_min": 1.5, "scope": "global"}]},
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "send_email_other",
                "rpm": 336.0,
                "emit": ["mandrill_api.api_req_other", "mandrill_api.kv_write_ok_other", "mail_worker.smtp_sent"],
                "latency_ms": [[5, 10], [6, 20], [80, 250]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "send_email_shard4_ok",
                "rpm": 84.0,
                "emit": ["mandrill_api.api_req_shard4", "mandrill_api.kv_write_ok_shard4", "mail_worker.smtp_sent"],
                "latency_ms": [[5, 12], [8, 30], [85, 260]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "send_email_other",
                "rpm": 336.0,
                "emit": ["mandrill_api.api_req_other", "mandrill_api.kv_write_ok_other", "mail_worker.smtp_sent"],
                "latency_ms": [[6, 14], [7, 30], [90, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "send_email_shard4_fail",
                "rpm": 84.0,
                "emit": ["mandrill_api.api_req_shard4", "mandrill_api.kv_write_err_shard4"],
                "latency_ms": [[6, 15], [40, 400]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["mandrill_api.retry_write"], "backoff_ms": [[150, 700], [300, 1200]]},
                "trace": False,
            },
            {
                "id": "spool_job_shard4",
                "rpm": 84.0,
                "emit": ["mandrill_api.job_spooled"],
                "latency_ms": [[8, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "send_email_shard4_ok",
                "rpm": 84.0,
                "emit": ["mandrill_api.api_req_shard4", "mandrill_api.kv_write_ok_shard4", "mail_worker.smtp_sent"],
                "latency_ms": [[6, 16], [12, 60], [95, 360]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "shard4_xid_wraparound_degradation",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "send_email_shard4_ok": 0.0,
                            "postgres_shard4.vacuum_progress": 0.0,
                            "postgres_shard4.xid_metric_post": 0.0,
                            "mandrill_api.disk_low": 0.0,
                            "mail_worker.spool_backlog_warn": 0.0,
                            "mail_worker.backlog_drain": 0.0,
                            "log_aggregator.alert_eval": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "postgres_shard4.wraparound_shutdown", "count": 1, "hosts": ["pg-shard4-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 32,
                        "rate_multipliers": {
                            "send_email_shard4_fail": 1.2,
                            "spool_job_shard4": 1.2,
                            "mandrill_api.disk_low": 1.0,
                            "mail_worker.spool_backlog_warn": 1.0,
                            "log_aggregator.alert_eval": 1.6,
                        },
                        "latency_multipliers": {"send_email_other": {"p50": 1.05, "p95": 1.25}},
                        "one_shots": [{"ref": "log_aggregator.incident_opened", "count": 1, "hosts": ["log-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 40,
                        "rate_multipliers": {"postgres_shard4.vacuum_progress": 1.0},
                        "latency_multipliers": {"send_email_shard4_fail": {"p50": 1.1, "p95": 1.2}},
                        "one_shots": [
                            {"ref": "postgres_shard4.standalone_mode", "count": 1, "hosts": ["pg-shard4-1"]},
                            {"ref": "postgres_shard4.vacuum_setting", "count": 1, "hosts": ["pg-shard4-1"]},
                            {"ref": "postgres_shard4.vacuum_started", "count": 1, "hosts": ["pg-shard4-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 46,
                        "rate_multipliers": {"postgres_shard4.conn_reject": 0.7, "postgres_shard4.vacuum_progress": 1.8, "log_aggregator.alert_eval": 1.2},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "postgres_shard4.truncate_search", "count": 1, "hosts": ["pg-shard4-1"]},
                            {"ref": "postgres_shard4.truncate_url", "count": 1, "hosts": ["pg-shard4-1"]},
                        ],
                    },
                    {
                        "order": 5,
                        "at_min": 48,
                        "rate_multipliers": {
                            "send_email_shard4_ok": 1.0,
                            "send_email_shard4_fail": 0.05,
                            "spool_job_shard4": 0.05,
                            "postgres_shard4.conn_reject": 0.05,
                            "postgres_shard4.vacuum_progress": 0.0,
                            "postgres_shard4.xid_metric": 0.0,
                            "postgres_shard4.xid_metric_post": 1.0,
                            "mandrill_api.disk_low": 0.2,
                            "mail_worker.spool_backlog_warn": 0.2,
                            "mail_worker.backlog_drain": 1.0,
                            "log_aggregator.alert_eval": 0.4,
                        },
                        "latency_multipliers": {"send_email_shard4_ok": {"p50": 1.2, "p95": 1.6}},
                        "one_shots": [{"ref": "postgres_shard4.vacuum_completed", "count": 1, "hosts": ["pg-shard4-1"]}],
                    },
                    {
                        "order": 6,
                        "at_min": 49,
                        "rate_multipliers": {"log_aggregator.alert_eval": 0.2},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "log_aggregator.alert_rule_added", "count": 1, "hosts": ["log-1"]}],
                    },
                ]
            }
        },
    }
}


SEED = 1337
random.seed(SEED)
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def stable_hash_int(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def rng_for(*parts: Any) -> random.Random:
    return random.Random((stable_hash_int(SEED, *parts) ^ 0x9E3779B97F4A7C15) & ((1 << 64) - 1))


def isoformat_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def sample_lognormal_ms(p50: float, p95: float, key: Tuple[Any, ...], hard_cap: Optional[float] = None) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    z95 = 1.6448536269514722
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / z95
    r = rng_for("lognorm", *key)
    x = math.exp(mu + sigma * r.gauss(0.0, 1.0))
    cap = 3.0 * p95
    if hard_cap is not None:
        cap = min(cap, hard_cap)
    return float(min(x, cap))


def schedule_times(start: datetime, end: datetime, count: int, key: str, jitter_ms: int = 350) -> List[datetime]:
    if count <= 0:
        return []
    dur = (end - start).total_seconds()
    if dur <= 0:
        return []
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        base = start + timedelta(seconds=dur * frac)
        r = rng_for("jitter", key, i)
        j = (r.random() - 0.5) * 2.0 * jitter_ms
        t = base + timedelta(milliseconds=j)
        if t < start:
            t = start + timedelta(milliseconds=1)
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


class StableRounding:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, expected: float, key: str) -> int:
        x = float(expected) + self.carry.get(key, 0.0)
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return max(0, n)


def domain_value(dom: Dict[str, Any], state: str, key: Tuple[Any, ...]) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    r = rng_for("dom", state, *key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return lo + int(r.random() * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            x = lo
        else:
            x = lo + r.random() * (hi - lo)
        return f"{x:.1f}"
    if k == "ch":
        arr = list(v)
        if not arr:
            return ""
        return arr[int(r.random() * len(arr)) % len(arr)]
    if k == "hex":
        n = int(v)
        x = stable_hash_int("hex", state, *key)
        return f"{x:0{n}x}"[-n:]
    if k == "uuid":
        rr = rng_for("uuid", state, *key)
        u_int = rr.getrandbits(128)
        u = uuid.UUID(int=u_int, version=4)
        return str(u)
    if k == "str":
        pattern = str(v) if v is not None else ""
        rr = rng_for("str", state, *key)
        while "{uuid}" in pattern:
            u_int = rr.getrandbits(128)
            u = uuid.UUID(int=u_int, version=4)
            pattern = pattern.replace("{uuid}", str(u), 1)
        if "*" in pattern:
            pattern = pattern.replace("*", f"{rr.randrange(0, 10**8):08d}")
        return pattern
    return ""


def render_log(component_id: str, log_id: str, state: str, t: datetime, host: str, bound: Dict[str, Any]) -> Dict[str, Any]:
    comp = SYSTEM["components"][component_id]
    tmpl = comp["logs"][log_id]
    vals: Dict[str, Any] = {}

    # Use deterministic keying based on timestamp milliseconds and host to avoid per-run drift.
    t_ms_key = int(t.timestamp() * 1000)

    for k, dom in (tmpl.get("vars") or {}).items():
        if k in bound:
            vals[k] = bound[k]
        else:
            vals[k] = domain_value(dom, state, (component_id, log_id, k, t_ms_key, host))

    sv = (tmpl.get("state_vars") or {}).get(state) or {}
    for k, dom in sv.items():
        if k in bound:
            vals[k] = bound[k]
        else:
            vals[k] = domain_value(dom, state, (component_id, log_id, k, t_ms_key, host))

    if component_id == "log_aggregator" and log_id == "ingest_summary" and state == "f":
        vals["top_service"] = "mandrill-api"

    msg = tmpl["msg"].format(**vals)
    return {
        "timestamp": t,
        "level": tmpl["lvl"],
        "message": msg,
        "trace_id": "",
        "service": comp.get("svc", "") or "",
        "host": host or "",
    }


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult_flow: Dict[str, float]
    rate_mult_bg: Dict[str, float]
    lat_mult_flow: Dict[str, Dict[str, float]]


def build_failure_intervals() -> List[Interval]:
    ph_f = SCENARIO["scenario"]["time"]["phases"]["f"]
    f_start, f_end = int(ph_f["start_min"]), int(ph_f["end_min"])
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events = sorted(events, key=lambda e: (e["at_min"], e.get("order", 0)))

    flow_mult: Dict[str, float] = {}
    bg_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}

    boundaries = [f_start] + [int(e["at_min"]) for e in events] + [f_end]
    boundaries = sorted(set(boundaries))

    event_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        event_by_min.setdefault(int(e["at_min"]), []).append(e)

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        if s < f_start or s >= f_end or e <= f_start:
            continue
        for ev in event_by_min.get(s, []):
            for k, v in (ev.get("rate_multipliers") or {}).items():
                if "." in k:
                    bg_mult[k] = float(v)
                else:
                    flow_mult[k] = float(v)
            for fid, mm in (ev.get("latency_multipliers") or {}).items():
                lat_mult[fid] = {"p50": float(mm.get("p50", 1.0)), "p95": float(mm.get("p95", 1.0))}
        intervals.append(
            Interval(
                state="f",
                start_min=s,
                end_min=e,
                rate_mult_flow=dict(flow_mult),
                rate_mult_bg=dict(bg_mult),
                lat_mult_flow=dict(lat_mult),
            )
        )
    return intervals


def flow_list(state: str) -> List[Dict[str, Any]]:
    return list(SYSTEM["flows"][state])


def choose_component_host(component_id: str, flow_id: str, instance_idx: int) -> str:
    hosts = SYSTEM["components"][component_id].get("hosts") or []
    if not hosts:
        return ""
    j = stable_hash_int("host", component_id, flow_id, instance_idx) % len(hosts)
    return hosts[j]


def attempt_count(flow_def: Dict[str, Any], flow_id: str, instance_idx: int) -> int:
    rdef = flow_def["retry"]
    max_a = int(rdef["max_attempts"])
    exp = float(rdef["expected_attempts"])
    if max_a <= 1:
        return 1
    exp = max(1.0, min(exp, float(max_a)))
    lo = int(math.floor(exp))
    hi = min(max_a, lo + 1)
    p = exp - lo
    if hi == lo:
        return lo
    u = rng_for("attempts", flow_id, instance_idx).random()
    return hi if u < p else lo


def clamp_float(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def parse_ref(ref: str) -> Tuple[str, str]:
    c, l = ref.split(".", 1)
    return c, l


def _int_domain_bounds(dom: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    if not dom or dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    state: str,
    flow_def: Dict[str, Any],
    flow_id: str,
    start_t: datetime,
    instance_idx: int,
    lat_mult: Optional[Dict[str, float]],
) -> None:
    lat_mult = lat_mult or {"p50": 1.0, "p95": 1.0}
    emit_refs = list(flow_def["emit"])
    latency_pairs = list(flow_def["latency_ms"])
    rdef = flow_def["retry"]
    emit_per_retry = list(rdef.get("emit_per_retry") or [])
    backoff_pairs = list(rdef.get("backoff_ms") or [])

    components_in_flow: List[str] = []
    for ref in emit_refs + emit_per_retry:
        c, _ = parse_ref(ref)
        if c not in components_in_flow:
            components_in_flow.append(c)
    comp_host = {c: choose_component_host(c, flow_id, instance_idx) for c in components_in_flow}

    bound_req: Dict[str, Any] = {}
    if any(ref.startswith("mandrill_api.api_req_") for ref in emit_refs):
        bound_req["msg_id"] = domain_value({"k": "uuid", "v": None}, state, ("flow", flow_id, "msg_id", instance_idx))
        bound_req["account"] = domain_value(
            {"k": "ch", "v": ["acct_01", "acct_02", "acct_03", "acct_04", "acct_05"]},
            state,
            ("flow", flow_id, "account", instance_idx),
        )
        if "api_req_other" in [parse_ref(r)[1] for r in emit_refs]:
            bound_req["shard"] = domain_value({"k": "ch", "v": ["shard1", "shard2", "shard3", "shard5"]}, state, ("flow", flow_id, "shard", instance_idx))

    if any(ref.endswith(".smtp_sent") for ref in emit_refs):
        bound_req["provider"] = domain_value({"k": "ch", "v": ["smtp_relay_a", "smtp_relay_b"]}, state, ("flow", flow_id, "provider", instance_idx))

    A = attempt_count(flow_def, flow_id, instance_idx)
    current_attempt_start = start_t

    for attempt in range(1, A + 1):
        attempt_key = (flow_id, instance_idx, attempt)

        # Base per-step delays from latency hints (already scaled by active latency multipliers).
        step_delays: List[float] = []
        for si, (p50, p95) in enumerate(latency_pairs):
            p50s = float(p50) * float(lat_mult.get("p50", 1.0))
            p95s = float(p95) * float(lat_mult.get("p95", 1.0))
            d = sample_lognormal_ms(p50s, p95s, ("lat", *attempt_key, si))
            step_delays.append(d)

        # Bind per-attempt context and ensure observed timing fields match emitted chronology.
        bound_attempt: Dict[str, Any] = dict(bound_req)

        # Align kv txn_ms and smtp dur_ms with the actual scheduled per-step delays.
        for si, ref in enumerate(emit_refs):
            c, lid = parse_ref(ref)
            tmpl = SYSTEM["components"][c]["logs"][lid]

            # txn_ms may be in state_vars (most kv_write* templates).
            sv_dom = (tmpl.get("state_vars") or {}).get(state, {}).get("txn_ms")
            b = _int_domain_bounds(sv_dom) if sv_dom is not None else None
            if b is not None:
                lo, hi = b
                d_adj = clamp_float(step_delays[si], float(lo), float(hi))
                # Ensure the *timestamp gap* matches the field to prevent contradictions.
                txn_ms = int(round(d_adj))
                if txn_ms < lo:
                    txn_ms = lo
                if txn_ms > hi:
                    txn_ms = hi
                step_delays[si] = float(txn_ms)
                bound_attempt["txn_ms"] = txn_ms

            # dur_ms in smtp_sent vars.
            if lid == "smtp_sent":
                dom = (tmpl.get("vars") or {}).get("dur_ms")
                b2 = _int_domain_bounds(dom)
                if b2 is not None:
                    lo2, hi2 = b2
                    d_adj2 = clamp_float(step_delays[si], float(lo2), float(hi2))
                    dur_ms = int(round(d_adj2))
                    if dur_ms < lo2:
                        dur_ms = lo2
                    if dur_ms > hi2:
                        dur_ms = hi2
                    step_delays[si] = float(dur_ms)
                    bound_attempt["dur_ms"] = dur_ms

        t = current_attempt_start
        prev_log_time: Optional[datetime] = None

        for si, ref in enumerate(emit_refs):
            c, lid = parse_ref(ref)
            delay_ms = step_delays[si] if si < len(step_delays) else 0.0
            t = t + timedelta(milliseconds=delay_ms)
            prev_log_time = t

            bound_local = dict(bound_attempt)

            if lid == "kv_write_ok_other" and "shard" in bound_req:
                bound_local["shard"] = bound_req["shard"]
            if lid == "api_req_other" and "shard" in bound_req:
                bound_local["shard"] = bound_req["shard"]
            if lid == "smtp_sent" and "msg_id" in bound_req:
                bound_local["msg_id"] = bound_req["msg_id"]

            if lid in ("kv_write_ok_other", "kv_write_ok_shard4"):
                bound_local["key"] = domain_value({"k": "str", "v": "msg:{uuid}|job:{uuid}"}, state, ("flow", flow_id, "key", instance_idx, attempt))

            if lid == "kv_write_err_shard4":
                bound_local["sqlstate"] = domain_value({"k": "ch", "v": ["57P01", "55000"]}, state, ("flow", flow_id, "sqlstate", instance_idx))
                bound_local["err"] = domain_value(
                    {
                        "k": "ch",
                        "v": [
                            "database is not accepting commands to avoid wraparound data loss",
                            "terminating connection due to administrator command",
                            "could not connect to server: Connection refused",
                        ],
                    },
                    state,
                    ("flow", flow_id, "err", instance_idx),
                )

            rows.append(render_log(c, lid, state, t, comp_host[c], bound_local))

        if attempt < A:
            bo_pair = backoff_pairs[attempt - 1] if attempt - 1 < len(backoff_pairs) else backoff_pairs[-1]
            bo = sample_lognormal_ms(float(bo_pair[0]), float(bo_pair[1]), ("backoff", *attempt_key), hard_cap=1500.0)
            bo_int = int(round(clamp_float(bo, 50.0, 1500.0)))

            if emit_per_retry:
                rt = (prev_log_time or t) + timedelta(milliseconds=1)
                for rref in emit_per_retry:
                    rc, rlid = parse_ref(rref)
                    bound_retry = dict(bound_req)
                    bound_retry["attempt"] = attempt + 1
                    bound_retry["backoff_ms"] = bo_int
                    rows.append(render_log(rc, rlid, state, rt, comp_host[rc], bound_retry))
                    rt = rt + timedelta(milliseconds=1)

            current_attempt_start = (prev_log_time or t) + timedelta(milliseconds=bo_int)


def simulate() -> pd.DataFrame:
    rounding = StableRounding()
    rows: List[Dict[str, Any]] = []

    n_start = int(SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"])
    f_start = int(SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"])

    intervals: List[Interval] = []
    intervals.append(Interval(state="n", start_min=n_start, end_min=n_end, rate_mult_flow={}, rate_mult_bg={}, lat_mult_flow={}))
    intervals.extend(build_failure_intervals())

    def active_lat_mult(interval: Interval, flow_id: str) -> Dict[str, float]:
        if interval.state != "f":
            return {"p50": 1.0, "p95": 1.0}
        return interval.lat_mult_flow.get(flow_id, {"p50": 1.0, "p95": 1.0})

    def active_rate_mult_flow(interval: Interval, flow_id: str) -> float:
        if interval.state != "f":
            return 1.0
        return float(interval.rate_mult_flow.get(flow_id, 1.0))

    def active_rate_mult_bg(interval: Interval, comp_id: str, log_id: str) -> float:
        if interval.state != "f":
            return 1.0
        return float(interval.rate_mult_bg.get(f"{comp_id}.{log_id}", 1.0))

    # Background emissions
    for interval in intervals:
        start_dt = BASE_TIME + timedelta(minutes=interval.start_min)
        end_dt = BASE_TIME + timedelta(minutes=interval.end_min)
        dur_min = (interval.end_min - interval.start_min)

        for comp_id, comp in SYSTEM["components"].items():
            beh = (comp.get("beh") or {}).get(interval.state)
            if not beh:
                continue
            for src in beh.get("emit", []):
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope") or "per_host"
                per_min_eff = per_min * active_rate_mult_bg(interval, comp_id, log_id)

                if per_min_eff <= 0.0 or dur_min <= 0:
                    continue

                if scope == "global":
                    expected = per_min_eff * dur_min
                    cnt = rounding.alloc(expected, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|global")
                    times = schedule_times(start_dt, end_dt, cnt, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|global", jitter_ms=420)
                    hosts = comp.get("hosts") or [""]
                    for i, t in enumerate(times):
                        host = hosts[i % len(hosts)] if hosts else ""
                        rows.append(render_log(comp_id, log_id, interval.state, t, host, bound={}))
                else:
                    hosts = comp.get("hosts") or [""]
                    for host in hosts:
                        expected = per_min_eff * dur_min
                        cnt = rounding.alloc(expected, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{host}")
                        times = schedule_times(start_dt, end_dt, cnt, f"bg|{interval.state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{host}", jitter_ms=420)

                        for j, t in enumerate(times):
                            bound: Dict[str, Any] = {}
                            if comp_id == "postgres_shard4" and log_id == "vacuum_progress":
                                mins = (t - (BASE_TIME + timedelta(minutes=f_start))).total_seconds() / 60.0
                                prog = max(0.0, min(1.0, (mins - 15.0) / 8.0))
                                rr = rng_for("vacprog", int(t.timestamp() * 1000), j)
                                wob = (rr.random() - 0.5) * 0.12
                                prog = max(0.0, min(1.0, prog + wob))
                                scanned = max(0.0, min(99.9, 100.0 * prog))
                                remaining = max(1.0, min(240.0, 240.0 * (1.0 - prog) + 5.0))
                                bound["scanned_pct"] = f"{scanned:.1f}"
                                bound["remaining_h"] = f"{remaining:.1f}"

                            rows.append(render_log(comp_id, log_id, interval.state, t, host, bound=bound))

    # Flow instances
    for interval in intervals:
        start_dt = BASE_TIME + timedelta(minutes=interval.start_min)
        end_dt = BASE_TIME + timedelta(minutes=interval.end_min)
        dur_min = (interval.end_min - interval.start_min)
        if dur_min <= 0:
            continue

        for fdef in flow_list(interval.state):
            fid = fdef["id"]
            rpm = float(fdef["rpm"])
            mult = active_rate_mult_flow(interval, fid)
            rpm_eff = rpm * mult
            if rpm_eff <= 0.0:
                continue

            expected_instances = rpm_eff * dur_min
            inst_cnt = rounding.alloc(expected_instances, f"flow|{interval.state}|{interval.start_min}-{interval.end_min}|{fid}")
            starts = schedule_times(start_dt, end_dt, inst_cnt, f"flow|{interval.state}|{interval.start_min}-{interval.end_min}|{fid}", jitter_ms=480)

            for local_idx, st in enumerate(starts):
                instance_idx = stable_hash_int("instance", interval.state, fid, interval.start_min, local_idx)
                latm = active_lat_mult(interval, fid)
                simulate_flow_instance(rows, interval.state, fdef, fid, st, instance_idx, latm)

    # One-shots
    for ev in SCENARIO["scenario"]["phases"]["f"]["events"]:
        at_min = int(ev["at_min"])
        when = BASE_TIME + timedelta(minutes=at_min)
        for os in ev.get("one_shots", []) or []:
            ref = os["ref"]
            cnt = int(os["count"])
            hosts = list(os.get("hosts") or [])
            comp_id, log_id = parse_ref(ref)
            comp_hosts = SYSTEM["components"][comp_id].get("hosts") or [""]
            allowed_hosts = hosts if hosts else comp_hosts
            if not allowed_hosts:
                allowed_hosts = [""]

            for i in range(cnt):
                rr = rng_for("oneshot", ref, at_min, i)
                t = when + timedelta(milliseconds=rr.random() * 600.0)
                host = allowed_hosts[i % len(allowed_hosts)]
                rows.append(render_log(comp_id, log_id, "f", t, host, bound={}))

    df = pd.DataFrame(rows)
    df["timestamp_dt"] = df["timestamp"]
    df = df.sort_values("timestamp_dt", kind="mergesort").drop(columns=["timestamp_dt"]).reset_index(drop=True)
    df["timestamp"] = df["timestamp"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
