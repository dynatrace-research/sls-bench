import math
import uuid
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional, DefaultDict
from collections import defaultdict

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "id": "pagerduty_notification_platform_2013",
    "states": ["n", "f"],
    "tracing": {"on": True, "trace_id_len": 32},
    "components": [
        {
            "id": "api_gateway",
            "svc": "api-gateway",
            "hosts": ["api-r1-1", "api-r2-1", "api-l1-1"],
            "logs": {
                "http_events_202": {
                    "lvl": "INFO",
                    "msg": "POST /v1/events status=202 dur_ms={dur_ms} client_ip={client_ip} trace_id={trace_id}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [10, 80]}},
                        "f": {"dur_ms": {"k": "i", "v": [20, 250]}},
                    },
                },
                "http_incident_200": {
                    "lvl": "INFO",
                    "msg": "GET /v1/incidents/{inc_id} status=200 dur_ms={dur_ms}",
                    "vars": {"inc_id": {"k": "i", "v": [1000, 9999]}},
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [20, 150]}},
                        "f": {"dur_ms": {"k": "i", "v": [40, 800]}},
                    },
                },
                "http_incident_500": {
                    "lvl": "ERROR",
                    "msg": "GET /v1/incidents/{inc_id} status=500 dur_ms={dur_ms} upstream={upstream}",
                    "vars": {
                        "inc_id": {"k": "i", "v": [1000, 9999]},
                        "upstream": {"k": "ch", "v": ["incident_svc"]},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [200, 1500]}},
                        "f": {"dur_ms": {"k": "i", "v": [300, 5000]}},
                    },
                },
                "http_server_metric": {
                    "lvl": "DEBUG",
                    "msg": "http metrics active_conns={active_conns} req_per_min={req_per_min}",
                    "vars": {
                        "active_conns": {"k": "i", "v": [50, 900]},
                        "req_per_min": {"k": "i", "v": [100, 800]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [{"id": "http_server_metric", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "http_server_metric", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        {
            "id": "event_ingest",
            "svc": "event-ingest",
            "hosts": ["ingest-r1-1", "ingest-r2-1"],
            "logs": {
                "event_ingested": {
                    "lvl": "INFO",
                    "msg": "event ingested event_id={event_id} source={source} trace_id={trace_id}",
                    "vars": {
                        "event_id": {"k": "uuid", "v": None},
                        "source": {"k": "ch", "v": ["api", "integrations", "email"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "ingest_lag_metric": {
                    "lvl": "INFO",
                    "msg": "ingest metrics lag_s={lag_s} inflight={inflight}",
                    "vars": {},
                    "state_vars": {
                        "n": {"lag_s": {"k": "f", "v": [0.0, 2.0]}, "inflight": {"k": "i", "v": [0, 500]}},
                        "f": {"lag_s": {"k": "f", "v": [0.0, 10.0]}, "inflight": {"k": "i", "v": [0, 3000]}},
                    },
                },
            },
            "beh": {
                "n": [{"id": "ingest_lag_metric", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "ingest_lag_metric", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        {
            "id": "event_queue",
            "svc": "event-queue",
            "hosts": ["queue-l1-1"],
            "logs": {
                "queue_depth_metric": {
                    "lvl": "INFO",
                    "msg": "queue depth depth={depth} oldest_age_s={oldest_age_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {"depth": {"k": "i", "v": [0, 2000]}, "oldest_age_s": {"k": "i", "v": [0, 60]}},
                        "f": {"depth": {"k": "i", "v": [0, 20000]}, "oldest_age_s": {"k": "i", "v": [0, 3600]}},
                    },
                },
                "queue_depth_snapshot": {
                    "lvl": "WARN",
                    "msg": "queue depth snapshot depth={depth} oldest_age_s={oldest_age_s}",
                    "vars": {
                        "depth": {"k": "i", "v": [2000, 20000]},
                        "oldest_age_s": {"k": "i", "v": [60, 3600]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [{"id": "queue_depth_metric", "per_min": 1.0, "scope": "global"}],
                "f": [{"id": "queue_depth_metric", "per_min": 1.0, "scope": "global"}],
            },
        },
        {
            "id": "coordinator",
            "svc": "coordinator",
            "hosts": ["coord-r1-1", "coord-r2-1", "coord-l1-1"],
            "logs": {
                "quorum_ok": {
                    "lvl": "INFO",
                    "msg": "quorum ok leader={leader} members_up={members_up} rtt_ms={rtt_ms}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["coord-r1-1", "coord-r2-1", "coord-l1-1"]},
                        "members_up": {"k": "i", "v": [2, 3]},
                    },
                    "state_vars": {"n": {"rtt_ms": {"k": "i", "v": [5, 40]}}, "f": {"rtt_ms": {"k": "i", "v": [80, 2500]}}},
                },
                "quorum_fail": {
                    "lvl": "ERROR",
                    "msg": "quorum unavailable members_up={members_up} rtt_ms={rtt_ms} reason={reason}",
                    "vars": {
                        "members_up": {"k": "i", "v": [0, 1]},
                        "rtt_ms": {"k": "i", "v": [200, 4000]},
                        "reason": {"k": "ch", "v": ["no_majority", "session_expired"]},
                    },
                    "state_vars": {},
                },
                "leader_heartbeat": {
                    "lvl": "DEBUG",
                    "msg": "leader heartbeat state={state} zxid={zxid}",
                    "vars": {"state": {"k": "ch", "v": ["leading", "following"]}, "zxid": {"k": "hex", "v": 8}},
                    "state_vars": {},
                },
                "session_expired": {
                    "lvl": "WARN",
                    "msg": "session expired peer={peer} rtt_ms={rtt_ms}",
                    "vars": {"peer": {"k": "ch", "v": ["coord-r1-1", "coord-r2-1", "coord-l1-1"]}},
                    "state_vars": {"n": {"rtt_ms": {"k": "i", "v": [10, 80]}}, "f": {"rtt_ms": {"k": "i", "v": [150, 4000]}}},
                },
                "quorum_lost": {
                    "lvl": "ERROR",
                    "msg": "lost quorum view={view_id} members_up={members_up}",
                    "vars": {"view_id": {"k": "hex", "v": 6}, "members_up": {"k": "i", "v": [0, 1]}},
                    "state_vars": {},
                },
                "config_warn": {
                    "lvl": "WARN",
                    "msg": "config warning key={key} value={value}",
                    "vars": {
                        "key": {"k": "ch", "v": ["tick_time_ms", "init_limit", "sync_limit", "session_timeout_ms"]},
                        "value": {"k": "str", "v": "numeric_string"},
                    },
                    "state_vars": {},
                },
                "config_reload": {
                    "lvl": "INFO",
                    "msg": "reloaded config session_timeout_ms {old}->{new}",
                    "vars": {"old": {"k": "i", "v": [2000, 10000]}, "new": {"k": "i", "v": [5000, 20000]}},
                    "state_vars": {},
                },
                "proc_restart": {
                    "lvl": "INFO",
                    "msg": "restarted coordinator process reason={reason} pid={pid}",
                    "vars": {"reason": {"k": "ch", "v": ["manual_restart", "watcher", "crashloop"]}, "pid": {"k": "i", "v": [1000, 50000]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [
                    {"id": "leader_heartbeat", "per_min": 1.0, "scope": "per_host"},
                    {"id": "session_expired", "per_min": 0.05, "scope": "per_host"},
                    {"id": "config_warn", "per_min": 0.02, "scope": "per_host"},
                ],
                "f": [
                    {"id": "leader_heartbeat", "per_min": 1.0, "scope": "per_host"},
                    {"id": "session_expired", "per_min": 0.25, "scope": "per_host"},
                    {"id": "config_warn", "per_min": 0.05, "scope": "per_host"},
                ],
            },
        },
        {
            "id": "dispatch_worker",
            "svc": "notif-dispatch",
            "hosts": ["dispatch-r1-1", "dispatch-r2-1", "dispatch-l1-1"],
            "logs": {
                "job_start": {
                    "lvl": "INFO",
                    "msg": "notif job start notif_id={notif_id} channel={channel} trace_id={trace_id}",
                    "vars": {"notif_id": {"k": "hex", "v": 16}, "channel": {"k": "ch", "v": ["sms", "email", "phone", "push"]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {},
                },
                "job_done_success": {
                    "lvl": "INFO",
                    "msg": "notif job done outcome=sent provider={provider} attempts={attempts} total_ms={total_ms} trace_id={trace_id}",
                    "vars": {"provider": {"k": "ch", "v": ["twilio", "sendgrid", "apns", "nexmo"]}, "attempts": {"k": "i", "v": [1, 2]}, "total_ms": {"k": "i", "v": [80, 900]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {},
                },
                "job_done_degraded": {
                    "lvl": "WARN",
                    "msg": "notif job done outcome={outcome} provider={provider} attempts={attempts} total_ms={total_ms} trace_id={trace_id}",
                    "vars": {
                        "outcome": {"k": "ch", "v": ["sent", "provider_timeout", "provider_5xx"]},
                        "provider": {"k": "ch", "v": ["twilio", "sendgrid", "apns", "nexmo"]},
                        "attempts": {"k": "i", "v": [1, 3]},
                        "total_ms": {"k": "i", "v": [200, 9000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "job_done_backlog": {
                    "lvl": "INFO",
                    "msg": "notif job done outcome=sent provider={provider} attempts={attempts} queue_age_s={queue_age_s} total_ms={total_ms} trace_id={trace_id}",
                    "vars": {
                        "provider": {"k": "ch", "v": ["twilio", "sendgrid", "apns", "nexmo"]},
                        "attempts": {"k": "i", "v": [1, 3]},
                        "queue_age_s": {"k": "i", "v": [30, 1800]},
                        "total_ms": {"k": "i", "v": [300, 12000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {},
                },
                "job_requeued": {
                    "lvl": "WARN",
                    "msg": "notif requeued reason=no_quorum queue_age_s={queue_age_s} trace_id={trace_id}",
                    "vars": {"queue_age_s": {"k": "i", "v": [30, 1800]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {},
                },
                "dispatch_node_restart": {
                    "lvl": "INFO",
                    "msg": "restart dispatch node host={host} result={result}",
                    "vars": {"host": {"k": "ch", "v": ["dispatch-r1-1", "dispatch-r2-1", "dispatch-l1-1"]}, "result": {"k": "ch", "v": ["success", "failed"]}},
                    "state_vars": {},
                },
                "worker_pool_metric": {
                    "lvl": "INFO",
                    "msg": "worker pool active={active} queued_jobs={queued_jobs}",
                    "vars": {"active": {"k": "i", "v": [0, 30]}, "queued_jobs": {"k": "i", "v": [0, 8000]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [{"id": "worker_pool_metric", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "worker_pool_metric", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "health_tick": {
                    "lvl": "DEBUG",
                    "msg": "monitor tick checks_ok={checks_ok} checks_fail={checks_fail}",
                    "vars": {"checks_ok": {"k": "i", "v": [20, 40]}, "checks_fail": {"k": "i", "v": [0, 8]}},
                    "state_vars": {},
                },
                "alert_multi_provider_failure": {
                    "lvl": "ERROR",
                    "msg": "alert multi-provider notification failures rate={fail_rate} latency_p95_ms={lat_p95_ms} sev={sev}",
                    "vars": {"fail_rate": {"k": "f", "v": [0.05, 0.9]}, "lat_p95_ms": {"k": "i", "v": [500, 10000]}, "sev": {"k": "ch", "v": ["warning", "critical"]}},
                    "state_vars": {},
                },
                "oncall_page_sent": {
                    "lvl": "INFO",
                    "msg": "sent page to oncall reason={reason} sev={sev}",
                    "vars": {"reason": {"k": "ch", "v": ["dispatch_node_down", "multi_provider_failure", "dispatch_quorum_lost"]}, "sev": {"k": "ch", "v": ["sev2", "sev1"]}},
                    "state_vars": {},
                },
                "incident_sev_set": {
                    "lvl": "INFO",
                    "msg": "incident severity set to {sev} by={by}",
                    "vars": {"sev": {"k": "ch", "v": ["sev2", "sev1"]}, "by": {"k": "ch", "v": ["oncall", "incident_cmd"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [{"id": "health_tick", "per_min": 1.0, "scope": "global"}, {"id": "alert_multi_provider_failure", "per_min": 0.0, "scope": "global"}],
                "f": [{"id": "health_tick", "per_min": 1.0, "scope": "global"}, {"id": "alert_multi_provider_failure", "per_min": 0.6, "scope": "global"}],
            },
        },
        {
            "id": "net_probe",
            "svc": "net-probe",
            "hosts": ["netmon-r1", "netmon-r2", "netmon-l1"],
            "logs": {
                "ping_metric": {
                    "lvl": "INFO",
                    "msg": "ping src={src} dst={dst} rtt_ms={rtt_ms} loss_pct={loss_pct}",
                    "vars": {"src": {"k": "ch", "v": ["aws_r1", "aws_r2", "linode"]}, "dst": {"k": "ch", "v": ["aws_r1", "aws_r2", "linode"]}},
                    "state_vars": {"n": {"rtt_ms": {"k": "i", "v": [10, 80]}, "loss_pct": {"k": "f", "v": [0.0, 2.0]}}, "f": {"rtt_ms": {"k": "i", "v": [80, 2500]}, "loss_pct": {"k": "f", "v": [0.0, 30.0]}}},
                },
                "route_warn": {
                    "lvl": "WARN",
                    "msg": "path degradation detected src={src} dst={dst} sample_rtt_ms={rtt_ms}",
                    "vars": {"src": {"k": "ch", "v": ["aws_r1", "aws_r2", "linode"]}, "dst": {"k": "ch", "v": ["aws_r1", "aws_r2", "linode"]}, "rtt_ms": {"k": "i", "v": [200, 4000]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": [{"id": "ping_metric", "per_min": 1.0, "scope": "per_host"}, {"id": "route_warn", "per_min": 0.02, "scope": "per_host"}],
                "f": [{"id": "ping_metric", "per_min": 1.5, "scope": "per_host"}, {"id": "route_warn", "per_min": 0.1, "scope": "per_host"}],
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "post_event",
                "rpm": 300.0,
                "emit": ["api_gateway.http_events_202", "event_ingest.event_ingested"],
                "latency_ms": [[20, 70], [10, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "get_incident_200",
                "rpm": 30.0,
                "emit": ["api_gateway.http_incident_200"],
                "latency_ms": [[40, 150]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "dispatch_notification",
                "rpm": 200.0,
                "emit": ["dispatch_worker.job_start", "coordinator.quorum_ok", "dispatch_worker.job_done_success"],
                "latency_ms": [[5, 20], [20, 120], [80, 900]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "post_event",
                "rpm": 300.0,
                "emit": ["api_gateway.http_events_202", "event_ingest.event_ingested"],
                "latency_ms": [[35, 120], [15, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "get_incident_200",
                "rpm": 20.0,
                "emit": ["api_gateway.http_incident_200"],
                "latency_ms": [[80, 600]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "get_incident_500",
                "rpm": 10.0,
                "emit": ["api_gateway.http_incident_500"],
                "latency_ms": [[300, 5000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "dispatch_degraded",
                "rpm": 180.0,
                "emit": ["dispatch_worker.job_start", "coordinator.quorum_ok", "dispatch_worker.job_done_degraded"],
                "latency_ms": [[5, 30], [120, 1500], [300, 9000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "dispatch_blocked_no_quorum",
                "rpm": 20.0,
                "emit": ["dispatch_worker.job_start", "coordinator.quorum_fail", "dispatch_worker.job_requeued"],
                "latency_ms": [[5, 30], [200, 2500], [20, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "dispatch_backlog_drain",
                "rpm": 250.0,
                "emit": ["dispatch_worker.job_start", "coordinator.quorum_ok", "dispatch_worker.job_done_backlog"],
                "latency_ms": [[5, 30], [150, 2000], [500, 12000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}


SCENARIO: Dict[str, Any] = {
    "id": "incident_2013_04_13_peering_quorum_loss",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "net_probe.ping_metric": 6.0,
                        "coordinator.session_expired": 2.5,
                        "monitoring.alert_multi_provider_failure": 0.0,
                        "dispatch_blocked_no_quorum": 0.0,
                        "dispatch_backlog_drain": 0.0,
                    },
                    "latency_multipliers": {"dispatch_degraded": {"p50": 1.5, "p95": 2.0}, "get_incident_500": {"p50": 1.2, "p95": 1.5}},
                    "one_shots": [{"ref": "monitoring.oncall_page_sent", "count": 1, "hosts": ["mon-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 22,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "dispatch_worker.dispatch_node_restart", "count": 1, "hosts": ["dispatch-r1-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 27,
                    "rate_multipliers": {"monitoring.alert_multi_provider_failure": 1.5},
                    "latency_multipliers": {"dispatch_degraded": {"p50": 2.0, "p95": 3.0}},
                    "one_shots": [
                        {"ref": "monitoring.incident_sev_set", "count": 1, "hosts": ["mon-1"]},
                        {"ref": "monitoring.oncall_page_sent", "count": 2, "hosts": ["mon-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 32,
                    "rate_multipliers": {"dispatch_degraded": 0.0, "dispatch_blocked_no_quorum": 9.0, "coordinator.session_expired": 4.0},
                    "latency_multipliers": {"dispatch_blocked_no_quorum": {"p50": 1.2, "p95": 1.5}},
                    "one_shots": [
                        {"ref": "monitoring.incident_sev_set", "count": 1, "hosts": ["mon-1"]},
                        {"ref": "monitoring.oncall_page_sent", "count": 1, "hosts": ["mon-1"]},
                        {"ref": "coordinator.quorum_lost", "count": 3, "hosts": ["coord-r1-1", "coord-r2-1", "coord-l1-1"]},
                        {"ref": "event_queue.queue_depth_snapshot", "count": 1, "hosts": ["queue-l1-1"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 40,
                    "rate_multipliers": {"dispatch_blocked_no_quorum": 0.2, "dispatch_backlog_drain": 2.0, "coordinator.session_expired": 1.5, "net_probe.ping_metric": 2.5},
                    "latency_multipliers": {"dispatch_backlog_drain": {"p50": 1.0, "p95": 1.2}},
                    "one_shots": [
                        {"ref": "coordinator.config_reload", "count": 1, "hosts": ["coord-l1-1"]},
                        {"ref": "coordinator.proc_restart", "count": 3, "hosts": ["coord-r1-1", "coord-r2-1", "coord-l1-1"]},
                        {"ref": "event_queue.queue_depth_snapshot", "count": 1, "hosts": ["queue-l1-1"]},
                    ],
                },
            ]
        }
    },
}


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


def _hex_from_rng(rng: np.random.Generator, n: int) -> str:
    arr = rng.integers(0, 16, size=n, dtype=np.int64)
    return "".join("0123456789abcdef"[int(x)] for x in arr)


def _uuid_from_rng(rng: np.random.Generator) -> str:
    b = bytearray(rng.integers(0, 256, size=16, dtype=np.uint8).tobytes())
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def _stable_int_hash(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big", signed=False)


def _clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _dt_to_iso_z_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def _alloc_count(expected_per_min: float, residuals: Dict[str, float], key: str) -> int:
    r = residuals.get(key, 0.0) + float(expected_per_min)
    n = int(math.floor(r + 1e-12))
    residuals[key] = r - n
    return n


def _spread_times(rng: np.random.Generator, start: datetime, end: datetime, count: int, jitter_ms: int = 180) -> List[datetime]:
    if count <= 0:
        return []
    span_ms = max(1, int((end - start).total_seconds() * 1000))
    times: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        base_off = int(frac * span_ms)
        jitter = int(rng.normal(0.0, jitter_ms / 3.0))
        off = int(_clip(base_off + jitter, 0, span_ms - 1))
        times.append(start + timedelta(milliseconds=off))
    times.sort()
    return times


def _lognormal_from_p50_p95(rng: np.random.Generator, p50: float, p95: float, soft_cap: Optional[float] = None) -> float:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    x = float(rng.lognormal(mean=mu, sigma=max(1e-6, sigma)))
    if soft_cap is None:
        soft_cap = 3.0 * p95
    if x > soft_cap:
        x = soft_cap
    if x < 1.0:
        x = 1.0
    return x


def _choose_domain_value(rng: np.random.Generator, dom: Dict[str, Any]) -> Any:
    k = dom["k"]
    v = dom.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(round((rng.random() * (hi - lo) + lo), 2))
    if k == "ch":
        return str(rng.choice(list(v)))
    if k == "uuid":
        return _uuid_from_rng(rng)
    if k == "hex":
        n = int(v)
        return _hex_from_rng(rng, n)
    if k == "ip":
        return f"198.51.100.{int(rng.integers(1, 255))}"
    if k == "str":
        if v == "numeric_string":
            return str(int(rng.integers(1, 20000)))
        return str(v) if v is not None else "str"
    return ""


def _resolve_template(templates: Dict[Tuple[str, str], LogTemplate], ref: str) -> LogTemplate:
    comp_id, log_id = ref.split(".", 1)
    return templates[(comp_id, log_id)]


def _render_from_template(
    rng: np.random.Generator,
    tpl: LogTemplate,
    state: str,
    bound_values: Dict[str, Any],
    extra_values: Optional[Dict[str, Any]] = None,
) -> str:
    values: Dict[str, Any] = {}
    if extra_values:
        values.update(extra_values)

    for k, dom in tpl.vars.items():
        if k in bound_values:
            values[k] = bound_values[k]
        elif k in values:
            pass
        else:
            values[k] = _choose_domain_value(rng, dom)

    if tpl.state_vars and state in tpl.state_vars:
        for k, dom in tpl.state_vars[state].items():
            if k in bound_values:
                values[k] = bound_values[k]
            elif k in values:
                pass
            else:
                values[k] = _choose_domain_value(rng, dom)

    return tpl.msg.format(**values)


def main() -> None:
    random.seed(1337)
    rng = np.random.default_rng(1337)

    base_time = datetime(2013, 4, 13, 0, 0, 0, tzinfo=timezone.utc)

    components: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

    templates: Dict[Tuple[str, str], LogTemplate] = {}
    for c in SYSTEM["components"]:
        cid = c["id"]
        for lid, td in c["logs"].items():
            templates[(cid, lid)] = LogTemplate(
                component_id=cid,
                log_id=lid,
                lvl=td["lvl"],
                msg=td["msg"],
                vars=td.get("vars", {}),
                state_vars=td.get("state_vars", {}),
            )

    events_f = list(SCENARIO["phases"]["f"]["events"])
    events_f.sort(key=lambda e: (e["at_min"], e.get("order", 0)))
    events_by_min: DefaultDict[int, List[Dict[str, Any]]] = defaultdict(list)
    for e in events_f:
        events_by_min[int(e["at_min"])].append(e)

    rate_mult_flow: Dict[str, float] = defaultdict(lambda: 1.0)
    rate_mult_bg: Dict[str, float] = defaultdict(lambda: 1.0)
    lat_mult_flow: Dict[str, Tuple[float, float]] = defaultdict(lambda: (1.0, 1.0))

    residuals: Dict[str, float] = {}

    backlog_depth = 250
    oldest_age_s = 10

    rows: List[Tuple[datetime, str, str, str, str, str]] = []

    total_minutes = int(SCENARIO["time"]["total_minutes"])
    failure_start = int(SCENARIO["time"]["phases"]["f"]["start_min"])

    def choose_host_for_component(comp_id: str, instance_key: str) -> str:
        hs = components[comp_id]["hosts"]
        if not hs:
            return ""
        idx = _stable_int_hash(f"{comp_id}|{instance_key}") % len(hs)
        return hs[idx]

    def emit_row(ts: datetime, tpl: LogTemplate, msg: str, trace_id: str, host: str) -> None:
        svc = components[tpl.component_id].get("svc") or ""
        rows.append((ts, tpl.lvl, msg, trace_id, svc, host))

    def clamp_int_to_domain(x: int, dom: Dict[str, Any]) -> int:
        if dom["k"] != "i":
            return x
        lo, hi = int(dom["v"][0]), int(dom["v"][1])
        return int(_clip(x, lo, hi))

    def backlog_update_for_minute(minute: int, state: str) -> None:
        nonlocal backlog_depth, oldest_age_s
        if state == "n":
            backlog_depth = int(_clip(180 + 80 * math.sin(minute / 3.0), 0, 2000))
            oldest_age_s = int(_clip(8 + 6 * math.cos(minute / 4.0), 0, 60))
            return

        blocked_mult = float(rate_mult_flow.get("dispatch_blocked_no_quorum", 1.0))
        drain_mult = float(rate_mult_flow.get("dispatch_backlog_drain", 1.0))

        inc = 200
        if blocked_mult >= 1.0:
            inc += 2500
        else:
            inc += 600

        if drain_mult > 0.0:
            inc -= int(1100 * drain_mult)

        backlog_depth = int(_clip(backlog_depth + inc, 0, 20000))

        if inc > 0:
            oldest_age_s = int(_clip(oldest_age_s + 120, 0, 3600))
        else:
            oldest_age_s = int(_clip(oldest_age_s - 180, 0, 3600))

    def synth_values_for_background(comp_id: str, log_id: str, state: str, minute: int) -> Dict[str, Any]:
        key = f"{comp_id}.{log_id}"

        if key == "event_queue.queue_depth_metric":
            return {"depth": int(backlog_depth), "oldest_age_s": int(oldest_age_s)}

        if key == "event_ingest.ingest_lag_metric":
            if state == "n":
                lag = float(round(_clip(oldest_age_s / 40.0, 0.0, 2.0), 2))
                inflight = int(_clip(backlog_depth / 6.0, 0, 500))
            else:
                lag = float(round(_clip(oldest_age_s / 220.0, 0.0, 10.0), 2))
                inflight = int(_clip(backlog_depth / 5.0, 0, 3000))
            return {"lag_s": lag, "inflight": inflight}

        if key == "dispatch_worker.worker_pool_metric":
            queued_jobs = int(_clip(backlog_depth * (0.2 if state == "n" else 0.45), 0, 8000))
            active = int(_clip(18 if state == "n" else 12, 0, 30))
            active = int(_clip(active + int(3 * math.sin(minute / 2.0)), 0, 30))
            return {"queued_jobs": queued_jobs, "active": active}

        if key == "net_probe.ping_metric":
            mult = float(rate_mult_bg.get("net_probe.ping_metric", 1.0)) if state == "f" else 1.0
            pairs = [
                ("aws_r1", "aws_r2"),
                ("aws_r2", "aws_r1"),
                ("aws_r1", "linode"),
                ("aws_r2", "linode"),
                ("linode", "aws_r1"),
                ("linode", "aws_r2"),
            ]
            src, dst = pairs[int(rng.integers(0, len(pairs)))]
            if state == "n":
                rtt = int(rng.integers(10, 81))
                loss = float(round(rng.random() * 1.5, 2))
            else:
                base_rtt = int(_clip(120 + (mult - 1.0) * 140.0, 80, 2500))
                rtt = int(_clip(base_rtt + rng.normal(0.0, 160.0), 80, 2500))
                base_loss = float(_clip((mult - 1.0) * 2.0, 0.0, 30.0))
                loss = float(round(_clip(base_loss + rng.normal(0.0, 2.0), 0.0, 30.0), 2))
            return {"src": src, "dst": dst, "rtt_ms": rtt, "loss_pct": loss}

        if key == "monitoring.alert_multi_provider_failure":
            if minute >= 32:
                sev = "critical"
                lat = int(_clip(7000 + rng.normal(0.0, 1200.0), 500, 10000))
                rate = float(round(_clip(0.35 + rng.normal(0.0, 0.12), 0.05, 0.9), 2))
            else:
                sev = "warning"
                lat = int(_clip(2500 + rng.normal(0.0, 700.0), 500, 10000))
                rate = float(round(_clip(0.18 + rng.normal(0.0, 0.08), 0.05, 0.9), 2))
            return {"sev": sev, "lat_p95_ms": lat, "fail_rate": rate}

        if key == "monitoring.health_tick":
            if state == "n":
                checks_fail = int(_clip(rng.integers(0, 2), 0, 8))
            else:
                checks_fail = int(_clip(rng.integers(1, 7), 0, 8))
            checks_ok = int(_clip(rng.integers(25, 41) - checks_fail, 20, 40))
            return {"checks_ok": checks_ok, "checks_fail": checks_fail}

        if key == "coordinator.session_expired":
            peer = str(rng.choice(components["coordinator"]["hosts"]))
            if state == "n":
                rtt = int(_clip(20 + rng.normal(0.0, 12.0), 10, 80))
            else:
                mult = float(rate_mult_bg.get("coordinator.session_expired", 1.0))
                base = int(_clip(240 + (mult - 1.0) * 240.0, 150, 4000))
                rtt = int(_clip(base + rng.normal(0.0, 260.0), 150, 4000))
            return {"peer": peer, "rtt_ms": rtt}

        return {}

    def emit_one_shots(event: Dict[str, Any], event_time: datetime, minute: int) -> None:
        for ospec in event.get("one_shots", []):
            ref = ospec["ref"]
            count = int(ospec["count"])
            allowed_hosts = list(ospec.get("hosts") or [])
            tpl = _resolve_template(templates, ref)
            comp_id = tpl.component_id

            ts_list = _spread_times(rng, event_time, event_time + timedelta(seconds=3), count, jitter_ms=80)
            for i in range(count):
                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]
                else:
                    hs = components[comp_id]["hosts"]
                    host = hs[i % len(hs)] if hs else ""

                trace_id = ""
                bound: Dict[str, Any] = {}
                key = f"{comp_id}.{tpl.log_id}"
                if key == "monitoring.oncall_page_sent":
                    if minute >= 32:
                        bound["reason"] = "dispatch_quorum_lost"
                        bound["sev"] = "sev1"
                    else:
                        bound["reason"] = "multi_provider_failure"
                        bound["sev"] = "sev2"
                elif key == "monitoring.incident_sev_set":
                    if minute >= 32:
                        bound["sev"] = "sev1"
                        bound["by"] = "incident_cmd"
                    else:
                        bound["sev"] = "sev2"
                        bound["by"] = "oncall"
                elif key == "dispatch_worker.dispatch_node_restart":
                    bound["host"] = host
                    bound["result"] = "failed"
                elif key == "coordinator.config_reload":
                    bound["old"] = 4000
                    bound["new"] = 12000
                elif key == "coordinator.proc_restart":
                    bound["reason"] = "manual_restart"
                    bound["pid"] = int(1000 + (i + 1) * 1137)
                elif key == "coordinator.quorum_lost":
                    bound["members_up"] = 1
                    bound["view_id"] = _hex_from_rng(rng, 6)
                elif key == "event_queue.queue_depth_snapshot":
                    bound["depth"] = int(_clip(backlog_depth, 2000, 20000))
                    bound["oldest_age_s"] = int(_clip(oldest_age_s, 60, 3600))

                msg = _render_from_template(rng, tpl, state="f", bound_values=bound)
                emit_row(ts_list[i], tpl, msg, trace_id, host)

    def simulate_flow_instance(flow: Dict[str, Any], start_ts: datetime, state: str, minute: int, lat_mult: Tuple[float, float]) -> None:
        flow_id = flow["id"]
        emit_refs: List[str] = list(flow["emit"])
        latency_pairs: List[List[float]] = list(flow["latency_ms"])
        assert len(emit_refs) == len(latency_pairs)

        trace_id = _hex_from_rng(rng, SYSTEM["tracing"]["trace_id_len"]) if flow.get("trace") else ""
        instance_key = f"{flow_id}|{minute}|{trace_id or _hex_from_rng(rng, 8)}|{int(start_ts.timestamp()*1000)}"

        comp_hosts: Dict[str, str] = {}
        for ref in emit_refs:
            cid, _ = ref.split(".", 1)
            if cid not in comp_hosts:
                comp_hosts[cid] = choose_host_for_component(cid, instance_key)

        delays_ms: List[int] = []
        for p50, p95 in latency_pairs:
            p50s = float(p50) * float(lat_mult[0])
            p95s = float(p95) * float(lat_mult[1])
            x = _lognormal_from_p50_p95(rng, p50s, p95s, soft_cap=3.0 * p95s)
            delays_ms.append(int(round(x)))

        bound_common: Dict[str, Any] = {}
        if trace_id:
            bound_common["trace_id"] = trace_id

        if flow_id == "post_event":
            bound_common["client_ip"] = f"203.0.113.{int(rng.integers(1, 255))}"
            bound_common["event_id"] = _uuid_from_rng(rng)
            bound_common["source"] = str(rng.choice(["api", "integrations", "email"]))
            tpl0 = _resolve_template(templates, emit_refs[0])
            dur_dom = tpl0.state_vars.get(state, {}).get("dur_ms")
            dur = int(delays_ms[0])
            if dur_dom:
                dur = clamp_int_to_domain(dur, dur_dom)
                delays_ms[0] = int(dur)  # keep scheduling + rendered dur_ms coherent
            bound_common["dur_ms"] = dur

        elif flow_id in ("get_incident_200", "get_incident_500"):
            bound_common["inc_id"] = int(rng.integers(1000, 10000))
            tpl0 = _resolve_template(templates, emit_refs[0])
            dur_dom = tpl0.state_vars.get(state, {}).get("dur_ms")
            dur = int(delays_ms[0])
            if dur_dom:
                dur = clamp_int_to_domain(dur, dur_dom)
                delays_ms[0] = int(dur)  # keep scheduling + rendered dur_ms coherent
            bound_common["dur_ms"] = dur
            if flow_id == "get_incident_500":
                bound_common["upstream"] = "incident_svc"

        elif flow_id in ("dispatch_notification", "dispatch_degraded", "dispatch_backlog_drain", "dispatch_blocked_no_quorum"):
            bound_common["notif_id"] = _hex_from_rng(rng, 16)
            bound_common["channel"] = str(rng.choice(["sms", "email", "phone", "push"]))

            # RTT is logged by coordinator in quorum_ok/quorum_fail and must be compatible with the
            # actual timestamp gap between job_start and quorum check (= delays_ms[1]).
            if len(delays_ms) >= 2:
                derived_rtt = int(_clip(delays_ms[1] * float(_clip(0.55 + rng.random() * 0.35, 0.55, 0.9)), 1, 5000))
            else:
                derived_rtt = int(rng.integers(10, 100))

            if flow_id == "dispatch_notification":
                bound_common["provider"] = str(rng.choice(["twilio", "sendgrid", "apns", "nexmo"]))
                bound_common["attempts"] = int(1 if rng.random() < 0.88 else 2)

                rtt_ms = int(_clip(derived_rtt, 5, 40))
                if len(delays_ms) > 1 and rtt_ms > delays_ms[1]:
                    delays_ms[1] = int(rtt_ms)
                bound_common["rtt_ms"] = int(rtt_ms)
                bound_common["members_up"] = 3

                total_min, total_max = 80, 900
                d1 = delays_ms[1] if len(delays_ms) > 1 else 20
                d2 = delays_ms[2] if len(delays_ms) > 2 else 80
                total = d1 + d2
                if total > total_max:
                    d2 = max(1, total_max - d1)
                    if d2 < 1:
                        d1 = max(1, total_max - 1)
                        d2 = 1
                total = d1 + d2
                if total < total_min:
                    d2 = max(d2, total_min - d1)
                    total = d1 + d2
                if len(delays_ms) > 1:
                    delays_ms[1] = int(d1)
                if len(delays_ms) > 2:
                    delays_ms[2] = int(d2)
                bound_common["total_ms"] = int(_clip(total, total_min, total_max))

            elif flow_id == "dispatch_degraded":
                bound_common["provider"] = str(rng.choice(["twilio", "sendgrid", "apns", "nexmo"]))
                if minute >= 27:
                    p_sent = 0.68
                    p_timeout = 0.22
                else:
                    p_sent = 0.80
                    p_timeout = 0.12
                u = rng.random()
                if u < p_sent:
                    outcome = "sent"
                elif u < p_sent + p_timeout:
                    outcome = "provider_timeout"
                else:
                    outcome = "provider_5xx"
                bound_common["outcome"] = outcome
                bound_common["attempts"] = int(rng.integers(1, 4))

                rtt_ms = int(_clip(derived_rtt, 80, 2500))
                if len(delays_ms) > 1 and rtt_ms > delays_ms[1]:
                    delays_ms[1] = int(rtt_ms)
                bound_common["rtt_ms"] = int(rtt_ms)
                bound_common["members_up"] = 2

                total_min, total_max = 200, 9000
                d1 = delays_ms[1] if len(delays_ms) > 1 else 120
                d2 = delays_ms[2] if len(delays_ms) > 2 else 300
                total = d1 + d2
                if total > total_max:
                    d2 = max(1, total_max - d1)
                    if d2 < 1:
                        d1 = max(1, total_max - 1)
                        d2 = 1
                total = d1 + d2
                if total < total_min:
                    d2 = max(d2, total_min - d1)
                    total = d1 + d2
                if len(delays_ms) > 1:
                    delays_ms[1] = int(d1)
                if len(delays_ms) > 2:
                    delays_ms[2] = int(d2)
                bound_common["total_ms"] = int(_clip(total, total_min, total_max))

            elif flow_id == "dispatch_backlog_drain":
                bound_common["provider"] = str(rng.choice(["twilio", "sendgrid", "apns", "nexmo"]))
                bound_common["attempts"] = int(rng.integers(1, 4))
                qage = int(_clip(oldest_age_s, 30, 1800))
                bound_common["queue_age_s"] = qage

                rtt_ms = int(_clip(derived_rtt, 80, 2500))
                if len(delays_ms) > 1 and rtt_ms > delays_ms[1]:
                    delays_ms[1] = int(rtt_ms)
                bound_common["rtt_ms"] = int(rtt_ms)
                bound_common["members_up"] = 2

                total_min, total_max = 300, 12000
                d1 = delays_ms[1] if len(delays_ms) > 1 else 150
                d2 = delays_ms[2] if len(delays_ms) > 2 else 500
                total = d1 + d2
                if total > total_max:
                    d2 = max(1, total_max - d1)
                    if d2 < 1:
                        d1 = max(1, total_max - 1)
                        d2 = 1
                total = d1 + d2
                if total < total_min:
                    d2 = max(d2, total_min - d1)
                    total = d1 + d2
                if len(delays_ms) > 1:
                    delays_ms[1] = int(d1)
                if len(delays_ms) > 2:
                    delays_ms[2] = int(d2)
                bound_common["total_ms"] = int(_clip(total, total_min, total_max))

            elif flow_id == "dispatch_blocked_no_quorum":
                qage = int(_clip(oldest_age_s, 30, 1800))
                bound_common["queue_age_s"] = qage
                bound_common["members_up"] = int(rng.choice([0, 1]))
                bound_common["reason"] = str(rng.choice(["no_majority", "session_expired"]))

                rtt_ms = int(_clip(max(200, derived_rtt), 200, 4000))
                if len(delays_ms) > 1 and rtt_ms > delays_ms[1]:
                    delays_ms[1] = int(rtt_ms)
                bound_common["rtt_ms"] = int(rtt_ms)

        t = start_ts
        for i, ref in enumerate(emit_refs):
            t = t + timedelta(milliseconds=int(delays_ms[i]))
            tpl = _resolve_template(templates, ref)
            comp_id = tpl.component_id
            host = comp_hosts.get(comp_id, "")

            extra: Dict[str, Any] = {}
            if f"{tpl.component_id}.{tpl.log_id}" == "coordinator.quorum_ok":
                extra["leader"] = host if host else str(rng.choice(components["coordinator"]["hosts"]))
                if "members_up" in bound_common:
                    extra["members_up"] = bound_common["members_up"]
                if "rtt_ms" in bound_common:
                    extra["rtt_ms"] = int(bound_common["rtt_ms"])

            msg = _render_from_template(rng, tpl, state=state, bound_values=bound_common, extra_values=extra)
            emit_row(t, tpl, msg, trace_id, host)

    for minute in range(total_minutes):
        minute_start = base_time + timedelta(minutes=minute)
        minute_end = base_time + timedelta(minutes=minute + 1)

        state = "n" if minute < failure_start else "f"

        if state == "f":
            for ev in events_by_min.get(minute, []):
                for k, v in ev.get("rate_multipliers", {}).items():
                    if "." in k:
                        rate_mult_bg[k] = float(v)
                    else:
                        rate_mult_flow[k] = float(v)
                for fid, mults in ev.get("latency_multipliers", {}).items():
                    lat_mult_flow[fid] = (float(mults.get("p50", 1.0)), float(mults.get("p95", 1.0)))

            backlog_update_for_minute(minute, state)

            for ev in events_by_min.get(minute, []):
                emit_one_shots(ev, minute_start, minute)
        else:
            backlog_update_for_minute(minute, state)

        for comp in SYSTEM["components"]:
            comp_id = comp["id"]
            beh_list = comp.get("beh", {}).get(state, [])
            for b in beh_list:
                log_id = b["id"]
                per_min = float(b["per_min"])
                scope = b.get("scope", "per_host")
                src_key = f"{comp_id}.{log_id}"

                mult = float(rate_mult_bg.get(src_key, 1.0)) if state == "f" else 1.0
                eff_rate = per_min * mult

                tpl = templates[(comp_id, log_id)]
                if scope == "global":
                    n = _alloc_count(eff_rate, residuals, f"bg|{state}|{src_key}|global")
                    ts_list = _spread_times(rng, minute_start, minute_end, n, jitter_ms=220)
                    hs = components[comp_id]["hosts"]
                    for i, ts in enumerate(ts_list):
                        host = hs[i % len(hs)] if hs else ""
                        bound = synth_values_for_background(comp_id, log_id, state, minute)
                        msg = _render_from_template(rng, tpl, state=state, bound_values=bound)
                        emit_row(ts, tpl, msg, "", host)
                else:
                    for host in components[comp_id]["hosts"]:
                        n = _alloc_count(eff_rate, residuals, f"bg|{state}|{src_key}|{host}")
                        ts_list = _spread_times(rng, minute_start, minute_end, n, jitter_ms=220)
                        for ts in ts_list:
                            bound = synth_values_for_background(comp_id, log_id, state, minute)
                            msg = _render_from_template(rng, tpl, state=state, bound_values=bound)
                            emit_row(ts, tpl, msg, "", host)

        flows_this_state = SYSTEM["flows"][state]
        for flow in flows_this_state:
            flow_id = flow["id"]
            rpm = float(flow["rpm"])

            mult = float(rate_mult_flow.get(flow_id, 1.0)) if state == "f" else 1.0
            eff_rpm = rpm * mult

            n_instances = _alloc_count(eff_rpm, residuals, f"flow|{state}|{flow_id}")
            start_times = _spread_times(rng, minute_start, minute_end, n_instances, jitter_ms=140)

            lat_mult = (1.0, 1.0)
            if state == "f":
                lat_mult = lat_mult_flow.get(flow_id, (1.0, 1.0))

            for st in start_times:
                simulate_flow_instance(flow, st, state, minute, lat_mult)

    rows.sort(key=lambda r: r[0])

    df = pd.DataFrame(
        {
            "timestamp": [_dt_to_iso_z_ms(r[0]) for r in rows],
            "level": [r[1] for r in rows],
            "message": [r[2] for r in rows],
            "trace_id": [r[3] for r in rows],
            "service": [r[4] for r in rows],
            "host": [r[5] for r in rows],
        }
    )

    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
