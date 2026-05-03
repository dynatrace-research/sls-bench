import math
import hashlib
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "browser_testing_platform_2014"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "web_frontend": {
            "svc": "web-frontend",
            "hosts": ["web-1", "web-2", "web-3"],
            "logs": {
                "http_req": {
                    "lvl": "INFO",
                    "msg": "req {method} {route} rid={rid} ip={ip} ua={ua}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/login", "/dashboard", "/sessions"]},
                        "rid": {"k": "hex", "v": 16},
                        "ip": {"k": "ip", "v": None},
                        "ua": {"k": "ch", "v": ["Chrome", "Firefox", "Safari", "CI"]},
                    },
                },
                "auth_ok": {
                    "lvl": "DEBUG",
                    "msg": "auth ok uid={uid} rid={rid}",
                    "vars": {"uid": {"k": "i", "v": [1000, 50000]}, "rid": {"k": "hex", "v": 16}},
                },
                "http_resp_ok": {
                    "lvl": "INFO",
                    "msg": "resp {status} bytes={bytes} dur_ms={dur_ms} rid={rid}",
                    "vars": {
                        "status": {"k": "ch", "v": [200, 302, 304]},
                        "bytes": {"k": "i", "v": [500, 60000]},
                        "rid": {"k": "hex", "v": 16},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [30, 900]}},
                        "f": {"dur_ms": {"k": "i", "v": [30, 30000]}},
                    },
                },
                "http_resp_err": {
                    "lvl": "ERROR",
                    "msg": "resp 500 err={err} dur_ms={dur_ms} rid={rid}",
                    "vars": {
                        "err": {"k": "ch", "v": ["db_timeout", "internal_error"]},
                        "dur_ms": {"k": "i", "v": [2000, 45000]},
                        "rid": {"k": "hex", "v": 16},
                    },
                },
                "http_resp_maint": {
                    "lvl": "WARN",
                    "msg": "resp 503 maintenance dur_ms={dur_ms} rid={rid}",
                    "vars": {"dur_ms": {"k": "i", "v": [5, 120]}, "rid": {"k": "hex", "v": 16}},
                },
                "shellshock_block": {
                    "lvl": "WARN",
                    "msg": "blocked request pattern=shellshock ip={ip} rid={rid}",
                    "vars": {"ip": {"k": "ip", "v": None}, "rid": {"k": "hex", "v": 16}},
                },
                "maintenance_enabled": {
                    "lvl": "INFO",
                    "msg": "maintenance mode enabled reason={reason} actor={actor}",
                    "vars": {"reason": {"k": "ch", "v": ["security_investigation"]}, "actor": {"k": "ch", "v": ["oncall"]}},
                },
                "web_health_ok": {
                    "lvl": "INFO",
                    "msg": "health ok active_conns={conns} cpu_pct={cpu_pct}",
                    "vars": {"conns": {"k": "i", "v": [20, 600]}, "cpu_pct": {"k": "i", "v": [5, 85]}},
                },
                "web_health_degraded": {
                    "lvl": "WARN",
                    "msg": "health degraded active_conns={conns} cpu_pct={cpu_pct}",
                    "vars": {"conns": {"k": "i", "v": [200, 1200]}, "cpu_pct": {"k": "i", "v": [40, 98]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "web_health_ok", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "web_health_ok", "per_min": 1.0, "scope": "per_host"}, {"id": "web_health_degraded", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        "legacy_prototype_host": {
            "svc": "legacy-proto",
            "hosts": ["proto-1"],
            "logs": {
                "proto_http_cgi": {
                    "lvl": "INFO",
                    "msg": "cgi req {path} ip={ip} ua={ua} rid={rid}",
                    "vars": {
                        "path": {"k": "ch", "v": ["/cgi-bin/status", "/cgi-bin/diag"]},
                        "ip": {"k": "ip", "v": None},
                        "ua": {"k": "ch", "v": ["curl", "python-requests", "unknown"]},
                        "rid": {"k": "hex", "v": 16},
                    },
                },
                "shellshock_exec": {
                    "lvl": "ERROR",
                    "msg": "bash imported function executed via {path}; cmd={cmd} rid={rid}",
                    "vars": {
                        "path": {"k": "ch", "v": ["/cgi-bin/status", "/cgi-bin/diag"]},
                        "cmd": {"k": "str", "v": "short shell command fragment"},
                        "rid": {"k": "hex", "v": 16},
                    },
                },
                "aws_cli_invoke": {
                    "lvl": "INFO",
                    "msg": "awscli service={service} op={op} region={region} rid={rid}",
                    "vars": {
                        "service": {"k": "ch", "v": ["iam", "ec2", "ses", "rds"]},
                        "op": {"k": "ch", "v": ["create", "modify", "list", "send"]},
                        "region": {"k": "ch", "v": ["us-east-1"]},
                        "rid": {"k": "hex", "v": 16},
                    },
                },
                "ssh_login": {
                    "lvl": "INFO",
                    "msg": "ssh login user={user} ip={ip} auth={auth}",
                    "vars": {"user": {"k": "ch", "v": ["ec2-user", "ubuntu", "root"]}, "ip": {"k": "ip", "v": None}, "auth": {"k": "ch", "v": ["key", "password"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "rds_database": {
            "svc": "rds-primary",
            "hosts": ["rds-primary"],
            "logs": {
                "db_checkpoint": {"lvl": "INFO", "msg": "checkpoint complete flushed_mb={mb}", "vars": {"mb": {"k": "i", "v": [50, 800]}}},
                "db_conn": {
                    "lvl": "INFO",
                    "msg": "conn from {src_ip} user={db_user} app={app} conn_id={conn_id}",
                    "vars": {"src_ip": {"k": "ip", "v": None}, "db_user": {"k": "ch", "v": ["app_user", "readonly", "admin"]}, "app": {"k": "ch", "v": ["web", "unknown"]}, "conn_id": {"k": "hex", "v": 12}},
                },
                "db_long_query": {
                    "lvl": "WARN",
                    "msg": "slow query sql={sql_tag} table={table} dur_ms={dur_ms} rows={rows}",
                    "vars": {"sql_tag": {"k": "ch", "v": ["select_users_export", "select_sessions", "update_session"]}, "table": {"k": "ch", "v": ["users", "sessions"]}, "dur_ms": {"k": "i", "v": [500, 25000]}, "rows": {"k": "i", "v": [10, 200000]}},
                },
                "db_lock_wait": {
                    "lvl": "ERROR",
                    "msg": "lock wait timeout table={table} waited_ms={waited_ms} blocker_conn={blocker}",
                    "vars": {"table": {"k": "ch", "v": ["users"]}, "waited_ms": {"k": "i", "v": [5000, 30000]}, "blocker": {"k": "hex", "v": 12}},
                },
                "db_audit_copy": {
                    "lvl": "WARN",
                    "msg": "bulk read detected table={table} rows_est={rows_est} src_ip={src_ip}",
                    "vars": {"table": {"k": "ch", "v": ["users"]}, "rows_est": {"k": "i", "v": [1000, 200000]}, "src_ip": {"k": "ip", "v": None}},
                },
            },
            "beh": {"n": {"emit": [{"id": "db_checkpoint", "per_min": 0.5, "scope": "global"}]}, "f": {"emit": [{"id": "db_checkpoint", "per_min": 0.5, "scope": "global"}]}},
        },
        "aws_cloudtrail": {
            "svc": "cloudtrail",
            "hosts": ["cloudtrail"],
            "logs": {
                "ct_event": {
                    "lvl": "INFO",
                    "msg": "CloudTrail event={event} actor={actor} src_ip={src_ip} resource={resource}",
                    "vars": {
                        "event": {"k": "ch", "v": ["CreateUser", "CreateAccessKey", "RunInstances", "AttachVolume", "AuthorizeSecurityGroupIngress", "DescribeSnapshots"]},
                        "actor": {"k": "ch", "v": ["root", "attacker-iam"]},
                        "src_ip": {"k": "ip", "v": None},
                        "resource": {"k": "str", "v": "arn-or-resource-id"},
                    },
                },
                "ct_ops_revoke_sg": {
                    "lvl": "INFO",
                    "msg": "CloudTrail event=RevokeSecurityGroupIngress sg={sg} cidr={cidr} actor={actor}",
                    "vars": {"sg": {"k": "ch", "v": ["db-sg"]}, "cidr": {"k": "ch", "v": ["203.0.113.55/32", "198.51.100.77/32"]}, "actor": {"k": "ch", "v": ["oncall"]}},
                },
                "ct_ops_terminate_instance": {
                    "lvl": "INFO",
                    "msg": "CloudTrail event=TerminateInstances instance_id={instance_id} actor={actor}",
                    "vars": {"instance_id": {"k": "str", "v": "i-[0-9a-f]{8,17}"}, "actor": {"k": "ch", "v": ["oncall"]}},
                },
                "ct_ops_delete_keys": {
                    "lvl": "INFO",
                    "msg": "CloudTrail event=DeleteAccessKey principal={principal} actor={actor}",
                    "vars": {"principal": {"k": "ch", "v": ["root", "app-keys"]}, "actor": {"k": "ch", "v": ["security"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "monitoring": {
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "metric_db_lockwait": {"lvl": "INFO", "msg": "metric db.lock_wait_ms={lock_wait_ms} db={db}", "vars": {"lock_wait_ms": {"k": "i", "v": [0, 30000]}, "db": {"k": "ch", "v": ["rds-primary"]}}},
                "alert_db_lock": {"lvl": "CRITICAL", "msg": "ALERT db lock wait high table={table} lock_wait_ms={lock_wait_ms}", "vars": {"table": {"k": "ch", "v": ["users"]}, "lock_wait_ms": {"k": "i", "v": [5000, 30000]}}},
                "alert_unrecognized_db_ip": {"lvl": "WARN", "msg": "ALERT unrecognized db client ip={ip}", "vars": {"ip": {"k": "ip", "v": None}}},
                "pagerduty_ack": {"lvl": "INFO", "msg": "incident {incident} acknowledged by {actor}", "vars": {"incident": {"k": "ch", "v": ["DB_LOCK"]}, "actor": {"k": "ch", "v": ["oncall"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "metric_db_lockwait", "per_min": 1.0, "scope": "global"}, {"id": "alert_db_lock", "per_min": 0.0, "scope": "global"}, {"id": "alert_unrecognized_db_ip", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "metric_db_lockwait", "per_min": 1.0, "scope": "global"}, {"id": "alert_db_lock", "per_min": 0.2, "scope": "global"}, {"id": "alert_unrecognized_db_ip", "per_min": 0.05, "scope": "global"}]},
            },
        },
        "email_service": {
            "svc": "ses",
            "hosts": ["ses"],
            "logs": {
                "ses_send_batch": {"lvl": "INFO", "msg": "send batch campaign={campaign} recipients={rcpt} src={src} msg_id={msg_id}", "vars": {"campaign": {"k": "ch", "v": ["account_notice"]}, "rcpt": {"k": "i", "v": [500, 1500]}, "src": {"k": "ch", "v": ["no-reply@service.test"]}, "msg_id": {"k": "uuid", "v": None}}},
                "ses_send_reject": {"lvl": "WARN", "msg": "send rejected reason={reason} src_ip={src_ip}", "vars": {"reason": {"k": "ch", "v": ["throttled", "invalid_credentials"]}, "src_ip": {"k": "ip", "v": None}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "user_dashboard_ok",
                    "rpm": 450.0,
                    "emit": ["web_frontend.http_req", "web_frontend.auth_ok", "web_frontend.http_resp_ok"],
                    "latency_ms": [[1, 5], [5, 25], [40, 220]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "shellshock_probe_prod",
                    "rpm": 2.0,
                    "emit": ["web_frontend.shellshock_block"],
                    "latency_ms": [[2, 15]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "user_dashboard_ok_f",
                    "rpm": 420.0,
                    "emit": ["web_frontend.http_req", "web_frontend.auth_ok", "web_frontend.http_resp_ok"],
                    "latency_ms": [[1, 5], [5, 25], [50, 300]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "user_dashboard_slow_ok",
                    "rpm": 320.0,
                    "emit": ["web_frontend.http_req", "web_frontend.auth_ok", "rds_database.db_long_query", "web_frontend.http_resp_ok"],
                    "latency_ms": [[1, 5], [8, 40], [800, 8000], [30, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "user_dashboard_500",
                    "rpm": 80.0,
                    "emit": ["web_frontend.http_req", "web_frontend.auth_ok", "rds_database.db_lock_wait", "web_frontend.http_resp_err"],
                    "latency_ms": [[1, 5], [8, 40], [5000, 30000], [5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "user_request_maintenance",
                    "rpm": 250.0,
                    "emit": ["web_frontend.http_req", "web_frontend.http_resp_maint"],
                    "latency_ms": [[1, 4], [5, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "attacker_shellshock_proto",
                    "rpm": 4.0,
                    "emit": ["legacy_prototype_host.proto_http_cgi", "legacy_prototype_host.shellshock_exec"],
                    "latency_ms": [[2, 15], [1, 8]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "attacker_aws_api_abuse",
                    "rpm": 12.0,
                    "emit": ["legacy_prototype_host.aws_cli_invoke", "aws_cloudtrail.ct_event"],
                    "latency_ms": [[5, 30], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "attacker_db_export",
                    "rpm": 1.0,
                    "emit": ["legacy_prototype_host.aws_cli_invoke", "rds_database.db_conn", "rds_database.db_audit_copy", "rds_database.db_long_query"],
                    "latency_ms": [[5, 25], [5, 30], [10, 80], [2000, 25000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "attacker_ses_send",
                    "rpm": 1.0,
                    "emit": ["legacy_prototype_host.aws_cli_invoke", "email_service.ses_send_batch"],
                    "latency_ms": [[5, 25], [30, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "browserstack_shellshock_legacy_host_2014",
        "time": {"total_minutes": 34, "phases": {"n": {"start_min": 0, "end_min": 17}, "f": {"start_min": 17, "end_min": 34}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 17,
                        "rate_multipliers": {
                            "user_dashboard_slow_ok": 0.0,
                            "user_dashboard_500": 0.0,
                            "user_request_maintenance": 0.0,
                            "attacker_db_export": 0.0,
                            "attacker_ses_send": 0.0,
                            "monitoring.alert_db_lock": 0.0,
                            "monitoring.alert_unrecognized_db_ip": 0.0,
                            "web_frontend.web_health_degraded": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 23,
                        "rate_multipliers": {
                            "user_dashboard_ok_f": 0.2,
                            "user_dashboard_slow_ok": 1.0,
                            "user_dashboard_500": 1.0,
                            "attacker_db_export": 2.0,
                            "attacker_ses_send": 1.0,
                            "monitoring.alert_db_lock": 6.0,
                            "monitoring.alert_unrecognized_db_ip": 3.0,
                            "web_frontend.web_health_degraded": 1.0,
                        },
                        "latency_multipliers": {
                            "user_dashboard_ok_f": {"p50": 1.5, "p95": 3.0},
                            "user_dashboard_slow_ok": {"p50": 1.2, "p95": 1.5},
                            "user_dashboard_500": {"p50": 1.1, "p95": 1.3},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 28,
                        "rate_multipliers": {
                            "user_dashboard_ok_f": 0.0,
                            "user_dashboard_slow_ok": 0.0,
                            "user_dashboard_500": 0.0,
                            "user_request_maintenance": 1.0,
                            "attacker_shellshock_proto": 0.0,
                            "attacker_aws_api_abuse": 0.0,
                            "attacker_db_export": 0.0,
                            "attacker_ses_send": 0.0,
                            "monitoring.alert_db_lock": 0.5,
                            "monitoring.alert_unrecognized_db_ip": 0.0,
                            "web_frontend.web_health_degraded": 0.2,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "monitoring.pagerduty_ack", "count": 1, "hosts": ["mon-1"]},
                            {"ref": "aws_cloudtrail.ct_ops_revoke_sg", "count": 1, "hosts": ["cloudtrail"]},
                            {"ref": "aws_cloudtrail.ct_ops_terminate_instance", "count": 1, "hosts": ["cloudtrail"]},
                            {"ref": "aws_cloudtrail.ct_ops_delete_keys", "count": 1, "hosts": ["cloudtrail"]},
                            {"ref": "web_frontend.maintenance_enabled", "count": 1, "hosts": ["web-1"]},
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

SEED = "browserstack_shellshock_legacy_host_2014|v3|deterministic"
SEED_INT = int(hashlib.sha256(SEED.encode("utf-8")).hexdigest()[:16], 16)
random.seed(SEED_INT)
np.random.seed(SEED_INT % (2**32 - 1))

BASE_TIME = datetime(2014, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

ATTACKER_IP = "203.0.113.55"
ATTACKER_IP_ALT = "198.51.100.77"


def _hbytes(s: str) -> bytes:
    return hashlib.sha256((SEED + "|" + s).encode("utf-8")).digest()


def h01(s: str) -> float:
    b = _hbytes(s)
    x = int.from_bytes(b[:8], "big") >> 11  # 53-bit mantissa
    return x / float(1 << 53)


def det_round(x: float, key: str) -> int:
    if x <= 0:
        return 0
    f = math.floor(x)
    frac = x - f
    if frac <= 1e-12:
        return int(f)
    return int(f + (1 if h01(f"round|{key}") < frac else 0))


def det_hex(n: int, key: str) -> str:
    return hashlib.sha256((SEED + "|" + key).encode("utf-8")).hexdigest()[:n]


def det_uuid(key: str) -> str:
    hx = det_hex(32, f"uuid|{key}")
    return f"{hx[0:8]}-{hx[8:12]}-4{hx[13:16]}-a{hx[17:20]}-{hx[20:32]}"


def iso8601_ms(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


def clamp_int(x: int, lo: Optional[int], hi: Optional[int]) -> int:
    if lo is not None and x < lo:
        return lo
    if hi is not None and x > hi:
        return hi
    return x


def interp_lognormalish(p50: float, p95: float, u: float) -> float:
    u = min(0.95, max(0.5, u))
    if p50 <= 0:
        return max(1.0, p95)
    if p95 < p50:
        p95 = p50
    t = (u - 0.5) / 0.45
    return p50 * ((p95 / p50) ** t)


def sample_delay_ms(pair: List[float], key: str, cap_hi: Optional[int] = None) -> int:
    p50, p95 = float(pair[0]), float(pair[1])
    u = 0.5 + 0.45 * h01(f"delay_u|{key}")
    val = interp_lognormalish(p50, p95, u)
    jitter = (h01(f"delay_j|{key}") - 0.5) * 2.0
    jitter_ms = min(25.0, 0.03 * max(1.0, p50)) * jitter
    ms = int(round(max(1.0, val + jitter_ms)))
    if cap_hi is not None:
        ms = min(ms, cap_hi)
    return ms


def choose_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        arr = list(v)
        idx = int(h01(f"ch|{key}") * len(arr))
        if idx == len(arr):
            idx = len(arr) - 1
        return arr[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return lo + int(h01(f"i|{key}") * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + h01(f"f|{key}") * (hi - lo)
    if k == "hex":
        return det_hex(int(v), f"hex|{key}")
    if k == "uuid":
        return det_uuid(f"{key}")
    if k == "ip":
        octet = 1 + int(h01(f"ip|{key}") * 254)
        return f"192.0.2.{octet}"
    if k == "str":
        hx = det_hex(10, f"str|{key}")
        hint = v if isinstance(v, str) else "text"
        if "i-[0-9a-f]" in hint:
            return "i-" + det_hex(10, f"inst|{key}")
        if "arn-or-resource-id" in hint:
            return "arn:aws:iam::123456789012:user/" + "u-" + hx
        if "short shell command" in hint:
            return '() { :;}; /bin/bash -c "id; uname -a; echo ' + hx + '"'
        return f"{hint}:{hx}"
    return str(v)


def get_template(ref: str) -> Dict[str, Any]:
    comp_id, log_id = ref.split(".", 1)
    return SYSTEM["components"][comp_id]["logs"][log_id]


def get_component(comp_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]


def choose_host(comp_id: str, key: str) -> str:
    hosts = get_component(comp_id).get("hosts") or []
    if not hosts:
        return ""
    idx = int(h01(f"host|{comp_id}|{key}") * len(hosts))
    if idx == len(hosts):
        idx = len(hosts) - 1
    return hosts[idx]


def choose_user_ip(flow_id: str, inst_idx: int) -> str:
    octet = 1 + ((inst_idx + int(h01(f"userip|{flow_id}") * 10000)) % 254)
    return f"192.0.2.{octet}"


def choose_probe_ip(flow_id: str, inst_idx: int) -> str:
    octet = 1 + ((inst_idx + int(h01(f"probeip|{flow_id}") * 10000)) % 254)
    return f"198.51.100.{octet}"


def render_log(ref: str, state: str, key: str, bound: Dict[str, Any], overrides: Dict[str, Any]) -> Tuple[str, str]:
    tpl = get_template(ref)
    values: Dict[str, Any] = {}

    for vn, dom in (tpl.get("vars") or {}).items():
        if vn in overrides:
            values[vn] = overrides[vn]
        elif vn in bound:
            values[vn] = bound[vn]
        else:
            values[vn] = choose_from_domain(dom, f"{key}|{ref}|{vn}")

    sv = tpl.get("state_vars", {}).get(state, {})
    for vn, dom in sv.items():
        if vn in overrides:
            values[vn] = overrides[vn]
        elif vn in bound:
            values[vn] = bound[vn]
        else:
            values[vn] = choose_from_domain(dom, f"{key}|{ref}|{vn}|state={state}")

    msg = tpl["msg"].format(**values)
    return tpl["lvl"], msg


# -----------------------------
# Control state for failure phase
# -----------------------------

def build_failure_intervals() -> List[Dict[str, Any]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    f_start = int(f_phase["start_min"])
    f_end = int(f_phase["end_min"])
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    f_flows = {f["id"]: 1.0 for f in SYSTEM["flows"]["f"]["req"]}
    f_lat = {f["id"]: {"p50": 1.0, "p95": 1.0} for f in SYSTEM["flows"]["f"]["req"]}

    f_bg = {}
    for comp_id, comp in SYSTEM["components"].items():
        for src in comp.get("beh", {}).get("f", {}).get("emit", []):
            k = f"{comp_id}.{src['id']}"
            f_bg[k] = 1.0

    boundaries = [f_start] + [int(e["at_min"]) for e in events if int(e["at_min"]) != f_start] + [f_end]
    boundaries = sorted(set(boundaries))
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries = boundaries + [f_end]
    boundaries = sorted(boundaries)

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(int(e["at_min"]), []).append(e)

    intervals: List[Dict[str, Any]] = []
    active_flow_mult = dict(f_flows)
    active_bg_mult = dict(f_bg)
    active_lat_mult = dict(f_lat)

    for i in range(len(boundaries) - 1):
        start_m = boundaries[i]
        end_m = boundaries[i + 1]

        for e in events_by_min.get(start_m, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                if k in active_flow_mult:
                    active_flow_mult[k] = float(v)
                else:
                    active_bg_mult[k] = float(v)
            for fk, mv in (e.get("latency_multipliers") or {}).items():
                if fk in active_lat_mult:
                    active_lat_mult[fk] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}

        intervals.append(
            {
                "start_min": start_m,
                "end_min": end_m,
                "flow_mult": dict(active_flow_mult),
                "bg_mult": dict(active_bg_mult),
                "lat_mult": dict(active_lat_mult),
                "events_at_start": events_by_min.get(start_m, []),
            }
        )
    return intervals


FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Simulation core
# -----------------------------

def schedule_evenly_ms(start_ms: int, end_ms: int, count: int, key: str, jitter_ms: int = 200) -> List[int]:
    if count <= 0:
        return []
    dur = end_ms - start_ms
    out: List[int] = []
    for i in range(count):
        pos = (i + 0.5) / count
        t = start_ms + int(pos * dur)
        j = int(round((h01(f"jit|{key}|{i}") - 0.5) * 2.0 * jitter_ms))
        t2 = t + j
        if t2 < start_ms:
            t2 = start_ms
        if t2 >= end_ms:
            t2 = end_ms - 1
        out.append(t2)
    return out


def emit_row(rows: List[Dict[str, Any]], ts_ms: int, level: str, msg: str, service: str, host: str, trace_id: str = "") -> None:
    rows.append(
        {
            "timestamp_ms": int(ts_ms),
            "level": level,
            "message": msg,
            "trace_id": trace_id or "",
            "service": service or "",
            "host": host or "",
        }
    )


def simulate_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]] = None) -> None:
    start_ms = start_min * 60_000
    end_ms = end_min * 60_000
    duration_min = (end_ms - start_ms) / 60_000.0
    bg_mult = bg_mult or {}

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope") or "per_host"
            mult_key = f"{comp_id}.{log_id}"
            eff = per_min
            if state == "f":
                eff *= float(bg_mult.get(mult_key, 1.0))

            if eff <= 0:
                continue

            if scope == "per_host":
                for h in comp.get("hosts") or [""]:
                    expected = eff * duration_min
                    c = det_round(expected, f"bg|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|{h}")
                    times = schedule_evenly_ms(start_ms, end_ms, c, f"bgts|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|{h}", jitter_ms=350)
                    for i, tms in enumerate(times):
                        bound: Dict[str, Any] = {}
                        overrides: Dict[str, Any] = {}
                        if comp_id == "monitoring" and log_id == "alert_unrecognized_db_ip":
                            overrides["ip"] = ATTACKER_IP
                        if comp_id == "monitoring" and log_id == "metric_db_lockwait":
                            if state == "f" and start_min >= 23 and start_min < 28:
                                overrides["lock_wait_ms"] = clamp_int(7000 + int(h01(f"mlw|{tms}") * 16000), 0, 30000)
                            elif state == "f" and start_min >= 28:
                                overrides["lock_wait_ms"] = clamp_int(1000 + int(h01(f"mlw|{tms}") * 8000), 0, 30000)
                            else:
                                overrides["lock_wait_ms"] = clamp_int(int(h01(f"mlw|{tms}") * 800), 0, 30000)

                        ref = f"{comp_id}.{log_id}"
                        lvl, msg = render_log(ref, state, f"bg|{state}|{start_min}|{comp_id}.{log_id}|{i}", bound, overrides)
                        emit_row(rows, tms, lvl, msg, comp.get("svc", ""), h)
            elif scope == "global":
                expected = eff * duration_min
                c = det_round(expected, f"bg|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|global")
                times = schedule_evenly_ms(start_ms, end_ms, c, f"bgts|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|global", jitter_ms=350)
                hosts = comp.get("hosts") or [""]
                for i, tms in enumerate(times):
                    h = hosts[i % len(hosts)] if hosts else ""
                    bound = {}
                    overrides = {}
                    if comp_id == "monitoring" and log_id == "alert_unrecognized_db_ip":
                        overrides["ip"] = ATTACKER_IP
                    if comp_id == "monitoring" and log_id == "metric_db_lockwait":
                        if state == "f" and start_min >= 23 and start_min < 28:
                            overrides["lock_wait_ms"] = clamp_int(7000 + int(h01(f"mlw|{tms}") * 16000), 0, 30000)
                        elif state == "f" and start_min >= 28:
                            overrides["lock_wait_ms"] = clamp_int(1000 + int(h01(f"mlw|{tms}") * 8000), 0, 30000)
                        else:
                            overrides["lock_wait_ms"] = clamp_int(int(h01(f"mlw|{tms}") * 800), 0, 30000)

                    ref = f"{comp_id}.{log_id}"
                    lvl, msg = render_log(ref, state, f"bg|{state}|{start_min}|{comp_id}.{log_id}|{i}", bound, overrides)
                    emit_row(rows, tms, lvl, msg, comp.get("svc", ""), h)
            else:
                expected = eff * duration_min
                c = det_round(expected, f"bg|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|unknownscope")
                times = schedule_evenly_ms(start_ms, end_ms, c, f"bgts|{state}|{start_min}-{end_min}|{comp_id}.{log_id}|unknownscope", jitter_ms=350)
                h = (comp.get("hosts") or [""])[0]
                for i, tms in enumerate(times):
                    ref = f"{comp_id}.{log_id}"
                    lvl, msg = render_log(ref, state, f"bg|{state}|{start_min}|{comp_id}.{log_id}|{i}", {}, {})
                    emit_row(rows, tms, lvl, msg, comp.get("svc", ""), h)


def aws_event_from_service_op(service: str, op: str, key: str) -> str:
    candidates = SYSTEM["components"]["aws_cloudtrail"]["logs"]["ct_event"]["vars"]["event"]["v"]
    mapping = {
        ("iam", "create"): "CreateUser",
        ("iam", "modify"): "CreateAccessKey",
        ("ec2", "create"): "RunInstances",
        ("ec2", "modify"): "AttachVolume",
        ("rds", "list"): "DescribeSnapshots",
        ("rds", "modify"): "AuthorizeSecurityGroupIngress",
        ("ses", "send"): "CreateAccessKey",
    }
    ev = mapping.get((service, op))
    if ev in candidates:
        return ev
    return candidates[int(h01(f"ctev|{key}|{service}|{op}") * len(candidates)) % len(candidates)]


def simulate_flow_instances(
    rows: List[Dict[str, Any]],
    state: str,
    interval_start_min: int,
    interval_end_min: int,
    flow_mult: Optional[Dict[str, float]] = None,
    lat_mult: Optional[Dict[str, Dict[str, float]]] = None,
) -> None:
    start_ms = interval_start_min * 60_000
    end_ms = interval_end_min * 60_000
    duration_min = (end_ms - start_ms) / 60_000.0
    flow_mult = flow_mult or {}
    lat_mult = lat_mult or {}

    flows = SYSTEM["flows"][state]["req"]
    for flow in flows:
        fid = flow["id"]
        rpm = float(flow["rpm"])
        eff_rpm = rpm
        if state == "f":
            eff_rpm *= float(flow_mult.get(fid, 1.0))
        if eff_rpm <= 0:
            continue

        expected = eff_rpm * duration_min
        n_inst = det_round(expected, f"flow|{state}|{interval_start_min}-{interval_end_min}|{fid}")
        start_times = schedule_evenly_ms(start_ms, end_ms, n_inst, f"flowts|{state}|{interval_start_min}-{interval_end_min}|{fid}", jitter_ms=250)

        for inst_idx, inst_start_ms in enumerate(start_times):
            inst_key = f"inst|{state}|{interval_start_min}-{interval_end_min}|{fid}|{inst_idx}"
            trace_id = ""

            comp_hosts: Dict[str, str] = {}
            for ref in flow["emit"]:
                comp_id, _ = ref.split(".", 1)
                if comp_id not in comp_hosts:
                    comp_hosts[comp_id] = choose_host(comp_id, inst_key)

            bound: Dict[str, Any] = {}
            rid16 = det_hex(16, f"rid|{inst_key}")
            uid = 1000 + int(h01(f"uid|{inst_key}") * (50000 - 1000 + 1))

            if fid.startswith("user_"):
                client_ip = choose_user_ip(fid, inst_idx)
                ua_vals = SYSTEM["components"]["web_frontend"]["logs"]["http_req"]["vars"]["ua"]["v"]
                ua = ua_vals[int(h01(f"ua|{inst_key}") * len(ua_vals)) % len(ua_vals)]
                r = h01(f"route_bias|{inst_key}")
                if r < 0.08:
                    route = "/login"
                elif r < 0.64:
                    route = "/dashboard"
                else:
                    route = "/sessions"
                method = "POST" if route == "/login" else "GET"
                bound.update({"rid": rid16, "uid": uid, "ip": client_ip, "ua": ua, "route": route, "method": method})

            if fid == "shellshock_probe_prod":
                bound.update({"rid": rid16, "ip": choose_probe_ip(fid, inst_idx)})

            if fid.startswith("attacker_"):
                bound.update({"rid": rid16, "ip": ATTACKER_IP, "ua": "curl"})

            p50m = 1.0
            p95m = 1.0
            if state == "f":
                lm = lat_mult.get(fid, {"p50": 1.0, "p95": 1.0})
                p50m = float(lm.get("p50", 1.0))
                p95m = float(lm.get("p95", 1.0))

            t_ms = inst_start_ms
            emit_refs = flow["emit"]
            latency_pairs = flow["latency_ms"]

            web_req_ts_ms: Optional[int] = None

            for j, ref in enumerate(emit_refs):
                comp_id, _ = ref.split(".", 1)

                pair = latency_pairs[j] if j < len(latency_pairs) else [1, 5]
                sp50 = float(pair[0]) * p50m
                sp95 = float(pair[1]) * p95m
                if sp95 < sp50:
                    sp95 = sp50

                cap_hi: Optional[int] = None
                if ref == "rds_database.db_lock_wait":
                    cap_hi = int(SYSTEM["components"]["rds_database"]["logs"]["db_lock_wait"]["vars"]["waited_ms"]["v"][1])
                if ref == "rds_database.db_long_query":
                    cap_hi = int(SYSTEM["components"]["rds_database"]["logs"]["db_long_query"]["vars"]["dur_ms"]["v"][1])

                delay_ms = sample_delay_ms([sp50, sp95], f"{inst_key}|step{j}|{ref}", cap_hi=cap_hi)
                t_ms = t_ms + delay_ms

                overrides: Dict[str, Any] = {}

                if ref == "web_frontend.http_req":
                    overrides["rid"] = rid16
                    if "ip" in bound:
                        overrides["ip"] = bound["ip"]
                    if "ua" in bound:
                        overrides["ua"] = bound["ua"]
                    if "route" in bound:
                        overrides["route"] = bound["route"]
                    if "method" in bound:
                        overrides["method"] = bound["method"]
                    web_req_ts_ms = t_ms

                elif ref == "web_frontend.auth_ok":
                    overrides["rid"] = rid16
                    overrides["uid"] = uid

                elif ref == "web_frontend.http_resp_ok":
                    overrides["rid"] = rid16
                    route = bound.get("route", "/dashboard")
                    method = bound.get("method", "GET")
                    u = h01(f"status|{inst_key}")
                    if route == "/login" and method == "POST":
                        status = 302
                    else:
                        status = 304 if (method == "GET" and u < 0.03) else 200
                    overrides["status"] = status
                    overrides["bytes"] = clamp_int(800 + int(h01(f"bytes|{inst_key}") * 55000), 500, 60000)
                    if web_req_ts_ms is not None:
                        dur = int(max(1, t_ms - web_req_ts_ms))
                        if state == "n":
                            lo, hi = SYSTEM["components"]["web_frontend"]["logs"]["http_resp_ok"]["state_vars"]["n"]["dur_ms"]["v"]
                        else:
                            lo, hi = SYSTEM["components"]["web_frontend"]["logs"]["http_resp_ok"]["state_vars"]["f"]["dur_ms"]["v"]
                        overrides["dur_ms"] = clamp_int(dur, int(lo), int(hi))
                    else:
                        overrides["dur_ms"] = 50

                elif ref == "web_frontend.http_resp_err":
                    overrides["rid"] = rid16
                    overrides["err"] = "db_timeout" if h01(f"err|{inst_key}") < 0.8 else "internal_error"
                    if web_req_ts_ms is not None:
                        dur = int(max(1, t_ms - web_req_ts_ms))
                        lo, hi = SYSTEM["components"]["web_frontend"]["logs"]["http_resp_err"]["vars"]["dur_ms"]["v"]
                        overrides["dur_ms"] = clamp_int(dur, int(lo), int(hi))
                    else:
                        overrides["dur_ms"] = 5000

                elif ref == "web_frontend.http_resp_maint":
                    overrides["rid"] = rid16
                    if web_req_ts_ms is not None:
                        dur = int(max(1, t_ms - web_req_ts_ms))
                        lo, hi = SYSTEM["components"]["web_frontend"]["logs"]["http_resp_maint"]["vars"]["dur_ms"]["v"]
                        overrides["dur_ms"] = clamp_int(dur, int(lo), int(hi))
                    else:
                        overrides["dur_ms"] = 10

                elif ref == "web_frontend.shellshock_block":
                    overrides["rid"] = rid16
                    overrides["ip"] = bound.get("ip", choose_probe_ip(fid, inst_idx))

                elif ref == "legacy_prototype_host.proto_http_cgi":
                    overrides["rid"] = rid16
                    overrides["ip"] = ATTACKER_IP
                    overrides["ua"] = "curl"
                    path = "/cgi-bin/status" if h01(f"path|{inst_key}") < 0.6 else "/cgi-bin/diag"
                    overrides["path"] = path
                    bound["path"] = path

                elif ref == "legacy_prototype_host.shellshock_exec":
                    overrides["rid"] = rid16
                    overrides["path"] = bound.get("path", "/cgi-bin/status")
                    overrides["cmd"] = choose_from_domain({"k": "str", "v": "short shell command fragment"}, f"cmd|{inst_key}")

                elif ref == "legacy_prototype_host.aws_cli_invoke":
                    overrides["rid"] = rid16
                    overrides["region"] = "us-east-1"
                    if fid == "attacker_ses_send":
                        overrides["service"] = "ses"
                        overrides["op"] = "send"
                    elif fid == "attacker_db_export":
                        overrides["service"] = "rds"
                        overrides["op"] = "list"
                    else:
                        svc_choices = ["iam", "ec2", "ses", "rds"]
                        op_choices = ["create", "modify", "list", "send"]
                        overrides["service"] = svc_choices[int(h01(f"awssvc|{inst_key}") * len(svc_choices)) % len(svc_choices)]
                        overrides["op"] = op_choices[int(h01(f"awsop|{inst_key}") * len(op_choices)) % len(op_choices)]
                    bound["aws_service"] = overrides["service"]
                    bound["aws_op"] = overrides["op"]

                elif ref == "aws_cloudtrail.ct_event":
                    overrides["actor"] = "attacker-iam"
                    overrides["src_ip"] = ATTACKER_IP
                    service = bound.get("aws_service", "iam")
                    op = bound.get("aws_op", "list")
                    overrides["event"] = aws_event_from_service_op(service, op, inst_key)
                    overrides["resource"] = choose_from_domain({"k": "str", "v": "arn-or-resource-id"}, f"res|{inst_key}|{service}|{op}")

                elif ref == "rds_database.db_conn":
                    overrides["src_ip"] = ATTACKER_IP
                    overrides["app"] = "unknown"
                    overrides["db_user"] = "readonly"
                    overrides["conn_id"] = det_hex(12, f"conn|{inst_key}")

                elif ref == "rds_database.db_audit_copy":
                    overrides["src_ip"] = ATTACKER_IP
                    overrides["table"] = "users"
                    overrides["rows_est"] = clamp_int(5000 + int(h01(f"rows_est|{inst_key}") * 160000), 1000, 200000)

                elif ref == "rds_database.db_long_query":
                    overrides["dur_ms"] = clamp_int(delay_ms, 500, 25000)
                    if fid == "attacker_db_export":
                        overrides["sql_tag"] = "select_users_export"
                        overrides["table"] = "users"
                        overrides["rows"] = clamp_int(50000 + int(h01(f"rows|{inst_key}") * 150000), 10, 200000)
                    else:
                        overrides["sql_tag"] = "select_sessions" if h01(f"sql|{inst_key}") < 0.7 else "update_session"
                        overrides["table"] = "sessions"
                        overrides["rows"] = clamp_int(50 + int(h01(f"rows|{inst_key}") * 6000), 10, 200000)

                elif ref == "rds_database.db_lock_wait":
                    overrides["table"] = "users"
                    overrides["waited_ms"] = clamp_int(delay_ms, 5000, 30000)
                    overrides["blocker"] = det_hex(12, f"blocker|{inst_key}")

                lvl, msg = render_log(ref, state, f"flow|{inst_key}|emit{j}", bound, overrides)
                emit_row(rows, t_ms, lvl, msg, get_component(comp_id).get("svc", ""), comp_hosts.get(comp_id, ""), trace_id=trace_id)


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    for interval in FAILURE_INTERVALS:
        start_min = interval["start_min"]
        for e in interval.get("events_at_start", []):
            for os in e.get("one_shots", []) or []:
                ref = os["ref"]
                count = int(os["count"])
                hosts = os.get("hosts") or []
                comp_id, _ = ref.split(".", 1)
                svc = get_component(comp_id).get("svc", "")
                event_ms = int(start_min * 60_000)
                for i in range(count):
                    jitter = int(h01(f"oneshot_j|{ref}|{start_min}|{i}") * 1000)
                    ts_ms = event_ms + jitter
                    host = hosts[i % len(hosts)] if hosts else choose_host(comp_id, f"oneshot|{ref}|{i}")
                    lvl, msg = render_log(ref, "f", f"oneshot|{ref}|{start_min}|{i}", {}, {})
                    emit_row(rows, ts_ms, lvl, msg, svc, host, trace_id="")


def main() -> None:
    rows: List[Dict[str, Any]] = []

    n_phase = SCENARIO["scenario"]["time"]["phases"]["n"]
    n_start, n_end = int(n_phase["start_min"]), int(n_phase["end_min"])
    simulate_background(rows, "n", n_start, n_end, bg_mult=None)
    simulate_flow_instances(rows, "n", n_start, n_end, flow_mult=None, lat_mult=None)

    for interval in FAILURE_INTERVALS:
        smin = interval["start_min"]
        emin = interval["end_min"]
        simulate_background(rows, "f", smin, emin, bg_mult=interval["bg_mult"])
        simulate_flow_instances(rows, "f", smin, emin, flow_mult=interval["flow_mult"], lat_mult=interval["lat_mult"])

    emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df.sort_values(["timestamp_ms", "service", "host", "level", "message"], inplace=True, kind="mergesort")
    ts = [iso8601_ms(BASE_TIME + timedelta(milliseconds=int(ms))) for ms in df["timestamp_ms"].astype(int).tolist()]
    out = pd.DataFrame(
        {
            "timestamp": ts,
            "level": df["level"].astype(str).tolist(),
            "message": df["message"].astype(str).tolist(),
            "trace_id": df["trace_id"].astype(str).tolist(),
            "service": df["service"].astype(str).tolist(),
            "host": df["host"].astype(str).tolist(),
        }
    )
    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
