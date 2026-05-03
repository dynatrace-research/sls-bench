from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
import hashlib
import math
import random

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "shapeshift_exchange_hotwallet",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_gateway", "exchange_core"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "api_gateway": {
            "svc": "api-gateway",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "http_in": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} {route} from {client_ip} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/api/v1/swap"]},
                        "client_ip": {"k": "ip", "v": None},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_out_200": {
                    "lvl": "INFO",
                    "msg": "HTTP 200 {route} req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "route": {"k": "ch", "v": ["/api/v1/swap"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [80, 6000]},
                        "bytes": {"k": "i", "v": [300, 2200]},
                    },
                },
                "http_out_503_maint": {
                    "lvl": "WARN",
                    "msg": "HTTP 503 maintenance {route} req_id={req_id} dur_ms={dur_ms}",
                    "vars": {
                        "route": {"k": "ch", "v": ["/api/v1/swap"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                    },
                },
                "health_tick": {
                    "lvl": "INFO",
                    "msg": "health ok active_conns={active_conns}",
                    "vars": {"active_conns": {"k": "i", "v": [30, 900]}},
                },
            },
            "beh": {
                "n": [{"id": "health_tick", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "health_tick", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "exchange_core": {
            "svc": "exchange-core",
            "hosts": ["core-1", "core-2"],
            "logs": {
                "swap_accepted": {
                    "lvl": "INFO",
                    "msg": "swap accepted pair={pair} req_id={req_id} quote_id={quote_id} trace={trace_id}",
                    "vars": {
                        "pair": {"k": "ch", "v": ["BTC_ETH", "ETH_BTC", "BTC_LTC", "LTC_BTC"]},
                        "req_id": {"k": "uuid", "v": None},
                        "quote_id": {"k": "hex", "v": 16},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "wallet_send_req": {
                    "lvl": "INFO",
                    "msg": "wallet send requested asset={asset} amount={amount} quote_id={quote_id}",
                    "vars": {
                        "asset": {"k": "ch", "v": ["BTC", "ETH", "LTC"]},
                        "amount": {"k": "f", "v": [0.001, 25.0]},
                        "quote_id": {"k": "hex", "v": 16},
                    },
                },
                "maintenance_enabled": {
                    "lvl": "WARN",
                    "msg": "maintenance mode enabled by {actor} reason={reason}",
                    "vars": {
                        "actor": {"k": "ch", "v": ["greg.ops", "oncall.eng"]},
                        "reason": {"k": "ch", "v": ["unexpected_hot_wallet_debits", "incident_investigation"]},
                    },
                },
                "maintenance_reject": {
                    "lvl": "WARN",
                    "msg": "swap rejected: maintenance mode req_id={req_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}},
                },
                "worker_loop": {
                    "lvl": "INFO",
                    "msg": "worker ok queue_depth={queue_depth}",
                    "vars": {"queue_depth": {"k": "i", "v": [0, 300]}},
                },
            },
            "beh": {
                "n": [{"id": "worker_loop", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "worker_loop", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "hot_wallet_server": {
            "svc": "hot-wallet",
            "hosts": ["wallet-1", "wallet-2"],
            "logs": {
                "ssh_login_success": {
                    "lvl": "INFO",
                    "msg": "sshd: Accepted publickey for {user} from {src_ip} key_fp={key_fp}",
                    "vars": {"user": {"k": "ch", "v": ["core", "admin"]}, "src_ip": {"k": "ip", "v": None}},
                    "state_vars": {
                        "n": {"key_fp": {"k": "ch", "v": ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:03"]}},
                        "f": {"key_fp": {"k": "ch", "v": ["9c:3f:4b:ad:d6:43:ec:9a:55:de:b9:0b:d8:f5:0a:cb"]}},
                    },
                },
                "tx_signed_malware": {
                    "lvl": "ERROR",
                    "msg": "wallet: signed tx asset=BTC txid={txid} amount={amount} to={dst_addr}",
                    "vars": {
                        "txid": {"k": "hex", "v": 64},
                        "amount": {"k": "f", "v": [0.1, 75.0]},
                        "dst_addr": {"k": "ch", "v": ["14Kt9i5MdQCKvjX6HS2hEevVgbPhK13SKD"]},
                    },
                },
                "tx_signed_malware_eth": {
                    "lvl": "ERROR",
                    "msg": "wallet: signed tx asset=ETH txid={txid} amount={amount} to={dst_addr}",
                    "vars": {
                        "txid": {"k": "hex", "v": 64},
                        "amount": {"k": "f", "v": [5.0, 3000.0]},
                        "dst_addr": {"k": "ch", "v": ["0xC26B321d50910f2f990EF92A8Effd8EC38aDE8f5"]},
                    },
                },
                "tx_broadcast_malware": {
                    "lvl": "CRITICAL",
                    "msg": "wallet: broadcast tx asset=BTC txid={txid} to={dst_addr} balance_after={balance_after}",
                    "vars": {
                        "txid": {"k": "hex", "v": 64},
                        "dst_addr": {"k": "ch", "v": ["14Kt9i5MdQCKvjX6HS2hEevVgbPhK13SKD"]},
                        "balance_after": {"k": "f", "v": [0.0, 2000.0]},
                    },
                },
                "tx_broadcast_malware_eth": {
                    "lvl": "CRITICAL",
                    "msg": "wallet: broadcast tx asset=ETH txid={txid} to={dst_addr} balance_after={balance_after}",
                    "vars": {
                        "txid": {"k": "hex", "v": 64},
                        "dst_addr": {"k": "ch", "v": ["0xC26B321d50910f2f990EF92A8Effd8EC38aDE8f5"]},
                        "balance_after": {"k": "f", "v": [0.0, 2000.0]},
                    },
                },
                "tx_broadcast_swap": {
                    "lvl": "INFO",
                    "msg": "wallet: broadcast tx asset={asset} txid={txid} balance_after={balance_after}",
                    "vars": {"asset": {"k": "ch", "v": ["BTC", "ETH", "LTC"]}, "txid": {"k": "hex", "v": 64}},
                    "state_vars": {
                        "n": {"balance_after": {"k": "f", "v": [200.0, 20000.0]}},
                        "f": {"balance_after": {"k": "f", "v": [0.0, 2000.0]}},
                    },
                },
                "sudo_exec": {
                    "lvl": "WARN",
                    "msg": "sudo: {user} ran '{cmd}'",
                    "vars": {
                        "user": {"k": "ch", "v": ["root", "admin"]},
                        "cmd": {"k": "ch", "v": ["systemctl restart walletd", "systemctl status walletd"]},
                    },
                },
                "sudo_exec_tamper": {
                    "lvl": "WARN",
                    "msg": "sudo: {user} ran '{cmd}'",
                    "vars": {
                        "user": {"k": "ch", "v": ["root", "admin"]},
                        "cmd": {
                            "k": "ch",
                            "v": ["systemctl stop fluent-bit", "truncate -s 0 /var/log/auth.log", "truncate -s 0 /var/log/syslog"],
                        },
                    },
                },
                "audit_log_truncate": {
                    "lvl": "ERROR",
                    "msg": "audit: file truncated path={file_path} by {user}",
                    "vars": {
                        "file_path": {"k": "ch", "v": ["/var/log/auth.log", "/var/log/syslog", "/var/log/walletd.log"]},
                        "user": {"k": "ch", "v": ["root", "admin"]},
                    },
                },
                "log_forwarder_stopped": {
                    "lvl": "ERROR",
                    "msg": "log forwarder stopped service={service} exit_code={exit_code}",
                    "vars": {"service": {"k": "ch", "v": ["fluent-bit"]}, "exit_code": {"k": "i", "v": [0, 143]}},
                },
                "hotwallet_balance": {
                    "lvl": "INFO",
                    "msg": "hotwallet balance asset={asset} balance={balance}",
                    "vars": {"asset": {"k": "ch", "v": ["BTC", "ETH", "LTC"]}},
                    "state_vars": {
                        "n": {"balance": {"k": "f", "v": [200.0, 20000.0]}},
                        "f": {"balance": {"k": "f", "v": [0.0, 2000.0]}},
                    },
                },
            },
            "beh": {
                "n": [{"id": "hotwallet_balance", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "hotwallet_balance", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "log_aggregator": {
            "svc": "log-agg",
            "hosts": ["log-1"],
            "logs": {
                "ingest_summary": {
                    "lvl": "INFO",
                    "msg": "ingest summary sources={sources} eps={eps} backlog_s={backlog_s}",
                    "vars": {"sources": {"k": "ch", "v": ["api_gateway,exchange_core,hot_wallet_server"]}, "backlog_s": {"k": "i", "v": [0, 30]}},
                    "state_vars": {"n": {"eps": {"k": "f", "v": [25.0, 70.0]}}, "f": {"eps": {"k": "f", "v": [10.0, 60.0]}}},
                },
                "source_gap_warn": {
                    "lvl": "WARN",
                    "msg": "missing logs from {missing_sources} for {gap_s}s",
                    "vars": {
                        "missing_sources": {"k": "ch", "v": ["hot_wallet_server", "exchange_core", "hot_wallet_server,exchange_core"]},
                        "gap_s": {"k": "i", "v": [10, 900]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "ingest_summary", "per_min": 1.0, "scope": "global"}, {"id": "source_gap_warn", "per_min": 0.02, "scope": "global"}],
                "f": [{"id": "ingest_summary", "per_min": 1.0, "scope": "global"}, {"id": "source_gap_warn", "per_min": 0.02, "scope": "global"}],
            },
        },
        "sec_ops": {
            "svc": "sec-ops",
            "hosts": ["secops-1"],
            "logs": {
                "incident_page": {
                    "lvl": "WARN",
                    "msg": "incident declared id={incident_id} by {actor}",
                    "vars": {"incident_id": {"k": "hex", "v": 8}, "actor": {"k": "ch", "v": ["greg.ops", "oncall.eng"]}},
                },
                "wallet_anomaly_alert": {
                    "lvl": "ERROR",
                    "msg": "alert: unexpected hotwallet transfer asset=BTC amount={amount} to={dst_addr} txid={txid}",
                    "vars": {"txid": {"k": "hex", "v": 64}},
                    "state_vars": {
                        "n": {"amount": {"k": "f", "v": [0.01, 10.0]}, "dst_addr": {"k": "ch", "v": ["1BoatSLRHtKNngkdXEeobR76b53LETtpyT"]}},
                        "f": {"amount": {"k": "f", "v": [0.1, 75.0]}, "dst_addr": {"k": "ch", "v": ["14Kt9i5MdQCKvjX6HS2hEevVgbPhK13SKD"]}},
                    },
                },
                "wallet_anomaly_alert_eth": {
                    "lvl": "ERROR",
                    "msg": "alert: unexpected hotwallet transfer asset=ETH amount={amount} to={dst_addr} txid={txid}",
                    "vars": {"txid": {"k": "hex", "v": 64}},
                    "state_vars": {
                        "n": {"amount": {"k": "f", "v": [0.1, 50.0]}, "dst_addr": {"k": "ch", "v": ["0x1111111111111111111111111111111111111111"]}},
                        "f": {"amount": {"k": "f", "v": [5.0, 3000.0]}, "dst_addr": {"k": "ch", "v": ["0xC26B321d50910f2f990EF92A8Effd8EC38aDE8f5"]}},
                    },
                },
                "keys_rotated": {
                    "lvl": "INFO",
                    "msg": "rotated credentials scope={scope} by {actor}",
                    "vars": {"scope": {"k": "ch", "v": ["ssh_keys", "wallet_keys", "all"]}, "actor": {"k": "ch", "v": ["greg.ops", "oncall.eng"]}},
                },
                "forensic_scan_found": {
                    "lvl": "ERROR",
                    "msg": "forensic scan found suspicious binary {file} sha256={sha256} host={host}",
                    "vars": {
                        "file": {"k": "ch", "v": ["/usr/bin/udevd-bridge", "/usr/local/bin/udevd-bridge"]},
                        "sha256": {"k": "hex", "v": 64},
                        "host": {"k": "ch", "v": ["wallet-1", "wallet-2"]},
                    },
                },
                "cron_tick": {"lvl": "INFO", "msg": "secops automation tick ok", "vars": {}},
            },
            "beh": {
                "n": [
                    {"id": "cron_tick", "per_min": 0.2, "scope": "global"},
                    {"id": "wallet_anomaly_alert", "per_min": 0.005, "scope": "global"},
                    {"id": "wallet_anomaly_alert_eth", "per_min": 0.005, "scope": "global"},
                ],
                "f": [
                    {"id": "cron_tick", "per_min": 0.2, "scope": "global"},
                    {"id": "wallet_anomaly_alert", "per_min": 0.35, "scope": "global"},
                    {"id": "wallet_anomaly_alert_eth", "per_min": 0.25, "scope": "global"},
                ],
            },
        },
    },
    "flows": {
        "n": {
            "swap_request_success": {
                "rpm": 500.0,
                "emit": [
                    "api_gateway.http_in",
                    "exchange_core.swap_accepted",
                    "exchange_core.wallet_send_req",
                    "hot_wallet_server.tx_broadcast_swap",
                    "api_gateway.http_out_200",
                ],
                "latency_ms": [[2, 8], [4, 20], [40, 220], [120, 900], [20, 180]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            }
        },
        "f": {
            "swap_request_maintenance": {
                "rpm": 500.0,
                "emit": ["api_gateway.http_in", "exchange_core.maintenance_reject", "api_gateway.http_out_503_maint"],
                "latency_ms": [[2, 8], [2, 15], [2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "attacker_ssh_session": {
                "rpm": 0.5,
                "emit": ["hot_wallet_server.ssh_login_success", "hot_wallet_server.sudo_exec"],
                "latency_ms": [[3, 40], [2, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "unauthorized_withdrawal": {
                "rpm": 1.0,
                "emit": ["hot_wallet_server.tx_signed_malware", "hot_wallet_server.tx_broadcast_malware"],
                "latency_ms": [[10, 120], [40, 500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "unauthorized_withdrawal_eth": {
                "rpm": 1.0,
                "emit": ["hot_wallet_server.tx_signed_malware_eth", "hot_wallet_server.tx_broadcast_malware_eth"],
                "latency_ms": [[10, 140], [40, 600]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "unauthorized_withdrawal_unobserved": {
                "rpm": 1.0,
                "emit": [],
                "latency_ms": [],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "unauthorized_withdrawal_eth_unobserved": {
                "rpm": 1.0,
                "emit": [],
                "latency_ms": [],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "attacker_log_wipe": {
                "rpm": 0.1,
                "emit": ["hot_wallet_server.sudo_exec_tamper", "hot_wallet_server.audit_log_truncate"],
                "latency_ms": [[2, 40], [5, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "hostco_hotwallet_backdoor_apr9",
    "time": {"total_minutes": 32, "phases": {"n": {"start_min": 0, "end_min": 16}, "f": {"start_min": 16, "end_min": 32}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 16,
                    "rate_multipliers": {
                        "unauthorized_withdrawal": 6.0,
                        "unauthorized_withdrawal_eth": 6.0,
                        "unauthorized_withdrawal_unobserved": 0.0,
                        "unauthorized_withdrawal_eth_unobserved": 0.0,
                        "attacker_ssh_session": 2.0,
                        "attacker_log_wipe": 0.0,
                        "sec_ops.wallet_anomaly_alert": 2.0,
                        "sec_ops.wallet_anomaly_alert_eth": 2.0,
                    },
                    "latency_multipliers": {"swap_request_maintenance": {"p50": 0.6, "p95": 0.6}},
                    "one_shots": [
                        {"ref": "sec_ops.incident_page", "count": 1, "hosts": ["secops-1"]},
                        {"ref": "exchange_core.maintenance_enabled", "count": 1, "hosts": ["core-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 18,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "sec_ops.keys_rotated", "count": 1, "hosts": ["secops-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 22,
                    "rate_multipliers": {"attacker_log_wipe": 20.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "hot_wallet_server.log_forwarder_stopped", "count": 2, "hosts": ["wallet-1", "wallet-2"]}],
                },
                {
                    "order": 4,
                    "at_min": 23,
                    "rate_multipliers": {
                        "unauthorized_withdrawal": 0.0,
                        "unauthorized_withdrawal_eth": 0.0,
                        "attacker_ssh_session": 0.0,
                        "attacker_log_wipe": 0.0,
                        "hot_wallet_server.hotwallet_balance": 0.0,
                        "unauthorized_withdrawal_unobserved": 6.0,
                        "unauthorized_withdrawal_eth_unobserved": 6.0,
                        "log_aggregator.source_gap_warn": 150.0,
                        "sec_ops.wallet_anomaly_alert": 3.0,
                        "sec_ops.wallet_anomaly_alert_eth": 3.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
                {
                    "order": 5,
                    "at_min": 26,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "sec_ops.forensic_scan_found", "count": 1, "hosts": ["secops-1"]}],
                },
            ]
        }
    },
}

SEED = 1337
BASE_TIME = datetime(2026, 4, 9, 0, 0, 0, tzinfo=timezone.utc)


def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _h_int(s: str) -> int:
    return int(_md5_hex(s), 16)


def _u01(s: str) -> float:
    return (_h_int(s) % 1_000_000) / 1_000_000.0


def _choice(options: List[Any], key: str) -> Any:
    if not options:
        return ""
    idx = _h_int(key) % len(options)
    return options[idx]


def _det_int(lo: int, hi: int, key: str) -> int:
    if hi <= lo:
        return int(lo)
    u = _u01(key)
    return int(lo + math.floor(u * (hi - lo + 1)))


def _det_float(lo: float, hi: float, key: str) -> float:
    if hi <= lo:
        return float(lo)
    u = _u01(key)
    return lo + (hi - lo) * u


def _det_hex(length: int, key: str) -> str:
    if length <= 0:
        return ""
    out = ""
    i = 0
    while len(out) < length:
        out += _md5_hex(f"{key}|{i}")
        i += 1
    return out[:length].lower()


def _det_uuid(key: str) -> str:
    h = _det_hex(32, f"uuid|{key}")
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _det_ip(key: str, kind: str = "public") -> str:
    if kind == "private":
        a = 10
        b = _h_int(f"ipb|{key}") % 256
        c = _h_int(f"ipc|{key}") % 256
        d = 1 + (_h_int(f"ipd|{key}") % 254)
        return f"{a}.{b}.{c}.{d}"
    a = 198
    b = 51
    c = 100
    d = 1 + (_h_int(f"ipd|{key}") % 254)
    return f"{a}.{b}.{c}.{d}"


def _fmt_float(x: float) -> str:
    s = f"{x:.8f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


def _iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sample_latency_ms(p50: float, p95: float, key: str, mult_p50: float = 1.0, mult_p95: float = 1.0) -> int:
    p50s = max(0.1, p50 * mult_p50)
    p95s = max(p50s, p95 * mult_p95)
    u = _u01(key)
    x = p50s + (p95s - p50s) * (u**2)
    return max(1, int(round(x)))


def _schedule_even(count: int, start: datetime, end: datetime, key_prefix: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = max(0.001, (end - start).total_seconds())
    out = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start + timedelta(seconds=dur_s * frac)
        jitter_ms = int(round((_u01(f"{key_prefix}|jitter|{i}") - 0.5) * 500.0))
        t = t + timedelta(milliseconds=jitter_ms)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Tuple[float, float]]


def _build_failure_intervals() -> List[Interval]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start <= e["at_min"] <= f_end}) + [f_end]
    boundaries = sorted(dict.fromkeys(boundaries))

    flow_rate_mult: Dict[str, float] = {}
    bg_rate_mult: Dict[str, float] = {}
    flow_latency_mult: Dict[str, Tuple[float, float]] = {}

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        m = boundaries[i]
        nxt = boundaries[i + 1]
        for e in events_by_min.get(m, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                if "." in k:
                    bg_rate_mult[k] = float(v)
                else:
                    flow_rate_mult[k] = float(v)
            for fid, mults in (e.get("latency_multipliers") or {}).items():
                flow_latency_mult[fid] = (float(mults.get("p50", 1.0)), float(mults.get("p95", 1.0)))
        intervals.append(
            Interval(
                state="f",
                start_min=m,
                end_min=nxt,
                flow_rate_mult=dict(flow_rate_mult),
                bg_rate_mult=dict(bg_rate_mult),
                flow_latency_mult=dict(flow_latency_mult),
            )
        )
    return intervals


def _alloc_count(expected: float, key: str, carry: Dict[str, float]) -> int:
    c = carry.get(key, 0.0)
    v = expected + c
    n = int(math.floor(v + 1e-12))
    carry[key] = v - n
    return max(0, n)


def _get_log_tpl(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def _domains_for_log(tpl: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    domains = dict(tpl.get("vars") or {})
    sv = tpl.get("state_vars") or {}
    if state in sv:
        domains.update(sv[state])
    return domains


def _gen_var_from_domain(domain: Dict[str, Any], key: str, ip_kind: str = "public") -> Any:
    k = domain["k"]
    v = domain.get("v")
    if k == "ch":
        return _choice(list(v or []), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return _det_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return _det_float(lo, hi, key)
    if k == "uuid":
        return _det_uuid(key)
    if k == "hex":
        return _det_hex(int(v), key)
    if k == "ip":
        return _det_ip(key, kind=ip_kind)
    if k == "str":
        return f"{key}"
    return ""


def _render_log_message(tpl: Dict[str, Any], state: str, bound: Dict[str, Any], key_prefix: str, ip_kind: str = "public") -> str:
    domains = _domains_for_log(tpl, state)
    msg_vars: Dict[str, Any] = {}
    for var_name, dom in domains.items():
        if var_name in bound:
            val = bound[var_name]
        else:
            val = _gen_var_from_domain(dom, f"{key_prefix}|{var_name}", ip_kind=ip_kind)
        if isinstance(val, float):
            msg_vars[var_name] = _fmt_float(val)
        else:
            msg_vars[var_name] = str(val)
    return tpl["msg"].format(**msg_vars)


PAIR_TO_ASSET = {
    "BTC_ETH": "ETH",
    "ETH_BTC": "BTC",
    "BTC_LTC": "LTC",
    "LTC_BTC": "BTC",
}


def _choose_component_host(comp_id: str, key: str) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts") or []
    if not hosts:
        return ""
    return str(_choice(hosts, key))


def _emit_row(rows: List[Dict[str, Any]], seq: int, dt: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"_dt": dt, "_seq": seq, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def _simulate_flow_instance(
    rows: List[Dict[str, Any]],
    seq_start: int,
    state: str,
    flow_id: str,
    flow_def: Dict[str, Any],
    start_dt: datetime,
    interval_lat_mult: Tuple[float, float],
    instance_idx: int,
) -> int:
    emit_refs: List[str] = list(flow_def.get("emit") or [])
    lat_pairs: List[List[float]] = list(flow_def.get("latency_ms") or [])
    if not emit_refs:
        return seq_start

    trace_on = bool(flow_def.get("trace", False) and SYSTEM.get("tracing", {}).get("on", False))
    trace_id = _det_hex(32, f"trace|{flow_id}|{state}|{instance_idx}") if trace_on else ""

    per_comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        comp_id, _ = ref.split(".", 1)
        if comp_id not in per_comp_host:
            per_comp_host[comp_id] = _choose_component_host(comp_id, f"host|{flow_id}|{instance_idx}|{comp_id}")

    bound: Dict[str, Any] = {}
    if trace_on:
        bound["trace_id"] = trace_id

    if flow_id in ("swap_request_success", "swap_request_maintenance"):
        bound["req_id"] = _det_uuid(f"req|{flow_id}|{instance_idx}")
        bound["client_ip"] = _det_ip(f"client|{flow_id}|{instance_idx}", kind="public")
    if flow_id == "swap_request_success":
        pair = _choice(SYSTEM["components"]["exchange_core"]["logs"]["swap_accepted"]["vars"]["pair"]["v"], f"pair|{instance_idx}")
        asset = PAIR_TO_ASSET.get(pair, _choice(["BTC", "ETH", "LTC"], f"assetfallback|{instance_idx}"))
        bound["pair"] = pair
        bound["quote_id"] = _det_hex(16, f"quote|{instance_idx}")
        bound["asset"] = asset
        bound["amount"] = _det_float(0.001, 25.0, f"amt|{asset}|{instance_idx}")
        bound["txid"] = _det_hex(64, f"txid|swap|{instance_idx}")
        bound["bytes"] = _det_int(300, 2200, f"bytes|{instance_idx}")
    elif flow_id == "unauthorized_withdrawal":
        bound["txid"] = _det_hex(64, f"txid|malbtc|{instance_idx}")
        bound["amount"] = _det_float(0.1, 75.0, f"amt|malbtc|{instance_idx}")
        bound["dst_addr"] = "14Kt9i5MdQCKvjX6HS2hEevVgbPhK13SKD"
        bound["balance_after"] = _det_float(0.0, 2000.0, f"bal|malbtc|{instance_idx}")
    elif flow_id == "unauthorized_withdrawal_eth":
        bound["txid"] = _det_hex(64, f"txid|maleth|{instance_idx}")
        bound["amount"] = _det_float(5.0, 3000.0, f"amt|maleth|{instance_idx}")
        bound["dst_addr"] = "0xC26B321d50910f2f990EF92A8Effd8EC38aDE8f5"
        bound["balance_after"] = _det_float(0.0, 2000.0, f"bal|maleth|{instance_idx}")
    elif flow_id == "attacker_ssh_session":
        bound["src_ip"] = _det_ip(f"sshsrc|{instance_idx}", kind="private")
    elif flow_id == "attacker_log_wipe":
        file_path = _choice(["/var/log/auth.log", "/var/log/syslog"], f"wipefile|{instance_idx}")
        bound["file_path"] = file_path
        bound["cmd"] = f"truncate -s 0 {file_path}"
        bound["user"] = _choice(["root", "admin"], f"wipeuser|{instance_idx}")

    p50_mult, p95_mult = interval_lat_mult
    times: List[datetime] = []
    cur = start_dt
    for j, pair in enumerate(lat_pairs[: len(emit_refs)]):
        p50, p95 = float(pair[0]), float(pair[1])
        d_ms = _sample_latency_ms(p50, p95, f"lat|{flow_id}|{instance_idx}|{j}", mult_p50=p50_mult, mult_p95=p95_mult)
        cur = cur + timedelta(milliseconds=d_ms)
        times.append(cur)

    if flow_id in ("swap_request_success", "swap_request_maintenance"):
        in_ts = times[0]
        out_ts = times[-1]
        dur_ms = int(round((out_ts - in_ts).total_seconds() * 1000.0))
        resp_ref = emit_refs[-1]
        resp_comp, resp_log = resp_ref.split(".", 1)
        resp_tpl = _get_log_tpl(resp_comp, resp_log)
        dur_dom = (resp_tpl.get("vars") or {}).get("dur_ms")
        if dur_dom and dur_dom.get("k") == "i":
            min_dur = int(dur_dom["v"][0])
            if dur_ms < min_dur:
                extra = min_dur - dur_ms
                times[-1] = times[-1] + timedelta(milliseconds=extra)
                dur_ms = min_dur
        bound["dur_ms"] = dur_ms

    seq = seq_start
    for j, ref in enumerate(emit_refs):
        comp_id, log_id = ref.split(".", 1)
        tpl = _get_log_tpl(comp_id, log_id)
        service = SYSTEM["components"][comp_id].get("svc") or ""
        host = per_comp_host.get(comp_id, "")
        ip_kind = "public"
        if comp_id == "hot_wallet_server" and log_id == "ssh_login_success":
            ip_kind = "private"

        if flow_id == "swap_request_success" and ref == "hot_wallet_server.tx_broadcast_swap":
            if "balance_after" not in bound:
                bound["balance_after"] = _det_float(200.0, 20000.0, f"bal|swap|{instance_idx}")

        msg = _render_log_message(tpl, state, bound, f"flow|{flow_id}|{instance_idx}|{j}", ip_kind=ip_kind)
        _emit_row(rows, seq, times[j], tpl["lvl"], msg, trace_id, service, host)
        seq += 1
    return seq


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    f_intervals = _build_failure_intervals()
    intervals: List[Interval] = [Interval(state="n", start_min=n_start, end_min=n_end, flow_rate_mult={}, bg_rate_mult={}, flow_latency_mult={})] + f_intervals

    rows: List[Dict[str, Any]] = []
    carry: Dict[str, float] = {}
    flow_instance_counters: Dict[Tuple[str, str], int] = {}

    seq = 0

    for itv in intervals:
        start_dt = BASE_TIME + timedelta(minutes=itv.start_min)
        end_dt = BASE_TIME + timedelta(minutes=itv.end_min)
        dur_min = max(0.0, (itv.end_min - itv.start_min))

        for comp_id, comp in SYSTEM["components"].items():
            beh = (comp.get("beh") or {}).get(itv.state, [])
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope") or "per_host"

                mult = 1.0
                if itv.state == "f":
                    mult = float(itv.bg_rate_mult.get(f"{comp_id}.{log_id}", 1.0))
                rate = per_min * mult
                if rate <= 0.0:
                    continue

                tpl = _get_log_tpl(comp_id, log_id)
                service = comp.get("svc") or ""

                if scope == "global":
                    key = f"bg|{itv.state}|{comp_id}.{log_id}|global"
                    expected = rate * dur_min
                    count = _alloc_count(expected, key, carry)
                    times = _schedule_even(count, start_dt, end_dt, f"{key}|{itv.start_min}")
                    for i, t in enumerate(times):
                        host = _choose_component_host(comp_id, f"{key}|host|{itv.start_min}|{i}") if (comp.get("hosts") or []) else ""
                        bound: Dict[str, Any] = {}
                        if comp_id == "log_aggregator" and log_id == "source_gap_warn":
                            bound["missing_sources"] = "hot_wallet_server"
                            bound["gap_s"] = _det_int(10, 900, f"{key}|gap|{itv.start_min}|{i}")
                        msg = _render_log_message(tpl, itv.state, bound, f"{key}|{itv.start_min}|{i}")
                        _emit_row(rows, seq, t, tpl["lvl"], msg, "", service, host)
                        seq += 1
                else:
                    for host in comp.get("hosts") or [""]:
                        key = f"bg|{itv.state}|{comp_id}.{log_id}|{host}"
                        expected = rate * dur_min
                        count = _alloc_count(expected, key, carry)
                        times = _schedule_even(count, start_dt, end_dt, f"{key}|{itv.start_min}")
                        for i, t in enumerate(times):
                            msg = _render_log_message(tpl, itv.state, {}, f"{key}|{itv.start_min}|{i}")
                            _emit_row(rows, seq, t, tpl["lvl"], msg, "", service, host)
                            seq += 1

    for itv in intervals:
        start_dt = BASE_TIME + timedelta(minutes=itv.start_min)
        end_dt = BASE_TIME + timedelta(minutes=itv.end_min)
        dur_min = max(0.0, (itv.end_min - itv.start_min))

        flows_in_state: Dict[str, Any] = SYSTEM["flows"].get(itv.state, {})
        for flow_id, flow_def in flows_in_state.items():
            base_rpm = float(flow_def["rpm"])
            mult = 1.0
            lat_mult = (1.0, 1.0)
            if itv.state == "f":
                mult = float(itv.flow_rate_mult.get(flow_id, 1.0))
                lat_mult = tuple(itv.flow_latency_mult.get(flow_id, (1.0, 1.0)))  # type: ignore
            rpm = base_rpm * mult
            if rpm <= 0.0:
                continue

            expected_instances = rpm * dur_min
            count = _alloc_count(expected_instances, f"flow|{itv.state}|{flow_id}", carry)
            if count <= 0:
                continue

            start_times = _schedule_even(count, start_dt, end_dt, f"flow|{itv.state}|{flow_id}|{itv.start_min}")
            counter_key = (itv.state, flow_id)
            idx0 = flow_instance_counters.get(counter_key, 0)

            for i, t0 in enumerate(start_times):
                inst_idx = idx0 + i
                seq = _simulate_flow_instance(rows, seq, itv.state, flow_id, flow_def, t0, lat_mult, inst_idx)

            flow_instance_counters[counter_key] = idx0 + count

    f_events = list(SCENARIO["phases"]["f"]["events"])
    f_events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    for ev in f_events:
        at_min = int(ev["at_min"])
        ev_base = BASE_TIME + timedelta(minutes=at_min)
        for os in (ev.get("one_shots") or []):
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            tpl = _get_log_tpl(comp_id, log_id)
            service = SYSTEM["components"][comp_id].get("svc") or ""
            hosts = list(os.get("hosts") or [])
            count = int(os["count"])
            for k in range(count):
                # Ensure one-shots are never timestamped before the event time.
                jitter_ms = int(round(_u01(f"oneshot|{ref}|{at_min}|{k}|j") * 800.0))  # [0, 800] ms
                dt = ev_base + timedelta(milliseconds=(50 * k + jitter_ms))
                if hosts:
                    host = hosts[k % len(hosts)]
                else:
                    host = _choose_component_host(comp_id, f"oneshot|{ref}|{at_min}|{k}|host")
                msg = _render_log_message(tpl, "f", {}, f"oneshot|{ref}|{at_min}|{k}")
                _emit_row(rows, seq, dt, tpl["lvl"], msg, "", service, host)
                seq += 1

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No logs generated (unexpected for this scenario).")

    df.sort_values(by=["_dt", "_seq"], ascending=True, inplace=True, kind="mergesort")
    df["timestamp"] = df["_dt"].apply(_iso_z)

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    n_rows = len(out)
    if not (20_000 <= n_rows <= 100_000):
        raise RuntimeError(f"Row count {n_rows} outside target range [20000, 100000].")

    def _ok_trace(s: str) -> bool:
        if s == "":
            return True
        if len(s) != 32:
            return False
        return all(c in "0123456789abcdef" for c in s)

    bad = out.loc[~out["trace_id"].map(_ok_trace)]
    if not bad.empty:
        raise RuntimeError("Invalid trace_id values detected.")

    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
