import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


# Verifier-required explicit seeding (even though this simulator is hash-deterministic)
random.seed(0)


SYSTEM: Dict[str, Any] = {
    "id": "m365_wan_connectivity",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id_len": 32},
    "components": {
        "wan_router_fleet": {
            "svc": "wan-core",
            "hosts": ["wan-rtr-01", "wan-rtr-02", "wan-rtr-03"],
            "logs": {
                "router_heartbeat": {
                    "lvl": "INFO",
                    "msg": "router alive router_id={router_id} uptime_s={uptime_s}",
                    "vars": {
                        "router_id": {"k": "ch", "v": ["wan-rtr-01", "wan-rtr-02", "wan-rtr-03"]},
                        "uptime_s": {"k": "i", "v": [1000, 900000]},
                    },
                },
                "config_cmd_ip_change": {
                    "lvl": "WARN",
                    "msg": "config change: set interface ip {old_ip}->{new_ip} cmd={cmd} change_id={change_id}",
                    "vars": {
                        "old_ip": {"k": "ip", "v": None},
                        "new_ip": {"k": "ip", "v": None},
                        "cmd": {"k": "ch", "v": ["set-ip-fast", "set-ip"]},
                        "change_id": {"k": "str", "v": "CHG[0-9]{6}"},
                    },
                },
                "adjacency_recompute_start": {
                    "lvl": "WARN",
                    "msg": "routing recompute started reason={reason} scope={scope}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["ip_change", "protocol_update"]},
                        "scope": {"k": "ch", "v": ["wan-wide", "local"]},
                    },
                },
                "routing_recompute_metric": {
                    "lvl": "WARN",
                    "msg": "routing convergence in_progress={in_progress} adjacencies_changed={adj_changed}",
                    "vars": {
                        "in_progress": {"k": "ch", "v": ["true", "false"]},
                        "adj_changed": {"k": "i", "v": [0, 500]},
                    },
                },
                "fwd_drop_metric": {
                    "lvl": "WARN",
                    "msg": "fwd impairment loss_pct={loss_pct} queue_depth={qdepth} vrf=core",
                    "vars": {
                        "loss_pct": {"k": "f", "v": [0.0, 25.0]},
                        "qdepth": {"k": "i", "v": [0, 5000]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "router_heartbeat", "per_min": 0.5, "scope": "per_host"}],
                "f": [
                    {"id": "routing_recompute_metric", "per_min": 0.6, "scope": "per_host"},
                    {"id": "fwd_drop_metric", "per_min": 1.5, "scope": "per_host"},
                ],
            },
        },
        "wan_automation_controller": {
            "svc": "wan-automation",
            "hosts": ["wan-auto-01", "wan-auto-02"],
            "logs": {
                "te_optimizer_tick": {
                    "lvl": "INFO",
                    "msg": "TE optimizer tick iter={iter} active_paths={paths} cost_delta={delta}",
                    "vars": {
                        "iter": {"k": "i", "v": [1, 200000]},
                        "paths": {"k": "i", "v": [50, 500]},
                        "delta": {"k": "i", "v": [-50, 50]},
                    },
                },
                "device_health_scan": {
                    "lvl": "INFO",
                    "msg": "device health scan unhealthy={unhealthy} remediated={remediated}",
                    "vars": {
                        "unhealthy": {"k": "i", "v": [0, 30]},
                        "remediated": {"k": "i", "v": [0, 30]},
                    },
                },
                "automation_paused": {
                    "lvl": "ERROR",
                    "msg": "automation paused subsystem={subsystem} reason={reason}",
                    "vars": {
                        "subsystem": {"k": "ch", "v": ["traffic_engineering", "device_health"]},
                        "reason": {"k": "ch", "v": ["control_plane_unreachable", "excessive_timeouts"]},
                    },
                },
                "automation_manual_restart": {
                    "lvl": "WARN",
                    "msg": "operator restarted automation subsystem={subsystem} run_id={run_id}",
                    "vars": {
                        "subsystem": {"k": "ch", "v": ["traffic_engineering", "device_health"]},
                        "run_id": {"k": "hex", "v": 8},
                    },
                },
                "probe_send": {
                    "lvl": "INFO",
                    "msg": "wan probe send probe_id={probe_id} src_region={src} dst_region={dst} timeout_ms={timeout_ms}",
                    "vars": {
                        "probe_id": {"k": "hex", "v": 16},
                        "src": {"k": "ch", "v": ["eu", "us", "apac"]},
                        "dst": {"k": "ch", "v": ["eu", "us", "apac"]},
                        "timeout_ms": {"k": "i", "v": [500, 5000]},
                    },
                },
                "probe_result_ok": {
                    "lvl": "INFO",
                    "msg": "wan probe ok probe_id={probe_id} rtt_ms={rtt_ms}",
                    "vars": {"probe_id": {"k": "hex", "v": 16}, "rtt_ms": {"k": "i", "v": [10, 300]}},
                },
                "probe_result_fail": {
                    "lvl": "WARN",
                    "msg": "wan probe failed probe_id={probe_id} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "probe_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["timeout", "packet_loss"]},
                        "waited_ms": {"k": "i", "v": [300, 8000]},
                    },
                },
            },
            "beh": {
                "n": [
                    {"id": "te_optimizer_tick", "per_min": 2.0, "scope": "global"},
                    {"id": "device_health_scan", "per_min": 0.6, "scope": "global"},
                ],
                "f": [
                    {"id": "te_optimizer_tick", "per_min": 2.0, "scope": "global"},
                    {"id": "device_health_scan", "per_min": 0.6, "scope": "global"},
                ],
            },
        },
        "dns_resolver": {
            "svc": "dns-internal",
            "hosts": ["dns-01", "dns-02"],
            "logs": {
                "dns_query": {
                    "lvl": "INFO",
                    "msg": "dns query name={qname} type=A client=edge-gw",
                    "vars": {"qname": {"k": "ch", "v": ["m365-backend.internal", "status-backend.internal"]}},
                },
                "dns_answer_ok": {
                    "lvl": "INFO",
                    "msg": "dns answer name={qname} rcode=NOERROR dur_ms={dur_ms} ttl_s={ttl_s}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["m365-backend.internal", "status-backend.internal"]},
                        "dur_ms": {"k": "i", "v": [1, 3000]},
                        "ttl_s": {"k": "i", "v": [10, 300]},
                    },
                },
                "dns_timeout": {
                    "lvl": "WARN",
                    "msg": "dns timeout name={qname} waited_ms={waited_ms}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["m365-backend.internal", "status-backend.internal"]},
                        "waited_ms": {"k": "i", "v": [100, 6000]},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
        "m365_edge_gateway": {
            "svc": "m365-edge",
            "hosts": ["edge-01", "edge-02", "edge-03", "edge-04"],
            "logs": {
                "req_in_m365": {
                    "lvl": "INFO",
                    "msg": "incoming req req_id={req_id} method={method} route={route} client_region={region}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/teams", "/exchange", "/sharepoint", "/onedrive", "/graph", "/admin"]},
                        "region": {"k": "ch", "v": ["eu", "us", "apac"]},
                    },
                },
                "req_in_status": {
                    "lvl": "INFO",
                    "msg": "incoming req req_id={req_id} method=GET route=/status client_region={region}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "region": {"k": "ch", "v": ["eu", "us", "apac"]}},
                },
                "upstream_retry": {
                    "lvl": "WARN",
                    "msg": "retrying upstream svc={svc} attempt={attempt} reason={reason}",
                    "vars": {
                        "svc": {"k": "ch", "v": ["m365", "status_portal"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["connect_timeout", "packet_loss", "no_route"]},
                    },
                },
                "upstream_timeout": {
                    "lvl": "WARN",
                    "msg": "upstream timeout svc={svc} attempt={attempt} waited_ms={waited_ms}",
                    "vars": {
                        "svc": {"k": "ch", "v": ["m365", "status_portal"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "waited_ms": {"k": "i", "v": [300, 25000]},
                    },
                },
                "resp_200_m365": {
                    "lvl": "INFO",
                    "msg": "response sent req_id={req_id} status=200 dur_ms={dur_ms} bytes={bytes}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [10, 20000]}, "bytes": {"k": "i", "v": [500, 5000000]}},
                },
                "resp_503_m365": {
                    "lvl": "WARN",
                    "msg": "response sent req_id={req_id} status=503 dur_ms={dur_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 20000]},
                        "err": {"k": "ch", "v": ["dns_timeout", "upstream_unreachable"]},
                    },
                },
                "resp_504_m365": {
                    "lvl": "WARN",
                    "msg": "response sent req_id={req_id} status=504 dur_ms={dur_ms} err=upstream_timeout",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [500, 80000]}},
                },
                "resp_200_status": {
                    "lvl": "INFO",
                    "msg": "response sent req_id={req_id} status=200 dur_ms={dur_ms} bytes={bytes}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [10, 20000]}, "bytes": {"k": "i", "v": [500, 300000]}},
                },
                "resp_504_status": {
                    "lvl": "WARN",
                    "msg": "response sent req_id={req_id} status=504 dur_ms={dur_ms} err=upstream_timeout",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [500, 80000]}},
                },
                "healthz_ok": {
                    "lvl": "INFO",
                    "msg": "healthz ok probe_id={probe_id} rtt_ms={rtt_ms}",
                    "vars": {"probe_id": {"k": "hex", "v": 16}, "rtt_ms": {"k": "i", "v": [5, 800]}},
                },
                "healthz_timeout": {
                    "lvl": "WARN",
                    "msg": "healthz timeout probe_id={probe_id} waited_ms={waited_ms}",
                    "vars": {"probe_id": {"k": "hex", "v": 16}, "waited_ms": {"k": "i", "v": [200, 8000]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
        "m365_app_cluster": {
            "svc": "m365-core",
            "hosts": ["m365-app-01", "m365-app-02", "m365-app-03"],
            "logs": {
                "req_received": {
                    "lvl": "INFO",
                    "msg": "backend received req_id={req_id} op={op}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "op": {"k": "ch", "v": ["auth", "mailbox", "chat", "files", "graph"]}},
                }
            },
            "beh": {"n": [], "f": []},
        },
        "status_portal_app": {
            "svc": "status-portal",
            "hosts": ["status-01", "status-02"],
            "logs": {
                "req_received": {
                    "lvl": "INFO",
                    "msg": "status backend received req_id={req_id} page={page}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "page": {"k": "ch", "v": ["/status", "/history"]}},
                }
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": {
            "m365_http_ok": {
                "rpm": 220.0,
                "emit": [
                    "m365_edge_gateway.req_in_m365",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "m365_app_cluster.req_received",
                    "m365_edge_gateway.resp_200_m365",
                ],
                "latency_ms": [[2, 6], [3, 10], [4, 14], [15, 50], [18, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "status_portal_ok": {
                "rpm": 8.0,
                "emit": [
                    "m365_edge_gateway.req_in_status",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "status_portal_app.req_received",
                    "m365_edge_gateway.resp_200_status",
                ],
                "latency_ms": [[2, 6], [3, 10], [4, 14], [10, 40], [15, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "wan_probe_ok": {
                "rpm": 4.0,
                "emit": ["wan_automation_controller.probe_send", "m365_edge_gateway.healthz_ok", "wan_automation_controller.probe_result_ok"],
                "latency_ms": [[1, 3], [15, 80], [1, 3]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
        },
        "f": {
            "m365_http_slow_success": {
                "rpm": 130.0,
                "emit": [
                    "m365_edge_gateway.req_in_m365",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "m365_app_cluster.req_received",
                    "m365_edge_gateway.resp_200_m365",
                ],
                "latency_ms": [[5, 20], [20, 120], [40, 220], [200, 1400], [250, 1600]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "m365_http_504": {
                "rpm": 70.0,
                "emit": [
                    "m365_edge_gateway.req_in_m365",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "m365_edge_gateway.upstream_timeout",
                    "m365_edge_gateway.upstream_retry",
                    "m365_edge_gateway.upstream_timeout",
                    "m365_edge_gateway.upstream_retry",
                    "m365_edge_gateway.upstream_timeout",
                    "m365_edge_gateway.resp_504_m365",
                ],
                "latency_ms": [[5, 20], [20, 120], [40, 220], [800, 12000], [10, 60], [800, 12000], [10, 60], [800, 12000], [20, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "m365_dns_timeout_503": {
                "rpm": 25.0,
                "emit": ["m365_edge_gateway.req_in_m365", "dns_resolver.dns_query", "dns_resolver.dns_timeout", "m365_edge_gateway.resp_503_m365"],
                "latency_ms": [[3, 12], [20, 120], [200, 6000], [5, 30]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "status_portal_ok_degraded": {
                "rpm": 4.0,
                "emit": [
                    "m365_edge_gateway.req_in_status",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "status_portal_app.req_received",
                    "m365_edge_gateway.resp_200_status",
                ],
                "latency_ms": [[5, 20], [20, 120], [40, 220], [80, 800], [120, 1000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "status_portal_504": {
                "rpm": 1.0,
                "emit": [
                    "m365_edge_gateway.req_in_status",
                    "dns_resolver.dns_query",
                    "dns_resolver.dns_answer_ok",
                    "m365_edge_gateway.upstream_timeout",
                    "m365_edge_gateway.upstream_retry",
                    "m365_edge_gateway.upstream_timeout",
                    "m365_edge_gateway.resp_504_status",
                ],
                "latency_ms": [[5, 20], [20, 120], [40, 220], [800, 12000], [10, 60], [800, 12000], [20, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "backoff_ms": []},
                "trace": True,
            },
            "wan_probe_lossy": {
                "rpm": 4.0,
                "emit": ["wan_automation_controller.probe_send", "m365_edge_gateway.healthz_timeout", "wan_automation_controller.probe_result_fail"],
                "latency_ms": [[1, 3], [500, 6000], [1, 3]],
                "retry": {"max_attempts": 2, "expected_attempts": 1.5, "backoff_ms": [[200, 1200]]},
                "trace": True,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "m365_wan_router_ip_change_outage",
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "events": [
        {
            "order": 1,
            "at_min": 20,
            "rate_multipliers": {
                "m365_http_504": 1.2,
                "m365_dns_timeout_503": 1.3,
                "status_portal_504": 1.0,
                "wan_probe_lossy": 1.0,
                "wan_router_fleet.fwd_drop_metric": 2.5,
                "wan_router_fleet.routing_recompute_metric": 2.0,
            },
            "latency_multipliers": {
                "m365_http_slow_success": {"p50": 1.8, "p95": 2.2},
                "m365_http_504": {"p50": 2.0, "p95": 2.5},
                "m365_dns_timeout_503": {"p50": 1.6, "p95": 2.0},
                "status_portal_ok_degraded": {"p50": 1.6, "p95": 2.0},
                "status_portal_504": {"p50": 2.0, "p95": 2.5},
                "wan_probe_lossy": {"p50": 2.0, "p95": 2.5},
            },
            "one_shots": [
                {"ref": "wan_router_fleet.config_cmd_ip_change", "count": 1, "hosts": ["wan-rtr-01"]},
                {"ref": "wan_router_fleet.adjacency_recompute_start", "count": 1, "hosts": ["wan-rtr-01"]},
            ],
        },
        {
            "order": 2,
            "at_min": 23,
            "rate_multipliers": {
                "m365_http_504": 1.7,
                "m365_dns_timeout_503": 1.6,
                "status_portal_504": 3.0,
                "wan_probe_lossy": 1.4,
                "wan_router_fleet.fwd_drop_metric": 3.2,
            },
            "latency_multipliers": {
                "m365_http_slow_success": {"p50": 2.4, "p95": 3.0},
                "m365_http_504": {"p50": 3.0, "p95": 3.5},
                "m365_dns_timeout_503": {"p50": 2.0, "p95": 2.8},
                "status_portal_ok_degraded": {"p50": 2.0, "p95": 2.8},
                "status_portal_504": {"p50": 3.0, "p95": 3.5},
                "wan_probe_lossy": {"p50": 2.8, "p95": 3.2},
            },
            "one_shots": [],
        },
        {
            "order": 3,
            "at_min": 26,
            "rate_multipliers": {
                "m365_http_slow_success": 1.2,
                "m365_http_504": 0.8,
                "m365_dns_timeout_503": 0.7,
                "status_portal_ok_degraded": 1.1,
                "status_portal_504": 1.2,
                "wan_probe_lossy": 1.0,
                "wan_router_fleet.fwd_drop_metric": 2.0,
                "wan_router_fleet.routing_recompute_metric": 1.4,
            },
            "latency_multipliers": {
                "m365_http_slow_success": {"p50": 1.5, "p95": 1.8},
                "m365_http_504": {"p50": 1.8, "p95": 2.1},
                "m365_dns_timeout_503": {"p50": 1.3, "p95": 1.6},
                "status_portal_ok_degraded": {"p50": 1.4, "p95": 1.7},
                "status_portal_504": {"p50": 1.8, "p95": 2.1},
                "wan_probe_lossy": {"p50": 1.6, "p95": 1.9},
            },
            "one_shots": [],
        },
        {
            "order": 4,
            "at_min": 34,
            "rate_multipliers": {
                "wan_automation_controller.te_optimizer_tick": 0.0,
                "wan_automation_controller.device_health_scan": 0.0,
                "m365_http_504": 0.9,
                "m365_dns_timeout_503": 0.8,
                "wan_probe_lossy": 1.1,
                "wan_router_fleet.fwd_drop_metric": 2.3,
            },
            "latency_multipliers": {
                "m365_http_slow_success": {"p50": 1.6, "p95": 1.9},
                "m365_http_504": {"p50": 1.9, "p95": 2.2},
                "m365_dns_timeout_503": {"p50": 1.4, "p95": 1.8},
                "wan_probe_lossy": {"p50": 1.8, "p95": 2.3},
            },
            "one_shots": [{"ref": "wan_automation_controller.automation_paused", "count": 1, "hosts": ["wan-auto-01"]}],
        },
        {
            "order": 5,
            "at_min": 38,
            "rate_multipliers": {
                "wan_automation_controller.te_optimizer_tick": 1.0,
                "wan_automation_controller.device_health_scan": 1.0,
                "m365_http_slow_success": 1.1,
                "m365_http_504": 0.6,
                "m365_dns_timeout_503": 0.5,
                "status_portal_504": 0.8,
                "wan_probe_lossy": 0.9,
                "wan_router_fleet.fwd_drop_metric": 1.6,
                "wan_router_fleet.routing_recompute_metric": 1.1,
            },
            "latency_multipliers": {
                "m365_http_slow_success": {"p50": 1.2, "p95": 1.4},
                "m365_http_504": {"p50": 1.4, "p95": 1.6},
                "m365_dns_timeout_503": {"p50": 1.2, "p95": 1.4},
                "status_portal_ok_degraded": {"p50": 1.2, "p95": 1.4},
                "status_portal_504": {"p50": 1.4, "p95": 1.6},
                "wan_probe_lossy": {"p50": 1.3, "p95": 1.6},
            },
            "one_shots": [{"ref": "wan_automation_controller.automation_manual_restart", "count": 2, "hosts": ["wan-auto-01"]}],
        },
    ],
}


# ------------------------ deterministic helpers ------------------------

def _sha256_bytes(s: str) -> bytes:
    return hashlib.sha256(s.encode("utf-8")).digest()


def h_int(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts)
    b = _sha256_bytes(s)
    return int.from_bytes(b[:8], "big", signed=False)


def u01(*parts: Any) -> float:
    return (h_int(*parts) % 10_000_000_000) / 10_000_000_000.0


def hex_str(*parts: Any, length: int) -> str:
    s = "|".join(str(p) for p in parts)
    hx = hashlib.sha256(s.encode("utf-8")).hexdigest()
    if length <= len(hx):
        return hx[:length]
    return (hx * ((length // len(hx)) + 1))[:length]


def choose(seq: List[Any], *parts: Any) -> Any:
    if not seq:
        return None
    idx = int(u01(*parts) * len(seq)) % len(seq)
    return seq[idx]


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


# Acklam inverse normal CDF approximation (sufficient for deterministic shaping)
def inv_norm_cdf(p: float) -> float:
    p = clamp(p, 1e-12, 1.0 - 1e-12)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5])
        den = ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)
        return num / den
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        num = -(((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5])
        den = ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)
        return num / den
    q = p - 0.5
    r = q * q
    num = (((((a[0]*r + a[1])*r + a[2])*r + a[3])*r + a[4])*r + a[5]) * q
    den = (((((b[0]*r + b[1])*r + b[2])*r + b[3])*r + b[4])*r + 1)
    return num / den


def sample_lognormal_ms(p50: float, p95: float, key: Tuple[Any, ...], soft_cap_mult: float = 3.0) -> int:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    u_main = 0.65 + 0.30 * u01(*key, "u_main")  # [0.65, 0.95)
    z = mu + sigma * inv_norm_cdf(u_main)
    x = math.exp(z)
    cap = soft_cap_mult * p95
    u_cap = 0.9 + 0.2 * u01(*key, "u_cap")  # [0.9, 1.1)
    x = min(x, cap * u_cap)
    return int(max(1.0, round(x)))


def stable_int_count(expected: float, key: Tuple[Any, ...]) -> int:
    expected = max(0.0, float(expected))
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 1e-12:
        return n
    return n + (1 if u01(*key, "frac") < frac else 0)


def schedule_times(start: datetime, end: datetime, count: int, key: Tuple[Any, ...]) -> List[datetime]:
    if count <= 0:
        return []
    dur_ms = max(1, int((end - start).total_seconds() * 1000))
    spacing = dur_ms / float(count)
    times: List[datetime] = []
    for j in range(count):
        base_ms = (j + 0.5) * spacing
        jitter_max = min(500.0, 0.30 * spacing)
        jitter = (u01(*key, j, "jit") - 0.5) * 2.0 * jitter_max
        t_ms = int(clamp(base_ms + jitter, 0.0, dur_ms - 1.0))
        times.append(start + timedelta(milliseconds=t_ms))
    return times


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def gen_uuid(*parts: Any) -> str:
    hx = hex_str(*parts, length=32)
    return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def gen_ip(*parts: Any) -> str:
    a = 10
    b = (h_int(*parts, "ip_b") % 256)
    c = (h_int(*parts, "ip_c") % 256)
    d = (h_int(*parts, "ip_d") % 254) + 1
    return f"{a}.{b}.{c}.{d}"


def gen_str_hint(hint: str, *parts: Any) -> str:
    if hint.startswith("CHG") and "{6}" in hint:
        n = h_int(*parts, "chg") % 1_000_000
        return f"CHG{n:06d}"
    return hint


def gen_from_domain(domain: Dict[str, Any], key: Tuple[Any, ...]) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return choose(list(v), *key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return lo + (h_int(*key) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return round(lo + (hi - lo) * u01(*key, "f"), 2)
    if k == "hex":
        ln = int(v)
        return hex_str(*key, length=ln)
    if k == "uuid":
        return gen_uuid(*key)
    if k == "ip":
        return gen_ip(*key)
    if k == "str":
        return gen_str_hint(str(v), *key)
    return ""


def render_log(comp_id: str, log_id: str, values: Dict[str, Any], host: str) -> Tuple[str, str, str, str]:
    comp = SYSTEM["components"][comp_id]
    tmpl = comp["logs"][log_id]
    msg = tmpl["msg"].format(**values)
    return tmpl["lvl"], msg, comp.get("svc", "") or "", host or ""


# ------------------------ control intervals ------------------------

@dataclass(frozen=True)
class IntervalCtl:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Tuple[float, float]]  # flow_id -> (p50_mult, p95_mult)


def build_failure_intervals() -> List[IntervalCtl]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [f_start] + [e["at_min"] for e in events if f_start <= e["at_min"] < f_end] + [f_end]
    uniq: List[int] = []
    for b in boundaries:
        if not uniq or uniq[-1] != b:
            uniq.append(b)
    boundaries = uniq

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Tuple[float, float]] = {}

    by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[IntervalCtl] = []
    for i in range(len(boundaries) - 1):
        start_m = boundaries[i]
        for e in by_min.get(start_m, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                active_rate[k] = float(v)
            for fk, mv in (e.get("latency_multipliers") or {}).items():
                active_lat[fk] = (float(mv["p50"]), float(mv["p95"]))
        end_m = boundaries[i + 1]
        intervals.append(IntervalCtl(start_m, end_m, dict(active_rate), dict(active_lat)))
    return intervals


# ------------------------ simulation logic ------------------------

def get_flow_lat_mult(flow_id: str, ctl: Optional[IntervalCtl], state: str) -> Tuple[float, float]:
    if state != "f" or ctl is None:
        return (1.0, 1.0)
    return ctl.lat_mult.get(flow_id, (1.0, 1.0))


def get_rate_mult(key: str, ctl: Optional[IntervalCtl], state: str) -> float:
    if state != "f" or ctl is None:
        return 1.0
    return float(ctl.rate_mult.get(key, 1.0))


def pick_component_host(comp_id: str, instance_key: Tuple[Any, ...]) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts") or []
    if not hosts:
        return ""
    return hosts[h_int(*instance_key, comp_id, "host") % len(hosts)]


def route_to_op(route: str) -> str:
    mapping = {
        "/teams": "chat",
        "/exchange": "mailbox",
        "/sharepoint": "files",
        "/onedrive": "files",
        "/graph": "graph",
        "/admin": "auth",
    }
    return mapping.get(route, "graph")


def simulate_flow_instance(
    state: str,
    flow_id: str,
    start_dt: datetime,
    ctl: Optional[IntervalCtl],
    inst_idx: int,
) -> List[Dict[str, Any]]:
    flow = SYSTEM["flows"][state][flow_id]
    emit_refs = flow["emit"]
    lat_pairs = flow["latency_ms"]
    trace_on = SYSTEM["tracing"]["on"] and flow.get("trace", False)

    trace_id = hex_str("trace", state, flow_id, fmt_ts(start_dt), inst_idx, length=32) if trace_on else ""

    lat_m50, lat_m95 = get_flow_lat_mult(flow_id, ctl, state)
    scaled_pairs: List[Tuple[float, float]] = []
    for (p50, p95) in lat_pairs:
        scaled_pairs.append((float(p50) * lat_m50, float(p95) * lat_m95))

    retry = flow.get("retry", {}) or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    backoff_pairs = retry.get("backoff_ms", []) or []

    if max_attempts <= 1:
        attempts = 1
    else:
        base = int(math.floor(expected_attempts))
        base = max(1, min(base, max_attempts))
        frac = clamp(expected_attempts - base, 0.0, 1.0)
        attempts = base + (1 if (base + 1) <= max_attempts and u01("attempts", flow_id, inst_idx) < frac else 0)

    ctx: Dict[str, Any] = {}
    instance_key = (state, flow_id, inst_idx, trace_id)
    regions = ["eu", "us", "apac"]

    if flow_id.startswith("m365_"):
        ctx["req_id"] = gen_uuid("req", flow_id, inst_idx, trace_id)
        ctx["method"] = choose(["GET", "POST"], *instance_key, "method")
        ctx["route"] = choose(["/teams", "/exchange", "/sharepoint", "/onedrive", "/graph", "/admin"], *instance_key, "route")
        ctx["region"] = choose(regions, *instance_key, "region")
        ctx["qname"] = "m365-backend.internal"
        ctx["op"] = route_to_op(ctx["route"])
    elif flow_id.startswith("status_"):
        ctx["req_id"] = gen_uuid("req", flow_id, inst_idx, trace_id)
        ctx["region"] = choose(regions, *instance_key, "region")
        ctx["qname"] = "status-backend.internal"
        ctx["page"] = "/status"
    elif flow_id.startswith("wan_probe"):
        ctx["src"] = choose(regions, *instance_key, "src")
        ctx["dst"] = choose(regions, *instance_key, "dst")

    comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        comp_id, _log_id = ref.split(".", 1)
        if comp_id not in comp_host:
            comp_host[comp_id] = pick_component_host(comp_id, instance_key)

    rows: List[Dict[str, Any]] = []
    attempt_start = start_dt

    for attempt in range(1, attempts + 1):
        att_key = (state, flow_id, inst_idx, trace_id, "attempt", attempt)
        att_ctx: Dict[str, Any] = {}
        if flow_id.startswith("wan_probe"):
            att_ctx["probe_id"] = hex_str("probe", flow_id, inst_idx, trace_id, attempt, length=16)

        # First sample all per-log gaps (ms).
        deltas_ms: List[int] = []
        for i, (p50, p95) in enumerate(scaled_pairs):
            d = sample_lognormal_ms(p50, p95, key=(att_key, "lat", i))
            deltas_ms.append(d)

        # Bind wan-probe timeout coherently:
        # timeout_ms chosen first (within domain); ensure send->healthz gap never exceeds timeout_ms.
        if flow_id.startswith("wan_probe") and len(deltas_ms) >= 2:
            sampled_gap = int(deltas_ms[1])
            timeout_ms = int(clamp(sampled_gap, 500, 5000))
            att_ctx["timeout_ms"] = timeout_ms
            second_log_id = emit_refs[1].split(".", 1)[1]
            if second_log_id == "healthz_timeout":
                deltas_ms[1] = timeout_ms
            else:
                deltas_ms[1] = int(min(deltas_ms[1], timeout_ms))
        elif flow_id.startswith("wan_probe"):
            att_ctx["timeout_ms"] = 1000

        # Clamp per-log delta values for timing fields that must match observed message fields.
        # This fixes coherence for repeated refs (e.g., multiple upstream_timeout entries) by clamping each occurrence.
        for i, ref in enumerate(emit_refs):
            comp_id, log_id = ref.split(".", 1)
            field: Optional[str] = None
            if log_id == "dns_answer_ok":
                field = "dur_ms"
            elif log_id in ("dns_timeout", "upstream_timeout", "healthz_timeout"):
                field = "waited_ms"
            if not field:
                continue
            dom = (SYSTEM["components"][comp_id]["logs"][log_id].get("vars") or {}).get(field)
            if not dom or dom.get("k") != "i":
                continue
            lo, hi = int(dom["v"][0]), int(dom["v"][1])
            deltas_ms[i] = int(clamp(deltas_ms[i], lo, hi))
            # Keep probe invariant after clamping: waited_ms must not exceed timeout_ms.
            if flow_id.startswith("wan_probe") and i == 1 and log_id == "healthz_timeout":
                deltas_ms[i] = int(min(deltas_ms[i], int(att_ctx.get("timeout_ms", 1000))))

        # Build timestamps from the final per-log deltas (each delta is since previous emitted log).
        stamps: List[datetime] = []
        t = attempt_start
        for d in deltas_ms:
            t = t + timedelta(milliseconds=int(d))
            stamps.append(t)

        def delta_prev(i_ref: int) -> int:
            return deltas_ms[i_ref] if i_ref >= 0 else deltas_ms[0]

        for i, ref in enumerate(emit_refs):
            comp_id, log_id = ref.split(".", 1)
            host = comp_host.get(comp_id, "")
            ts = stamps[i]
            values: Dict[str, Any] = {}

            tmpl_vars = (SYSTEM["components"][comp_id]["logs"][log_id].get("vars") or {})
            for vn, dom in tmpl_vars.items():
                values[vn] = gen_from_domain(dom, key=(comp_id, log_id, vn, state, flow_id, inst_idx, attempt, i, trace_id))

            if "req_id" in ctx:
                values["req_id"] = ctx["req_id"]
            if log_id == "req_in_m365":
                values["method"] = ctx["method"]
                values["route"] = ctx["route"]
                values["region"] = ctx["region"]
            if log_id == "req_in_status":
                values["region"] = ctx["region"]
            if log_id in ("dns_query", "dns_answer_ok", "dns_timeout"):
                values["qname"] = ctx["qname"]
            if log_id == "dns_answer_ok":
                # Must match the actual dns_query->dns_answer_ok gap (delta at this index).
                values["dur_ms"] = int(clamp(delta_prev(i), 1, 3000))
                values["ttl_s"] = int(clamp(values.get("ttl_s", 60), 10, 300))
            if log_id == "dns_timeout":
                values["waited_ms"] = int(clamp(delta_prev(i), 100, 6000))
            if comp_id == "m365_app_cluster" and log_id == "req_received":
                values["op"] = ctx.get("op", values.get("op"))
            if comp_id == "status_portal_app" and log_id == "req_received":
                values["page"] = ctx.get("page", "/status")

            if log_id == "upstream_timeout":
                svc = "m365" if flow_id.startswith("m365_") else "status_portal"
                values["svc"] = svc
                if flow_id == "m365_http_504":
                    attempt_no = 1 if i == 3 else 2 if i == 5 else 3
                elif flow_id == "status_portal_504":
                    attempt_no = 1 if i == 3 else 2
                else:
                    attempt_no = int(clamp(values.get("attempt", 1), 1, 3))
                values["attempt"] = attempt_no
                # Must match the actual gap leading into this upstream timeout (delta at this index).
                values["waited_ms"] = int(clamp(delta_prev(i), 300, 25000))

            if log_id == "upstream_retry":
                svc = "m365" if flow_id.startswith("m365_") else "status_portal"
                values["svc"] = svc
                if flow_id == "m365_http_504":
                    attempt_no = 2 if i == 4 else 3
                elif flow_id == "status_portal_504":
                    attempt_no = 2
                else:
                    attempt_no = int(clamp(values.get("attempt", 2), 2, 3))
                values["attempt"] = attempt_no
                values["reason"] = choose(["connect_timeout", "packet_loss", "no_route"], *att_key, "retry_reason", i)

            if log_id in ("resp_200_m365", "resp_503_m365", "resp_504_m365", "resp_200_status", "resp_504_status"):
                dur = int(round((ts - stamps[0]).total_seconds() * 1000))
                dur_dom = (SYSTEM["components"][comp_id]["logs"][log_id]["vars"].get("dur_ms") or {"k": "i", "v": [0, 10**9]})
                lo, hi = int(dur_dom["v"][0]), int(dur_dom["v"][1])
                values["dur_ms"] = int(clamp(dur, lo, hi))
                if log_id == "resp_503_m365":
                    if flow_id == "m365_dns_timeout_503":
                        values["err"] = "dns_timeout"
                if "bytes" in tmpl_vars:
                    base = 2000 if flow_id.startswith("status_") else 50000
                    mult = 10 if (ctx.get("route") in ("/sharepoint", "/onedrive")) else 4
                    b = base + (h_int(*instance_key, "bytes") % (base * mult))
                    values["bytes"] = int(clamp(b, int(tmpl_vars["bytes"]["v"][0]), int(tmpl_vars["bytes"]["v"][1])))

            if flow_id.startswith("wan_probe"):
                timeout_ms = int(att_ctx.get("timeout_ms", 1000))
                if log_id == "probe_send":
                    values["probe_id"] = att_ctx["probe_id"]
                    values["src"] = ctx["src"]
                    values["dst"] = ctx["dst"]
                    values["timeout_ms"] = int(clamp(timeout_ms, 500, 5000))
                if log_id == "healthz_ok":
                    values["probe_id"] = att_ctx["probe_id"]
                    rtt = int(round((ts - stamps[0]).total_seconds() * 1000))
                    values["rtt_ms"] = int(clamp(rtt, 5, 800))
                if log_id == "probe_result_ok":
                    values["probe_id"] = att_ctx["probe_id"]
                    rtt = int(round((stamps[1] - stamps[0]).total_seconds() * 1000))
                    values["rtt_ms"] = int(clamp(rtt, 10, 300))
                if log_id == "healthz_timeout":
                    values["probe_id"] = att_ctx["probe_id"]
                    waited = int(round((stamps[1] - stamps[0]).total_seconds() * 1000))
                    waited = int(min(waited, timeout_ms))
                    values["waited_ms"] = int(clamp(waited, 200, 8000))
                if log_id == "probe_result_fail":
                    values["probe_id"] = att_ctx["probe_id"]
                    waited = int(round((stamps[1] - stamps[0]).total_seconds() * 1000))
                    waited = int(min(waited, timeout_ms))
                    values["waited_ms"] = int(clamp(waited, 300, 8000))
                    values["err"] = choose(["timeout", "packet_loss"], *att_key, "probe_err")

            lvl, msg, svc, out_host = render_log(comp_id, log_id, values, host=host)
            rows.append(
                {
                    "ts": ts,
                    "level": lvl,
                    "message": msg,
                    "trace_id": trace_id,
                    "service": svc,
                    "host": out_host,
                }
            )

        if attempt < attempts:
            if backoff_pairs:
                p50, p95 = backoff_pairs[min(attempt - 1, len(backoff_pairs) - 1)]
                backoff = sample_lognormal_ms(p50, p95, key=(att_key, "backoff", attempt - 1), soft_cap_mult=3.0)
            else:
                backoff = 200
            attempt_start = stamps[-1] + timedelta(milliseconds=int(backoff))

    return rows


def simulate_background(
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    ctl: Optional[IntervalCtl],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for comp_id, comp in SYSTEM["components"].items():
        for emit in comp.get("beh", {}).get(state, []) or []:
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope") or "per_host"
            rate_key = f"{comp_id}.{log_id}"
            mult = get_rate_mult(rate_key, ctl, state)
            eff = per_min * mult
            if eff <= 0.0:
                continue

            dur_min = (end_dt - start_dt).total_seconds() / 60.0
            if scope == "global":
                cnt = stable_int_count(eff * dur_min, key=("bg", state, rate_key, fmt_ts(start_dt), fmt_ts(end_dt)))
                times = schedule_times(start_dt, end_dt, cnt, key=("bgts", state, rate_key, fmt_ts(start_dt), fmt_ts(end_dt)))
                hosts = comp.get("hosts") or [""]
                for j, ts in enumerate(times):
                    host = hosts[j % len(hosts)] if hosts else ""
                    values: Dict[str, Any] = {}
                    tmpl_vars = (comp["logs"][log_id].get("vars") or {})
                    for vn, dom in tmpl_vars.items():
                        values[vn] = gen_from_domain(dom, key=(comp_id, log_id, vn, state, fmt_ts(ts), j))
                    if comp_id == "wan_router_fleet" and log_id == "router_heartbeat":
                        values["router_id"] = host
                        up = 100_000 + int((ts - start_dt).total_seconds()) + (j % 1000)
                        values["uptime_s"] = int(clamp(up, 1000, 900000))
                    if comp_id == "wan_automation_controller" and log_id == "te_optimizer_tick":
                        # Keep within declared domain [1, 200000]
                        values["iter"] = 1 + (h_int("te_iter", fmt_ts(start_dt), j) % 200000)
                        values["paths"] = int(clamp(200 + (h_int("paths", fmt_ts(ts)) % 250), 50, 500))
                        values["delta"] = int(clamp(-10 + (h_int("delta", fmt_ts(ts)) % 21), -50, 50))
                    if comp_id == "wan_automation_controller" and log_id == "device_health_scan":
                        u = h_int("unhealthy", fmt_ts(ts)) % 5
                        r = max(0, u - (h_int("remed", fmt_ts(ts)) % 3))
                        values["unhealthy"] = int(clamp(u, 0, 30))
                        values["remediated"] = int(clamp(u - r, 0, 30))
                    if comp_id == "wan_router_fleet" and log_id == "routing_recompute_metric":
                        minute = int((ts - BASE).total_seconds() // 60)
                        values["in_progress"] = "true" if minute < 38 else "false"
                        values["adj_changed"] = int(clamp(50 + (h_int("adj", fmt_ts(ts)) % 350), 0, 500))
                    if comp_id == "wan_router_fleet" and log_id == "fwd_drop_metric":
                        m = mult
                        loss = clamp(2.0 + 4.0 * m + 6.0 * u01("loss", fmt_ts(ts)), 0.0, 25.0)
                        qd = int(clamp(300 + int(1200 * m) + (h_int("q", fmt_ts(ts)) % 1500), 0, 5000))
                        values["loss_pct"] = round(loss, 2)
                        values["qdepth"] = qd

                    lvl, msg, svc, out_host = render_log(comp_id, log_id, values, host=host)
                    rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": out_host})
            else:
                for host in (comp.get("hosts") or [""]):
                    cnt = stable_int_count(eff * dur_min, key=("bg", state, rate_key, host, fmt_ts(start_dt), fmt_ts(end_dt)))
                    times = schedule_times(start_dt, end_dt, cnt, key=("bgts", state, rate_key, host, fmt_ts(start_dt), fmt_ts(end_dt)))
                    for j, ts in enumerate(times):
                        values = {}
                        tmpl_vars = (comp["logs"][log_id].get("vars") or {})
                        for vn, dom in tmpl_vars.items():
                            values[vn] = gen_from_domain(dom, key=(comp_id, log_id, vn, state, host, fmt_ts(ts), j))
                        if comp_id == "wan_router_fleet" and log_id == "router_heartbeat":
                            values["router_id"] = host
                            minute = int((ts - BASE).total_seconds() // 60)
                            up = 200_000 + minute * 60 + (j * 5) + (h_int(host, "u") % 1000)
                            values["uptime_s"] = int(clamp(up, 1000, 900000))
                        if comp_id == "wan_router_fleet" and log_id == "routing_recompute_metric":
                            minute = int((ts - BASE).total_seconds() // 60)
                            values["in_progress"] = "true" if minute < 38 else "false"
                            values["adj_changed"] = int(clamp(50 + (h_int(host, fmt_ts(ts), "adj") % 350), 0, 500))
                        if comp_id == "wan_router_fleet" and log_id == "fwd_drop_metric":
                            m = mult
                            loss = clamp(2.0 + 4.0 * m + 6.0 * u01(host, "loss", fmt_ts(ts)), 0.0, 25.0)
                            qd = int(clamp(300 + int(1200 * m) + (h_int(host, "q", fmt_ts(ts)) % 1500), 0, 5000))
                            values["loss_pct"] = round(loss, 2)
                            values["qdepth"] = qd
                        lvl, msg, svc, out_host = render_log(comp_id, log_id, values, host=host)
                        rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": out_host})
    return rows


def simulate_one_shots(base: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for e in sorted(SCENARIO["events"], key=lambda x: (x["at_min"], x["order"])):
        at_dt = base + timedelta(minutes=int(e["at_min"]))
        for os in (e.get("one_shots") or []):
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            count = int(os["count"])
            hosts = os.get("hosts") or (SYSTEM["components"][comp_id].get("hosts") or [""])
            for i in range(count):
                host = hosts[i % len(hosts)] if hosts else ""
                jit_ms = int(20 + 200 * u01("oneshot", ref, e["at_min"], i))
                ts = at_dt + timedelta(milliseconds=jit_ms)
                values: Dict[str, Any] = {}
                tmpl_vars = (SYSTEM["components"][comp_id]["logs"][log_id].get("vars") or {})
                for vn, dom in tmpl_vars.items():
                    values[vn] = gen_from_domain(dom, key=("oneshot", comp_id, log_id, vn, e["at_min"], i))
                if ref == "wan_router_fleet.config_cmd_ip_change":
                    values["old_ip"] = "10.10.0.1"
                    values["new_ip"] = "10.10.0.2"
                    values["cmd"] = choose(["set-ip-fast", "set-ip"], "cmd", e["at_min"])
                    values["change_id"] = gen_str_hint("CHG[0-9]{6}", "chg", e["at_min"])
                if ref == "wan_router_fleet.adjacency_recompute_start":
                    values["reason"] = "ip_change"
                    values["scope"] = "wan-wide"
                if ref == "wan_automation_controller.automation_paused":
                    values["subsystem"] = "traffic_engineering"
                    values["reason"] = "excessive_timeouts"
                if ref == "wan_automation_controller.automation_manual_restart":
                    values["subsystem"] = "traffic_engineering" if i % 2 == 0 else "device_health"
                    values["run_id"] = hex_str("run", e["at_min"], i, length=8)

                lvl, msg, svc, out_host = render_log(comp_id, log_id, values, host=host)
                rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": out_host})
    return rows


# ------------------------ main simulation ------------------------

BASE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

def simulate() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    rows.extend(simulate_one_shots(BASE))

    n_start_m = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end_m = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start = BASE + timedelta(minutes=n_start_m)
    n_end = BASE + timedelta(minutes=n_end_m)

    rows.extend(simulate_background("n", n_start, n_end, ctl=None))

    for flow_id, flow in SYSTEM["flows"]["n"].items():
        rpm = float(flow["rpm"])
        dur_min = (n_end - n_start).total_seconds() / 60.0
        inst_count = stable_int_count(rpm * dur_min, key=("flowcnt", "n", flow_id, n_start_m, n_end_m))
        starts = schedule_times(n_start, n_end, inst_count, key=("flowts", "n", flow_id, n_start_m, n_end_m))
        for idx, st in enumerate(starts):
            rows.extend(simulate_flow_instance("n", flow_id, st, ctl=None, inst_idx=idx))

    f_intervals = build_failure_intervals()
    for ctl in f_intervals:
        f_start = BASE + timedelta(minutes=ctl.start_min)
        f_end = BASE + timedelta(minutes=ctl.end_min)

        rows.extend(simulate_background("f", f_start, f_end, ctl=ctl))

        for flow_id, flow in SYSTEM["flows"]["f"].items():
            base_rpm = float(flow["rpm"])
            mult = get_rate_mult(flow_id, ctl, "f")
            eff_rpm = base_rpm * mult
            if eff_rpm <= 0.0:
                continue
            dur_min = (f_end - f_start).total_seconds() / 60.0
            inst_count = stable_int_count(eff_rpm * dur_min, key=("flowcnt", "f", flow_id, ctl.start_min, ctl.end_min))
            starts = schedule_times(f_start, f_end, inst_count, key=("flowts", "f", flow_id, ctl.start_min, ctl.end_min))
            for idx, st in enumerate(starts):
                inst_idx2 = int(h_int("inst", flow_id, ctl.start_min, ctl.end_min, idx) % 1_000_000_000)
                rows.extend(simulate_flow_instance("f", flow_id, st, ctl=ctl, inst_idx=inst_idx2))

    df = pd.DataFrame(rows)
    df = df.sort_values("ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["ts"].map(fmt_ts)
    df = df.drop(columns=["ts"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


if __name__ == "__main__":
    df_out = simulate()
    df_out.to_csv("logs.csv", index=False)
