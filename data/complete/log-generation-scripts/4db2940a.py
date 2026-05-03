import hashlib
import math
import random
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "code_hosting_platform"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_lb", "net_monitor"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_lb",
            "svc": "edge-lb",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "access_web_200": {
                    "lvl": "INFO",
                    "msg": "web access {method} {route} status=200 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "bytes_out": {"k": "i", "v": [300, 2000000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [10, 600]}}, "f": {"duration_ms": {"k": "i", "v": [80, 8000]}}},
                },
                "access_web_504": {
                    "lvl": "WARN",
                    "msg": "web access {method} {route} status=504 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "bytes_out": {"k": "i", "v": [0, 5000]},
                        "duration_ms": {"k": "i", "v": [2000, 12000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {},
                },
                "access_web_503": {
                    "lvl": "ERROR",
                    "msg": "web access {method} {route} status=503 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "bytes_out": {"k": "i", "v": [0, 2000]},
                        "duration_ms": {"k": "i", "v": [10, 3000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {},
                },
                "access_git_200": {
                    "lvl": "INFO",
                    "msg": "git access {route} status=200 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "route": {"k": "str", "v": "git_route"},
                        "bytes_out": {"k": "i", "v": [10000, 50000000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [80, 9000]}}, "f": {"duration_ms": {"k": "i", "v": [500, 25000]}}},
                },
                "access_git_504": {
                    "lvl": "WARN",
                    "msg": "git access {route} status=504 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "route": {"k": "str", "v": "git_route"},
                        "bytes_out": {"k": "i", "v": [0, 8000]},
                        "duration_ms": {"k": "i", "v": [4000, 30000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {},
                },
                "access_git_503": {
                    "lvl": "ERROR",
                    "msg": "git access {route} status=503 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "route": {"k": "str", "v": "git_route"},
                        "bytes_out": {"k": "i", "v": [0, 3000]},
                        "duration_ms": {"k": "i", "v": [10, 3000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {},
                },
                "access_git_500": {
                    "lvl": "ERROR",
                    "msg": "git access {route} status=500 bytes={bytes_out} duration_ms={duration_ms} req_id={req_id} client_ip={client_ip}",
                    "vars": {
                        "route": {"k": "str", "v": "git_route"},
                        "bytes_out": {"k": "i", "v": [0, 5000]},
                        "duration_ms": {"k": "i", "v": [50, 10000]},
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": None},
                    },
                    "state_vars": {},
                },
                "access_probe_200": {
                    "lvl": "INFO",
                    "msg": "probe access {route} status=200 duration_ms={duration_ms} req_id={req_id}",
                    "vars": {"route": {"k": "ch", "v": ["/", "/api/status"]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [5, 200]}}, "f": {"duration_ms": {"k": "i", "v": [50, 5000]}}},
                },
                "access_probe_504": {
                    "lvl": "WARN",
                    "msg": "probe access {route} status=504 duration_ms={duration_ms} req_id={req_id}",
                    "vars": {"route": {"k": "ch", "v": ["/", "/api/status"]}, "duration_ms": {"k": "i", "v": [300, 15000]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "access_probe_503": {
                    "lvl": "ERROR",
                    "msg": "probe access {route} status=503 duration_ms={duration_ms} req_id={req_id}",
                    "vars": {"route": {"k": "ch", "v": ["/", "/api/status"]}, "duration_ms": {"k": "i", "v": [10, 4000]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "app_web",
            "svc": "app-web",
            "hosts": ["app-1", "app-2"],
            "logs": {
                "req_ok": {
                    "lvl": "INFO",
                    "msg": "handled {method} {route} status=200 duration_ms={duration_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "duration_ms": {"k": "i", "v": [5, 500]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                    "state_vars": {},
                },
                "req_ok_slow": {
                    "lvl": "WARN",
                    "msg": "handled {method} {route} status=200 slow duration_ms={duration_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "duration_ms": {"k": "i", "v": [200, 9000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                    "state_vars": {},
                },
                "req_failed_timeout": {
                    "lvl": "ERROR",
                    "msg": "request failed {method} {route} error=timeout dep={dep} timeout_ms={timeout_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "str", "v": "web_route"},
                        "dep": {"k": "ch", "v": ["db", "cache", "fileserver"]},
                        "timeout_ms": {"k": "i", "v": [500, 8000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                    "state_vars": {},
                },
                "threadpool_queue_warn": {
                    "lvl": "WARN",
                    "msg": "threadpool queueing queue_depth={queue_depth} active_threads={active_threads}",
                    "vars": {"queue_depth": {"k": "i", "v": [0, 300]}, "active_threads": {"k": "i", "v": [20, 200]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "threadpool_queue_warn", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "threadpool_queue_warn", "per_min": 0.4, "scope": "per_host"}]},
            },
        },
        {
            "id": "git_service",
            "svc": "git",
            "hosts": ["git-1", "git-2"],
            "logs": {
                "clone_complete": {
                    "lvl": "INFO",
                    "msg": "git session complete repo={repo} pack_bytes={pack_bytes} duration_ms={duration_ms} req_id={req_id}",
                    "vars": {"repo": {"k": "str", "v": "owner/repo"}, "pack_bytes": {"k": "i", "v": [10000, 50000000]}, "duration_ms": {"k": "i", "v": [50, 12000]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "clone_slow": {
                    "lvl": "WARN",
                    "msg": "git session slow repo={repo} pack_bytes={pack_bytes} duration_ms={duration_ms} req_id={req_id}",
                    "vars": {"repo": {"k": "str", "v": "owner/repo"}, "pack_bytes": {"k": "i", "v": [10000, 50000000]}, "duration_ms": {"k": "i", "v": [1000, 30000]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "clone_failed_timeout": {
                    "lvl": "ERROR",
                    "msg": "git session failed repo={repo} error=timeout stage={stage} timeout_ms={timeout_ms} req_id={req_id}",
                    "vars": {"repo": {"k": "str", "v": "owner/repo"}, "stage": {"k": "ch", "v": ["negotiate", "read_objects", "pack"]}, "timeout_ms": {"k": "i", "v": [2000, 30000]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "clone_failed_storage": {
                    "lvl": "ERROR",
                    "msg": "git session failed repo={repo} error=storage fileserver={fileserver} detail={detail} req_id={req_id}",
                    "vars": {"repo": {"k": "str", "v": "owner/repo"}, "fileserver": {"k": "ch", "v": ["fs-1", "fs-2"]}, "detail": {"k": "ch", "v": ["stale_handle", "io_error", "not_primary"]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "fileserver_cluster",
            "svc": "fileserver",
            "hosts": ["fs-1", "fs-2"],
            "logs": {
                "ha_heartbeat": {
                    "lvl": "INFO",
                    "msg": "ha heartbeat peer={peer} role={role} latency_ms={latency_ms}",
                    "vars": {"peer": {"k": "ch", "v": ["fs-1", "fs-2"]}, "role": {"k": "ch", "v": ["primary", "secondary"]}},
                    "state_vars": {"n": {"latency_ms": {"k": "i", "v": [1, 50]}}, "f": {"latency_ms": {"k": "i", "v": [20, 2000]}}},
                },
                "ha_peer_disconnected": {
                    "lvl": "ERROR",
                    "msg": "ha peer link down peer={peer} cluster_state={cluster_state}",
                    "vars": {"peer": {"k": "ch", "v": ["fs-1", "fs-2"]}, "cluster_state": {"k": "ch", "v": ["degraded", "split_brain_suspected", "standby"]}},
                    "state_vars": {},
                },
                "node_fenced": {
                    "lvl": "CRITICAL",
                    "msg": "fencing node={node} reason={reason}",
                    "vars": {"node": {"k": "ch", "v": ["fs-1", "fs-2"]}, "reason": {"k": "ch", "v": ["heartbeat_timeout", "quorum_loss"]}},
                    "state_vars": {},
                },
                "repo_io_error": {
                    "lvl": "ERROR",
                    "msg": "repo i/o error repo={repo} op={op} err={err}",
                    "vars": {"repo": {"k": "str", "v": "owner/repo"}, "op": {"k": "ch", "v": ["read_pack", "stat_objects"]}, "err": {"k": "ch", "v": ["stale_handle", "eio", "etimedout"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ha_heartbeat", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "ha_heartbeat", "per_min": 0.6, "scope": "per_host"}, {"id": "ha_peer_disconnected", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "access_switch",
            "svc": None,
            "hosts": ["asw-7a", "asw-7b"],
            "logs": {
                "uplink_crc_errors": {
                    "lvl": "WARN",
                    "msg": "uplink errors cabinet={cabinet} port={port} crc_errors={crc_errors} drops={drops}",
                    "vars": {"cabinet": {"k": "ch", "v": ["cab-7"]}, "port": {"k": "ch", "v": ["xe-0/0/1", "xe-0/0/2"]}},
                    "state_vars": {"n": {"crc_errors": {"k": "i", "v": [0, 50]}, "drops": {"k": "i", "v": [0, 200]}}, "f": {"crc_errors": {"k": "i", "v": [50, 5000]}, "drops": {"k": "i", "v": [200, 20000]}}},
                },
                "watchdog_errdisable": {
                    "lvl": "ERROR",
                    "msg": "link watchdog triggered; errdisabling ports {port_a},{port_b} reason={reason}",
                    "vars": {"port_a": {"k": "ch", "v": ["xe-0/0/1", "xe-0/0/2"]}, "port_b": {"k": "ch", "v": ["xe-0/0/1", "xe-0/0/2"]}, "reason": {"k": "ch", "v": ["rx_mismatch", "partial_failure_detected"]}},
                    "state_vars": {},
                },
                "stp_loop_guard": {
                    "lvl": "CRITICAL",
                    "msg": "stp loop guard: bridge loop detected; blocking port {port} vlan={vlan}",
                    "vars": {"port": {"k": "ch", "v": ["xe-0/0/1", "xe-0/0/2"]}, "vlan": {"k": "i", "v": [1, 4094]}},
                    "state_vars": {},
                },
                "config_apply": {
                    "lvl": "INFO",
                    "msg": "applied config change id={change_id} setting={setting} result={result}",
                    "vars": {"change_id": {"k": "hex", "v": 8}, "setting": {"k": "ch", "v": ["lacp_link_watchdog_disable"]}, "result": {"k": "ch", "v": ["ok", "failed"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "uplink_crc_errors", "per_min": 0.4, "scope": "per_host"}]},
                "f": {"emit": [{"id": "uplink_crc_errors", "per_min": 2.0, "scope": "per_host"}, {"id": "watchdog_errdisable", "per_min": 0.3, "scope": "per_host"}]},
            },
        },
        {
            "id": "aggregation_switch",
            "svc": None,
            "hosts": ["aggsw-1", "aggsw-2"],
            "logs": {
                "mac_table_stats": {
                    "lvl": "INFO",
                    "msg": "fdb stats learned={learned} aged_out={aged_out} table_usage_pct={table_usage_pct}",
                    "vars": {},
                    "state_vars": {"n": {"learned": {"k": "i", "v": [50000, 120000]}, "aged_out": {"k": "i", "v": [100, 3000]}, "table_usage_pct": {"k": "i", "v": [10, 60]}}, "f": {"learned": {"k": "i", "v": [10000, 70000]}, "aged_out": {"k": "i", "v": [500, 8000]}, "table_usage_pct": {"k": "i", "v": [20, 95]}}},
                },
                "fdb_miss_flood": {
                    "lvl": "WARN",
                    "msg": "fdb miss: flooding unknown dst_mac={dst_mac} pps={pps} rate_limited={rate_limited}",
                    "vars": {"dst_mac": {"k": "str", "v": "mac"}, "rate_limited": {"k": "ch", "v": ["true", "false"]}},
                    "state_vars": {"n": {"pps": {"k": "i", "v": [10, 2000]}}, "f": {"pps": {"k": "i", "v": [500, 100000]}}},
                },
                "control_plane_cpu": {
                    "lvl": "WARN",
                    "msg": "control-plane cpu high cpu_pct={cpu_pct} cause={cause}",
                    "vars": {"cpu_pct": {"k": "i", "v": [50, 99]}, "cause": {"k": "ch", "v": ["l2_flood", "stp_recalc"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "mac_table_stats", "per_min": 1.0, "scope": "per_host"}, {"id": "fdb_miss_flood", "per_min": 0.3, "scope": "per_host"}]},
                "f": {"emit": [{"id": "mac_table_stats", "per_min": 1.5, "scope": "per_host"}, {"id": "fdb_miss_flood", "per_min": 8.0, "scope": "per_host"}, {"id": "control_plane_cpu", "per_min": 0.4, "scope": "per_host"}]},
            },
        },
        {
            "id": "net_monitor",
            "svc": "netmon",
            "hosts": ["netmon-1"],
            "logs": {
                "uplink_utilization": {
                    "lvl": "INFO",
                    "msg": "uplinks util avg_pct={avg_pct} p95_pct={p95_pct} drops_per_s={drops_per_s}",
                    "vars": {},
                    "state_vars": {"n": {"avg_pct": {"k": "i", "v": [10, 40]}, "p95_pct": {"k": "i", "v": [15, 60]}, "drops_per_s": {"k": "i", "v": [0, 50]}}, "f": {"avg_pct": {"k": "i", "v": [60, 95]}, "p95_pct": {"k": "i", "v": [80, 99]}, "drops_per_s": {"k": "i", "v": [50, 4000]}}},
                },
                "alert_saturation": {
                    "lvl": "WARN",
                    "msg": "ALERT saturation agg_access_links util_p95={util_p95} drops_per_s={drops_per_s}",
                    "vars": {"util_p95": {"k": "i", "v": [80, 99]}, "drops_per_s": {"k": "i", "v": [200, 5000]}},
                    "state_vars": {},
                },
                "poll_failed": {
                    "lvl": "ERROR",
                    "msg": "poll failed target={target} proto={proto} error={error}",
                    "vars": {"target": {"k": "ch", "v": ["asw-7a", "asw-7b", "aggsw-1", "aggsw-2"]}, "proto": {"k": "ch", "v": ["snmp", "icmp"]}, "error": {"k": "ch", "v": ["timeout", "no_route"]}},
                    "state_vars": {},
                },
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "synthetic probe ok path={path_name} status=200 latency_ms={latency_ms} req_id={req_id}",
                    "vars": {"path_name": {"k": "ch", "v": ["homepage", "api_status"]}, "latency_ms": {"k": "i", "v": [5, 300]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "probe_fail_504": {
                    "lvl": "ERROR",
                    "msg": "synthetic probe failed path={path_name} status=504 latency_ms={latency_ms} err={err} req_id={req_id}",
                    "vars": {"path_name": {"k": "ch", "v": ["homepage", "api_status"]}, "latency_ms": {"k": "i", "v": [300, 15000]}, "err": {"k": "ch", "v": ["upstream_timeout", "backend_slow"]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "probe_fail_503": {
                    "lvl": "ERROR",
                    "msg": "synthetic probe failed path={path_name} status=503 latency_ms={latency_ms} err={err} req_id={req_id}",
                    "vars": {"path_name": {"k": "ch", "v": ["homepage", "api_status"]}, "latency_ms": {"k": "i", "v": [10, 4000]}, "err": {"k": "ch", "v": ["no_upstreams", "routing_blackhole"]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {},
                },
                "change_connect_uplink": {"lvl": "INFO", "msg": "operator action connect uplink target={target} change_id={change_id}", "vars": {"target": {"k": "ch", "v": ["asw-7a", "asw-7b", "aggsw-1", "aggsw-2"]}, "change_id": {"k": "hex", "v": 8}}, "state_vars": {}},
                "change_shutdown_uplink": {"lvl": "INFO", "msg": "operator action shutdown uplink target={target} change_id={change_id}", "vars": {"target": {"k": "ch", "v": ["asw-7a", "asw-7b", "aggsw-1", "aggsw-2"]}, "change_id": {"k": "hex", "v": 8}}, "state_vars": {}},
                "change_remove_watchdog_setting": {"lvl": "INFO", "msg": "operator action remove watchdog setting target={target} change_id={change_id}", "vars": {"target": {"k": "ch", "v": ["asw-7a", "asw-7b"]}, "change_id": {"k": "hex", "v": 8}}, "state_vars": {}},
                "change_enable_uplink": {"lvl": "INFO", "msg": "operator action enable uplink target={target} change_id={change_id}", "vars": {"target": {"k": "ch", "v": ["asw-7a", "asw-7b", "aggsw-1", "aggsw-2"]}, "change_id": {"k": "hex", "v": 8}}, "state_vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "uplink_utilization", "per_min": 2.0, "scope": "global"}, {"id": "alert_saturation", "per_min": 0.02, "scope": "global"}, {"id": "poll_failed", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "uplink_utilization", "per_min": 3.0, "scope": "global"}, {"id": "alert_saturation", "per_min": 0.8, "scope": "global"}, {"id": "poll_failed", "per_min": 0.05, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "web_page_view_ok", "rpm": 400.0, "emit": ["app_web.req_ok", "edge_lb.access_web_200"], "latency_ms": [[15, 120], [20, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "git_clone_ok", "rpm": 60.0, "emit": ["git_service.clone_complete", "edge_lb.access_git_200"], "latency_ms": [[300, 4000], [350, 4500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "synthetic_probe_ok", "rpm": 12.0, "emit": ["edge_lb.access_probe_200", "net_monitor.probe_ok"], "latency_ms": [[10, 120], [5, 60]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
        "f": {
            "req": [
                {"id": "web_page_view_slow_ok", "rpm": 240.0, "emit": ["app_web.req_ok_slow", "edge_lb.access_web_200"], "latency_ms": [[300, 2500], [350, 3000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "web_page_view_timeout_504", "rpm": 140.0, "emit": ["app_web.req_failed_timeout", "edge_lb.access_web_504"], "latency_ms": [[800, 6000], [1000, 12000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "web_page_view_hard_down_503", "rpm": 380.0, "emit": ["edge_lb.access_web_503"], "latency_ms": [[50, 1500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "git_clone_slow_ok", "rpm": 40.0, "emit": ["git_service.clone_slow", "edge_lb.access_git_200"], "latency_ms": [[1500, 12000], [1800, 14000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "git_clone_timeout_504", "rpm": 20.0, "emit": ["git_service.clone_failed_timeout", "edge_lb.access_git_504"], "latency_ms": [[3000, 20000], [4000, 30000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "git_clone_hard_down_503", "rpm": 55.0, "emit": ["edge_lb.access_git_503"], "latency_ms": [[50, 1500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "git_clone_repo_unavailable_500", "rpm": 1.5, "emit": ["fileserver_cluster.repo_io_error", "git_service.clone_failed_storage", "edge_lb.access_git_500"], "latency_ms": [[50, 1500], [200, 6000], [250, 8000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "synthetic_probe_ok_f", "rpm": 2.0, "emit": ["edge_lb.access_probe_200", "net_monitor.probe_ok"], "latency_ms": [[50, 800], [20, 300]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "synthetic_probe_timeout_504", "rpm": 10.0, "emit": ["edge_lb.access_probe_504", "net_monitor.probe_fail_504"], "latency_ms": [[300, 12000], [500, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "synthetic_probe_no_upstreams_503", "rpm": 12.0, "emit": ["edge_lb.access_probe_503", "net_monitor.probe_fail_503"], "latency_ms": [[20, 2000], [10, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "agg_switch_mac_learning_incident",
        "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 55}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "web_page_view_hard_down_503": 0.0,
                            "git_clone_hard_down_503": 0.0,
                            "synthetic_probe_no_upstreams_503": 0.0,
                            "access_switch.watchdog_errdisable": 0.0,
                            "net_monitor.poll_failed": 0.0,
                            "fileserver_cluster.ha_peer_disconnected": 0.0,
                        },
                        "latency_multipliers": {
                            "web_page_view_slow_ok": {"p50": 1.0, "p95": 1.0},
                            "web_page_view_timeout_504": {"p50": 1.0, "p95": 1.0},
                            "git_clone_slow_ok": {"p50": 1.0, "p95": 1.0},
                            "git_clone_timeout_504": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [
                            {"ref": "net_monitor.change_connect_uplink", "count": 1, "hosts": ["netmon-1"]},
                            {"ref": "access_switch.stp_loop_guard", "count": 1, "hosts": ["asw-7a"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 30,
                        "rate_multipliers": {
                            "web_page_view_slow_ok": 0.0,
                            "web_page_view_timeout_504": 0.0,
                            "git_clone_slow_ok": 0.0,
                            "git_clone_timeout_504": 0.0,
                            "git_clone_repo_unavailable_500": 0.0,
                            "synthetic_probe_ok_f": 0.0,
                            "synthetic_probe_timeout_504": 0.0,
                            "web_page_view_hard_down_503": 1.0,
                            "git_clone_hard_down_503": 1.0,
                            "synthetic_probe_no_upstreams_503": 1.0,
                            "access_switch.watchdog_errdisable": 25.0,
                            "net_monitor.poll_failed": 20.0,
                            "fileserver_cluster.ha_peer_disconnected": 6.0,
                        },
                        "latency_multipliers": {
                            "web_page_view_hard_down_503": {"p50": 1.0, "p95": 1.0},
                            "git_clone_hard_down_503": {"p50": 1.0, "p95": 1.0},
                            "synthetic_probe_no_upstreams_503": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [{"ref": "net_monitor.change_shutdown_uplink", "count": 1, "hosts": ["netmon-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 48,
                        "rate_multipliers": {
                            "web_page_view_hard_down_503": 0.0,
                            "git_clone_hard_down_503": 0.0,
                            "synthetic_probe_no_upstreams_503": 0.0,
                            "web_page_view_slow_ok": 1.0,
                            "web_page_view_timeout_504": 1.0,
                            "git_clone_slow_ok": 1.0,
                            "git_clone_timeout_504": 1.0,
                            "git_clone_repo_unavailable_500": 1.0,
                            "synthetic_probe_ok_f": 1.0,
                            "synthetic_probe_timeout_504": 1.0,
                            "net_monitor.poll_failed": 0.0,
                            "access_switch.watchdog_errdisable": 0.0,
                            "fileserver_cluster.ha_peer_disconnected": 3.0,
                            "aggregation_switch.fdb_miss_flood": 1.3,
                            "net_monitor.alert_saturation": 1.2,
                        },
                        "latency_multipliers": {
                            "web_page_view_slow_ok": {"p50": 1.0, "p95": 1.0},
                            "web_page_view_timeout_504": {"p50": 1.0, "p95": 1.0},
                            "git_clone_slow_ok": {"p50": 1.0, "p95": 1.0},
                            "git_clone_timeout_504": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [
                            {"ref": "net_monitor.change_remove_watchdog_setting", "count": 1, "hosts": ["netmon-1"]},
                            {"ref": "net_monitor.change_enable_uplink", "count": 2, "hosts": ["netmon-1"]},
                            {"ref": "access_switch.config_apply", "count": 2, "hosts": ["asw-7a", "asw-7b"]},
                            {"ref": "fileserver_cluster.node_fenced", "count": 1, "hosts": ["fs-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def hash_u01(key: str) -> float:
    h = hashlib.md5(key.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big") & ((1 << 53) - 1)
    return x / float(1 << 53)


def hash_i64(key: str) -> int:
    h = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def stable_int(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if hash_u01(key + ":round") < frac else 0)


def norminv(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def sample_lognormal_ms(p50: float, p95: float, u: float) -> int:
    p50 = max(1.0, float(p50))
    p95 = max(p50 * 1.01, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = norminv(u)
    x = math.exp(mu + sigma * z)
    cap = 3.0 * p95
    x = min(x, cap)
    return max(1, int(round(x)))


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    us = int(round(dt.microsecond / 1000.0)) * 1000
    if us >= 1_000_000:
        dt = dt + timedelta(seconds=1)
        us = 0
    dt = dt.replace(microsecond=us)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def schedule_even_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_ms = int((end - start).total_seconds() * 1000)
    if duration_ms <= 0:
        return [start] * count
    out: List[datetime] = []
    for i in range(count):
        base = int(round((i + 0.5) * duration_ms / count)) if count > 1 else duration_ms // 2
        step = duration_ms / count
        max_jitter = int(min(500, max(0.0, step * 0.2)))
        j = int(round((hash_u01(f"{key}:jit:{i}") - 0.5) * 2.0 * max_jitter)) if max_jitter > 0 else 0
        off = min(max(0, base + j), duration_ms - 1)
        out.append(start + timedelta(milliseconds=off))
    return out


def get_placeholders(msg: str) -> List[str]:
    return list(dict.fromkeys(PLACEHOLDER_RE.findall(msg)))


WEB_ROUTES = ["/", "/login", "/settings", "/notifications", "/api/status", "/api/repos", "/api/issues", "/api/pulls", "/assets/app.js"]
REPOS = [
    "acme/widgets",
    "acme/api",
    "octo/hello",
    "infra/terraform",
    "infra/ansible",
    "ml/models",
    "ml/serving",
    "frontend/web",
    "backend/core",
    "security/audit",
    "docs/site",
    "tools/cli",
    "tools/release",
    "ops/runbooks",
    "data/pipelines",
    "mobile/app",
]
GIT_SUFFIXES = [".git/info/refs", ".git/git-upload-pack", ".git/objects/pack/pack-123.pack"]


def gen_str_hint(hint: str, key: str) -> str:
    if hint == "web_route":
        return WEB_ROUTES[hash_i64(key) % len(WEB_ROUTES)]
    if hint == "owner/repo":
        return REPOS[hash_i64(key) % len(REPOS)]
    if hint == "git_route":
        repo = REPOS[hash_i64(key + ":repo") % len(REPOS)]
        suf = GIT_SUFFIXES[hash_i64(key + ":suf") % len(GIT_SUFFIXES)]
        return f"/{repo}{suf}"
    if hint == "mac":
        h = md5_hex(key)
        return ":".join([h[i : i + 2] for i in range(0, 12, 2)])
    return f"{hint}-{md5_hex(key)[:8]}"


def sample_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "i":
        a, b = int(v[0]), int(v[1])
        if b < a:
            a, b = b, a
        return a + (hash_i64(key) % (b - a + 1))
    if k == "f":
        a, b = float(v[0]), float(v[1])
        u = hash_u01(key)
        return a + u * (b - a)
    if k == "ch":
        arr = list(v)
        if not arr:
            return ""
        return arr[hash_i64(key) % len(arr)]
    if k == "uuid":
        h = md5_hex(key)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "hex":
        ln = int(v)
        return md5_hex(key)[:ln]
    if k == "ip":
        last = (hash_i64(key) % 254) + 1
        return f"198.51.100.{last}"
    if k == "str":
        return gen_str_hint(str(v), key)
    return str(v) if v is not None else ""


def merge_domains(template: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    out = {}
    out.update(template.get("vars", {}) or {})
    sv = template.get("state_vars", {}) or {}
    if state in sv:
        out.update(sv[state] or {})
    return out


def choose_host(component: Dict[str, Any], key: str) -> str:
    hosts = component.get("hosts") or []
    if not hosts:
        return ""
    return hosts[hash_i64(key) % len(hosts)]


def flow_trace_id(flow: Dict[str, Any], inst_key: str) -> str:
    if not flow.get("trace", False):
        return ""
    return md5_hex(inst_key + ":trace")[:32]


@dataclass
class Segment:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    comps = {c["id"]: c for c in system["components"]}
    templates: Dict[str, Dict[str, Any]] = {}
    for cid, c in comps.items():
        for lid, t in (c.get("logs") or {}).items():
            templates[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **t}
    return comps, templates


def build_segments(scenario: Dict[str, Any]) -> List[Segment]:
    ph = scenario["scenario"]["time"]["phases"]
    n_start, n_end = ph["n"]["start_min"], ph["n"]["end_min"]
    f_start, f_end = ph["f"]["start_min"], ph["f"]["end_min"]

    segs: List[Segment] = [Segment(state="n", start_min=n_start, end_min=n_end, rate_mult={}, latency_mult={})]

    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted({f_start, f_end, *[e["at_min"] for e in events]})
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries.append(f_end)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    idx = 0
    for i in range(len(boundaries) - 1):
        seg_start = boundaries[i]
        seg_end = boundaries[i + 1]
        while idx < len(events) and events[idx]["at_min"] == seg_start:
            e = events[idx]
            for k, v in (e.get("rate_multipliers") or {}).items():
                active_rate[k] = float(v)
            for fid, m in (e.get("latency_multipliers") or {}).items():
                active_lat[fid] = {"p50": float(m.get("p50", 1.0)), "p95": float(m.get("p95", 1.0))}
            idx += 1
        segs.append(Segment(state="f", start_min=seg_start, end_min=seg_end, rate_mult=deepcopy(active_rate), latency_mult=deepcopy(active_lat)))
    return segs


def get_flow_defs(system: Dict[str, Any], state: str) -> List[Dict[str, Any]]:
    return list(system["flows"][state]["req"])


def get_allowed_range_for_key(template: Dict[str, Any], state: str, keyname: str) -> Optional[Tuple[int, int]]:
    domains = merge_domains(template, state)
    if keyname not in domains:
        return None
    dom = domains[keyname]
    if dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    if hi < lo:
        lo, hi = hi, lo
    return lo, hi


def _timing_bounds_for_template(template: Dict[str, Any], state: str) -> Optional[Tuple[int, int]]:
    ph = set(get_placeholders(template["msg"]))
    if "duration_ms" in ph:
        rng = get_allowed_range_for_key(template, state, "duration_ms")
        if rng:
            return rng
    if "latency_ms" in ph:
        rng = get_allowed_range_for_key(template, state, "latency_ms")
        if rng:
            return rng
    return None


def _apply_constraints_by_scaling_and_adjustment(delays: List[int], constraints: Dict[int, Tuple[int, int]]) -> List[int]:
    n = len(delays)
    if n == 0 or not constraints:
        return delays

    def cum_list(ds: List[int]) -> List[int]:
        out = []
        s = 0
        for d in ds:
            s += int(d)
            out.append(s)
        return out

    c0 = cum_list(delays)
    s_lo = 0.0
    s_hi = float("inf")
    for j, (lo, hi) in constraints.items():
        base = max(1, c0[j])
        s_lo = max(s_lo, float(lo) / float(base))
        s_hi = min(s_hi, float(hi) / float(base))

    s = 1.0
    if s < s_lo:
        s = s_lo
    if s > s_hi:
        s = s_hi
    if not math.isfinite(s) or s <= 0:
        s = 1.0

    scaled = [max(1, int(round(d * s))) for d in delays]

    for _ in range(2):
        cum = 0
        for j in range(n):
            cum += scaled[j]
            if j in constraints:
                lo, _hi = constraints[j]
                if cum < lo:
                    delta = lo - cum
                    scaled[j] += delta
                    cum += delta

        c = cum_list(scaled)
        for j in range(n - 1, -1, -1):
            if j not in constraints:
                continue
            _lo, hi = constraints[j]
            if c[j] <= hi:
                continue
            excess = c[j] - hi
            k = j
            while excess > 0 and k >= 0:
                reducible = scaled[k] - 1
                if reducible > 0:
                    d = min(excess, reducible)
                    scaled[k] -= d
                    for m in range(k, n):
                        c[m] -= d
                    excess -= d
                k -= 1

    c = cum_list(scaled)
    if (n - 1) in constraints:
        lo_f, hi_f = constraints[n - 1]
        if c[-1] < lo_f:
            scaled[-1] += lo_f - c[-1]
        elif c[-1] > hi_f:
            scaled[-1] = max(1, scaled[-1] - (c[-1] - hi_f))

    return scaled


def plan_flow_delays_ms(flow: Dict[str, Any], state: str, seg: Segment, templates: Dict[str, Dict[str, Any]], inst_key: str) -> List[int]:
    pairs = flow["latency_ms"]
    mult = seg.latency_mult.get(flow["id"], {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}

    delays: List[int] = []
    for j, (p50, p95) in enumerate(pairs):
        p50s = float(p50) * float(mult.get("p50", 1.0))
        p95s = float(p95) * float(mult.get("p95", 1.0))
        u = hash_u01(f"{inst_key}:lat:{j}")
        delays.append(sample_lognormal_ms(p50s, p95s, u))

    constraints: Dict[int, Tuple[int, int]] = {}
    for j, ref in enumerate(flow["emit"]):
        tmpl = templates[ref]
        b = _timing_bounds_for_template(tmpl, state)
        if b is not None:
            constraints[j] = b

    delays = _apply_constraints_by_scaling_and_adjustment(delays, constraints)
    return delays


def bind_flow_base_context(flow: Dict[str, Any], state: str, inst_key: str, templates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    ctx["req_id"] = md5_hex(inst_key + ":reqid")[:16]

    refs = flow["emit"]
    phs = set()
    for ref in refs:
        phs.update(get_placeholders(templates[ref]["msg"]))

    if "method" in phs:
        ctx["method"] = sample_domain({"k": "ch", "v": ["GET", "POST"]}, inst_key + ":method")
    if "route" in phs:
        ctx["route"] = gen_str_hint("web_route", inst_key + ":web_route")
    if "client_ip" in phs:
        ctx["client_ip"] = sample_domain({"k": "ip", "v": None}, inst_key + ":client_ip")

    if "repo" in phs:
        ctx["repo"] = gen_str_hint("owner/repo", inst_key + ":repo")
    if "route" in phs and any(ref.startswith("edge_lb.access_git_") for ref in refs):
        repo = ctx.get("repo", gen_str_hint("owner/repo", inst_key + ":repo"))
        suf = GIT_SUFFIXES[hash_i64(inst_key + ":git_suf") % len(GIT_SUFFIXES)]
        ctx["route"] = f"/{repo}{suf}"

    if "path_name" in phs:
        ctx["path_name"] = sample_domain({"k": "ch", "v": ["homepage", "api_status"]}, inst_key + ":path_name")
        if ctx["path_name"] == "homepage":
            ctx["route"] = "/"
        else:
            ctx["route"] = "/api/status"

    # Only bind pack_bytes/bytes_out for *successful* git sessions where the git_service log includes pack_bytes.
    # Do NOT pre-bind bytes_out for generic edge_lb.access_git_* flows, because 503/504/500 have much smaller domains.
    if any(ref.endswith(".clone_complete") or ref.endswith(".clone_slow") for ref in refs):
        pack = int(sample_domain({"k": "i", "v": [10000, 50000000]}, inst_key + ":pack_bytes"))
        ctx["pack_bytes"] = pack
        ctx["bytes_out"] = pack
    elif any(ref.startswith("edge_lb.access_web_") for ref in refs):
        if any(ref.endswith("_200") for ref in refs):
            ctx["bytes_out"] = int(sample_domain({"k": "i", "v": [300, 2000000]}, inst_key + ":web_bytes_out"))
        else:
            if any(ref.endswith("_503") for ref in refs):
                ctx["bytes_out"] = int(sample_domain({"k": "i", "v": [0, 2000]}, inst_key + ":web_bytes_out_fail"))
            else:
                ctx["bytes_out"] = int(sample_domain({"k": "i", "v": [0, 5000]}, inst_key + ":web_bytes_out_fail"))
    return ctx


def emit_log_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str, seq: int) -> None:
    rows.append(
        {
            "_dt": ts,
            "_seq": seq,
            "timestamp": fmt_ts(ts),
            "level": level,
            "message": message,
            "trace_id": trace_id,
            "service": service or "",
            "host": host or "",
        }
    )


def render_log(template: Dict[str, Any], state: str, key: str, provided: Dict[str, Any]) -> str:
    msg = template["msg"]
    placeholders = get_placeholders(msg)
    domains = merge_domains(template, state)
    vals = dict(provided)
    for p in placeholders:
        if p in vals:
            continue
        if p in domains:
            vals[p] = sample_domain(domains[p], key + f":{p}")
        else:
            vals[p] = md5_hex(key + f":{p}")[:8]
    return msg.format(**vals)


def simulate_background_for_segment(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, Dict[str, Any]],
    base_time: datetime,
    seg: Segment,
    seq_start: int,
) -> int:
    seq = seq_start
    seg_start = base_time + timedelta(minutes=seg.start_min)
    seg_end = base_time + timedelta(minutes=seg.end_min)
    duration_min = float(seg.end_min - seg.start_min)

    for cid, comp in comps.items():
        beh = (comp.get("beh") or {}).get(seg.state, {}) or {}
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope") or "per_host"
            source_key = f"{cid}.{log_id}"
            mult = 1.0
            if seg.state == "f":
                mult = float(seg.rate_mult.get(source_key, 1.0))
            eff = per_min * duration_min * mult
            tmpl_ref = f"{cid}.{log_id}"
            tmpl = templates[tmpl_ref]

            if scope == "global" or not (comp.get("hosts") or []):
                cnt = stable_int(eff, f"bg:{seg.state}:{seg.start_min}-{seg.end_min}:{source_key}")
                times = schedule_even_times(seg_start, seg_end, cnt, f"bg:{seg.state}:{seg.start_min}:{source_key}:global")
                host = choose_host(comp, f"bg:{seg.state}:{seg.start_min}:{source_key}:globalhost")
                for i, t in enumerate(times):
                    provided: Dict[str, Any] = {}
                    if tmpl_ref == "fileserver_cluster.ha_heartbeat" and host in ["fs-1", "fs-2"]:
                        provided["peer"] = "fs-2" if host == "fs-1" else "fs-1"
                        provided["role"] = "primary" if host == "fs-1" else "secondary"
                    if tmpl_ref == "fileserver_cluster.ha_peer_disconnected" and host in ["fs-1", "fs-2"]:
                        provided["peer"] = "fs-2" if host == "fs-1" else "fs-1"
                    if tmpl_ref == "aggregation_switch.fdb_miss_flood":
                        provided["dst_mac"] = gen_str_hint("mac", f"{tmpl_ref}:{seg.start_min}:{i}")
                    msg = render_log(tmpl, seg.state, f"bg:{seg.state}:{seg.start_min}:{source_key}:{i}", provided)
                    emit_log_row(rows, t, tmpl["lvl"], msg, "", comp.get("svc") or "", host, seq)
                    seq += 1
            else:
                for h in comp["hosts"]:
                    eff_h = per_min * duration_min * mult
                    cnt = stable_int(eff_h, f"bg:{seg.state}:{seg.start_min}-{seg.end_min}:{source_key}:{h}")
                    times = schedule_even_times(seg_start, seg_end, cnt, f"bg:{seg.state}:{seg.start_min}:{source_key}:{h}")
                    for i, t in enumerate(times):
                        provided = {}
                        if tmpl_ref == "fileserver_cluster.ha_heartbeat" and h in ["fs-1", "fs-2"]:
                            provided["peer"] = "fs-2" if h == "fs-1" else "fs-1"
                            provided["role"] = "primary" if h == "fs-1" else "secondary"
                        if tmpl_ref == "fileserver_cluster.ha_peer_disconnected" and h in ["fs-1", "fs-2"]:
                            provided["peer"] = "fs-2" if h == "fs-1" else "fs-1"
                        if tmpl_ref == "aggregation_switch.fdb_miss_flood":
                            provided["dst_mac"] = gen_str_hint("mac", f"{tmpl_ref}:{seg.start_min}:{h}:{i}")
                        msg = render_log(tmpl, seg.state, f"bg:{seg.state}:{seg.start_min}:{source_key}:{h}:{i}", provided)
                        emit_log_row(rows, t, tmpl["lvl"], msg, "", comp.get("svc") or "", h, seq)
                        seq += 1
    return seq


def simulate_flows_for_segment(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, Dict[str, Any]],
    base_time: datetime,
    seg: Segment,
    seq_start: int,
) -> int:
    seq = seq_start
    seg_start = base_time + timedelta(minutes=seg.start_min)
    seg_end = base_time + timedelta(minutes=seg.end_min)
    duration_min = float(seg.end_min - seg.start_min)

    for flow in get_flow_defs(SYSTEM, seg.state):
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        mult = 1.0
        if seg.state == "f":
            mult = float(seg.rate_mult.get(flow_id, 1.0))
        expected_instances = rpm * duration_min * mult
        n_instances = stable_int(expected_instances, f"flow:{seg.state}:{seg.start_min}-{seg.end_min}:{flow_id}")
        start_times = schedule_even_times(seg_start, seg_end, n_instances, f"flow:{seg.state}:{seg.start_min}:{flow_id}")

        for idx, start_ts in enumerate(start_times):
            inst_key = f"{seg.state}:{flow_id}:{seg.start_min}:{idx}"
            trace_id = flow_trace_id(flow, inst_key)

            # Choose component-local hosts first so we can bind cross-log fields coherently.
            comp_host_map: Dict[str, str] = {}
            for ref in flow["emit"]:
                cid = ref.split(".", 1)[0]
                if cid not in comp_host_map:
                    comp_host_map[cid] = choose_host(comps[cid], inst_key + f":host:{cid}")

            base_ctx = bind_flow_base_context(flow, seg.state, inst_key, templates)

            # Bind storage attribution consistently across the repo_io_error -> clone_failed_storage chain.
            # The fileserver host in this flow instance is the "fileserver" field in clone_failed_storage.
            fs_host = comp_host_map.get("fileserver_cluster", "")
            if fs_host in ("fs-1", "fs-2"):
                base_ctx.setdefault("fileserver", fs_host)

            delays_ms = plan_flow_delays_ms(flow, seg.state, seg, templates, inst_key)
            cum = 0
            for j, ref in enumerate(flow["emit"]):
                tmpl = templates[ref]
                cid = tmpl["component_id"]
                comp = comps[cid]
                host = comp_host_map.get(cid, choose_host(comp, inst_key + f":host:{cid}"))

                cum += delays_ms[j]
                ts = start_ts + timedelta(milliseconds=cum)

                provided = dict(base_ctx)

                msg_placeholders = get_placeholders(tmpl["msg"])
                if "duration_ms" in msg_placeholders:
                    provided["duration_ms"] = int(cum)
                if "latency_ms" in msg_placeholders:
                    provided["latency_ms"] = int(cum)

                if ref == "git_service.clone_failed_storage":
                    # Ensure clone_failed_storage.fileserver matches the fileserver involved in this chain.
                    if "fileserver" not in provided:
                        if fs_host in ("fs-1", "fs-2"):
                            provided["fileserver"] = fs_host

                # Ensure success access_git_200 bytes_out follows pack_bytes when available; otherwise let template domain sampling handle it.
                if ref.startswith("edge_lb.access_git_") and "bytes_out" in msg_placeholders and "pack_bytes" in provided:
                    provided["bytes_out"] = int(provided["pack_bytes"])

                msg = render_log(tmpl, seg.state, f"{inst_key}:log:{ref}:{j}", provided)
                emit_log_row(rows, ts, tmpl["lvl"], msg, trace_id, comp.get("svc") or "", host, seq)
                seq += 1

    return seq


def simulate_one_shots(rows: List[Dict[str, Any]], comps: Dict[str, Any], templates: Dict[str, Dict[str, Any]], base_time: datetime, seq_start: int) -> int:
    seq = seq_start
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        ones = e.get("one_shots") or []
        for os_idx, os in enumerate(ones):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = list(os.get("hosts") or [])
            tmpl = templates[ref]
            cid = tmpl["component_id"]
            comp = comps[cid]

            minute_start = base_time + timedelta(minutes=at_min)
            minute_end = minute_start + timedelta(minutes=1)
            times = schedule_even_times(minute_start, minute_end, count, f"oneshot:{at_min}:{ref}:{os_idx}")

            for i, t in enumerate(times):
                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]
                else:
                    host = choose_host(comp, f"oneshot:{at_min}:{ref}:{os_idx}:{i}:host")
                provided: Dict[str, Any] = {}

                if ref == "fileserver_cluster.node_fenced":
                    # Bind node to the emitting host to avoid contradictions (e.g., host=fs-1 but node=fs-2).
                    provided["node"] = host

                if "target" in get_placeholders(tmpl["msg"]):
                    if host in ["asw-7a", "asw-7b", "aggsw-1", "aggsw-2"]:
                        provided["target"] = host

                msg = render_log(tmpl, "f", f"oneshot:{at_min}:{ref}:{os_idx}:{i}", provided)
                emit_log_row(rows, t, tmpl["lvl"], msg, "", comp.get("svc") or "", host, seq)
                seq += 1
    return seq


def main() -> None:
    random.seed(0)
    np.random.seed(0)

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    comps, templates = build_indices(SYSTEM)
    segments = build_segments(SCENARIO)

    rows: List[Dict[str, Any]] = []
    seq = 0

    for seg in segments:
        seq = simulate_background_for_segment(rows, comps, templates, base_time, seg, seq)
        seq = simulate_flows_for_segment(rows, comps, templates, base_time, seg, seq)

    seq = simulate_one_shots(rows, comps, templates, base_time, seq)

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["_dt", "_seq"], ascending=[True, True], kind="mergesort").reset_index(drop=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} outside required range [20000, 100000].")

    bad = df[(df["trace_id"] != "") & (~df["trace_id"].str.match(r"^[0-9a-f]{32}$"))]
    if len(bad) > 0:
        raise RuntimeError("Invalid trace_id format found.")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
