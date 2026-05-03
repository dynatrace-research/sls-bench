import math
import re
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "elastic_cloud_useast1_2019"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["proxy_router", "control_plane_api", "kibana_instance"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "lb_edge",
            "svc": "aws-alb",
            "hosts": ["alb-1"],
            "logs": {
                "lb_metrics": {
                    "lvl": "INFO",
                    "msg": "lb_metrics mode={mode} region={region} req_rps={req_rps} http_5xx_rate={http_5xx_rate} target_reset_rate={target_reset_rate} p95_ms={p95_ms}",
                    "vars": {"region": {"k": "ch", "v": ["us-east-1"]}},
                    "state_vars": {
                        "n": {
                            "mode": {"k": "ch", "v": ["normal"]},
                            "req_rps": {"k": "i", "v": [3, 14]},
                            "http_5xx_rate": {"k": "f", "v": [0.0, 0.02]},
                            "target_reset_rate": {"k": "f", "v": [0.0, 0.01]},
                            "p95_ms": {"k": "i", "v": [20, 250]},
                        },
                        "f": {
                            "mode": {"k": "ch", "v": ["degraded"]},
                            "req_rps": {"k": "i", "v": [0, 35]},
                            "http_5xx_rate": {"k": "f", "v": [0.05, 0.6]},
                            "target_reset_rate": {"k": "f", "v": [0.0, 0.3]},
                            "p95_ms": {"k": "i", "v": [120, 3000]},
                        },
                    },
                },
                "lb_metrics_recovered": {
                    "lvl": "INFO",
                    "msg": "lb_metrics mode=recovered region={region} req_rps={req_rps} http_5xx_rate={http_5xx_rate} target_reset_rate={target_reset_rate} p95_ms={p95_ms}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-east-1"]},
                        "req_rps": {"k": "i", "v": [6, 35]},
                        "http_5xx_rate": {"k": "f", "v": [0.0, 0.03]},
                        "target_reset_rate": {"k": "f", "v": [0.0, 0.02]},
                        "p95_ms": {"k": "i", "v": [20, 400]},
                    },
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "lb_metrics", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "lb_metrics", "per_min": 1.0, "scope": "global"}, {"id": "lb_metrics_recovered", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "zk_ensemble",
            "svc": "zookeeper",
            "hosts": ["zk-1", "zk-2", "zk-3"],
            "logs": {
                "zk_health": {
                    "lvl": "INFO",
                    "msg": "zk_health node={node} role={role} state={state} outstanding={outstanding} cpu_pct={cpu_pct} client_conns={client_conns}",
                    "vars": {"node": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "role": {"k": "ch", "v": ["leader", "follower"]}},
                    "state_vars": {
                        "n": {"state": {"k": "ch", "v": ["ok"]}, "outstanding": {"k": "i", "v": [0, 300]}, "cpu_pct": {"k": "i", "v": [5, 40]}, "client_conns": {"k": "i", "v": [200, 1200]}},
                        "f": {"state": {"k": "ch", "v": ["degraded"]}, "outstanding": {"k": "i", "v": [500, 5000]}, "cpu_pct": {"k": "i", "v": [40, 100]}, "client_conns": {"k": "i", "v": [1000, 5000]}},
                    },
                },
                "zk_client_conn_fail": {
                    "lvl": "WARN",
                    "msg": "client_connection_failed node={node} client={client} reason={reason}",
                    "vars": {"node": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "client": {"k": "ch", "v": ["proxy", "control_plane", "allocator_tls"]}, "reason": {"k": "ch", "v": ["session_expired", "connection_refused", "read_timeout"]}},
                    "state_vars": {},
                },
                "zk_quorum_lost": {
                    "lvl": "ERROR",
                    "msg": "quorum_lost term={term} connected_peers={connected_peers} needed={needed}",
                    "vars": {"term": {"k": "i", "v": [1000, 2000]}, "connected_peers": {"k": "i", "v": [0, 1]}, "needed": {"k": "i", "v": [2, 2]}},
                    "state_vars": {},
                },
                "zk_leader_election": {
                    "lvl": "INFO",
                    "msg": "leader_election term={term} candidate={candidate} prev_leader={prev_leader}",
                    "vars": {"term": {"k": "i", "v": [1000, 2000]}, "candidate": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "prev_leader": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}},
                    "state_vars": {},
                },
                "zk_leader_shutdown": {
                    "lvl": "ERROR",
                    "msg": "leader_shutdown node={node} reason=overwhelmed open_conns={open_conns} outstanding={outstanding}",
                    "vars": {"node": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "open_conns": {"k": "i", "v": [2000, 8000]}, "outstanding": {"k": "i", "v": [3000, 15000]}},
                    "state_vars": {},
                },
                "zk_quorum_established": {
                    "lvl": "INFO",
                    "msg": "quorum_established term={term} leader={leader} followers={followers}",
                    "vars": {"term": {"k": "i", "v": [1000, 2000]}, "leader": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "followers": {"k": "i", "v": [1, 2]}},
                    "state_vars": {},
                },
                "kernel_softlock": {
                    "lvl": "CRITICAL",
                    "msg": "kernel_softlock host={host} task={task} duration_s={duration_s}",
                    "vars": {"host": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "task": {"k": "ch", "v": ["runc", "containerd"]}, "duration_s": {"k": "i", "v": [60, 600]}},
                    "state_vars": {},
                },
                "host_reboot": {
                    "lvl": "WARN",
                    "msg": "host_reboot initiated host={host} reason={reason}",
                    "vars": {"host": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}, "reason": {"k": "ch", "v": ["softlock", "unresponsive"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "zk_health", "per_min": 1.0}]},
                "f": {"emit": [{"id": "zk_health", "per_min": 2.0}, {"id": "zk_client_conn_fail", "per_min": 3.0}, {"id": "zk_quorum_lost", "per_min": 0.25, "scope": "global"}]},
            },
        },
        {
            "id": "zk_observer",
            "svc": "zookeeper-observer",
            "hosts": ["zkobs-1", "zkobs-2"],
            "logs": {
                "observer_health": {
                    "lvl": "INFO",
                    "msg": "observer_health node={node} connected={connected} sync_lag_ms={sync_lag_ms} init_limit={init_limit}",
                    "vars": {"node": {"k": "ch", "v": ["zkobs-1", "zkobs-2"]}},
                    "state_vars": {
                        "n": {"connected": {"k": "ch", "v": ["true"]}, "sync_lag_ms": {"k": "i", "v": [0, 200]}, "init_limit": {"k": "i", "v": [5, 8]}},
                        "f": {"connected": {"k": "ch", "v": ["true", "false"]}, "sync_lag_ms": {"k": "i", "v": [200, 5000]}, "init_limit": {"k": "i", "v": [5, 20]}},
                    },
                },
                "observer_sync_timeout": {
                    "lvl": "WARN",
                    "msg": "observer_sync_timeout node={node} waited_ms={waited_ms} leader={leader}",
                    "vars": {"node": {"k": "ch", "v": ["zkobs-1", "zkobs-2"]}, "waited_ms": {"k": "i", "v": [500, 10000]}, "leader": {"k": "ch", "v": ["zk-1", "zk-2", "zk-3"]}},
                    "state_vars": {},
                },
                "initlimit_changed": {
                    "lvl": "INFO",
                    "msg": "config_changed node={node} key=initLimit old={old} new={new}",
                    "vars": {"node": {"k": "ch", "v": ["zkobs-1", "zkobs-2"]}, "old": {"k": "i", "v": [5, 8]}, "new": {"k": "i", "v": [10, 20]}},
                    "state_vars": {},
                },
            },
            "beh": {"n": {"emit": [{"id": "observer_health", "per_min": 1.0}]}, "f": {"emit": [{"id": "observer_health", "per_min": 1.2}, {"id": "observer_sync_timeout", "per_min": 0.8}]}},
        },
        {
            "id": "proxy_router",
            "svc": "cloud-proxy",
            "hosts": ["proxy-1", "proxy-2", "proxy-3"],
            "logs": {
                "proxy_es_access_ok": {
                    "lvl": "INFO",
                    "msg": "proxy_access service=es vhost={vhost} status=200 attempt={attempt} dur_ms={dur_ms} route_src={route_src} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "dur_ms": {"k": "i", "v": [5, 3000]}, "route_src": {"k": "ch", "v": ["cache", "observer"]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 1]}}},
                },
                "proxy_es_access_err": {
                    "lvl": "WARN",
                    "msg": "proxy_access service=es vhost={vhost} status={status} attempt={attempt} err={err} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "status": {"k": "ch", "v": ["502", "503", "504"]}, "err": {"k": "ch", "v": ["route_unavailable", "upstream_timeout", "connect_reset", "observer_unstable", "upstream_503"]}, "dur_ms": {"k": "i", "v": [20, 10000]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 3]}}},
                },
                "proxy_kb_access_ok": {
                    "lvl": "INFO",
                    "msg": "proxy_access service=kibana vhost={vhost} status=200 attempt={attempt} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "dur_ms": {"k": "i", "v": [10, 8000]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 1]}}},
                },
                "proxy_kb_access_err_route": {
                    "lvl": "WARN",
                    "msg": "proxy_access service=kibana vhost={vhost} status={status} attempt={attempt} err={err} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "status": {"k": "ch", "v": ["502", "503", "504"]}, "err": {"k": "ch", "v": ["route_unavailable", "observer_unstable", "connect_reset"]}, "dur_ms": {"k": "i", "v": [20, 15000]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 1]}}},
                },
                "proxy_kb_access_err_upstream": {
                    "lvl": "WARN",
                    "msg": "proxy_access service=kibana vhost={vhost} status={status} attempt={attempt} err={err} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "status": {"k": "ch", "v": ["502", "503", "504"]}, "err": {"k": "ch", "v": ["upstream_timeout", "upstream_503", "kibana_unhealthy", "connect_reset"]}, "dur_ms": {"k": "i", "v": [20, 15000]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 1]}}},
                },
                "route_lookup_retry": {
                    "lvl": "INFO",
                    "msg": "route_lookup_retry vhost={vhost} attempt={attempt} reason={reason} backoff_ms={backoff_ms} trace={trace_id}",
                    "vars": {"vhost": {"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, "attempt": {"k": "i", "v": [2, 3]}, "reason": {"k": "ch", "v": ["observer_disconnect", "sync_timeout", "session_expired"]}, "backoff_ms": {"k": "i", "v": [30, 700]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {},
                },
                "zk_session_reset": {
                    "lvl": "WARN",
                    "msg": "zk_session_reset proxy={proxy} observer={observer} reason={reason}",
                    "vars": {"proxy": {"k": "ch", "v": ["proxy-1", "proxy-2", "proxy-3"]}, "observer": {"k": "ch", "v": ["zkobs-1", "zkobs-2"]}, "reason": {"k": "ch", "v": ["session_expired", "connection_loss"]}},
                    "state_vars": {},
                },
                "proxy_cpu": {
                    "lvl": "INFO",
                    "msg": "proxy_cpu proxy={proxy} cpu_pct={cpu_pct} active_conns={active_conns}",
                    "vars": {"proxy": {"k": "ch", "v": ["proxy-1", "proxy-2", "proxy-3"]}},
                    "state_vars": {"n": {"cpu_pct": {"k": "i", "v": [5, 40]}, "active_conns": {"k": "i", "v": [100, 4000]}}, "f": {"cpu_pct": {"k": "i", "v": [30, 100]}, "active_conns": {"k": "i", "v": [2000, 20000]}}},
                },
                "autoscale_add": {"lvl": "INFO", "msg": "autoscale action=add_instances count={count} new_total={new_total}", "vars": {"count": {"k": "i", "v": [1, 5]}, "new_total": {"k": "i", "v": [3, 12]}}, "state_vars": {}},
                "traffic_shed_enabled": {"lvl": "WARN", "msg": "traffic_shed enabled reason={reason}", "vars": {"reason": {"k": "ch", "v": ["coordination_unstable"]}}, "state_vars": {}},
                "traffic_shed_disabled": {"lvl": "INFO", "msg": "traffic_shed disabled", "vars": {}, "state_vars": {}},
            },
            "beh": {"n": {"emit": [{"id": "proxy_cpu", "per_min": 0.6}, {"id": "zk_session_reset", "per_min": 0.02}]}, "f": {"emit": [{"id": "proxy_cpu", "per_min": 1.0}, {"id": "zk_session_reset", "per_min": 1.0}]}},
        },
        {
            "id": "control_plane_api",
            "svc": "cloud-control-plane",
            "hosts": ["cp-1", "cp-2"],
            "logs": {
                "console_access_ok": {
                    "lvl": "INFO",
                    "msg": "console_api op=list_deployments status=200 attempt={attempt} dur_ms={dur_ms} zk_reads={zk_reads} trace={trace_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [30, 4000]}, "zk_reads": {"k": "i", "v": [1, 5]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 1]}}},
                },
                "console_access_err": {
                    "lvl": "WARN",
                    "msg": "console_api op=list_deployments status={status} attempt={attempt} err={err} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"status": {"k": "ch", "v": ["503", "504"]}, "err": {"k": "ch", "v": ["zk_timeout", "zk_unavailable"]}, "dur_ms": {"k": "i", "v": [500, 15000]}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {"n": {"attempt": {"k": "i", "v": [1, 1]}}, "f": {"attempt": {"k": "i", "v": [1, 2]}}},
                },
                "state_read_retry": {"lvl": "INFO", "msg": "zk_read_retry op=list_deployments attempt={attempt} backoff_ms={backoff_ms} trace={trace_id}", "vars": {"attempt": {"k": "i", "v": [2, 2]}, "backoff_ms": {"k": "i", "v": [50, 500]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "api_health": {
                    "lvl": "INFO",
                    "msg": "api_health state={state} inflight={inflight} p95_ms={p95_ms}",
                    "vars": {},
                    "state_vars": {"n": {"state": {"k": "ch", "v": ["ok"]}, "inflight": {"k": "i", "v": [0, 120]}, "p95_ms": {"k": "i", "v": [20, 300]}}, "f": {"state": {"k": "ch", "v": ["degraded"]}, "inflight": {"k": "i", "v": [100, 500]}, "p95_ms": {"k": "i", "v": [300, 5000]}}},
                },
            },
            "beh": {"n": {"emit": [{"id": "api_health", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "api_health", "per_min": 1.2, "scope": "global"}]}},
        },
        {
            "id": "kibana_instance",
            "svc": "kibana",
            "hosts": ["kb-1", "kb-2", "kb-3", "kb-4"],
            "logs": {
                "kibana_ui_ok": {"lvl": "INFO", "msg": "kibana_ui status=200 path={path} dur_ms={dur_ms} trace={trace_id}", "vars": {"path": {"k": "ch", "v": ["/app/kibana", "/api/status"]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {"n": {"dur_ms": {"k": "i", "v": [10, 300]}}, "f": {"dur_ms": {"k": "i", "v": [50, 8000]}}}},
                "kibana_ui_err": {"lvl": "WARN", "msg": "kibana_ui status={status} path={path} err={err} dur_ms={dur_ms} trace={trace_id}", "vars": {"status": {"k": "ch", "v": ["503", "504"]}, "path": {"k": "ch", "v": ["/app/kibana", "/api/status"]}, "err": {"k": "ch", "v": ["es_unreachable", "timeout"]}, "dur_ms": {"k": "i", "v": [100, 15000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "es_call_start": {"lvl": "DEBUG", "msg": "es_call_start target=elasticsearch endpoint={endpoint} trace={trace_id}", "vars": {"endpoint": {"k": "ch", "v": ["/_cluster/health", "/_search"]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "es_call_ok": {"lvl": "INFO", "msg": "es_call_done outcome=ok dur_ms={dur_ms} trace={trace_id}", "vars": {"dur_ms": {"k": "i", "v": [20, 3000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "es_call_err": {"lvl": "WARN", "msg": "es_call_done outcome=error err={err} dur_ms={dur_ms} trace={trace_id}", "vars": {"err": {"k": "ch", "v": ["proxy_unavailable", "timeout"]}, "dur_ms": {"k": "i", "v": [200, 15000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "es_call_err_conntrack": {"lvl": "WARN", "msg": "es_call_done outcome=error err={err} dur_ms={dur_ms} trace={trace_id}", "vars": {"err": {"k": "ch", "v": ["conntrack_full", "socket_leak"]}, "dur_ms": {"k": "i", "v": [500, 15000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "es_call_retry": {"lvl": "INFO", "msg": "es_call_retry attempt={attempt} backoff_ms={backoff_ms} trace={trace_id}", "vars": {"attempt": {"k": "i", "v": [2, 2]}, "backoff_ms": {"k": "i", "v": [100, 1000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "kibana_health": {"lvl": "INFO", "msg": "kibana_health state={state} event_loop_lag_ms={lag} heap_pct={heap_pct}", "vars": {}, "state_vars": {"n": {"state": {"k": "ch", "v": ["ok"]}, "lag": {"k": "i", "v": [0, 80]}, "heap_pct": {"k": "i", "v": [10, 60]}}, "f": {"state": {"k": "ch", "v": ["degraded"]}, "lag": {"k": "i", "v": [100, 2000]}, "heap_pct": {"k": "i", "v": [40, 95]}}}},
                "open_sockets_metric": {"lvl": "INFO", "msg": "kibana_sockets open={open} fd_limit={fd_limit} conntrack_used_pct={conntrack_used_pct}", "vars": {"fd_limit": {"k": "i", "v": [65535, 65535]}}, "state_vars": {"n": {"open": {"k": "i", "v": [50, 2000]}, "conntrack_used_pct": {"k": "i", "v": [1, 40]}}, "f": {"open": {"k": "i", "v": [100, 8000]}, "conntrack_used_pct": {"k": "i", "v": [1, 60]}}}},
                "open_sockets_pressure": {"lvl": "WARN", "msg": "kibana_sockets_pressure open={open} fd_limit={fd_limit} conntrack_used_pct={conntrack_used_pct}", "vars": {"open": {"k": "i", "v": [8000, 50000]}, "fd_limit": {"k": "i", "v": [65535, 65535]}, "conntrack_used_pct": {"k": "i", "v": [60, 100]}}, "state_vars": {}},
            },
            "beh": {"n": {"emit": [{"id": "kibana_health", "per_min": 0.5}, {"id": "open_sockets_metric", "per_min": 0.2}]}, "f": {"emit": [{"id": "kibana_health", "per_min": 0.8}, {"id": "open_sockets_metric", "per_min": 0.3}, {"id": "open_sockets_pressure", "per_min": 0.8}]}},
        },
        {
            "id": "tls_proxy",
            "svc": "allocator-tls-proxy",
            "hosts": ["kb-1", "kb-2", "kb-3", "kb-4"],
            "logs": {
                "forward_ok": {"lvl": "INFO", "msg": "tls_proxy_forward outcome=ok upstream=proxy dur_ms={dur_ms} trace={trace_id}", "vars": {"dur_ms": {"k": "i", "v": [5, 2000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "forward_err": {"lvl": "WARN", "msg": "tls_proxy_forward outcome=error err={err} dur_ms={dur_ms} trace={trace_id}", "vars": {"err": {"k": "ch", "v": ["proxy_connect_failed", "upstream_timeout"]}, "dur_ms": {"k": "i", "v": [50, 15000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "forward_err_conntrack": {"lvl": "WARN", "msg": "tls_proxy_forward outcome=error err={err} dur_ms={dur_ms} trace={trace_id}", "vars": {"err": {"k": "ch", "v": ["conntrack_full"]}, "dur_ms": {"k": "i", "v": [200, 15000]}, "trace_id": {"k": "hex", "v": 32}}, "state_vars": {}},
                "conntrack_metric": {"lvl": "INFO", "msg": "tls_proxy_conntrack open_conns={open_conns} nf_conntrack_used_pct={used_pct} cpu_pct={cpu_pct}", "vars": {}, "state_vars": {"n": {"open_conns": {"k": "i", "v": [100, 3000]}, "used_pct": {"k": "i", "v": [1, 40]}, "cpu_pct": {"k": "i", "v": [5, 40]}}, "f": {"open_conns": {"k": "i", "v": [200, 10000]}, "used_pct": {"k": "i", "v": [1, 60]}, "cpu_pct": {"k": "i", "v": [10, 80]}}}},
                "conntrack_pressure": {"lvl": "WARN", "msg": "tls_proxy_conntrack_pressure open_conns={open_conns} nf_conntrack_used_pct={used_pct} cpu_pct={cpu_pct}", "vars": {"open_conns": {"k": "i", "v": [10000, 60000]}, "used_pct": {"k": "i", "v": [60, 100]}, "cpu_pct": {"k": "i", "v": [50, 100]}}, "state_vars": {}},
            },
            "beh": {"n": {"emit": [{"id": "conntrack_metric", "per_min": 0.3}]}, "f": {"emit": [{"id": "conntrack_metric", "per_min": 0.4}, {"id": "conntrack_pressure", "per_min": 0.8}]}},
        },
    ],
    "flows": {
        "n": [
            {"id": "es_search_ok", "rpm": 250.0, "emit": ["proxy_router.proxy_es_access_ok"], "latency_ms": [[25, 90]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "kibana_ui_ok", "rpm": 80.0, "emit": ["kibana_instance.kibana_ui_ok", "proxy_router.proxy_kb_access_ok"], "latency_ms": [[30, 140], [20, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "console_list_deployments_ok", "rpm": 20.0, "emit": ["control_plane_api.console_access_ok"], "latency_ms": [[60, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "kibana_es_call_ok", "rpm": 80.0, "emit": ["kibana_instance.es_call_start", "tls_proxy.forward_ok", "kibana_instance.es_call_ok"], "latency_ms": [[1, 5], [15, 80], [25, 140]], "retry": {"max_attempts": 2, "expected_attempts": 1.2, "emit_per_retry": ["kibana_instance.es_call_retry"], "backoff_ms": [[150, 800]]}, "trace": True},
        ],
        "f": [
            {"id": "es_search_503_route_unavailable", "rpm": 220.0, "emit": ["proxy_router.proxy_es_access_err"], "latency_ms": [[60, 700]], "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["proxy_router.route_lookup_retry"], "backoff_ms": [[80, 350], [120, 650]]}, "trace": True},
            {"id": "es_search_200_after_recovery", "rpm": 250.0, "emit": ["proxy_router.proxy_es_access_ok"], "latency_ms": [[35, 180]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "console_list_deployments_timeout", "rpm": 18.0, "emit": ["control_plane_api.console_access_err"], "latency_ms": [[1200, 9000]], "retry": {"max_attempts": 2, "expected_attempts": 1.3, "emit_per_retry": ["control_plane_api.state_read_retry"], "backoff_ms": [[120, 600]]}, "trace": True},
            {"id": "console_list_deployments_ok_after_restore", "rpm": 20.0, "emit": ["control_plane_api.console_access_ok"], "latency_ms": [[180, 900]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "kibana_ui_proxy_fail", "rpm": 80.0, "emit": ["proxy_router.proxy_kb_access_err_route"], "latency_ms": [[80, 900]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "kibana_ui_503_kibana_unhealthy", "rpm": 50.0, "emit": ["kibana_instance.kibana_ui_err", "proxy_router.proxy_kb_access_err_upstream"], "latency_ms": [[300, 4000], [50, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "kibana_es_call_fail_proxy", "rpm": 80.0, "emit": ["kibana_instance.es_call_start", "tls_proxy.forward_err", "kibana_instance.es_call_err"], "latency_ms": [[1, 10], [150, 2500], [300, 7000]], "retry": {"max_attempts": 2, "expected_attempts": 1.2, "emit_per_retry": ["kibana_instance.es_call_retry"], "backoff_ms": [[200, 900]]}, "trace": True},
            {"id": "kibana_es_call_fail_conntrack", "rpm": 80.0, "emit": ["kibana_instance.es_call_start", "tls_proxy.forward_err_conntrack", "kibana_instance.es_call_err_conntrack"], "latency_ms": [[1, 10], [200, 6000], [500, 12000]], "retry": {"max_attempts": 2, "expected_attempts": 1.2, "emit_per_retry": ["kibana_instance.es_call_retry"], "backoff_ms": [[250, 1000]]}, "trace": True},
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "incident_20190204_useast1_coordination_proxy_outage",
        "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 55}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "es_search_200_after_recovery": 0.0,
                            "console_list_deployments_ok_after_restore": 0.0,
                            "kibana_ui_503_kibana_unhealthy": 0.0,
                            "kibana_es_call_fail_conntrack": 0.0,
                            "zk_ensemble.zk_client_conn_fail": 1.4,
                            "zk_ensemble.zk_quorum_lost": 1.6,
                            "proxy_router.zk_session_reset": 1.5,
                            "zk_observer.observer_sync_timeout": 1.3,
                            "kibana_instance.open_sockets_pressure": 0.0,
                            "tls_proxy.conntrack_pressure": 0.0,
                            "lb_edge.lb_metrics_recovered": 0.0,
                        },
                        "latency_multipliers": {"es_search_503_route_unavailable": {"p50": 1.2, "p95": 1.6}, "console_list_deployments_timeout": {"p50": 1.1, "p95": 1.3}},
                        "one_shots": [{"ref": "zk_ensemble.zk_leader_election", "count": 1, "hosts": ["zk-1"]}, {"ref": "zk_ensemble.zk_leader_shutdown", "count": 1, "hosts": ["zk-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 33,
                        "rate_multipliers": {"zk_ensemble.zk_client_conn_fail": 1.8, "zk_ensemble.zk_quorum_lost": 2.2, "proxy_router.zk_session_reset": 1.7, "zk_observer.observer_sync_timeout": 1.6},
                        "latency_multipliers": {"es_search_503_route_unavailable": {"p50": 1.3, "p95": 2.2}},
                        "one_shots": [{"ref": "zk_ensemble.kernel_softlock", "count": 2, "hosts": ["zk-1", "zk-2"]}, {"ref": "zk_ensemble.host_reboot", "count": 2, "hosts": ["zk-1", "zk-2"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 40,
                        "rate_multipliers": {
                            "es_search_503_route_unavailable": 0.7,
                            "console_list_deployments_timeout": 0.5,
                            "kibana_ui_proxy_fail": 0.7,
                            "kibana_es_call_fail_proxy": 0.7,
                            "zk_ensemble.zk_client_conn_fail": 0.3,
                            "zk_ensemble.zk_quorum_lost": 0.1,
                            "proxy_router.zk_session_reset": 0.4,
                            "zk_observer.observer_sync_timeout": 0.6,
                        },
                        "latency_multipliers": {"es_search_503_route_unavailable": {"p50": 1.0, "p95": 1.3}},
                        "one_shots": [{"ref": "proxy_router.traffic_shed_enabled", "count": 1, "hosts": ["proxy-1"]}, {"ref": "zk_ensemble.zk_quorum_established", "count": 1, "hosts": ["zk-3"]}],
                    },
                    {
                        "order": 4,
                        "at_min": 44,
                        "rate_multipliers": {
                            "es_search_503_route_unavailable": 1.3,
                            "kibana_ui_proxy_fail": 1.3,
                            "console_list_deployments_ok_after_restore": 1.0,
                            "console_list_deployments_timeout": 0.05,
                            "zk_observer.observer_sync_timeout": 2.5,
                            "proxy_router.proxy_cpu": 1.7,
                        },
                        "latency_multipliers": {"es_search_503_route_unavailable": {"p50": 1.2, "p95": 2.0}, "console_list_deployments_ok_after_restore": {"p50": 1.0, "p95": 1.2}},
                        "one_shots": [{"ref": "proxy_router.traffic_shed_disabled", "count": 1, "hosts": ["proxy-1"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 49,
                        "rate_multipliers": {
                            "es_search_503_route_unavailable": 0.05,
                            "es_search_200_after_recovery": 3.0,
                            "kibana_ui_proxy_fail": 0.05,
                            "kibana_es_call_fail_proxy": 0.1,
                            "console_list_deployments_timeout": 0.0,
                            "console_list_deployments_ok_after_restore": 1.0,
                            "zk_observer.observer_sync_timeout": 0.2,
                            "proxy_router.proxy_cpu": 0.8,
                            "lb_edge.lb_metrics": 0.0,
                            "lb_edge.lb_metrics_recovered": 1.0,
                        },
                        "latency_multipliers": {"es_search_200_after_recovery": {"p50": 1.0, "p95": 1.2}},
                        "one_shots": [{"ref": "zk_observer.initlimit_changed", "count": 1, "hosts": ["zkobs-1"]}, {"ref": "proxy_router.autoscale_add", "count": 1, "hosts": ["proxy-1"]}],
                    },
                    {
                        "order": 6,
                        "at_min": 52,
                        "rate_multipliers": {
                            "kibana_es_call_fail_proxy": 0.0,
                            "kibana_ui_proxy_fail": 0.0,
                            "kibana_es_call_fail_conntrack": 1.0,
                            "kibana_ui_503_kibana_unhealthy": 1.0,
                            "kibana_instance.open_sockets_pressure": 2.5,
                            "tls_proxy.conntrack_pressure": 2.5,
                            "kibana_instance.kibana_health": 1.5,
                        },
                        "latency_multipliers": {"kibana_es_call_fail_conntrack": {"p50": 1.2, "p95": 1.5}, "kibana_ui_503_kibana_unhealthy": {"p50": 1.1, "p95": 1.4}},
                        "one_shots": [],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Deterministic utilities
# -----------------------------

SEED = "incident_20190204_useast1_coordination_proxy_outage|v3|deterministic"
_seed_int = int(hashlib.md5(SEED.encode("utf-8")).hexdigest()[:8], 16)
random.seed(_seed_int)
np.random.seed(_seed_int)


def _md5_bytes(s: str) -> bytes:
    return hashlib.md5((SEED + "|" + s).encode("utf-8")).digest()


def hash_u01(s: str) -> float:
    b = _md5_bytes(s)
    x = int.from_bytes(b[:8], "big", signed=False)
    return (x % (10**15)) / float(10**15)


def hash_int(s: str, lo: int, hi: int) -> int:
    if hi < lo:
        lo, hi = hi, lo
    if lo == hi:
        return lo
    u = hash_u01(s)
    return lo + int(math.floor(u * (hi - lo + 1)))


def hash_choice(s: str, choices: List[Any]) -> Any:
    if not choices:
        return None
    idx = int(math.floor(hash_u01(s) * len(choices)))
    if idx >= len(choices):
        idx = len(choices) - 1
    return choices[idx]


def hash_hex(s: str, n: int) -> str:
    h = hashlib.md5((SEED + "|" + s).encode("utf-8")).hexdigest()
    if n <= len(h):
        return h[:n]
    reps = (n + len(h) - 1) // len(h)
    return (h * reps)[:n]


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def inv_norm_cdf(p: float) -> float:
    p = float(p)
    p = min(max(p, 1e-12), 1.0 - 1e-12)

    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1.0 - plow

    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den
    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def sample_lognormal_ms(
    p50_ms: float,
    p95_ms: float,
    key: str,
    cap_ms: Optional[float] = None,
    min_ms: Optional[int] = None,
    max_ms: Optional[int] = None,
) -> int:
    p50 = max(0.001, float(p50_ms))
    p95 = max(p50 * 1.001, float(p95_ms))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    sigma = max(0.001, sigma)

    u = 0.55 + 0.40 * hash_u01(key)  # (0.55..0.95)
    z = inv_norm_cdf(u)
    x = float(math.exp(mu + sigma * z))

    soft_cap = 3.0 * p95
    if cap_ms is not None:
        soft_cap = min(soft_cap, float(cap_ms))
    x = min(x, soft_cap)

    if min_ms is not None:
        x = max(x, float(min_ms))
    if max_ms is not None:
        x = min(x, float(max_ms))

    ms = int(round(x))
    if min_ms is not None:
        ms = max(ms, int(min_ms))
    if max_ms is not None:
        ms = min(ms, int(max_ms))
    return max(0, ms)


PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def extract_placeholders(msg: str) -> List[str]:
    return list(dict.fromkeys(PLACEHOLDER_RE.findall(msg)))


# -----------------------------
# Indices and control state
# -----------------------------

COMP: Dict[str, Any] = {c["id"]: c for c in SYSTEM["components"]}
LOGDEF: Dict[str, Any] = {}
for cid, c in COMP.items():
    for lid, ldef in c["logs"].items():
        LOGDEF[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **ldef}

FLOWS: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ("n", "f"):
    for f in SYSTEM["flows"][st]:
        FLOWS[st][f["id"]] = f

FAIL_EVENTS: List[Dict[str, Any]] = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    idx: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Dict[str, float]]


def build_intervals() -> List[Interval]:
    n0 = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
    n1 = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
    f0 = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
    f1 = SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"]

    intervals: List[Interval] = []
    intervals.append(Interval(state="n", start_min=n0, end_min=n1, idx=0, flow_rate_mult={}, bg_rate_mult={}, flow_latency_mult={}))

    boundaries = [f0] + [e["at_min"] for e in FAIL_EVENTS if e["at_min"] != f0] + [f1]
    boundaries = sorted(boundaries)

    flow_rate_mult: Dict[str, float] = {}
    bg_rate_mult: Dict[str, float] = {}
    flow_latency_mult: Dict[str, Dict[str, float]] = {}

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in FAIL_EVENTS:
        events_by_min.setdefault(int(e["at_min"]), []).append(e)

    f_idx = 0
    for i in range(len(boundaries) - 1):
        start = int(boundaries[i])
        end = int(boundaries[i + 1])

        for ev in events_by_min.get(start, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                if "." in k:
                    bg_rate_mult[k] = float(v)
                else:
                    flow_rate_mult[k] = float(v)
            for fid, lm in ev.get("latency_multipliers", {}).items():
                flow_latency_mult[fid] = {"p50": float(lm["p50"]), "p95": float(lm["p95"])}

        intervals.append(
            Interval(
                state="f",
                start_min=start,
                end_min=end,
                idx=f_idx,
                flow_rate_mult=dict(flow_rate_mult),
                bg_rate_mult=dict(bg_rate_mult),
                flow_latency_mult=dict(flow_latency_mult),
            )
        )
        f_idx += 1

    return intervals


INTERVALS = build_intervals()

ONE_SHOTS_BY_MIN: Dict[int, List[Dict[str, Any]]] = {}
for ev in FAIL_EVENTS:
    at = int(ev["at_min"])
    for ospec in ev.get("one_shots", []) or []:
        ONE_SHOTS_BY_MIN.setdefault(at, []).append(ospec)

# -----------------------------
# Allocation & scheduling
# -----------------------------


class CarryAllocator:
    def __init__(self):
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return max(0, n)


ALLOC = CarryAllocator()


def even_schedule_seconds(start_s: float, end_s: float, n: int, key: str) -> List[float]:
    if n <= 0:
        return []
    dur = max(0.0, end_s - start_s)
    if dur <= 0.0:
        return [start_s for _ in range(n)]
    step = dur / n
    jitter_max = min(0.4, step * 0.2)
    out = []
    for i in range(n):
        center = start_s + (i + 0.5) * step
        j = (hash_u01(f"{key}|j|{i}") - 0.5) * 2.0 * jitter_max
        t = center + j
        if t < start_s:
            t = start_s + (hash_u01(f"{key}|jl|{i}") * min(0.001, dur))
        if t >= end_s:
            t = end_s - (hash_u01(f"{key}|jr|{i}") * min(0.001, dur))
        out.append(t)
    return out


def to_iso8601_ms(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


# -----------------------------
# Message rendering and domains
# -----------------------------


def gen_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return hash_choice(key, list(v))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return hash_int(key, lo, hi)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        x = lo + (hi - lo) * hash_u01(key)
        return round(x, 4)
    if k == "hex":
        return hash_hex(key, int(v))
    if k == "str":
        pattern = str(v)
        if "{hex8}" in pattern:
            return pattern.replace("{hex8}", hash_hex(key + "|hex8", 8))
        if "{hex}" in pattern:
            return pattern.replace("{hex}", hash_hex(key + "|hex", 8))
        return pattern
    if k == "uuid":
        h = hashlib.md5((SEED + "|" + key).encode("utf-8")).hexdigest()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "ip":
        a = 10
        b = 0
        c = hash_int(key + "|ip|c", 0, 255)
        d = hash_int(key + "|ip|d", 1, 254)
        return f"{a}.{b}.{c}.{d}"
    return ""


def get_int_domain_for_var(ref: str, state: str, varname: str) -> Optional[Tuple[int, int]]:
    ldef = LOGDEF[ref]
    dom = None
    if (ldef.get("vars") or {}).get(varname) is not None:
        dom = (ldef.get("vars") or {}).get(varname)
    sv = ((ldef.get("state_vars") or {}).get(state) or {})
    if sv.get(varname) is not None:
        dom = sv.get(varname)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    if hi < lo:
        lo, hi = hi, lo
    return (lo, hi)


def get_float_domain_for_var(ref: str, state: str, varname: str) -> Optional[Tuple[float, float]]:
    ldef = LOGDEF[ref]
    dom = None
    if (ldef.get("vars") or {}).get(varname) is not None:
        dom = (ldef.get("vars") or {}).get(varname)
    sv = ((ldef.get("state_vars") or {}).get(state) or {})
    if sv.get(varname) is not None:
        dom = sv.get(varname)
    if not dom:
        return None
    if dom.get("k") != "f":
        return None
    lo, hi = float(dom["v"][0]), float(dom["v"][1])
    if hi < lo:
        lo, hi = hi, lo
    return (lo, hi)


def clamp_int_to_domain(x: int, dom: Optional[Tuple[int, int]]) -> int:
    if dom is None:
        return int(x)
    lo, hi = dom
    return int(max(lo, min(hi, int(x))))


def render_log(ref: str, state: str, host: str, service: str, key: str, overrides: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    ldef = LOGDEF[ref]
    lvl = ldef["lvl"]
    msg_tpl = ldef["msg"]
    placeholders = extract_placeholders(msg_tpl)

    doms: Dict[str, Dict[str, Any]] = {}
    doms.update(ldef.get("vars", {}) or {})
    state_vars = (ldef.get("state_vars", {}) or {}).get(state, {}) if ldef.get("state_vars") else {}
    doms.update(state_vars)

    # Host-sticky convenient bindings
    if "node" in doms and "node" not in overrides:
        overrides = dict(overrides)
        overrides["node"] = host
    if "proxy" in doms and "proxy" not in overrides and host.startswith("proxy-"):
        overrides = dict(overrides)
        overrides["proxy"] = host
    if "host" in doms and "host" not in overrides:
        overrides = dict(overrides)
        overrides["host"] = host

    ctx: Dict[str, Any] = {}
    for name in placeholders:
        if name in overrides:
            ctx[name] = overrides[name]
        elif name in doms:
            ctx[name] = gen_from_domain(doms[name], f"{key}|{ref}|{name}")
        else:
            ctx[name] = ""

    # Enforce model assumptions
    if ref == "zk_ensemble.zk_quorum_lost":
        ctx["needed"] = 2
        ctx["connected_peers"] = int(min(int(ctx.get("connected_peers", 1)), 1))
    if ref in ("kibana_instance.open_sockets_metric", "kibana_instance.open_sockets_pressure"):
        fd = int(ctx.get("fd_limit", 65535))
        op = int(ctx.get("open", 0))
        ctx["fd_limit"] = fd
        ctx["open"] = min(op, fd)

    msg = msg_tpl.format(**ctx)
    trace_id = overrides.get("trace_id", "")
    if not isinstance(trace_id, str):
        trace_id = str(trace_id)
    return lvl, msg, trace_id, service, host


# -----------------------------
# Flow simulation helpers
# -----------------------------


def flow_latency_mult(interval: Interval, flow_id: str) -> Dict[str, float]:
    if interval.state != "f":
        return {"p50": 1.0, "p95": 1.0}
    return interval.flow_latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})


def flow_rate_mult(interval: Interval, flow_id: str) -> float:
    if interval.state != "f":
        return 1.0
    return float(interval.flow_rate_mult.get(flow_id, 1.0))


def bg_rate_mult(interval: Interval, ref: str) -> float:
    if interval.state != "f":
        return 1.0
    return float(interval.bg_rate_mult.get(ref, 1.0))


def allocate_attempt_counts(n: int, expected_attempts: float, max_attempts: int, key: str) -> List[int]:
    if n <= 0:
        return []
    e = float(expected_attempts)
    e = clamp(e, 1.0, float(max_attempts))
    lo = int(math.floor(e))
    hi = int(math.ceil(e))
    lo = max(1, lo)
    hi = min(max_attempts, hi)
    if lo == hi:
        return [lo] * n
    frac = e - lo
    k = int(round(frac * n))
    k = max(0, min(n, k))
    order = sorted(range(n), key=lambda i: hash_u01(f"{key}|mix|{i}"))
    counts = [lo] * n
    for idx in order[:k]:
        counts[idx] = hi
    return counts


def pick_component_host(component_id: str, key: str, preferred: Optional[str] = None) -> str:
    hosts = COMP[component_id]["hosts"]
    if preferred is not None and preferred in hosts:
        return preferred
    return str(hash_choice(f"{key}|host|{component_id}", hosts))


def choose_error_mapping(flow_id: str, key: str) -> Dict[str, str]:
    if flow_id in ("es_search_503_route_unavailable",):
        err = hash_choice(f"{key}|es_err", ["route_unavailable", "upstream_timeout", "connect_reset", "observer_unstable", "upstream_503"])
        if err in ("upstream_timeout",):
            status = "504"
        elif err in ("connect_reset",):
            status = "502"
        else:
            status = "503"
        return {"err": err, "status": status}

    if flow_id in ("kibana_ui_proxy_fail",):
        err = hash_choice(f"{key}|kb_route_err", ["route_unavailable", "observer_unstable", "connect_reset"])
        status = "502" if err == "connect_reset" else "503"
        return {"err": err, "status": status}

    if flow_id in ("kibana_ui_503_kibana_unhealthy",):
        kb_err = hash_choice(f"{key}|kb_ui_err", ["es_unreachable", "timeout"])
        kb_status = "504" if kb_err == "timeout" else "503"
        proxy_err = "upstream_timeout" if kb_err == "timeout" else "kibana_unhealthy"
        proxy_status = kb_status
        return {"kibana_err": kb_err, "kibana_status": kb_status, "proxy_err": proxy_err, "proxy_status": proxy_status}

    if flow_id in ("console_list_deployments_timeout",):
        err = hash_choice(f"{key}|cp_err", ["zk_timeout", "zk_unavailable"])
        status = "504" if err == "zk_timeout" else "503"
        return {"err": err, "status": status}

    if flow_id in ("kibana_es_call_fail_proxy",):
        kb_err = hash_choice(f"{key}|es_call_err", ["proxy_unavailable", "timeout"])
        tls_err = "proxy_connect_failed" if kb_err == "proxy_unavailable" else "upstream_timeout"
        return {"err": kb_err, "tls_err": tls_err}

    if flow_id in ("kibana_es_call_fail_conntrack",):
        err = hash_choice(f"{key}|es_call_ct_err", ["conntrack_full", "socket_leak"])
        return {"err": err}

    return {}


def flow_chain_p95_ms(interval: Interval, state: str, flow_id: str) -> float:
    fdef = FLOWS[state][flow_id]
    lm = flow_latency_mult(interval, flow_id)
    p95 = 0.0
    for pair in fdef["latency_ms"]:
        p95 += float(pair[1]) * float(lm["p95"])
    return p95


def modeled_external_rpms(interval: Interval) -> Tuple[float, float, float, float]:
    """
    Returns: (modeled_total_rpm, modeled_err_rpm, success_p95_ms_est, err_p95_ms_est)
    p95 estimates are weighted averages of per-flow chain p95 hints (not true quantiles).
    """
    if interval.state == "n":
        succ = [("es_search_ok", 250.0), ("kibana_ui_ok", 80.0), ("console_list_deployments_ok", 20.0)]
        total = 0.0
        err = 0.0
        w_s = 0.0
        for fid, rpm in succ:
            total += rpm
            w_s += flow_chain_p95_ms(interval, "n", fid) * rpm
        s_p95 = w_s / total if total > 0 else 80.0
        return total, err, s_p95, 0.0

    succ = [("es_search_200_after_recovery", FLOWS["f"]["es_search_200_after_recovery"]["rpm"]), ("console_list_deployments_ok_after_restore", FLOWS["f"]["console_list_deployments_ok_after_restore"]["rpm"])]
    errs = [
        ("es_search_503_route_unavailable", FLOWS["f"]["es_search_503_route_unavailable"]["rpm"]),
        ("kibana_ui_proxy_fail", FLOWS["f"]["kibana_ui_proxy_fail"]["rpm"]),
        ("kibana_ui_503_kibana_unhealthy", FLOWS["f"]["kibana_ui_503_kibana_unhealthy"]["rpm"]),
        ("console_list_deployments_timeout", FLOWS["f"]["console_list_deployments_timeout"]["rpm"]),
    ]

    total = 0.0
    err = 0.0
    w_s = 0.0
    s_sum = 0.0
    w_e = 0.0
    e_sum = 0.0

    for fid, rpm in succ:
        m = flow_rate_mult(interval, fid)
        rr = rpm * m
        if rr <= 0:
            continue
        total += rr
        s_sum += rr
        w_s += flow_chain_p95_ms(interval, "f", fid) * rr

    for fid, rpm in errs:
        m = flow_rate_mult(interval, fid)
        rr = rpm * m
        if rr <= 0:
            continue
        total += rr
        err += rr
        e_sum += rr
        w_e += flow_chain_p95_ms(interval, "f", fid) * rr

    s_p95 = w_s / s_sum if s_sum > 0 else 120.0
    e_p95 = w_e / e_sum if e_sum > 0 else 600.0
    return total, err, s_p95, e_p95


def derive_alb_overrides(interval: Interval, state: str, ref: str, key: str) -> Dict[str, Any]:
    """
    Produce ALB metric values that are coherent with the modeled external flows, but
    allow for additional unmodeled *successful* traffic to keep the metric within
    its encoded domain (avoids post-hoc clamping that can contradict the flow mix).
    """
    ldef = LOGDEF[ref]
    dom_req = get_int_domain_for_var(ref, state, "req_rps") or (0, 999999)
    dom_5xx = get_float_domain_for_var(ref, state, "http_5xx_rate") or (0.0, 1.0)
    dom_reset = get_float_domain_for_var(ref, state, "target_reset_rate") or (0.0, 1.0)
    dom_p95 = get_int_domain_for_var(ref, state, "p95_ms") or (0, 999999)

    total_rpm, err_rpm, succ_p95, err_p95 = modeled_external_rpms(interval)
    if total_rpm <= 0:
        req_rps = clamp_int_to_domain(0, dom_req)
        return {"req_rps": req_rps, "http_5xx_rate": 0.0, "target_reset_rate": 0.0, "p95_ms": clamp_int_to_domain(int(round((dom_p95[0] + dom_p95[1]) / 2)), dom_p95)}

    # If modeled error fraction is above the template's max, pad with unmodeled successful traffic.
    max_5xx = float(dom_5xx[1])
    pad_ok_rpm = 0.0
    if max_5xx > 0 and err_rpm > 0:
        min_total_needed = (err_rpm / max_5xx) * 1.0005  # small slack ensures computed rate <= max_5xx after rounding
        if min_total_needed > total_rpm:
            pad_ok_rpm = min_total_needed - total_rpm

    padded_total_rpm = total_rpm + pad_ok_rpm
    rate = (err_rpm / padded_total_rpm) if padded_total_rpm > 0 else 0.0
    # rate should already be <= max_5xx; keep numerical stability.
    if max_5xx > 0:
        rate = min(rate, max_5xx)

    # req_rps based on padded total to keep 5xx-rate and req_rps mutually consistent.
    raw_rps = padded_total_rpm / 60.0
    # Small deterministic jitter but keep within domain by adjusting by +/- 1 rps.
    j = (hash_u01(f"{key}|rps_j") - 0.5) * 2.0
    req_rps = int(round(raw_rps + j))
    req_rps = clamp_int_to_domain(req_rps, dom_req)

    # p95_ms: use success baseline for recovered mode, error baseline for degraded.
    is_recovered = (ref == "lb_edge.lb_metrics_recovered")
    base_p95 = succ_p95 if is_recovered else max(succ_p95, err_p95)
    err_frac = (err_rpm / padded_total_rpm) if padded_total_rpm > 0 else 0.0
    if is_recovered:
        # If errors are <=3%, p95 usually remains close to success baseline.
        mult = 0.95 + 0.10 * hash_u01(f"{key}|p95_j")
        p95_ms = int(round(base_p95 * mult * (1.0 + 0.15 * min(err_frac, 0.05))))
    else:
        # Degraded: p95 tends to increase with error fraction and coordination instability.
        mult = 0.92 + 0.18 * hash_u01(f"{key}|p95_j")
        p95_ms = int(round(base_p95 * mult * (1.0 + 0.35 * min(err_frac, 0.6))))

    p95_ms = clamp_int_to_domain(p95_ms, dom_p95)

    reset = rate * 0.5
    reset = clamp(reset, float(dom_reset[0]), float(dom_reset[1]))

    return {"req_rps": req_rps, "http_5xx_rate": round(rate, 4), "target_reset_rate": round(reset, 4), "p95_ms": p95_ms}


def retry_backoff_domain_for_flow(state: str, fdef: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    refs = list(fdef["retry"].get("emit_per_retry", []) or [])
    if not refs:
        return None
    los: List[int] = []
    his: List[int] = []
    for rref in refs:
        if "backoff_ms" not in extract_placeholders(LOGDEF[rref]["msg"]):
            continue
        dom = get_int_domain_for_var(rref, state, "backoff_ms")
        if dom is None:
            continue
        lo, hi = dom
        los.append(lo)
        his.append(hi)
    if not los:
        return None
    lo_i = max(los)
    hi_i = min(his)
    if lo_i <= hi_i:
        return (lo_i, hi_i)
    return (los[0], his[0])


def dur_mode_for_ref(ref: str) -> str:
    if ref.startswith("tls_proxy.forward_"):
        return "step"
    return "total"


# -----------------------------
# Simulation
# -----------------------------

BASE_TIME = datetime(2019, 2, 4, 0, 0, 0, tzinfo=timezone.utc)

rows: List[Dict[str, Any]] = []
seq = 0


def emit_row(ts_s: float, level: str, message: str, trace_id: str, service: str, host: str):
    global seq
    rows.append({"_ts": ts_s, "_seq": seq, "level": level, "message": message, "trace_id": trace_id or "", "service": service or "", "host": host or ""})
    seq += 1


def simulate_background(interval: Interval):
    start_s = interval.start_min * 60.0
    end_s = interval.end_min * 60.0
    state = interval.state
    for cid, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            ref = f"{cid}.{log_id}"
            base_rate = float(e["per_min"])
            scope = e.get("scope", None) or "per_host"

            mult = bg_rate_mult(interval, ref)
            eff_rate = base_rate * mult
            if eff_rate <= 0.0:
                continue

            if scope == "global":
                expected = eff_rate * ((end_s - start_s) / 60.0)
                n = ALLOC.alloc(f"bg|{state}|{ref}|global", expected)
                times = even_schedule_seconds(start_s, end_s, n, f"bg|{state}|{ref}|global|{interval.start_min}")
                host = comp["hosts"][0] if comp["hosts"] else ""
                service = comp.get("svc", "") or ""
                for i, t in enumerate(times):
                    overrides: Dict[str, Any] = {}
                    if cid == "lb_edge" and log_id in ("lb_metrics", "lb_metrics_recovered"):
                        # Keep metrics within their encoded domain *without clamping* by allowing
                        # additional unmodeled successful traffic to satisfy the domain bound.
                        overrides.update(derive_alb_overrides(interval, state, ref, f"alb|{interval.start_min}|{ref}|{i}"))

                    lvl, msg, _, svc, h = render_log(ref, state, host, service, f"bg|{interval.state}|{ref}|{interval.start_min}|{i}", overrides)
                    emit_row(t, lvl, msg, "", svc, h)
            else:
                for host in comp.get("hosts", []) or [""]:
                    expected = eff_rate * ((end_s - start_s) / 60.0)
                    n = ALLOC.alloc(f"bg|{state}|{ref}|{host}", expected)
                    times = even_schedule_seconds(start_s, end_s, n, f"bg|{state}|{ref}|{host}|{interval.start_min}")
                    service = comp.get("svc", "") or ""
                    for i, t in enumerate(times):
                        overrides = {}
                        if ref == "proxy_router.zk_session_reset":
                            overrides["proxy"] = host
                            overrides["observer"] = hash_choice(f"bg|sess|{interval.start_min}|{host}|{i}", COMP["zk_observer"]["hosts"])
                        lvl, msg, _, svc, h = render_log(ref, state, host, service, f"bg|{interval.state}|{ref}|{host}|{interval.start_min}|{i}", overrides)
                        emit_row(t, lvl, msg, "", svc, h)


def simulate_flow_instances(interval: Interval):
    start_s = interval.start_min * 60.0
    end_s = interval.end_min * 60.0
    state = interval.state
    flows = SYSTEM["flows"][state]

    for fdef in flows:
        fid = fdef["id"]
        rpm = float(fdef["rpm"])
        mult = flow_rate_mult(interval, fid)
        eff_rpm = rpm * mult
        if eff_rpm <= 0.0:
            continue

        expected = eff_rpm * ((end_s - start_s) / 60.0)
        n_instances = ALLOC.alloc(f"flow|{state}|{fid}|{interval.start_min}", expected)
        if n_instances <= 0:
            continue

        start_times = even_schedule_seconds(start_s, end_s, n_instances, f"flow|{state}|{fid}|{interval.start_min}")
        attempt_counts = allocate_attempt_counts(
            n_instances,
            fdef["retry"]["expected_attempts"],
            fdef["retry"]["max_attempts"],
            f"flow|{state}|{fid}|{interval.start_min}|attempts",
        )

        lm = flow_latency_mult(interval, fid)
        lat_pairs = fdef["latency_ms"]
        backoff_dom = retry_backoff_domain_for_flow(state, fdef)

        for idx, t0 in enumerate(start_times):
            inst_key = f"inst|{state}|{fid}|{interval.start_min}|{idx}"
            trace_id = ""
            if SYSTEM["tracing"]["on"] and fdef.get("trace", False):
                trace_id = hash_hex(f"trace|{inst_key}", 32)

            # Host stickiness across the request chain (and retries)
            emit_refs_base = list(fdef["emit"]) + list(fdef["retry"].get("emit_per_retry", []) or [])
            emitting_components = sorted(set(ref.split(".")[0] for ref in emit_refs_base))
            host_map: Dict[str, str] = {}
            kb_host: Optional[str] = None
            if "kibana_instance" in emitting_components:
                kb_host = pick_component_host("kibana_instance", inst_key)
                host_map["kibana_instance"] = kb_host
            if "tls_proxy" in emitting_components:
                host_map["tls_proxy"] = pick_component_host("tls_proxy", inst_key, preferred=kb_host)
            if "proxy_router" in emitting_components:
                host_map["proxy_router"] = pick_component_host("proxy_router", inst_key)
            if "control_plane_api" in emitting_components:
                host_map["control_plane_api"] = pick_component_host("control_plane_api", inst_key)

            vhost = gen_from_domain({"k": "str", "v": "dep-{hex8}.us-east-1.aws.example.com"}, f"{inst_key}|vhost|{trace_id}")
            base_err_map = choose_error_mapping(fid, inst_key)

            attempts = attempt_counts[idx]
            prev_attempt_end = t0

            for a in range(1, attempts + 1):
                if a == 1:
                    attempt_start = t0
                else:
                    back_pairs = fdef["retry"].get("backoff_ms", []) or []
                    pair = back_pairs[a - 2] if (a - 2) < len(back_pairs) else back_pairs[-1]
                    b_p50, b_p95 = float(pair[0]), float(pair[1])
                    backoff_ms = sample_lognormal_ms(b_p50, b_p95, f"{inst_key}|backoff|{a}", cap_ms=3.0 * b_p95, min_ms=1, max_ms=20000)
                    backoff_ms = clamp_int_to_domain(backoff_ms, backoff_dom)

                    attempt_start = prev_attempt_end + (backoff_ms / 1000.0)

                    # Retry marker(s) are emitted once on retry attempts (2..A)
                    for r_i, rref in enumerate(fdef["retry"].get("emit_per_retry", []) or []):
                        rcid = rref.split(".")[0]
                        rhost = host_map.get(rcid, pick_component_host(rcid, inst_key))
                        rsvc = COMP[rcid].get("svc", "") or ""
                        overrides = {"trace_id": trace_id, "attempt": a, "backoff_ms": backoff_ms, "vhost": vhost}
                        lvl, msg, _, svc, host = render_log(rref, state, rhost, rsvc, f"{inst_key}|retry|{a}|{r_i}", overrides)
                        emit_row(attempt_start + (r_i * 0.001), lvl, msg, trace_id, svc, host)

                endpoint = hash_choice(f"{inst_key}|endpoint|{a}", ["/_cluster/health", "/_search"])
                kb_path = hash_choice(f"{inst_key}|kbpath|{a}", ["/app/kibana", "/api/status"])

                # For a "success" flow that can retry (kibana_es_call_ok),
                # do not emit terminal success on earlier attempts (would imply retry-after-success).
                emit_chain = list(fdef["emit"])
                if fid == "kibana_es_call_ok" and attempts > 1 and a < attempts:
                    emit_chain = ["kibana_instance.es_call_start"]

                attempt_err_map = dict(base_err_map)
                route_src = "cache" if state == "n" else "observer"

                cum_ms = 0
                for li, ref in enumerate(emit_chain):
                    placeholders = extract_placeholders(LOGDEF[ref]["msg"])

                    p50 = float(lat_pairs[li][0]) * float(lm["p50"])
                    p95 = float(lat_pairs[li][1]) * float(lm["p95"])
                    step_ms = sample_lognormal_ms(p50, p95, f"{inst_key}|lat|{a}|{li}|{ref}", cap_ms=None, min_ms=1, max_ms=None)

                    if "dur_ms" in placeholders:
                        dom = get_int_domain_for_var(ref, state, "dur_ms")
                        mode = dur_mode_for_ref(ref)
                        if mode == "step":
                            step_ms = clamp_int_to_domain(step_ms, dom)

                    cum_ms += int(step_ms)

                    if "dur_ms" in placeholders:
                        dom = get_int_domain_for_var(ref, state, "dur_ms")
                        mode = dur_mode_for_ref(ref)
                        if mode == "total":
                            desired_cum = clamp_int_to_domain(cum_ms, dom)
                            if desired_cum != cum_ms:
                                delta = desired_cum - cum_ms
                                cum_ms = desired_cum
                                step_ms = max(1, int(step_ms) + int(delta))

                    ts = attempt_start + (cum_ms / 1000.0)

                    cid = ref.split(".")[0]
                    host = host_map.get(cid, pick_component_host(cid, inst_key))
                    svc = COMP[cid].get("svc", "") or ""
                    overrides2: Dict[str, Any] = {"trace_id": trace_id, "vhost": vhost}

                    if "attempt" in placeholders:
                        overrides2["attempt"] = a

                    if "dur_ms" in placeholders:
                        if dur_mode_for_ref(ref) == "step":
                            overrides2["dur_ms"] = int(step_ms)
                        else:
                            overrides2["dur_ms"] = int(cum_ms)

                    if ref == "proxy_router.proxy_es_access_ok":
                        overrides2["route_src"] = route_src
                    elif ref == "proxy_router.proxy_es_access_err":
                        overrides2.update(attempt_err_map)
                    elif ref == "proxy_router.proxy_kb_access_err_route":
                        overrides2.update(attempt_err_map)
                    elif ref == "proxy_router.proxy_kb_access_err_upstream":
                        if "proxy_err" in attempt_err_map and "proxy_status" in attempt_err_map:
                            overrides2["err"] = attempt_err_map["proxy_err"]
                            overrides2["status"] = attempt_err_map["proxy_status"]
                        else:
                            overrides2.update(attempt_err_map)
                    elif ref == "control_plane_api.console_access_ok":
                        overrides2["zk_reads"] = hash_int(f"{inst_key}|zk_reads|{a}", 1, 5)
                    elif ref == "control_plane_api.console_access_err":
                        overrides2.update(attempt_err_map)
                    elif ref == "kibana_instance.kibana_ui_ok":
                        overrides2["path"] = kb_path
                    elif ref == "kibana_instance.kibana_ui_err":
                        overrides2["path"] = kb_path
                        if "kibana_err" in attempt_err_map and "kibana_status" in attempt_err_map:
                            overrides2["err"] = attempt_err_map["kibana_err"]
                            overrides2["status"] = attempt_err_map["kibana_status"]
                        else:
                            overrides2["err"] = hash_choice(f"{inst_key}|kb_ui_err|{a}", ["es_unreachable", "timeout"])
                            overrides2["status"] = "504" if overrides2["err"] == "timeout" else "503"
                    elif ref == "kibana_instance.es_call_start":
                        overrides2["endpoint"] = endpoint
                    elif ref == "kibana_instance.es_call_err":
                        overrides2["err"] = attempt_err_map.get("err", hash_choice(f"{inst_key}|es_call_err2|{a}", ["proxy_unavailable", "timeout"]))
                    elif ref == "kibana_instance.es_call_err_conntrack":
                        overrides2["err"] = attempt_err_map.get("err", hash_choice(f"{inst_key}|es_call_ct2|{a}", ["conntrack_full", "socket_leak"]))
                    elif ref == "tls_proxy.forward_err":
                        overrides2["err"] = attempt_err_map.get("tls_err", hash_choice(f"{inst_key}|tls_err|{a}", ["proxy_connect_failed", "upstream_timeout"]))
                    elif ref == "tls_proxy.forward_err_conntrack":
                        overrides2["err"] = "conntrack_full"

                    lvl, msg, _, svc2, host2 = render_log(ref, state, host, svc, f"{inst_key}|emit|{a}|{li}|{ref}", overrides2)
                    emit_row(ts, lvl, msg, trace_id, svc2, host2)

                prev_attempt_end = attempt_start + (cum_ms / 1000.0)


def simulate_one_shots():
    for at_min, shots in ONE_SHOTS_BY_MIN.items():
        base_s = at_min * 60.0
        for si, ospec in enumerate(shots):
            ref = ospec["ref"]
            cid = ref.split(".")[0]
            comp = COMP[cid]
            service = comp.get("svc", "") or ""
            hosts = ospec.get("hosts", []) or (comp.get("hosts", []) or [""])
            count = int(ospec.get("count", 1))
            for j in range(count):
                host = hosts[j % len(hosts)]
                t = base_s + (hash_u01(f"oneshot|{at_min}|{si}|{j}") * 5.0)
                overrides: Dict[str, Any] = {}
                if ref.startswith("zk_ensemble.kernel_softlock") or ref.startswith("zk_ensemble.host_reboot"):
                    overrides["host"] = host
                if ref.startswith("zk_observer.initlimit_changed"):
                    overrides["node"] = host
                lvl, msg, _, svc, h = render_log(ref, "f", host, service, f"oneshot|{at_min}|{si}|{j}", overrides)
                emit_row(t, lvl, msg, "", svc, h)


def run():
    for interval in INTERVALS:
        simulate_background(interval)
        simulate_flow_instances(interval)
    simulate_one_shots()

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["_ts", "_seq"], ascending=[True, True], kind="mergesort").reset_index(drop=True)

    ts = []
    for t in df["_ts"].to_numpy():
        dt = BASE_TIME + timedelta(seconds=float(t))
        ts.append(to_iso8601_ms(dt))

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

    def fix_trace(x: str) -> str:
        x = (x or "").strip()
        if x == "":
            return ""
        x = re.sub(r"[^0-9a-f]", "", x.lower())
        if len(x) < 32:
            x = (x + ("0" * 32))[:32]
        return x[:32]

    out["trace_id"] = out["trace_id"].map(fix_trace)
    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    run()
