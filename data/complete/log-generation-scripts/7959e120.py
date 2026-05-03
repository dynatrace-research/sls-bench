import math
import hashlib
import random
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "slack_like_collaboration_stack"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_lb",
            "svc": "edge-lb",
            "hosts": ["lb-a", "lb-b"],
            "logs": {
                "access_message_200": {
                    "lvl": "INFO",
                    "msg": "access route=/api/messages.send status=200 dur_ms={dur_ms} target=web-{slot} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [10, 9000]},
                        "slot": {"k": "i", "v": [1, 200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_message_503": {
                    "lvl": "WARN",
                    "msg": "access route=/api/messages.send status=503 dur_ms={dur_ms} target=web-{slot} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [50, 12000]},
                        "slot": {"k": "i", "v": [1, 200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_ping_200": {
                    "lvl": "INFO",
                    "msg": "access route=/api/ping status=200 dur_ms={dur_ms} target=web-{slot}",
                    "vars": {"dur_ms": {"k": "i", "v": [2, 3000]}, "slot": {"k": "i", "v": [1, 200]}},
                },
                "access_ping_503": {
                    "lvl": "WARN",
                    "msg": "access route=/api/ping status=503 dur_ms={dur_ms} target=web-{slot}",
                    "vars": {"dur_ms": {"k": "i", "v": [50, 9000]}, "slot": {"k": "i", "v": [1, 200]}},
                },
                "healthcheck_summary_small": {
                    "lvl": "INFO",
                    "msg": "healthchecks targets={targets} failing={failing}",
                    "vars": {},
                    "state_vars": {
                        "n": {"targets": {"k": "i", "v": [40, 70]}, "failing": {"k": "i", "v": [0, 4]}},
                        "f": {"targets": {"k": "i", "v": [40, 90]}, "failing": {"k": "i", "v": [0, 25]}},
                    },
                },
                "healthcheck_summary_large": {
                    "lvl": "INFO",
                    "msg": "healthchecks targets={targets} failing={failing}",
                    "vars": {},
                    "state_vars": {
                        "n": {"targets": {"k": "i", "v": [40, 70]}, "failing": {"k": "i", "v": [0, 4]}},
                        "f": {"targets": {"k": "i", "v": [100, 160]}, "failing": {"k": "i", "v": [10, 120]}},
                    },
                },
                "panic_mode_enabled": {
                    "lvl": "WARN",
                    "msg": "panic_mode enabled reason=healthcheck_failures threshold_pct={threshold_pct}",
                    "vars": {"threshold_pct": {"k": "i", "v": [50, 80]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "healthcheck_summary_small", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "healthcheck_summary_small", "per_min": 1.0, "scope": "per_host"},
                        {"id": "healthcheck_summary_large", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "web_tier",
            "svc": "web-tier",
            "hosts": [f"web-{i}" for i in range(1, 201)],
            "logs": {
                "msg_send_start": {
                    "lvl": "INFO",
                    "msg": "recv route=/api/messages.send req_id={req_id} trace={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "msg_send_backend_call": {
                    "lvl": "INFO",
                    "msg": "backend_call backend=messages op=CreateMessage timeout_ms={timeout_ms} trace={trace_id}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"timeout_ms": {"k": "i", "v": [1500, 2000]}},
                        "f": {"timeout_ms": {"k": "i", "v": [1500, 8000]}},
                    },
                },
                "msg_send_backend_timeout": {
                    "lvl": "ERROR",
                    "msg": "backend_timeout backend=messages op=CreateMessage waited_ms={waited_ms} err={err} trace={trace_id}",
                    "vars": {
                        "waited_ms": {"k": "i", "v": [1500, 8200]},
                        "err": {"k": "ch", "v": ["deadline_exceeded", "connect_timeout"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "msg_send_respond_200": {
                    "lvl": "INFO",
                    "msg": "respond status=200 dur_ms={dur_ms} bytes={bytes} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [10, 9000]},
                        "bytes": {"k": "i", "v": [200, 8000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "msg_send_respond_503": {
                    "lvl": "WARN",
                    "msg": "respond status=503 dur_ms={dur_ms} err={err} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [50, 12000]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "overload"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ping_start": {"lvl": "DEBUG", "msg": "recv route=/api/ping req_id={req_id}", "vars": {"req_id": {"k": "uuid", "v": None}}},
                "ping_respond_200": {
                    "lvl": "DEBUG",
                    "msg": "respond route=/api/ping status=200 dur_ms={dur_ms}",
                    "vars": {"dur_ms": {"k": "i", "v": [1, 2000]}},
                },
                "ping_respond_503": {
                    "lvl": "DEBUG",
                    "msg": "respond route=/api/ping status=503 dur_ms={dur_ms} err={err}",
                    "vars": {"dur_ms": {"k": "i", "v": [20, 9000]}, "err": {"k": "ch", "v": ["overload", "upstream_unreachable"]}},
                },
                "apache_status": {
                    "lvl": "INFO",
                    "msg": "apache_status busy={busy} idle={idle} cpu_pct={cpu_pct} queue={queue}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "busy": {"k": "i", "v": [10, 90]},
                            "idle": {"k": "i", "v": [120, 320]},
                            "cpu_pct": {"k": "f", "v": [15.0, 60.0]},
                            "queue": {"k": "i", "v": [0, 30]},
                        },
                        "f": {
                            "busy": {"k": "i", "v": [60, 256]},
                            "idle": {"k": "i", "v": [0, 260]},
                            "cpu_pct": {"k": "f", "v": [5.0, 55.0]},
                            "queue": {"k": "i", "v": [0, 400]},
                        },
                    },
                },
                "worker_queue_high": {
                    "lvl": "WARN",
                    "msg": "worker_saturation queue={queue} est_wait_ms={est_wait_ms}",
                    "vars": {"queue": {"k": "i", "v": [80, 500]}, "est_wait_ms": {"k": "i", "v": [200, 7000]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "apache_status", "per_min": 1.0, "scope": "global"},
                        {"id": "worker_queue_high", "per_min": 0.08, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "apache_status", "per_min": 1.0, "scope": "global"},
                        {"id": "worker_queue_high", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "backend_api",
            "svc": "messages-backend",
            "hosts": ["be-1", "be-2"],
            "logs": {
                "msg_send_rpc_recv": {
                    "lvl": "INFO",
                    "msg": "rpc_recv op=CreateMessage from=web-tier trace={trace_id}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}},
                },
                "msg_send_rpc_ok": {
                    "lvl": "INFO",
                    "msg": "rpc_ok op=CreateMessage dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [5, 6000]}, "trace_id": {"k": "hex", "v": 32}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "autoscaler",
            "svc": "autoscaler",
            "hosts": ["asctl-1"],
            "logs": {
                "scale_eval": {
                    "lvl": "INFO",
                    "msg": "eval asg=web-asg cpu_pct={cpu_pct} worker_util={worker_util} current={current} desired={desired}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "cpu_pct": {"k": "f", "v": [20.0, 55.0]},
                            "worker_util": {"k": "f", "v": [0.25, 0.75]},
                            "current": {"k": "i", "v": [40, 60]},
                            "desired": {"k": "i", "v": [40, 65]},
                        },
                        "f": {
                            "cpu_pct": {"k": "f", "v": [5.0, 35.0]},
                            "worker_util": {"k": "f", "v": [0.75, 1.20]},
                            "current": {"k": "i", "v": [30, 160]},
                            "desired": {"k": "i", "v": [60, 160]},
                        },
                    },
                },
                "asg_limit": {
                    "lvl": "WARN",
                    "msg": "asg_limit_reached asg=web-asg desired={desired} max={max}",
                    "vars": {"desired": {"k": "i", "v": [120, 160]}, "max": {"k": "i", "v": [120, 120]}},
                },
                "scale_out_request": {"lvl": "INFO", "msg": "scale_out_request asg=web-asg instance_id=i-{iid}", "vars": {"iid": {"k": "hex", "v": 8}}},
                "instance_terminated": {
                    "lvl": "WARN",
                    "msg": "terminate instance_id=i-{iid} reason={reason} active_ssh={active_ssh}",
                    "vars": {"iid": {"k": "hex", "v": 8}, "reason": {"k": "ch", "v": ["cpu_low", "unhealthy", "scale_in_policy"]}, "active_ssh": {"k": "i", "v": [0, 5]}},
                },
                "downscaling_disabled": {
                    "lvl": "INFO",
                    "msg": "policy_change asg=web-asg downscaling=disabled requested_by={actor}",
                    "vars": {"actor": {"k": "ch", "v": ["incident_commander", "oncall_infra", "oncall_web"]}},
                },
                "asg_max_increased": {
                    "lvl": "INFO",
                    "msg": "config_change asg=web-asg max_increased old_max={old_max} new_max={new_max}",
                    "vars": {"old_max": {"k": "i", "v": [120, 120]}, "new_max": {"k": "i", "v": [140, 180]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "scale_eval", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "scale_eval", "per_min": 1.0, "scope": "global"}, {"id": "asg_limit", "per_min": 0.3, "scope": "global"}]},
            },
        },
        {
            "id": "provision_service",
            "svc": "provision-service",
            "hosts": ["prov-1", "prov-2"],
            "logs": {
                "provision_begin": {"lvl": "INFO", "msg": "provision_start instance_id=i-{iid} role=web", "vars": {"iid": {"k": "hex", "v": 8}}},
                "provision_ok": {"lvl": "INFO", "msg": "provision_ok instance_id=i-{iid} dur_s={dur_s}", "vars": {"iid": {"k": "hex", "v": 8}, "dur_s": {"k": "i", "v": [20, 900]}}},
                "provision_fail_emfile": {"lvl": "ERROR", "msg": "provision_fail instance_id=i-{iid} err=EMFILE open_files={open_files} limit=8192", "vars": {"iid": {"k": "hex", "v": 8}, "open_files": {"k": "i", "v": [8192, 8192]}}},
                "provision_fail_quota": {"lvl": "ERROR", "msg": "aws_api_error instance_id=i-{iid} api={api} code=Throttling retries={retries}", "vars": {"iid": {"k": "hex", "v": 8}, "api": {"k": "ch", "v": ["RunInstances", "DescribeInstances", "CreateTags"]}, "retries": {"k": "i", "v": [0, 6]}}},
                "pool_metrics_8192": {
                    "lvl": "INFO",
                    "msg": "pool active={active} queued={queued} open_files={open_files} limit=8192",
                    "vars": {},
                    "state_vars": {
                        "n": {"active": {"k": "i", "v": [5, 40]}, "queued": {"k": "i", "v": [0, 80]}, "open_files": {"k": "i", "v": [400, 1800]}},
                        "f": {"active": {"k": "i", "v": [20, 160]}, "queued": {"k": "i", "v": [0, 2600]}, "open_files": {"k": "i", "v": [800, 8192]}},
                    },
                },
                "pool_metrics_16384": {"lvl": "INFO", "msg": "pool active={active} queued={queued} open_files={open_files} limit=16384", "vars": {"active": {"k": "i", "v": [10, 140]}, "queued": {"k": "i", "v": [0, 1800]}, "open_files": {"k": "i", "v": [800, 16384]}}},
                "pool_overload_8192": {"lvl": "WARN", "msg": "pool_overload active={active} queued={queued} open_files={open_files} limit=8192", "vars": {"active": {"k": "i", "v": [50, 160]}, "queued": {"k": "i", "v": [300, 2600]}, "open_files": {"k": "i", "v": [7600, 8192]}}},
                "pool_overload_16384": {"lvl": "WARN", "msg": "pool_overload active={active} queued={queued} open_files={open_files} limit=16384", "vars": {"active": {"k": "i", "v": [40, 140]}, "queued": {"k": "i", "v": [200, 1800]}, "open_files": {"k": "i", "v": [12000, 16384]}}},
                "fd_exhaustion_8192": {"lvl": "ERROR", "msg": "resource_pressure kind=open_files open_files={open_files} limit=8192", "vars": {"open_files": {"k": "i", "v": [7800, 8192]}}},
                "fd_exhaustion_16384": {"lvl": "ERROR", "msg": "resource_pressure kind=open_files open_files={open_files} limit=16384", "vars": {"open_files": {"k": "i", "v": [15000, 16384]}}},
                "restarted": {"lvl": "INFO", "msg": "restart completed ulimit_nofile={ulimit_nofile}", "vars": {"ulimit_nofile": {"k": "i", "v": [16384, 16384]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "pool_metrics_8192", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "pool_metrics_8192", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_metrics_16384", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_overload_8192", "per_min": 1.0, "scope": "per_host"},
                        {"id": "fd_exhaustion_8192", "per_min": 0.4, "scope": "per_host"},
                        {"id": "pool_overload_16384", "per_min": 0.7, "scope": "per_host"},
                        {"id": "fd_exhaustion_16384", "per_min": 0.2, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "monitoring_ui",
            "svc": "monitoring-ui",
            "hosts": ["mon-1", "mon-2"],
            "logs": {
                "ui_health": {"lvl": "INFO", "msg": "ui_health ok=true build={build}", "vars": {"build": {"k": "ch", "v": ["2021.01.04-1", "2021.01.04-2"]}}},
                "db_timeout": {"lvl": "ERROR", "msg": "backend_unreachable upstream=db waited_ms={waited_ms} err=timeout", "vars": {"waited_ms": {"k": "i", "v": [500, 15000]}}},
                "ui_request_ok": {"lvl": "INFO", "msg": "http_request path=/dashboards/overview status=200 dur_ms={dur_ms}", "vars": {"dur_ms": {"k": "i", "v": [20, 2000]}}},
                "ui_request_fail": {"lvl": "ERROR", "msg": "http_request path=/dashboards/overview status=502 dur_ms={dur_ms} err={err}", "vars": {"dur_ms": {"k": "i", "v": [200, 15000]}, "err": {"k": "ch", "v": ["upstream_timeout", "connection_reset"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "ui_health", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "ui_health", "per_min": 1.0, "scope": "per_host"}, {"id": "db_timeout", "per_min": 1.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "network_fabric",
            "svc": "aws-tgw",
            "hosts": ["tgw-1"],
            "logs": {
                "tgw_stats": {
                    "lvl": "INFO",
                    "msg": "tgw_stats tgw_id={tgw_id} pps={pps} drop_pct={drop_pct} rtt_ms={rtt_ms}",
                    "vars": {"tgw_id": {"k": "ch", "v": ["tgw-main"]}},
                    "state_vars": {
                        "n": {"pps": {"k": "i", "v": [500, 3000]}, "drop_pct": {"k": "f", "v": [0.0, 0.2]}, "rtt_ms": {"k": "i", "v": [2, 8]}},
                        "f": {"pps": {"k": "i", "v": [4000, 30000]}, "drop_pct": {"k": "f", "v": [1.0, 8.0]}, "rtt_ms": {"k": "i", "v": [20, 220]}},
                    },
                },
                "packet_drop_alert": {"lvl": "WARN", "msg": "packet_drops elevated drop_pct={drop_pct} pps={pps}", "vars": {"drop_pct": {"k": "f", "v": [1.0, 10.0]}, "pps": {"k": "i", "v": [4000, 40000]}}},
            },
            "beh": {"n": {"emit": [{"id": "tgw_stats", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "tgw_stats", "per_min": 1.0, "scope": "global"}, {"id": "packet_drop_alert", "per_min": 0.8, "scope": "global"}]}},
        },
        {
            "id": "external_monitor",
            "svc": "ext-monitor",
            "hosts": ["extmon-1"],
            "logs": {
                "ping_check_ok": {"lvl": "INFO", "msg": "synthetic_check name=ping status=200 dur_ms={dur_ms}", "vars": {"dur_ms": {"k": "i", "v": [10, 2500]}}},
                "ping_check_fail": {"lvl": "WARN", "msg": "synthetic_check name=ping status=503 dur_ms={dur_ms} err_rate={err_rate}", "vars": {"dur_ms": {"k": "i", "v": [50, 8000]}, "err_rate": {"k": "f", "v": [0.01, 0.80]}}},
                "page_sent": {"lvl": "CRITICAL", "msg": "page_sent monitor=ping reason=error_rate_high observed_err_rate={err_rate}", "vars": {"err_rate": {"k": "f", "v": [0.05, 0.80]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "message_send_normal",
                    "rpm": 220.0,
                    "emit": [
                        "web_tier.msg_send_start",
                        "web_tier.msg_send_backend_call",
                        "backend_api.msg_send_rpc_recv",
                        "backend_api.msg_send_rpc_ok",
                        "web_tier.msg_send_respond_200",
                        "edge_lb.access_message_200",
                    ],
                    "latency_ms": [[2, 5], [5, 15], [8, 25], [20, 80], [2, 6], [1, 3]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "synthetic_ping_ok",
                    "rpm": 15.0,
                    "emit": ["web_tier.ping_start", "web_tier.ping_respond_200", "edge_lb.access_ping_200", "external_monitor.ping_check_ok"],
                    "latency_ms": [[1, 3], [1, 8], [1, 5], [5, 40]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {"id": "view_dashboard_ok", "rpm": 3.0, "emit": ["monitoring_ui.ui_request_ok"], "latency_ms": [[40, 400]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {
                    "id": "provision_web_instance_ok",
                    "rpm": 2.0,
                    "emit": ["autoscaler.scale_out_request", "provision_service.provision_begin", "provision_service.provision_ok"],
                    "latency_ms": [[5, 20], [20, 80], [30000, 70000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "message_send_success",
                    "rpm": 180.0,
                    "emit": [
                        "web_tier.msg_send_start",
                        "web_tier.msg_send_backend_call",
                        "backend_api.msg_send_rpc_recv",
                        "backend_api.msg_send_rpc_ok",
                        "web_tier.msg_send_respond_200",
                        "edge_lb.access_message_200",
                    ],
                    "latency_ms": [[3, 10], [15, 80], [20, 120], [60, 600], [5, 20], [2, 8]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "message_send_fail_timeout",
                    "rpm": 40.0,
                    "emit": ["web_tier.msg_send_start", "web_tier.msg_send_backend_call", "web_tier.msg_send_backend_timeout", "web_tier.msg_send_respond_503", "edge_lb.access_message_503"],
                    "latency_ms": [[3, 12], [30, 180], [1800, 8200], [5, 20], [2, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "synthetic_ping_fail",
                    "rpm": 12.0,
                    "emit": ["web_tier.ping_start", "web_tier.ping_respond_503", "edge_lb.access_ping_503", "external_monitor.ping_check_fail"],
                    "latency_ms": [[1, 5], [10, 200], [1, 10], [10, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "synthetic_ping_ok_small",
                    "rpm": 3.0,
                    "emit": ["web_tier.ping_start", "web_tier.ping_respond_200", "edge_lb.access_ping_200", "external_monitor.ping_check_ok"],
                    "latency_ms": [[1, 5], [2, 40], [1, 10], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {"id": "view_dashboard_fail", "rpm": 3.0, "emit": ["monitoring_ui.ui_request_fail"], "latency_ms": [[200, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {
                    "id": "provision_web_instance_ok",
                    "rpm": 2.0,
                    "emit": ["autoscaler.scale_out_request", "provision_service.provision_begin", "provision_service.provision_ok"],
                    "latency_ms": [[10, 60], [50, 250], [80000, 300000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "provision_web_instance_fail_emfile",
                    "rpm": 5.0,
                    "emit": ["autoscaler.scale_out_request", "provision_service.provision_begin", "provision_service.provision_fail_emfile"],
                    "latency_ms": [[10, 60], [100, 800], [10, 50]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "provision_web_instance_fail_quota",
                    "rpm": 3.0,
                    "emit": ["autoscaler.scale_out_request", "provision_service.provision_begin", "provision_service.provision_fail_quota"],
                    "latency_ms": [[10, 60], [100, 1200], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "tgw_packet_loss_autoscale_provision_bottleneck_2021_01_04"},
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "message_send_success": 1.1,
                        "message_send_fail_timeout": 0.05,
                        "synthetic_ping_fail": 0.1,
                        "synthetic_ping_ok_small": 4.0,
                        "provision_web_instance_ok": 0.0,
                        "provision_web_instance_fail_emfile": 0.0,
                        "provision_web_instance_fail_quota": 0.0,
                        "network_fabric.packet_drop_alert": 1.2,
                        "monitoring_ui.db_timeout": 1.5,
                        "autoscaler.asg_limit": 0.0,
                        "edge_lb.healthcheck_summary_small": 1.0,
                        "edge_lb.healthcheck_summary_large": 0.0,
                        "provision_service.pool_metrics_8192": 1.0,
                        "provision_service.pool_metrics_16384": 0.0,
                        "provision_service.pool_overload_8192": 0.0,
                        "provision_service.fd_exhaustion_8192": 0.0,
                        "provision_service.pool_overload_16384": 0.0,
                        "provision_service.fd_exhaustion_16384": 0.0,
                        "web_tier.worker_queue_high": 0.2,
                    },
                    "latency_multipliers": {"message_send_success": {"p50": 1.4, "p95": 1.6}, "message_send_fail_timeout": {"p50": 1.0, "p95": 1.0}, "view_dashboard_fail": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [{"ref": "external_monitor.page_sent", "count": 1, "hosts": ["extmon-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 25,
                    "rate_multipliers": {
                        "message_send_success": 1.3,
                        "message_send_fail_timeout": 8.0,
                        "synthetic_ping_fail": 1.0,
                        "synthetic_ping_ok_small": 0.2,
                        "provision_web_instance_ok": 3.0,
                        "provision_web_instance_fail_emfile": 4.0,
                        "provision_web_instance_fail_quota": 2.0,
                        "autoscaler.asg_limit": 0.0,
                        "edge_lb.healthcheck_summary_small": 0.0,
                        "edge_lb.healthcheck_summary_large": 1.0,
                        "provision_service.pool_metrics_8192": 1.0,
                        "provision_service.pool_metrics_16384": 0.0,
                        "provision_service.pool_overload_8192": 1.0,
                        "provision_service.fd_exhaustion_8192": 3.0,
                        "provision_service.pool_overload_16384": 0.0,
                        "provision_service.fd_exhaustion_16384": 0.0,
                        "web_tier.worker_queue_high": 2.0,
                    },
                    "latency_multipliers": {"message_send_success": {"p50": 3.0, "p95": 4.0}, "message_send_fail_timeout": {"p50": 1.0, "p95": 1.0}, "provision_web_instance_ok": {"p50": 2.2, "p95": 3.0}},
                    "one_shots": [{"ref": "edge_lb.panic_mode_enabled", "count": 1, "hosts": ["lb-a"]}, {"ref": "autoscaler.instance_terminated", "count": 6, "hosts": ["asctl-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 29,
                    "rate_multipliers": {
                        "message_send_success": 1.1,
                        "message_send_fail_timeout": 7.0,
                        "synthetic_ping_fail": 1.0,
                        "synthetic_ping_ok_small": 0.1,
                        "provision_web_instance_ok": 0.2,
                        "provision_web_instance_fail_emfile": 0.1,
                        "provision_web_instance_fail_quota": 0.1,
                        "autoscaler.asg_limit": 5.0,
                        "provision_service.pool_metrics_8192": 1.0,
                        "provision_service.pool_metrics_16384": 0.0,
                        "provision_service.pool_overload_8192": 0.6,
                        "provision_service.fd_exhaustion_8192": 1.5,
                        "provision_service.pool_overload_16384": 0.0,
                        "provision_service.fd_exhaustion_16384": 0.0,
                        "web_tier.worker_queue_high": 2.2,
                    },
                    "latency_multipliers": {"message_send_success": {"p50": 3.2, "p95": 4.2}, "message_send_fail_timeout": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [],
                },
                {
                    "order": 4,
                    "at_min": 33,
                    "rate_multipliers": {
                        "provision_web_instance_ok": 6.0,
                        "provision_web_instance_fail_emfile": 0.0,
                        "provision_web_instance_fail_quota": 0.5,
                        "synthetic_ping_fail": 0.25,
                        "synthetic_ping_ok_small": 2.0,
                        "message_send_success": 1.6,
                        "message_send_fail_timeout": 0.3,
                        "autoscaler.asg_limit": 0.0,
                        "provision_service.pool_metrics_8192": 0.0,
                        "provision_service.pool_metrics_16384": 1.0,
                        "provision_service.pool_overload_8192": 0.0,
                        "provision_service.fd_exhaustion_8192": 0.0,
                        "provision_service.pool_overload_16384": 0.3,
                        "provision_service.fd_exhaustion_16384": 0.2,
                        "web_tier.worker_queue_high": 1.2,
                    },
                    "latency_multipliers": {"message_send_success": {"p50": 2.0, "p95": 3.0}, "message_send_fail_timeout": {"p50": 1.0, "p95": 1.0}, "provision_web_instance_ok": {"p50": 1.6, "p95": 2.2}},
                    "one_shots": [{"ref": "provision_service.restarted", "count": 1, "hosts": ["prov-1"]}, {"ref": "autoscaler.downscaling_disabled", "count": 1, "hosts": ["asctl-1"]}, {"ref": "autoscaler.asg_max_increased", "count": 1, "hosts": ["asctl-1"]}],
                },
            ]
        }
    },
}

BASE_TIME = datetime(2021, 1, 4, 0, 0, 0, tzinfo=timezone.utc)
SCENARIO_END_DT = BASE_TIME + timedelta(minutes=int(SCENARIO["time"]["total_minutes"]))


def seed_from_str(s: str) -> int:
    d = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(d[:8], "big", signed=False)


GLOBAL_SEED = seed_from_str(SYSTEM["sys"]["id"] + "|" + SCENARIO["scenario"]["id"]) & 0xFFFFFFFF
random.seed(GLOBAL_SEED)
np.random.seed(GLOBAL_SEED)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def sample_lognormal_from_p50_p95(p50: float, p95: float, rng: np.random.Generator, cap: Optional[float] = None) -> float:
    p50 = max(0.0, float(p50))
    p95 = max(0.0, float(p95))
    if p50 <= 0.0 and p95 <= 0.0:
        return 0.0
    if p50 <= 0.0:
        p50 = p95 / 4.0 if p95 > 0.0 else 1.0
    if p95 <= 0.0:
        p95 = p50 * 4.0
    if p95 < p50:
        p95 = p50
    mu = math.log(max(p50, 1e-9))
    if p95 == p50:
        sigma = 0.001
    else:
        sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
        sigma = max(0.001, sigma)
    x = float(rng.lognormal(mean=mu, sigma=sigma))
    if cap is not None:
        x = min(x, float(cap))
    return max(0.0, x)


def gen_hex(rng: np.random.Generator, n: int) -> str:
    b = rng.integers(0, 256, size=(n + 1) // 2, dtype=np.uint8).tobytes()
    s = b.hex()
    return s[:n]


def gen_uuid(rng: np.random.Generator) -> str:
    h = gen_hex(rng, 32)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def gen_value(domain: Dict[str, Any], rng: np.random.Generator) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            return round(lo, 3)
        return round(float(rng.uniform(lo, hi)), 3)
    if k == "ch":
        return str(v[int(rng.integers(0, len(v)))])
    if k == "hex":
        return gen_hex(rng, int(v))
    if k == "uuid":
        return gen_uuid(rng)
    if k == "ip":
        return "192.0.2.1"
    if k == "str":
        return "value"
    return ""


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    comps = {c["id"]: c for c in system["components"]}
    logs: Dict[str, Any] = {}
    for cid, c in comps.items():
        for lid, ldef in c.get("logs", {}).items():
            logs[f"{cid}.{lid}"] = ldef
    return comps, logs


COMPONENTS, LOG_DEFS = build_indices(SYSTEM)

FLOWS_BY_STATE: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ("n", "f"):
    for fd in SYSTEM["flows"][st]["req"]:
        FLOWS_BY_STATE[st][fd["id"]] = fd


def derive_failure_intervals(scenario: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    f_phase = scenario["phases"]["f"]
    start_min = scenario["time"]["phases"]["f"]["start_min"]
    end_min = scenario["time"]["phases"]["f"]["end_min"]
    events = sorted(f_phase["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [start_min] + [e["at_min"] for e in events if start_min <= e["at_min"] < end_min] + [end_min]
    boundaries = sorted(set(boundaries))
    boundaries = [b for b in boundaries if start_min <= b <= end_min]
    boundaries.sort()

    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}
    ev_by_at: Dict[int, List[Dict[str, Any]]] = {}
    for ev in events:
        ev_by_at.setdefault(ev["at_min"], []).append(ev)

    intervals: List[Dict[str, Any]] = []
    one_shots: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        b0 = boundaries[i]
        b1 = boundaries[i + 1]
        for ev in ev_by_at.get(b0, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
            for os in ev.get("one_shots", []) or []:
                one_shots.append({"at_min": b0, "ref": os["ref"], "count": int(os["count"]), "hosts": list(os.get("hosts") or [])})
        intervals.append(
            {
                "state": "f",
                "start_min": b0,
                "end_min": b1,
                "start_dt": BASE_TIME + timedelta(minutes=b0),
                "end_dt": BASE_TIME + timedelta(minutes=b1),
                "rate_mult": deepcopy(rate_mult),
                "lat_mult": deepcopy(lat_mult),
            }
        )
    return intervals, one_shots


def allocate_count(expected: float, key: str) -> int:
    expected = max(0.0, float(expected))
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0.0:
        return base
    rng = np.random.default_rng(seed_from_str(f"count:{key}"))
    return base + (1 if float(rng.random()) < frac else 0)


def schedule_times(count: int, start_dt: datetime, end_dt: datetime, key: str) -> List[datetime]:
    if count <= 0:
        return []
    total_sec = (end_dt - start_dt).total_seconds()
    if total_sec <= 0:
        return [start_dt for _ in range(count)]
    rng = np.random.default_rng(seed_from_str(f"times:{key}"))
    base_step = total_sec / count
    jitter = min(0.4, base_step / 10.0)
    out: List[datetime] = []
    for i in range(count):
        t = (i + 0.5) / count
        sec = t * total_sec
        j = float(rng.uniform(-jitter, jitter))
        sec = clamp(sec + j, 0.0, total_sec - 1e-6)
        out.append(start_dt + timedelta(seconds=sec))
    return out


def choose_host_for_component(component_id: str, rng: np.random.Generator) -> str:
    hosts = COMPONENTS[component_id].get("hosts") or []
    if not hosts:
        return ""
    return str(hosts[int(rng.integers(0, len(hosts)))])


def merge_var_domains(log_def: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    doms = {}
    for k, v in (log_def.get("vars") or {}).items():
        doms[k] = v
    stv = log_def.get("state_vars") or {}
    if state in stv:
        for k, v in stv[state].items():
            doms[k] = v
    return doms


def domain_bounds(component_id: str, log_id: str, state: str, field: str) -> Optional[Tuple[str, float, float]]:
    log_def = LOG_DEFS[f"{component_id}.{log_id}"]
    doms = merge_var_domains(log_def, state)
    if field not in doms:
        return None
    dom = doms[field]
    k = dom.get("k")
    v = dom.get("v")
    if k in ("i", "f") and isinstance(v, list) and len(v) == 2:
        lo = float(v[0])
        hi = float(v[1])
        return k, lo, hi
    return None


def clamp_to_domain(component_id: str, log_id: str, state: str, field: str, value: Any) -> Any:
    b = domain_bounds(component_id, log_id, state, field)
    if b is None:
        return value
    k, lo, hi = b
    if k == "i":
        try:
            iv = int(round(float(value)))
        except Exception:
            iv = int(lo)
        return int(clamp(iv, lo, hi))
    if k == "f":
        try:
            fv = float(value)
        except Exception:
            fv = lo
        return round(float(clamp(fv, lo, hi)), 3)
    return value


def render_message(component_id: str, log_id: str, state: str, overrides: Dict[str, Any], rng: np.random.Generator) -> Tuple[str, str]:
    log_def = LOG_DEFS[f"{component_id}.{log_id}"]
    doms = merge_var_domains(log_def, state)
    values: Dict[str, Any] = {}
    for k, dom in doms.items():
        if k in overrides:
            values[k] = overrides[k]
        else:
            values[k] = gen_value(dom, rng)
    for k, v in overrides.items():
        if k not in values:
            values[k] = v
    for k, v in list(values.items()):
        if isinstance(v, float):
            values[k] = round(v, 3)
    msg = log_def["msg"].format(**values)
    lvl = log_def["lvl"]
    return lvl, msg


def parse_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def web_slot_from_host(host: str) -> int:
    try:
        if host.startswith("web-"):
            return int(host.split("-", 1)[1])
    except Exception:
        pass
    return 1


def synthetic_err_rate_by_time(start_min: int) -> float:
    if start_min < 25:
        return 0.12
    if start_min < 33:
        return 0.65
    return 0.22


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str, seq: int) -> None:
    rows.append({"timestamp_dt": ts, "level": level, "message": message, "trace_id": trace_id, "service": service or "", "host": host or "", "seq": seq})


E2E_DUR_LOGS = {
    ("web_tier", "msg_send_respond_200"),
    ("web_tier", "msg_send_respond_503"),
    ("web_tier", "ping_respond_200"),
    ("web_tier", "ping_respond_503"),
    ("edge_lb", "access_message_200"),
    ("edge_lb", "access_message_503"),
    ("edge_lb", "access_ping_200"),
    ("edge_lb", "access_ping_503"),
    ("external_monitor", "ping_check_ok"),
    ("external_monitor", "ping_check_fail"),
    ("monitoring_ui", "ui_request_ok"),
    ("monitoring_ui", "ui_request_fail"),
}


def conservative_chain_ms_upper(flow_def: Dict[str, Any], state: str, interval: Dict[str, Any]) -> float:
    """
    Conservative upper bound on chain duration, used only to avoid scheduling starts too late when we *do* want to
    keep the bulk of chains near the modeled scenario window.

    This specifically addresses verifier feedback: using a p50-based expected duration can underestimate long-tail steps
    and, combined with end-of-scenario dropping, can truncate chains.
    """
    flow_id = flow_def["id"]
    latm = {"p50": 1.0, "p95": 1.0}
    if state == "f":
        latm = interval["lat_mult"].get(flow_id, latm)

    elapsed = 0.0
    for idx, (_p50, p95) in enumerate(flow_def["latency_ms"]):
        p95s = float(p95) * float(latm.get("p95", 1.0))
        step_max = 2.8 * p95s if p95s > 0 else 0.0  # matches simulate_flow_instance cap behavior

        comp_id, log_id = parse_ref(flow_def["emit"][idx])
        log_def = LOG_DEFS[f"{comp_id}.{log_id}"]
        doms = merge_var_domains(log_def, state)

        # Clamp-like maxima for duration-bearing steps.
        if "waited_ms" in doms and isinstance(doms["waited_ms"].get("v"), list):
            step_max = min(step_max, float(doms["waited_ms"]["v"][1]))
        if comp_id == "backend_api" and log_id == "msg_send_rpc_ok" and "dur_ms" in doms:
            step_max = min(step_max, float(doms["dur_ms"]["v"][1]))
        if comp_id == "provision_service" and log_id == "provision_ok" and "dur_s" in doms:
            step_max = min(step_max, float(doms["dur_s"]["v"][1]) * 1000.0)

        # If the log carries an end-to-end dur_ms domain, overall elapsed at that point is bounded by its hi.
        if (comp_id, log_id) in E2E_DUR_LOGS:
            b = domain_bounds(comp_id, log_id, state, "dur_ms")
            if b is not None:
                _, _lo_e2e, hi_e2e = b
                step_max = min(step_max, max(0.0, float(hi_e2e) - elapsed))

        elapsed += max(0.0, step_max)

    return elapsed


def simulate_flow_instance(flow_def: Dict[str, Any], state: str, start_dt: datetime, interval: Dict[str, Any], inst_key: str, global_seq_start: int, rows: List[Dict[str, Any]]) -> int:
    rng = np.random.default_rng(seed_from_str(f"flowinst:{inst_key}"))
    flow_id = flow_def["id"]

    trace_id = ""
    if SYSTEM["tracing"]["on"] and bool(flow_def.get("trace")):
        trace_id = gen_hex(rng, 32)

    emitting_components = [parse_ref(r)[0] for r in flow_def["emit"]]
    comp_host: Dict[str, str] = {}
    for cid in sorted(set(emitting_components)):
        comp_host[cid] = choose_host_for_component(cid, rng)

    web_host = comp_host.get("web_tier", "web-1")
    slot = web_slot_from_host(web_host)

    req_id = gen_uuid(rng)
    iid = gen_hex(rng, 8)

    latm = {"p50": 1.0, "p95": 1.0}
    if state == "f":
        latm = interval["lat_mult"].get(flow_id, latm)

    ref_list = flow_def["emit"]

    # Ensure waited_ms never exceeds timeout_ms for deadline-bounded timeout flow.
    timeout_ms_bound: Optional[int] = None
    if flow_id == "message_send_fail_timeout":
        base = 6000 + (int(iid, 16) % 2001)  # 6000..8000
        timeout_ms_bound = int(clamp_to_domain("web_tier", "msg_send_backend_call", state, "timeout_ms", base))

    timestamps: List[datetime] = []
    t = start_dt
    for idx, (p50, p95) in enumerate(flow_def["latency_ms"]):
        p50s = float(p50) * float(latm.get("p50", 1.0))
        p95s = float(p95) * float(latm.get("p95", 1.0))
        cap = 2.8 * p95s if p95s > 0 else None

        comp_id, log_id = parse_ref(ref_list[idx])
        log_def = LOG_DEFS[f"{comp_id}.{log_id}"]
        doms = merge_var_domains(log_def, state)

        if comp_id == "web_tier" and log_id == "msg_send_backend_timeout" and timeout_ms_bound is not None:
            d = float(int(timeout_ms_bound))
            if "waited_ms" in doms:
                lo = float(doms["waited_ms"]["v"][0])
                hi = float(doms["waited_ms"]["v"][1])
                d = clamp(d, lo, min(hi, float(timeout_ms_bound)))
        else:
            if "waited_ms" in doms:
                lo = float(doms["waited_ms"]["v"][0])
                hi = float(doms["waited_ms"]["v"][1])
                cap2 = min(cap if cap is not None else hi, hi)
                d = sample_lognormal_from_p50_p95(p50s, p95s, rng, cap=cap2)
                d = clamp(d, lo, hi)
            elif comp_id == "backend_api" and log_id == "msg_send_rpc_ok" and "dur_ms" in doms:
                lo = float(doms["dur_ms"]["v"][0])
                hi = float(doms["dur_ms"]["v"][1])
                cap2 = min(cap if cap is not None else hi, hi)
                d = sample_lognormal_from_p50_p95(p50s, p95s, rng, cap=cap2)
                d = clamp(d, lo, hi)
            elif comp_id == "provision_service" and log_id == "provision_ok" and "dur_s" in doms:
                lo_ms = float(doms["dur_s"]["v"][0]) * 1000.0
                hi_ms = float(doms["dur_s"]["v"][1]) * 1000.0
                cap2 = min(cap if cap is not None else hi_ms, hi_ms)
                d = sample_lognormal_from_p50_p95(p50s, p95s, rng, cap=cap2)
                d = clamp(d, lo_ms, hi_ms)
            else:
                d = sample_lognormal_from_p50_p95(p50s, p95s, rng, cap=cap)

        # Constrain per-log elapsed dur_ms fields to be compatible with domains.
        if (comp_id, log_id) in E2E_DUR_LOGS and domain_bounds(comp_id, log_id, state, "dur_ms") is not None:
            _, lo_e2e, hi_e2e = domain_bounds(comp_id, log_id, state, "dur_ms")  # type: ignore[misc]
            elapsed_prev = (t - start_dt).total_seconds() * 1000.0
            min_needed = max(0.0, lo_e2e - elapsed_prev)
            max_allowed = max(0.0, hi_e2e - elapsed_prev)
            if max_allowed >= min_needed:
                d = clamp(d, min_needed, max_allowed)
            else:
                d = min(d, max_allowed)

        t = t + timedelta(milliseconds=float(d))
        timestamps.append(t)

    def elapsed_ms(at: datetime) -> int:
        return int(round((at - start_dt).total_seconds() * 1000.0))

    seq = global_seq_start

    idx_backend_call = None
    idx_backend_timeout = None
    idx_rpc_recv = None
    idx_rpc_ok = None
    for i, ref in enumerate(ref_list):
        if ref == "web_tier.msg_send_backend_call":
            idx_backend_call = i
        elif ref == "web_tier.msg_send_backend_timeout":
            idx_backend_timeout = i
        elif ref == "backend_api.msg_send_rpc_recv":
            idx_rpc_recv = i
        elif ref == "backend_api.msg_send_rpc_ok":
            idx_rpc_ok = i

    waited_ms_val: Optional[int] = None
    if idx_backend_call is not None and idx_backend_timeout is not None:
        waited_ms_val = int(round((timestamps[idx_backend_timeout] - timestamps[idx_backend_call]).total_seconds() * 1000.0))
        if timeout_ms_bound is not None:
            waited_ms_val = min(waited_ms_val, int(timeout_ms_bound))
        waited_ms_val = int(clamp_to_domain("web_tier", "msg_send_backend_timeout", state, "waited_ms", waited_ms_val))

    rpc_dur_ms_val: Optional[int] = None
    if idx_rpc_recv is not None and idx_rpc_ok is not None:
        rpc_dur_ms_val = int(round((timestamps[idx_rpc_ok] - timestamps[idx_rpc_recv]).total_seconds() * 1000.0))
        rpc_dur_ms_val = int(clamp_to_domain("backend_api", "msg_send_rpc_ok", state, "dur_ms", rpc_dur_ms_val))

    for i, ref in enumerate(ref_list):
        comp_id, log_id = parse_ref(ref)
        comp = COMPONENTS[comp_id]
        host = comp_host.get(comp_id, "")
        service = comp.get("svc") or ""
        ts = timestamps[i]
        overrides: Dict[str, Any] = {}

        if trace_id:
            overrides["trace_id"] = trace_id

        if "req_id" in merge_var_domains(LOG_DEFS[f"{comp_id}.{log_id}"], state):
            overrides["req_id"] = req_id

        if "iid" in merge_var_domains(LOG_DEFS[f"{comp_id}.{log_id}"], state):
            overrides["iid"] = iid

        if comp_id == "edge_lb" and log_id in ("access_message_200", "access_message_503", "access_ping_200", "access_ping_503"):
            overrides["slot"] = slot

        if (comp_id, log_id) in E2E_DUR_LOGS and domain_bounds(comp_id, log_id, state, "dur_ms") is not None:
            overrides["dur_ms"] = clamp_to_domain(comp_id, log_id, state, "dur_ms", elapsed_ms(ts))

        if comp_id == "external_monitor" and log_id in ("ping_check_ok", "ping_check_fail"):
            overrides["dur_ms"] = clamp_to_domain(comp_id, log_id, state, "dur_ms", elapsed_ms(ts))

        if comp_id == "backend_api" and log_id == "msg_send_rpc_ok" and rpc_dur_ms_val is not None:
            overrides["dur_ms"] = rpc_dur_ms_val

        if comp_id == "web_tier" and log_id == "msg_send_backend_timeout" and waited_ms_val is not None:
            overrides["waited_ms"] = waited_ms_val
            overrides["err"] = "deadline_exceeded" if int(iid, 16) % 5 != 0 else "connect_timeout"

        if comp_id == "web_tier" and log_id == "msg_send_backend_call":
            if flow_id == "message_send_fail_timeout" and timeout_ms_bound is not None:
                overrides["timeout_ms"] = int(timeout_ms_bound)
            elif state == "n":
                overrides["timeout_ms"] = int(clamp_to_domain(comp_id, log_id, state, "timeout_ms", 1800))
            else:
                timeout_guess = int(2000 * float(latm.get("p50", 1.0)) + 1000)
                overrides["timeout_ms"] = int(clamp_to_domain(comp_id, log_id, state, "timeout_ms", timeout_guess))

        if comp_id == "web_tier" and log_id == "msg_send_respond_200":
            overrides["bytes"] = int(rng.integers(200, 8001))

        if comp_id == "web_tier" and log_id == "msg_send_respond_503":
            overrides["err"] = "upstream_timeout" if flow_id == "message_send_fail_timeout" else "overload"

        if comp_id == "web_tier" and log_id == "ping_respond_503":
            overrides["err"] = "overload"

        if comp_id == "monitoring_ui" and log_id == "ui_request_fail":
            overrides["err"] = "upstream_timeout"

        if comp_id == "external_monitor" and log_id == "ping_check_fail":
            overrides["err_rate"] = float(clamp_to_domain(comp_id, log_id, state, "err_rate", synthetic_err_rate_by_time(interval["start_min"])))

        if comp_id == "provision_service" and log_id == "provision_ok":
            idx_begin = None
            for j, r2 in enumerate(ref_list):
                if r2 == "provision_service.provision_begin":
                    idx_begin = j
                    break
            if idx_begin is not None:
                dur_s = int(round((ts - timestamps[idx_begin]).total_seconds()))
                overrides["dur_s"] = int(clamp_to_domain(comp_id, log_id, state, "dur_s", dur_s))

        level, message = render_message(comp_id, log_id, state, overrides, rng)
        emit_row(rows, ts, level, message, trace_id if flow_def.get("trace") else "", service, host, seq)
        seq += 1

    return seq


def simulate_background(interval: Dict[str, Any], rows: List[Dict[str, Any]], global_seq_start: int) -> int:
    state = interval["state"]
    seq = global_seq_start
    duration_min = (interval["end_dt"] - interval["start_dt"]).total_seconds() / 60.0

    for comp_id in sorted(COMPONENTS.keys()):
        comp = COMPONENTS[comp_id]
        beh = comp.get("beh", {}).get(state, {})
        for emit_def in beh.get("emit", []) or []:
            log_id = emit_def["id"]
            per_min = float(emit_def["per_min"])
            scope = emit_def.get("scope") or "per_host"
            mult = 1.0
            if state == "f":
                mult = float(interval["rate_mult"].get(f"{comp_id}.{log_id}", 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0.0:
                continue

            if scope == "global":
                expected = eff_per_min * duration_min
                count = allocate_count(expected, f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:global")
                times = schedule_times(count, interval["start_dt"], interval["end_dt"], f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:global")
                hosts = comp.get("hosts") or []
                for i, ts in enumerate(times):
                    rng = np.random.default_rng(seed_from_str(f"bgr:{comp_id}.{log_id}:{interval['start_min']}:{i}"))
                    host = hosts[i % len(hosts)] if hosts else ""
                    lvl, msg = render_message(comp_id, log_id, state, overrides={}, rng=rng)
                    emit_row(rows, ts, lvl, msg, "", comp.get("svc") or "", host, seq)
                    seq += 1
            else:
                hosts = comp.get("hosts") or []
                if not hosts:
                    expected = eff_per_min * duration_min
                    count = allocate_count(expected, f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:nohost")
                    times = schedule_times(count, interval["start_dt"], interval["end_dt"], f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:nohost")
                    for i, ts in enumerate(times):
                        rng = np.random.default_rng(seed_from_str(f"bgr:{comp_id}.{log_id}:{interval['start_min']}:nohost:{i}"))
                        lvl, msg = render_message(comp_id, log_id, state, overrides={}, rng=rng)
                        emit_row(rows, ts, lvl, msg, "", comp.get("svc") or "", "", seq)
                        seq += 1
                else:
                    for h in hosts:
                        expected = eff_per_min * duration_min
                        count = allocate_count(expected, f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:{h}")
                        times = schedule_times(count, interval["start_dt"], interval["end_dt"], f"bg:{interval['start_min']}:{interval['end_min']}:{comp_id}.{log_id}:{h}")
                        for i, ts in enumerate(times):
                            rng = np.random.default_rng(seed_from_str(f"bgr:{comp_id}.{log_id}:{interval['start_min']}:{h}:{i}"))
                            lvl, msg = render_message(comp_id, log_id, state, overrides={}, rng=rng)
                            emit_row(rows, ts, lvl, msg, "", comp.get("svc") or "", h, seq)
                            seq += 1
    return seq


def simulate_flows_for_interval(interval: Dict[str, Any], rows: List[Dict[str, Any]], global_seq_start: int) -> int:
    state = interval["state"]
    seq = global_seq_start
    duration_min = (interval["end_dt"] - interval["start_dt"]).total_seconds() / 60.0

    flows = SYSTEM["flows"][state]["req"]
    for flow_def in sorted(flows, key=lambda x: x["id"]):
        flow_id = flow_def["id"]
        mult = 1.0
        if state == "f":
            mult = float(interval["rate_mult"].get(flow_id, 1.0))
        eff_rpm = float(flow_def["rpm"]) * mult
        if eff_rpm <= 0.0:
            continue
        expected = eff_rpm * duration_min
        count = allocate_count(expected, f"flowcount:{interval['start_min']}:{interval['end_min']}:{flow_id}")

        # Conservative chain duration estimate to avoid scheduling starts too late when we want to keep most logs in-window.
        chain_ms = conservative_chain_ms_upper(flow_def, state, interval)
        latest_by_scenario = SCENARIO_END_DT - timedelta(milliseconds=chain_ms)
        end_for_starts = min(interval["end_dt"], latest_by_scenario)
        if end_for_starts < interval["start_dt"]:
            end_for_starts = interval["start_dt"]

        starts = schedule_times(count, interval["start_dt"], end_for_starts, f"flowstarts:{interval['start_min']}:{interval['end_min']}:{flow_id}")

        for i, st_dt in enumerate(starts):
            inst_key = f"{flow_id}:{interval['start_min']}:{i}:{int(st_dt.timestamp() * 1000)}"
            seq = simulate_flow_instance(flow_def, state, st_dt, interval, inst_key, seq, rows)
    return seq


def emit_one_shots(one_shots: List[Dict[str, Any]], rows: List[Dict[str, Any]], global_seq_start: int) -> int:
    seq = global_seq_start
    for idx, os in enumerate(one_shots):
        at_min = int(os["at_min"])
        ref = os["ref"]
        count = int(os["count"])
        hosts_allowed = os.get("hosts") or []
        comp_id, log_id = parse_ref(ref)
        comp = COMPONENTS[comp_id]
        service = comp.get("svc") or ""
        base_dt = BASE_TIME + timedelta(minutes=at_min)
        for j in range(count):
            rng = np.random.default_rng(seed_from_str(f"oneshot:{ref}:{at_min}:{j}:{idx}"))
            jitter_ms = int(rng.integers(0, 9000))
            ts = base_dt + timedelta(milliseconds=jitter_ms)

            if hosts_allowed:
                host = str(hosts_allowed[min(j, len(hosts_allowed) - 1)])
            else:
                host = choose_host_for_component(comp_id, rng)

            overrides: Dict[str, Any] = {}
            if ref == "external_monitor.page_sent":
                overrides["err_rate"] = float(clamp_to_domain(comp_id, log_id, "f", "err_rate", 0.12))
            if ref == "edge_lb.panic_mode_enabled":
                overrides["threshold_pct"] = clamp_to_domain(comp_id, log_id, "f", "threshold_pct", 70)
            if ref == "autoscaler.asg_max_increased":
                overrides["new_max"] = clamp_to_domain(comp_id, log_id, "f", "new_max", 160)

            lvl, msg = render_message(comp_id, log_id, "f", overrides, rng)
            emit_row(rows, ts, lvl, msg, "", service, host, seq)
            seq += 1
    return seq


def main() -> None:
    rows: List[Dict[str, Any]] = []
    seq = 0

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    normal_interval = {
        "state": "n",
        "start_min": n_start,
        "end_min": n_end,
        "start_dt": BASE_TIME + timedelta(minutes=n_start),
        "end_dt": BASE_TIME + timedelta(minutes=n_end),
        "rate_mult": {},
        "lat_mult": {},
    }

    seq = simulate_background(normal_interval, rows, seq)
    seq = simulate_flows_for_interval(normal_interval, rows, seq)

    failure_intervals, one_shots = derive_failure_intervals(SCENARIO)
    for interval in failure_intervals:
        seq = simulate_background(interval, rows, seq)
        seq = simulate_flows_for_interval(interval, rows, seq)

    seq = emit_one_shots(one_shots, rows, seq)

    df = pd.DataFrame(rows)

    # Important: do NOT drop rows beyond SCENARIO_END_DT. Flow chains can legitimately spill past the scenario end
    # due to modeled latencies; dropping tail rows can truncate chains and violate flow ordering/completeness.

    df.sort_values(["timestamp_dt", "seq"], inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"log volume {len(df)} out of target range [20000, 100000]")
    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV column order mismatch")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
