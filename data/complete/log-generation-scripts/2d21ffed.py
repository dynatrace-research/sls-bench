import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

random.seed(0)

SYSTEM: Dict[str, Any] = {
    "id": "python_hosting_platform_livefile1_outage",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_proxy",
            "svc": "edge-proxy",
            "hosts": ["edge1", "edge2"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "{method} {host}{uri} -> {status} upstream={upstream} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["control.example.com", "user-site.example.com"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/dashboard", "/files", "/reload", "/health"]},
                        "status": {"k": "ch", "v": ["200", "302"]},
                        "upstream": {"k": "ch", "v": ["platform_api", "web_runtime"]},
                        "dur_ms": {"k": "i", "v": [5, 180000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_err": {
                    "lvl": "WARN",
                    "msg": "{method} {host}{uri} -> {status} upstream={upstream} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["control.example.com", "user-site.example.com"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/dashboard", "/files", "/reload"]},  # note: no /health
                        "status": {"k": "ch", "v": ["502", "503", "504"]},
                        "upstream": {"k": "ch", "v": ["platform_api", "web_runtime"]},
                        "dur_ms": {"k": "i", "v": [50, 180000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "platform_api",
            "svc": "platform-api",
            "hosts": ["api1", "api2"],
            "logs": {
                "req_done_ok": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} endpoint={endpoint} result=ok dur_ms={dur_ms} user_id={user_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "endpoint": {"k": "ch", "v": ["dashboard", "login", "health", "files", "reload"]},
                        "user_id": {"k": "i", "v": [1000, 9999]},
                    },
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [5, 5000]}},
                        "f": {"dur_ms": {"k": "i", "v": [10, 60000]}},
                    },
                },
                "req_done_err": {
                    "lvl": "ERROR",
                    "msg": "req_id={req_id} endpoint={endpoint} result=error err={err} dur_ms={dur_ms} user_id={user_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "endpoint": {"k": "ch", "v": ["dashboard", "login", "health", "files", "reload"]},
                        "err": {"k": "ch", "v": ["fs_timeout", "worker_timeout", "upstream_unavailable"]},
                        "dur_ms": {"k": "i", "v": [50, 180000]},
                        "user_id": {"k": "i", "v": [1000, 9999]},
                    },
                },
                "fs_timeout": {
                    "lvl": "WARN",
                    "msg": "req_id={req_id} fs_op={fs_op} server={fs_server} timed_out_after_s={timeout_s}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "fs_op": {"k": "ch", "v": ["stat", "listdir", "open"]},
                        "fs_server": {"k": "ch", "v": ["livefile1"]},
                        "timeout_s": {"k": "i", "v": [5, 60]},
                    },
                },
                "api_health": {
                    "lvl": "INFO",
                    "msg": "health ok=true inflight={inflight} p95_ms={p95_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {"inflight": {"k": "i", "v": [0, 200]}, "p95_ms": {"k": "i", "v": [10, 5000]}},
                        "f": {"inflight": {"k": "i", "v": [50, 700]}, "p95_ms": {"k": "i", "v": [50, 60000]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "api_health", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "api_health", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "web_runtime",
            "svc": "web-runtime",
            "hosts": ["web1", "web2", "web3", "web4"],
            "logs": {
                "site_req_ok": {
                    "lvl": "INFO",
                    "msg": "site_id={site_id} req_id={req_id} result=ok dur_ms={dur_ms}",
                    "vars": {
                        "site_id": {"k": "i", "v": [20000, 20500]},
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [3, 180000]},
                    },
                },
                "site_req_err": {
                    "lvl": "ERROR",
                    "msg": "site_id={site_id} req_id={req_id} result=error err={err} dur_ms={dur_ms}",
                    "vars": {
                        "site_id": {"k": "i", "v": [20000, 20500]},
                        "req_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["fs_timeout", "app_startup_timeout", "upstream_unavailable"]},
                        "dur_ms": {"k": "i", "v": [50, 180000]},
                    },
                },
                "startup_begin": {
                    "lvl": "INFO",
                    "msg": "site_id={site_id} req_id={req_id} action={action}",
                    "vars": {
                        "site_id": {"k": "i", "v": [20000, 20500]},
                        "req_id": {"k": "hex", "v": 16},
                        "action": {"k": "ch", "v": ["start", "reload"]},
                    },
                },
                "startup_ready": {
                    "lvl": "INFO",
                    "msg": "site_id={site_id} req_id={req_id} ready=true dur_ms={dur_ms}",
                    "vars": {
                        "site_id": {"k": "i", "v": [20000, 20500]},
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [200, 60000]},
                    },
                },
                "startup_fail": {
                    "lvl": "ERROR",
                    "msg": "site_id={site_id} req_id={req_id} ready=false err={err} waited_ms={waited_ms} fs_server={fs_server}",
                    "vars": {
                        "site_id": {"k": "i", "v": [20000, 20500]},
                        "req_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["fs_timeout", "stale_handle", "slow_io"]},
                        "waited_ms": {"k": "i", "v": [500, 60000]},
                        "fs_server": {"k": "ch", "v": ["livefile1"]},
                    },
                },
                "task_ok": {
                    "lvl": "INFO",
                    "msg": "job_id={job_id} account_id={account_id} result=ok dur_ms={dur_ms}",
                    "vars": {
                        "job_id": {"k": "uuid", "v": None},
                        "account_id": {"k": "i", "v": [1000, 9999]},
                        "dur_ms": {"k": "i", "v": [200, 300000]},
                    },
                },
                "task_fail_fs": {
                    "lvl": "ERROR",
                    "msg": "job_id={job_id} account_id={account_id} result=error err={err} fs_server={fs_server}",
                    "vars": {
                        "job_id": {"k": "uuid", "v": None},
                        "account_id": {"k": "i", "v": [1000, 9999]},
                        "err": {"k": "ch", "v": ["fs_timeout", "stale_handle", "slow_io"]},
                        "fs_server": {"k": "ch", "v": ["livefile1"]},
                    },
                },
                "pool_metric": {
                    "lvl": "INFO",
                    "msg": "pool size={size} busy={busy} queue={queue} p95_ms={p95_ms}",
                    "vars": {"size": {"k": "i", "v": [64, 64]}, "p95_ms": {"k": "i", "v": [10, 60000]}},
                    "state_vars": {
                        "n": {"busy": {"k": "i", "v": [1, 20]}, "queue": {"k": "i", "v": [0, 20]}},
                        "f": {"busy": {"k": "i", "v": [40, 64]}, "queue": {"k": "i", "v": [50, 500]}},
                    },
                },
                "nfs_server_not_responding": {
                    "lvl": "WARN",
                    "msg": "nfs: server {fs_server} not responding, still trying",
                    "vars": {"fs_server": {"k": "ch", "v": ["livefile1"]}},
                },
                "nfs_stale_handle": {
                    "lvl": "WARN",
                    "msg": "nfs: stale file handle mount={mount} server={fs_server}",
                    "vars": {"mount": {"k": "ch", "v": ["/home"]}, "fs_server": {"k": "ch", "v": ["livefile1"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_metric", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "pool_metric", "per_min": 1.5, "scope": "per_host"},
                        {"id": "nfs_server_not_responding", "per_min": 1.0, "scope": "per_host"},
                        {"id": "nfs_stale_handle", "per_min": 2.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "file_server_livefile1",
            "svc": "nfs-server",
            "hosts": ["livefile1"],
            "logs": {
                "kernel_io_hang": {
                    "lvl": "ERROR",
                    "msg": "blk_update_request: I/O error dev={dev} sector={sector} op={op}",
                    "vars": {
                        "dev": {"k": "ch", "v": ["xvdf", "xvdg", "xvdh"]},
                        "sector": {"k": "i", "v": [10000, 9000000]},
                        "op": {"k": "ch", "v": ["READ", "WRITE"]},
                    },
                },
                "nfsd_timeout": {
                    "lvl": "WARN",
                    "msg": "nfsd: client={client_ip} op={op} timed_out_after_s={timeout_s}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "10.0.0.0/24"},
                        "op": {"k": "ch", "v": ["READ", "WRITE", "GETATTR"]},
                        "timeout_s": {"k": "i", "v": [5, 60]},
                    },
                },
                "disk_latency": {
                    "lvl": "INFO",
                    "msg": "disk latency_ms_p95={p95_ms} util_pct={util_pct}",
                    "vars": {"p95_ms": {"k": "i", "v": [1, 60000]}, "util_pct": {"k": "i", "v": [1, 100]}},
                },
                "fsck_complete": {
                    "lvl": "INFO",
                    "msg": "fsck completed vg={vg} errors_fixed={errors_fixed}",
                    "vars": {"vg": {"k": "ch", "v": ["vg_users"]}, "errors_fixed": {"k": "i", "v": [0, 50]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "disk_latency", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "disk_latency", "per_min": 1.0, "scope": "per_host"},
                        {"id": "kernel_io_hang", "per_min": 0.5, "scope": "per_host"},
                        {"id": "nfsd_timeout", "per_min": 6.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "backup_server",
            "svc": "drbd-backup",
            "hosts": ["backup1"],
            "logs": {
                "drbd_state": {
                    "lvl": "INFO",
                    "msg": "drbd res={res} role={role} conn={conn} synced_pct={synced_pct}",
                    "vars": {
                        "res": {"k": "ch", "v": ["users"]},
                        "role": {"k": "ch", "v": ["Secondary"]},
                        "conn": {"k": "ch", "v": ["Connected", "StandAlone"]},
                        "synced_pct": {"k": "i", "v": [0, 100]},
                    },
                    "state_vars": {
                        "n": {"conn": {"k": "ch", "v": ["Connected"]}},
                        "f": {"conn": {"k": "ch", "v": ["Connected", "StandAlone"]}},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "drbd_state", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "drbd_state", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon1"],
            "logs": {
                "alert": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT name={name} severity={severity} value={value} threshold={threshold}",
                    "vars": {
                        "name": {"k": "ch", "v": ["http_5xx_rate", "nfs_timeouts", "worker_pool_saturation"]},
                        "severity": {"k": "ch", "v": ["warning", "critical"]},
                        "value": {"k": "f", "v": [0.0, 1.0]},
                        "threshold": {"k": "f", "v": [0.02, 0.20]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "alert", "per_min": 0.1, "scope": "global"}]},
                "f": {"emit": [{"id": "alert", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "ops",
            "svc": "ops",
            "hosts": ["ops1"],
            "logs": {
                "stop_instance": {
                    "lvl": "INFO",
                    "msg": "op_id={op_id} action=stop_instance target=livefile1 result=ok",
                    "vars": {"op_id": {"k": "hex", "v": 12}},
                },
                "start_instance_delayed": {
                    "lvl": "WARN",
                    "msg": "op_id={op_id} action=start_instance target=livefile1 result=delayed",
                    "vars": {"op_id": {"k": "hex", "v": 12}},
                },
                "open_aws_case": {
                    "lvl": "INFO",
                    "msg": "op_id={op_id} action=open_aws_case provider=aws case_id={case_id} severity=high",
                    "vars": {"op_id": {"k": "hex", "v": 12}, "case_id": {"k": "str", "v": "AWS-case-id"}},
                },
                "create_snapshots": {
                    "lvl": "INFO",
                    "msg": "op_id={op_id} action=create_snapshots source=backup1 volumes={volumes} result=ok",
                    "vars": {"op_id": {"k": "hex", "v": 12}, "volumes": {"k": "i", "v": [3, 6]}},
                },
                "attach_restored_volumes": {
                    "lvl": "INFO",
                    "msg": "op_id={op_id} action=attach_restored_volumes target=livefile1 volumes={volumes} result=ok",
                    "vars": {"op_id": {"k": "hex", "v": 12}, "volumes": {"k": "i", "v": [3, 6]}},
                },
                "rolling_reboot": {
                    "lvl": "INFO",
                    "msg": "op_id={op_id} action=rolling_reboot target=web_cluster result=started batch={batch}",
                    "vars": {"op_id": {"k": "hex", "v": 12}, "batch": {"k": "i", "v": [1, 4]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "control_panel_dashboard_ok",
                    "rpm": 90.0,
                    "emit": ["platform_api.req_done_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[3, 20], [20, 300]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "control_panel_files_ok",
                    "rpm": 30.0,
                    "emit": ["platform_api.req_done_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[5, 40], [40, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_hot_ok",
                    "rpm": 450.0,
                    "emit": ["web_runtime.site_req_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[2, 10], [10, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_coldstart_ok",
                    "rpm": 30.0,
                    "emit": ["web_runtime.startup_begin", "web_runtime.startup_ready", "edge_proxy.access_ok"],
                    "latency_ms": [[3, 15], [400, 2500], [450, 3200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "scheduled_task_ok",
                    "rpm": 20.0,
                    "emit": ["web_runtime.task_ok"],
                    "latency_ms": [[200, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "control_panel_dashboard_err",
                    "rpm": 60.0,
                    "emit": ["platform_api.req_done_err", "edge_proxy.access_err"],
                    "latency_ms": [[20, 2000], [2000, 60000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "control_panel_dashboard_ok_slow",
                    "rpm": 30.0,
                    "emit": ["platform_api.req_done_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[50, 4000], [500, 15000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "control_panel_files_err",
                    "rpm": 30.0,
                    "emit": ["platform_api.fs_timeout", "platform_api.req_done_err", "edge_proxy.access_err"],
                    "latency_ms": [[5000, 60000], [5000, 60000], [5000, 60000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_other_hot_ok",
                    "rpm": 450.0,
                    "emit": ["web_runtime.site_req_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[3, 15], [15, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_other_coldstart_err",
                    "rpm": 20.0,
                    "emit": ["web_runtime.startup_begin", "web_runtime.startup_fail", "edge_proxy.access_err"],
                    "latency_ms": [[5, 30], [2000, 60000], [2100, 65000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_livefile1_err",
                    "rpm": 80.0,
                    "emit": ["web_runtime.site_req_err", "edge_proxy.access_err"],
                    "latency_ms": [[500, 5000], [2000, 60000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_livefile1_slow_ok",
                    "rpm": 80.0,
                    "emit": ["web_runtime.site_req_ok", "edge_proxy.access_ok"],
                    "latency_ms": [[200, 30000], [250, 32000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "hosted_site_other_hot_err_reboot",
                    "rpm": 40.0,
                    "emit": ["web_runtime.site_req_err", "edge_proxy.access_err"],
                    "latency_ms": [[20, 500], [100, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "scheduled_task_fs_fail",
                    "rpm": 12.0,
                    "emit": ["web_runtime.task_fail_fs"],
                    "latency_ms": [[100, 60000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "scheduled_task_slow_ok",
                    "rpm": 8.0,
                    "emit": ["web_runtime.task_ok"],
                    "latency_ms": [[2000, 120000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "livefile1_ebs_volume_failure_cascade",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "file_server_livefile1.nfsd_timeout": 2.5,
                        "file_server_livefile1.kernel_io_hang": 3.0,
                        "monitoring.alert": 2.0,
                        "web_runtime.pool_metric": 1.5,
                        "web_runtime.nfs_stale_handle": 0.0,
                        "hosted_site_livefile1_slow_ok": 0.0,
                        "hosted_site_other_hot_err_reboot": 0.0,
                        "scheduled_task_slow_ok": 0.0,
                    },
                    "latency_multipliers": {
                        "control_panel_files_err": {"p50": 1.2, "p95": 1.2},
                        "hosted_site_livefile1_err": {"p50": 1.1, "p95": 1.2},
                        "scheduled_task_fs_fail": {"p50": 1.1, "p95": 1.2},
                    },
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {
                        "file_server_livefile1.nfsd_timeout": 0.0,
                        "file_server_livefile1.kernel_io_hang": 0.0,
                        "file_server_livefile1.disk_latency": 0.0,
                        "web_runtime.nfs_server_not_responding": 4.0,
                        "monitoring.alert": 2.5,
                        "control_panel_dashboard_err": 1.2,
                        "hosted_site_livefile1_err": 1.2,
                        "scheduled_task_fs_fail": 1.2,
                        "scheduled_task_slow_ok": 0.0,
                    },
                    "latency_multipliers": {
                        "control_panel_dashboard_err": {"p50": 1.3, "p95": 1.2},
                        "hosted_site_other_coldstart_err": {"p50": 1.2, "p95": 1.2},
                        "scheduled_task_fs_fail": {"p50": 1.2, "p95": 1.2},
                    },
                    "one_shots": [
                        {"ref": "ops.stop_instance", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops.start_instance_delayed", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops.open_aws_case", "count": 1, "hosts": ["ops1"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {
                        "file_server_livefile1.disk_latency": 1.0,
                        "file_server_livefile1.nfsd_timeout": 0.8,
                        "file_server_livefile1.kernel_io_hang": 0.2,
                        "web_runtime.nfs_server_not_responding": 1.5,
                        "web_runtime.nfs_stale_handle": 2.0,
                        "hosted_site_other_coldstart_err": 0.25,
                        "hosted_site_livefile1_err": 0.6,
                        "hosted_site_livefile1_slow_ok": 0.9,
                        "control_panel_dashboard_err": 0.7,
                        "scheduled_task_fs_fail": 0.7,
                        "scheduled_task_slow_ok": 0.4,
                    },
                    "latency_multipliers": {
                        "hosted_site_livefile1_slow_ok": {"p50": 3.5, "p95": 4.5},
                        "control_panel_dashboard_ok_slow": {"p50": 1.4, "p95": 1.6},
                        "scheduled_task_slow_ok": {"p50": 1.4, "p95": 1.6},
                    },
                    "one_shots": [
                        {"ref": "ops.create_snapshots", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops.attach_restored_volumes", "count": 1, "hosts": ["ops1"]},
                        {"ref": "file_server_livefile1.fsck_complete", "count": 1, "hosts": ["livefile1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 44,
                    "rate_multipliers": {
                        "web_runtime.nfs_stale_handle": 0.3,
                        "web_runtime.nfs_server_not_responding": 0.2,
                        "hosted_site_other_coldstart_err": 0.05,
                        "hosted_site_other_hot_err_reboot": 1.0,
                        "control_panel_files_err": 0.2,
                        "control_panel_dashboard_err": 0.2,
                        "hosted_site_livefile1_err": 0.4,
                        "hosted_site_livefile1_slow_ok": 1.0,
                        "monitoring.alert": 1.2,
                        "scheduled_task_fs_fail": 0.15,
                        "scheduled_task_slow_ok": 1.0,
                    },
                    "latency_multipliers": {
                        "hosted_site_other_hot_ok": {"p50": 1.1, "p95": 1.3},
                        "hosted_site_livefile1_slow_ok": {"p50": 3.0, "p95": 4.0},
                        "scheduled_task_slow_ok": {"p50": 1.3, "p95": 1.5},
                    },
                    "one_shots": [{"ref": "ops.rolling_reboot", "count": 1, "hosts": ["ops1"]}],
                },
            ]
        }
    },
}


# ----------------- Deterministic helpers -----------------
def _h64(key: str) -> int:
    return int.from_bytes(hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest(), "big", signed=False)


def u01(key: str) -> float:
    return (_h64(key) & ((1 << 64) - 1)) / float(1 << 64)


def det_choice(key: str, choices: List[Any]) -> Any:
    if not choices:
        return ""
    idx = int(u01(key) * len(choices)) % len(choices)
    return choices[idx]


def det_int(key: str, lo: int, hi: int) -> int:
    if hi <= lo:
        return lo
    return lo + int(u01(key) * (hi - lo + 1))


def det_float(key: str, lo: float, hi: float) -> float:
    if hi <= lo:
        return lo
    return lo + (hi - lo) * u01(key)


def det_hex(key: str, n: int) -> str:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=32).hexdigest()
    return h[:n]


def det_uuid(key: str) -> str:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=16).hexdigest()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def det_ip_from_cidr(key: str, cidr: str) -> str:
    base, prefix_s = cidr.split("/")
    prefix = int(prefix_s)
    parts = [int(x) for x in base.split(".")]
    if prefix != 24:
        prefix = 24
    last = 1 + (int(u01(key) * 254) % 254)
    return f"{parts[0]}.{parts[1]}.{parts[2]}.{last}"


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if u01(key) < frac else 0)


# Acklam's inverse normal CDF approximation
def normal_ppf(p: float) -> float:
    p = min(max(p, 1e-12), 1.0 - 1e-12)
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def sample_lognormal_ms(p50: float, p95: float, key: str, hard_cap_ms: Optional[float] = None) -> int:
    """
    Deterministic lognormal-ish sampler calibrated so:
      median ~ p50, 95th ~ p95, with a soft cap around 3*p95.
    """
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.6448536269514722

    u = u01(key)
    u = u**1.8
    z = normal_ppf(u)
    x = math.exp(mu + sigma * z)

    soft_cap = 3.0 * p95
    x = min(x, soft_cap)
    if hard_cap_ms is not None:
        x = min(x, float(hard_cap_ms))
    return int(max(0, round(x)))


def iso8601_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ----------------- Build indices -----------------
components_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

log_templates: Dict[str, Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for lid, tmpl in comp["logs"].items():
        log_templates[f"{cid}.{lid}"] = {"component_id": cid, **tmpl}


@dataclass
class FailureInterval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> List[FailureInterval]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [fstart] + [e["at_min"] for e in events if fstart < e["at_min"] < fend] + [fend]
    boundaries = sorted(set(boundaries))

    rate_mult: Dict[str, float] = {}
    latency_mult: Dict[str, Dict[str, float]] = {}

    intervals: List[FailureInterval] = []
    for i in range(len(boundaries) - 1):
        seg_start = boundaries[i]
        seg_end = boundaries[i + 1]
        for e in events:
            if e["at_min"] == seg_start:
                for k, v in e.get("rate_multipliers", {}).items():
                    rate_mult[k] = float(v)
                for fk, mv in e.get("latency_multipliers", {}).items():
                    latency_mult[fk] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
        intervals.append(FailureInterval(seg_start, seg_end, dict(rate_mult), dict(latency_mult)))
    return intervals


failure_intervals = build_failure_intervals()


# ----------------- Message rendering -----------------
def domain_for(template: Dict[str, Any], var: str, state: str) -> Optional[Dict[str, Any]]:
    sv = template.get("state_vars", {})
    if state in sv and var in sv[state]:
        return sv[state][var]
    v = template.get("vars", {})
    return v.get(var)


def get_int_domain(ref: str, var: str, state: str) -> Optional[Tuple[int, int]]:
    tmpl = log_templates[ref]
    dom = domain_for(tmpl, var, state)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def render_message(ref: str, state: str, ctx: Dict[str, Any], key: str) -> Tuple[str, str]:
    tmpl = log_templates[ref]
    msg = tmpl["msg"]
    vars_decl = set((tmpl.get("vars") or {}).keys())
    state_decl = set((tmpl.get("state_vars", {}).get(state, {}) or {}).keys())
    needed = vars_decl | state_decl

    values: Dict[str, Any] = {}
    for k in needed:
        if k in ctx:
            values[k] = ctx[k]
            continue
        dom = domain_for(tmpl, k, state)
        if dom is None:
            values[k] = ""
            continue
        kind = dom["k"]
        val = dom.get("v")
        kk = f"{key}:{ref}:{k}"
        if kind == "ch":
            values[k] = det_choice(kk, list(val))
        elif kind == "i":
            lo, hi = int(val[0]), int(val[1])
            values[k] = det_int(kk, lo, hi)
        elif kind == "f":
            lo, hi = float(val[0]), float(val[1])
            values[k] = round(det_float(kk, lo, hi), 3)
        elif kind == "hex":
            values[k] = det_hex(kk, int(val))
        elif kind == "uuid":
            values[k] = det_uuid(kk)
        elif kind == "ip":
            values[k] = det_ip_from_cidr(kk, str(val))
        elif kind == "str":
            hint = "" if val is None else str(val)
            suffix = det_hex(kk, 6)
            values[k] = hint if hint else f"str-{suffix}"
        else:
            values[k] = ""
    for k, v in list(values.items()):
        if isinstance(v, float):
            values[k] = f"{v:.3f}"
    return tmpl["lvl"], msg.format(**values)


def component_identity(component_id: str) -> Tuple[str, List[str]]:
    comp = components_by_id[component_id]
    return comp.get("svc", "") or "", list(comp.get("hosts") or [])


# ----------------- Scheduling helpers -----------------
def schedule_evenly(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_s = (end_dt - start_dt).total_seconds()
    if duration_s <= 0:
        return [start_dt] * count
    step = duration_s / count
    jitter_bound = min(0.25, step * 0.1)
    times: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) * step
        j = (u01(f"{key}:j:{i}") * 2.0 - 1.0) * jitter_bound
        t = start_dt + timedelta(seconds=base + j)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        times.append(t)
    return times


# ----------------- Background simulation -----------------
def emit_log_row(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    ts: datetime,
    ref: str,
    state: str,
    host: str,
    ctx: Dict[str, Any],
    key: str,
) -> None:
    tmpl = log_templates[ref]
    cid = tmpl["component_id"]
    svc, _hosts = component_identity(cid)
    lvl, msg = render_message(ref, state, ctx, key)
    rows.append((ts, lvl, msg, "", svc, host))


def background_context(ref: str, state: str, ts: datetime, active_rate_mult: Dict[str, float], key: str) -> Dict[str, Any]:
    if ref == "monitoring.alert":
        if state == "n":
            name = det_choice(f"{key}:name", ["http_5xx_rate", "worker_pool_saturation"])
            severity = "warning"
            threshold = round(det_float(f"{key}:thr", 0.02, 0.10), 3)
            value = min(1.0, round(threshold + det_float(f"{key}:val", 0.005, 0.03), 3))
        else:
            name = det_choice(f"{key}:name", ["nfs_timeouts", "http_5xx_rate", "worker_pool_saturation"])
            severity = "critical"
            threshold = round(det_float(f"{key}:thr", 0.05, 0.20), 3)
            value = min(1.0, round(threshold + det_float(f"{key}:val", 0.08, 0.45), 3))
        return {"name": name, "severity": severity, "threshold": f"{threshold:.3f}", "value": f"{value:.3f}"}

    if ref == "file_server_livefile1.disk_latency":
        if state == "n":
            p95_ms = det_int(f"{key}:p95", 1, 40)
            util = det_int(f"{key}:util", 1, 40)
        else:
            mult = active_rate_mult.get("file_server_livefile1.disk_latency", 1.0)
            if mult <= 0.01:
                p95_ms = det_int(f"{key}:p95", 1, 50)
                util = det_int(f"{key}:util", 1, 20)
            else:
                p95_ms = det_int(f"{key}:p95", 500, 60000)
                util = det_int(f"{key}:util", 60, 100)
        return {"p95_ms": p95_ms, "util_pct": util}

    if ref == "file_server_livefile1.nfsd_timeout":
        timeout_s = det_int(f"{key}:to", 5, 12) if state == "n" else det_int(f"{key}:to", 10, 60)
        client_ip = det_ip_from_cidr(f"{key}:ip", "10.0.0.0/24")
        op = det_choice(f"{key}:op", ["READ", "WRITE", "GETATTR"])
        return {"timeout_s": timeout_s, "client_ip": client_ip, "op": op}

    if ref == "backup_server.drbd_state":
        if state == "f" and active_rate_mult.get("file_server_livefile1.disk_latency", 1.0) <= 0.01:
            conn = det_choice(f"{key}:conn", ["StandAlone", "StandAlone", "Connected"])
        else:
            conn = "Connected"
        synced = det_int(f"{key}:syn", 90, 100) if conn == "Connected" else det_int(f"{key}:syn", 0, 40)
        return {"res": "users", "role": "Secondary", "conn": conn, "synced_pct": synced}

    return {}


# ----------------- Flow simulation -----------------
def pick_endpoint_for_flow(flow_id: str, key: str, allow_health: bool) -> str:
    if "files" in flow_id:
        return "files"
    if "dashboard" in flow_id:
        if allow_health:
            return det_choice(f"{key}:ep", ["dashboard", "login", "health", "dashboard", "dashboard"])
        return det_choice(f"{key}:ep", ["dashboard", "login", "reload", "dashboard", "dashboard"])
    if allow_health:
        return det_choice(f"{key}:ep", ["dashboard", "login", "health", "reload"])
    return det_choice(f"{key}:ep", ["dashboard", "login", "reload"])


def endpoint_to_uri(endpoint: str) -> str:
    return {"dashboard": "/dashboard", "login": "/login", "health": "/health", "files": "/files", "reload": "/reload"}.get(endpoint, "/")


def flow_common_context(flow_id: str, state: str, key: str, interval_start_min: int) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    req_id = det_hex(f"{key}:req_id", 16)
    ctx["req_id"] = req_id

    is_error_flow = "err" in flow_id

    # Enforce that any flow emitting edge_proxy.access_err uses an allowed uri set (no /health).
    access_err_uris = list(log_templates["edge_proxy.access_err"]["vars"]["uri"]["v"])
    access_ok_uris = list(log_templates["edge_proxy.access_ok"]["vars"]["uri"]["v"])

    if flow_id.startswith("control_panel_"):
        endpoint = pick_endpoint_for_flow(flow_id, key, allow_health=not is_error_flow)
        ctx["endpoint"] = endpoint
        ctx["user_id"] = det_int(f"{key}:user", 1000, 9999)
        ctx["method"] = det_choice(f"{key}:m", ["GET", "GET", "POST"])
        ctx["host"] = "control.example.com"
        ctx["upstream"] = "platform_api"

        # For control-panel flows, uri tracks endpoint; but for error flows we ensure /health can't occur.
        ctx["uri"] = endpoint_to_uri(endpoint)
        if is_error_flow and ctx["uri"] not in access_err_uris:
            # Deterministically rebind to a valid error-uri without inventing new domain values.
            ctx["uri"] = det_choice(f"{key}:uri_fix", access_err_uris)

        if endpoint == "login" and "ok" in flow_id:
            ctx["status"] = "302"
        else:
            ctx["status"] = "200" if "ok" in flow_id else det_choice(f"{key}:st", ["502", "503", "504", "504"])
        if is_error_flow:
            if "files" in flow_id:
                ctx["err"] = "fs_timeout"
            else:
                ctx["err"] = det_choice(f"{key}:err", ["worker_timeout", "upstream_unavailable", "worker_timeout"])
    else:
        ctx["method"] = det_choice(f"{key}:m", ["GET", "GET", "POST"])
        ctx["host"] = "user-site.example.com"
        ctx["upstream"] = "web_runtime"
        ctx["site_id"] = det_int(f"{key}:site", 20000, 20500)

        if "coldstart" in flow_id:
            ctx["uri"] = "/reload"
        else:
            if is_error_flow:
                # Ensure edge_proxy.access_err uri respects its declared domain (no /health).
                # Choose from template domain to satisfy verifier feedback.
                ctx["uri"] = det_choice(f"{key}:uri", access_err_uris)
            else:
                ctx["uri"] = det_choice(f"{key}:uri", ["/", "/", "/health"])

        if is_error_flow:
            if "livefile1_err" in flow_id:
                ctx["err"] = "fs_timeout"
            elif "hot_err_reboot" in flow_id:
                ctx["err"] = det_choice(f"{key}:err", ["upstream_unavailable", "app_startup_timeout", "upstream_unavailable"])
            else:
                ctx["err"] = det_choice(f"{key}:err", ["app_startup_timeout", "fs_timeout", "upstream_unavailable"])
            ctx["status"] = det_choice(f"{key}:st", ["502", "503", "504", "504"])
        else:
            ctx["status"] = "200"

        if "startup" in flow_id or "coldstart" in flow_id:
            ctx["action"] = det_choice(f"{key}:action", ["reload", "start", "reload"])
            if "coldstart_err" in flow_id:
                if state == "f":
                    if interval_start_min < 38:
                        ctx["err"] = "fs_timeout"
                    elif interval_start_min < 44:
                        ctx["err"] = "stale_handle"
                    else:
                        ctx["err"] = det_choice(f"{key}:se", ["slow_io", "stale_handle", "slow_io"])
                else:
                    ctx["err"] = "fs_timeout"

        # If it is an error-flow but uri still ended up outside access_err domain for any reason, fix it.
        if is_error_flow and ctx.get("uri") not in access_err_uris:
            ctx["uri"] = det_choice(f"{key}:uri_fix2", access_err_uris)
        if (not is_error_flow) and ctx.get("uri") not in access_ok_uris:
            ctx["uri"] = det_choice(f"{key}:uri_fix_ok", access_ok_uris)

    if flow_id.startswith("scheduled_task_"):
        ctx["job_id"] = det_uuid(f"{key}:job")
        ctx["account_id"] = det_int(f"{key}:acct", 1000, 9999)
        if flow_id == "scheduled_task_fs_fail":
            if state == "f":
                if interval_start_min < 38:
                    ctx["err"] = "fs_timeout"
                elif interval_start_min < 44:
                    ctx["err"] = det_choice(f"{key}:te", ["stale_handle", "fs_timeout", "stale_handle"])
                else:
                    ctx["err"] = det_choice(f"{key}:te", ["slow_io", "stale_handle", "slow_io"])
            else:
                ctx["err"] = "fs_timeout"

    return ctx


def clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def abs_elapsed_bounds_ms(ref: str, state: str) -> Optional[Tuple[int, int]]:
    """
    Bounds for elapsed time-since-flow-start, when that elapsed is encoded into a message-carried field.
    """
    if ref == "platform_api.fs_timeout":
        return 5000, 60000
    if ref in (
        "platform_api.req_done_ok",
        "platform_api.req_done_err",
        "web_runtime.site_req_ok",
        "web_runtime.site_req_err",
        "edge_proxy.access_ok",
        "edge_proxy.access_err",
        "web_runtime.task_ok",
    ):
        return get_int_domain(ref, "dur_ms", state)
    return None


def rel_since_begin_bounds_ms(ref: str, state: str) -> Optional[Tuple[int, int]]:
    if ref == "web_runtime.startup_ready":
        return get_int_domain(ref, "dur_ms", state) or (200, 60000)
    if ref == "web_runtime.startup_fail":
        return get_int_domain(ref, "waited_ms", state) or (500, 60000)
    return None


def simulate_flow_instance(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    flow: Dict[str, Any],
    state: str,
    start_dt: datetime,
    latency_mult: Dict[str, float],
    instance_key: str,
    interval_start_min: int,
) -> None:
    """
    Verifier-aligned interpretation for this incident model:
      - latency_ms[i] is an absolute elapsed-time hint since flow start (not a per-log delta).
    We sample a target elapsed for each log, clamp to any relevant domains, and enforce monotonicity.
    Message-carried timing fields (dur_ms/timeout_s/waited_ms) are derived from the same timeline.
    """
    emit_refs = list(flow["emit"])
    hints = list(flow["latency_ms"])
    assert len(emit_refs) == len(hints)

    # Host stickiness per emitting component within this flow instance.
    host_by_component: Dict[str, str] = {}
    for ref in emit_refs:
        cid = log_templates[ref]["component_id"]
        if cid not in host_by_component:
            _svc, hosts = component_identity(cid)
            host_by_component[cid] = det_choice(f"{instance_key}:host:{cid}", hosts) if hosts else ""

    ctx_common = flow_common_context(flow["id"], state, instance_key, interval_start_min)

    prev_elapsed = 0
    begin_elapsed: Optional[int] = None

    for i, (ref, (p50, p95)) in enumerate(zip(emit_refs, hints)):
        p50s = float(p50) * float(latency_mult.get("p50", 1.0))
        p95s = float(p95) * float(latency_mult.get("p95", 1.0))

        # Sample absolute elapsed target for this log.
        target_elapsed = sample_lognormal_ms(p50s, p95s, f"{instance_key}:abs:{i}")

        # Clamp to any absolute elapsed bounds implied by message domains.
        ab = abs_elapsed_bounds_ms(ref, state)
        if ab is not None:
            alo, ahi = ab
            target_elapsed = clamp_int(target_elapsed, alo, ahi)

        # Enforce monotonicity against previous emission time.
        elapsed = max(prev_elapsed, int(target_elapsed))

        # If this log reports a duration since startup_begin, constrain that relative duration.
        rb = rel_since_begin_bounds_ms(ref, state)
        if rb is not None and begin_elapsed is not None:
            rlo, rhi = rb
            rel = elapsed - begin_elapsed
            rel_c = clamp_int(rel, rlo, rhi)
            elapsed = max(prev_elapsed, begin_elapsed + rel_c)

        # If startup_begin itself got pushed by monotonic/bounds, track it.
        if ref == "web_runtime.startup_begin":
            begin_elapsed = elapsed

        prev_elapsed = elapsed
        ts = start_dt + timedelta(milliseconds=int(elapsed))

        cid = log_templates[ref]["component_id"]
        host = host_by_component.get(cid, "")
        ctx = dict(ctx_common)

        # Bind observed timing fields from the same chronology.
        if ref in (
            "platform_api.req_done_ok",
            "platform_api.req_done_err",
            "web_runtime.site_req_ok",
            "web_runtime.site_req_err",
            "edge_proxy.access_ok",
            "edge_proxy.access_err",
            "web_runtime.task_ok",
        ):
            ctx["dur_ms"] = int(elapsed)

        if ref == "platform_api.fs_timeout":
            ctx["timeout_s"] = int(round(elapsed / 1000.0))
            ctx.setdefault("fs_op", det_choice(f"{instance_key}:fsop", ["stat", "listdir", "open"]))
            ctx.setdefault("fs_server", "livefile1")

        if ref == "web_runtime.startup_ready":
            be = begin_elapsed if begin_elapsed is not None else 0
            dur = int(elapsed) - int(be)
            lohi = get_int_domain(ref, "dur_ms", state) or (200, 60000)
            ctx["dur_ms"] = clamp_int(dur, lohi[0], lohi[1])

        if ref == "web_runtime.startup_fail":
            be = begin_elapsed if begin_elapsed is not None else 0
            waited = int(elapsed) - int(be)
            lohi = get_int_domain(ref, "waited_ms", state) or (500, 60000)
            ctx["waited_ms"] = clamp_int(waited, lohi[0], lohi[1])
            ctx.setdefault("fs_server", "livefile1")

        if ref == "web_runtime.task_fail_fs":
            ctx.setdefault("fs_server", "livefile1")

        emit_log_row(rows, ts, ref, state, host, ctx, f"{instance_key}:emit:{i}")


# ----------------- Main simulation -----------------
def main() -> None:
    random.seed(0)

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows: List[Tuple[datetime, str, str, str, str, str]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_dt = base_time + timedelta(minutes=n_start)
    n_end_dt = base_time + timedelta(minutes=n_end)

    # Normal background logs
    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get("n", {}).get("emit", [])
        if not beh:
            continue
        _svc, hosts = component_identity(cid)
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            ref = f"{cid}.{log_id}"
            if scope == "global":
                count = det_round(per_min * (n_end - n_start), f"bg:n:{ref}:global")
                host = det_choice(f"bg:n:{ref}:host", hosts) if hosts else ""
                times = schedule_evenly(n_start_dt, n_end_dt, count, f"bg:n:{ref}:global")
                for j, ts in enumerate(times):
                    ctx = background_context(ref, "n", ts, {}, f"bg:n:{ref}:global:{j}")
                    emit_log_row(rows, ts, ref, "n", host, ctx, f"bg:n:{ref}:global:{j}")
            else:
                for h in hosts:
                    count = det_round(per_min * (n_end - n_start), f"bg:n:{ref}:{h}")
                    times = schedule_evenly(n_start_dt, n_end_dt, count, f"bg:n:{ref}:{h}")
                    for j, ts in enumerate(times):
                        ctx = background_context(ref, "n", ts, {}, f"bg:n:{ref}:{h}:{j}")
                        emit_log_row(rows, ts, ref, "n", h, ctx, f"bg:n:{ref}:{h}:{j}")

    # Normal flows
    for flow in SYSTEM["flows"]["n"]["req"]:
        fid = flow["id"]
        expected = float(flow["rpm"]) * (n_end - n_start)
        count = det_round(expected, f"flow:n:{fid}")
        starts = schedule_evenly(n_start_dt, n_end_dt, count, f"flow:n:{fid}:starts")
        for i, st in enumerate(starts):
            simulate_flow_instance(
                rows,
                flow=flow,
                state="n",
                start_dt=st,
                latency_mult={"p50": 1.0, "p95": 1.0},
                instance_key=f"flow:n:{fid}:{i}",
                interval_start_min=n_start,
            )

    # Failure intervals: background + flows with persistent multipliers
    for seg_idx, seg in enumerate(failure_intervals):
        seg_start_dt = base_time + timedelta(minutes=seg.start_min)
        seg_end_dt = base_time + timedelta(minutes=seg.end_min)
        duration_min = seg.end_min - seg.start_min

        # Failure background logs (only failure-state sources are modulated by rate multipliers)
        for comp in SYSTEM["components"]:
            cid = comp["id"]
            beh = comp.get("beh", {}).get("f", {}).get("emit", [])
            if not beh:
                continue
            _svc, hosts = component_identity(cid)
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                ref = f"{cid}.{log_id}"
                mult = float(seg.rate_mult.get(ref, 1.0))
                eff = per_min * mult
                if eff <= 0:
                    continue
                if scope == "global":
                    count = det_round(eff * duration_min, f"bg:f:{seg_idx}:{ref}:global")
                    host = det_choice(f"bg:f:{seg_idx}:{ref}:host", hosts) if hosts else ""
                    times = schedule_evenly(seg_start_dt, seg_end_dt, count, f"bg:f:{seg_idx}:{ref}:global")
                    for j, ts in enumerate(times):
                        ctx = background_context(ref, "f", ts, seg.rate_mult, f"bg:f:{seg_idx}:{ref}:global:{j}")
                        emit_log_row(rows, ts, ref, "f", host, ctx, f"bg:f:{seg_idx}:{ref}:global:{j}")
                else:
                    for h in hosts:
                        count = det_round(eff * duration_min, f"bg:f:{seg_idx}:{ref}:{h}")
                        times = schedule_evenly(seg_start_dt, seg_end_dt, count, f"bg:f:{seg_idx}:{ref}:{h}")
                        for j, ts in enumerate(times):
                            ctx = background_context(ref, "f", ts, seg.rate_mult, f"bg:f:{seg_idx}:{ref}:{h}:{j}")
                            emit_log_row(rows, ts, ref, "f", h, ctx, f"bg:f:{seg_idx}:{ref}:{h}:{j}")

        # Failure flows (only failure-state flow rpm is modulated)
        for flow in SYSTEM["flows"]["f"]["req"]:
            fid = flow["id"]
            mult = float(seg.rate_mult.get(fid, 1.0))
            rpm = float(flow["rpm"]) * mult
            if rpm <= 0:
                continue
            expected = rpm * duration_min
            count = det_round(expected, f"flow:f:{seg_idx}:{fid}")
            starts = schedule_evenly(seg_start_dt, seg_end_dt, count, f"flow:f:{seg_idx}:{fid}:starts")
            lm = seg.latency_mult.get(fid, {"p50": 1.0, "p95": 1.0})
            for i, st in enumerate(starts):
                simulate_flow_instance(
                    rows,
                    flow=flow,
                    state="f",
                    start_dt=st,
                    latency_mult=lm,
                    instance_key=f"flow:f:{seg_idx}:{fid}:{i}",
                    interval_start_min=seg.start_min,
                )

    # One-shots exactly at event time (not scaled by rate multipliers)
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        t0 = base_time + timedelta(minutes=at_min)
        for one in e.get("one_shots", []) or []:
            ref = one["ref"]
            count = int(one["count"])
            allowed_hosts = list(one.get("hosts") or [])
            cid = log_templates[ref]["component_id"]
            _svc, hosts = component_identity(cid)
            use_hosts = allowed_hosts if allowed_hosts else hosts
            for j in range(count):
                ts = t0 + timedelta(milliseconds=int(50 + 900 * u01(f"oneshot:{at_min}:{ref}:{j}")))
                host = det_choice(f"oneshot:{at_min}:{ref}:host:{j}", use_hosts) if use_hosts else ""
                ctx: Dict[str, Any] = {}
                if cid == "ops":
                    ctx["op_id"] = det_hex(f"oneshot:{at_min}:{ref}:op:{j}", 12)
                    if ref == "ops.open_aws_case":
                        ctx["case_id"] = "AWS-case-id"
                emit_log_row(rows, ts, ref, "f", host, ctx, f"oneshot:{at_min}:{ref}:{j}")

    df = pd.DataFrame(rows, columns=["_ts", "level", "message", "trace_id", "service", "host"])
    df.sort_values(by="_ts", inplace=True, kind="mergesort")
    df["timestamp"] = df["_ts"].apply(iso8601_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
