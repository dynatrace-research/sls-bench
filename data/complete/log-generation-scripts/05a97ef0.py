import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "elb_service_us_east"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["elb_api", "elb_control_plane"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "elb_api": {
            "svc": "elb-api",
            "hosts": ["elb-api-1", "elb-api-2"],
            "logs": {
                "api_create_req": {
                    "lvl": "INFO",
                    "msg": "CreateLoadBalancer request lb={lb_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_create_ok": {
                    "lvl": "INFO",
                    "msg": "CreateLoadBalancer succeeded status=200 req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [30, 1500]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "api_modify_req": {
                    "lvl": "INFO",
                    "msg": "ModifyLoadBalancer request lb={lb_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_modify_ok": {
                    "lvl": "INFO",
                    "msg": "ModifyLoadBalancer succeeded status=200 req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [40, 4000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "api_modify_req_state": {
                    "lvl": "INFO",
                    "msg": "ModifyLoadBalancer request (state-read) lb={lb_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_modify_err_state": {
                    "lvl": "ERROR",
                    "msg": "ModifyLoadBalancer failed status=503 err={err} req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "err": {"k": "ch", "v": ["StateNotFound", "StateReadTimeout", "ControlPlaneBusy"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [500, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_modify_req_rejected": {
                    "lvl": "INFO",
                    "msg": "ModifyLoadBalancer request (workflow-gated) lb={lb_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_modify_err_disabled": {
                    "lvl": "ERROR",
                    "msg": "ModifyLoadBalancer failed status=503 err=WorkflowDisabled req_id={req_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [10, 800]}, "trace_id": {"k": "hex", "v": 32}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "elb_control_plane": {
            "svc": "elb-control",
            "hosts": ["elb-cp-1", "elb-cp-2", "elb-cp-3"],
            "logs": {
                "cp_create_start": {
                    "lvl": "INFO",
                    "msg": "provision start lb={lb_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_modify_start": {
                    "lvl": "INFO",
                    "msg": "modify start lb={lb_id} change_id={change_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "change_id": {"k": "hex", "v": 12},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_push_config_modify": {
                    "lvl": "INFO",
                    "msg": "push config (modify) lb={lb_id} backend_count={backend_count} change_id={change_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "backend_count": {"k": "i", "v": [2, 35]},
                        "change_id": {"k": "hex", "v": 12},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_scale_tick": {
                    "lvl": "INFO",
                    "msg": "autoscale reconcile tick lb={lb_id} change_id={change_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "change_id": {"k": "hex", "v": 12},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_push_config_scale": {
                    "lvl": "INFO",
                    "msg": "push config (autoscale) lb={lb_id} backend_count={backend_count} change_id={change_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "backend_count": {"k": "i", "v": [2, 35]},
                        "change_id": {"k": "hex", "v": 12},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_modify_start_state": {
                    "lvl": "INFO",
                    "msg": "modify start (state-read) lb={lb_id} change_id={change_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "change_id": {"k": "hex", "v": 12},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_state_missing": {
                    "lvl": "ERROR",
                    "msg": "state missing lb={lb_id} missing_key={missing_key} change_id={change_id} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "missing_key": {"k": "ch", "v": ["backend_map", "listener_map", "policy_map"]},
                        "change_id": {"k": "hex", "v": 12},
                        "dur_ms": {"k": "i", "v": [50, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_modify_reject": {
                    "lvl": "WARN",
                    "msg": "modify rejected lb={lb_id} reason=workflow_disabled req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_reconcile_tick": {
                    "lvl": "INFO",
                    "msg": "reconcile tick lb={lb_id} change_id={change_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "change_id": {"k": "hex", "v": 12},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cp_push_config_reconcile": {
                    "lvl": "WARN",
                    "msg": "push config (reconcile) lb={lb_id} backend_count={backend_count} change_id={change_id} trace_id={trace_id}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "backend_count": {"k": "i", "v": [0, 30]},
                        "change_id": {"k": "hex", "v": 12},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "workflow_gate_disabled": {
                    "lvl": "WARN",
                    "msg": "workflow gate set enabled=false actor={actor} reason={reason}",
                    "vars": {"actor": {"k": "ch", "v": ["oncall_elb", "engineer_b"]}, "reason": {"k": "ch", "v": ["prevent_propagation", "mitigate_state_loss"]}},
                },
                "workflow_gate_status": {
                    "lvl": "INFO",
                    "msg": "workflow gate status enabled=false disabled_workflows={disabled_workflows}",
                    "vars": {"disabled_workflows": {"k": "i", "v": [3, 10]}},
                },
                "workflow_metrics": {
                    "lvl": "INFO",
                    "msg": "cp_metrics api_err_rate={api_err_rate} state_read_p95_ms={state_read_p95_ms} active_workflows={active_workflows}",
                    "vars": {"active_workflows": {"k": "i", "v": [0, 40]}},
                    "state_vars": {
                        "n": {"api_err_rate": {"k": "f", "v": [0.0, 0.02]}, "state_read_p95_ms": {"k": "i", "v": [5, 60]}},
                        "f": {"api_err_rate": {"k": "f", "v": [0.1, 0.85]}, "state_read_p95_ms": {"k": "i", "v": [200, 5000]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "workflow_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "workflow_metrics", "per_min": 2.0, "scope": "per_host"},
                        {"id": "workflow_gate_status", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "elb_state_store": {
            "svc": "elb-state-db",
            "hosts": ["elb-db-1"],
            "logs": {
                "db_create_write": {
                    "lvl": "INFO",
                    "msg": "INSERT lb_state lb={lb_id} ok rows=1 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "dur_ms": {"k": "i", "v": [5, 200]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "db_read_state_ok_for_modify": {
                    "lvl": "INFO",
                    "msg": "SELECT lb_state (modify) lb={lb_id} rows=1 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "dur_ms": {"k": "i", "v": [3, 120]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "db_read_state_ok_for_scale": {
                    "lvl": "INFO",
                    "msg": "SELECT lb_state (autoscale) lb={lb_id} rows=1 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "dur_ms": {"k": "i", "v": [3, 150]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "db_read_state_notfound": {
                    "lvl": "WARN",
                    "msg": "SELECT lb_state (state-loss) lb={lb_id} rows=0 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "dur_ms": {"k": "i", "v": [20, 8000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "db_read_backend_map_notfound": {
                    "lvl": "WARN",
                    "msg": "SELECT backend_map (reconcile) lb={lb_id} rows=0 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "dur_ms": {"k": "i", "v": [20, 8000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "audit_delete": {
                    "lvl": "ERROR",
                    "msg": "AUDIT logical_delete table=elb_state scope=us-east rows={rows} actor={actor} ticket={ticket}",
                    "vars": {"rows": {"k": "i", "v": [5000, 500000]}, "actor": {"k": "ch", "v": ["dev_a"]}, "ticket": {"k": "ch", "v": ["none"]}},
                },
                "db_metrics": {
                    "lvl": "INFO",
                    "msg": "db_metrics qps={qps} p95_ms={p95_ms} notfound_rate={notfound_rate}",
                    "vars": {},
                    "state_vars": {
                        "n": {"qps": {"k": "i", "v": [1, 2]}, "p95_ms": {"k": "i", "v": [3, 30]}, "notfound_rate": {"k": "f", "v": [0.0, 0.02]}},
                        "f": {"qps": {"k": "i", "v": [2, 12]}, "p95_ms": {"k": "i", "v": [100, 8000]}, "notfound_rate": {"k": "f", "v": [0.2, 0.95]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "db_metrics", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "db_metrics", "per_min": 1.0, "scope": "global"}]},
            },
        },
        "elb_data_plane": {
            "svc": "elb-lb",
            "hosts": ["elb-lb-1", "elb-lb-2", "elb-lb-3", "elb-lb-4"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "lb={lb_id} client_ip={client_ip} method={method} uri={uri} status=200 target_status=200 latency_ms={latency_ms}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "client_ip": {"k": "ip", "v": None},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "str", "v": "/{service}/health|/{service}/api"},
                        "latency_ms": {"k": "i", "v": [5, 800]},
                    },
                },
                "access_err": {
                    "lvl": "WARN",
                    "msg": "lb={lb_id} client_ip={client_ip} method={method} uri={uri} status={status} target_status=- latency_ms={latency_ms}",
                    "vars": {
                        "lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]},
                        "client_ip": {"k": "ip", "v": None},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "str", "v": "/{service}/health|/{service}/api"},
                        "status": {"k": "i", "v": [503, 504]},
                        "latency_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
                "dp_config_applied_modify": {
                    "lvl": "INFO",
                    "msg": "config applied (modify) lb={lb_id} backend_count={backend_count} change_id={change_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "backend_count": {"k": "i", "v": [2, 35]}, "change_id": {"k": "hex", "v": 12}},
                },
                "dp_config_applied_scale": {
                    "lvl": "INFO",
                    "msg": "config applied (autoscale) lb={lb_id} backend_count={backend_count} change_id={change_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "backend_count": {"k": "i", "v": [2, 35]}, "change_id": {"k": "hex", "v": 12}},
                },
                "dp_config_applied_reconcile": {
                    "lvl": "WARN",
                    "msg": "config applied (reconcile) lb={lb_id} backend_count={backend_count} change_id={change_id}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}, "backend_count": {"k": "i", "v": [0, 30]}, "change_id": {"k": "hex", "v": 12}},
                },
                "healthcheck_summary": {
                    "lvl": "INFO",
                    "msg": "hc_summary lb={lb_id} backend_count={backend_count} fails={fails}",
                    "vars": {"lb_id": {"k": "ch", "v": ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]}},
                    "state_vars": {"n": {"backend_count": {"k": "i", "v": [2, 35]}, "fails": {"k": "i", "v": [0, 5]}}, "f": {"backend_count": {"k": "i", "v": [0, 35]}, "fails": {"k": "i", "v": [0, 120]}}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "healthcheck_summary", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "healthcheck_summary", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "ops_recovery": {
            "svc": "elb-ops",
            "hosts": ["ops-1"],
            "logs": {
                "restore_start": {
                    "lvl": "WARN",
                    "msg": "restore start method=point_in_time_restore_v1 target_snapshot=pre_delete actor={actor} ticket={ticket}",
                    "vars": {"actor": {"k": "ch", "v": ["oncall_elb"]}, "ticket": {"k": "ch", "v": ["INC-5521"]}},
                },
                "restore_failed": {
                    "lvl": "ERROR",
                    "msg": "restore failed method=point_in_time_restore_v1 err={err} waited_min={waited_min} ticket={ticket}",
                    "vars": {"err": {"k": "ch", "v": ["SnapshotCorrupt", "SnapshotIncomplete", "TimeoutWaitingForSnapshot"]}, "waited_min": {"k": "i", "v": [5, 5]}, "ticket": {"k": "ch", "v": ["INC-5521"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "client_request_ok",
                    "rpm": 400.0,
                    "emit": ["elb_data_plane.access_ok"],
                    "latency_ms": [[25, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_create_lb",
                    "rpm": 5.0,
                    "emit": ["elb_api.api_create_req", "elb_control_plane.cp_create_start", "elb_state_store.db_create_write", "elb_api.api_create_ok"],
                    "latency_ms": [[1, 5], [5, 25], [10, 60], [40, 1500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "api_modify_lb_ok",
                    "rpm": 20.0,
                    "emit": [
                        "elb_api.api_modify_req",
                        "elb_control_plane.cp_modify_start",
                        "elb_state_store.db_read_state_ok_for_modify",
                        "elb_control_plane.cp_push_config_modify",
                        "elb_data_plane.dp_config_applied_modify",
                        "elb_api.api_modify_ok",
                    ],
                    "latency_ms": [[1, 5], [3, 20], [8, 80], [10, 120], [15, 200], [50, 4000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "cp_autoscale_reconcile",
                    "rpm": 6.0,
                    "emit": ["elb_control_plane.cp_scale_tick", "elb_state_store.db_read_state_ok_for_scale", "elb_control_plane.cp_push_config_scale", "elb_data_plane.dp_config_applied_scale"],
                    "latency_ms": [[2, 10], [5, 120], [10, 150], [10, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {"id": "client_request_healthy", "rpm": 380.0, "emit": ["elb_data_plane.access_ok"], "latency_ms": [[30, 180]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "client_request_impacted", "rpm": 20.0, "emit": ["elb_data_plane.access_err"], "latency_ms": [[250, 1500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_create_lb", "rpm": 5.0, "emit": ["elb_api.api_create_req", "elb_control_plane.cp_create_start", "elb_state_store.db_create_write", "elb_api.api_create_ok"], "latency_ms": [[1, 5], [5, 25], [10, 80], [40, 2000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_modify_lb_fail_state", "rpm": 20.0, "emit": ["elb_api.api_modify_req_state", "elb_control_plane.cp_modify_start_state", "elb_state_store.db_read_state_notfound", "elb_control_plane.cp_state_missing", "elb_api.api_modify_err_state"], "latency_ms": [[1, 5], [10, 100], [50, 8000], [100, 20000], [500, 20000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_modify_lb_rejected", "rpm": 20.0, "emit": ["elb_api.api_modify_req_rejected", "elb_control_plane.cp_modify_reject", "elb_api.api_modify_err_disabled"], "latency_ms": [[1, 5], [2, 20], [10, 800]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "cp_reconcile_push_config", "rpm": 6.0, "emit": ["elb_control_plane.cp_reconcile_tick", "elb_state_store.db_read_backend_map_notfound", "elb_control_plane.cp_push_config_reconcile", "elb_data_plane.dp_config_applied_reconcile"], "latency_ms": [[2, 10], [50, 8000], [10, 200], [10, 300]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "elb_state_data_logical_delete_us_east_2012_12_24",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 30}, "f": {"start_min": 30, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 30,
                        "rate_multipliers": {
                            "client_request_healthy": 1.05,
                            "client_request_impacted": 0.0,
                            "cp_reconcile_push_config": 0.0,
                            "api_modify_lb_rejected": 0.0,
                            "elb_control_plane.workflow_gate_status": 0.0,
                            "elb_data_plane.healthcheck_summary": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "elb_state_store.audit_delete", "count": 1, "hosts": ["elb-db-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 40,
                        "rate_multipliers": {
                            "client_request_healthy": 1.0,
                            "client_request_impacted": 1.0,
                            "cp_reconcile_push_config": 1.0,
                            "elb_data_plane.healthcheck_summary": 1.5,
                        },
                        "latency_multipliers": {},
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 50,
                        "rate_multipliers": {
                            "api_modify_lb_fail_state": 0.25,
                            "api_modify_lb_rejected": 0.75,
                            "cp_reconcile_push_config": 0.1,
                            "elb_control_plane.workflow_gate_status": 1.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "elb_control_plane.workflow_gate_disabled", "count": 1, "hosts": ["elb-cp-2"]},
                            {"ref": "ops_recovery.restore_start", "count": 1, "hosts": ["ops-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 55,
                        "rate_multipliers": {"elb_state_store.db_metrics": 2.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "ops_recovery.restore_failed", "count": 1, "hosts": ["ops-1"]}],
                    },
                ]
            }
        },
    }
}


SEED = "simseed-v3-elb-2012-12-24"
BASE_TIME = datetime(2012, 12, 24, 0, 0, 0, tzinfo=timezone.utc)
Z95 = 1.6448536269514722


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def stable_u01(key: str) -> float:
    h = md5_hex(f"{SEED}|{key}")
    x = int(h[:16], 16)
    return (x + 0.5) / (16**16)


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def inv_norm_cdf(p: float) -> float:
    if p <= 0.0:
        return -float("inf")
    if p >= 1.0:
        return float("inf")

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


def sample_lognormal_ms(p50: float, p95: float, u: float, cap_mult: float = 3.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.01, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / Z95
    u = min(0.9995, max(0.0005, u))
    z = inv_norm_cdf(u)
    x = math.exp(mu + sigma * z)

    # Soft-cap tails so scheduling remains stable; observed-field domains are enforced separately.
    cap = cap_mult * p95
    if x > cap:
        x = cap + (x - cap) * 0.1
    return max(1.0, x)


def clampf(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def to_int_ms(x: float, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    v = int(math.floor(x + 0.5))
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def allocate_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 0:
        return n
    u = stable_u01(f"alloc|{key}")
    return n + (1 if u < frac else 0)


def schedule_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start] * n
    out: List[datetime] = []
    for i in range(n):
        base_frac = (i + 0.5) / n
        u = stable_u01(f"jitter|{key}|{i}")
        jitter_s = (u - 0.5) * 0.8
        t = start + timedelta(seconds=base_frac * total_s + jitter_s)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    out.sort()
    return out


def deterministic_uuid(key: str) -> str:
    h = md5_hex(f"uuid|{key}")
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def deterministic_hex(key: str, n: int) -> str:
    h = md5_hex(f"hex|{key}")
    while len(h) < n:
        h += md5_hex(f"hex2|{key}|{len(h)}")
    return h[:n]


def deterministic_ip(key: str) -> str:
    u = stable_u01(f"ip|{key}")
    last = 1 + int(u * 253)
    return f"198.51.100.{last}"


def choose_from_list(lst: List[Any], key: str) -> Any:
    if not lst:
        return None
    u = stable_u01(f"ch|{key}")
    idx = int(u * len(lst))
    if idx >= len(lst):
        idx = len(lst) - 1
    return lst[idx]


def sample_int_range(a: int, b: int, key: str) -> int:
    if a > b:
        a, b = b, a
    u = stable_u01(f"i|{key}")
    span = b - a + 1
    v = a + int(u * span)
    if v > b:
        v = b
    return v


def sample_float_range(a: float, b: float, key: str) -> float:
    if a > b:
        a, b = b, a
    u = stable_u01(f"f|{key}")
    return a + u * (b - a)


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def get_log_template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def template_placeholders(msg: str) -> List[str]:
    out = []
    i = 0
    while True:
        j = msg.find("{", i)
        if j < 0:
            break
        k = msg.find("}", j + 1)
        if k < 0:
            break
        out.append(msg[j + 1 : k])
        i = k + 1
    return out


def render_log_message(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], key: str) -> str:
    tmpl = get_log_template(comp_id, log_id)
    msg = tmpl["msg"]
    ph = template_placeholders(msg)

    specs: Dict[str, Any] = {}
    specs.update(tmpl.get("vars", {}) or {})
    stv = tmpl.get("state_vars", {}) or {}
    if state in stv:
        specs.update(stv[state] or {})

    values: Dict[str, Any] = {}
    service_names = ["orders", "billing", "auth", "search", "media"]

    for name in ph:
        if name in bound:
            values[name] = bound[name]
            continue
        spec = specs.get(name)
        if spec is None:
            values[name] = deterministic_hex(f"{key}|{comp_id}.{log_id}|{name}", 8)
            continue
        knd = spec["k"]
        dom = spec.get("v")
        if knd == "ch":
            values[name] = choose_from_list(list(dom), f"{key}|{comp_id}.{log_id}|{name}")
        elif knd == "i":
            values[name] = sample_int_range(int(dom[0]), int(dom[1]), f"{key}|{comp_id}.{log_id}|{name}")
        elif knd == "f":
            v = sample_float_range(float(dom[0]), float(dom[1]), f"{key}|{comp_id}.{log_id}|{name}")
            values[name] = f"{v:.3f}".rstrip("0").rstrip(".") if v < 10 else f"{v:.2f}"
        elif knd == "uuid":
            values[name] = deterministic_uuid(f"{key}|{comp_id}.{log_id}|{name}")
        elif knd == "hex":
            values[name] = deterministic_hex(f"{key}|{comp_id}.{log_id}|{name}", int(dom))
        elif knd == "ip":
            values[name] = deterministic_ip(f"{key}|{comp_id}.{log_id}|{name}")
        elif knd == "str":
            pattern = str(dom)
            options = pattern.split("|") if "|" in pattern else [pattern]
            opt = choose_from_list(options, f"{key}|{comp_id}.{log_id}|{name}|opt")
            svc = choose_from_list(service_names, f"{key}|{comp_id}.{log_id}|{name}|svc")
            values[name] = opt.replace("{service}", str(svc))
        else:
            values[name] = str(dom)

    rendered = msg
    for name, val in values.items():
        rendered = rendered.replace("{" + name + "}", str(val))
    return rendered


def get_component_identity(comp_id: str) -> Tuple[str, List[str]]:
    c = SYSTEM["components"][comp_id]
    return c.get("svc") or "", list(c.get("hosts") or [])


def choose_component_host(comp_id: str, inst_key: str, host_hint: Optional[str] = None) -> str:
    _, hosts = get_component_identity(comp_id)
    if not hosts:
        return ""
    if host_hint and host_hint in hosts:
        return host_hint
    u = stable_u01(f"host|{inst_key}|{comp_id}")
    idx = int(u * len(hosts))
    if idx >= len(hosts):
        idx = len(hosts) - 1
    return hosts[idx]


def find_var_domain(comp_id: str, log_id: str, state: str, var: str) -> Optional[Tuple[str, Any]]:
    tmpl = get_log_template(comp_id, log_id)
    specs: Dict[str, Any] = {}
    specs.update(tmpl.get("vars", {}) or {})
    stv = tmpl.get("state_vars", {}) or {}
    if state in stv:
        specs.update(stv[state] or {})
    if var in specs:
        spec = specs[var]
        return spec["k"], spec.get("v")
    return None


def step_bounds_ms(comp_id: str, log_id: str, state: str, p50: float, p95: float) -> Tuple[float, float]:
    # Base bounds from latency hint (soft cap) to keep timings stable.
    lo = 1.0
    hi = max(1.0, 3.0 * float(p95))

    # If the log message carries an observed timing field that corresponds to this step delay,
    # enforce that declared integer domain exactly.
    # - DB logs and cp_state_missing carry dur_ms for the step.
    # - Access logs carry latency_ms for the step.
    for observed in ("dur_ms", "latency_ms"):
        dom = find_var_domain(comp_id, log_id, state, observed)
        if dom is not None and dom[0] == "i":
            dmin, dmax = int(dom[1][0]), int(dom[1][1])
            lo = max(lo, float(dmin))
            # Keep hi within declared domain; if latency hint is smaller, that's OK.
            hi = min(hi, float(dmax))
            if hi < lo:
                hi = lo
            break

    return lo, hi


def fit_delays_to_target_with_bounds(delays: List[float], mins: List[float], maxs: List[float], target_total: float) -> List[float]:
    d = [clampf(delays[i], mins[i], maxs[i]) for i in range(len(delays))]
    total = sum(d)
    diff = target_total - total
    if abs(diff) < 0.5:
        return d

    # Prefer adjusting later steps to preserve earlier per-hop timings.
    idxs = list(range(len(d) - 1, -1, -1))

    if diff > 0:
        for i in idxs:
            headroom = maxs[i] - d[i]
            if headroom <= 0:
                continue
            inc = min(headroom, diff)
            d[i] += inc
            diff -= inc
            if diff <= 0.5:
                break
    else:
        need = -diff
        for i in idxs:
            reducible = d[i] - mins[i]
            if reducible <= 0:
                continue
            dec = min(reducible, need)
            d[i] -= dec
            need -= dec
            if need <= 0.5:
                break

    return [clampf(d[i], mins[i], maxs[i]) for i in range(len(d))]


@dataclass(frozen=True)
class ControlInterval:
    start_min: int
    end_min: int
    flow_mult: Dict[str, float]
    bg_mult: Dict[str, float]
    lat_mult: Dict[str, float]


def build_failure_control_intervals() -> Tuple[List[ControlInterval], List[Dict[str, Any]]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    start = int(f_phase["start_min"])
    end = int(f_phase["end_min"])
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (int(e["at_min"]), int(e.get("order", 0))))

    boundaries = [start] + sorted({int(e["at_min"]) for e in events if start <= int(e["at_min"]) < end and int(e["at_min"]) != start}) + [end]
    event_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        event_by_min.setdefault(int(e["at_min"]), []).append(e)

    active_flow: Dict[str, float] = {}
    active_bg: Dict[str, float] = {}
    active_lat: Dict[str, float] = {}
    intervals: List[ControlInterval] = []

    for i in range(len(boundaries) - 1):
        seg_start = boundaries[i]
        seg_end = boundaries[i + 1]
        for e in event_by_min.get(seg_start, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                if "." in k:
                    active_bg[k] = float(v)
                else:
                    active_flow[k] = float(v)
            for k, v in (e.get("latency_multipliers") or {}).items():
                active_lat[k] = float(v)

        intervals.append(ControlInterval(seg_start, seg_end, dict(active_flow), dict(active_bg), dict(active_lat)))

    oneshots: List[Dict[str, Any]] = []
    for e in events:
        for os in (e.get("one_shots") or []):
            oneshots.append({"at_min": int(e["at_min"]), "ref": os["ref"], "count": int(os["count"]), "hosts": list(os.get("hosts") or [])})
    return intervals, oneshots


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {
            "timestamp": isoformat_ms(ts),
            "level": level,
            "message": message,
            "trace_id": trace_id if trace_id else "",
            "service": service if service else "",
            "host": host if host else "",
        }
    )


def simulate_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]] = None) -> None:
    bg_mult = bg_mult or {}
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id in sorted(SYSTEM["components"].keys()):
        comp = SYSTEM["components"][comp_id]
        emits = list((comp.get("beh", {}).get(state, {}) or {}).get("emit") or [])
        if not emits:
            continue
        svc, hosts = get_component_identity(comp_id)

        for em in emits:
            log_id = em["id"]
            per_min = float(em["per_min"])
            scope = em.get("scope") or "per_host"
            mult_key = f"{comp_id}.{log_id}"
            mult = float(bg_mult.get(mult_key, 1.0)) if state == "f" else 1.0
            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                expected = eff * dur_min
                n = allocate_count(expected, f"bg|{state}|{start_min}-{end_min}|{mult_key}|global")
                times = schedule_times(start_dt, end_dt, n, f"bg|{state}|{start_min}-{end_min}|{mult_key}|global")
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    msg = render_log_message(comp_id, log_id, state, {}, f"bg|{state}|{mult_key}|{start_min}-{end_min}|{i}")
                    lvl = get_log_template(comp_id, log_id)["lvl"]
                    emit_row(rows, t, lvl, msg, "", svc, host)
            else:
                if not hosts:
                    expected = eff * dur_min
                    n = allocate_count(expected, f"bg|{state}|{start_min}-{end_min}|{mult_key}|nohosts")
                    times = schedule_times(start_dt, end_dt, n, f"bg|{state}|{start_min}-{end_min}|{mult_key}|nohosts")
                    for i, t in enumerate(times):
                        msg = render_log_message(comp_id, log_id, state, {}, f"bg|{state}|{mult_key}|{start_min}-{end_min}|{i}")
                        lvl = get_log_template(comp_id, log_id)["lvl"]
                        emit_row(rows, t, lvl, msg, "", svc, "")
                else:
                    for h in hosts:
                        expected = eff * dur_min
                        n = allocate_count(expected, f"bg|{state}|{start_min}-{end_min}|{mult_key}|{h}")
                        times = schedule_times(start_dt, end_dt, n, f"bg|{state}|{start_min}-{end_min}|{mult_key}|{h}")
                        for i, t in enumerate(times):
                            bound: Dict[str, Any] = {}
                            if comp_id == "elb_data_plane" and log_id == "healthcheck_summary" and state == "f":
                                impacted = ["lb-a1", "lb-b1", "lb-c1"]
                                lb_id = choose_from_list(get_log_template(comp_id, log_id)["vars"]["lb_id"]["v"], f"hc|{start_min}-{end_min}|{h}|{i}")
                                bound["lb_id"] = lb_id
                                if lb_id in impacted:
                                    u0 = stable_u01(f"hc|{start_min}-{end_min}|{h}|{i}|b0")
                                    bound["backend_count"] = 0 if u0 < 0.8 else sample_int_range(1, 10, f"hc|{start_min}-{end_min}|{h}|{i}|b1")
                                    bound["fails"] = sample_int_range(10, 120, f"hc|{start_min}-{end_min}|{h}|{i}|f")
                                else:
                                    bound["backend_count"] = sample_int_range(2, 35, f"hc|{start_min}-{end_min}|{h}|{i}|b2")
                                    bound["fails"] = sample_int_range(0, 15, f"hc|{start_min}-{end_min}|{h}|{i}|f2")
                            msg = render_log_message(comp_id, log_id, state, bound, f"bg|{state}|{mult_key}|{start_min}-{end_min}|{h}|{i}")
                            lvl = get_log_template(comp_id, log_id)["lvl"]
                            emit_row(rows, t, lvl, msg, "", svc, h)


def simulate_flow_instances(rows: List[Dict[str, Any]], state: str, flow: Dict[str, Any], start_min: int, end_min: int, flow_mult: float = 1.0, latency_mult: float = 1.0) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    rpm = float(flow["rpm"])
    eff_rpm = rpm * (flow_mult if state == "f" else 1.0)
    if eff_rpm <= 0:
        return

    expected_instances = eff_rpm * dur_min
    n_inst = allocate_count(expected_instances, f"flow|{state}|{flow['id']}|{start_min}-{end_min}")
    starts = schedule_times(start_dt, end_dt, n_inst, f"flowstart|{state}|{flow['id']}|{start_min}-{end_min}")

    all_lbs = ["lb-a1", "lb-a2", "lb-a3", "lb-b1", "lb-b2", "lb-c1", "lb-c2", "lb-d1", "lb-d2", "lb-e1"]
    impacted_lbs = ["lb-a1", "lb-b1", "lb-c1"]
    healthy_lbs = [x for x in all_lbs if x not in impacted_lbs]

    emit_refs: List[str] = list(flow["emit"])
    lat_pairs: List[List[float]] = list(flow["latency_ms"])

    for idx, chain_start in enumerate(starts):
        inst_key = f"{state}|{flow['id']}|{start_min}-{end_min}|{idx}|{isoformat_ms(chain_start)}"
        trace_id = ""
        if flow.get("trace", False) and SYSTEM["tracing"]["on"]:
            trace_id = deterministic_hex(f"trace|{inst_key}", 32)

        bound: Dict[str, Any] = {}
        if flow["id"] in ("client_request_impacted", "cp_reconcile_push_config"):
            bound["lb_id"] = choose_from_list(impacted_lbs, f"lb|{inst_key}")
        elif flow["id"] in ("client_request_ok", "client_request_healthy"):
            bound["lb_id"] = choose_from_list(healthy_lbs, f"lb|{inst_key}")
        else:
            bound["lb_id"] = choose_from_list(all_lbs, f"lb|{inst_key}")

        bound["req_id"] = deterministic_uuid(f"req|{inst_key}")
        bound["change_id"] = deterministic_hex(f"chg|{inst_key}", 12)
        bound["trace_id"] = trace_id

        bound["client_ip"] = deterministic_ip(f"cip|{inst_key}")
        bound["method"] = choose_from_list(["GET", "POST"], f"method|{inst_key}")
        svc = choose_from_list(["orders", "billing", "auth", "search", "media"], f"urisvc|{inst_key}")
        uri_opt = choose_from_list(["/{service}/health", "/{service}/api"], f"uriopt|{inst_key}")
        bound["uri"] = uri_opt.replace("{service}", str(svc))

        if flow["id"] == "client_request_impacted":
            u = stable_u01(f"status|{inst_key}")
            bound["status"] = 503 if u < 0.8 else 504

        if flow["id"] in ("api_modify_lb_ok", "cp_autoscale_reconcile"):
            bound["backend_count"] = sample_int_range(2, 35, f"bc|{inst_key}")
        elif flow["id"] == "cp_reconcile_push_config":
            u = stable_u01(f"bc0|{inst_key}")
            bound["backend_count"] = 0 if u < 0.7 else sample_int_range(1, 10, f"bc1|{inst_key}")

        if flow["id"] == "api_modify_lb_fail_state":
            bound["missing_key"] = choose_from_list(["backend_map", "listener_map", "policy_map"], f"mk|{inst_key}")
            u = stable_u01(f"err|{inst_key}")
            if u < 0.7:
                bound["err"] = "StateNotFound"
            elif u < 0.9:
                bound["err"] = "StateReadTimeout"
            else:
                bound["err"] = "ControlPlaneBusy"

        # Sample per-step delays and enforce per-log observed-timing domains where applicable.
        delays_ms: List[float] = []
        mins: List[float] = []
        maxs: List[float] = []
        for j, pair in enumerate(lat_pairs):
            p50, p95 = float(pair[0]) * latency_mult, float(pair[1]) * latency_mult
            comp_id, log_id = parse_ref(emit_refs[j])
            lo, hi = step_bounds_ms(comp_id, log_id, state, p50, p95)
            u = stable_u01(f"lat|{inst_key}|{j}")
            d = sample_lognormal_ms(p50, p95, u)
            d = clampf(d, lo, hi)
            delays_ms.append(d)
            mins.append(lo)
            maxs.append(hi)

        # If the terminal log carries dur_ms, ensure the total duration fits its declared domain
        # without violating any per-step observed timing domains.
        last_comp, last_log = parse_ref(emit_refs[-1])
        term_dom = find_var_domain(last_comp, last_log, state, "dur_ms")
        if term_dom is not None and term_dom[0] == "i":
            tmin, tmax = int(term_dom[1][0]), int(term_dom[1][1])
            prelim_total = sum(delays_ms)
            min_feas = sum(mins)
            max_feas = sum(maxs)
            lo_total = max(float(tmin), float(min_feas))
            hi_total = min(float(tmax), float(max_feas))
            if lo_total > hi_total:
                # Fallback to feasible bounds if intersection is empty (should be rare).
                lo_total, hi_total = float(min_feas), float(max_feas)
            target_total = clampf(prelim_total, lo_total, hi_total)
            delays_ms = fit_delays_to_target_with_bounds(delays_ms, mins, maxs, target_total)

        comp_host: Dict[str, str] = {}

        t0 = chain_start
        t = chain_start
        for j, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            if comp_id not in comp_host:
                comp_host[comp_id] = choose_component_host(comp_id, inst_key)
            svc_name, _ = get_component_identity(comp_id)

            t = t + timedelta(milliseconds=delays_ms[j])

            bound_local = dict(bound)

            # Per-step observed timings
            if find_var_domain(comp_id, log_id, state, "dur_ms") is not None:
                dom = find_var_domain(comp_id, log_id, state, "dur_ms")
                lo_i = int(dom[1][0]) if dom and dom[0] == "i" else None
                hi_i = int(dom[1][1]) if dom and dom[0] == "i" else None
                # Some logs (e.g., API terminal) will overwrite dur_ms below; this sets the step value for DB/CP logs.
                bound_local["dur_ms"] = to_int_ms(delays_ms[j], lo=lo_i, hi=hi_i)

            if find_var_domain(comp_id, log_id, state, "latency_ms") is not None:
                dom = find_var_domain(comp_id, log_id, state, "latency_ms")
                lo_i = int(dom[1][0]) if dom and dom[0] == "i" else None
                hi_i = int(dom[1][1]) if dom and dom[0] == "i" else None
                bound_local["latency_ms"] = to_int_ms(delays_ms[j], lo=lo_i, hi=hi_i)

            # Terminal API-style total duration (must be consistent with timestamps and within its domain).
            if log_id in ("api_create_ok", "api_modify_ok", "api_modify_err_state", "api_modify_err_disabled"):
                dom = find_var_domain(comp_id, log_id, state, "dur_ms")
                lo_i = int(dom[1][0]) if dom and dom[0] == "i" else None
                hi_i = int(dom[1][1]) if dom and dom[0] == "i" else None
                bound_local["dur_ms"] = to_int_ms((t - t0).total_seconds() * 1000.0, lo=lo_i, hi=hi_i)

            msg = render_log_message(comp_id, log_id, state, bound_local, f"flowmsg|{inst_key}|{j}")
            lvl = get_log_template(comp_id, log_id)["lvl"]
            emit_row(rows, t, lvl, msg, trace_id if flow.get("trace", False) else "", svc_name, comp_host[comp_id])


def simulate_flows(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, flow_mults: Optional[Dict[str, float]] = None, lat_mults: Optional[Dict[str, float]] = None) -> None:
    flow_mults = flow_mults or {}
    lat_mults = lat_mults or {}
    flows = list(SYSTEM["flows"][state]["req"])
    flows.sort(key=lambda f: f["id"])
    for f in flows:
        fm = float(flow_mults.get(f["id"], 1.0)) if state == "f" else 1.0
        lm = float(lat_mults.get(f["id"], 1.0)) if state == "f" else 1.0
        simulate_flow_instances(rows, state, f, start_min, end_min, flow_mult=fm, latency_mult=lm)


def simulate_oneshots(rows: List[Dict[str, Any]], oneshots: List[Dict[str, Any]]) -> None:
    for os in oneshots:
        at_min = int(os["at_min"])
        ref = os["ref"]
        count = int(os["count"])
        hosts = list(os.get("hosts") or [])
        comp_id, log_id = parse_ref(ref)
        svc, comp_hosts = get_component_identity(comp_id)
        allowed_hosts = hosts if hosts else comp_hosts
        if not allowed_hosts:
            allowed_hosts = [""]

        base_t = BASE_TIME + timedelta(minutes=at_min)
        times = []
        for i in range(count):
            u = stable_u01(f"oneshot|{at_min}|{ref}|{i}")
            jitter_ms = int((u - 0.5) * 800)
            t = base_t + timedelta(milliseconds=jitter_ms)
            times.append(t)
        times.sort()

        for i, t in enumerate(times):
            host = allowed_hosts[i % len(allowed_hosts)]
            msg = render_log_message(comp_id, log_id, "f", {}, f"oneshotmsg|{at_min}|{ref}|{i}")
            lvl = get_log_template(comp_id, log_id)["lvl"]
            emit_row(rows, t, lvl, msg, "", svc, host)


def main() -> None:
    random.seed(SEED)

    rows: List[Dict[str, Any]] = []

    n_phase = SCENARIO["scenario"]["time"]["phases"]["n"]
    n_start, n_end = int(n_phase["start_min"]), int(n_phase["end_min"])
    simulate_background(rows, "n", n_start, n_end, bg_mult=None)
    simulate_flows(rows, "n", n_start, n_end)

    f_intervals, oneshots = build_failure_control_intervals()
    for ci in f_intervals:
        simulate_background(rows, "f", ci.start_min, ci.end_min, bg_mult=ci.bg_mult)
        simulate_flows(rows, "f", ci.start_min, ci.end_min, flow_mults=ci.flow_mult, lat_mults=ci.lat_mult)

    simulate_oneshots(rows, oneshots)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    nrows = len(df)
    assert 20000 <= nrows <= 100000, f"Row count out of bounds: {nrows}"

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
