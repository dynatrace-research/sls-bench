import hashlib
import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Deterministic seeds (required by verifier; simulation itself is hash-deterministic)
random.seed(0)
np.random.seed(0)

SYSTEM: Dict[str, Any] = {
    "sys_id": "cloud_edge_service_tokens_incident_2023_01_24",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "access_token_service": {
            "svc": "access-token",
            "hosts": ["access-1", "access-2"],
            "logs": {
                "deploy_started": {
                    "lvl": "INFO",
                    "msg": "deploy started service={service} version={version} feature={feature}",
                    "vars": {
                        "service": {"k": "ch", "v": ["access-token"]},
                        "version": {"k": "ch", "v": ["2023.01.24.1655"]},
                        "feature": {"k": "ch", "v": ["service_token_last_seen"]},
                    },
                },
                "deploy_rolled_back": {
                    "lvl": "INFO",
                    "msg": "deploy rolled back service={service} version={version} feature={feature}",
                    "vars": {
                        "service": {"k": "ch", "v": ["access-token"]},
                        "version": {"k": "ch", "v": ["2023.01.24.1705"]},
                        "feature": {"k": "ch", "v": ["service_token_last_seen"]},
                    },
                },
                "last_seen_consumer_poll": {
                    "lvl": "INFO",
                    "msg": "last_seen poll scanned_events={scanned} candidate_tokens={candidates} lag_ms={lag_ms}",
                    "vars": {
                        "scanned": {"k": "i", "v": [50, 400]},
                        "candidates": {"k": "i", "v": [0, 40]},
                        "lag_ms": {"k": "i", "v": [10, 1500]},
                    },
                },
                "token_update_write_bug": {
                    "lvl": "INFO",
                    "msg": "token write account={account} token_id={token_id} fields={fields} client_secret_len={secret_len} txn_ms={txn_ms}",
                    "vars": {
                        "account": {"k": "ch", "v": ["warp_internal", "api_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "fields": {"k": "ch", "v": ["last_seen_at,client_secret"]},
                        "secret_len": {"k": "i", "v": [0, 0]},
                        "txn_ms": {"k": "i", "v": [8, 80]},
                    },
                },
                "token_admin_update": {
                    "lvl": "INFO",
                    "msg": "token admin_update account={account} token_id={token_id} fields={fields} client_secret_len={secret_len}",
                    "vars": {
                        "account": {"k": "ch", "v": ["customer_account_a", "customer_account_b"]},
                        "token_id": {"k": "uuid", "v": None},
                        "fields": {"k": "ch", "v": ["rotate_secret", "update_expiry"]},
                        "secret_len": {"k": "i", "v": [32, 64]},
                    },
                },
                "account_changes_locked": {
                    "lvl": "WARN",
                    "msg": "ops lock enabled scope={scope} reason={reason}",
                    "vars": {
                        "scope": {"k": "ch", "v": ["internal_accounts"]},
                        "reason": {"k": "ch", "v": ["incident_mitigation"]},
                    },
                },
                "manual_token_restore_warp": {
                    "lvl": "INFO",
                    "msg": "token restore account={account} token_id={token_id} method={method}",
                    "vars": {
                        "account": {"k": "ch", "v": ["warp_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "method": {"k": "ch", "v": ["manual_revert"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "token_admin_update", "per_min": 0.05, "scope": "per_host"}],
                "f": [
                    {"id": "last_seen_consumer_poll", "per_min": 2.0, "scope": "per_host"},
                    {"id": "token_update_write_bug", "per_min": 1.2, "scope": "per_host"},
                ],
            },
        },
        "config_sync": {
            "svc": "config-sync",
            "hosts": ["sync-1"],
            "logs": {
                "sync_cycle": {
                    "lvl": "DEBUG",
                    "msg": "sync cycle tick batch_id={batch_id} pending_updates={pending} duration_ms={dur_ms}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 16},
                        "pending": {"k": "i", "v": [0, 50]},
                        "dur_ms": {"k": "i", "v": [20, 400]},
                    },
                },
                "token_applied_invalid_warp": {
                    "lvl": "INFO",
                    "msg": "edge_config applied account={account} token_id={token_id} client_secret_len={secret_len} rev={rev}",
                    "vars": {
                        "account": {"k": "ch", "v": ["warp_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "secret_len": {"k": "i", "v": [0, 0]},
                        "rev": {"k": "i", "v": [1000, 5000]},
                    },
                },
                "token_applied_invalid_api": {
                    "lvl": "INFO",
                    "msg": "edge_config applied account={account} token_id={token_id} client_secret_len={secret_len} rev={rev}",
                    "vars": {
                        "account": {"k": "ch", "v": ["api_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "secret_len": {"k": "i", "v": [0, 0]},
                        "rev": {"k": "i", "v": [1000, 5000]},
                    },
                },
                "token_applied_valid_warp": {
                    "lvl": "INFO",
                    "msg": "edge_config applied account={account} token_id={token_id} client_secret_len={secret_len} rev={rev}",
                    "vars": {
                        "account": {"k": "ch", "v": ["warp_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "secret_len": {"k": "i", "v": [32, 64]},
                        "rev": {"k": "i", "v": [5001, 9000]},
                    },
                },
                "sync_health": {
                    "lvl": "INFO",
                    "msg": "sync health ok=true edge_push_backlog={backlog} apply_errors={errors}",
                    "vars": {"backlog": {"k": "i", "v": [0, 20]}, "errors": {"k": "i", "v": [0, 3]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "sync_cycle", "per_min": 2.0, "scope": "per_host"},
                    {"id": "sync_health", "per_min": 0.5, "scope": "per_host"},
                ],
                "f": [
                    {"id": "sync_cycle", "per_min": 2.0, "scope": "per_host"},
                    {"id": "sync_health", "per_min": 0.5, "scope": "per_host"},
                ],
            },
        },
        "edge_auth_gateway": {
            "svc": "edge-access",
            "hosts": ["edge-ams-1", "edge-fra-1", "edge-sjc-1"],
            "logs": {
                "edge_access_ok_posture": {
                    "lvl": "INFO",
                    "msg": "request ok req_id={req_id} method={method} route={route} account={account} token_id={token_id} status=200 latency_ms={lat_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/device/posture/upload", "/device/posture/read"]},
                        "account": {"k": "ch", "v": ["warp_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "lat_ms": {"k": "i", "v": [5, 140]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_access_denied_posture": {
                    "lvl": "WARN",
                    "msg": "request denied req_id={req_id} method={method} route={route} account={account} token_id={token_id} status=403 reason={reason} latency_ms={lat_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/device/posture/upload", "/device/posture/read"]},
                        "account": {"k": "ch", "v": ["warp_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["invalid_service_token"]},
                        "lat_ms": {"k": "i", "v": [1, 60]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_access_ok_internal": {
                    "lvl": "INFO",
                    "msg": "request ok req_id={req_id} method={method} route={route} account={account} token_id={token_id} status=200 latency_ms={lat_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/internal/api"]},
                        "account": {"k": "ch", "v": ["api_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "lat_ms": {"k": "i", "v": [5, 140]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_access_denied_internal": {
                    "lvl": "WARN",
                    "msg": "request denied req_id={req_id} method={method} route={route} account={account} token_id={token_id} status=403 reason={reason} latency_ms={lat_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/internal/api"]},
                        "account": {"k": "ch", "v": ["api_internal"]},
                        "token_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["invalid_service_token"]},
                        "lat_ms": {"k": "i", "v": [1, 60]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_health": {
                    "lvl": "INFO",
                    "msg": "edge health cpu_pct={cpu} mem_pct={mem} active_conns={conns}",
                    "vars": {"cpu": {"k": "i", "v": [10, 85]}, "mem": {"k": "i", "v": [20, 90]}, "conns": {"k": "i", "v": [200, 5000]}},
                },
                "auth_denied_metric": {
                    "lvl": "INFO",
                    "msg": "metric auth_denied_per_min={denied} route_group={group}",
                    "vars": {"group": {"k": "ch", "v": ["device_posture", "internal_api"]}},
                    "state_vars": {"n": {"denied": {"k": "i", "v": [0, 80]}}, "f": {"denied": {"k": "i", "v": [0, 120]}}},
                },
                "auth_denied_metric_spike_posture": {
                    "lvl": "INFO",
                    "msg": "metric auth_denied_per_min={denied} route_group={group}",
                    "vars": {"denied": {"k": "i", "v": [250, 900]}, "group": {"k": "ch", "v": ["device_posture"]}},
                },
                "auth_denied_metric_spike_internal": {
                    "lvl": "INFO",
                    "msg": "metric auth_denied_per_min={denied} route_group={group}",
                    "vars": {"denied": {"k": "i", "v": [80, 350]}, "group": {"k": "ch", "v": ["internal_api"]}},
                },
                "http_5xx_metric": {
                    "lvl": "INFO",
                    "msg": "metric http_5xx_per_min={errs} product={product}",
                    "vars": {"product": {"k": "ch", "v": ["zero_trust_gateway", "control_plane"]}},
                    "state_vars": {"n": {"errs": {"k": "i", "v": [0, 120]}}, "f": {"errs": {"k": "i", "v": [0, 200]}}},
                },
                "http_5xx_metric_spike_zero_trust": {
                    "lvl": "INFO",
                    "msg": "metric http_5xx_per_min={errs} product={product}",
                    "vars": {"errs": {"k": "i", "v": [50, 400]}, "product": {"k": "ch", "v": ["zero_trust_gateway"]}},
                },
                "http_5xx_metric_spike_control_plane": {
                    "lvl": "INFO",
                    "msg": "metric http_5xx_per_min={errs} product={product}",
                    "vars": {"errs": {"k": "i", "v": [20, 250]}, "product": {"k": "ch", "v": ["control_plane"]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "edge_health", "per_min": 1.0, "scope": "per_host"},
                    {"id": "auth_denied_metric", "per_min": 1.0, "scope": "global"},
                    {"id": "http_5xx_metric", "per_min": 1.0, "scope": "global"},
                ],
                "f": [
                    {"id": "edge_health", "per_min": 1.0, "scope": "per_host"},
                    {"id": "auth_denied_metric", "per_min": 1.0, "scope": "global"},
                    {"id": "auth_denied_metric_spike_posture", "per_min": 1.0, "scope": "global"},
                    {"id": "auth_denied_metric_spike_internal", "per_min": 1.0, "scope": "global"},
                    {"id": "http_5xx_metric", "per_min": 1.0, "scope": "global"},
                    {"id": "http_5xx_metric_spike_zero_trust", "per_min": 1.0, "scope": "global"},
                    {"id": "http_5xx_metric_spike_control_plane", "per_min": 1.0, "scope": "global"},
                ],
            },
        },
        "device_state_service": {
            "svc": "device-state",
            "hosts": ["devstate-1", "devstate-2"],
            "logs": {
                "posture_ingest_ok": {
                    "lvl": "INFO",
                    "msg": "posture ingest ok device_id={device_id} bytes={bytes} latency_ms={lat_ms}",
                    "vars": {"device_id": {"k": "hex", "v": 12}, "bytes": {"k": "i", "v": [200, 6000]}, "lat_ms": {"k": "i", "v": [5, 250]}},
                },
                "posture_read_ok": {
                    "lvl": "INFO",
                    "msg": "posture read ok subject_id={subj} cache={cache} latency_ms={lat_ms}",
                    "vars": {"subj": {"k": "hex", "v": 10}, "cache": {"k": "ch", "v": ["hit", "miss"]}, "lat_ms": {"k": "i", "v": [3, 160]}},
                },
                "svc_health": {
                    "lvl": "INFO",
                    "msg": "svc health ok=true req_queue={q} db_latency_ms={db_ms}",
                    "vars": {"q": {"k": "i", "v": [0, 200]}, "db_ms": {"k": "i", "v": [2, 60]}},
                },
            },
            "beh": {
                "n": [{"id": "svc_health", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "svc_health", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        "gateway_policy_engine": {
            "svc": "zt-gateway",
            "hosts": ["gateway-1", "gateway-2"],
            "logs": {
                "posture_fetch_ok": {
                    "lvl": "INFO",
                    "msg": "posture fetch ok subject_id={subj} upstream_status=200 duration_ms={dur_ms} applied_action={action}",
                    "vars": {"subj": {"k": "hex", "v": 10}, "dur_ms": {"k": "i", "v": [10, 350]}, "action": {"k": "ch", "v": ["allow"]}},
                },
                "posture_fetch_error": {
                    "lvl": "ERROR",
                    "msg": "posture fetch error subject_id={subj} upstream_status={up_status} duration_ms={dur_ms} applied_action={action}",
                    "vars": {"subj": {"k": "hex", "v": 10}, "up_status": {"k": "ch", "v": ["401", "403", "503"]}, "dur_ms": {"k": "i", "v": [5, 400]}, "action": {"k": "ch", "v": ["block"]}},
                },
                "policy_block_metric": {
                    "lvl": "WARN",
                    "msg": "metric policy_blocks_per_min={blocks} reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["posture_unavailable"]}},
                    "state_vars": {"n": {"blocks": {"k": "i", "v": [0, 80]}}, "f": {"blocks": {"k": "i", "v": [0, 120]}}},
                },
                "policy_block_metric_spike": {
                    "lvl": "WARN",
                    "msg": "metric policy_blocks_per_min={blocks} reason={reason}",
                    "vars": {"blocks": {"k": "i", "v": [80, 300]}, "reason": {"k": "ch", "v": ["posture_unavailable"]}},
                },
                "alert_posture_upload_drop": {
                    "lvl": "CRITICAL",
                    "msg": "alert posture_uploads_success dropped observed_rpm={rpm} window_min={window}",
                    "vars": {"rpm": {"k": "i", "v": [0, 20]}, "window": {"k": "i", "v": [3, 10]}},
                },
                "gateway_health": {
                    "lvl": "INFO",
                    "msg": "gateway health ok=true worker_lag_ms={lag_ms}",
                    "vars": {"lag_ms": {"k": "i", "v": [0, 1200]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "gateway_health", "per_min": 0.5, "scope": "per_host"},
                    {"id": "policy_block_metric", "per_min": 1.0, "scope": "global"},
                ],
                "f": [
                    {"id": "gateway_health", "per_min": 0.5, "scope": "per_host"},
                    {"id": "policy_block_metric", "per_min": 1.0, "scope": "global"},
                    {"id": "policy_block_metric_spike", "per_min": 1.0, "scope": "global"},
                ],
            },
        },
        "api_orchestrator_service": {
            "svc": "control-plane",
            "hosts": ["cp-1", "cp-2"],
            "logs": {
                "api_call_ok": {
                    "lvl": "INFO",
                    "msg": "api call ok op={op} account={account} upstream_status=200 duration_ms={dur_ms} attempt={attempt} trace_id={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["cache_purge", "r2_admin", "images_purge"]},
                        "account": {"k": "ch", "v": ["api_internal"]},
                        "dur_ms": {"k": "i", "v": [15, 500]},
                        "attempt": {"k": "i", "v": [1, 1]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_call_denied": {
                    "lvl": "ERROR",
                    "msg": "api call denied op={op} account={account} upstream_status=403 duration_ms={dur_ms} attempt={attempt} trace_id={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["cache_purge", "r2_admin", "images_purge"]},
                        "account": {"k": "ch", "v": ["api_internal"]},
                        "dur_ms": {"k": "i", "v": [10, 400]},
                        "attempt": {"k": "i", "v": [1, 3]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_retry_scheduled": {
                    "lvl": "WARN",
                    "msg": "api retry scheduled op={op} next_attempt={attempt} backoff_ms={backoff}",
                    "vars": {"op": {"k": "ch", "v": ["cache_purge", "r2_admin", "images_purge"]}, "attempt": {"k": "i", "v": [2, 3]}, "backoff": {"k": "i", "v": [50, 600]}},
                },
                "cp_health": {
                    "lvl": "INFO",
                    "msg": "cp health ok=true inflight={inflight} q_depth={q}",
                    "vars": {"inflight": {"k": "i", "v": [0, 500]}, "q": {"k": "i", "v": [0, 2000]}},
                },
            },
            "beh": {
                "n": [{"id": "cp_health", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "cp_health", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        "incident_bot": {
            "svc": "incident-bot",
            "hosts": ["ops-1"],
            "logs": {
                "incident_declared": {
                    "lvl": "WARN",
                    "msg": "incident declared id={inc_id} severity={sev} primary_signal={signal}",
                    "vars": {"inc_id": {"k": "ch", "v": ["inc_2023_01_24_service_tokens"]}, "sev": {"k": "ch", "v": ["sev2"]}, "signal": {"k": "ch", "v": ["posture_upload_drop"]}},
                },
                "scope_expanded": {
                    "lvl": "WARN",
                    "msg": "incident scope expanded id={inc_id} added={added}",
                    "vars": {"inc_id": {"k": "ch", "v": ["inc_2023_01_24_service_tokens"]}, "added": {"k": "ch", "v": ["control_plane_products"]}},
                },
                "bot_heartbeat": {"lvl": "DEBUG", "msg": "incident-bot heartbeat ok=true", "vars": {}},
            },
            "beh": {"n": [{"id": "bot_heartbeat", "per_min": 0.2, "scope": "global"}], "f": [{"id": "bot_heartbeat", "per_min": 0.2, "scope": "global"}]},
        },
    },
    "flows": {
        "n": [
            {
                "id": "posture_upload_ok",
                "rpm": 300.0,
                "emit": ["device_state_service.posture_ingest_ok", "edge_auth_gateway.edge_access_ok_posture"],
                "latency_ms": [[10, 80], [8, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "posture_read_ok",
                "rpm": 150.0,
                "emit": ["device_state_service.posture_read_ok", "edge_auth_gateway.edge_access_ok_posture", "gateway_policy_engine.posture_fetch_ok"],
                "latency_ms": [[6, 45], [6, 30], [12, 220]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "internal_api_call_ok",
                "rpm": 80.0,
                "emit": ["edge_auth_gateway.edge_access_ok_internal", "api_orchestrator_service.api_call_ok"],
                "latency_ms": [[6, 40], [18, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "posture_upload_ok_f",
                "rpm": 300.0,
                "emit": ["device_state_service.posture_ingest_ok", "edge_auth_gateway.edge_access_ok_posture"],
                "latency_ms": [[10, 90], [8, 45]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "posture_upload_denied_f",
                "rpm": 300.0,
                "emit": ["edge_auth_gateway.edge_access_denied_posture"],
                "latency_ms": [[2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "posture_read_ok_f",
                "rpm": 150.0,
                "emit": ["device_state_service.posture_read_ok", "edge_auth_gateway.edge_access_ok_posture", "gateway_policy_engine.posture_fetch_ok"],
                "latency_ms": [[6, 55], [6, 35], [12, 260]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "posture_read_denied_f",
                "rpm": 150.0,
                "emit": ["edge_auth_gateway.edge_access_denied_posture", "gateway_policy_engine.posture_fetch_error"],
                "latency_ms": [[2, 20], [10, 180]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "internal_api_call_ok_f",
                "rpm": 80.0,
                "emit": ["edge_auth_gateway.edge_access_ok_internal", "api_orchestrator_service.api_call_ok"],
                "latency_ms": [[6, 45], [18, 360]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "internal_api_call_denied_f",
                "rpm": 80.0,
                "emit": ["edge_auth_gateway.edge_access_denied_internal", "api_orchestrator_service.api_call_denied"],
                "latency_ms": [[2, 20], [12, 260]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 2.2,
                    "emit_per_retry": ["api_orchestrator_service.api_retry_scheduled"],
                    "backoff_ms": [[60, 220], [120, 500]],
                },
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "inc_2023_01_24_service_tokens_overwrite_staggered_sync",
    "time": {"total_minutes": 56, "phases": {"n": {"start_min": 0, "end_min": 28}, "f": {"start_min": 28, "end_min": 56}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 28,
                    "rate_multipliers": {
                        "posture_upload_denied_f": 0.0,
                        "posture_read_denied_f": 0.0,
                        "internal_api_call_denied_f": 0.0,
                        "edge_auth_gateway.auth_denied_metric_spike_posture": 0.0,
                        "edge_auth_gateway.auth_denied_metric_spike_internal": 0.0,
                        "edge_auth_gateway.http_5xx_metric_spike_zero_trust": 0.0,
                        "edge_auth_gateway.http_5xx_metric_spike_control_plane": 0.0,
                        "gateway_policy_engine.policy_block_metric_spike": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "access_token_service.deploy_started", "count": 1, "hosts": ["access-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 31,
                    "rate_multipliers": {"access_token_service.last_seen_consumer_poll": 0.0, "access_token_service.token_update_write_bug": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "access_token_service.deploy_rolled_back", "count": 1, "hosts": ["access-2"]}],
                },
                {
                    "order": 3,
                    "at_min": 39,
                    "rate_multipliers": {
                        "posture_upload_ok_f": 0.0,
                        "posture_upload_denied_f": 1.0,
                        "posture_read_ok_f": 0.0,
                        "posture_read_denied_f": 1.0,
                        "edge_auth_gateway.auth_denied_metric": 0.0,
                        "edge_auth_gateway.http_5xx_metric": 0.0,
                        "gateway_policy_engine.policy_block_metric": 0.0,
                        "edge_auth_gateway.auth_denied_metric_spike_posture": 1.0,
                        "edge_auth_gateway.http_5xx_metric_spike_zero_trust": 1.0,
                        "gateway_policy_engine.policy_block_metric_spike": 1.0,
                    },
                    "latency_multipliers": {"posture_read_denied_f": {"p50": 1.0, "p95": 1.1}},
                    "one_shots": [{"ref": "config_sync.token_applied_invalid_warp", "count": 1, "hosts": ["sync-1"]}],
                },
                {
                    "order": 4,
                    "at_min": 43,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "gateway_policy_engine.alert_posture_upload_drop", "count": 1, "hosts": ["gateway-1"]},
                        {"ref": "incident_bot.incident_declared", "count": 1, "hosts": ["ops-1"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 45,
                    "rate_multipliers": {
                        "internal_api_call_ok_f": 0.0,
                        "internal_api_call_denied_f": 1.0,
                        "edge_auth_gateway.auth_denied_metric_spike_internal": 1.0,
                        "edge_auth_gateway.http_5xx_metric_spike_control_plane": 1.0,
                    },
                    "latency_multipliers": {"internal_api_call_denied_f": {"p50": 1.0, "p95": 1.2}},
                    "one_shots": [
                        {"ref": "config_sync.token_applied_invalid_api", "count": 1, "hosts": ["sync-1"]},
                        {"ref": "incident_bot.scope_expanded", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "access_token_service.account_changes_locked", "count": 1, "hosts": ["access-1"]},
                    ],
                },
                {
                    "order": 6,
                    "at_min": 51,
                    "rate_multipliers": {
                        "posture_upload_ok_f": 1.0,
                        "posture_upload_denied_f": 0.0,
                        "posture_read_ok_f": 1.0,
                        "posture_read_denied_f": 0.0,
                        "edge_auth_gateway.auth_denied_metric_spike_posture": 0.0,
                        "edge_auth_gateway.http_5xx_metric_spike_zero_trust": 0.0,
                        "gateway_policy_engine.policy_block_metric_spike": 0.0,
                        "gateway_policy_engine.policy_block_metric": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "access_token_service.manual_token_restore_warp", "count": 1, "hosts": ["access-2"]},
                        {"ref": "config_sync.token_applied_valid_warp", "count": 1, "hosts": ["sync-1"]},
                    ],
                },
            ]
        }
    },
}

# ----------------------- Deterministic helpers -----------------------


def md5_int(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest(), "big")


def stable_u01(s: str) -> float:
    return (md5_int(s) % 10_000_000) / 10_000_000.0


def stable_hex(s: str, n: int) -> str:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    if n <= len(h):
        return h[:n]
    out = h
    while len(out) < n:
        out += hashlib.md5((s + out).encode("utf-8")).hexdigest()
    return out[:n]


def stable_uuid(s: str) -> str:
    d = hashlib.md5(s.encode("utf-8")).digest()
    u = uuid.UUID(bytes=d)
    return str(u)


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if stable_u01(f"round:{key}") < frac else 0)


def choose_from_list(lst: List[Any], key: str) -> Any:
    if not lst:
        return None
    idx = md5_int(f"ch:{key}") % len(lst)
    return lst[idx]


def sample_int(lo: int, hi: int, key: str) -> int:
    if hi <= lo:
        return int(lo)
    return lo + (md5_int(f"i:{key}") % (hi - lo + 1))


def sample_float(lo: float, hi: float, key: str) -> float:
    if hi <= lo:
        return float(lo)
    return lo + (hi - lo) * stable_u01(f"f:{key}")


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722 if p95 > p50 else 0.0
    z = NormalDist().inv_cdf(min(0.999999, max(0.000001, u)))
    return math.exp(mu + sigma * z)


def bounded_lognormal_ms(p50: float, p95: float, key: str, cap_mult: float = 2.5, hard_min: int = 1, hard_max: Optional[int] = None) -> int:
    """
    Deterministic lognormal sampler with soft cap (cap_mult * p95) and optional hard cap.
    Used for both per-step latencies and retry backoffs. When messages carry timing fields,
    callers also enforce template-domain constraints in the chain planning step.
    """
    u = stable_u01(f"ln:{key}")
    v = lognormal_from_p50_p95(p50, p95, u)
    soft_cap = max(p95, 1.0) * cap_mult
    cap = soft_cap if hard_max is None else min(soft_cap, float(hard_max))
    v = min(v, cap)
    v = max(float(hard_min), v)
    return int(round(v))


PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def extract_placeholders(msg: str) -> List[str]:
    return list(dict.fromkeys(PLACEHOLDER_RE.findall(msg)))


def sample_domain(domain: Dict[str, Any], key: str) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return choose_from_list(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return sample_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return sample_float(lo, hi, key)
    if k == "uuid":
        return stable_uuid(f"uuid:{key}")
    if k == "hex":
        n = int(v)
        return stable_hex(f"hex:{key}", n)
    if k == "ip":
        return f"192.0.2.{1 + (md5_int(key) % 250)}"
    if k == "str":
        return f"s-{stable_hex(key, 8)}"
    return ""


def get_var_domain(comp_id: str, log_id: str, var_name: str, state: str) -> Optional[Dict[str, Any]]:
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    dom = None
    if "vars" in tmpl and var_name in tmpl["vars"]:
        dom = tmpl["vars"][var_name]
    st = tmpl.get("state_vars", {})
    if state in st and var_name in st[state]:
        dom = st[state][var_name]
    return dom


def get_int_range(comp_id: str, log_id: str, var_name: str, state: str) -> Optional[Tuple[int, int]]:
    dom = get_var_domain(comp_id, log_id, var_name, state)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    v = dom.get("v")
    return int(v[0]), int(v[1])


def clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


# ----------------------- Control intervals -----------------------


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> Tuple[List[Interval], List[Dict[str, Any]]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted({f_start, f_end} | {e["at_min"] for e in events})
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    idx = 0
    intervals: List[Interval] = []
    one_shots: List[Dict[str, Any]] = []
    for b_i in range(len(boundaries) - 1):
        s = boundaries[b_i]
        e = boundaries[b_i + 1]
        while idx < len(events) and events[idx]["at_min"] == s:
            ev = events[idx]
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                active_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
            for os_ in ev.get("one_shots", []):
                one_shots.append({"at_min": ev["at_min"], **os_})
            idx += 1
        intervals.append(Interval(state="f", start_min=s, end_min=e, rate_mult=dict(active_rate), latency_mult=dict(active_lat)))
    return intervals, one_shots


# ----------------------- Simulation -----------------------


def render_log(
    comp_id: str,
    log_id: str,
    state: str,
    overrides: Dict[str, Any],
    key: str,
) -> Tuple[str, str]:
    comp = SYSTEM["components"][comp_id]
    tmpl = comp["logs"][log_id]
    msg = tmpl["msg"]
    lvl = tmpl["lvl"]
    placeholders = extract_placeholders(msg)

    vars_domains = dict(tmpl.get("vars", {}))
    state_vars = tmpl.get("state_vars", {})
    if state in state_vars:
        for k2, dom in state_vars[state].items():
            vars_domains[k2] = dom

    vals: Dict[str, Any] = {}
    for p in placeholders:
        if p in overrides:
            vals[p] = overrides[p]
        elif p in vars_domains:
            vals[p] = sample_domain(vars_domains[p], f"{key}:{comp_id}.{log_id}:{p}")
        else:
            vals[p] = ""

    for k2, v2 in overrides.items():
        vals[k2] = v2

    try:
        rendered = msg.format(**vals)
    except Exception:
        rendered = msg
        for p in placeholders:
            rendered = rendered.replace("{" + p + "}", str(vals.get(p, "")))
    return lvl, rendered


def schedule_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    dur = (end - start).total_seconds()
    if dur <= 0:
        return [start] * n
    out: List[datetime] = []
    for i in range(n):
        frac = (i + 0.5) / n
        base_t = start + timedelta(seconds=frac * dur)
        j = (stable_u01(f"jit:{key}:{i}") - 0.5) * 0.4  # +/- 0.2s
        t = base_t + timedelta(seconds=j)
        # keep within [start, end) for interval semantics
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def pick_host_for_component(comp_id: str, key: str) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts", [])
    if not hosts:
        return ""
    return hosts[md5_int(f"host:{key}:{comp_id}") % len(hosts)]


def base_time() -> datetime:
    return datetime(2023, 1, 24, 16, 0, 0, tzinfo=timezone.utc)


def flow_attempt_count(max_attempts: int, expected_attempts: float, key: str) -> int:
    max_attempts = max(1, int(max_attempts))
    e = max(1.0, float(expected_attempts))
    lo = int(math.floor(e))
    hi = min(max_attempts, lo + 1)
    if hi == lo:
        return lo
    w = e - lo
    return hi if stable_u01(f"attempts:{key}") < w else lo


def enforce_elapsed_constraints(
    delays_ms: List[int],
    constraints: List[Tuple[int, int, int]],
    key: str,
) -> List[int]:
    """
    Enforce that cumulative elapsed time at certain step indices stays within [lo, hi]
    as declared by the emitting log template domains (e.g., lat_ms/dur_ms).
    This keeps emitted timestamps and rendered timing fields coherent and in-domain.
    """
    if not constraints or not delays_ms:
        return delays_ms

    by_step_hi: Dict[int, List[int]] = {}
    by_step_lo: Dict[int, List[int]] = {}
    for step_idx, lo, hi in constraints:
        by_step_hi.setdefault(step_idx, []).append(hi)
        by_step_lo.setdefault(step_idx, []).append(lo)

    # Iteratively scale down if any cumulative elapsed exceeds a hi bound.
    for it in range(4):
        cum = 0
        ratios: List[float] = []
        for i, d in enumerate(delays_ms):
            cum += d
            if i in by_step_hi:
                hi = min(by_step_hi[i])
                if cum > hi and cum > 0:
                    ratios.append(hi / float(cum))
        if not ratios:
            break
        factor = min(ratios) * 0.995  # slight undershoot to absorb rounding
        if factor >= 1.0:
            break
        delays_ms = [max(1, int(round(d * factor))) for d in delays_ms]

    # Final per-step clamp to fix any residual due to rounding; then ensure mins.
    cum = 0
    for i in range(len(delays_ms)):
        cum += delays_ms[i]
        if i in by_step_hi:
            hi = min(by_step_hi[i])
            if cum > hi:
                diff = cum - hi
                reducible = delays_ms[i] - 1
                red = min(diff, reducible)
                delays_ms[i] -= red
                cum -= red
        if i in by_step_lo:
            lo = max(by_step_lo[i])
            if cum < lo:
                inc = lo - cum
                delays_ms[i] += inc
                cum += inc

    # One more pass of hi clamps after potential increases.
    cum = 0
    for i in range(len(delays_ms)):
        cum += delays_ms[i]
        if i in by_step_hi:
            hi = min(by_step_hi[i])
            if cum > hi:
                diff = cum - hi
                reducible = delays_ms[i] - 1
                red = min(diff, reducible)
                delays_ms[i] -= red
                cum -= red

    return delays_ms


def simulate_flow_instance(
    state: str,
    flow: Dict[str, Any],
    start_ts: datetime,
    rate_key: str,
    latency_mult: Optional[Dict[str, float]],
    token_ids: Dict[str, str],
    records: List[Dict[str, Any]],
) -> None:
    flow_id = flow["id"]
    trace_on = SYSTEM["tracing"]["on"] and bool(flow.get("trace", False))
    trace_id = stable_hex(f"trace:{state}:{flow_id}:{rate_key}", 32) if trace_on else ""
    max_attempts = int(flow["retry"]["max_attempts"])
    expected_attempts = float(flow["retry"]["expected_attempts"])
    attempts = flow_attempt_count(max_attempts, expected_attempts, f"{trace_id}:{rate_key}:{flow_id}")

    involved_components: List[str] = []
    for ref in flow["emit"]:
        involved_components.append(ref.split(".", 1)[0])
    for ref in flow["retry"].get("emit_per_retry", []):
        involved_components.append(ref.split(".", 1)[0])
    involved_components = list(dict.fromkeys(involved_components))
    chain_hosts = {cid: pick_host_for_component(cid, f"{trace_id}:{rate_key}") for cid in involved_components}

    if flow_id.startswith("posture_upload"):
        route = "/device/posture/upload"
        method = "POST"
        account = "warp_internal"
        token_id = token_ids["warp_internal"]
        device_id = stable_hex(f"device:{trace_id}", 12)
        bytes_ = 200 + (md5_int(f"bytes:{trace_id}") % (6000 - 200 + 1))
        subj = None
    elif flow_id.startswith("posture_read"):
        route = "/device/posture/read"
        method = "GET"
        account = "warp_internal"
        token_id = token_ids["warp_internal"]
        device_id = None
        bytes_ = None
        subj = stable_hex(f"subj:{trace_id}", 10)
    else:
        route = "/internal/api"
        method = choose_from_list(["POST", "GET"], f"method:{trace_id}")
        account = "api_internal"
        token_id = token_ids["api_internal"]
        device_id = None
        bytes_ = None
        subj = None
    op = choose_from_list(["cache_purge", "r2_admin", "images_purge"], f"op:{trace_id}") if "internal_api_call" in flow_id else None

    lat_hints: List[List[float]] = flow["latency_ms"]
    lm_p50 = float(latency_mult.get("p50", 1.0)) if latency_mult else 1.0
    lm_p95 = float(latency_mult.get("p95", 1.0)) if latency_mult else 1.0

    def scaled_hint(pair: List[float]) -> Tuple[float, float]:
        p50, p95 = float(pair[0]), float(pair[1])
        return p50 * lm_p50, p95 * lm_p95

    backoff_hints: List[List[float]] = flow["retry"].get("backoff_ms", [])

    current_attempt_start = start_ts
    for attempt in range(1, attempts + 1):
        req_id = stable_hex(f"req:{trace_id}:{attempt}", 16)

        # Sample per-step delays.
        delays_ms: List[int] = []
        for j, pair in enumerate(lat_hints):
            p50s, p95s = scaled_hint(pair)
            d = bounded_lognormal_ms(
                p50s,
                p95s,
                key=f"{trace_id}:{flow_id}:{rate_key}:a{attempt}:s{j}",
                cap_mult=2.5,
                hard_min=1,
                hard_max=None,
            )
            delays_ms.append(d)

        # Build constraints from the log templates when we render elapsed-style fields.
        constraints: List[Tuple[int, int, int]] = []
        for step, ref in enumerate(flow["emit"]):
            comp_id, log_id = ref.split(".", 1)
            # The script overrides these elapsed-carrying timing fields; keep in-domain.
            var_name: Optional[str] = None
            if comp_id == "edge_auth_gateway":
                var_name = "lat_ms"
            elif comp_id == "device_state_service":
                var_name = "lat_ms"
            elif comp_id == "gateway_policy_engine":
                var_name = "dur_ms"
            elif comp_id == "api_orchestrator_service":
                var_name = "dur_ms"

            if var_name:
                r = get_int_range(comp_id, log_id, var_name, state)
                if r:
                    constraints.append((step, r[0], r[1]))

        delays_ms = enforce_elapsed_constraints(delays_ms, constraints, key=f"{trace_id}:{rate_key}:{attempt}")

        elapsed = 0
        for step, ref in enumerate(flow["emit"]):
            elapsed += delays_ms[step]
            ts = current_attempt_start + timedelta(milliseconds=elapsed)
            comp_id, log_id = ref.split(".", 1)

            overrides: Dict[str, Any] = {}
            if comp_id == "edge_auth_gateway":
                # lat_ms is the observed request latency at the edge; keep within template domain by construction.
                overrides.update(
                    {
                        "req_id": req_id,
                        "method": method,
                        "route": route,
                        "account": account,
                        "token_id": token_id,
                        "lat_ms": int(elapsed),
                        "trace_id": trace_id,
                    }
                )
            elif comp_id == "device_state_service":
                overrides.update({"lat_ms": int(elapsed)})
                if log_id == "posture_ingest_ok":
                    overrides.update({"device_id": device_id, "bytes": int(bytes_)})
                elif log_id == "posture_read_ok":
                    overrides.update({"subj": subj, "cache": choose_from_list(["hit", "miss"], f"cache:{trace_id}")})
            elif comp_id == "gateway_policy_engine":
                if log_id == "posture_fetch_ok":
                    overrides.update({"subj": subj, "dur_ms": int(elapsed), "action": "allow"})
                elif log_id == "posture_fetch_error":
                    overrides.update({"subj": subj, "up_status": "403", "dur_ms": int(elapsed), "action": "block"})
            elif comp_id == "api_orchestrator_service":
                if log_id == "api_call_ok":
                    overrides.update({"op": op, "account": account, "dur_ms": int(elapsed), "attempt": 1, "trace_id": trace_id})
                elif log_id == "api_call_denied":
                    overrides.update({"op": op, "account": account, "dur_ms": int(elapsed), "attempt": attempt, "trace_id": trace_id})

            lvl, msg = render_log(comp_id, log_id, state=state, overrides=overrides, key=f"{trace_id}:{rate_key}:{attempt}:{step}")
            records.append(
                {
                    "timestamp": ts,
                    "level": lvl,
                    "message": msg,
                    "trace_id": trace_id,
                    "service": SYSTEM["components"][comp_id]["svc"] or "",
                    "host": chain_hosts.get(comp_id, "") or "",
                }
            )

        if attempt < attempts:
            bo_pair = backoff_hints[attempt - 1] if (attempt - 1) < len(backoff_hints) else [50.0, 200.0]
            # Constrain backoff to the retry log template domain when present.
            bo_dom = get_int_range("api_orchestrator_service", "api_retry_scheduled", "backoff", state)
            bo_lo, bo_hi = (bo_dom if bo_dom else (1, 10_000))
            backoff_ms = bounded_lognormal_ms(
                float(bo_pair[0]),
                float(bo_pair[1]),
                key=f"{trace_id}:{flow_id}:{rate_key}:bo{attempt}",
                cap_mult=2.5,
                hard_min=bo_lo,
                hard_max=bo_hi,
            )
            backoff_ms = clamp_int(int(backoff_ms), bo_lo, bo_hi)

            for rstep, rref in enumerate(flow["retry"].get("emit_per_retry", [])):
                comp_id, log_id = rref.split(".", 1)
                ts_retry = (current_attempt_start + timedelta(milliseconds=elapsed)) + timedelta(milliseconds=1 + rstep)
                overrides = {"op": op, "attempt": attempt + 1, "backoff": int(backoff_ms)}
                lvl, msg = render_log(comp_id, log_id, state=state, overrides=overrides, key=f"{trace_id}:{rate_key}:retry:{attempt}:{rstep}")
                records.append(
                    {
                        "timestamp": ts_retry,
                        "level": lvl,
                        "message": msg,
                        "trace_id": trace_id,
                        "service": SYSTEM["components"][comp_id]["svc"] or "",
                        "host": chain_hosts.get(comp_id, "") or "",
                    }
                )

            current_attempt_start = (current_attempt_start + timedelta(milliseconds=elapsed + 2)) + timedelta(milliseconds=backoff_ms)


def simulate_background_interval(
    state: str,
    start_ts: datetime,
    end_ts: datetime,
    rate_mult: Optional[Dict[str, float]],
    records: List[Dict[str, Any]],
    token_ids: Dict[str, str],
) -> None:
    duration_min = (end_ts - start_ts).total_seconds() / 60.0
    if duration_min <= 0:
        return
    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f" and rate_mult is not None:
                mult = float(rate_mult.get(mult_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * duration_min
                n = deterministic_round(expected, key=f"bg:{state}:{mult_key}:{start_ts.isoformat()}:{end_ts.isoformat()}")
                times = schedule_times(start_ts, end_ts, n, key=f"bg:{state}:{mult_key}:{start_ts.isoformat()}")
                for i, ts in enumerate(times):
                    host = comp.get("hosts", [""])[i % max(1, len(comp.get("hosts", [""])))]

                    overrides: Dict[str, Any] = {}
                    lvl, msg = render_log(comp_id, log_id, state=state, overrides=overrides, key=f"bg:{state}:{mult_key}:{i}")
                    records.append({"timestamp": ts, "level": lvl, "message": msg, "trace_id": "", "service": comp["svc"] or "", "host": host or ""})
            else:
                hosts = comp.get("hosts", [])
                for host in hosts:
                    expected = eff_per_min * duration_min
                    n = deterministic_round(expected, key=f"bg:{state}:{mult_key}:{host}:{start_ts.isoformat()}:{end_ts.isoformat()}")
                    times = schedule_times(start_ts, end_ts, n, key=f"bg:{state}:{mult_key}:{host}:{start_ts.isoformat()}")
                    for i, ts in enumerate(times):
                        overrides = {}
                        lvl, msg = render_log(comp_id, log_id, state=state, overrides=overrides, key=f"bg:{state}:{mult_key}:{host}:{i}")
                        records.append({"timestamp": ts, "level": lvl, "message": msg, "trace_id": "", "service": comp["svc"] or "", "host": host or ""})


def emit_one_shots(one_shots: List[Dict[str, Any]], records: List[Dict[str, Any]], token_ids: Dict[str, str]) -> None:
    bt = base_time()
    for idx, os_ in enumerate(one_shots):
        at_min = int(os_["at_min"])
        ref = os_["ref"]
        count = int(os_["count"])
        hosts = list(os_.get("hosts", []))
        comp_id, log_id = ref.split(".", 1)
        comp = SYSTEM["components"][comp_id]

        for j in range(count):
            t0 = bt + timedelta(minutes=at_min) + timedelta(milliseconds=int(50 + 900 * stable_u01(f"os:{idx}:{j}:{ref}")))
            host = hosts[j % len(hosts)] if hosts else (comp.get("hosts", [""])[0] if comp.get("hosts") else "")

            overrides: Dict[str, Any] = {}
            if comp_id == "config_sync" and log_id in ("token_applied_invalid_warp", "token_applied_valid_warp"):
                overrides["token_id"] = token_ids["warp_internal"]
            if comp_id == "config_sync" and log_id == "token_applied_invalid_api":
                overrides["token_id"] = token_ids["api_internal"]
            if comp_id == "access_token_service" and log_id == "manual_token_restore_warp":
                overrides["token_id"] = token_ids["warp_internal"]

            lvl, msg = render_log(comp_id, log_id, state="f", overrides=overrides, key=f"os:{idx}:{j}:{ref}")
            records.append({"timestamp": t0, "level": lvl, "message": msg, "trace_id": "", "service": comp["svc"] or "", "host": host or ""})


def simulate() -> pd.DataFrame:
    bt = base_time()
    records: List[Dict[str, Any]] = []

    token_ids = {
        "warp_internal": stable_uuid("token:warp_internal"),
        "api_internal": stable_uuid("token:api_internal"),
    }

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_ts = bt + timedelta(minutes=n_start)
    n_end_ts = bt + timedelta(minutes=n_end)

    simulate_background_interval(state="n", start_ts=n_start_ts, end_ts=n_end_ts, rate_mult=None, records=records, token_ids=token_ids)

    n_duration_min = (n_end_ts - n_start_ts).total_seconds() / 60.0
    for flow in SYSTEM["flows"]["n"]:
        expected_instances = float(flow["rpm"]) * n_duration_min
        n_instances = deterministic_round(expected_instances, key=f"flow:n:{flow['id']}:{n_start_ts.isoformat()}:{n_end_ts.isoformat()}")
        starts = schedule_times(n_start_ts, n_end_ts, n_instances, key=f"flow:n:{flow['id']}:{n_start_ts.isoformat()}")
        for i, st in enumerate(starts):
            simulate_flow_instance(
                state="n",
                flow=flow,
                start_ts=st,
                rate_key=f"n:{flow['id']}:{i}",
                latency_mult=None,
                token_ids=token_ids,
                records=records,
            )

    f_intervals, one_shots = build_failure_intervals()
    emit_one_shots(one_shots, records, token_ids)

    for interval in f_intervals:
        start_ts = bt + timedelta(minutes=interval.start_min)
        end_ts = bt + timedelta(minutes=interval.end_min)

        simulate_background_interval(state="f", start_ts=start_ts, end_ts=end_ts, rate_mult=interval.rate_mult, records=records, token_ids=token_ids)

        dur_min = (end_ts - start_ts).total_seconds() / 60.0
        for flow in SYSTEM["flows"]["f"]:
            fid = flow["id"]
            mult = float(interval.rate_mult.get(fid, 1.0))
            eff_rpm = float(flow["rpm"]) * mult
            if eff_rpm <= 0:
                continue
            expected_instances = eff_rpm * dur_min
            n_instances = deterministic_round(expected_instances, key=f"flow:f:{fid}:{interval.start_min}:{interval.end_min}")
            starts = schedule_times(start_ts, end_ts, n_instances, key=f"flow:f:{fid}:{interval.start_min}:{interval.end_min}")
            lat_mult = interval.latency_mult.get(fid)
            for i, st in enumerate(starts):
                simulate_flow_instance(
                    state="f",
                    flow=flow,
                    start_ts=st,
                    rate_key=f"f:{fid}:{interval.start_min}-{interval.end_min}:{i}",
                    latency_mult=lat_mult,
                    token_ids=token_ids,
                    records=records,
                )

    df = pd.DataFrame.from_records(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    ts_str = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(0, 23) + "Z"
    df["timestamp"] = ts_str

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
