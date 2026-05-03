import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "bot_feature_rollout_proxy_outage_2025_11_18"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "clickhouse_cluster": {
            "svc": "clickhouse",
            "hosts": ["ch-1", "ch-2", "ch-3"],
            "logs": {
                "ch_query_start": {
                    "lvl": "INFO",
                    "msg": "query start query_id={query_id} user={user} sql=system.columns table={table}",
                    "vars": {
                        "query_id": {"k": "uuid", "v": None},
                        "user": {"k": "ch", "v": ["bot_feature_builder", "ops_job"]},
                        "table": {"k": "ch", "v": ["http_requests_features"]},
                    },
                },
                "ch_query_result": {
                    "lvl": "INFO",
                    "msg": "query done query_id={query_id} rows={rows} elapsed_ms={elapsed_ms} shard={shard} dup_columns={dup_columns}",
                    "vars": {
                        "query_id": {"k": "uuid", "v": None},
                        "rows": {"k": "i", "v": [50, 320]},
                        "elapsed_ms": {"k": "i", "v": [10, 900]},
                        "shard": {"k": "ch", "v": ["ch-1", "ch-2", "ch-3"]},
                        "dup_columns": {"k": "ch", "v": ["true", "false"]},
                    },
                },
                "ch_cluster_health": {
                    "lvl": "INFO",
                    "msg": "health ok shard={shard} repl_lag_ms={repl_lag_ms} pending_merges={pending_merges}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["ch-1", "ch-2", "ch-3"]},
                        "repl_lag_ms": {"k": "i", "v": [0, 500]},
                        "pending_merges": {"k": "i", "v": [0, 40]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "ch_cluster_health", "per_min": 0.05}],  # default scope per_host
                "f": [{"id": "ch_cluster_health", "per_min": 0.05}],
            },
        },
        "bot_feature_builder": {
            "svc": "bot-feature-builder",
            "hosts": ["fb-1"],
            "logs": {
                "build_start": {
                    "lvl": "INFO",
                    "msg": "build start build_id={build_id} source=clickhouse system_table=system.columns table={table}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "table": {"k": "ch", "v": ["http_requests_features"]},
                    },
                },
                "build_complete_good": {
                    "lvl": "INFO",
                    "msg": "build complete build_id={build_id} file_version={file_version} features={features} file_kb={file_kb} sha={sha}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "file_version": {"k": "ch", "v": ["feat_good"]},
                        "features": {"k": "i", "v": [55, 70]},
                        "file_kb": {"k": "i", "v": [40, 80]},
                        "sha": {"k": "hex", "v": 12},
                    },
                },
                "build_complete_bad": {
                    "lvl": "WARN",
                    "msg": "build complete build_id={build_id} file_version={file_version} features={features} file_kb={file_kb} dup_ratio={dup_ratio}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "file_version": {"k": "ch", "v": ["feat_bad"]},
                        "features": {"k": "i", "v": [210, 270]},
                        "file_kb": {"k": "i", "v": [140, 240]},
                        "dup_ratio": {"k": "f", "v": [1.6, 3.5]},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
        "config_distribution": {
            "svc": "config-dist",
            "hosts": ["cd-1"],
            "logs": {
                "queued": {
                    "lvl": "INFO",
                    "msg": "queued artifact=bot_features file_version={file_version} size_kb={size_kb} targets={targets} queue_depth={queue_depth}",
                    "vars": {
                        "file_version": {"k": "ch", "v": ["feat_good", "feat_bad"]},
                        "size_kb": {"k": "i", "v": [40, 240]},
                        "targets": {"k": "i", "v": [100, 350]},
                        "queue_depth": {"k": "i", "v": [0, 5000]},
                    },
                },
                "push_complete": {
                    "lvl": "INFO",
                    "msg": "push complete artifact=bot_features file_version={file_version} succeeded={succeeded} failed={failed} elapsed_ms={elapsed_ms}",
                    "vars": {
                        "file_version": {"k": "ch", "v": ["feat_good", "feat_bad"]},
                        "succeeded": {"k": "i", "v": [80, 350]},
                        "failed": {"k": "i", "v": [0, 40]},
                        "elapsed_ms": {"k": "i", "v": [2000, 45000]},
                    },
                },
                "propagation_paused": {
                    "lvl": "WARN",
                    "msg": "propagation paused artifact=bot_features reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["bad_artifact_detected", "incident_mitigation"]}},
                },
                "manual_insert_good": {
                    "lvl": "INFO",
                    "msg": "manual insert artifact=bot_features file_version={file_version} source={source}",
                    "vars": {
                        "file_version": {"k": "ch", "v": ["feat_good"]},
                        "source": {"k": "ch", "v": ["last_known_good", "operator_upload"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "push_complete", "per_min": 0.2, "scope": "global"}],
                "f": [{"id": "push_complete", "per_min": 0.2, "scope": "global"}],
            },
        },
        "observability_enricher": {
            "svc": "obs-enricher",
            "hosts": ["obs-1", "obs-2"],
            "logs": {
                "error_enriched": {
                    "lvl": "WARN",
                    "msg": "enriched error err_type={err_type} stack_frames={stack_frames} enrich_ms={enrich_ms} dropped={dropped}",
                    "vars": {
                        "err_type": {"k": "ch", "v": ["panic_unwrap", "module_panic", "runtime_panic"]},
                        "stack_frames": {"k": "i", "v": [10, 120]},
                        "enrich_ms": {"k": "i", "v": [5, 400]},
                        "dropped": {"k": "ch", "v": ["true", "false"]},
                    },
                },
                "queue_metrics": {
                    "lvl": "INFO",
                    "msg": "queue metrics depth={depth} cpu_pct={cpu_pct} dropped_per_min={dropped_per_min}",
                    "vars": {
                        "depth": {"k": "i", "v": [0, 20000]},
                        "cpu_pct": {"k": "i", "v": [5, 100]},
                        "dropped_per_min": {"k": "i", "v": [0, 5000]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "queue_metrics", "per_min": 0.2}],
                "f": [{"id": "queue_metrics", "per_min": 2.5}],
            },
        },
        "core_proxy_fl2": {
            "svc": "fl2",
            "hosts": ["fl2-1", "fl2-2", "fl2-3"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start req_id={req_id} host={host} method={method} uri={uri} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "str", "v": "customer hostname"},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "str", "v": "/path?query"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "bot_features_over_limit": {
                    "lvl": "ERROR",
                    "msg": "bot features over limit file_version={file_version} features={features} limit={limit} action=panic",
                    "vars": {
                        "file_version": {"k": "ch", "v": ["feat_bad"]},
                        "features": {"k": "i", "v": [210, 270]},
                        "limit": {"k": "i", "v": [200, 200]},
                    },
                },
                "req_responded_200": {
                    "lvl": "INFO",
                    "msg": "req done req_id={req_id} status=200 dur_ms={dur_ms} cache={cache} bot_score={bot_score}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [5, 2000]},
                        "cache": {"k": "ch", "v": ["HIT", "MISS"]},
                        "bot_score": {"k": "i", "v": [1, 99]},
                    },
                },
                "req_responded_502": {
                    "lvl": "WARN",
                    "msg": "req done req_id={req_id} status=502 dur_ms={dur_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 2500]},
                        "err": {"k": "ch", "v": ["origin_timeout", "origin_reset", "upstream_5xx"]},
                    },
                },
                "req_responded_503": {
                    "lvl": "ERROR",
                    "msg": "req done req_id={req_id} status=503 dur_ms={dur_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 4000]},
                        "err": {"k": "ch", "v": ["bot_module_panic", "module_unavailable"]},
                    },
                },
                "req_responded_503_recovery": {
                    "lvl": "WARN",
                    "msg": "req done req_id={req_id} status=503 dur_ms={dur_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [20, 9000]},
                        "err": {"k": "ch", "v": ["process_restarting", "module_unavailable"]},
                    },
                },
                "worker_panic_bg": {
                    "lvl": "ERROR",
                    "msg": "worker panic thread={thread} reason={reason}",
                    "vars": {
                        "thread": {"k": "ch", "v": ["fl2_worker_1", "fl2_worker_2", "fl2_worker_3", "fl2_worker_4"]},
                        "reason": {"k": "ch", "v": ["unwrap_err", "feature_limit_exceeded"]},
                    },
                },
                "process_restart": {
                    "lvl": "WARN",
                    "msg": "process restart requested by={by} reason={reason}",
                    "vars": {
                        "by": {"k": "ch", "v": ["sre", "incident_bot"]},
                        "reason": {"k": "ch", "v": ["apply_known_good_config", "recover_workers"]},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "worker_panic_bg", "per_min": 1.0}]},
        },
        "edge_gateway": {
            "svc": "edge-gw",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "edge_req_in": {
                    "lvl": "INFO",
                    "msg": "edge in req_id={req_id} client_ip={client_ip} host={host} method={method} uri={uri} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "client_ip": {"k": "ip", "v": None},
                        "host": {"k": "str", "v": "customer hostname"},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "str", "v": "/path?query"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_resp_out_ok": {
                    "lvl": "INFO",
                    "msg": "edge out req_id={req_id} status={status} bytes={bytes} dur_ms={dur_ms} colo={colo}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "status": {"k": "ch", "v": ["200", "301", "304"]},
                        "bytes": {"k": "i", "v": [200, 250000]},
                        "dur_ms": {"k": "i", "v": [5, 3000]},
                        "colo": {"k": "ch", "v": ["LHR", "FRA", "IAD", "SJC"]},
                    },
                },
                "edge_resp_out_5xx": {
                    "lvl": "WARN",
                    "msg": "edge out req_id={req_id} status={status} cf_error={cf_error} bytes={bytes} dur_ms={dur_ms} colo={colo}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "status": {"k": "ch", "v": ["500", "502", "503", "504"]},
                        "cf_error": {"k": "ch", "v": ["520", "522", "523"]},
                        "bytes": {"k": "i", "v": [400, 12000]},
                        "dur_ms": {"k": "i", "v": [10, 6000]},
                        "colo": {"k": "ch", "v": ["LHR", "FRA", "IAD", "SJC"]},
                    },
                },
                "edge_5xx_metric": {
                    "lvl": "INFO",
                    "msg": "metric http_5xx_rate_per_min={rate} colo={colo}",
                    "vars": {
                        "rate": {"k": "i", "v": [0, 5000]},
                        "colo": {"k": "ch", "v": ["LHR", "FRA", "IAD", "SJC"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "edge_5xx_metric", "per_min": 0.3, "scope": "global"}],
                "f": [{"id": "edge_5xx_metric", "per_min": 1.2, "scope": "global"}],
            },
        },
        "workers_kv_gateway": {
            "svc": "workers-kv",
            "hosts": ["kv-1"],
            "logs": {
                "kv_req": {
                    "lvl": "INFO",
                    "msg": "kv req req_id={req_id} op={op} keyspace={keyspace} key_hash={key_hash} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "op": {"k": "ch", "v": ["GET", "PUT"]},
                        "keyspace": {"k": "ch", "v": ["sessions", "config", "tokens"]},
                        "key_hash": {"k": "hex", "v": 8},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "kv_resp_ok_via_proxy": {
                    "lvl": "INFO",
                    "msg": "kv resp req_id={req_id} status=200 dur_ms={dur_ms} route=via_proxy",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [5, 2500]}},
                },
                "kv_resp_ok_bypass": {
                    "lvl": "INFO",
                    "msg": "kv resp req_id={req_id} status=200 dur_ms={dur_ms} route=bypass",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [5, 2500]}},
                },
                "kv_upstream_error": {
                    "lvl": "WARN",
                    "msg": "kv resp req_id={req_id} status=502 err={err} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "err": {"k": "ch", "v": ["core_proxy_unavailable", "upstream_5xx"]},
                        "dur_ms": {"k": "i", "v": [10, 4000]},
                    },
                },
                "bypass_enabled": {
                    "lvl": "WARN",
                    "msg": "bypass enabled route=bypass reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["incident_mitigation", "reduce_dependency_on_proxy"]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
        "access_service": {
            "svc": "access",
            "hosts": ["access-1"],
            "logs": {
                "auth_request": {
                    "lvl": "INFO",
                    "msg": "auth start auth_id={auth_id} user_hint={user_hint} app={app} trace_id={trace_id}",
                    "vars": {
                        "auth_id": {"k": "uuid", "v": None},
                        "user_hint": {"k": "hex", "v": 6},
                        "app": {"k": "ch", "v": ["dashboard", "customer_app"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "auth_ok": {
                    "lvl": "INFO",
                    "msg": "auth ok auth_id={auth_id} dur_ms={dur_ms} method={method}",
                    "vars": {
                        "auth_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [20, 5000]},
                        "method": {"k": "ch", "v": ["login", "token_refresh"]},
                    },
                },
                "auth_failed_upstream": {
                    "lvl": "WARN",
                    "msg": "auth fail auth_id={auth_id} err={err} dur_ms={dur_ms}",
                    "vars": {
                        "auth_id": {"k": "uuid", "v": None},
                        "err": {"k": "ch", "v": ["kv_unavailable", "dependency_5xx"]},
                        "dur_ms": {"k": "i", "v": [20, 6000]},
                    },
                },
                "bypass_enabled": {
                    "lvl": "WARN",
                    "msg": "mitigation enabled component=access mode=kv_bypass reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["incident_mitigation", "restore_logins"]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
        "synthetic_monitor": {
            "svc": "synth-mon",
            "hosts": ["mon-1"],
            "logs": {
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe result target={target} status=200 latency_ms={latency_ms} region={region}",
                    "vars": {
                        "target": {"k": "ch", "v": ["https_edge_root", "https_edge_login"]},
                        "latency_ms": {"k": "i", "v": [10, 1200]},
                        "region": {"k": "ch", "v": ["eu", "us"]},
                    },
                },
                "probe_fail": {
                    "lvl": "WARN",
                    "msg": "probe result target={target} status={status} latency_ms={latency_ms} region={region}",
                    "vars": {
                        "target": {"k": "ch", "v": ["https_edge_root", "https_edge_login"]},
                        "status": {"k": "ch", "v": ["503", "502"]},
                        "latency_ms": {"k": "i", "v": [20, 4000]},
                        "region": {"k": "ch", "v": ["eu", "us"]},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": [
            {
                "id": "http_user_success",
                "rpm": 240.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "edge_gateway.edge_resp_out_ok",
                ],
                "latency_ms": [[1, 3], [2, 6], [15, 80], [2, 6]],
                "trace": True,
            },
            {
                "id": "http_user_baseline_5xx",
                "rpm": 3.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_502",
                    "edge_gateway.edge_resp_out_5xx",
                ],
                "latency_ms": [[1, 3], [2, 6], [80, 600], [2, 6]],
                "trace": True,
            },
            {
                "id": "kv_get_via_proxy_ok",
                "rpm": 25.0,
                "emit": [
                    "workers_kv_gateway.kv_req",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "workers_kv_gateway.kv_resp_ok_via_proxy",
                ],
                "latency_ms": [[1, 2], [2, 6], [20, 200], [2, 6]],
                "trace": True,
            },
            {
                "id": "access_auth_ok",
                "rpm": 8.0,
                "emit": [
                    "access_service.auth_request",
                    "workers_kv_gateway.kv_req",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "workers_kv_gateway.kv_resp_ok_via_proxy",
                    "access_service.auth_ok",
                ],
                "latency_ms": [[1, 2], [2, 6], [2, 8], [20, 250], [2, 6], [5, 40]],
                "trace": True,
            },
            {
                "id": "bot_feature_publish_good",
                "rpm": 0.2,
                "emit": [
                    "bot_feature_builder.build_start",
                    "clickhouse_cluster.ch_query_start",
                    "clickhouse_cluster.ch_query_result",
                    "bot_feature_builder.build_complete_good",
                    "config_distribution.queued",
                ],
                "latency_ms": [[1, 2], [5, 20], [10, 100], [5, 30], [2, 10]],
                "trace": False,
            },
            {
                "id": "synth_probe_ok",
                "rpm": 1.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "edge_gateway.edge_resp_out_ok",
                    "synthetic_monitor.probe_ok",
                ],
                "latency_ms": [[1, 3], [2, 6], [15, 120], [2, 6], [1, 2]],
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "http_user_ok_flap",
                "rpm": 120.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "edge_gateway.edge_resp_out_ok",
                ],
                "latency_ms": [[1, 3], [2, 8], [25, 200], [2, 10]],
                "trace": True,
            },
            {
                "id": "http_user_5xx_bot_panic",
                "rpm": 130.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.bot_features_over_limit",
                    "observability_enricher.error_enriched",
                    "core_proxy_fl2.req_responded_503",
                    "edge_gateway.edge_resp_out_5xx",
                ],
                "latency_ms": [[1, 3], [2, 10], [1, 5], [10, 220], [120, 1200], [2, 10]],
                "trace": True,
            },
            {
                "id": "http_user_5xx_recovery_tail",
                "rpm": 0.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_503_recovery",
                    "edge_gateway.edge_resp_out_5xx",
                ],
                "latency_ms": [[1, 3], [2, 10], [80, 2500], [2, 10]],
                "trace": True,
            },
            {
                "id": "kv_get_via_proxy_fail",
                "rpm": 25.0,
                "emit": [
                    "workers_kv_gateway.kv_req",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_503",
                    "workers_kv_gateway.kv_upstream_error",
                ],
                "latency_ms": [[1, 2], [2, 10], [50, 1200], [2, 10]],
                "trace": True,
            },
            {
                "id": "kv_get_bypass_ok",
                "rpm": 0.0,
                "emit": ["workers_kv_gateway.kv_req", "workers_kv_gateway.kv_resp_ok_bypass"],
                "latency_ms": [[1, 2], [5, 120]],
                "trace": True,
            },
            {
                "id": "access_auth_fail",
                "rpm": 8.0,
                "emit": [
                    "access_service.auth_request",
                    "workers_kv_gateway.kv_req",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_503",
                    "workers_kv_gateway.kv_upstream_error",
                    "access_service.auth_failed_upstream",
                ],
                "latency_ms": [[1, 2], [2, 10], [2, 10], [50, 1500], [2, 10], [5, 50]],
                "trace": True,
            },
            {
                "id": "access_auth_ok_after_bypass",
                "rpm": 0.0,
                "emit": [
                    "access_service.auth_request",
                    "workers_kv_gateway.kv_req",
                    "workers_kv_gateway.kv_resp_ok_bypass",
                    "access_service.auth_ok",
                ],
                "latency_ms": [[1, 2], [2, 8], [15, 250], [5, 60]],
                "trace": True,
            },
            {
                "id": "bot_feature_publish_good_f",
                "rpm": 0.1,
                "emit": [
                    "bot_feature_builder.build_start",
                    "clickhouse_cluster.ch_query_start",
                    "clickhouse_cluster.ch_query_result",
                    "bot_feature_builder.build_complete_good",
                    "config_distribution.queued",
                ],
                "latency_ms": [[1, 2], [5, 20], [10, 120], [5, 40], [2, 10]],
                "trace": False,
            },
            {
                "id": "bot_feature_publish_bad_f",
                "rpm": 0.1,
                "emit": [
                    "bot_feature_builder.build_start",
                    "clickhouse_cluster.ch_query_start",
                    "clickhouse_cluster.ch_query_result",
                    "bot_feature_builder.build_complete_bad",
                    "config_distribution.queued",
                ],
                "latency_ms": [[1, 2], [5, 20], [10, 120], [5, 40], [2, 10]],
                "trace": False,
            },
            {
                "id": "synth_probe_fail",
                "rpm": 1.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_503",
                    "edge_gateway.edge_resp_out_5xx",
                    "synthetic_monitor.probe_fail",
                ],
                "latency_ms": [[1, 3], [2, 10], [80, 1200], [2, 10], [1, 2]],
                "trace": True,
            },
            {
                "id": "synth_probe_ok_f",
                "rpm": 1.0,
                "emit": [
                    "edge_gateway.edge_req_in",
                    "core_proxy_fl2.req_start",
                    "core_proxy_fl2.req_responded_200",
                    "edge_gateway.edge_resp_out_ok",
                    "synthetic_monitor.probe_ok",
                ],
                "latency_ms": [[1, 3], [2, 10], [20, 400], [2, 10], [1, 2]],
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "cf_like_bot_feature_file_over_limit_2025_11_18",
        "time": {
            "total_minutes": 56,
            "phases": {"n": {"start_min": 0, "end_min": 28}, "f": {"start_min": 28, "end_min": 56}},
        },
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 28,
                        "rate_multipliers": {"synth_probe_fail": 0.6, "synth_probe_ok_f": 0.4},
                        "latency_multipliers": {
                            "http_user_5xx_bot_panic": {"p50": 1.2, "p95": 1.6},
                            "kv_get_via_proxy_fail": {"p50": 1.1, "p95": 1.4},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 36,
                        "rate_multipliers": {
                            "bot_feature_publish_good_f": 0.0,
                            "bot_feature_publish_bad_f": 2.0,
                            "http_user_ok_flap": 0.2,
                            "http_user_5xx_bot_panic": 1.6,
                            "synth_probe_ok_f": 0.0,
                            "synth_probe_fail": 1.0,
                            "core_proxy_fl2.worker_panic_bg": 2.0,
                            "observability_enricher.queue_metrics": 1.8,
                        },
                        "latency_multipliers": {
                            "http_user_5xx_bot_panic": {"p50": 1.6, "p95": 2.6},
                            "http_user_ok_flap": {"p50": 1.3, "p95": 2.0},
                            "kv_get_via_proxy_fail": {"p50": 1.5, "p95": 2.2},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 44,
                        "rate_multipliers": {"kv_get_via_proxy_fail": 0.2, "access_auth_fail": 0.2},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "workers_kv_gateway.bypass_enabled", "count": 1, "hosts": ["kv-1"]},
                            {"ref": "access_service.bypass_enabled", "count": 1, "hosts": ["access-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 52,
                        "rate_multipliers": {
                            "bot_feature_publish_bad_f": 0.0,
                            "bot_feature_publish_good_f": 0.0,
                            "config_distribution.push_complete": 0.0,
                            "http_user_5xx_bot_panic": 0.03,
                            "http_user_ok_flap": 1.5,
                            "synth_probe_fail": 0.2,
                            "synth_probe_ok_f": 1.0,
                            "core_proxy_fl2.worker_panic_bg": 0.3,
                            "observability_enricher.queue_metrics": 0.6,
                            "edge_gateway.edge_5xx_metric": 0.3,
                            "kv_get_via_proxy_fail": 0.05,
                            "access_auth_fail": 0.05,
                        },
                        "latency_multipliers": {
                            "http_user_ok_flap": {"p50": 1.2, "p95": 1.7},
                            "synth_probe_ok_f": {"p50": 1.1, "p95": 1.4},
                        },
                        "one_shots": [
                            {"ref": "config_distribution.propagation_paused", "count": 1, "hosts": ["cd-1"]},
                            {"ref": "config_distribution.manual_insert_good", "count": 1, "hosts": ["cd-1"]},
                            {"ref": "core_proxy_fl2.process_restart", "count": 3, "hosts": ["fl2-1", "fl2-2", "fl2-3"]},
                        ],
                    },
                ]
            }
        },
    }
}

MIN_GAP_MS = 1.0


# --------------------------
# Deterministic helpers
# --------------------------
def _md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def dhash32(s: str) -> int:
    return int.from_bytes(_md5_bytes(s)[:4], "big", signed=False)


def u01(s: str) -> float:
    return dhash32(s) / 0xFFFFFFFF


def det_hex(s: str, n: int) -> str:
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    while len(h) < n:
        h += hashlib.sha256((h + s).encode("utf-8")).hexdigest()
    return h[:n].lower()


def det_uuid(s: str) -> str:
    b = hashlib.sha1(s.encode("utf-8")).digest()[:16]
    hexs = b.hex()
    return f"{hexs[:8]}-{hexs[8:12]}-{hexs[12:16]}-{hexs[16:20]}-{hexs[20:32]}"


def det_ip(s: str) -> str:
    o = 1 + (dhash32("ip:" + s) % 254)
    return f"203.0.113.{o}"


def normal_ppf(u: float) -> float:
    u = min(1.0 - 1e-12, max(1e-12, u))
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
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    plow = 0.02425
    phigh = 1 - plow
    if u < plow:
        q = math.sqrt(-2 * math.log(u))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if u > phigh:
        q = math.sqrt(-2 * math.log(1 - u))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = u - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def lognormal_quantile_from_p50_p95(p50: float, p95: float, u: float, soft_cap_mult: float = 3.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = max(1e-6, (math.log(p95) - math.log(p50)) / 1.645)
    z = normal_ppf(u)
    x = math.exp(mu + sigma * z)
    cap = soft_cap_mult * p95
    return float(min(x, cap))


def alloc_int(expected: float, key: str) -> int:
    expected = max(0.0, float(expected))
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if u01(f"alloc:{key}") < frac else 0)


def iso_utc_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def schedule_times(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return [start_dt] * n
    out: List[datetime] = []
    max_jitter_s = min(0.25, max(0.02, dur_s / max(n, 1) * 0.15))
    for i in range(n):
        frac = (i + 0.5) / n
        t = start_dt + timedelta(seconds=dur_s * frac)
        jit = (u01(f"{key}:jit:{i}") - 0.5) * 2.0 * max_jitter_s
        t2 = t + timedelta(seconds=jit)
        if t2 < start_dt:
            t2 = start_dt + timedelta(milliseconds=1)
        if t2 >= end_dt:
            t2 = end_dt - timedelta(milliseconds=1)
        out.append(t2)
    return out


# --------------------------
# Value generation / rendering
# --------------------------
def str_from_hint(hint: Any, key: str) -> str:
    if isinstance(hint, str):
        if "customer hostname" in hint:
            n = 1 + (dhash32("cust:" + key) % 2000)
            return f"cust{n}.example.com"
        if hint == "/path?query":
            n = 1 + (dhash32("uri:" + key) % 50)
            q = det_hex("q:" + key, 4)
            return f"/path{n}?q={q}"
        return hint
    return f"str_{det_hex('s:' + key, 8)}"


def gen_from_domain(spec: Dict[str, Any], key: str) -> Any:
    k = spec["k"]
    v = spec.get("v", None)
    if k == "uuid":
        return det_uuid(key)
    if k == "hex":
        n = int(v) if v is not None else 8
        return det_hex(key, n)
    if k == "ip":
        return det_ip(key)
    if k == "ch":
        choices = list(v) if v is not None else [""]
        return choices[dhash32("ch:" + key) % len(choices)]
    if k == "i":
        mn, mx = int(v[0]), int(v[1])
        if mx <= mn:
            return mn
        return mn + (dhash32("i:" + key) % (mx - mn + 1))
    if k == "f":
        mn, mx = float(v[0]), float(v[1])
        return mn + (mx - mn) * u01("f:" + key)
    if k == "str":
        return str_from_hint(v, key)
    return ""


def format_value(val: Any) -> str:
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def render_message(comp_id: str, log_id: str, bound: Dict[str, Any]) -> Tuple[str, str]:
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    msg = tmpl["msg"]
    full_vars: Dict[str, Any] = {}
    for var, spec in tmpl.get("vars", {}).items():
        if var in bound:
            full_vars[var] = bound[var]
        else:
            full_vars[var] = gen_from_domain(spec, f"{comp_id}.{log_id}:{var}:{bound.get('_key', '')}")
    full_vars_str = {k: format_value(v) for k, v in full_vars.items()}
    return tmpl["lvl"], msg.format_map(full_vars_str)


def choose_host_for_component(comp_id: str, chain_key: str) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts", [])
    if not hosts:
        return ""
    return hosts[dhash32(f"host:{comp_id}:{chain_key}") % len(hosts)]


def int_domain_range(comp_id: str, log_id: str, var: str) -> Optional[Tuple[int, int]]:
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    spec = tmpl.get("vars", {}).get(var)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    mn, mx = int(spec["v"][0]), int(spec["v"][1])
    return (mn, mx)


# --------------------------
# Controls (failure events)
# --------------------------
def build_failure_intervals() -> List[Dict[str, Any]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    start_f = int(f_phase["start_min"])
    end_f = int(f_phase["end_min"])
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [start_f] + [int(e["at_min"]) for e in events if start_f <= int(e["at_min"]) < end_f] + [end_f]
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        a = boundaries[i]
        b = boundaries[i + 1]
        for e in events:
            if int(e["at_min"]) == a:
                for k, v in e.get("rate_multipliers", {}).items():
                    active_rate[k] = float(v)
                for k, v in e.get("latency_multipliers", {}).items():
                    active_lat[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        one_shots: List[Dict[str, Any]] = []
        for e in events:
            if int(e["at_min"]) == a:
                one_shots.extend(e.get("one_shots", []))
        intervals.append(
            {
                "start_min": a,
                "end_min": b,
                "rate_multipliers": dict(active_rate),
                "latency_multipliers": dict(active_lat),
                "one_shots": one_shots,
            }
        )
    return intervals


def latency_multiplier_for_flow(flow_id: str, interval_lat: Dict[str, Dict[str, float]]) -> Tuple[float, float]:
    m = interval_lat.get(flow_id)
    if not m:
        return (1.0, 1.0)
    return (float(m.get("p50", 1.0)), float(m.get("p95", 1.0)))


def rate_multiplier_for_source(source_key: str, interval_rate: Dict[str, float]) -> float:
    return float(interval_rate.get(source_key, 1.0))


# --------------------------
# Simulation: background
# --------------------------
def interval_context(state: str, interval_rate: Dict[str, float]) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"state": state}
    if state == "n":
        base_5xx = next(f["rpm"] for f in SYSTEM["flows"]["n"] if f["id"] == "http_user_baseline_5xx")
        ctx["edge_5xx_rpm"] = float(base_5xx)
        ctx["bot_panic_rpm"] = 0.0
    else:
        base_bot = next(f["rpm"] for f in SYSTEM["flows"]["f"] if f["id"] == "http_user_5xx_bot_panic")
        ctx["bot_panic_rpm"] = float(base_bot) * rate_multiplier_for_source("http_user_5xx_bot_panic", interval_rate)
        ctx["edge_5xx_rpm"] = ctx["bot_panic_rpm"]
    return ctx


def background_values(comp_id: str, log_id: str, key: str, ctx: Dict[str, Any], chosen_host: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"_key": key}
    if comp_id == "edge_gateway" and log_id == "edge_5xx_metric":
        rate_val = int(min(5000, max(0, round(ctx.get("edge_5xx_rpm", 0.0) * 20.0))))
        out["rate"] = rate_val
        out["colo"] = ["LHR", "FRA", "IAD", "SJC"][dhash32("colo:" + key) % 4]
        return out
    if comp_id == "observability_enricher" and log_id == "queue_metrics":
        bot = float(ctx.get("bot_panic_rpm", 0.0))
        depth = int(min(20000, max(0, round(bot * 80.0 + u01("qd:" + key) * 400.0))))
        cpu = int(min(100, max(5, round(10.0 + bot / 3.0 + u01("cpu:" + key) * 5.0))))
        dropped = int(min(5000, max(0, round(bot * 12.0 * (0.4 + 0.6 * u01("drop:" + key))))))
        out["depth"] = depth
        out["cpu_pct"] = cpu
        out["dropped_per_min"] = dropped
        return out
    if comp_id == "config_distribution" and log_id == "push_complete":
        file_version = "feat_good" if ctx["state"] == "n" else "feat_bad"
        if ctx["state"] == "f" and u01("mix:" + key) < 0.15:
            file_version = "feat_good"
        failed = 0
        if ctx["state"] == "f":
            failed = int(min(40, max(0, round(5 + u01("f:" + key) * 25))))
        succeeded = int(min(350, max(80, 320 - failed - int(u01("s:" + key) * 40))))
        elapsed_ms = int(
            min(
                45000,
                max(2000, round((5000 if ctx["state"] == "n" else 22000) * (0.7 + 0.8 * u01("e:" + key)))),
            )
        )
        out.update({"file_version": file_version, "succeeded": succeeded, "failed": failed, "elapsed_ms": elapsed_ms})
        return out
    if comp_id == "clickhouse_cluster" and log_id == "ch_cluster_health":
        out["shard"] = chosen_host if chosen_host in ["ch-1", "ch-2", "ch-3"] else ["ch-1", "ch-2", "ch-3"][0]
        out["repl_lag_ms"] = int(min(500, round(u01("lag:" + key) * (40 if ctx["state"] == "n" else 120))))
        out["pending_merges"] = int(min(40, round(u01("pm:" + key) * (6 if ctx["state"] == "n" else 12))))
        return out
    if comp_id == "core_proxy_fl2" and log_id == "worker_panic_bg":
        out["thread"] = ["fl2_worker_1", "fl2_worker_2", "fl2_worker_3", "fl2_worker_4"][dhash32("t:" + key) % 4]
        out["reason"] = "feature_limit_exceeded" if u01("r:" + key) < 0.85 else "unwrap_err"
        return out
    return out


def emit_background(
    rows: List[Dict[str, Any]],
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    interval_rate: Dict[str, float],
) -> None:
    ctx = interval_context(state, interval_rate)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0
    for comp_id, comp in SYSTEM["components"].items():
        beh_list = comp.get("beh", {}).get(state, [])
        for emit in beh_list:
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            source_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = rate_multiplier_for_source(source_key, interval_rate)
            eff_rate = per_min * mult

            hosts = comp.get("hosts", [])
            if scope == "global":
                expected = eff_rate * dur_min
                n = alloc_int(expected, f"bg:{state}:{source_key}:{start_dt.isoformat()}:{end_dt.isoformat()}")
                times = schedule_times(start_dt, end_dt, n, f"bg:{state}:{source_key}:{start_dt.timestamp()}")
                for i, ts in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    key = f"{source_key}:{iso_utc_ms(ts)}:{i}"
                    bound = background_values(comp_id, log_id, key, ctx, host)
                    lvl, msg = render_message(comp_id, log_id, bound)
                    rows.append(
                        {
                            "timestamp": ts,
                            "level": lvl,
                            "message": msg,
                            "trace_id": "",
                            "service": comp.get("svc", "") or "",
                            "host": host,
                        }
                    )
            else:
                for h in hosts:
                    expected = eff_rate * dur_min
                    n = alloc_int(expected, f"bg:{state}:{source_key}:{h}:{start_dt.isoformat()}:{end_dt.isoformat()}")
                    times = schedule_times(start_dt, end_dt, n, f"bg:{state}:{source_key}:{h}:{start_dt.timestamp()}")
                    for i, ts in enumerate(times):
                        key = f"{source_key}:{h}:{iso_utc_ms(ts)}:{i}"
                        bound = background_values(comp_id, log_id, key, ctx, h)
                        lvl, msg = render_message(comp_id, log_id, bound)
                        rows.append(
                            {
                                "timestamp": ts,
                                "level": lvl,
                                "message": msg,
                                "trace_id": "",
                                "service": comp.get("svc", "") or "",
                                "host": h,
                            }
                        )


# --------------------------
# Simulation: flows
# --------------------------
def flow_effective_rpm(flow: Dict[str, Any], state: str, interval_rate: Dict[str, float]) -> float:
    base = float(flow["rpm"])
    if state != "f":
        return base
    mult = rate_multiplier_for_source(flow["id"], interval_rate)
    return base * mult


def choose_chain_trace_id(flow: Dict[str, Any], chain_key: str) -> str:
    if not SYSTEM["tracing"]["on"]:
        return ""
    if not flow.get("trace", False):
        return ""
    return det_hex("trace:" + chain_key, 32)


def sample_gap_ms(
    p50: float,
    p95: float,
    key: str,
    hard_min: Optional[float] = None,
    hard_max: Optional[float] = None,
) -> float:
    u = 0.22 + 0.76 * u01("u:" + key)
    x = lognormal_quantile_from_p50_p95(p50, p95, u, soft_cap_mult=3.0)
    if hard_min is not None:
        x = max(float(hard_min), x)
    x = max(MIN_GAP_MS, x)
    if hard_max is not None:
        x = min(x, float(hard_max))
    x = max(MIN_GAP_MS, x)
    return float(x)


def build_ts_from_gaps(start_ts: datetime, gaps_ms: List[float]) -> List[datetime]:
    ts = [start_ts]
    cur = start_ts
    for g in gaps_ms:
        cur = cur + timedelta(milliseconds=float(g))
        ts.append(cur)
    return ts


def segment_ms_int(gaps_ms: List[float], i0: int, i1: int) -> int:
    if i0 is None or i1 is None:
        return 0
    if i1 <= i0:
        return 0
    return int(round(sum(gaps_ms[i0:i1])))


def enforce_range_on_segment(
    gaps_ms: List[float],
    i0: int,
    i1: int,
    mn: int,
    mx: int,
    _key: str,
) -> bool:
    if i0 is None or i1 is None or i1 <= i0:
        return False
    mn = int(mn)
    mx = int(mx)
    if mx < mn:
        mx = mn
    changed = False

    total = segment_ms_int(gaps_ms, i0, i1)
    if total < mn:
        diff = mn - total
        gaps_ms[i1 - 1] = float(gaps_ms[i1 - 1] + diff)
        changed = True

    total = segment_ms_int(gaps_ms, i0, i1)
    if total > mx:
        diff = total - mx
        seg_idxs = list(range(i0, i1))
        seg_idxs.sort(key=lambda k: (-gaps_ms[k], k))
        for k in seg_idxs:
            if diff <= 0:
                break
            reducible = max(0.0, gaps_ms[k] - MIN_GAP_MS)
            if reducible <= 0:
                continue
            r = min(float(diff), reducible)
            gaps_ms[k] = float(gaps_ms[k] - r)
            diff -= int(round(r))
            changed = True

        for k in range(i0, i1):
            if gaps_ms[k] < MIN_GAP_MS:
                gaps_ms[k] = MIN_GAP_MS
                changed = True

    total = segment_ms_int(gaps_ms, i0, i1)
    if total < mn:
        diff = mn - total
        gaps_ms[i1 - 1] = float(gaps_ms[i1 - 1] + diff)
        changed = True

    return changed


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_ts: datetime,
    interval_rate: Dict[str, float],
    interval_lat: Dict[str, Dict[str, float]],
    inst_idx: int,
) -> List[Dict[str, Any]]:
    flow_id = flow["id"]
    chain_key = f"{state}:{flow_id}:{inst_idx}:{iso_utc_ms(start_ts)}"
    trace_id = choose_chain_trace_id(flow, chain_key)

    refs = [parse_ref(r) for r in flow["emit"]]
    lat_pairs = flow["latency_ms"]

    # Chain-level coherence flags.
    has_proxy_200 = ("core_proxy_fl2", "req_responded_200") in refs
    has_probe_ok = ("synthetic_monitor", "probe_ok") in refs

    m50, m95 = (1.0, 1.0)
    if state == "f":
        m50, m95 = latency_multiplier_for_flow(flow_id, interval_lat)

    req_id = det_uuid("req:" + chain_key)
    auth_id = det_uuid("auth:" + chain_key)
    build_id = det_uuid("build:" + chain_key)
    query_id = det_uuid("query:" + chain_key)
    key_hash = det_hex("kh:" + chain_key, 8)
    customer_host = str_from_hint("customer hostname", chain_key)
    method = ["GET", "POST"][dhash32("m:" + chain_key) % 2]
    uri = str_from_hint("/path?query", chain_key)
    client_ip = det_ip(chain_key)
    colo = ["LHR", "FRA", "IAD", "SJC"][dhash32("colo:" + chain_key) % 4]
    region = ["eu", "us"][dhash32("reg:" + chain_key) % 2]
    target = ["https_edge_root", "https_edge_login"][dhash32("tgt:" + chain_key) % 2]
    app = ["dashboard", "customer_app"][dhash32("app:" + chain_key) % 2]

    comp_host: Dict[str, str] = {}
    for comp_id, _ in refs:
        if comp_id not in comp_host:
            comp_host[comp_id] = choose_host_for_component(comp_id, chain_key)

    is_good_build = flow_id in ("bot_feature_publish_good", "bot_feature_publish_good_f")
    is_bad_build = flow_id in ("bot_feature_publish_bad_f",)
    if state == "n" and flow_id == "bot_feature_publish_good":
        is_good_build = True
        is_bad_build = False

    if is_good_build:
        file_version = "feat_good"
        features = int(55 + (dhash32("feat:" + chain_key) % 16))
        file_kb = int(40 + (dhash32("kb:" + chain_key) % 41))
        dup_ratio = None
    elif is_bad_build:
        file_version = "feat_bad"
        features = int(210 + (dhash32("feat:" + chain_key) % 61))
        file_kb = int(140 + (dhash32("kb:" + chain_key) % 101))
        dup_ratio = 1.6 + (3.5 - 1.6) * u01("dup:" + chain_key)
    else:
        file_version = "feat_bad"
        features = int(210 + (dhash32("feat:" + chain_key) % 61))
        file_kb = int(140 + (dhash32("kb:" + chain_key) % 101))
        dup_ratio = 2.0 + u01("dup:" + chain_key)

    gaps_ms: List[float] = []
    for j in range(1, len(refs)):
        p50, p95 = float(lat_pairs[j][0]), float(lat_pairs[j][1])
        sp50, sp95 = p50 * m50, p95 * m95

        comp_id, log_id = refs[j]
        hard_min = None
        hard_max = None
        if (comp_id, log_id) == ("clickhouse_cluster", "ch_query_result"):
            r = int_domain_range("clickhouse_cluster", "ch_query_result", "elapsed_ms")
            if r:
                hard_min, hard_max = float(r[0]), float(r[1])
        if (comp_id, log_id) == ("observability_enricher", "error_enriched"):
            r = int_domain_range("observability_enricher", "error_enriched", "enrich_ms")
            if r:
                hard_min, hard_max = float(r[0]), float(r[1])

        gap = sample_gap_ms(sp50, sp95, f"gap:{chain_key}:{j}", hard_min=hard_min, hard_max=hard_max)
        gaps_ms.append(gap)

    idx_of: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(refs):
        idx_of[r] = i

    constraints: List[Tuple[str, int, int, int, int]] = []

    if ("clickhouse_cluster", "ch_query_start") in idx_of and ("clickhouse_cluster", "ch_query_result") in idx_of:
        i0 = idx_of[("clickhouse_cluster", "ch_query_start")]
        i1 = idx_of[("clickhouse_cluster", "ch_query_result")]
        r = int_domain_range("clickhouse_cluster", "ch_query_result", "elapsed_ms")
        if r:
            constraints.append(("ch_elapsed_ms", i0, i1, r[0], r[1]))

    if ("core_proxy_fl2", "bot_features_over_limit") in idx_of and ("observability_enricher", "error_enriched") in idx_of:
        i0 = idx_of[("core_proxy_fl2", "bot_features_over_limit")]
        i1 = idx_of[("observability_enricher", "error_enriched")]
        r = int_domain_range("observability_enricher", "error_enriched", "enrich_ms")
        if r:
            constraints.append(("enrich_ms", i0, i1, r[0], r[1]))

    if ("core_proxy_fl2", "req_start") in idx_of:
        i0 = idx_of[("core_proxy_fl2", "req_start")]
        for done in ("req_responded_200", "req_responded_502", "req_responded_503", "req_responded_503_recovery"):
            if ("core_proxy_fl2", done) in idx_of:
                i1 = idx_of[("core_proxy_fl2", done)]
                r = int_domain_range("core_proxy_fl2", done, "dur_ms")
                if r:
                    constraints.append((f"proxy_dur_ms:{done}", i0, i1, r[0], r[1]))
                break

    if ("edge_gateway", "edge_req_in") in idx_of:
        i0 = idx_of[("edge_gateway", "edge_req_in")]
        for out in ("edge_resp_out_ok", "edge_resp_out_5xx"):
            if ("edge_gateway", out) in idx_of:
                i1 = idx_of[("edge_gateway", out)]
                r = int_domain_range("edge_gateway", out, "dur_ms")
                if r:
                    constraints.append((f"edge_dur_ms:{out}", i0, i1, r[0], r[1]))
                break

    if ("workers_kv_gateway", "kv_req") in idx_of:
        i0 = idx_of[("workers_kv_gateway", "kv_req")]
        for out in ("kv_resp_ok_via_proxy", "kv_resp_ok_bypass", "kv_upstream_error"):
            if ("workers_kv_gateway", out) in idx_of:
                i1 = idx_of[("workers_kv_gateway", out)]
                r = int_domain_range("workers_kv_gateway", out, "dur_ms")
                if r:
                    constraints.append((f"kv_dur_ms:{out}", i0, i1, r[0], r[1]))
                break

    if ("access_service", "auth_request") in idx_of:
        i0 = idx_of[("access_service", "auth_request")]
        for out in ("auth_ok", "auth_failed_upstream"):
            if ("access_service", out) in idx_of:
                i1 = idx_of[("access_service", out)]
                r = int_domain_range("access_service", out, "dur_ms")
                if r:
                    constraints.append((f"auth_dur_ms:{out}", i0, i1, r[0], r[1]))
                break

    if ("edge_gateway", "edge_req_in") in idx_of:
        i0 = idx_of[("edge_gateway", "edge_req_in")]
        for out in ("probe_ok", "probe_fail"):
            if ("synthetic_monitor", out) in idx_of:
                i1 = idx_of[("synthetic_monitor", out)]
                r = int_domain_range("synthetic_monitor", out, "latency_ms")
                if r:
                    constraints.append((f"probe_latency_ms:{out}", i0, i1, r[0], r[1]))
                break

    for _pass in range(5):
        changed_any = False
        for name, i0, i1, mn, mx in constraints:
            changed_any |= enforce_range_on_segment(gaps_ms, i0, i1, mn, mx, f"{chain_key}:{name}")
        if not changed_any:
            break

    ts = build_ts_from_gaps(start_ts, gaps_ms)

    def seg(i0: Optional[int], i1: Optional[int]) -> int:
        if i0 is None or i1 is None:
            return 0
        return segment_ms_int(gaps_ms, i0, i1)

    proxy_start_idx = idx_of.get(("core_proxy_fl2", "req_start"))
    proxy_done_idx = None
    for cand in ("req_responded_200", "req_responded_502", "req_responded_503", "req_responded_503_recovery"):
        if ("core_proxy_fl2", cand) in idx_of:
            proxy_done_idx = idx_of[("core_proxy_fl2", cand)]
            break
    proxy_dur_ms = seg(proxy_start_idx, proxy_done_idx) if (proxy_start_idx is not None and proxy_done_idx is not None) else None

    edge_in_idx = idx_of.get(("edge_gateway", "edge_req_in"))
    edge_out_idx = None
    for cand in ("edge_resp_out_ok", "edge_resp_out_5xx"):
        if ("edge_gateway", cand) in idx_of:
            edge_out_idx = idx_of[("edge_gateway", cand)]
            break
    edge_dur_ms = seg(edge_in_idx, edge_out_idx) if (edge_in_idx is not None and edge_out_idx is not None) else None

    kv_req_idx = idx_of.get(("workers_kv_gateway", "kv_req"))
    kv_resp_idx = None
    for cand in ("kv_resp_ok_via_proxy", "kv_resp_ok_bypass", "kv_upstream_error"):
        if ("workers_kv_gateway", cand) in idx_of:
            kv_resp_idx = idx_of[("workers_kv_gateway", cand)]
            break
    kv_dur_ms = seg(kv_req_idx, kv_resp_idx) if (kv_req_idx is not None and kv_resp_idx is not None) else None

    auth_req_idx = idx_of.get(("access_service", "auth_request"))
    auth_done_idx = None
    for cand in ("auth_ok", "auth_failed_upstream"):
        if ("access_service", cand) in idx_of:
            auth_done_idx = idx_of[("access_service", cand)]
            break
    auth_dur_ms = seg(auth_req_idx, auth_done_idx) if (auth_req_idx is not None and auth_done_idx is not None) else None

    probe_idx = None
    probe_log = None
    for cand in ("probe_ok", "probe_fail"):
        if ("synthetic_monitor", cand) in idx_of:
            probe_idx = idx_of[("synthetic_monitor", cand)]
            probe_log = cand
            break
    probe_lat_ms = seg(edge_in_idx, probe_idx) if (edge_in_idx is not None and probe_idx is not None) else None

    ch_qs = idx_of.get(("clickhouse_cluster", "ch_query_start"))
    ch_qr = idx_of.get(("clickhouse_cluster", "ch_query_result"))
    ch_elapsed_ms = seg(ch_qs, ch_qr) if (ch_qs is not None and ch_qr is not None) else None

    over_idx = idx_of.get(("core_proxy_fl2", "bot_features_over_limit"))
    enr_idx = idx_of.get(("observability_enricher", "error_enriched"))
    enrich_ms = seg(over_idx, enr_idx) if (over_idx is not None and enr_idx is not None) else None

    out_rows: List[Dict[str, Any]] = []
    for i, (comp_id, log_id) in enumerate(refs):
        bound: Dict[str, Any] = {"_key": f"{chain_key}:{i}:{comp_id}.{log_id}"}

        if comp_id in ("edge_gateway", "core_proxy_fl2"):
            bound["req_id"] = req_id
            bound["trace_id"] = trace_id
            bound["host"] = customer_host
            bound["method"] = method
            bound["uri"] = uri
        if comp_id == "edge_gateway":
            bound["client_ip"] = client_ip
            bound["colo"] = colo
        if comp_id == "workers_kv_gateway":
            bound["req_id"] = req_id
            bound["trace_id"] = trace_id
            bound["key_hash"] = key_hash
        if comp_id == "access_service":
            bound["auth_id"] = auth_id
            bound["trace_id"] = trace_id
            bound["user_hint"] = det_hex("uh:" + chain_key, 6)
            bound["app"] = app
        if comp_id == "bot_feature_builder":
            bound["build_id"] = build_id
            bound["table"] = "http_requests_features"
        if comp_id == "clickhouse_cluster":
            bound["query_id"] = query_id
            bound["user"] = "bot_feature_builder"
            bound["table"] = "http_requests_features"
            bound["shard"] = comp_host.get("clickhouse_cluster", "ch-1")
        if comp_id == "synthetic_monitor":
            bound["target"] = target
            bound["region"] = region

        if comp_id == "core_proxy_fl2" and log_id == "req_responded_200":
            bound["dur_ms"] = int(proxy_dur_ms if proxy_dur_ms is not None else 50)
            bound["cache"] = ["HIT", "MISS"][dhash32("cache:" + chain_key) % 2]
            bound["bot_score"] = 1 + (dhash32("bs:" + chain_key) % 99)
        if comp_id == "core_proxy_fl2" and log_id == "req_responded_502":
            bound["dur_ms"] = int(proxy_dur_ms if proxy_dur_ms is not None else 150)
            bound["err"] = ["origin_timeout", "origin_reset", "upstream_5xx"][dhash32("e502:" + chain_key) % 3]
        if comp_id == "core_proxy_fl2" and log_id == "req_responded_503":
            bound["dur_ms"] = int(proxy_dur_ms if proxy_dur_ms is not None else 400)
            if flow_id == "http_user_5xx_bot_panic":
                bound["err"] = "bot_module_panic"
            elif flow_id in ("kv_get_via_proxy_fail", "access_auth_fail", "synth_probe_fail"):
                bound["err"] = "module_unavailable"
            else:
                bound["err"] = "bot_module_panic" if u01("e503:" + chain_key) < 0.5 else "module_unavailable"
        if comp_id == "core_proxy_fl2" and log_id == "req_responded_503_recovery":
            bound["dur_ms"] = int(proxy_dur_ms if proxy_dur_ms is not None else 1200)
            bound["err"] = ["process_restarting", "module_unavailable"][dhash32("e503r:" + chain_key) % 2]
        if comp_id == "core_proxy_fl2" and log_id == "bot_features_over_limit":
            bound["file_version"] = "feat_bad"
            bound["features"] = features
            bound["limit"] = 200
        if comp_id == "observability_enricher" and log_id == "error_enriched":
            bound["err_type"] = ["panic_unwrap", "module_panic", "runtime_panic"][dhash32("et:" + chain_key) % 3]
            bound["stack_frames"] = int(10 + (dhash32("sf:" + chain_key) % 111))
            bound["enrich_ms"] = int(enrich_ms if enrich_ms is not None else 50)
            bound["dropped"] = "false" if u01("drop:" + chain_key) < 0.92 else "true"
        if comp_id == "edge_gateway" and log_id == "edge_resp_out_ok":
            bound["dur_ms"] = int(edge_dur_ms if edge_dur_ms is not None else 60)
            # Coherence fix: if the chain includes proxy 200 and/or probe_ok (which is fixed to status=200),
            # edge "ok" response must also be 200 to avoid contradictory per-request outcomes.
            if has_proxy_200 or has_probe_ok:
                bound["status"] = "200"
            else:
                bound["status"] = "200" if u01("sok:" + chain_key) < 0.92 else (["301", "304"][dhash32("r:" + chain_key) % 2])
            bound["bytes"] = int(200 + (dhash32("b:" + chain_key) % (250000 - 200 + 1)))
        if comp_id == "edge_gateway" and log_id == "edge_resp_out_5xx":
            bound["dur_ms"] = int(edge_dur_ms if edge_dur_ms is not None else 500)
            if flow_id in ("http_user_5xx_bot_panic", "synth_probe_fail"):
                bound["status"] = "503"
                bound["cf_error"] = "520" if u01("cf:" + chain_key) < 0.7 else "522"
            elif flow_id == "http_user_baseline_5xx":
                bound["status"] = "502"
                bound["cf_error"] = "522"
            else:
                bound["status"] = ["500", "502", "503", "504"][dhash32("st:" + chain_key) % 4]
                bound["cf_error"] = ["520", "522", "523"][dhash32("cf2:" + chain_key) % 3]
            bound["bytes"] = int(400 + (dhash32("eb:" + chain_key) % (12000 - 400 + 1)))
        if comp_id == "workers_kv_gateway" and log_id == "kv_req":
            bound["op"] = ["GET", "PUT"][dhash32("op:" + chain_key) % 2]
            bound["keyspace"] = ["sessions", "config", "tokens"][dhash32("ks:" + chain_key) % 3]
        if comp_id == "workers_kv_gateway" and log_id == "kv_resp_ok_via_proxy":
            bound["dur_ms"] = int(kv_dur_ms if kv_dur_ms is not None else 35)
        if comp_id == "workers_kv_gateway" and log_id == "kv_resp_ok_bypass":
            bound["dur_ms"] = int(kv_dur_ms if kv_dur_ms is not None else 20)
        if comp_id == "workers_kv_gateway" and log_id == "kv_upstream_error":
            bound["dur_ms"] = int(kv_dur_ms if kv_dur_ms is not None else 300)
            bound["err"] = "core_proxy_unavailable" if u01("kerr:" + chain_key) < 0.85 else "upstream_5xx"
        if comp_id == "access_service" and log_id == "auth_ok":
            bound["dur_ms"] = int(auth_dur_ms if auth_dur_ms is not None else 120)
            bound["method"] = ["login", "token_refresh"][dhash32("am:" + chain_key) % 2]
        if comp_id == "access_service" and log_id == "auth_failed_upstream":
            bound["dur_ms"] = int(auth_dur_ms if auth_dur_ms is not None else 400)
            bound["err"] = "kv_unavailable" if u01("aerr:" + chain_key) < 0.8 else "dependency_5xx"
        if comp_id == "clickhouse_cluster" and log_id == "ch_query_result":
            bound["elapsed_ms"] = int(ch_elapsed_ms if ch_elapsed_ms is not None else 50)
            if is_bad_build:
                bound["rows"] = int(min(320, max(200, 220 + (dhash32("rows:" + chain_key) % 90))))
                bound["dup_columns"] = "true"
            else:
                bound["rows"] = int(60 + (dhash32("rows:" + chain_key) % 60))
                bound["dup_columns"] = "false"
            bound["shard"] = comp_host.get("clickhouse_cluster", "ch-1")
        if comp_id == "bot_feature_builder" and log_id == "build_complete_good":
            bound["file_version"] = "feat_good"
            bound["features"] = features
            bound["file_kb"] = file_kb
            bound["sha"] = det_hex("sha:" + chain_key, 12)
        if comp_id == "bot_feature_builder" and log_id == "build_complete_bad":
            bound["file_version"] = "feat_bad"
            bound["features"] = features
            bound["file_kb"] = file_kb
            bound["dup_ratio"] = dup_ratio if dup_ratio is not None else (1.6 + (3.5 - 1.6) * u01("dup2:" + chain_key))
        if comp_id == "config_distribution" and log_id == "queued":
            bound["file_version"] = file_version
            bound["size_kb"] = file_kb
            if file_version == "feat_bad":
                bound["queue_depth"] = int(min(5000, 1200 + int(u01("qd:" + chain_key) * 2500)))
                bound["targets"] = int(300 + (dhash32("t:" + chain_key) % 51))
            else:
                bound["queue_depth"] = int(u01("qd:" + chain_key) * 300)
                bound["targets"] = int(180 + (dhash32("t:" + chain_key) % 81))
        if comp_id == "synthetic_monitor" and log_id == "probe_ok":
            bound["latency_ms"] = int(probe_lat_ms if probe_lat_ms is not None else 80)
        if comp_id == "synthetic_monitor" and log_id == "probe_fail":
            bound["status"] = "503"
            bound["latency_ms"] = int(probe_lat_ms if probe_lat_ms is not None else 900)

        lvl, msg = render_message(comp_id, log_id, bound)
        out_rows.append(
            {
                "timestamp": ts[i],
                "level": lvl,
                "message": msg,
                "trace_id": trace_id if flow.get("trace", False) else "",
                "service": SYSTEM["components"][comp_id].get("svc", "") or "",
                "host": comp_host.get(comp_id, ""),
            }
        )

    return out_rows


def emit_flows(
    rows: List[Dict[str, Any]],
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    interval_rate: Dict[str, float],
    interval_lat: Dict[str, Dict[str, float]],
) -> None:
    dur_min = (end_dt - start_dt).total_seconds() / 60.0
    flows = SYSTEM["flows"][state]
    for flow in flows:
        eff_rpm = flow_effective_rpm(flow, state, interval_rate)
        expected = eff_rpm * dur_min
        n = alloc_int(expected, f"flow:{state}:{flow['id']}:{start_dt.isoformat()}:{end_dt.isoformat()}")
        starts = schedule_times(start_dt, end_dt, n, f"flow:{state}:{flow['id']}:{start_dt.timestamp()}")
        for i, st in enumerate(starts):
            rows.extend(simulate_flow_instance(flow, state, st, interval_rate, interval_lat, i))


# --------------------------
# One-shots
# --------------------------
def emit_one_shots(rows: List[Dict[str, Any]], base_dt: datetime, at_min: int, shots: List[Dict[str, Any]]) -> None:
    event_dt = base_dt + timedelta(minutes=at_min)
    for sidx, shot in enumerate(shots):
        ref = shot["ref"]
        count = int(shot["count"])
        allowed_hosts = shot.get("hosts", None)
        comp_id, log_id = parse_ref(ref)
        comp = SYSTEM["components"][comp_id]
        svc = comp.get("svc", "") or ""
        hosts = comp.get("hosts", [])
        host_pool = list(allowed_hosts) if allowed_hosts is not None else list(hosts)
        if not host_pool:
            host_pool = [""]

        for j in range(count):
            jit_s = u01(f"oneshot:{ref}:{at_min}:{sidx}:{j}") * 50.0
            ts = event_dt + timedelta(seconds=jit_s)
            chosen_host = host_pool[j % len(host_pool)]
            key = f"oneshot:{ref}:{at_min}:{sidx}:{j}:{iso_utc_ms(ts)}"
            bound: Dict[str, Any] = {"_key": key}

            if comp_id == "workers_kv_gateway" and log_id == "bypass_enabled":
                bound["reason"] = "incident_mitigation"
            if comp_id == "access_service" and log_id == "bypass_enabled":
                bound["reason"] = "incident_mitigation"
            if comp_id == "config_distribution" and log_id == "propagation_paused":
                bound["reason"] = "bad_artifact_detected"
            if comp_id == "config_distribution" and log_id == "manual_insert_good":
                bound["file_version"] = "feat_good"
                bound["source"] = "last_known_good"
            if comp_id == "core_proxy_fl2" and log_id == "process_restart":
                bound["by"] = "sre"
                bound["reason"] = "apply_known_good_config"

            lvl, msg = render_message(comp_id, log_id, bound)
            rows.append(
                {
                    "timestamp": ts,
                    "level": lvl,
                    "message": msg,
                    "trace_id": "",
                    "service": svc,
                    "host": chosen_host,
                }
            )


# --------------------------
# Main
# --------------------------
def main() -> None:
    random.seed(0)
    np.random.seed(0)

    base_dt = datetime(2025, 11, 18, 0, 0, 0, tzinfo=timezone.utc)

    n_phase = SCENARIO["scenario"]["time"]["phases"]["n"]
    rows: List[Dict[str, Any]] = []

    n_start = base_dt + timedelta(minutes=int(n_phase["start_min"]))
    n_end = base_dt + timedelta(minutes=int(n_phase["end_min"]))
    emit_background(rows, "n", n_start, n_end, interval_rate={})
    emit_flows(rows, "n", n_start, n_end, interval_rate={}, interval_lat={})

    f_intervals = build_failure_intervals()
    for interval in f_intervals:
        a_min = int(interval["start_min"])
        b_min = int(interval["end_min"])
        a_dt = base_dt + timedelta(minutes=a_min)
        b_dt = base_dt + timedelta(minutes=b_min)

        rate_mult = interval["rate_multipliers"]
        lat_mult = interval["latency_multipliers"]

        emit_background(rows, "f", a_dt, b_dt, interval_rate=rate_mult)
        emit_flows(rows, "f", a_dt, b_dt, interval_rate=rate_mult, interval_lat=lat_mult)

        if interval.get("one_shots"):
            emit_one_shots(rows, base_dt, a_min, interval["one_shots"])

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].map(iso_utc_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
