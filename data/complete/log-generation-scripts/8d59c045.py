import math
import re
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "cloud_backbone_low_priority_capacity"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "rollout_controller": {
            "svc": "wan-rollout",
            "hosts": ["rc-1"],
            "logs": {
                "rollout_started": {
                    "lvl": "INFO",
                    "msg": "rollout started domain={domain} config_version={config_version} scope={scope}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "config_version": {"k": "ch", "v": ["2022.07.14.1", "2022.07.15.0"]},
                        "scope": {"k": "ch", "v": ["multi_region", "global"]},
                    },
                },
                "rollout_paused": {
                    "lvl": "WARN",
                    "msg": "rollout paused domain={domain} reason={reason}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "reason": {"k": "ch", "v": ["capacity_drop_detected", "operator_request"]},
                    },
                },
                "rollback_initiated": {
                    "lvl": "WARN",
                    "msg": "rollback initiated domain={domain} target_config_version={config_version}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "config_version": {"k": "ch", "v": ["2022.07.14.1"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "sdn_controller": {
            "svc": "wan-sdn",
            "hosts": ["sdn-a", "sdn-b"],
            "logs": {
                "controller_heartbeat": {
                    "lvl": "INFO",
                    "msg": "control-plane heartbeat domain={domain} leader={leader} pending_ops={pending_ops}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "leader": {"k": "ch", "v": ["sdn-a", "sdn-b"]},
                    },
                    "state_vars": {
                        "n": {"pending_ops": {"k": "i", "v": [0, 2]}},
                        "f": {"pending_ops": {"k": "i", "v": [10, 200]}},
                    },
                },
                "apply_ok": {
                    "lvl": "INFO",
                    "msg": "config applied domain={domain} version={config_version} nodes_total={nodes_total} duration_ms={duration_ms}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "config_version": {"k": "ch", "v": ["2022.07.14.1", "2022.07.15.0"]},
                        "nodes_total": {"k": "i", "v": [800, 1200]},
                        "duration_ms": {"k": "i", "v": [5000, 60000]},
                    },
                },
                "apply_partial": {
                    "lvl": "WARN",
                    "msg": "config partially applied domain={domain} version={config_version} nodes_total={nodes_total} nodes_updated_pct={nodes_updated_pct} duration_ms={duration_ms}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["wan-backbone"]},
                        "config_version": {"k": "ch", "v": ["2022.07.14.1", "2022.07.15.0"]},
                        "nodes_total": {"k": "i", "v": [800, 1200]},
                        "nodes_updated_pct": {"k": "f", "v": [50.0, 99.5]},
                        "duration_ms": {"k": "i", "v": [10000, 180000]},
                    },
                },
                "controller_disconnected": {
                    "lvl": "WARN",
                    "msg": "local controller unreachable controller_id={controller_id} region={region} consecutive_failures={consecutive_failures}",
                    "vars": {
                        "controller_id": {"k": "ch", "v": ["lc-01", "lc-02", "lc-03", "lc-04"]},
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "consecutive_failures": {"k": "i", "v": [3, 30]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "controller_heartbeat", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "controller_heartbeat", "per_min": 1.2, "scope": "per_host"},
                        {"id": "controller_disconnected", "per_min": 0.15, "scope": "per_host"},
                    ]
                },
            },
        },
        "backbone_edge": {
            "svc": "wan-edge",
            "hosts": ["be-uscentral", "be-useast", "be-samw"],
            "logs": {
                "qos_stats": {
                    "lvl": "INFO",
                    "msg": "qos stats region={region} low_prio_drop_pct={low_prio_drop_pct} low_prio_queue_depth={low_prio_queue_depth} link_group={link_group}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "link_group": {"k": "ch", "v": ["bgp-1", "bgp-2"]},
                    },
                    "state_vars": {
                        "n": {
                            "low_prio_drop_pct": {"k": "f", "v": [0.0, 1.0]},
                            "low_prio_queue_depth": {"k": "i", "v": [0, 50]},
                        },
                        "f": {
                            "low_prio_drop_pct": {"k": "f", "v": [0.0, 1.0]},
                            "low_prio_queue_depth": {"k": "i", "v": [0, 50]},
                        },
                    },
                },
                "qos_stats_mild": {
                    "lvl": "INFO",
                    "msg": "qos stats region={region} low_prio_drop_pct={low_prio_drop_pct} low_prio_queue_depth={low_prio_queue_depth} link_group={link_group}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "link_group": {"k": "ch", "v": ["bgp-1", "bgp-2"]},
                        "low_prio_drop_pct": {"k": "f", "v": [5.0, 18.0]},
                        "low_prio_queue_depth": {"k": "i", "v": [200, 1200]},
                    },
                },
                "qos_stats_severe": {
                    "lvl": "INFO",
                    "msg": "qos stats region={region} low_prio_drop_pct={low_prio_drop_pct} low_prio_queue_depth={low_prio_queue_depth} link_group={link_group}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "link_group": {"k": "ch", "v": ["bgp-1", "bgp-2"]},
                        "low_prio_drop_pct": {"k": "f", "v": [20.0, 35.0]},
                        "low_prio_queue_depth": {"k": "i", "v": [1200, 3500]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "qos_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "qos_stats_mild", "per_min": 6.0, "scope": "per_host"},
                        {"id": "qos_stats_severe", "per_min": 6.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "net_telemetry": {
            "svc": "net-mon",
            "hosts": ["tele-1"],
            "logs": {
                "capacity_summary": {
                    "lvl": "INFO",
                    "msg": "wan capacity summary region={region} low_prio_capacity_pct={low_prio_capacity_pct} egress_loss_pct={egress_loss_pct}",
                    "vars": {"region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]}},
                    "state_vars": {
                        "n": {"low_prio_capacity_pct": {"k": "f", "v": [95.0, 100.0]}, "egress_loss_pct": {"k": "f", "v": [0.0, 1.0]}},
                        "f": {"low_prio_capacity_pct": {"k": "f", "v": [95.0, 100.0]}, "egress_loss_pct": {"k": "f", "v": [0.0, 1.0]}},
                    },
                },
                "capacity_summary_mild": {
                    "lvl": "INFO",
                    "msg": "wan capacity summary region={region} low_prio_capacity_pct={low_prio_capacity_pct} egress_loss_pct={egress_loss_pct}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "low_prio_capacity_pct": {"k": "f", "v": [60.0, 85.0]},
                        "egress_loss_pct": {"k": "f", "v": [0.0, 12.0]},
                    },
                },
                "capacity_summary_severe": {
                    "lvl": "INFO",
                    "msg": "wan capacity summary region={region} low_prio_capacity_pct={low_prio_capacity_pct} egress_loss_pct={egress_loss_pct}",
                    "vars": {
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"]},
                        "low_prio_capacity_pct": {"k": "f", "v": [35.0, 60.0]},
                        "egress_loss_pct": {"k": "f", "v": [15.0, 35.0]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "capacity_summary", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "capacity_summary_mild", "per_min": 1.0, "scope": "global"},
                        {"id": "capacity_summary_severe", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        "gcs_api": {
            "svc": "gcs-frontend",
            "hosts": ["gcs-fe-1", "gcs-fe-2"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start op={op} method={method} pri={pri} bucket_loc={bucket_loc} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["metadata_get", "object_get", "object_put", "multipart_upload_part"]},
                        "method": {"k": "ch", "v": ["GET", "PUT", "POST"]},
                        "pri": {"k": "ch", "v": ["high", "low"]},
                        "bucket_loc": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "nam4"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_ok": {
                    "lvl": "INFO",
                    "msg": "req ok op={op} status=200 latency_ms={latency_ms} bytes={bytes} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["metadata_get", "object_get", "object_put", "multipart_upload_part"]},
                        "latency_ms": {"k": "i", "v": [5, 8000]},
                        "bytes": {"k": "i", "v": [0, 10485760]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_500": {
                    "lvl": "ERROR",
                    "msg": "req failed op={op} status=500 cause={cause} latency_ms={latency_ms} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["metadata_get", "object_get", "object_put", "multipart_upload_part"]},
                        "cause": {"k": "ch", "v": ["upstream_timeout", "connect_reset", "internal_error"]},
                        "latency_ms": {"k": "i", "v": [500, 8000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http5xx_metric": {
                    "lvl": "INFO",
                    "msg": "gcs http5xx rate pri=low scope_region={scope_region} http_500_ppm={http_500_ppm}",
                    "vars": {"scope_region": {"k": "ch", "v": ["global", "us-central1", "us-east1", "southamerica-west1"]}},
                    "state_vars": {"n": {"http_500_ppm": {"k": "i", "v": [0, 5]}}, "f": {"http_500_ppm": {"k": "i", "v": [10, 90]}}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "http5xx_metric", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "http5xx_metric", "per_min": 1.0, "scope": "global"}]},
            },
        },
        "gcs_storage": {
            "svc": "gcs-backend",
            "hosts": ["gcs-store-1", "gcs-store-2"],
            "logs": {
                "backend_op_ok": {
                    "lvl": "INFO",
                    "msg": "backend op ok op={op} latency_ms={latency_ms} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["metadata_get", "object_get", "object_put", "multipart_upload_part"]},
                        "latency_ms": {"k": "i", "v": [3, 4000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "transfer_service": {
            "svc": "transfer",
            "hosts": ["xfer-1", "xfer-2"],
            "logs": {
                "job_start": {
                    "lvl": "INFO",
                    "msg": "transfer start job_id={job_id} src={src} dst_bucket_loc={dst_bucket_loc} pri=low trace={trace_id}",
                    "vars": {
                        "job_id": {"k": "uuid", "v": None},
                        "src": {"k": "ch", "v": ["bigquery_export", "streaming_ingest", "gsutil_cp"]},
                        "dst_bucket_loc": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1", "nam4"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "job_done": {
                    "lvl": "INFO",
                    "msg": "transfer done job_id={job_id} status=ok duration_ms={duration_ms} trace={trace_id}",
                    "vars": {"job_id": {"k": "uuid", "v": None}, "duration_ms": {"k": "i", "v": [200, 30000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "job_failed": {
                    "lvl": "WARN",
                    "msg": "transfer done job_id={job_id} status=failed reason={reason} duration_ms={duration_ms} trace={trace_id}",
                    "vars": {
                        "job_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["http_500", "timeout", "packet_loss"]},
                        "duration_ms": {"k": "i", "v": [1000, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "worker_backpressure": {
                    "lvl": "WARN",
                    "msg": "worker backlog queue_depth={queue_depth} oldest_age_s={oldest_age_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {"queue_depth": {"k": "i", "v": [0, 50]}, "oldest_age_s": {"k": "i", "v": [0, 5]}},
                        "f": {"queue_depth": {"k": "i", "v": [200, 4000]}, "oldest_age_s": {"k": "i", "v": [30, 600]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_backpressure", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_backpressure", "per_min": 0.8, "scope": "per_host"}]},
            },
        },
        "bq_storage_api": {
            "svc": "bq-storage",
            "hosts": ["bq-sa-1", "bq-sa-2"],
            "logs": {
                "read_start": {
                    "lvl": "INFO",
                    "msg": "storage read start dataset={dataset} region={region} pri=low req_id={req_id} trace={trace_id}",
                    "vars": {
                        "dataset": {"k": "ch", "v": ["analytics", "sales", "logs"]},
                        "region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "read_ok": {
                    "lvl": "INFO",
                    "msg": "storage read ok dataset={dataset} status=200 latency_ms={latency_ms} bytes={bytes} req_id={req_id} trace={trace_id}",
                    "vars": {
                        "dataset": {"k": "ch", "v": ["analytics", "sales", "logs"]},
                        "latency_ms": {"k": "i", "v": [10, 15000]},
                        "bytes": {"k": "i", "v": [1024, 104857600]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "rpc_latency_metric": {
                    "lvl": "INFO",
                    "msg": "rpc latency p95_ms={p95_ms} region={region}",
                    "vars": {"region": {"k": "ch", "v": ["us-central1", "us-east1", "southamerica-west1"]}},
                    "state_vars": {"n": {"p95_ms": {"k": "i", "v": [50, 250]}}, "f": {"p95_ms": {"k": "i", "v": [500, 8000]}}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rpc_latency_metric", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "rpc_latency_metric", "per_min": 1.2, "scope": "per_host"}]},
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "gcs_high_pri_metadata_get",
                "rpm": 250.0,
                "emit": ["gcs_api.req_start", "gcs_storage.backend_op_ok", "gcs_api.req_ok"],
                "latency_ms": [[0, 0], [4, 20], [8, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "gcs_low_pri_bulk_ops",
                "rpm": 450.0,
                "emit": ["gcs_api.req_start", "gcs_api.req_ok"],
                "latency_ms": [[0, 0], [25, 250]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "transfer_low_pri_success",
                "rpm": 12.0,
                "emit": ["transfer_service.job_start", "gcs_api.req_start", "gcs_storage.backend_op_ok", "gcs_api.req_ok", "transfer_service.job_done"],
                "latency_ms": [[0, 0], [10, 80], [80, 500], [10, 80], [20, 2000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "bigquery_storage_read",
                "rpm": 60.0,
                "emit": ["bq_storage_api.read_start", "bq_storage_api.read_ok"],
                "latency_ms": [[0, 0], [80, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "gcs_high_pri_metadata_get",
                "rpm": 250.0,
                "emit": ["gcs_api.req_start", "gcs_storage.backend_op_ok", "gcs_api.req_ok"],
                "latency_ms": [[0, 0], [5, 25], [10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "gcs_low_pri_bulk_ops",
                "rpm": 450.0,
                "emit": ["gcs_api.req_start", "gcs_api.req_ok"],
                "latency_ms": [[0, 0], [120, 900]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "transfer_low_pri_ok",
                "rpm": 10.0,
                "emit": ["transfer_service.job_start", "gcs_api.req_start", "gcs_storage.backend_op_ok", "gcs_api.req_ok", "transfer_service.job_done"],
                "latency_ms": [[0, 0], [30, 200], [250, 2500], [20, 200], [50, 5000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "transfer_low_pri_500",
                "rpm": 0.04,
                "emit": ["transfer_service.job_start", "gcs_api.req_start", "gcs_api.req_500", "transfer_service.job_failed"],
                "latency_ms": [[0, 0], [50, 400], [800, 6000], [50, 2000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "bigquery_storage_read",
                "rpm": 60.0,
                "emit": ["bq_storage_api.read_start", "bq_storage_api.read_ok"],
                "latency_ms": [[0, 0], [250, 2000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "wan_low_priority_capacity_reduction_rollout_rollback",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "sdn_controller.controller_disconnected": 0.0,
                            "transfer_service.worker_backpressure": 0.0,
                            "backbone_edge.qos_stats_mild": 1.0,
                            "backbone_edge.qos_stats_severe": 0.0,
                            "net_telemetry.capacity_summary_mild": 1.0,
                            "net_telemetry.capacity_summary_severe": 0.0,
                        },
                        "latency_multipliers": {
                            "gcs_low_pri_bulk_ops": {"p50": 1.2, "p95": 1.5},
                            "transfer_low_pri_ok": {"p50": 1.2, "p95": 1.4},
                            "transfer_low_pri_500": {"p50": 1.1, "p95": 1.3},
                            "bigquery_storage_read": {"p50": 1.3, "p95": 1.6},
                            "gcs_high_pri_metadata_get": {"p50": 1.0, "p95": 1.0},
                        },
                        "one_shots": [
                            {"ref": "rollout_controller.rollout_started", "count": 1, "hosts": ["rc-1"]},
                            {"ref": "sdn_controller.apply_partial", "count": 1, "hosts": ["sdn-a"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 30,
                        "rate_multipliers": {
                            "sdn_controller.controller_disconnected": 1.0,
                            "transfer_service.worker_backpressure": 1.0,
                            "transfer_low_pri_ok": 0.8,
                            "backbone_edge.qos_stats_mild": 0.0,
                            "backbone_edge.qos_stats_severe": 1.0,
                            "net_telemetry.capacity_summary_mild": 0.0,
                            "net_telemetry.capacity_summary_severe": 1.0,
                        },
                        "latency_multipliers": {
                            "gcs_low_pri_bulk_ops": {"p50": 1.8, "p95": 2.6},
                            "transfer_low_pri_ok": {"p50": 1.6, "p95": 2.2},
                            "transfer_low_pri_500": {"p50": 1.4, "p95": 1.8},
                            "bigquery_storage_read": {"p50": 1.8, "p95": 2.5},
                        },
                        "one_shots": [{"ref": "rollout_controller.rollout_paused", "count": 1, "hosts": ["rc-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "rate_multipliers": {
                            "sdn_controller.controller_disconnected": 2.0,
                            "backbone_edge.qos_stats_mild": 0.0,
                            "backbone_edge.qos_stats_severe": 1.0,
                            "net_telemetry.capacity_summary_mild": 0.0,
                            "net_telemetry.capacity_summary_severe": 1.0,
                        },
                        "latency_multipliers": {
                            "gcs_low_pri_bulk_ops": {"p50": 1.9, "p95": 2.8},
                            "transfer_low_pri_ok": {"p50": 1.7, "p95": 2.3},
                            "transfer_low_pri_500": {"p50": 1.5, "p95": 1.9},
                            "bigquery_storage_read": {"p50": 1.9, "p95": 2.6},
                        },
                        "one_shots": [
                            {"ref": "rollout_controller.rollback_initiated", "count": 1, "hosts": ["rc-1"]},
                            {"ref": "sdn_controller.apply_partial", "count": 1, "hosts": ["sdn-b"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 45,
                        "rate_multipliers": {
                            "sdn_controller.controller_disconnected": 0.5,
                            "transfer_service.worker_backpressure": 0.6,
                            "transfer_low_pri_ok": 1.0,
                            "transfer_low_pri_500": 0.4,
                            "backbone_edge.qos_stats_mild": 1.0,
                            "backbone_edge.qos_stats_severe": 0.0,
                            "net_telemetry.capacity_summary_mild": 1.0,
                            "net_telemetry.capacity_summary_severe": 0.0,
                        },
                        "latency_multipliers": {
                            "gcs_low_pri_bulk_ops": {"p50": 1.2, "p95": 1.6},
                            "transfer_low_pri_ok": {"p50": 1.2, "p95": 1.6},
                            "transfer_low_pri_500": {"p50": 1.2, "p95": 1.5},
                            "bigquery_storage_read": {"p50": 1.2, "p95": 1.5},
                        },
                        "one_shots": [{"ref": "sdn_controller.apply_ok", "count": 1, "hosts": ["sdn-a"]}],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Helpers
# -----------------------------

SEED = "incident-sim-v3|cloud_backbone_low_priority_capacity|wan_low_priority_capacity_reduction_rollout_rollback"
random.seed(SEED)
np.random.seed(int(hashlib.md5(SEED.encode("utf-8")).hexdigest()[:8], 16))


def md5_hex(s: str) -> str:
    return hashlib.md5((SEED + "|" + s).encode("utf-8")).hexdigest()


def stable_u01(key: str) -> float:
    h = md5_hex(key)
    v = int(h[:14], 16)  # 56 bits
    return (v & ((1 << 53) - 1)) / float(1 << 53)


def stable_choice(options: List[Any], key: str) -> Any:
    if not options:
        return None
    idx = int(stable_u01(key) * len(options)) % len(options)
    return options[idx]


def stable_int(lo: int, hi: int, key: str) -> int:
    if hi <= lo:
        return int(lo)
    u = stable_u01(key)
    return int(lo + math.floor(u * (hi - lo + 1)))


def stable_float(lo: float, hi: float, key: str, decimals: int = 1) -> float:
    if hi <= lo:
        return round(float(lo), decimals)
    u = stable_u01(key)
    return round(lo + u * (hi - lo), decimals)


def pseudo_uuid(key: str) -> str:
    h = md5_hex("uuid|" + key)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def hex_n(key: str, n: int) -> str:
    h = md5_hex("hex|" + key)
    return h[:n].lower()


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")


def ms_between(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds() * 1000.0


def ms_floor(x: float) -> int:
    return int(math.floor(x + 1e-9))


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Dict[str, Any]]
    state_vars: Dict[str, Dict[str, Dict[str, Any]]]


PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def render_template(tpl: LogTemplate, state: str, key: str, overrides: Dict[str, Any]) -> str:
    domains: Dict[str, Dict[str, Any]] = {}
    domains.update(tpl.vars or {})
    if tpl.state_vars and state in tpl.state_vars:
        domains.update(tpl.state_vars[state] or {})

    needed = set(PLACEHOLDER_RE.findall(tpl.msg))

    vals: Dict[str, Any] = {}
    for var in needed:
        if var in overrides:
            vals[var] = overrides[var]
            continue
        dom = domains.get(var)
        if dom is None:
            vals[var] = ""
            continue
        k = dom.get("k")
        v = dom.get("v")
        if k == "ch":
            vals[var] = stable_choice(list(v), f"{key}|{tpl.component_id}.{tpl.log_id}|{var}")
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            vals[var] = stable_int(lo, hi, f"{key}|{tpl.component_id}.{tpl.log_id}|{var}")
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            vals[var] = stable_float(lo, hi, f"{key}|{tpl.component_id}.{tpl.log_id}|{var}", decimals=1)
        elif k == "uuid":
            vals[var] = pseudo_uuid(f"{key}|{tpl.component_id}.{tpl.log_id}|{var}")
        elif k == "hex":
            n = int(v) if v is not None else 32
            vals[var] = hex_n(f"{key}|{tpl.component_id}.{tpl.log_id}|{var}", n)
        else:
            vals[var] = ""

    for k2, v2 in list(vals.items()):
        if isinstance(v2, float):
            vals[k2] = f"{v2:.1f}"
        else:
            vals[k2] = str(v2)

    return tpl.msg.format(**vals)


def get_int_bounds(tpl_ref: str, state: str, var_name: str) -> Optional[Tuple[int, int]]:
    tpl = LOGS[tpl_ref]
    dom = (tpl.vars or {}).get(var_name)
    if dom is None and tpl.state_vars and state in tpl.state_vars:
        dom = (tpl.state_vars[state] or {}).get(var_name)
    if not dom:
        return None
    if dom.get("k") != "i" or dom.get("v") is None:
        return None
    return int(dom["v"][0]), int(dom["v"][1])


def sample_lognormal_from_p50_p95(p50: float, p95: float, q: float) -> float:
    if p50 <= 0.0 and p95 <= 0.0:
        return 0.0
    if p50 <= 0.0:
        return max(0.0, 0.1 * p95)
    if p95 <= p50:
        return max(0.0, p50)
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722
    z = NormalDist().inv_cdf(min(max(q, 1e-6), 1 - 1e-6))
    return math.exp(mu + sigma * z)


def clamp_dt(dt: datetime, start: datetime, end: datetime) -> datetime:
    if dt < start:
        return start
    if dt >= end:
        return end - timedelta(milliseconds=1)
    return dt


def spread_times(start: datetime, end: datetime, count: int, key: str, jitter_ms: int = 200) -> List[datetime]:
    if count <= 0:
        return []
    total_s = (end - start).total_seconds()
    out: List[datetime] = []
    for i in range(count):
        pos = (i + 0.5) / count
        base = start + timedelta(seconds=total_s * pos)
        j = (stable_u01(f"{key}|t|{i}") - 0.5) * jitter_ms
        dt = base + timedelta(milliseconds=j)
        out.append(clamp_dt(dt, start, end))
    return out


# -----------------------------
# Build indices
# -----------------------------

COMP = SYSTEM["components"]

LOGS: Dict[str, LogTemplate] = {}
for cid, c in COMP.items():
    for lid, l in c["logs"].items():
        LOGS[f"{cid}.{lid}"] = LogTemplate(
            component_id=cid,
            log_id=lid,
            lvl=l["lvl"],
            msg=l["msg"],
            vars=l.get("vars") or {},
            state_vars=l.get("state_vars") or {},
        )

FLOWS: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]:
        FLOWS[st][f["id"]] = f

# -----------------------------
# Failure control timeline (piecewise persistent)
# -----------------------------


def build_failure_intervals() -> List[Dict[str, Any]]:
    scen = SCENARIO["scenario"]
    fstart = int(scen["time"]["phases"]["f"]["start_min"])
    fend = int(scen["time"]["phases"]["f"]["end_min"])
    events = sorted(scen["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [fstart] + [int(e["at_min"]) for e in events if fstart <= int(e["at_min"]) < fend] + [fend]
    b2 = []
    for b in boundaries:
        if not b2 or b2[-1] != b:
            b2.append(b)
    boundaries = b2

    rate_state: Dict[str, float] = {}
    lat_state: Dict[str, Dict[str, float]] = {}

    at_map: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        at = int(e["at_min"])
        at_map.setdefault(at, []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        a = boundaries[i]
        b = boundaries[i + 1]
        for e in at_map.get(a, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                rate_state[k] = float(v)
            for fk, mv in (e.get("latency_multipliers") or {}).items():
                lat_state[fk] = {"p50": float(mv["p50"]), "p95": float(mv["p95"])}
        intervals.append(
            {
                "start_min": a,
                "end_min": b,
                "rate_multipliers": dict(rate_state),
                "latency_multipliers": dict(lat_state),
                "events_at_start": at_map.get(a, []),
            }
        )
    return intervals


FAIL_INTERVALS = build_failure_intervals()

# -----------------------------
# Deterministic count allocator
# -----------------------------

CARRY: Dict[str, float] = {}


def alloc_count(expected: float, source_key: str, interval_idx: int) -> int:
    x = expected + CARRY.get(source_key, 0.0)
    base = math.floor(x + 1e-12)
    frac = x - base
    u = stable_u01(f"{source_key}|alloc|{interval_idx}")
    add = 1 if u < frac else 0
    n = int(base + add)
    CARRY[source_key] = x - n
    if n < 0:
        n = 0
        CARRY[source_key] = 0.0
    return n


# -----------------------------
# Simulation
# -----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

rows: List[Dict[str, Any]] = []

trace_counter = 0


def make_trace_id() -> str:
    global trace_counter
    trace_counter += 1
    return md5_hex(f"trace|{trace_counter}")[:32].lower()


def component_identity(component_id: str, host: Optional[str]) -> Tuple[str, str]:
    svc = COMP[component_id].get("svc") or ""
    h = host or ""
    return svc, h


EDGE_HOST_REGION = {
    "be-uscentral": "us-central1",
    "be-useast": "us-east1",
    "be-samw": "southamerica-west1",
}


def pick_host_for_component(component_id: str, chain_key: str) -> str:
    hosts = COMP[component_id].get("hosts") or []
    if not hosts:
        return ""
    return str(stable_choice(hosts, f"{chain_key}|host|{component_id}"))


def emit_log(dt: datetime, tpl_ref: str, state: str, key: str, host: Optional[str], trace_id_col: str, overrides: Dict[str, Any]) -> None:
    tpl = LOGS[tpl_ref]
    svc, h = component_identity(tpl.component_id, host)
    msg = render_template(tpl, state=state, key=key, overrides=overrides)
    rows.append(
        {
            "timestamp": dt,
            "level": tpl.lvl,
            "message": msg,
            "trace_id": trace_id_col,
            "service": svc,
            "host": h,
        }
    )


def gen_background_for_interval(state: str, start_min: int, end_min: int, interval_idx: int, rate_mult: Dict[str, float]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for cid, c in COMP.items():
        beh = c.get("beh", {}).get(state, {}).get("emit", []) or []
        for e in beh:
            log_id = e["id"]
            tpl_ref = f"{cid}.{log_id}"
            per_min = float(e["per_min"])
            scope = e.get("scope") or "per_host"

            mult = float(rate_mult.get(tpl_ref, 1.0)) if state == "f" else 1.0
            eff = per_min * mult
            if eff <= 0.0:
                continue

            if scope == "global":
                expected = eff * dur_min
                n = alloc_count(expected, f"bg|{state}|{tpl_ref}|global", interval_idx)
                times = spread_times(start_dt, end_dt, n, f"bg|{state}|{tpl_ref}|global|{interval_idx}", jitter_ms=400)
                hosts = c.get("hosts") or []
                for j, t in enumerate(times):
                    host = ""
                    if hosts:
                        host = str(stable_choice(hosts, f"bg|{state}|{tpl_ref}|global|host|{interval_idx}|{j}"))
                    overrides: Dict[str, Any] = {}

                    if tpl_ref == "sdn_controller.controller_heartbeat" and host:
                        overrides["leader"] = host
                    if tpl_ref.startswith("net_telemetry.capacity_summary"):
                        overrides["region"] = stable_choice(
                            COMP["net_telemetry"]["logs"][tpl_ref.split(".", 1)[1]]["vars"]["region"]["v"],
                            f"bg|{tpl_ref}|region|{interval_idx}|{j}",
                        )
                    emit_log(t, tpl_ref, state, f"bg|{tpl_ref}|{interval_idx}|{j}", host, "", overrides)
            else:
                hosts = c.get("hosts") or []
                for host in hosts:
                    expected = eff * dur_min
                    n = alloc_count(expected, f"bg|{state}|{tpl_ref}|{host}", interval_idx)
                    times = spread_times(start_dt, end_dt, n, f"bg|{state}|{tpl_ref}|{host}|{interval_idx}", jitter_ms=400)
                    for j, t in enumerate(times):
                        overrides = {}
                        if tpl_ref == "sdn_controller.controller_heartbeat":
                            overrides["leader"] = host
                        if tpl_ref.startswith("backbone_edge.qos_stats"):
                            overrides["region"] = EDGE_HOST_REGION.get(
                                host,
                                stable_choice(["us-central1", "us-east1", "southamerica-west1", "southamerica-east1"], f"{tpl_ref}|region|{host}"),
                            )
                        emit_log(t, tpl_ref, state, f"bg|{tpl_ref}|{host}|{interval_idx}|{j}", host, "", overrides)


def gcs_method_for_op(op: str) -> str:
    if op in ("metadata_get", "object_get"):
        return "GET"
    if op == "object_put":
        return "PUT"
    return "POST"


def clamp_gap_to_template(delta_ms: float, tpl_ref: str, state: str, var_name: str) -> float:
    bounds = get_int_bounds(tpl_ref, state, var_name)
    if not bounds:
        return max(0.0, delta_ms)
    lo, hi = bounds
    return max(0.0, min(max(delta_ms, float(lo)), float(hi)))


def clamp_total_to_template(delta_ms: float, current_time: datetime, start_time: datetime, tpl_ref: str, state: str, var_name: str) -> float:
    bounds = get_int_bounds(tpl_ref, state, var_name)
    if not bounds:
        return max(0.0, delta_ms)
    lo, hi = bounds
    before_ms = ms_between(current_time, start_time)
    min_d = max(0.0, float(lo) - before_ms)
    max_d = max(0.0, float(hi) - before_ms + 0.999)  # allow floor() <= hi
    if max_d < min_d:
        return 0.0
    return max(0.0, min(max(delta_ms, min_d), max_d))


def simulate_flow_instance(flow: Dict[str, Any], state: str, start_dt: datetime, interval_idx: int, lat_mult: Dict[str, Dict[str, float]], rate_key: str) -> None:
    trace_id = make_trace_id() if (SYSTEM["tracing"]["on"] and flow.get("trace")) else ""
    chain_key = f"{rate_key}|{flow['id']}|{interval_idx}|{trace_id}"

    comp_host: Dict[str, str] = {}
    for ref in flow["emit"]:
        cid, _ = ref.split(".", 1)
        if cid not in comp_host:
            comp_host[cid] = pick_host_for_component(cid, chain_key)

    req_id = pseudo_uuid(f"req|{chain_key}")
    job_id = pseudo_uuid(f"job|{chain_key}")

    flow_id = flow["id"]
    if flow_id == "gcs_high_pri_metadata_get":
        op = "metadata_get"
        pri = "high"
    elif flow_id.startswith("gcs_low_pri_bulk_ops"):
        pri = "low"
        op = stable_choice(["object_get", "object_put", "multipart_upload_part"], f"{chain_key}|op")
    elif flow_id.startswith("transfer_"):
        pri = "low"
        op = stable_choice(["object_put", "multipart_upload_part"], f"{chain_key}|op")
    else:
        pri = "low"
        op = stable_choice(["object_get", "object_put", "multipart_upload_part"], f"{chain_key}|op")

    method = gcs_method_for_op(op)
    bucket_loc = stable_choice(["us-central1", "us-east1", "southamerica-west1", "nam4"], f"{chain_key}|bucket")
    dataset = stable_choice(["analytics", "sales", "logs"], f"{chain_key}|dataset")
    bq_region = stable_choice(["us-central1", "us-east1", "southamerica-west1"], f"{chain_key}|bqregion")
    xfer_src = stable_choice(["bigquery_export", "streaming_ingest", "gsutil_cp"], f"{chain_key}|src")

    m = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}

    emit_refs = flow["emit"]
    gcs_start_idx = emit_refs.index("gcs_api.req_start") if "gcs_api.req_start" in emit_refs else None
    job_start_idx = emit_refs.index("transfer_service.job_start") if "transfer_service.job_start" in emit_refs else None
    bq_start_idx = emit_refs.index("bq_storage_api.read_start") if "bq_storage_api.read_start" in emit_refs else None

    times: List[datetime] = [start_dt]
    for j in range(1, len(emit_refs)):
        p50, p95 = float(flow["latency_ms"][j][0]), float(flow["latency_ms"][j][1])
        sp50 = p50 * float(m["p50"])
        sp95 = p95 * float(m["p95"])
        u = stable_u01(f"{chain_key}|lat|{j}")
        q = 0.50 + 0.45 * u
        delta = sample_lognormal_from_p50_p95(sp50, max(sp95, sp50), q)

        ref_j = emit_refs[j]
        if ref_j == "gcs_storage.backend_op_ok":
            delta = clamp_gap_to_template(delta, ref_j, state, "latency_ms")

        if ref_j in ("gcs_api.req_ok", "gcs_api.req_500") and gcs_start_idx is not None:
            delta = clamp_total_to_template(delta, times[-1], times[gcs_start_idx], ref_j, state, "latency_ms")
        elif ref_j == "bq_storage_api.read_ok" and bq_start_idx is not None:
            delta = clamp_total_to_template(delta, times[-1], times[bq_start_idx], ref_j, state, "latency_ms")
        elif ref_j in ("transfer_service.job_done", "transfer_service.job_failed") and job_start_idx is not None:
            delta = clamp_total_to_template(delta, times[-1], times[job_start_idx], ref_j, state, "duration_ms")

        times.append(times[-1] + timedelta(milliseconds=float(max(0.0, delta))))

    ref_to_time = {emit_refs[k]: times[k] for k in range(len(times))}
    gcs_req_start_time = ref_to_time.get("gcs_api.req_start")
    transfer_start_time = ref_to_time.get("transfer_service.job_start")

    for idx, ref in enumerate(emit_refs):
        cid, _lid = ref.split(".", 1)
        host = comp_host.get(cid, "")
        overrides: Dict[str, Any] = {}

        if "{trace_id}" in LOGS[ref].msg:
            overrides["trace_id"] = trace_id

        if ref == "gcs_api.req_start":
            overrides.update({"op": op, "method": method, "pri": pri, "bucket_loc": bucket_loc, "req_id": req_id, "trace_id": trace_id})
        elif ref == "gcs_api.req_ok":
            if gcs_req_start_time is None:
                gcs_req_start_time = start_dt
            latency_ms = max(0, ms_floor(ms_between(times[idx], gcs_req_start_time)))
            overrides.update(
                {
                    "op": op,
                    "latency_ms": latency_ms,
                    "bytes": 0 if op == "metadata_get" else stable_int(1024, 10485760, f"{chain_key}|bytes|{idx}"),
                    "req_id": req_id,
                    "trace_id": trace_id,
                }
            )
        elif ref == "gcs_api.req_500":
            if gcs_req_start_time is None:
                gcs_req_start_time = start_dt
            latency_ms = max(0, ms_floor(ms_between(times[idx], gcs_req_start_time)))
            cause = "upstream_timeout" if stable_u01(f"{chain_key}|cause") < 0.8 else stable_choice(["connect_reset", "internal_error"], f"{chain_key}|cause2")
            overrides.update({"op": op, "cause": cause, "latency_ms": latency_ms, "req_id": req_id, "trace_id": trace_id})
        elif ref == "gcs_storage.backend_op_ok":
            prev_time = times[idx - 1] if idx > 0 else start_dt
            bl = max(0, ms_floor(ms_between(times[idx], prev_time)))
            overrides.update({"op": op, "latency_ms": bl, "trace_id": trace_id})
        elif ref == "transfer_service.job_start":
            overrides.update({"job_id": job_id, "src": xfer_src, "dst_bucket_loc": bucket_loc, "trace_id": trace_id})
        elif ref == "transfer_service.job_done":
            if transfer_start_time is None:
                transfer_start_time = start_dt
            dur_ms = max(0, ms_floor(ms_between(times[idx], transfer_start_time)))
            overrides.update({"job_id": job_id, "duration_ms": dur_ms, "trace_id": trace_id})
        elif ref == "transfer_service.job_failed":
            if transfer_start_time is None:
                transfer_start_time = start_dt
            dur_ms = max(0, ms_floor(ms_between(times[idx], transfer_start_time)))
            overrides.update({"job_id": job_id, "reason": "http_500", "duration_ms": dur_ms, "trace_id": trace_id})
        elif ref == "bq_storage_api.read_start":
            overrides.update({"dataset": dataset, "region": bq_region, "req_id": req_id, "trace_id": trace_id})
        elif ref == "bq_storage_api.read_ok":
            start_t = ref_to_time.get("bq_storage_api.read_start", start_dt)
            latency_ms = max(0, ms_floor(ms_between(times[idx], start_t)))
            overrides.update(
                {
                    "dataset": dataset,
                    "latency_ms": latency_ms,
                    "bytes": stable_int(1024, 104857600, f"{chain_key}|bqbytes"),
                    "req_id": req_id,
                    "trace_id": trace_id,
                }
            )

        emit_log(times[idx], ref, state, f"{chain_key}|emit|{idx}", host, trace_id, overrides)


def gen_flows_for_interval(state: str, start_min: int, end_min: int, interval_idx: int, rate_mult: Dict[str, float], lat_mult: Dict[str, Dict[str, float]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    flows = SYSTEM["flows"][state]
    for f in flows:
        flow_id = f["id"]
        rpm = float(f["rpm"])
        mult = float(rate_mult.get(flow_id, 1.0)) if state == "f" else 1.0
        eff_rpm = rpm * mult
        if eff_rpm <= 0.0:
            continue
        expected = eff_rpm * dur_min
        n = alloc_count(expected, f"flow|{state}|{flow_id}", interval_idx)
        starts = spread_times(start_dt, end_dt, n, f"flow|{state}|{flow_id}|{interval_idx}", jitter_ms=300)
        for j, sdt in enumerate(starts):
            simulate_flow_instance(FLOWS[state][flow_id], state, sdt, interval_idx, lat_mult, f"flow|{state}|{flow_id}|{interval_idx}|{j}")


def emit_one_shots(event: Dict[str, Any], at_min: int, event_idx: int) -> None:
    # One-shots must occur at/after the scenario's specified event time (not before),
    # and with small sub-minute forward jitter.
    base = BASE_TIME + timedelta(minutes=at_min)
    window_end = base + timedelta(minutes=1)
    for os in event.get("one_shots") or []:
        ref = os["ref"]
        count = int(os["count"])
        hosts = os.get("hosts") or []
        for k in range(count):
            j_ms = stable_u01(f"oneshot|{event_idx}|{ref}|{k}") * 4000.0  # 0..4s forward jitter
            dt = clamp_dt(base + timedelta(milliseconds=j_ms), base, window_end)

            cid, _lid = ref.split(".", 1)
            host = hosts[k % len(hosts)] if hosts else pick_host_for_component(cid, f"oneshot|{event_idx}|{ref}|{k}")

            overrides: Dict[str, Any] = {}
            if ref == "rollout_controller.rollout_started":
                overrides["config_version"] = "2022.07.15.0"
                overrides["scope"] = "global"
                overrides["domain"] = "wan-backbone"
            elif ref == "rollout_controller.rollout_paused":
                overrides["domain"] = "wan-backbone"
                overrides["reason"] = "capacity_drop_detected"
            elif ref == "rollout_controller.rollback_initiated":
                overrides["domain"] = "wan-backbone"
                overrides["config_version"] = "2022.07.14.1"
            elif ref == "sdn_controller.apply_partial":
                overrides["domain"] = "wan-backbone"
                overrides["config_version"] = "2022.07.15.0" if at_min == 25 else "2022.07.14.1"
            elif ref == "sdn_controller.apply_ok":
                overrides["domain"] = "wan-backbone"
                overrides["config_version"] = "2022.07.14.1"

            emit_log(dt, ref, "f", f"oneshot|{event_idx}|{ref}|{k}", host, "", overrides)


# -----------------------------
# Run timeline
# -----------------------------

n_start = int(SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"])
n_end = int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"])
gen_background_for_interval("n", n_start, n_end, interval_idx=0, rate_mult={})
gen_flows_for_interval("n", n_start, n_end, interval_idx=0, rate_mult={}, lat_mult={})

for idx, itv in enumerate(FAIL_INTERVALS, start=1):
    sm, em = int(itv["start_min"]), int(itv["end_min"])
    rmult = itv["rate_multipliers"]
    lmult = itv["latency_multipliers"]

    gen_background_for_interval("f", sm, em, interval_idx=idx, rate_mult=rmult)
    gen_flows_for_interval("f", sm, em, interval_idx=idx, rate_mult=rmult, lat_mult=lmult)

    for ev in itv.get("events_at_start") or []:
        emit_one_shots(ev, at_min=int(ev["at_min"]), event_idx=int(ev["order"]))

# -----------------------------
# Output logs.csv
# -----------------------------

df = pd.DataFrame(rows)
df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
df["timestamp"] = df["timestamp"].map(isoformat_ms)

df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
for c in ["timestamp", "level", "message", "trace_id", "service", "host"]:
    df[c] = df[c].astype(str)

df.to_csv("logs.csv", index=False)
