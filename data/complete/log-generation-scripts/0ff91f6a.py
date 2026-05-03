import math
import re
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "slack_like_messaging",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_gateway"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "slack_client": {
            "svc": "client",
            "hosts": ["client_pool"],
            "logs": {
                "boot_req_send": {
                    "lvl": "INFO",
                    "msg": "boot attempt send req_id={req_id} user_id={user_id} client_ver={client_ver}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "user_id": {"k": "i", "v": [100000, 199999]},
                        "client_ver": {"k": "ch", "v": ["desktop_4.29", "desktop_4.30", "web_2024.02"]},
                    },
                },
                "boot_retry": {
                    "lvl": "WARN",
                    "msg": "boot retry scheduled req_id={req_id} next_attempt={next_attempt} backoff_ms={backoff_ms} reason={reason}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "backoff_ms": {"k": "i", "v": [300, 15000]},
                        "reason": {"k": "ch", "v": ["http_503", "http_429", "timeout"]},
                    },
                    "state_vars": {
                        "n": {"next_attempt": {"k": "i", "v": [2, 3]}},
                        "f": {"next_attempt": {"k": "i", "v": [2, 4]}},
                    },
                },
                "boot_resp_ok": {
                    "lvl": "INFO",
                    "msg": "boot attempt result req_id={req_id} status=200 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [80, 2500]}},
                },
                "boot_resp_503": {
                    "lvl": "ERROR",
                    "msg": "boot attempt result req_id={req_id} status=503 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [400, 9000]}},
                },
                "boot_resp_429": {
                    "lvl": "WARN",
                    "msg": "boot attempt result req_id={req_id} status=429 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [20, 400]}},
                },
                "msg_req_send": {
                    "lvl": "INFO",
                    "msg": "send_message send req_id={req_id} user_id={user_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "user_id": {"k": "i", "v": [100000, 199999]}},
                },
                "msg_resp_ok": {
                    "lvl": "INFO",
                    "msg": "send_message result req_id={req_id} status=200 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [20, 900]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "edge_gateway": {
            "svc": "edge",
            "hosts": ["edge-a", "edge-b"],
            "logs": {
                "boot_access_200": {
                    "lvl": "INFO",
                    "msg": "access op=boot status=200 dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [50, 3000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_access_503": {
                    "lvl": "WARN",
                    "msg": "access op=boot status=503 dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [200, 10000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_access_429": {
                    "lvl": "INFO",
                    "msg": "access op=boot status=429 dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [10, 600]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "msg_access_200": {
                    "lvl": "INFO",
                    "msg": "access op=send_message status=200 dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [20, 1500]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_health": {
                    "lvl": "INFO",
                    "msg": "edge health ok active_upstreams={active_upstreams}",
                    "vars": {"active_upstreams": {"k": "i", "v": [2, 6]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_health", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "edge_health", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        "webapp_api": {
            "svc": "webapp",
            "hosts": ["app-a", "app-b"],
            "logs": {
                "boot_start": {
                    "lvl": "INFO",
                    "msg": "boot start req_id={req_id} user_id={user_id} workspace_id={workspace_id} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "user_id": {"k": "i", "v": [100000, 199999]},
                        "workspace_id": {"k": "i", "v": [1000, 1999]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_complete_200": {
                    "lvl": "INFO",
                    "msg": "boot complete req_id={req_id} status=200 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [80, 3000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_complete_503": {
                    "lvl": "ERROR",
                    "msg": "boot complete req_id={req_id} status=503 dur_ms={dur_ms} err={err} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [400, 10000]},
                        "err": {"k": "ch", "v": ["vitess_timeout", "vitess_overloaded", "upstream_unavailable"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_throttled_429": {
                    "lvl": "WARN",
                    "msg": "boot throttled req_id={req_id} status=429 limit_rpm={limit_rpm} trace_id={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "limit_rpm": {"k": "i", "v": [40, 160]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "boot_throttle_updated": {
                    "lvl": "INFO",
                    "msg": "boot throttle updated new_limit_rpm={limit_rpm} actor={actor}",
                    "vars": {
                        "limit_rpm": {"k": "i", "v": [40, 200]},
                        "actor": {"k": "ch", "v": ["incident_commander", "oncall_webapp", "sre"]},
                    },
                },
                "deploy_query_patch": {
                    "lvl": "INFO",
                    "msg": "deploy applied change={change} rollout_id={rollout_id}",
                    "vars": {"change": {"k": "ch", "v": ["gdm_query_targeted_misses_read_replicas"]}, "rollout_id": {"k": "hex", "v": 8}},
                },
                "msg_start": {
                    "lvl": "INFO",
                    "msg": "send_message start req_id={req_id} user_id={user_id} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "user_id": {"k": "i", "v": [100000, 199999]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "msg_complete_200": {
                    "lvl": "INFO",
                    "msg": "send_message complete req_id={req_id} status=200 dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [20, 1500]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "app_health": {
                    "lvl": "INFO",
                    "msg": "webapp health ok in_flight={in_flight}",
                    "vars": {"in_flight": {"k": "i", "v": [10, 400]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "app_health", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "app_health", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        "memcached_fleet": {
            "svc": "memcached",
            "hosts": ["memcached-12", "memcached-34", "memcached-57", "memcached-81"],
            "logs": {
                "memcached_stats": {
                    "lvl": "INFO",
                    "msg": "memcached stats uptime_s={uptime_s} curr_items={curr_items} evictions={evictions}",
                    "vars": {"uptime_s": {"k": "i", "v": [1000, 250000]}},
                    "state_vars": {
                        "n": {"curr_items": {"k": "i", "v": [200000, 800000]}, "evictions": {"k": "i", "v": [0, 50]}},
                        "f": {"curr_items": {"k": "i", "v": [10000, 300000]}, "evictions": {"k": "i", "v": [0, 200]}},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "memcached_stats", "per_min": 0.1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "memcached_stats", "per_min": 0.1, "scope": "per_host"}]},
            },
        },
        "mcrouter_cache": {
            "svc": "mcrouter",
            "hosts": ["mcrouter-a", "mcrouter-b"],
            "logs": {
                "mc_get_hit": {
                    "lvl": "INFO",
                    "msg": "mc get hit key={key} ring_ver={ring_ver} trace_id={trace_id}",
                    "vars": {"key": {"k": "str", "v": "gdm_membership:{channel_id}"}, "ring_ver": {"k": "i", "v": [1200, 1400]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "mc_get_miss": {
                    "lvl": "WARN",
                    "msg": "mc get miss key={key} ring_ver={ring_ver} trace_id={trace_id}",
                    "vars": {"key": {"k": "str", "v": "gdm_membership:{channel_id}"}, "ring_ver": {"k": "i", "v": [1200, 1400]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "mcrouter_stats": {
                    "lvl": "INFO",
                    "msg": "mcrouter stats hit_rate={hit_rate} ring_ver={ring_ver} active_nodes={active_nodes}",
                    "vars": {"active_nodes": {"k": "i", "v": [850, 1000]}},
                    "state_vars": {
                        "n": {"hit_rate": {"k": "f", "v": [0.92, 0.99]}, "ring_ver": {"k": "i", "v": [1200, 1215]}},
                        "f": {"hit_rate": {"k": "f", "v": [0.25, 0.70]}, "ring_ver": {"k": "i", "v": [1210, 1350]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "mcrouter_stats", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "mcrouter_stats", "per_min": 1.0, "scope": "global"}]},
            },
        },
        "mcrib_control": {
            "svc": "mcrib",
            "hosts": ["mcrib-1"],
            "logs": {
                "ring_push": {
                    "lvl": "INFO",
                    "msg": "mcrib pushed ring ring_ver={ring_ver} active_nodes={active_nodes} spares_used={spares_used} reason={reason}",
                    "vars": {
                        "ring_ver": {"k": "i", "v": [1200, 1400]},
                        "active_nodes": {"k": "i", "v": [850, 1000]},
                        "spares_used": {"k": "i", "v": [0, 80]},
                        "reason": {"k": "ch", "v": ["consul_deregister", "node_flap", "maintenance_restart"]},
                    },
                },
                "node_flush": {
                    "lvl": "WARN",
                    "msg": "mcrib flushed node={node} reason={reason}",
                    "vars": {"node": {"k": "ch", "v": ["memcached-12", "memcached-34", "memcached-57", "memcached-81"]}, "reason": {"k": "ch", "v": ["rejoin_after_unavailable", "stale_data_risk"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ring_push", "per_min": 0.2, "scope": "global"}, {"id": "node_flush", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "ring_push", "per_min": 1.2, "scope": "global"}, {"id": "node_flush", "per_min": 0.4, "scope": "global"}]},
            },
        },
        "consul": {
            "svc": "consul",
            "hosts": ["consul-ops-1"],
            "logs": {
                "rollout_step": {
                    "lvl": "INFO",
                    "msg": "consul maintenance step={step} percent={percent}",
                    "vars": {"step": {"k": "ch", "v": ["pbr_binary_update", "sequential_agent_restart"]}, "percent": {"k": "i", "v": [10, 50]}},
                },
                "agent_restart": {
                    "lvl": "INFO",
                    "msg": "consul agent restart initiated node={node} service=memcached",
                    "vars": {"node": {"k": "ch", "v": ["memcached-12", "memcached-34", "memcached-57", "memcached-81"]}},
                },
                "service_deregister": {
                    "lvl": "WARN",
                    "msg": "consul service deregistered node={node} service=memcached",
                    "vars": {"node": {"k": "ch", "v": ["memcached-12", "memcached-34", "memcached-57", "memcached-81"]}},
                },
                "service_register": {
                    "lvl": "INFO",
                    "msg": "consul service registered node={node} service=memcached",
                    "vars": {"node": {"k": "ch", "v": ["memcached-12", "memcached-34", "memcached-57", "memcached-81"]}},
                },
                "rollout_paused": {
                    "lvl": "INFO",
                    "msg": "consul maintenance paused step=sequential_agent_restart by={by}",
                    "vars": {"by": {"k": "ch", "v": ["incident_commander", "sre"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "service_deregister", "per_min": 0.02, "scope": "global"}, {"id": "service_register", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "agent_restart", "per_min": 1.0, "scope": "global"}, {"id": "service_deregister", "per_min": 1.5, "scope": "global"}, {"id": "service_register", "per_min": 1.5, "scope": "global"}]},
            },
        },
        "vitess_vtgate": {
            "svc": "vitess",
            "hosts": ["vtgate-1", "vtgate-2"],
            "logs": {
                "scatter_query_ok": {
                    "lvl": "INFO",
                    "msg": "vitess query ok keyspace={keyspace} query={query} scatter=true shards={shards} tablet={tablet} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "keyspace": {"k": "ch", "v": ["chan_membership_user"]},
                        "query": {"k": "ch", "v": ["gdm_membership_lookup"]},
                        "shards": {"k": "i", "v": [64, 128]},
                        "tablet": {"k": "ch", "v": ["primary"]},
                        "dur_ms": {"k": "i", "v": [50, 1500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "scatter_query_timeout": {
                    "lvl": "ERROR",
                    "msg": "vitess query timeout keyspace={keyspace} query={query} scatter=true shards={shards} timeout_ms={timeout_ms} trace_id={trace_id}",
                    "vars": {
                        "keyspace": {"k": "ch", "v": ["chan_membership_user"]},
                        "query": {"k": "ch", "v": ["gdm_membership_lookup"]},
                        "shards": {"k": "i", "v": [64, 128]},
                        "timeout_ms": {"k": "i", "v": [800, 4000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "targeted_query_ok": {
                    "lvl": "INFO",
                    "msg": "vitess query ok keyspace={keyspace} query={query} scatter=false shards={shards} tablet={tablet} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "keyspace": {"k": "ch", "v": ["chan_membership_user"]},
                        "query": {"k": "ch", "v": ["gdm_membership_lookup_missing_only"]},
                        "shards": {"k": "i", "v": [1, 4]},
                        "tablet": {"k": "ch", "v": ["replica"]},
                        "dur_ms": {"k": "i", "v": [30, 900]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "simple_query_ok": {
                    "lvl": "INFO",
                    "msg": "vitess query ok keyspace={keyspace} query={query} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "keyspace": {"k": "ch", "v": ["messages"]},
                        "query": {"k": "ch", "v": ["insert_message"]},
                        "dur_ms": {"k": "i", "v": [5, 350]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "keyspace_load": {
                    "lvl": "WARN",
                    "msg": "vitess load keyspace={keyspace} qps={qps} timeouts_per_min={timeouts_per_min} cpu_pct={cpu_pct}",
                    "vars": {"keyspace": {"k": "ch", "v": ["chan_membership_user"]}},
                    "state_vars": {
                        "n": {"qps": {"k": "i", "v": [200, 450]}, "timeouts_per_min": {"k": "i", "v": [0, 5]}, "cpu_pct": {"k": "i", "v": [25, 60]}},
                        "f": {"qps": {"k": "i", "v": [800, 1600]}, "timeouts_per_min": {"k": "i", "v": [150, 650]}, "cpu_pct": {"k": "i", "v": [80, 99]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "keyspace_load", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "keyspace_load", "per_min": 1.0, "scope": "global"}]},
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "client_boot_cache_hit",
                "rpm": 70,
                "emit": [
                    "slack_client.boot_req_send",
                    "webapp_api.boot_start",
                    "mcrouter_cache.mc_get_hit",
                    "webapp_api.boot_complete_200",
                    "edge_gateway.boot_access_200",
                    "slack_client.boot_resp_ok",
                ],
                "latency_ms": [[3, 10], [5, 15], [2, 8], [80, 220], [2, 8], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.03, "emit_per_retry": ["slack_client.boot_retry"], "backoff_ms": [[200, 600], [500, 1200]]},
                "trace": True,
            },
            {
                "id": "client_boot_cache_miss_scatter_ok",
                "rpm": 3,
                "emit": [
                    "slack_client.boot_req_send",
                    "webapp_api.boot_start",
                    "mcrouter_cache.mc_get_miss",
                    "vitess_vtgate.scatter_query_ok",
                    "webapp_api.boot_complete_200",
                    "edge_gateway.boot_access_200",
                    "slack_client.boot_resp_ok",
                ],
                "latency_ms": [[3, 10], [5, 20], [3, 10], [120, 450], [150, 600], [2, 10], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.08, "emit_per_retry": ["slack_client.boot_retry"], "backoff_ms": [[250, 800], [800, 2000]]},
                "trace": True,
            },
            {
                "id": "send_message_ok",
                "rpm": 240,
                "emit": [
                    "slack_client.msg_req_send",
                    "webapp_api.msg_start",
                    "vitess_vtgate.simple_query_ok",
                    "webapp_api.msg_complete_200",
                    "edge_gateway.msg_access_200",
                    "slack_client.msg_resp_ok",
                ],
                "latency_ms": [[2, 6], [3, 10], [6, 25], [25, 90], [2, 8], [2, 6]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "client_boot_cache_miss_scatter_timeout",
                "rpm": 100,
                "emit": [
                    "slack_client.boot_req_send",
                    "webapp_api.boot_start",
                    "mcrouter_cache.mc_get_miss",
                    "vitess_vtgate.scatter_query_timeout",
                    "webapp_api.boot_complete_503",
                    "edge_gateway.boot_access_503",
                    "slack_client.boot_resp_503",
                ],
                "latency_ms": [[3, 12], [8, 25], [4, 12], [900, 4000], [1200, 6500], [3, 12], [3, 12]],
                "retry": {"max_attempts": 4, "expected_attempts": 2.6, "emit_per_retry": ["slack_client.boot_retry"], "backoff_ms": [[500, 1500], [1500, 4000], [3000, 9000]]},
                "trace": True,
            },
            {
                "id": "client_boot_throttled_429",
                "rpm": 35,
                "emit": ["slack_client.boot_req_send", "webapp_api.boot_throttled_429", "edge_gateway.boot_access_429", "slack_client.boot_resp_429"],
                "latency_ms": [[2, 8], [5, 20], [2, 8], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.5, "emit_per_retry": ["slack_client.boot_retry"], "backoff_ms": [[2000, 6000], [5000, 15000]]},
                "trace": True,
            },
            {
                "id": "client_boot_cache_miss_targeted_replica_ok",
                "rpm": 25,
                "emit": [
                    "slack_client.boot_req_send",
                    "webapp_api.boot_start",
                    "mcrouter_cache.mc_get_miss",
                    "vitess_vtgate.targeted_query_ok",
                    "webapp_api.boot_complete_200",
                    "edge_gateway.boot_access_200",
                    "slack_client.boot_resp_ok",
                ],
                "latency_ms": [[3, 10], [6, 20], [4, 12], [80, 350], [250, 1200], [2, 10], [2, 10]],
                "retry": {"max_attempts": 3, "expected_attempts": 1.3, "emit_per_retry": ["slack_client.boot_retry"], "backoff_ms": [[800, 2000], [1500, 5000]]},
                "trace": True,
            },
            {
                "id": "send_message_degraded_ok",
                "rpm": 220,
                "emit": [
                    "slack_client.msg_req_send",
                    "webapp_api.msg_start",
                    "vitess_vtgate.simple_query_ok",
                    "webapp_api.msg_complete_200",
                    "edge_gateway.msg_access_200",
                    "slack_client.msg_resp_ok",
                ],
                "latency_ms": [[2, 8], [4, 15], [8, 60], [30, 180], [2, 10], [2, 8]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "feb22_cache_churn_scatter_query_cascade",
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {"client_boot_throttled_429": 0.0, "client_boot_cache_miss_targeted_replica_ok": 0.0, "mcrib_control.ring_push": 1.5, "mcrib_control.node_flush": 1.5},
                    "latency_multipliers": {"client_boot_cache_miss_scatter_timeout": {"p50": 1.2, "p95": 1.3}},
                    "one_shots": [{"ref": "consul.rollout_step", "count": 1, "hosts": ["consul-ops-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 24,
                    "rate_multipliers": {"consul.agent_restart": 0.0, "consul.service_deregister": 0.0, "consul.service_register": 0.0, "mcrib_control.ring_push": 0.8, "mcrib_control.node_flush": 0.6},
                    "latency_multipliers": {"client_boot_cache_miss_scatter_timeout": {"p50": 1.2, "p95": 1.3}},
                    "one_shots": [{"ref": "consul.rollout_paused", "count": 1, "hosts": ["consul-ops-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 27,
                    "rate_multipliers": {"client_boot_cache_miss_scatter_timeout": 0.35, "client_boot_throttled_429": 2.5},
                    "latency_multipliers": {"send_message_degraded_ok": {"p50": 0.95, "p95": 0.9}},
                    "one_shots": [{"ref": "webapp_api.boot_throttle_updated", "count": 1, "hosts": ["app-a"]}],
                },
                {
                    "order": 4,
                    "at_min": 31,
                    "rate_multipliers": {"client_boot_cache_miss_scatter_timeout": 0.9, "client_boot_throttled_429": 1.3},
                    "latency_multipliers": {"client_boot_cache_miss_scatter_timeout": {"p50": 1.3, "p95": 1.6}},
                    "one_shots": [{"ref": "webapp_api.boot_throttle_updated", "count": 1, "hosts": ["app-b"]}],
                },
                {
                    "order": 5,
                    "at_min": 33,
                    "rate_multipliers": {"client_boot_cache_miss_scatter_timeout": 0.4, "client_boot_throttled_429": 2.2},
                    "latency_multipliers": {"client_boot_cache_miss_scatter_timeout": {"p50": 1.15, "p95": 1.25}},
                    "one_shots": [{"ref": "webapp_api.boot_throttle_updated", "count": 1, "hosts": ["app-a"]}],
                },
                {
                    "order": 6,
                    "at_min": 36,
                    "rate_multipliers": {"client_boot_cache_miss_targeted_replica_ok": 1.4, "client_boot_cache_miss_scatter_timeout": 0.15, "client_boot_throttled_429": 2.0},
                    "latency_multipliers": {"client_boot_cache_miss_targeted_replica_ok": {"p50": 1.0, "p95": 1.1}},
                    "one_shots": [{"ref": "webapp_api.deploy_query_patch", "count": 1, "hosts": ["app-b"]}],
                },
            ]
        }
    },
}

SEED = 1337
BASE_TIME = datetime(2026, 2, 22, 0, 0, 0, tzinfo=timezone.utc)
PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

# For flows whose terminal emit entries encode success (200/ok), retries are modeled as timeouts:
# intermediate attempts emit a prefix of the chain (no completion/access/client response), followed by a retry.
SUCCESS_FLOW_TIMEOUT_CUTOFFS: Dict[str, int] = {
    # Emit up through cache result; then "stall" silently until client times out and retries.
    "client_boot_cache_hit": 2,  # send, boot_start, mc_get_hit
    "client_boot_cache_miss_scatter_ok": 2,  # send, boot_start, mc_get_miss
    "client_boot_cache_miss_targeted_replica_ok": 2,  # send, boot_start, mc_get_miss
    # For messaging, emit up through Vitess query; then timeout before app/edge/client completes.
    "send_message_ok": 2,  # send, msg_start, simple_query_ok
    "send_message_degraded_ok": 2,
}


def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def h01(key: str) -> float:
    hx = _md5_hex(f"{SEED}:{key}")
    v = int(hx[:16], 16)
    return v / float(1 << 64)


def stable_int(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    return base + (1 if h01(f"round:{key}") < frac else 0)


def fmt_ts(ms_from_base: int) -> str:
    dt = BASE_TIME + timedelta(milliseconds=int(ms_from_base))
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def schedule_evenly(start_ms: int, end_ms: int, count: int, key: str) -> List[int]:
    if count <= 0:
        return []
    duration = max(1, end_ms - start_ms)
    step = duration / float(count)
    times: List[int] = []
    for i in range(count):
        center = start_ms + (i + 0.5) * step
        jitter_bound = min(200.0, step * 0.30)
        jitter = (h01(f"jit:{key}:{i}") - 0.5) * 2.0 * jitter_bound
        t = int(round(center + jitter))
        if t < start_ms:
            t = start_ms
        if t >= end_ms:
            t = end_ms - 1
        times.append(t)
    times.sort()
    return times


def deterministic_uuid(key: str) -> str:
    hx = _md5_hex(f"{SEED}:uuid:{key}")
    return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def deterministic_hex(key: str, n: int) -> str:
    hx = _md5_hex(f"{SEED}:hex:{key}")
    if n <= 32:
        return hx[:n]
    out = hx
    while len(out) < n:
        out += _md5_hex(out)
    return out[:n]


def pick_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        r = h01(f"i:{key}")
        return lo + int(math.floor(r * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        r = h01(f"f:{key}")
        val = lo + r * (hi - lo)
        return float(f"{val:.3f}")
    if k == "ch":
        arr = list(v)
        if not arr:
            return ""
        idx = int(math.floor(h01(f"ch:{key}") * len(arr))) % len(arr)
        return arr[idx]
    if k == "hex":
        n = int(v)
        return deterministic_hex(key, n)
    if k == "uuid":
        return deterministic_uuid(key)
    if k == "str":
        return str(v)
    if k == "ip":
        return "127.0.0.1"
    return ""


def merge_domains_for_state(tpl: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    domains = dict(tpl.get("vars", {}))
    svars = tpl.get("state_vars", {}).get(state, {})
    for kk, dom in svars.items():
        domains[kk] = dom
    return domains


def sample_ms(p50: float, p95: float, key: str, lo_q: float, hi_q: float, cap: Optional[float] = None) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50, float(p95))
    ratio = p95 / p50 if p50 > 0 else 1.0
    u = lo_q + (hi_q - lo_q) * h01(f"samp:{key}")
    val = p50 * (ratio ** u)
    if cap is not None:
        val = min(val, cap)
    return max(1, int(round(val)))


def choose_host(component_id: str, trace_or_req: str) -> str:
    comp = SYSTEM["components"][component_id]
    hosts = comp.get("hosts", [])
    if not hosts:
        return ""
    if len(hosts) == 1:
        return hosts[0]
    idx = int(h01(f"host:{component_id}:{trace_or_req}") * len(hosts)) % len(hosts)
    return hosts[idx]


def ring_ver_for_minute(minute: int, state: str) -> int:
    if state == "n":
        val = 1200 + int(minute / 6)
        return max(1200, min(1215, val))
    val = 1210 + int((minute - 20) * 6)
    return max(1210, min(1350, val))


def mcrouter_hit_rate_for_minute(minute: int, state: str) -> float:
    if state == "n":
        return float(f"{0.96 + 0.02 * (h01(f"hitrate:n:{minute}") - 0.5):.3f}")
    if minute < 27:
        base = 0.33
    elif minute < 33:
        base = 0.40
    else:
        base = 0.48
    wobble = 0.10 * (h01(f"hitrate:f:{minute}") - 0.5)
    return float(f"{max(0.25, min(0.70, base + wobble)):.3f}")


def throttle_limit_for_minute(minute: int) -> int:
    if minute < 31:
        return 60
    if minute < 33:
        return 140
    return 70


def build_failure_intervals() -> List[Dict[str, Any]]:
    fphase = SCENARIO["time"]["phases"]["f"]
    start_min = int(fphase["start_min"])
    end_min = int(fphase["end_min"])
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))

    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Tuple[float, float]] = {}
    intervals: List[Dict[str, Any]] = []

    cursor = start_min
    for ev in events:
        at = int(ev["at_min"])
        if at > cursor:
            intervals.append({"start_min": cursor, "end_min": at, "rate_mult": dict(rate_mult), "lat_mult": dict(lat_mult)})
            cursor = at
        for k, v in ev.get("rate_multipliers", {}).items():
            rate_mult[k] = float(v)
        for fid, mult in ev.get("latency_multipliers", {}).items():
            lat_mult[fid] = (float(mult["p50"]), float(mult["p95"]))
    if cursor < end_min:
        intervals.append({"start_min": cursor, "end_min": end_min, "rate_mult": dict(rate_mult), "lat_mult": dict(lat_mult)})
    return intervals


def get_template(ref: str) -> Tuple[str, str, Dict[str, Any]]:
    comp_id, log_id = ref.split(".", 1)
    tpl = SYSTEM["components"][comp_id]["logs"][log_id]
    return comp_id, log_id, tpl


def render_message(tpl: Dict[str, Any], state: str, key: str, overrides: Dict[str, Any]) -> str:
    msg = tpl["msg"]
    placeholders = set(PLACEHOLDER_RE.findall(msg))
    domains = merge_domains_for_state(tpl, state)
    vals: Dict[str, Any] = {}
    for name in placeholders:
        if name in overrides:
            vals[name] = overrides[name]
            continue
        dom = domains.get(name)
        if dom is None:
            vals[name] = ""
            continue
        val = pick_from_domain(dom, f"{key}:{name}")
        if dom.get("k") == "str" and isinstance(val, str) and "{" in val:
            try:
                vals[name] = val.format_map(overrides)
            except Exception:
                vals[name] = val
        else:
            vals[name] = val
    try:
        return msg.format_map(vals)
    except Exception:
        out = msg
        for k, v in vals.items():
            out = out.replace("{" + k + "}", str(v))
        return out


def attempt_plan_counts(expected_attempts: float, max_attempts: int, n_instances: int) -> List[int]:
    if n_instances <= 0:
        return []
    e = float(expected_attempts)
    m = int(max_attempts)
    if m <= 1:
        return [1] * n_instances
    e = max(1.0, min(float(m), e))
    base = int(math.floor(e))
    frac = e - base
    out: List[int] = []
    acc = 0.0
    for _ in range(n_instances):
        acc += frac
        a = base
        if acc >= 1.0 - 1e-12 and base < m:
            a = base + 1
            acc -= 1.0
        out.append(min(m, max(1, a)))
    return out


def plan_attempt_delays(flow_id: str, lat_pairs: List[List[float]], lat_mult: Tuple[float, float], key: str) -> List[int]:
    p50m, p95m = lat_mult
    delays: List[int] = []
    for i, (p50, p95) in enumerate(lat_pairs):
        d = sample_ms(p50 * p50m, p95 * p95m, f"{key}:lat:{i}", lo_q=0.20, hi_q=0.80, cap=None)
        delays.append(d)

    # Keep very-fast 429 chains from producing client-side durations below the template's dur_ms domain (min 20ms).
    # We enforce this by adjusting the attempt timeline (not by clamping just the logged dur_ms).
    if flow_id == "client_boot_throttled_429":
        total = sum(delays[1:])
        if total < 20:
            delays[-1] += (20 - total)

    # Keep overload flow totals within the modeled dur_ms domains (client max 9000ms) while remaining consistent.
    if flow_id == "client_boot_cache_miss_scatter_timeout":
        if len(delays) > 3:
            delays[3] = min(delays[3], 4000)
            delays[3] = max(delays[3], 800)
        cap_total = 8800
        total = sum(delays[1:])
        if total > cap_total and len(delays) >= 7:
            excess = total - cap_total
            floors = {4: 150, 3: 800, 2: 1}
            for idx in (4, 3, 2):
                if excess <= 0:
                    break
                reducible = max(0, delays[idx] - floors[idx])
                dec = min(excess, reducible)
                delays[idx] -= dec
                excess -= dec

    return delays


def timeout_wait_for_flow(flow_id: str, state: str, key: str) -> int:
    # Silent waiting between last emitted server-side log and the client deciding to retry.
    # Chosen to be plausible and deterministic; not rendered directly in messages.
    if "boot" in flow_id:
        # Boots are heavier; timeouts are typically seconds.
        return sample_ms(1400, 6000, f"tout:{state}:{flow_id}:{key}", lo_q=0.35, hi_q=1.15, cap=9000)
    # Messaging timeouts are shorter in this model.
    return sample_ms(350, 2200, f"tout:{state}:{flow_id}:{key}", lo_q=0.35, hi_q=1.05, cap=5000)


def emit_row(rows: List[Tuple[int, int, str, str, str, str, str]], ts_ms: int, seq: int, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append((ts_ms, seq, level, message, trace_id, service, host))


def simulate_background(rows: List[Tuple[int, int, str, str, str, str, str]], state: str, interval: Dict[str, Any], seq0: int) -> int:
    start_ms = interval["start_min"] * 60_000
    end_ms = interval["end_min"] * 60_000
    rate_mult = interval.get("rate_mult", {}) if state == "f" else {}
    seq = seq0

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            per_min_eff = per_min * float(rate_mult.get(mult_key, 1.0))
            duration_min = (end_ms - start_ms) / 60_000.0
            expected = per_min_eff * duration_min
            if expected <= 0:
                continue

            tpl = comp["logs"][log_id]
            if scope == "global":
                cnt = stable_int(expected, f"bg:{state}:{mult_key}:{interval['start_min']}:{interval['end_min']}")
                times = schedule_evenly(start_ms, end_ms, cnt, f"bg:{state}:{mult_key}:{interval['start_min']}")
                hosts = comp.get("hosts", [])
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    overrides: Dict[str, Any] = {}
                    if comp_id == "mcrouter_cache" and log_id == "mcrouter_stats":
                        minute = t // 60_000
                        overrides["hit_rate"] = mcrouter_hit_rate_for_minute(minute, state)
                        overrides["ring_ver"] = ring_ver_for_minute(minute, state)
                    msg = render_message(tpl, state, f"bg:{mult_key}:{t}", overrides)
                    emit_row(rows, t, seq, tpl["lvl"], msg, "", comp.get("svc", "") or "", host)
                    seq += 1
            else:
                for host in comp.get("hosts", []) or [""]:
                    cnt = stable_int(expected, f"bg:{state}:{mult_key}:{host}:{interval['start_min']}:{interval['end_min']}")
                    times = schedule_evenly(start_ms, end_ms, cnt, f"bg:{state}:{mult_key}:{host}:{interval['start_min']}")
                    for t in times:
                        overrides = {}
                        if comp_id == "mcrouter_cache" and log_id == "mcrouter_stats":
                            minute = t // 60_000
                            overrides["hit_rate"] = mcrouter_hit_rate_for_minute(minute, state)
                            overrides["ring_ver"] = ring_ver_for_minute(minute, state)
                        msg = render_message(tpl, state, f"bg:{mult_key}:{host}:{t}", overrides)
                        emit_row(rows, t, seq, tpl["lvl"], msg, "", comp.get("svc", "") or "", host)
                        seq += 1

    return seq


def simulate_flow_instances(rows: List[Tuple[int, int, str, str, str, str, str]], state: str, interval: Dict[str, Any], flow: Dict[str, Any], seq0: int) -> int:
    start_ms = interval["start_min"] * 60_000
    end_ms = interval["end_min"] * 60_000
    duration_min = (end_ms - start_ms) / 60_000.0
    rate_mult = interval.get("rate_mult", {}) if state == "f" else {}
    lat_mult_map = interval.get("lat_mult", {}) if state == "f" else {}
    seq = seq0

    mult = float(rate_mult.get(flow["id"], 1.0)) if state == "f" else 1.0
    rpm_eff = float(flow["rpm"]) * mult
    expected_instances = rpm_eff * duration_min
    n_instances = stable_int(expected_instances, f"flow:{state}:{flow['id']}:{interval['start_min']}:{interval['end_min']}")
    if n_instances <= 0:
        return seq

    starts = schedule_evenly(start_ms, end_ms, n_instances, f"flow:{state}:{flow['id']}:{interval['start_min']}")
    retry = flow["retry"]

    # Compute attempts for all flows per modeled retry semantics.
    attempts_list = attempt_plan_counts(float(retry["expected_attempts"]), int(retry["max_attempts"]), n_instances)
    lat_mult = lat_mult_map.get(flow["id"], (1.0, 1.0))

    for idx, t0 in enumerate(starts):
        trace_id = deterministic_hex(f"trace:{state}:{flow['id']}:{interval['start_min']}:{idx}:{t0}", 32) if flow.get("trace") else ""
        req_id = deterministic_uuid(f"req:{state}:{flow['id']}:{interval['start_min']}:{idx}:{t0}")
        user_id = 100000 + int(h01(f"user:{req_id}") * 100000)
        workspace_id = 1000 + int(h01(f"ws:{req_id}") * 1000)
        channel_id = 1_000_000 + int(h01(f"chan:{req_id}") * 1_000_000)
        client_ver = ["desktop_4.29", "desktop_4.30", "web_2024.02"][int(h01(f"cver:{req_id}") * 3) % 3]

        minute = t0 // 60_000
        per_req_ring_ver = max(1200, min(1400, 1200 + int((minute - 0) * 4 + (h01(f"ring:{req_id}") - 0.5) * 10)))
        if state == "f":
            per_req_ring_ver = max(1210, min(1400, 1210 + int((minute - 20) * 10 + (h01(f"ringf:{req_id}") - 0.5) * 20)))

        trace_or_req = trace_id if trace_id else req_id
        comp_host: Dict[str, str] = {}
        for ref in flow["emit"]:
            comp_id, _, _ = get_template(ref)
            if comp_id not in comp_host:
                comp_host[comp_id] = choose_host(comp_id, trace_or_req)

        attempt_count = attempts_list[idx]
        prev_end_ms: Optional[int] = None

        for attempt in range(1, attempt_count + 1):
            # Bind the per-attempt timeline (used for both timestamp gaps and any dur_ms fields).
            delays_full = plan_attempt_delays(flow["id"], flow["latency_ms"], lat_mult, f"{flow['id']}:{idx}:a{attempt}")

            if attempt == 1:
                attempt_start_ms = t0
            else:
                bi = min(attempt - 2, max(0, len(retry["backoff_ms"]) - 1)) if retry["backoff_ms"] else 0
                p50, p95 = retry["backoff_ms"][bi] if retry["backoff_ms"] else (0, 0)
                backoff_ms = sample_ms(p50, p95, f"backoff:{flow['id']}:{idx}:a{attempt}", lo_q=0.30, hi_q=1.30, cap=2.5 * p95 if p95 else None)
                backoff_ms = int(max(300, min(15000, backoff_ms)))

                if prev_end_ms is None:
                    prev_end_ms = t0

                # Retry-only logs are emitted on retry attempts (2..A).
                retry_log_ts = prev_end_ms + 1
                for j, rref in enumerate(retry.get("emit_per_retry", [])):
                    rcid, _, rtpl = get_template(rref)
                    overrides = {"req_id": req_id, "next_attempt": attempt, "backoff_ms": backoff_ms}
                    if flow["id"] == "client_boot_throttled_429":
                        overrides["reason"] = "http_429"
                    elif flow["id"] == "client_boot_cache_miss_scatter_timeout":
                        overrides["reason"] = "http_503"
                    else:
                        overrides["reason"] = "timeout"
                    rmsg = render_message(rtpl, state, f"retry:{flow['id']}:{idx}:a{attempt}:{j}", overrides)
                    emit_row(rows, retry_log_ts + j, seq, rtpl["lvl"], rmsg, trace_id, SYSTEM["components"][rcid]["svc"], comp_host.get(rcid, ""))
                    seq += 1

                # Make the first log of the new attempt occur at retry_log_ts + backoff_ms.
                attempt_start_ms = retry_log_ts + backoff_ms - delays_full[0]

            # If this is a retrying success-terminal flow, model intermediate attempts as timeouts by truncating
            # the emission chain before completion/access/client-response logs.
            emit_refs = list(flow["emit"])
            delays = list(delays_full)
            is_final_attempt = (attempt == attempt_count)
            if (not is_final_attempt) and (flow["id"] in SUCCESS_FLOW_TIMEOUT_CUTOFFS) and (attempt_count > 1):
                cutoff = max(0, min(int(SUCCESS_FLOW_TIMEOUT_CUTOFFS[flow["id"]]), len(emit_refs) - 1))
                emit_refs = emit_refs[: cutoff + 1]
                delays = delays[: cutoff + 1]

            attempt_first_log_ms = attempt_start_ms + delays[0]
            ts = attempt_first_log_ms
            anchor: Dict[str, int] = {}

            for li, ref in enumerate(emit_refs):
                if li > 0:
                    ts += delays[li]
                comp_id, log_id, tpl = get_template(ref)
                overrides: Dict[str, Any] = {
                    "req_id": req_id,
                    "trace_id": trace_id,
                    "user_id": user_id,
                    "workspace_id": workspace_id,
                    "client_ver": client_ver,
                    "channel_id": channel_id,
                    "ring_ver": per_req_ring_ver,
                }

                if comp_id == "webapp_api" and log_id == "boot_complete_503":
                    overrides["err"] = "vitess_timeout"
                if comp_id == "webapp_api" and log_id == "boot_throttled_429":
                    overrides["limit_rpm"] = throttle_limit_for_minute(minute)

                if ref in ("slack_client.boot_req_send", "slack_client.msg_req_send"):
                    anchor["client_send"] = ts
                if ref in ("webapp_api.boot_start", "webapp_api.msg_start"):
                    anchor["webapp_start"] = ts

                # Vitess per-log observed timing fields should match the emit gap for that log.
                if comp_id == "vitess_vtgate" and log_id in ("scatter_query_ok", "targeted_query_ok", "simple_query_ok"):
                    overrides["dur_ms"] = int(max(1, delays[li]))
                if comp_id == "vitess_vtgate" and log_id == "scatter_query_timeout":
                    overrides["timeout_ms"] = int(max(800, min(4000, delays[li])))

                # Webapp/edge/client dur_ms are derived from the same timeline.
                if comp_id == "webapp_api" and log_id in ("boot_complete_200", "boot_complete_503", "msg_complete_200"):
                    if "webapp_start" in anchor:
                        overrides["dur_ms"] = max(1, ts - anchor["webapp_start"])

                if comp_id == "edge_gateway" and log_id in ("boot_access_200", "boot_access_503", "boot_access_429", "msg_access_200"):
                    if "client_send" in anchor:
                        overrides["dur_ms"] = max(1, ts - anchor["client_send"])

                if comp_id == "slack_client" and log_id in ("boot_resp_ok", "boot_resp_503", "boot_resp_429", "msg_resp_ok"):
                    if "client_send" in anchor:
                        overrides["dur_ms"] = max(1, ts - anchor["client_send"])

                msg = render_message(tpl, state, f"flow:{flow['id']}:{idx}:a{attempt}:l{li}", overrides)
                emit_row(rows, ts, seq, tpl["lvl"], msg, trace_id, SYSTEM["components"][comp_id]["svc"], comp_host.get(comp_id, ""))
                seq += 1

            # If we truncated a success flow attempt, insert a silent "client wait then timeout" before the retry.
            if (not is_final_attempt) and (flow["id"] in SUCCESS_FLOW_TIMEOUT_CUTOFFS) and (attempt_count > 1):
                ts += timeout_wait_for_flow(flow["id"], state, f"{idx}:a{attempt}")
            prev_end_ms = ts

    return seq


def emit_one_shots(rows: List[Tuple[int, int, str, str, str, str, str]], seq0: int) -> int:
    seq = seq0
    for ev in sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0))):
        at_ms = int(ev["at_min"]) * 60_000
        for os in ev.get("one_shots", []):
            ref = os["ref"]
            cnt = int(os["count"])
            hosts = os.get("hosts", [])
            comp_id, _, tpl = get_template(ref)
            comp = SYSTEM["components"][comp_id]
            for i in range(cnt):
                t = at_ms + int(round((h01(f"oneshot:{ref}:{ev['at_min']}:{i}") - 0.5) * 800.0))
                if t < at_ms:
                    t = at_ms
                host = (hosts[i % len(hosts)] if hosts else (comp.get("hosts", [""])[0] if comp.get("hosts") else ""))
                overrides: Dict[str, Any] = {}
                if ref == "webapp_api.boot_throttle_updated":
                    minute = int(ev["at_min"])
                    overrides["limit_rpm"] = throttle_limit_for_minute(minute)
                    overrides["actor"] = ["incident_commander", "oncall_webapp", "sre"][int(h01(f"actor:{minute}") * 3) % 3]
                if ref == "webapp_api.deploy_query_patch":
                    overrides["change"] = "gdm_query_targeted_misses_read_replicas"
                    overrides["rollout_id"] = deterministic_hex(f"rollout:{ev['at_min']}", 8)
                if ref == "consul.rollout_step":
                    overrides["step"] = "sequential_agent_restart"
                    overrides["percent"] = 10 + (int(h01(f"percent:{ev['at_min']}") * 5) * 10)
                    overrides["percent"] = max(10, min(50, int(overrides["percent"])))
                msg = render_message(tpl, "f", f"oneshot:{ref}:{ev['at_min']}:{i}", overrides)
                emit_row(rows, t, seq, tpl["lvl"], msg, "", comp.get("svc", "") or "", host)
                seq += 1
    return seq


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    failure_intervals = build_failure_intervals()
    rows: List[Tuple[int, int, str, str, str, str, str]] = []
    seq = 0

    n_phase = SCENARIO["time"]["phases"]["n"]
    normal_interval = {"start_min": int(n_phase["start_min"]), "end_min": int(n_phase["end_min"])}

    seq = simulate_background(rows, "n", normal_interval, seq)
    for flow in SYSTEM["flows"]["n"]:
        seq = simulate_flow_instances(rows, "n", normal_interval, flow, seq)

    for interval in failure_intervals:
        seq = simulate_background(rows, "f", interval, seq)
        for flow in SYSTEM["flows"]["f"]:
            seq = simulate_flow_instances(rows, "f", interval, flow, seq)

    seq = emit_one_shots(rows, seq)

    rows.sort(key=lambda r: (r[0], r[1]))
    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r[0]) for r in rows],
            "level": [r[2] for r in rows],
            "message": [r[3] for r in rows],
            "trace_id": [r[4] for r in rows],
            "service": [r[5] for r in rows],
            "host": [r[6] for r in rows],
        },
        columns=["timestamp", "level", "message", "trace_id", "service", "host"],
    )
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
