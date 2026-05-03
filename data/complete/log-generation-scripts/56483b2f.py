import math
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Deterministic seeding
# ----------------------------
GLOBAL_SEED = "wikipedia-redirect-loop-v3-seed"
_SEED_INT = int(hashlib.md5(GLOBAL_SEED.encode("utf-8")).hexdigest()[:8], 16)
random.seed(_SEED_INT)
np.random.seed(_SEED_INT)

# ----------------------------
# Embedded executable model (normalized)
# ----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "wikipedia_portal_redirect_loop"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "cdn_edge": {
            "svc": "edge-cache",
            "hosts": ["edge-eqiad-1", "edge-codfw-1", "edge-drmrs-1"],
            "logs": {
                "edge_access_200_hit_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=200 cache=HIT req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_200_hit_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=200 cache=HIT req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_200_hit_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=200 cache=HIT req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_200_miss_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=200 cache=MISS req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [25, 350]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_200_miss_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=200 cache=MISS req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [25, 350]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_200_miss_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=200 cache=MISS req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [25, 350]},
                        "bytes": {"k": "i", "v": [5000, 22000]},
                    },
                },
                "edge_access_301_hit_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [3, 80]}},
                },
                "edge_access_301_hit_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [3, 80]}},
                },
                "edge_access_301_hit_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [3, 80]}},
                },
                "edge_access_301_miss_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [20, 320]}},
                },
                "edge_access_301_miss_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [20, 320]}},
                },
                "edge_access_301_miss_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [20, 320]}},
                },
                "cache_miss_eqiad": {
                    "lvl": "INFO",
                    "msg": "cache miss url=https://www.wikipedia.org/ dc=eqiad origin=portal-eqiad-1 req_id={req_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}},
                },
                "cache_miss_codfw": {
                    "lvl": "INFO",
                    "msg": "cache miss url=https://www.wikipedia.org/ dc=codfw origin=portal-codfw-1 req_id={req_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}},
                },
                "cache_miss_drmrs": {
                    "lvl": "INFO",
                    "msg": "cache miss url=https://www.wikipedia.org/ dc=drmrs origin=portal-drmrs-1 req_id={req_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}},
                },
                "edge_metric": {
                    "lvl": "INFO",
                    "msg": "edge metrics s200={s200} s301={s301} hit_ratio={hit_ratio}",
                    "vars": {
                        "s200": {"k": "i", "v": [0, 3000]},
                        "s301": {"k": "i", "v": [0, 3000]},
                        "hit_ratio": {"k": "f", "v": [0.0, 1.0]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_metric", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "edge_metric", "per_min": 1.0, "scope": "global"}]},
            },
        },
        "portal_apache": {
            "svc": "portal-web",
            "hosts": ["portal-eqiad-1", "portal-codfw-1", "portal-drmrs-1"],
            "logs": {
                "apache_access_200_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=200 req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [8, 220]},
                        "bytes": {"k": "i", "v": [4500, 18000]},
                    },
                },
                "apache_access_200_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=200 req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [8, 220]},
                        "bytes": {"k": "i", "v": [4500, 18000]},
                    },
                },
                "apache_access_200_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=200 req_id={req_id} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [8, 220]},
                        "bytes": {"k": "i", "v": [4500, 18000]},
                    },
                },
                "apache_access_301_self_eqiad": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=eqiad status=301 location=https://www.wikipedia.org/ req_id={req_id} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 160]}},
                },
                "apache_access_301_self_codfw": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=codfw status=301 location=https://www.wikipedia.org/ req_id={req_id} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 160]}},
                },
                "apache_access_301_self_drmrs": {
                    "lvl": "INFO",
                    "msg": "GET https://www.wikipedia.org/ dc=drmrs status=301 location=https://www.wikipedia.org/ req_id={req_id} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 160]}},
                },
                "config_snapshot": {
                    "lvl": "INFO",
                    "msg": "loaded vhost config rev={rev}",
                    "vars": {},
                    "state_vars": {
                        "n": {"rev": {"k": "i", "v": [1080300, 1080340]}},
                        "f": {"rev": {"k": "i", "v": [1080340, 1080357]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "config_snapshot", "per_min": 0.05}]},
                "f": {"emit": [{"id": "config_snapshot", "per_min": 0.05}]},
            },
        },
        "deploy_manager": {
            "svc": "config-deploy",
            "hosts": ["deploy01"],
            "logs": {
                "deploy_start_portal_revert": {
                    "lvl": "INFO",
                    "msg": "deploy start change=1080357-revert target=portal-web",
                    "vars": {},
                },
                "deploy_blocked_mediawiki_config": {
                    "lvl": "ERROR",
                    "msg": "deploy blocked change=1080357-revert reason=\"config validation errors in mediawiki-config\"",
                    "vars": {},
                },
                "deploy_heartbeat": {
                    "lvl": "INFO",
                    "msg": "deploy-manager heartbeat queue_depth={queue_depth}",
                    "vars": {"queue_depth": {"k": "i", "v": [0, 20]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "deploy_heartbeat", "per_min": 0.5, "scope": "global"}]},
                "f": {"emit": [{"id": "deploy_heartbeat", "per_min": 0.5, "scope": "global"}]},
            },
        },
        "cache_purger": {
            "svc": "cache-purge",
            "hosts": ["purge01"],
            "logs": {
                "purge_start_codfw": {"lvl": "WARN", "msg": "purge requested url=https://www.wikipedia.org/ scope=dc dc=codfw", "vars": {}},
                "purge_done_codfw": {
                    "lvl": "INFO",
                    "msg": "purge completed url=https://www.wikipedia.org/ dc=codfw removed={removed}",
                    "vars": {"removed": {"k": "i", "v": [500, 200000]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "ops_shell": {
            "svc": None,
            "hosts": ["bastion01"],
            "logs": {
                "curl_run_eqiad": {
                    "lvl": "INFO",
                    "msg": "curl https://www.wikipedia.org/ dc=eqiad follow_redirects=--location req_id={req_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "ops_chat": {
            "svc": None,
            "hosts": ["ops01"],
            "logs": {
                "user_report": {
                    "lvl": "WARN",
                    "msg": "user report channel={channel} symptom=\"{symptom}\" url=https://www.wikipedia.org/",
                    "vars": {
                        "channel": {"k": "ch", "v": ["social_media", "phabricator", "email"]},
                        "symptom": {"k": "ch", "v": ["Too Many Redirects", "portal fails to load", "Wikipedia is down"]},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "status_page": {
            "svc": "status",
            "hosts": ["status01"],
            "logs": {
                "status_update_investigating": {
                    "lvl": "INFO",
                    "msg": "status page update state=investigating summary=\"Investigating excessive redirects on www.wikipedia.org\"",
                    "vars": {},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": [
            {
                "id": "portal_eqiad_hit_200_n",
                "rpm": 700,
                "emit": ["cdn_edge.edge_access_200_hit_eqiad"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_eqiad_miss_200_n",
                "rpm": 35,
                "emit": ["cdn_edge.cache_miss_eqiad", "portal_apache.apache_access_200_eqiad", "cdn_edge.edge_access_200_miss_eqiad"],
                "latency_ms": [[2, 8], [25, 140], [40, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_hit_200_n",
                "rpm": 350,
                "emit": ["cdn_edge.edge_access_200_hit_codfw"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_miss_200_n",
                "rpm": 18,
                "emit": ["cdn_edge.cache_miss_codfw", "portal_apache.apache_access_200_codfw", "cdn_edge.edge_access_200_miss_codfw"],
                "latency_ms": [[2, 8], [25, 140], [40, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_drmrs_hit_200_n",
                "rpm": 180,
                "emit": ["cdn_edge.edge_access_200_hit_drmrs"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_drmrs_miss_200_n",
                "rpm": 9,
                "emit": ["cdn_edge.cache_miss_drmrs", "portal_apache.apache_access_200_drmrs", "cdn_edge.edge_access_200_miss_drmrs"],
                "latency_ms": [[2, 8], [25, 140], [40, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "ops_curl_origin_n",
                "rpm": 0.0,
                "emit": ["ops_shell.curl_run_eqiad", "portal_apache.apache_access_200_eqiad"],
                "latency_ms": [[1, 5], [15, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "portal_eqiad_hit_200_f",
                "rpm": 700,
                "emit": ["cdn_edge.edge_access_200_hit_eqiad"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_eqiad_hit_301_f",
                "rpm": 30,
                "emit": ["cdn_edge.edge_access_301_hit_eqiad"],
                "latency_ms": [[6, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_eqiad_miss_301_f",
                "rpm": 10,
                "emit": ["cdn_edge.cache_miss_eqiad", "portal_apache.apache_access_301_self_eqiad", "cdn_edge.edge_access_301_miss_eqiad"],
                "latency_ms": [[2, 8], [15, 110], [35, 260]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_hit_200_f",
                "rpm": 350,
                "emit": ["cdn_edge.edge_access_200_hit_codfw"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_hit_301_f",
                "rpm": 10,
                "emit": ["cdn_edge.edge_access_301_hit_codfw"],
                "latency_ms": [[6, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_miss_301_f",
                "rpm": 5,
                "emit": ["cdn_edge.cache_miss_codfw", "portal_apache.apache_access_301_self_codfw", "cdn_edge.edge_access_301_miss_codfw"],
                "latency_ms": [[2, 8], [15, 110], [35, 260]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_codfw_miss_200_f",
                "rpm": 18,
                "emit": ["cdn_edge.cache_miss_codfw", "portal_apache.apache_access_200_codfw", "cdn_edge.edge_access_200_miss_codfw"],
                "latency_ms": [[2, 8], [25, 140], [40, 320]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_drmrs_hit_200_f",
                "rpm": 200,
                "emit": ["cdn_edge.edge_access_200_hit_drmrs"],
                "latency_ms": [[10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_drmrs_hit_301_f",
                "rpm": 20,
                "emit": ["cdn_edge.edge_access_301_hit_drmrs"],
                "latency_ms": [[6, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "portal_drmrs_miss_301_f",
                "rpm": 5,
                "emit": ["cdn_edge.cache_miss_drmrs", "portal_apache.apache_access_301_self_drmrs", "cdn_edge.edge_access_301_miss_drmrs"],
                "latency_ms": [[2, 8], [15, 110], [35, 260]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "ops_curl_origin_f",
                "rpm": 1.0,
                "emit": ["ops_shell.curl_run_eqiad", "portal_apache.apache_access_301_self_eqiad"],
                "latency_ms": [[1, 6], [10, 90]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

# FIX (verifier S1): remove trailing commas from cached 301 HIT and MISS edge access logs.
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_hit_eqiad"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=eqiad status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_hit_codfw"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=codfw status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_hit_drmrs"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=drmrs status=301 cache=HIT req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_miss_eqiad"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=eqiad status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_miss_codfw"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=codfw status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)
SYSTEM["components"]["cdn_edge"]["logs"]["edge_access_301_miss_drmrs"]["msg"] = (
    "GET https://www.wikipedia.org/ dc=drmrs status=301 cache=MISS req_id={req_id} dur_ms={dur_ms} location=https://www.wikipedia.org/"
)

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "incident_2025_02_28_wikipedia_portal_redirect"},
    "time": {"total_minutes": 44, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 44}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "portal_codfw_hit_301_f": 0.0,
                        "portal_codfw_miss_301_f": 0.0,
                        "portal_codfw_miss_200_f": 0.0,
                        "portal_drmrs_hit_301_f": 0.0,
                        "portal_drmrs_miss_301_f": 0.0,
                        "ops_curl_origin_f": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 26,
                    "rate_multipliers": {
                        "portal_eqiad_hit_200_f": 0.6,
                        "portal_eqiad_hit_301_f": 6.0,
                        "portal_eqiad_miss_301_f": 1.5,
                        "portal_drmrs_hit_200_f": 0.8,
                        "portal_drmrs_hit_301_f": 3.0,
                        "portal_drmrs_miss_301_f": 1.5,
                        "portal_codfw_hit_301_f": 1.0,
                        "portal_codfw_miss_301_f": 1.0,
                        "portal_codfw_miss_200_f": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 34,
                    "rate_multipliers": {
                        "ops_curl_origin_f": 1.0,
                        "portal_codfw_hit_301_f": 0.0,
                        "portal_codfw_miss_301_f": 0.0,
                        "portal_codfw_miss_200_f": 3.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_chat.user_report", "count": 4, "hosts": ["ops01"]},
                        {"ref": "status_page.status_update_investigating", "count": 1, "hosts": ["status01"]},
                        {"ref": "deploy_manager.deploy_start_portal_revert", "count": 1, "hosts": ["deploy01"]},
                        {"ref": "cache_purger.purge_start_codfw", "count": 1, "hosts": ["purge01"]},
                        {"ref": "cache_purger.purge_done_codfw", "count": 1, "hosts": ["purge01"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 40,
                    "rate_multipliers": {
                        "portal_eqiad_hit_200_f": 0.3,
                        "portal_eqiad_hit_301_f": 10.0,
                        "portal_eqiad_miss_301_f": 2.0,
                        "portal_drmrs_hit_301_f": 4.0,
                        "portal_drmrs_miss_301_f": 2.0,
                        "portal_codfw_miss_200_f": 1.2,
                        "portal_codfw_hit_200_f": 1.1,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "deploy_manager.deploy_blocked_mediawiki_config", "count": 1, "hosts": ["deploy01"]}],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------
def _md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def h64(s: str) -> int:
    d = _md5_bytes(s)
    return int.from_bytes(d[:8], "little", signed=False)


def u01(s: str) -> float:
    return (h64(s) % (1 << 53)) / float(1 << 53)


def choose_int(lo: int, hi: int, key: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    r = u01(key)
    return lo + int(r * (hi - lo + 1))


def choose_float(lo: float, hi: float, key: str) -> float:
    r = u01(key)
    return lo + (hi - lo) * r


def choose_choice(options: List[Any], key: str) -> Any:
    if not options:
        return None
    idx = int(u01(key) * len(options))
    if idx >= len(options):
        idx = len(options) - 1
    return options[idx]


def gen_hex(n: int, key: str) -> str:
    out = hashlib.md5((key + "|hex|" + str(n)).encode("utf-8")).hexdigest()
    if n <= len(out):
        return out[:n]
    while len(out) < n:
        out += hashlib.md5((out + key).encode("utf-8")).hexdigest()
    return out[:n]


def clamp(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def sample_lognormal_ms(p50: float, p95: float, key: str, multiplier: float = 1.0) -> int:
    p50 = max(0.2, p50 * multiplier)
    p95 = max(p50 * 1.001, p95 * multiplier)
    z95 = 1.6448536269514722
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / z95

    u1 = max(1e-12, u01(key + ":u1"))
    u2 = u01(key + ":u2")
    z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)

    x = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    if x > soft_cap:
        x = soft_cap * (1.0 + 0.03 * u01(key + ":cap"))
    ms = int(round(x))
    return max(1, ms)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def infer_dc_from_id(s: str) -> Optional[str]:
    for dc in ("eqiad", "codfw", "drmrs"):
        if dc in s:
            return dc
    return None


def choose_host_for_log(component_id: str, log_id: str, preferred_hosts: Optional[List[str]] = None) -> str:
    comp = SYSTEM["components"][component_id]
    hosts = preferred_hosts if preferred_hosts is not None else comp.get("hosts", [])
    if not hosts:
        return ""
    dc = infer_dc_from_id(log_id)
    if dc:
        for h in hosts:
            if dc in h:
                return h
    return hosts[0]


def get_service(component_id: str) -> str:
    svc = SYSTEM["components"][component_id].get("svc", None)
    return "" if svc is None else str(svc)


def log_template(ref: str) -> Tuple[str, str, Dict[str, Any]]:
    comp_id, log_id = ref.split(".", 1)
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    return comp_id, log_id, tmpl


def merged_vars_for_template(tmpl: Dict[str, Any], state: str) -> Dict[str, Any]:
    v = dict(tmpl.get("vars", {}))
    sv = tmpl.get("state_vars", {})
    if sv and state in sv:
        v.update(sv[state])
    return v


class RemainderAllocator:
    def __init__(self) -> None:
        self.rem: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        r = self.rem.get(key, 0.0) + expected
        n = int(math.floor(r + 1e-12))
        self.rem[key] = r - n
        return n


def even_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start] * n
    ts: List[datetime] = []
    base_spacing = total_s / n
    jitter_amp = min(0.25, base_spacing * 0.35)
    for i in range(n):
        frac = (i + 0.5) / n
        base = start + timedelta(seconds=frac * total_s)
        j = (u01(f"{key}:j:{i}") - 0.5) * 2.0 * jitter_amp
        t = base + timedelta(seconds=j)
        if t < start:
            t = start + timedelta(milliseconds=1)
        if t >= end:
            t = end - timedelta(milliseconds=1)
        ts.append(t)
    return ts


# ----------------------------
# Scenario control derivation
# ----------------------------
def build_failure_segments() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    current_rate: Dict[str, float] = {}
    current_lat: Dict[str, float] = {}

    segments: List[Dict[str, Any]] = []

    idx = 0
    while idx < len(events) and events[idx]["at_min"] == f_start:
        current_rate.update(events[idx].get("rate_multipliers", {}))
        current_lat.update(events[idx].get("latency_multipliers", {}))
        idx += 1

    seg_start = f_start
    while idx < len(events):
        ev = events[idx]
        ev_t = ev["at_min"]
        if ev_t > seg_start:
            segments.append(
                {
                    "start_min": seg_start,
                    "end_min": min(ev_t, f_end),
                    "rate_multipliers": dict(current_rate),
                    "latency_multipliers": dict(current_lat),
                }
            )
            seg_start = ev_t
            if seg_start >= f_end:
                break
        while idx < len(events) and events[idx]["at_min"] == ev_t:
            current_rate.update(events[idx].get("rate_multipliers", {}))
            current_lat.update(events[idx].get("latency_multipliers", {}))
            idx += 1

    if seg_start < f_end:
        segments.append(
            {
                "start_min": seg_start,
                "end_min": f_end,
                "rate_multipliers": dict(current_rate),
                "latency_multipliers": dict(current_lat),
            }
        )
    return segments


FAILURE_SEGMENTS = build_failure_segments()


def active_failure_rate_multiplier(minute: int, flow_id: str) -> float:
    for seg in FAILURE_SEGMENTS:
        if seg["start_min"] <= minute < seg["end_min"]:
            return float(seg["rate_multipliers"].get(flow_id, 1.0))
    return 1.0


# ----------------------------
# Metrics synthesis (edge_metric)
# ----------------------------
def classify_edge_access(flow: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    last_edge = None
    for ref in flow["emit"][::-1]:
        comp_id, log_id = ref.split(".", 1)
        if comp_id == "cdn_edge" and log_id.startswith("edge_access_"):
            last_edge = log_id
            break
    if not last_edge:
        return None, None
    status = 200 if "edge_access_200_" in last_edge else 301 if "edge_access_301_" in last_edge else None
    cache = "HIT" if "_hit_" in last_edge else "MISS" if "_miss_" in last_edge else None
    return status, cache


def compute_edge_metric_vars(minute: int) -> Dict[str, Any]:
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    state = "n" if minute < n_end else "f"
    flows = SYSTEM["flows"]["n"] if state == "n" else SYSTEM["flows"]["f"]

    s200 = 0.0
    s301 = 0.0
    hits = 0.0
    total = 0.0

    for f in flows:
        rpm = float(f["rpm"])
        if state == "f":
            rpm *= active_failure_rate_multiplier(minute, f["id"])
        if rpm <= 0:
            continue
        status, cache = classify_edge_access(f)
        if status is None:
            continue
        if status == 200:
            s200 += rpm
        elif status == 301:
            s301 += rpm
        if cache == "HIT":
            hits += rpm
        if cache in ("HIT", "MISS"):
            total += rpm

    wig200 = 0.98 + 0.04 * u01(f"edge_metric:{minute}:s200")
    wig301 = 0.98 + 0.04 * u01(f"edge_metric:{minute}:s301")
    s200_i = int(round(s200 * wig200))
    s301_i = int(round(s301 * wig301))

    hit_ratio = (hits / total) if total > 0 else 1.0
    hit_ratio = max(0.0, min(1.0, hit_ratio))
    hit_ratio_s = f"{hit_ratio:.3f}"

    s200_i = clamp(s200_i, 0, 3000)
    s301_i = clamp(s301_i, 0, 3000)

    return {"s200": s200_i, "s301": s301_i, "hit_ratio": hit_ratio_s}


# ----------------------------
# Flow simulation (no retries in this model)
# ----------------------------
def fit_delays_to_dur_domains(flow: Dict[str, Any], delays: List[int], state: str) -> List[int]:
    emits = flow["emit"]
    last_idx = len(emits) - 1

    for i, ref in enumerate(emits):
        _, _, tmpl = log_template(ref)
        vars_ = merged_vars_for_template(tmpl, state)
        if "dur_ms" in vars_ and i != last_idx:
            lo, hi = vars_["dur_ms"]["v"]
            delays[i] = clamp(delays[i], int(lo), int(hi))

    _, _, tmpl_last = log_template(emits[last_idx])
    vars_last = merged_vars_for_template(tmpl_last, state)
    if "dur_ms" in vars_last:
        lo_t, hi_t = vars_last["dur_ms"]["v"]
        lo_t = int(lo_t)
        hi_t = int(hi_t)
        total = sum(delays)
        target = clamp(total, lo_t, hi_t)
        delta = target - total
        if delta != 0:
            delays[last_idx] = max(1, delays[last_idx] + delta)

        total2 = sum(delays)
        if total2 > hi_t:
            excess = total2 - hi_t
            for j in range(last_idx, -1, -1):
                if excess <= 0:
                    break
                reducible = max(0, delays[j] - 1)
                d = min(reducible, excess)
                delays[j] -= d
                excess -= d
        elif total2 < lo_t:
            delays[last_idx] += (lo_t - total2)

    return delays


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {
            "timestamp_dt": ts,
            "level": level,
            "message": message,
            "trace_id": trace_id,
            "service": service,
            "host": host,
        }
    )


def simulate_flow_instance(rows: List[Dict[str, Any]], flow: Dict[str, Any], state: str, start_ts: datetime, instance_key: str) -> None:
    emits = flow["emit"]
    latency_hints = flow["latency_ms"]
    latency_mult = 1.0  # scenario has no latency multipliers

    req_id = gen_hex(16, instance_key + ":req_id")
    response_bytes_edge = None
    response_bytes_origin = None

    delays: List[int] = []
    for i, (p50, p95) in enumerate(latency_hints):
        ms = sample_lognormal_ms(float(p50), float(p95), f"{instance_key}:lat:{i}", multiplier=latency_mult)
        delays.append(ms)

    delays = fit_delays_to_dur_domains(flow, delays, state)
    total_ms = sum(delays)

    host_cache: Dict[str, str] = {}
    ts = start_ts

    for i, ref in enumerate(emits):
        comp_id, log_id = ref.split(".", 1)
        tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
        lvl = tmpl["lvl"]
        vars_ = merged_vars_for_template(tmpl, state)

        ts = ts + timedelta(milliseconds=delays[i])

        if comp_id not in host_cache:
            host_cache[comp_id] = choose_host_for_log(comp_id, log_id)
        host = host_cache[comp_id]
        service = get_service(comp_id)
        trace_id = ""  # tracing disabled in model

        vals: Dict[str, Any] = {}
        for k, dom in vars_.items():
            kind = dom["k"]
            if k == "req_id":
                vals[k] = req_id
            elif k == "dur_ms":
                vals[k] = total_ms if i == len(emits) - 1 else delays[i]
            elif k == "bytes":
                lo, hi = dom["v"]
                if comp_id == "cdn_edge":
                    if response_bytes_edge is None:
                        response_bytes_edge = choose_int(int(lo), int(hi), instance_key + ":bytes:edge")
                    vals[k] = response_bytes_edge
                else:
                    if response_bytes_origin is None:
                        response_bytes_origin = choose_int(int(lo), int(hi), instance_key + ":bytes:origin")
                    vals[k] = response_bytes_origin
            elif kind == "i":
                lo, hi = dom["v"]
                vals[k] = choose_int(int(lo), int(hi), f"{instance_key}:{ref}:{k}")
            elif kind == "f":
                lo, hi = dom["v"]
                vals[k] = choose_float(float(lo), float(hi), f"{instance_key}:{ref}:{k}")
            elif kind == "ch":
                vals[k] = choose_choice(list(dom["v"]), f"{instance_key}:{ref}:{k}")
            elif kind == "hex":
                vals[k] = gen_hex(int(dom["v"]), f"{instance_key}:{ref}:{k}")
            else:
                vals[k] = str(choose_choice(["x"], f"{instance_key}:{ref}:{k}"))

        msg = tmpl["msg"].format(**vals) if vals else tmpl["msg"]
        emit_row(rows, ts, lvl, msg, trace_id, service, host)


# ----------------------------
# Background + one-shot simulation
# ----------------------------
def emit_background_logs(rows: List[Dict[str, Any]], base: datetime) -> None:
    alloc = RemainderAllocator()
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]

    # cdn_edge.edge_metric (global, 1/min) for all minutes, with state-appropriate vars
    for minute in range(SCENARIO["time"]["total_minutes"]):
        state = "n" if minute < n_end else "f"
        comp_id = "cdn_edge"
        log_id = "edge_metric"
        tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
        jitter_ms = int(200 * u01(f"edge_metric:{minute}:jitter"))
        ts = base + timedelta(minutes=minute, seconds=55, milliseconds=jitter_ms)

        vars_ = merged_vars_for_template(tmpl, state)
        metric_vals = compute_edge_metric_vars(minute)
        s200 = clamp(int(metric_vals["s200"]), vars_["s200"]["v"][0], vars_["s200"]["v"][1])
        s301 = clamp(int(metric_vals["s301"]), vars_["s301"]["v"][0], vars_["s301"]["v"][1])
        hit_ratio = metric_vals["hit_ratio"]
        msg = tmpl["msg"].format(s200=s200, s301=s301, hit_ratio=hit_ratio)

        host = choose_host_for_log(comp_id, log_id)
        service = get_service(comp_id)
        emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)

    # deploy_manager.deploy_heartbeat (global, 0.5/min) per state interval
    for state, (smin, emin) in [("n", (n_start, n_end)), ("f", (f_start, f_end))]:
        dur = emin - smin
        per_min = 0.5
        expected = per_min * dur
        count = alloc.alloc(f"bg:deploy_manager:deploy_heartbeat:{state}", expected)
        start_ts = base + timedelta(minutes=smin)
        end_ts = base + timedelta(minutes=emin)
        times = even_times(start_ts, end_ts, count, f"bg:deploy_heartbeat:{state}")
        for idx, ts in enumerate(times):
            comp_id = "deploy_manager"
            log_id = "deploy_heartbeat"
            tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
            vdom = tmpl["vars"]["queue_depth"]["v"]
            queue_depth = choose_int(int(vdom[0]), int(vdom[1]), f"deploy_heartbeat:{state}:{idx}")
            msg = tmpl["msg"].format(queue_depth=queue_depth)
            host = "deploy01"
            service = get_service(comp_id)
            emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)

    # portal_apache.config_snapshot (per_host by default, 0.05/min/host) per state interval
    for state, (smin, emin) in [("n", (n_start, n_end)), ("f", (f_start, f_end))]:
        dur = emin - smin
        per_min = 0.05
        start_ts = base + timedelta(minutes=smin)
        end_ts = base + timedelta(minutes=emin)
        tmpl = SYSTEM["components"]["portal_apache"]["logs"]["config_snapshot"]
        rev_dom = tmpl["state_vars"][state]["rev"]["v"]
        for host in SYSTEM["components"]["portal_apache"]["hosts"]:
            expected = per_min * dur
            count = alloc.alloc(f"bg:portal_apache:config_snapshot:{state}:{host}", expected)
            times = even_times(start_ts, end_ts, count, f"bg:config_snapshot:{state}:{host}")
            for idx, ts in enumerate(times):
                rev = choose_int(int(rev_dom[0]), int(rev_dom[1]), f"config_snapshot:{state}:{host}:{idx}")
                msg = tmpl["msg"].format(rev=rev)
                service = get_service("portal_apache")
                emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)


def emit_one_shots(rows: List[Dict[str, Any]], base: datetime) -> None:
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    for ev in events:
        at_min = ev["at_min"]
        shots = ev.get("one_shots", []) or []
        base_ts = base + timedelta(minutes=at_min)
        for shot in shots:
            ref = shot["ref"]
            comp_id, log_id = ref.split(".", 1)
            tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
            count = int(shot["count"])
            hosts = shot.get("hosts", None)
            for ci in range(count):
                jitter_s = 1.0 + 48.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t")
                if at_min == 34:
                    if ref == "ops_chat.user_report":
                        jitter_s = 0.5 + 8.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")
                    elif ref == "deploy_manager.deploy_start_portal_revert":
                        jitter_s = 10.0 + 5.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")
                    elif ref == "cache_purger.purge_start_codfw":
                        jitter_s = 15.0 + 3.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")
                    elif ref == "status_page.status_update_investigating":
                        jitter_s = 25.0 + 4.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")
                    elif ref == "cache_purger.purge_done_codfw":
                        jitter_s = 40.0 + 5.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")
                if at_min == 40 and ref == "deploy_manager.deploy_blocked_mediawiki_config":
                    jitter_s = 12.0 + 6.0 * u01(f"oneshot:{at_min}:{ref}:{ci}:t2")

                ts = base_ts + timedelta(seconds=float(jitter_s))
                service = get_service(comp_id)
                host = hosts[0] if hosts else choose_host_for_log(comp_id, log_id)

                vals: Dict[str, Any] = {}
                vars_ = merged_vars_for_template(tmpl, "f")
                for k, dom in vars_.items():
                    kind = dom["k"]
                    if kind == "i":
                        lo, hi = dom["v"]
                        vals[k] = choose_int(int(lo), int(hi), f"oneshot:{at_min}:{ref}:{ci}:{k}")
                    elif kind == "f":
                        lo, hi = dom["v"]
                        vals[k] = choose_float(float(lo), float(hi), f"oneshot:{at_min}:{ref}:{ci}:{k}")
                    elif kind == "ch":
                        vals[k] = choose_choice(list(dom["v"]), f"oneshot:{at_min}:{ref}:{ci}:{k}")
                    elif kind == "hex":
                        vals[k] = gen_hex(int(dom["v"]), f"oneshot:{at_min}:{ref}:{ci}:{k}")
                    else:
                        vals[k] = str(choose_choice(["x"], f"oneshot:{at_min}:{ref}:{ci}:{k}"))

                msg = tmpl["msg"].format(**vals) if vals else tmpl["msg"]
                emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)


# ----------------------------
# Flow scheduling and execution
# ----------------------------
def emit_flows(rows: List[Dict[str, Any]], base: datetime) -> None:
    alloc = RemainderAllocator()
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    # Normal phase
    start_ts = base + timedelta(minutes=n_start)
    end_ts = base + timedelta(minutes=n_end)
    dur = n_end - n_start
    for flow in SYSTEM["flows"]["n"]:
        expected = float(flow["rpm"]) * dur
        count = alloc.alloc(f"flow:n:{flow['id']}", expected)
        times = even_times(start_ts, end_ts, count, f"flowstarts:n:{flow['id']}")
        for i, t0 in enumerate(times):
            simulate_flow_instance(rows, flow, "n", t0, f"n:{flow['id']}:{i}")

    # Failure phase in piecewise segments with persistent rate multipliers
    for seg in FAILURE_SEGMENTS:
        smin = seg["start_min"]
        emin = seg["end_min"]
        seg_start = base + timedelta(minutes=smin)
        seg_end = base + timedelta(minutes=emin)
        dur_m = emin - smin
        rate_mults = seg["rate_multipliers"]

        for flow in SYSTEM["flows"]["f"]:
            mult = float(rate_mults.get(flow["id"], 1.0))
            eff_rpm = float(flow["rpm"]) * mult
            if eff_rpm <= 0:
                continue
            expected = eff_rpm * dur_m
            count = alloc.alloc(f"flow:f:{flow['id']}:{smin}-{emin}", expected)
            times = even_times(seg_start, seg_end, count, f"flowstarts:f:{flow['id']}:{smin}-{emin}")
            for i, t0 in enumerate(times):
                simulate_flow_instance(rows, flow, "f", t0, f"f:{flow['id']}:{smin}-{emin}:{i}")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    base = datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)

    rows: List[Dict[str, Any]] = []
    emit_background_logs(rows, base)
    emit_flows(rows, base)
    emit_one_shots(rows, base)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)

    out = pd.DataFrame(
        {
            "timestamp": [fmt_ts(t) for t in df["timestamp_dt"].tolist()],
            "level": df["level"].astype(str),
            "message": df["message"].astype(str),
            "trace_id": df["trace_id"].astype(str),
            "service": df["service"].astype(str),
            "host": df["host"].astype(str),
        }
    )
    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
