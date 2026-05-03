import math
import re
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "edge_html_rewrite_leak_incident"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_nginx": {
            "svc": "edge-http",
            "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-sfo-01", "edge-sfo-02"],
            "logs": {
                "worker_stats": {
                    "lvl": "INFO",
                    "msg": "worker stats conns={conns} req_s={req_s} rss_mb={rss_mb}",
                    "vars": {
                        "conns": {"k": "i", "v": [20, 800]},
                        "req_s": {"k": "f", "v": [0.2, 8.0]},
                        "rss_mb": {"k": "i", "v": [150, 1200]},
                    },
                },
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start {method} {host}{uri} ua={ua} ray={ray}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/static/app.js", "/cdn-cgi/probe/malformed.html"]},
                        "ua": {"k": "ch", "v": ["chrome", "googlebot", "curl", "cf-synthetic"]},
                        "ray": {"k": "hex", "v": 16},
                    },
                },
                "html_parse_warn": {
                    "lvl": "WARN",
                    "msg": "html parse warning: malformed tag near EOF tag={tag} rule={rule}",
                    "vars": {"tag": {"k": "ch", "v": ["script", "img"]}, "rule": {"k": "ch", "v": ["consume_attr", "consume_tag"]}},
                },
                "html_buffer_overread_email": {
                    "lvl": "ERROR",
                    "msg": "html filter overread detected chain=dual_chain feature=email_obfuscation extra_bytes={extra_bytes} out_bytes={out_bytes}",
                    "vars": {"extra_bytes": {"k": "i", "v": [1, 1024]}, "out_bytes": {"k": "i", "v": [400, 8000]}},
                },
                "html_buffer_overread_ahr": {
                    "lvl": "ERROR",
                    "msg": "html filter overread detected chain=dual_chain feature=auto_https_rewrites extra_bytes={extra_bytes} out_bytes={out_bytes}",
                    "vars": {"extra_bytes": {"k": "i", "v": [1, 1024]}, "out_bytes": {"k": "i", "v": [400, 8000]}},
                },
                "html_buffer_overread_sse": {
                    "lvl": "ERROR",
                    "msg": "html filter overread detected chain=dual_chain feature=server_side_excludes extra_bytes={extra_bytes} out_bytes={out_bytes}",
                    "vars": {"extra_bytes": {"k": "i", "v": [1, 1024]}, "out_bytes": {"k": "i", "v": [400, 8000]}},
                },
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "resp {status} {method} {host}{uri} bytes={bytes} dur_ms={dur_ms} cache={cache}",
                    "vars": {
                        "status": {"k": "ch", "v": [200, 304]},
                        "method": {"k": "ch", "v": ["GET"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/static/app.js", "/cdn-cgi/probe/malformed.html"]},
                        "bytes": {"k": "i", "v": [200, 20000]},
                        "dur_ms": {"k": "i", "v": [5, 1200]},
                        "cache": {"k": "ch", "v": ["DYNAMIC", "HIT", "MISS", "BYPASS"]},
                    },
                },
                "access_leak_email": {
                    "lvl": "INFO",
                    "msg": "resp 200 {method} {host}{uri} bytes={bytes} dur_ms={dur_ms} html_chain=dual_chain features=email_obfuscation leak_bytes={leak_bytes}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/cdn-cgi/probe/malformed.html"]},
                        "bytes": {"k": "i", "v": [400, 30000]},
                        "dur_ms": {"k": "i", "v": [5, 2000]},
                        "leak_bytes": {"k": "i", "v": [1, 1024]},
                    },
                },
                "access_leak_ahr": {
                    "lvl": "INFO",
                    "msg": "resp 200 {method} {host}{uri} bytes={bytes} dur_ms={dur_ms} html_chain=dual_chain features=auto_https_rewrites leak_bytes={leak_bytes}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/cdn-cgi/probe/malformed.html"]},
                        "bytes": {"k": "i", "v": [400, 30000]},
                        "dur_ms": {"k": "i", "v": [5, 2200]},
                        "leak_bytes": {"k": "i", "v": [1, 1024]},
                    },
                },
                "access_leak_sse": {
                    "lvl": "INFO",
                    "msg": "resp 200 {method} {host}{uri} bytes={bytes} dur_ms={dur_ms} html_chain=dual_chain features=server_side_excludes leak_bytes={leak_bytes}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/cdn-cgi/probe/malformed.html"]},
                        "bytes": {"k": "i", "v": [400, 30000]},
                        "dur_ms": {"k": "i", "v": [5, 2400]},
                        "leak_bytes": {"k": "i", "v": [1, 1024]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_stats", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "origin_server": {
            "svc": "origin",
            "hosts": ["origin-01", "origin-02"],
            "logs": {
                "origin_resp": {
                    "lvl": "INFO",
                    "msg": "origin resp {host}{uri} status={status} bytes={bytes} ttfb_ms={ttfb_ms}",
                    "vars": {
                        "host": {"k": "ch", "v": ["example.com", "shop.example", "blog.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/index.html", "/product/123", "/login", "/static/app.js", "/cdn-cgi/probe/malformed.html"]},
                        "status": {"k": "ch", "v": [200, 404, 500]},
                        "bytes": {"k": "i", "v": [200, 20000]},
                        "ttfb_ms": {"k": "i", "v": [10, 600]},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "feature_flag_service": {
            "svc": "control-plane",
            "hosts": ["control-01", "control-02"],
            "logs": {
                "cp_heartbeat": {
                    "lvl": "INFO",
                    "msg": "control-plane heartbeat queued_updates={queued_updates} healthy_edges={healthy_edges}",
                    "vars": {"queued_updates": {"k": "i", "v": [0, 50]}, "healthy_edges": {"k": "i", "v": [1000, 20000]}},
                },
                "deploy_email_migration": {
                    "lvl": "INFO",
                    "msg": "edge deploy change=email_obfuscation_dual_chain version=2017.02.13.1 rollout_pct={rollout_pct} change_id={change_id}",
                    "vars": {"rollout_pct": {"k": "i", "v": [1, 100]}, "change_id": {"k": "str", "v": "CHG-#######"}},
                },
                "deploy_add_sse_kill": {
                    "lvl": "INFO",
                    "msg": "edge deploy change=add_kill_switch_server_side_excludes version=2017.02.18.1 rollout_pct={rollout_pct} change_id={change_id}",
                    "vars": {"rollout_pct": {"k": "i", "v": [1, 100]}, "change_id": {"k": "str", "v": "CHG-#######"}},
                },
                "kill_set_email_obfuscation": {
                    "lvl": "INFO",
                    "msg": "global kill set feature=email_obfuscation state=disabled actor={actor} change_id={change_id}",
                    "vars": {"actor": {"k": "ch", "v": ["sre", "seceng", "release_bot"]}, "change_id": {"k": "str", "v": "CHG-#######"}},
                },
                "kill_propagated_email_obfuscation": {
                    "lvl": "INFO",
                    "msg": "global kill propagated feature=email_obfuscation state=disabled edges={edges} latency_s={latency_s}",
                    "vars": {"edges": {"k": "i", "v": [1000, 20000]}, "latency_s": {"k": "f", "v": [0.5, 10.0]}},
                },
                "kill_set_auto_https_rewrites": {
                    "lvl": "INFO",
                    "msg": "global kill set feature=auto_https_rewrites state=disabled actor={actor} change_id={change_id}",
                    "vars": {"actor": {"k": "ch", "v": ["sre", "seceng", "release_bot"]}, "change_id": {"k": "str", "v": "CHG-#######"}},
                },
                "kill_propagated_auto_https_rewrites": {
                    "lvl": "INFO",
                    "msg": "global kill propagated feature=auto_https_rewrites state=disabled edges={edges} latency_s={latency_s}",
                    "vars": {"edges": {"k": "i", "v": [1000, 20000]}, "latency_s": {"k": "f", "v": [0.5, 10.0]}},
                },
                "kill_set_server_side_excludes": {
                    "lvl": "INFO",
                    "msg": "global kill set feature=server_side_excludes state=disabled actor={actor} change_id={change_id}",
                    "vars": {"actor": {"k": "ch", "v": ["sre", "seceng", "release_bot"]}, "change_id": {"k": "str", "v": "CHG-#######"}},
                },
                "kill_propagated_server_side_excludes": {
                    "lvl": "INFO",
                    "msg": "global kill propagated feature=server_side_excludes state=disabled edges={edges} latency_s={latency_s}",
                    "vars": {"edges": {"k": "i", "v": [1000, 20000]}, "latency_s": {"k": "f", "v": [0.5, 10.0]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cp_heartbeat", "per_min": 0.2, "scope": "global"}]},
                "f": {"emit": [{"id": "cp_heartbeat", "per_min": 0.2, "scope": "global"}]},
            },
        },
        "synthetic_monitor": {
            "svc": "synthetic",
            "hosts": ["synthetic-01"],
            "logs": {
                "probe_start_detect": {
                    "lvl": "INFO",
                    "msg": "probe start {host}{uri} expect=detect_leak run_id={run_id}",
                    "vars": {"host": {"k": "ch", "v": ["blog.example"]}, "uri": {"k": "ch", "v": ["/cdn-cgi/probe/malformed.html"]}, "run_id": {"k": "hex", "v": 8}},
                },
                "probe_start_clean": {
                    "lvl": "INFO",
                    "msg": "probe start {host}{uri} expect=clean_html run_id={run_id}",
                    "vars": {"host": {"k": "ch", "v": ["blog.example"]}, "uri": {"k": "ch", "v": ["/cdn-cgi/probe/malformed.html"]}, "run_id": {"k": "hex", "v": 8}},
                },
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe ok {host}{uri} status={status} dur_ms={dur_ms}",
                    "vars": {"host": {"k": "ch", "v": ["blog.example"]}, "uri": {"k": "ch", "v": ["/cdn-cgi/probe/malformed.html"]}, "status": {"k": "ch", "v": [200]}, "dur_ms": {"k": "i", "v": [10, 2000]}},
                },
                "probe_leak_detected": {
                    "lvl": "ERROR",
                    "msg": "probe detected leaked bytes bytes={bytes} leak_markers={leak_markers} sample_len={sample_len}",
                    "vars": {"bytes": {"k": "i", "v": [400, 30000]}, "leak_markers": {"k": "ch", "v": ["cookie", "auth_token", "post_body", "binary_garbage"]}, "sample_len": {"k": "i", "v": [32, 256]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "secops": {
            "svc": "secops",
            "hosts": ["secops-01"],
            "logs": {
                "report_received": {
                    "lvl": "WARN",
                    "msg": "external vuln report received reporter={reporter} channel={channel} ref={ref}",
                    "vars": {"reporter": {"k": "ch", "v": ["project_zero"]}, "channel": {"k": "ch", "v": ["twitter", "email"]}, "ref": {"k": "str", "v": "P0-####"}},
                },
                "incident_declared": {
                    "lvl": "INFO",
                    "msg": "incident declared id={inc_id} sev={sev} owner={owner}",
                    "vars": {"inc_id": {"k": "str", "v": "INC-####"}, "sev": {"k": "ch", "v": ["SEV1"]}, "owner": {"k": "ch", "v": ["seceng", "sre"]}},
                },
                "cache_purge_requested": {
                    "lvl": "INFO",
                    "msg": "search cache purge requested engine={engine} urls={urls}",
                    "vars": {"engine": {"k": "ch", "v": ["google", "bing", "yahoo"]}, "urls": {"k": "i", "v": [1, 1000]}},
                },
                "incident_status": {
                    "lvl": "INFO",
                    "msg": "incident status id={inc_id} phase={phase} open_actions={open_actions}",
                    "vars": {"inc_id": {"k": "str", "v": "INC-####"}, "phase": {"k": "ch", "v": ["triage", "mitigation", "cache_purge"]}, "open_actions": {"k": "i", "v": [0, 20]}},
                },
            },
            "beh": {
                "n": {"emit": []},
                "f": {"emit": [{"id": "incident_status", "per_min": 0.2, "scope": "global"}]},
            },
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "n_client_get_html_ok",
                    "rpm": 400.0,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.access_ok"],
                    "latency_ms": [[1, 3], [35, 200], [40, 260]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "n_client_get_html_malformed",
                    "rpm": 0.25,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.html_parse_warn", "edge_nginx.access_ok"],
                    "latency_ms": [[1, 3], [35, 220], [36, 230], [42, 280]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "f_client_get_html_ok",
                    "rpm": 400.0,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.access_ok"],
                    "latency_ms": [[1, 3], [35, 240], [45, 320]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_client_get_html_malformed",
                    "rpm": 0.25,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.html_parse_warn", "edge_nginx.access_ok"],
                    "latency_ms": [[1, 3], [35, 260], [36, 270], [48, 340]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_client_get_html_email_leak_rare",
                    "rpm": 0.00015,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.html_buffer_overread_email", "edge_nginx.access_leak_email"],
                    "latency_ms": [[1, 3], [35, 300], [36, 320], [60, 600]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_client_get_html_ahr_leak_rare",
                    "rpm": 0.00002,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.html_buffer_overread_ahr", "edge_nginx.access_leak_ahr"],
                    "latency_ms": [[1, 3], [35, 320], [36, 340], [65, 700]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_client_get_html_sse_leak_rare",
                    "rpm": 0.000002,
                    "emit": ["edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.html_buffer_overread_sse", "edge_nginx.access_leak_sse"],
                    "latency_ms": [[1, 3], [35, 350], [36, 380], [70, 850]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_probe_detect_leak",
                    "rpm": 2.0,
                    "emit": [
                        "synthetic_monitor.probe_start_detect",
                        "edge_nginx.req_start",
                        "origin_server.origin_resp",
                        "edge_nginx.html_buffer_overread_email",
                        "edge_nginx.access_leak_email",
                        "synthetic_monitor.probe_leak_detected",
                    ],
                    "latency_ms": [[1, 2], [2, 5], [35, 250], [36, 280], [45, 380], [46, 450]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "f_probe_confirm_clean",
                    "rpm": 1.0,
                    "emit": ["synthetic_monitor.probe_start_clean", "edge_nginx.req_start", "origin_server.origin_resp", "edge_nginx.access_ok", "synthetic_monitor.probe_ok"],
                    "latency_ms": [[1, 2], [2, 5], [35, 250], [45, 350], [46, 420]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "cloudbleed_style_edge_html_leak",
        "time": {"total_minutes": 56, "phases": {"n": {"start_min": 0, "end_min": 28}, "f": {"start_min": 28, "end_min": 56}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 28,
                        "rate_multipliers": {"f_probe_detect_leak": 0.0, "f_probe_confirm_clean": 0.0, "secops.incident_status": 0.0},
                        "latency_multipliers": {"f_client_get_html_ok": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [{"ref": "feature_flag_service.deploy_email_migration", "count": 1, "hosts": ["control-01"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 32,
                        "rate_multipliers": {"f_probe_detect_leak": 1.0, "secops.incident_status": 1.0},
                        "latency_multipliers": {"f_probe_detect_leak": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "secops.report_received", "count": 1, "hosts": ["secops-01"]},
                            {"ref": "secops.incident_declared", "count": 1, "hosts": ["secops-01"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 33,
                        "rate_multipliers": {"f_client_get_html_email_leak_rare": 0.0, "f_probe_detect_leak": 0.0, "f_probe_confirm_clean": 1.0},
                        "latency_multipliers": {"f_probe_confirm_clean": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "feature_flag_service.kill_set_email_obfuscation", "count": 1, "hosts": ["control-01"]},
                            {"ref": "feature_flag_service.kill_propagated_email_obfuscation", "count": 1, "hosts": ["control-02"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 44,
                        "rate_multipliers": {"f_client_get_html_ahr_leak_rare": 0.0},
                        "latency_multipliers": {"f_client_get_html_ok": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "feature_flag_service.kill_set_auto_https_rewrites", "count": 1, "hosts": ["control-01"]},
                            {"ref": "feature_flag_service.kill_propagated_auto_https_rewrites", "count": 1, "hosts": ["control-02"]},
                        ],
                    },
                    {
                        "order": 5,
                        "at_min": 52,
                        "rate_multipliers": {"f_client_get_html_sse_leak_rare": 0.0},
                        "latency_multipliers": {"f_probe_confirm_clean": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "feature_flag_service.deploy_add_sse_kill", "count": 1, "hosts": ["control-01"]},
                            {"ref": "feature_flag_service.kill_set_server_side_excludes", "count": 1, "hosts": ["control-01"]},
                            {"ref": "feature_flag_service.kill_propagated_server_side_excludes", "count": 1, "hosts": ["control-02"]},
                            {"ref": "secops.cache_purge_requested", "count": 2, "hosts": ["secops-01"]},
                        ],
                    },
                ]
            }
        },
    }
}

# ------------------------ Deterministic helpers ------------------------

def _md5_int(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest(), "big", signed=False)

def u01(key: str) -> float:
    return (_md5_int(key) % (10**12)) / float(10**12)

def choice(values: List[Any], key: str) -> Any:
    if not values:
        return ""
    idx = _md5_int(key) % len(values)
    return values[idx]

def sample_int(a: int, b: int, key: str) -> int:
    if b < a:
        a, b = b, a
    if a == b:
        return a
    r = u01(key)
    return a + int(math.floor(r * (b - a + 1)))

def sample_float(a: float, b: float, key: str) -> float:
    if b < a:
        a, b = b, a
    if abs(b - a) < 1e-12:
        return float(a)
    r = u01(key)
    return a + (b - a) * r

def sample_hex(n: int, key: str) -> str:
    out = ""
    i = 0
    while len(out) < n:
        d = hashlib.md5(f"{key}:{i}".encode("utf-8")).hexdigest()
        out += d
        i += 1
    return out[:n].lower()

def sample_uuid_like(key: str) -> str:
    h = sample_hex(32, key)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def sample_str_pattern(template: str, key: str) -> str:
    if "#" not in template:
        return template
    parts = list(template)
    needed = parts.count("#")
    digits = str(_md5_int(key) % (10**needed)).zfill(needed)
    di = 0
    for i, ch in enumerate(parts):
        if ch == "#":
            parts[i] = digits[di]
            di += 1
    return "".join(parts)

def allocate_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if u01(f"alloc:{key}") < frac:
        n += 1
    return n

def skewed_ms(p50: float, p95: float, key: str) -> int:
    p50 = max(0.1, float(p50))
    p95 = max(p50, float(p95))
    r = u01(f"lat:{key}")
    t = r * r
    ms = p50 + (p95 - p50) * t
    jit = (u01(f"lat_jit:{key}") - 0.5) * min(2.0, 0.05 * ms)
    ms = max(0.0, ms + jit)
    return int(round(ms))

def clamp_dt(dt: datetime, lo: datetime, hi: datetime) -> datetime:
    if dt < lo:
        return lo
    if dt >= hi:
        return hi - timedelta(milliseconds=1)
    return dt

def iso_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"

_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")

def render_log_message(comp_id: str, log_id: str, bound: Dict[str, Any], key_prefix: str) -> Tuple[str, str]:
    log = SYSTEM["components"][comp_id]["logs"][log_id]
    msg_tmpl = log["msg"]
    vars_spec = log.get("vars", {})
    values: Dict[str, Any] = {}
    for var_name, spec in vars_spec.items():
        if var_name in bound:
            v = bound[var_name]
        else:
            k = spec["k"]
            dom = spec["v"]
            vkey = f"{key_prefix}:{comp_id}.{log_id}:{var_name}"
            if k == "i":
                v = sample_int(int(dom[0]), int(dom[1]), vkey)
            elif k == "f":
                v = sample_float(float(dom[0]), float(dom[1]), vkey)
            elif k == "ch":
                v = choice(list(dom), vkey)
            elif k == "uuid":
                v = sample_uuid_like(vkey)
            elif k == "hex":
                v = sample_hex(int(dom), vkey)
            elif k == "ip":
                v = "127.0.0.1"
            elif k == "str":
                v = sample_str_pattern(str(dom), vkey)
            else:
                v = str(dom)
        if isinstance(v, float):
            # keep stable, short-ish float representation
            v2 = f"{v:.2f}"
            v = v2.rstrip("0").rstrip(".")
        values[var_name] = v
    for m in _PLACEHOLDER_RE.findall(msg_tmpl):
        if m not in values:
            values[m] = bound.get(m, "")
    return log["lvl"], msg_tmpl.format(**values)

def comp_ident(comp_id: str, chosen_host: str) -> Tuple[str, str]:
    comp = SYSTEM["components"][comp_id]
    svc = comp.get("svc") or ""
    host = chosen_host or ""
    return svc, host

# ------------------------ Control timeline ------------------------

def build_failure_intervals() -> List[Dict[str, Any]]:
    scen = SCENARIO["scenario"]
    f_start = scen["time"]["phases"]["f"]["start_min"]
    f_end = scen["time"]["phases"]["f"]["end_min"]
    events = list(scen["phases"]["f"]["events"])
    events = sorted(events, key=lambda e: (e["at_min"], e["order"]))

    boundaries = sorted(set([f_start] + [e["at_min"] for e in events] + [f_end]))
    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        t0 = boundaries[i]
        for ev in [e for e in events if e["at_min"] == t0]:
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for k, vv in ev.get("latency_multipliers", {}).items():
                lat_mult[k] = {"p50": float(vv.get("p50", 1.0)), "p95": float(vv.get("p95", 1.0))}
        t1 = boundaries[i + 1]
        if t1 <= t0:
            continue
        intervals.append(
            {
                "start_min": t0,
                "end_min": t1,
                "rate_mult": dict(rate_mult),
                "lat_mult": dict(lat_mult),
                "events_at_start": [e for e in events if e["at_min"] == t0],
            }
        )
    return intervals

FAILURE_INTERVALS = build_failure_intervals()

# ------------------------ Scheduling ------------------------

BASE_TIME = datetime(2017, 2, 18, 0, 0, 0, tzinfo=timezone.utc)

def minutes_to_dt(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(minute))

def schedule_times(interval_start: datetime, interval_end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = max(0.001, (interval_end - interval_start).total_seconds())
    step = dur_s / count
    jitter_max = min(0.2, step * 0.3)
    out = []
    for i in range(count):
        base = interval_start + timedelta(seconds=(i + 0.5) * step)
        jit = (u01(f"jit:{key}:{i}") - 0.5) * 2.0 * jitter_max
        dt = base + timedelta(seconds=jit)
        dt = clamp_dt(dt, interval_start, interval_end)
        out.append(dt)
    return out

# ------------------------ Flow simulation ------------------------

EDGE_HOSTS_HTML = ["example.com", "shop.example", "blog.example", "api.example"]
EDGE_URIS_OK = ["/", "/index.html", "/product/123", "/login", "/static/app.js"]
EDGE_URIS_PROBE = ["/cdn-cgi/probe/malformed.html"]
EDGE_URIS_LEAK = ["/", "/index.html", "/product/123", "/login", "/cdn-cgi/probe/malformed.html"]

def choose_component_host(comp_id: str, key: str) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts") or []
    if not hosts:
        return ""
    return choice(hosts, f"host:{comp_id}:{key}")

def flow_latency_multiplier(flow_id: str, interval_lat_mult: Dict[str, Dict[str, float]]) -> Tuple[float, float]:
    mm = interval_lat_mult.get(flow_id)
    if not mm:
        return 1.0, 1.0
    return float(mm.get("p50", 1.0)), float(mm.get("p95", 1.0))

def emit_row(rows: List[Dict[str, Any]], ts: datetime, comp_id: str, log_id: str, msg_bound: Dict[str, Any], trace_id: str, key_prefix: str, chosen_host: str) -> None:
    lvl, msg = render_log_message(comp_id, log_id, msg_bound, key_prefix)
    svc, host = comp_ident(comp_id, chosen_host)
    rows.append({"timestamp_dt": ts, "level": lvl, "message": msg, "trace_id": trace_id, "service": svc, "host": host})

def _clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))

def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    state: str,
    flow: Dict[str, Any],
    start_time: datetime,
    instance_key: str,
    lat_mult_p50: float,
    lat_mult_p95: float,
    incident_ctx: Dict[str, Any],
) -> None:
    emit_refs = flow["emit"]
    lat_pairs = flow["latency_ms"]
    assert len(emit_refs) == len(lat_pairs)

    # Host stickiness per component within a flow instance
    components_in_flow: List[str] = []
    for ref in emit_refs:
        comp_id, _ = ref.split(".", 1)
        if comp_id not in components_in_flow:
            components_in_flow.append(comp_id)
    host_map = {cid: choose_component_host(cid, f"{instance_key}:{cid}") for cid in components_in_flow}

    trace_id = ""  # tracing off per model

    is_probe_detect = flow["id"] == "f_probe_detect_leak"
    is_probe_clean = flow["id"] == "f_probe_confirm_clean"
    is_probe = is_probe_detect or is_probe_clean
    is_malformed = "malformed" in flow["id"]
    has_overread_log = any(".html_buffer_overread_" in ref for ref in emit_refs)

    # Request identity
    if is_probe:
        req_host = "blog.example"
        req_uri = "/cdn-cgi/probe/malformed.html"
        ua = "cf-synthetic"
    else:
        req_host = choice(EDGE_HOSTS_HTML, f"req_host:{instance_key}")
        if "leak" in flow["id"]:
            req_uri = choice(EDGE_URIS_LEAK, f"req_uri:{instance_key}")
        elif is_malformed:
            req_uri = choice(EDGE_URIS_OK + EDGE_URIS_PROBE, f"req_uri:{instance_key}")
        else:
            req_uri = choice(EDGE_URIS_OK + EDGE_URIS_PROBE, f"req_uri:{instance_key}")
        ua = choice(["chrome", "googlebot", "curl"], f"ua:{instance_key}")

    method = "GET"
    ray = sample_hex(16, f"ray:{instance_key}")

    # Upstream + response sizing.
    # For overread flows/logs, keep out_bytes within modeled domain [400, 8000] by construction.
    origin_status = 200
    if has_overread_log:
        origin_bytes = sample_int(400, 7000, f"obytes_overread:{instance_key}")
        leak_bytes = sample_int(1, 1024, f"leak:{instance_key}")
        leak_bytes = _clamp_int(leak_bytes, 1, max(1, 8000 - origin_bytes))
        resp_bytes = origin_bytes + leak_bytes
        resp_bytes = _clamp_int(resp_bytes, 400, 8000)
        # If clamped by resp_bytes ceiling, ensure internal consistency (small adjustment)
        leak_bytes = _clamp_int(resp_bytes - origin_bytes, 1, 1024)
    else:
        origin_bytes = sample_int(500, 15000, f"obytes:{instance_key}")
        leak_bytes = 0
        resp_bytes = origin_bytes

    run_id = sample_hex(8, f"run:{instance_key}") if is_probe else ""

    common = {
        "method": method,
        "host": req_host,
        "uri": req_uri,
        "ua": ua,
        "ray": ray,
        "bytes": resp_bytes,
        "status": 200,
    }

    # IMPORTANT REPAIR: interpret each latency_ms entry as an absolute offset from flow start,
    # not an incremental per-step delay; enforce monotonic timestamps.
    offsets_ms: List[int] = []
    prev = -1
    for j, (p50, p95) in enumerate(lat_pairs):
        off = skewed_ms(p50 * lat_mult_p50, p95 * lat_mult_p95, f"{instance_key}:off{j}")
        if off <= prev:
            off = prev + 1
        offsets_ms.append(off)
        prev = off

    req_start_ts: Optional[datetime] = None
    probe_start_ts: Optional[datetime] = None

    for j, ref in enumerate(emit_refs):
        comp_id, log_id = ref.split(".", 1)
        t = start_time + timedelta(milliseconds=offsets_ms[j])

        bound = dict(common)

        if comp_id == "origin_server" and log_id == "origin_resp":
            if req_start_ts is not None:
                ttfb = int(round((t - req_start_ts).total_seconds() * 1000.0))
            else:
                ttfb = 10
            bound.update({"status": origin_status, "bytes": origin_bytes, "ttfb_ms": _clamp_int(ttfb, 10, 600)})

        elif comp_id == "edge_nginx" and log_id == "req_start":
            req_start_ts = t
            bound.update({"method": method, "host": req_host, "uri": req_uri, "ua": ua, "ray": ray})

        elif comp_id == "edge_nginx" and log_id == "access_ok":
            if req_start_ts is not None:
                dur = int(round((t - req_start_ts).total_seconds() * 1000.0))
            else:
                dur = sample_int(5, 1200, f"dur:{instance_key}")
            cache = choice(["DYNAMIC", "HIT", "MISS", "BYPASS"], f"cache:{instance_key}")
            # Keep within template domain [5, 1200]
            bound.update({"status": 200, "bytes": origin_bytes, "dur_ms": _clamp_int(dur, 5, 1200), "cache": cache})

        elif comp_id == "edge_nginx" and log_id in ("access_leak_email", "access_leak_ahr", "access_leak_sse"):
            if req_start_ts is not None:
                dur = int(round((t - req_start_ts).total_seconds() * 1000.0))
            else:
                dur = sample_int(5, 2400, f"dur:{instance_key}")
            dur_max = 2400
            if log_id == "access_leak_email":
                dur_max = 2000
            elif log_id == "access_leak_ahr":
                dur_max = 2200
            bound.update({"bytes": resp_bytes, "dur_ms": _clamp_int(dur, 5, dur_max), "leak_bytes": leak_bytes})

        elif comp_id == "edge_nginx" and log_id.startswith("html_buffer_overread_"):
            # Keep out_bytes within modeled domain [400, 8000] and consistent with response sizing above.
            bound.update({"extra_bytes": _clamp_int(leak_bytes if leak_bytes > 0 else 1, 1, 1024), "out_bytes": _clamp_int(resp_bytes, 400, 8000)})

        elif comp_id == "synthetic_monitor" and log_id in ("probe_start_detect", "probe_start_clean"):
            probe_start_ts = t
            bound.update({"host": "blog.example", "uri": "/cdn-cgi/probe/malformed.html", "run_id": run_id})

        elif comp_id == "synthetic_monitor" and log_id == "probe_ok":
            if probe_start_ts is not None:
                dur = int(round((t - probe_start_ts).total_seconds() * 1000.0))
            else:
                dur = sample_int(10, 2000, f"p_dur:{instance_key}")
            bound.update({"host": "blog.example", "uri": "/cdn-cgi/probe/malformed.html", "status": 200, "dur_ms": _clamp_int(dur, 10, 2000)})

        elif comp_id == "synthetic_monitor" and log_id == "probe_leak_detected":
            bound.update(
                {
                    "bytes": resp_bytes,
                    "leak_markers": choice(["cookie", "auth_token", "post_body", "binary_garbage"], f"marker:{instance_key}"),
                    "sample_len": sample_int(32, 256, f"slen:{instance_key}"),
                }
            )

        if incident_ctx:
            bound.update(incident_ctx)

        emit_row(rows, t, comp_id, log_id, bound, trace_id, f"flow:{flow['id']}:{instance_key}", host_map.get(comp_id, ""))

# ------------------------ Background emissions ------------------------

def simulate_background(rows: List[Dict[str, Any]], state: str, interval_start: datetime, interval_end: datetime, rate_mult: Dict[str, float], incident_ctx: Dict[str, Any]) -> None:
    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for emit in beh.get("emit", []):
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            source_key = f"{comp_id}.{log_id}"
            mult = float(rate_mult.get(source_key, 1.0)) if state == "f" else 1.0
            eff = per_min * mult
            if eff <= 0:
                continue
            dur_min = (interval_end - interval_start).total_seconds() / 60.0
            if scope == "global":
                n = allocate_count(eff * dur_min, f"bg:{state}:{source_key}:{iso_ms(interval_start)}")
                times = schedule_times(interval_start, interval_end, n, f"bg:{state}:{source_key}")
                chosen = choose_component_host(comp_id, f"bg:{state}:{source_key}")
                for i, ts in enumerate(times):
                    bound: Dict[str, Any] = {}
                    if comp_id == "edge_nginx" and log_id == "worker_stats":
                        minute_index = int((ts - BASE_TIME).total_seconds() // 60)
                        req_s = 0.8 + 1.7 * u01(f"reqs:{comp_id}:{chosen}:{minute_index}")
                        bound.update(
                            {
                                "conns": sample_int(20, 800, f"conns:{comp_id}:{chosen}:{minute_index}"),
                                "req_s": float(req_s),
                                "rss_mb": sample_int(150, 1200, f"rss:{comp_id}:{chosen}:{minute_index}"),
                            }
                        )
                    if comp_id == "secops" and log_id == "incident_status":
                        minute = (ts - BASE_TIME).total_seconds() / 60.0
                        if minute < 33:
                            phase = "triage"
                            open_actions = 15
                        elif minute < 52:
                            phase = "mitigation"
                            open_actions = 8
                        else:
                            phase = "cache_purge"
                            open_actions = 5
                        bound.update({"phase": phase, "open_actions": open_actions})
                    bound.update(incident_ctx)
                    emit_row(rows, ts, comp_id, log_id, bound, "", f"bg:{state}:{source_key}:{i}", chosen)
            else:
                hosts = comp.get("hosts", []) or [""]
                for h in hosts:
                    n = allocate_count(eff * dur_min, f"bg:{state}:{source_key}:{h}:{iso_ms(interval_start)}")
                    times = schedule_times(interval_start, interval_end, n, f"bg:{state}:{source_key}:{h}")
                    for i, ts in enumerate(times):
                        bound = {}
                        if comp_id == "edge_nginx" and log_id == "worker_stats":
                            minute_index = int((ts - BASE_TIME).total_seconds() // 60)
                            req_s = 0.8 + 1.7 * u01(f"reqs:{comp_id}:{h}:{minute_index}")
                            bound.update(
                                {
                                    "conns": sample_int(20, 800, f"conns:{comp_id}:{h}:{minute_index}"),
                                    "req_s": float(req_s),
                                    "rss_mb": sample_int(150, 1200, f"rss:{comp_id}:{h}:{minute_index}"),
                                }
                            )
                        bound.update(incident_ctx)
                        emit_row(rows, ts, comp_id, log_id, bound, "", f"bg:{state}:{source_key}:{h}:{i}", h)

# ------------------------ One-shots ------------------------

def simulate_one_shots(rows: List[Dict[str, Any]], event: Dict[str, Any], incident_ctx: Dict[str, Any]) -> None:
    at_min = int(event["at_min"])
    ev_time = minutes_to_dt(at_min)
    for os in event.get("one_shots", []):
        ref = os["ref"]
        comp_id, log_id = ref.split(".", 1)
        count = int(os["count"])
        allowed_hosts = os.get("hosts", [])
        for i in range(count):
            jitter_s = 10.0 * u01(f"oneshot:{ref}:{at_min}:{i}")
            ts = ev_time + timedelta(seconds=jitter_s, milliseconds=5 * i)
            chosen_host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_component_host(comp_id, f"oneshot:{ref}:{at_min}:{i}")
            bound: Dict[str, Any] = {}
            if comp_id == "secops" and log_id in ("incident_declared", "incident_status"):
                bound.update({"inc_id": incident_ctx.get("inc_id", sample_str_pattern("INC-####", f"inc:{at_min}"))})
            if comp_id == "secops" and log_id == "report_received":
                bound.update({"ref": incident_ctx.get("ref", sample_str_pattern("P0-####", "p0ref"))})
            bound.update(incident_ctx)
            emit_row(rows, ts, comp_id, log_id, bound, "", f"oneshot:{ref}:{at_min}:{i}", chosen_host)

# ------------------------ Main simulation ------------------------

def main() -> None:
    # Explicit seeding for verifier-required reproducibility; core sim is md5-keyed deterministic.
    random.seed(0)
    np.random.seed(0)

    rows: List[Dict[str, Any]] = []

    scen = SCENARIO["scenario"]
    n_start = scen["time"]["phases"]["n"]["start_min"]
    n_end = scen["time"]["phases"]["n"]["end_min"]

    incident_ctx = {
        "inc_id": sample_str_pattern("INC-####", "incident_id"),
        "ref": sample_str_pattern("P0-####", "p0ref"),
    }

    # NORMAL PHASE
    n0 = minutes_to_dt(n_start)
    n1 = minutes_to_dt(n_end)

    simulate_background(rows, "n", n0, n1, rate_mult={}, incident_ctx={})

    for flow in SYSTEM["flows"]["n"]["req"]:
        expected = float(flow["rpm"]) * ((n1 - n0).total_seconds() / 60.0)
        count = allocate_count(expected, f"flow:n:{flow['id']}:{iso_ms(n0)}")
        starts = schedule_times(n0, n1, count, f"flow:n:{flow['id']}")
        for i, st in enumerate(starts):
            simulate_flow_instance(
                rows=rows,
                state="n",
                flow=flow,
                start_time=st,
                instance_key=f"n:{flow['id']}:{i}",
                lat_mult_p50=1.0,
                lat_mult_p95=1.0,
                incident_ctx={},
            )

    # FAILURE PHASE
    for interval in FAILURE_INTERVALS:
        i0 = minutes_to_dt(interval["start_min"])
        i1 = minutes_to_dt(interval["end_min"])
        rate_mult = interval["rate_mult"]
        lat_mult_table = interval["lat_mult"]

        simulate_background(rows, "f", i0, i1, rate_mult=rate_mult, incident_ctx=incident_ctx)

        for flow in SYSTEM["flows"]["f"]["req"]:
            mult = float(rate_mult.get(flow["id"], 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            if rpm_eff <= 0:
                continue
            expected = rpm_eff * ((i1 - i0).total_seconds() / 60.0)
            count = allocate_count(expected, f"flow:f:{flow['id']}:{interval['start_min']}:{interval['end_min']}")
            starts = schedule_times(i0, i1, count, f"flow:f:{flow['id']}:{interval['start_min']}")
            lm50, lm95 = flow_latency_multiplier(flow["id"], lat_mult_table)
            for i, st in enumerate(starts):
                simulate_flow_instance(
                    rows=rows,
                    state="f",
                    flow=flow,
                    start_time=st,
                    instance_key=f"f:{flow['id']}:{interval['start_min']}:{i}",
                    lat_mult_p50=lm50,
                    lat_mult_p95=lm95,
                    incident_ctx=incident_ctx,
                )

        for ev in interval.get("events_at_start", []):
            simulate_one_shots(rows, ev, incident_ctx)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(iso_ms)
    df = df.drop(columns=["timestamp_dt"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Log volume out of bounds: {len(df)} rows")
    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV columns incorrect")

    df.to_csv("logs.csv", index=False)

if __name__ == "__main__":
    main()
