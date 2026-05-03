import math
import re
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "appnexus_impbus_ad_serving",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id_len": 32},
    "components": {
        "internal_db": {
            "svc": "internal-db",
            "hosts": ["db-1"],
            "logs": {
                "commit_change": {
                    "lvl": "INFO",
                    "msg": "committed data change change_id={change_id} table={table} op={op}",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "table": {"k": "ch", "v": ["rare_object_table"]},
                        "op": {"k": "ch", "v": ["DELETE"]},
                    },
                }
            },
            "beh": {"n": [], "f": []},
        },
        "data_distributor": {
            "svc": "data-dist",
            "hosts": ["dd-1"],
            "logs": {
                "delta_publish": {
                    "lvl": "INFO",
                    "msg": "delta published change_id={change_id} kind={kind} items={items}",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "kind": {"k": "ch", "v": ["object_delete"]},
                        "items": {"k": "i", "v": [1, 5]},
                    },
                },
                "snapshot_progress": {
                    "lvl": "INFO",
                    "msg": "snapshot build dc={dc} pct={pct}",
                    "vars": {
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                        "pct": {"k": "i", "v": [0, 100]},
                    },
                },
                "snapshot_ready_ams1": {
                    "lvl": "INFO",
                    "msg": "snapshot ready dc=AMS1 snapshot_id={snapshot_id} duration_s={dur_s}",
                    "vars": {
                        "snapshot_id": {"k": "hex", "v": 10},
                        "dur_s": {"k": "i", "v": [300, 3600]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "snapshot_progress", "per_min": 0.05, "scope": "global"}],
                "f": [{"id": "snapshot_progress", "per_min": 0.10, "scope": "global"}],
            },
        },
        "validation_engine": {
            "svc": "validation-eng",
            "hosts": ["val-1"],
            "logs": {
                "candidate_pass": {
                    "lvl": "INFO",
                    "msg": "validation passed change_id={change_id} checks={checks} wait_window_s={wait_s}",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "checks": {"k": "ch", "v": ["schema_ok", "invariants_ok"]},
                        "wait_s": {"k": "i", "v": [0, 0]},
                    },
                },
                "val_crash": {
                    "lvl": "ERROR",
                    "msg": "validation crashed reason={reason} change_id={change_id} pid={pid}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["malloc(): double free or corruption", "SIGABRT", "segfault"]},
                        "change_id": {"k": "hex", "v": 12},
                        "pid": {"k": "i", "v": [1000, 9999]},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "val_crash", "per_min": 0.02, "scope": "global"}]},
        },
        "ad_gateway": {
            "svc": "ad-gateway",
            "hosts": ["gw-ams1", "gw-iad1", "gw-sjc1", "gw-fra1", "gw-sin1", "gw-syd1"],
            "logs": {
                "req_direct": {
                    "lvl": "INFO",
                    "msg": "direct req {method} {route} req_id={req_id} dc={dc}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "route": {"k": "ch", "v": ["/tag"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "req_exchange": {
                    "lvl": "INFO",
                    "msg": "exchange req {method} {route} req_id={req_id} dc={dc}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/auction"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "upstream_connect_fail": {
                    "lvl": "WARN",
                    "msg": "upstream impbus {err} req_id={req_id} dc={dc}",
                    "vars": {
                        "err": {"k": "ch", "v": ["connection refused", "timeout", "no healthy upstream"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "resp_200": {
                    "lvl": "INFO",
                    "msg": "resp 200 bytes={bytes} dur_ms={dur_ms} req_id={req_id} dc={dc}",
                    "vars": {
                        "bytes": {"k": "i", "v": [200, 25000]},
                        "dur_ms": {"k": "i", "v": [1, 300]},
                        "req_id": {"k": "uuid", "v": None},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "resp_503": {
                    "lvl": "INFO",
                    "msg": "resp 503 bytes={bytes} dur_ms={dur_ms} req_id={req_id} dc={dc}",
                    "vars": {
                        "bytes": {"k": "i", "v": [0, 600]},
                        "dur_ms": {"k": "i", "v": [5, 10000]},
                        "req_id": {"k": "uuid", "v": None},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "pool_health": {
                    "lvl": "INFO",
                    "msg": "impbus_pool healthy={healthy} unhealthy={unhealthy} dc={dc}",
                    "vars": {"dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]}},
                    "state_vars": {
                        "n": {"healthy": {"k": "i", "v": [6, 8]}, "unhealthy": {"k": "i", "v": [0, 1]}},
                        "f": {"healthy": {"k": "i", "v": [0, 6]}, "unhealthy": {"k": "i", "v": [0, 8]}},
                    },
                },
            },
            "beh": {
                "n": [{"id": "pool_health", "per_min": 0.20, "scope": "per_host"}],
                "f": [{"id": "pool_health", "per_min": 0.20, "scope": "per_host"}],
            },
        },
        "impbus": {
            "svc": "impbus",
            "hosts": ["imp-ams1", "imp-iad1", "imp-sjc1", "imp-fra1", "imp-sin1", "imp-syd1"],
            "logs": {
                "auction_direct_ok": {
                    "lvl": "INFO",
                    "msg": "served direct ad req_id={req_id} creative={creative} price_cpm={price} dur_ms={dur_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "creative": {"k": "hex", "v": 8},
                        "price": {"k": "f", "v": [0.05, 12.0]},
                        "dur_ms": {"k": "i", "v": [1, 250]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "auction_exchange_ok": {
                    "lvl": "INFO",
                    "msg": "processed exchange auction req_id={req_id} bid={bid} dur_ms={dur_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "bid": {"k": "ch", "v": ["bid", "no_bid"]},
                        "dur_ms": {"k": "i", "v": [1, 280]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "delta_applied": {
                    "lvl": "INFO",
                    "msg": "delta applied change_id={change_id} op={op} obj={obj} retire_after_ms={retire_ms} dc={dc}",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "op": {"k": "ch", "v": ["delete"]},
                        "obj": {"k": "ch", "v": ["rare_object"]},
                        "retire_ms": {"k": "i", "v": [60000, 240000]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
                "snapshot_loaded_ams1": {
                    "lvl": "INFO",
                    "msg": "snapshot loaded snapshot_id={snapshot_id} dc=AMS1 age_s={age_s}",
                    "vars": {"snapshot_id": {"k": "hex", "v": 10}, "age_s": {"k": "i", "v": [0, 7200]}},
                },
                "crash": {
                    "lvl": "ERROR",
                    "msg": "impbus crashed signal={signal} reason={reason} dc={dc} pid={pid}",
                    "vars": {
                        "signal": {"k": "ch", "v": ["SIGABRT", "SIGSEGV"]},
                        "reason": {"k": "ch", "v": ["malloc(): double free or corruption", "segfault", "heap corruption detected"]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                        "pid": {"k": "i", "v": [1000, 9999]},
                    },
                },
                "crash_wave": {
                    "lvl": "ERROR",
                    "msg": "impbus fatal crash (wave=initial) signal={signal} reason={reason} dc={dc} pid={pid}",
                    "vars": {
                        "signal": {"k": "ch", "v": ["SIGABRT", "SIGSEGV"]},
                        "reason": {"k": "ch", "v": ["malloc(): double free or corruption", "segfault", "heap corruption detected"]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                        "pid": {"k": "i", "v": [1000, 9999]},
                    },
                },
                "restart": {
                    "lvl": "WARN",
                    "msg": "impbus restarting backoff_ms={backoff} dc={dc}",
                    "vars": {
                        "backoff": {"k": "i", "v": [200, 5000]},
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                    },
                },
            },
            "beh": {
                "n": [],
                "f": [
                    {"id": "crash", "per_min": 0.05, "scope": "per_host"},
                    {"id": "restart", "per_min": 0.05, "scope": "per_host"},
                ],
            },
        },
        "deploy_orchestrator": {
            "svc": "deploy",
            "hosts": ["deploy-1"],
            "logs": {
                "deploy_start": {
                    "lvl": "INFO",
                    "msg": "deploy start version={version} scope={scope} mode={mode}",
                    "vars": {
                        "version": {"k": "ch", "v": ["impbus_2013_09_17_1"]},
                        "scope": {"k": "ch", "v": ["global"]},
                        "mode": {"k": "ch", "v": ["simultaneous"]},
                    },
                },
                "queue_sat": {
                    "lvl": "WARN",
                    "msg": "deploy queue saturated pending={pending} workers={workers} mode={mode}",
                    "vars": {
                        "pending": {"k": "i", "v": [50, 5000]},
                        "workers": {"k": "i", "v": [5, 200]},
                        "mode": {"k": "ch", "v": ["simultaneous", "rolling"]},
                    },
                },
                "rolling_switch": {
                    "lvl": "INFO",
                    "msg": "deploy switching to rolling batch_size={batch} interval_s={interval_s}",
                    "vars": {"batch": {"k": "i", "v": [10, 200]}, "interval_s": {"k": "i", "v": [30, 600]}},
                },
                "deploy_complete": {
                    "lvl": "INFO",
                    "msg": "deploy complete version={version} scope={scope} succeeded={succeeded} duration_min={dur_min}",
                    "vars": {
                        "version": {"k": "ch", "v": ["impbus_2013_09_17_1"]},
                        "scope": {"k": "ch", "v": ["global"]},
                        "succeeded": {"k": "ch", "v": ["true"]},
                        "dur_min": {"k": "i", "v": [1, 90]},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "queue_sat", "per_min": 0.05, "scope": "global"}]},
        },
        "monitor": {
            "svc": "monitor",
            "hosts": ["mon-1"],
            "logs": {
                "alert_down": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT impbus down dc={dc} healthy={healthy} total={total} err_rate={err_rate}",
                    "vars": {
                        "dc": {"k": "ch", "v": ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]},
                        "healthy": {"k": "i", "v": [0, 3]},
                        "total": {"k": "i", "v": [6, 8]},
                        "err_rate": {"k": "f", "v": [0.30, 1.00]},
                    },
                },
                "alert_partial": {
                    "lvl": "WARN",
                    "msg": "ALERT partial recovery dc={dc} direct_success_rate={rate}",
                    "vars": {"dc": {"k": "ch", "v": ["AMS1"]}, "rate": {"k": "f", "v": [0.10, 0.95]}},
                },
                "supply_ramp": {
                    "lvl": "INFO",
                    "msg": "supply ramp started target_exchange_pct={pct} reason={reason}",
                    "vars": {"pct": {"k": "i", "v": [10, 100]}, "reason": {"k": "ch", "v": ["stability_check", "capacity_ramp"]}},
                },
            },
            "beh": {
                "n": [],
                "f": [
                    {"id": "alert_down", "per_min": 0.20, "scope": "global"},
                    {"id": "alert_partial", "per_min": 0.05, "scope": "global"},
                ],
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "direct_ad_serving_ok",
                "rpm": 160,
                "emit": ["ad_gateway.req_direct", "impbus.auction_direct_ok", "ad_gateway.resp_200"],
                "latency_ms": [[0, 0], [8, 30], [3, 12]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "exchange_auction_ok",
                "rpm": 240,
                "emit": ["ad_gateway.req_exchange", "impbus.auction_exchange_ok", "ad_gateway.resp_200"],
                "latency_ms": [[0, 0], [10, 40], [3, 12]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "direct_ad_serving_ok_f",
                "rpm": 155,
                "emit": ["ad_gateway.req_direct", "impbus.auction_direct_ok", "ad_gateway.resp_200"],
                "latency_ms": [[0, 0], [12, 60], [4, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "direct_ad_serving_503",
                "rpm": 5,
                "emit": ["ad_gateway.req_direct", "ad_gateway.upstream_connect_fail", "ad_gateway.resp_503"],
                "latency_ms": [[0, 0], [60, 900], [3, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "exchange_auction_ok_f",
                "rpm": 230,
                "emit": ["ad_gateway.req_exchange", "impbus.auction_exchange_ok", "ad_gateway.resp_200"],
                "latency_ms": [[0, 0], [14, 70], [4, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "exchange_auction_503",
                "rpm": 10,
                "emit": ["ad_gateway.req_exchange", "ad_gateway.upstream_connect_fail", "ad_gateway.resp_503"],
                "latency_ms": [[0, 0], [60, 1000], [3, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "incident_2013_09_17_impbus_crash_from_data_delta",
    "time": {
        "base_utc": "2013-09-17T00:00:00Z",
        "total_minutes": 50,
        "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}},
    },
    "failure_events": [
        {
            "order": 1,
            "at_min": 20,
            "rate_multipliers": {
                "direct_ad_serving_ok_f": 0.0,
                "exchange_auction_ok_f": 0.0,
                "direct_ad_serving_503": 35.0,
                "exchange_auction_503": 24.0,
                "impbus.crash": 20.0,
                "impbus.restart": 10.0,
                "monitor.alert_down": 6.0,
                "monitor.alert_partial": 0.0,
                "ad_gateway.pool_health": 3.0,
                "data_distributor.snapshot_progress": 0.0,
                "validation_engine.val_crash": 0.0,
                "deploy_orchestrator.queue_sat": 0.0,
            },
            "latency_multipliers": {
                "direct_ad_serving_503": {"p50": 1.8, "p95": 3.0},
                "exchange_auction_503": {"p50": 1.8, "p95": 3.2},
            },
            "one_shots": [
                {"ref": "internal_db.commit_change", "count": 1, "hosts": ["db-1"]},
                {"ref": "data_distributor.delta_publish", "count": 1, "hosts": ["dd-1"]},
                {"ref": "validation_engine.candidate_pass", "count": 1, "hosts": ["val-1"]},
                {
                    "ref": "impbus.delta_applied",
                    "count": 6,
                    "hosts": ["imp-ams1", "imp-iad1", "imp-sjc1", "imp-fra1", "imp-sin1", "imp-syd1"],
                },
                {
                    "ref": "impbus.crash_wave",
                    "count": 6,
                    "hosts": ["imp-ams1", "imp-iad1", "imp-sjc1", "imp-fra1", "imp-sin1", "imp-syd1"],
                },
            ],
        },
        {
            "order": 2,
            "at_min": 23,
            "rate_multipliers": {
                "direct_ad_serving_ok_f": 0.01,
                "exchange_auction_ok_f": 0.01,
                "direct_ad_serving_503": 32.0,
                "exchange_auction_503": 22.0,
                "impbus.crash": 12.0,
                "impbus.restart": 12.0,
                "monitor.alert_down": 5.0,
                "monitor.alert_partial": 0.0,
                "ad_gateway.pool_health": 3.0,
                "data_distributor.snapshot_progress": 2.0,
                "validation_engine.val_crash": 8.0,
            },
            "latency_multipliers": {
                "direct_ad_serving_503": {"p50": 1.6, "p95": 2.7},
                "exchange_auction_503": {"p50": 1.6, "p95": 2.9},
            },
            "one_shots": [],
        },
        {
            "order": 3,
            "at_min": 36,
            "rate_multipliers": {
                "direct_ad_serving_ok_f": 0.08,
                "direct_ad_serving_503": 30.0,
                "exchange_auction_ok_f": 0.02,
                "exchange_auction_503": 22.0,
                "impbus.crash": 8.0,
                "impbus.restart": 8.0,
                "monitor.alert_down": 3.0,
                "monitor.alert_partial": 6.0,
                "ad_gateway.pool_health": 2.0,
                "data_distributor.snapshot_progress": 6.0,
                "validation_engine.val_crash": 6.0,
            },
            "latency_multipliers": {
                "direct_ad_serving_503": {"p50": 1.3, "p95": 2.2},
                "exchange_auction_503": {"p50": 1.4, "p95": 2.5},
            },
            "one_shots": [
                {"ref": "data_distributor.snapshot_ready_ams1", "count": 1, "hosts": ["dd-1"]},
                {"ref": "impbus.snapshot_loaded_ams1", "count": 1, "hosts": ["imp-ams1"]},
            ],
        },
        {
            "order": 4,
            "at_min": 44,
            "rate_multipliers": {
                "direct_ad_serving_ok_f": 0.70,
                "direct_ad_serving_503": 10.0,
                "exchange_auction_ok_f": 0.15,
                "exchange_auction_503": 15.0,
                "impbus.crash": 5.0,
                "impbus.restart": 5.0,
                "monitor.alert_down": 2.0,
                "monitor.alert_partial": 4.0,
                "ad_gateway.pool_health": 1.5,
                "data_distributor.snapshot_progress": 2.0,
                "validation_engine.val_crash": 4.0,
                "deploy_orchestrator.queue_sat": 10.0,
            },
            "latency_multipliers": {
                "direct_ad_serving_ok_f": {"p50": 1.1, "p95": 1.4},
                "exchange_auction_ok_f": {"p50": 1.1, "p95": 1.5},
                "direct_ad_serving_503": {"p50": 1.2, "p95": 2.0},
                "exchange_auction_503": {"p50": 1.2, "p95": 2.2},
            },
            "one_shots": [
                {"ref": "deploy_orchestrator.deploy_start", "count": 1, "hosts": ["deploy-1"]},
                {"ref": "deploy_orchestrator.rolling_switch", "count": 1, "hosts": ["deploy-1"]},
            ],
        },
        {
            "order": 5,
            "at_min": 48,
            "rate_multipliers": {
                "direct_ad_serving_ok_f": 1.0,
                "direct_ad_serving_503": 0.2,
                "exchange_auction_ok_f": 0.70,
                "exchange_auction_503": 6.0,
                "impbus.crash": 0.5,
                "impbus.restart": 0.5,
                "monitor.alert_down": 0.0,
                "monitor.alert_partial": 0.0,
                "ad_gateway.pool_health": 1.0,
                "data_distributor.snapshot_progress": 0.5,
                "validation_engine.val_crash": 0.0,
                "deploy_orchestrator.queue_sat": 0.0,
            },
            "latency_multipliers": {
                "direct_ad_serving_ok_f": {"p50": 1.0, "p95": 1.1},
                "exchange_auction_ok_f": {"p50": 1.0, "p95": 1.2},
            },
            "one_shots": [
                {"ref": "deploy_orchestrator.deploy_complete", "count": 1, "hosts": ["deploy-1"]},
                {"ref": "monitor.supply_ramp", "count": 1, "hosts": ["mon-1"]},
            ],
        },
    ],
}

DCS = ["AMS1", "IAD1", "SJC1", "FRA1", "SIN1", "SYD1"]
DC_RE = re.compile(r"(ams1|iad1|sjc1|fra1|sin1|syd1)", re.IGNORECASE)


def parse_base_time(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def iso8601_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    dt2 = dt.replace(microsecond=ms * 1000)
    return dt2.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def infer_dc_from_host(host: str) -> Optional[str]:
    m = DC_RE.search(host)
    if not m:
        return None
    return m.group(1).upper()


def host_for_dc(component_id: str, dc: str) -> str:
    hosts = SYSTEM["components"][component_id]["hosts"]
    needle = dc.lower()
    for h in hosts:
        if needle in h.lower():
            return h
    return hosts[0] if hosts else ""


def get_template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def choose_hex(rng: np.random.Generator, n: int) -> str:
    x = rng.integers(0, 16, size=n, dtype=np.int64)
    return "".join("0123456789abcdef"[int(v)] for v in x)


def choose_uuid(rng: np.random.Generator) -> str:
    h = choose_hex(rng, 32)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def choose_int(rng: np.random.Generator, lo: int, hi: int) -> int:
    if lo == hi:
        return int(lo)
    return int(rng.integers(lo, hi + 1))


def choose_float(rng: np.random.Generator, lo: float, hi: float) -> float:
    if lo == hi:
        return float(lo)
    return float(lo + (hi - lo) * rng.random())


def bounded_lognormal_ms(rng: np.random.Generator, p50: float, p95: float, cap_mult: float = 3.0) -> float:
    if p50 <= 0 and p95 <= 0:
        return 0.0
    if p50 <= 0:
        return max(0.0, min(float(p95) * 0.5, float(p95) * cap_mult))
    if p95 <= 0:
        return float(p50)

    ratio = max(1.0001, float(p95) / float(p50))
    sigma = math.log(ratio) / 1.645
    sigma = max(0.05, min(2.0, sigma))
    mu = math.log(float(p50))
    x = float(rng.lognormal(mean=mu, sigma=sigma))
    cap = cap_mult * float(p95)
    if x > cap:
        x = cap
    return max(0.0, x)


def alloc_count(expected: float, carry: float) -> Tuple[int, float]:
    total = expected + carry
    n = int(math.floor(total + 1e-12))
    new_carry = total - n
    if new_carry < 0:
        new_carry = 0.0
    if new_carry >= 1.0:
        bump = int(math.floor(new_carry))
        n += bump
        new_carry -= bump
    return n, new_carry


def schedule_times(
    rng: np.random.Generator,
    start_dt: datetime,
    end_dt: datetime,
    n: int,
    jitter_ms: int = 250,
) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return [start_dt] * n
    step = dur_s / n
    times: List[datetime] = []
    for i in range(n):
        base = (i + 0.5) * step
        jitter = (rng.random() - 0.5) * 2.0 * (jitter_ms / 1000.0)
        t = start_dt + timedelta(seconds=base + jitter)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        times.append(t)
    return times


def render_message(
    rng: np.random.Generator,
    comp_id: str,
    log_id: str,
    state: str,
    bound: Dict[str, Any],
) -> str:
    tpl = get_template(comp_id, log_id)
    msg = tpl["msg"]
    vars_spec: Dict[str, Any] = dict(tpl.get("vars", {}))
    state_vars = tpl.get("state_vars", {})
    if state in state_vars:
        vars_spec.update(state_vars[state])

    values: Dict[str, Any] = {}
    for k, spec in vars_spec.items():
        if k in bound:
            values[k] = bound[k]
            continue
        kind = spec["k"]
        v = spec["v"]
        if kind == "ch":
            choices = list(v)
            values[k] = choices[int(rng.integers(0, len(choices)))]
        elif kind == "i":
            lo, hi = int(v[0]), int(v[1])
            values[k] = choose_int(rng, lo, hi)
        elif kind == "f":
            lo, hi = float(v[0]), float(v[1])
            values[k] = f"{choose_float(rng, lo, hi):.2f}"
        elif kind == "uuid":
            values[k] = choose_uuid(rng)
        elif kind == "hex":
            values[k] = choose_hex(rng, int(v))
        else:
            values[k] = str(v) if v is not None else ""
    return msg.format(**values)


def get_int_var_domain(comp_id: str, log_id: str, var_name: str, state: str) -> Optional[Tuple[int, int]]:
    tpl = get_template(comp_id, log_id)
    spec = None
    if var_name in tpl.get("vars", {}):
        spec = tpl["vars"][var_name]
    else:
        state_vars = tpl.get("state_vars", {})
        if state in state_vars and var_name in state_vars[state]:
            spec = state_vars[state][var_name]
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    lo, hi = int(spec["v"][0]), int(spec["v"][1])
    return lo, hi


def constrain_delays_by_dur_ms(
    delays_ms_int: List[int],
    emit_chain: List[str],
    state: str,
) -> List[int]:
    """
    Ensure that any emitted log that carries dur_ms stays within its modeled integer domain.
    We do this by adjusting the latency-derived delays (in ms) so that message timing matches timestamps
    and never exceeds the domain bounds.
    """
    delays = list(delays_ms_int)
    constraints: List[Tuple[int, int, int]] = []  # (idx, lo, hi) for dur_ms at that log index
    for idx, ref in enumerate(emit_chain):
        comp_id, log_id = ref.split(".", 1)
        dom = get_int_var_domain(comp_id, log_id, "dur_ms", state)
        if dom is not None:
            lo, hi = dom
            constraints.append((idx, lo, hi))

    def elapsed_to(i: int) -> int:
        if i <= 0:
            return 0
        return int(sum(delays[1 : i + 1]))

    for idx, _lo, hi in sorted(constraints, key=lambda x: x[0]):
        e = elapsed_to(idx)
        if e <= hi:
            continue
        excess = e - hi
        for j in range(idx, 0, -1):
            if excess <= 0:
                break
            min_allowed = 1
            reducible = max(0, delays[j] - min_allowed)
            if reducible <= 0:
                continue
            take = reducible if reducible <= excess else excess
            delays[j] -= take
            excess -= take

    for idx, lo, hi in sorted(constraints, key=lambda x: x[0]):
        e = elapsed_to(idx)
        if e >= lo:
            continue
        need = lo - e
        if idx >= 1:
            add_cap = max(0, hi - e)
            add = min(need, add_cap)
            delays[idx] += add

    return delays


@dataclass(frozen=True)
class IntervalControls:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> List[IntervalControls]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["failure_events"], key=lambda e: (e["at_min"], e["order"]))

    active_rate: Dict[str, float] = {}
    active_latency: Dict[str, Dict[str, float]] = {}

    intervals: List[IntervalControls] = []
    for i, ev in enumerate(events):
        at = int(ev["at_min"])
        if at < fstart or at >= fend:
            continue
        for k, v in ev.get("rate_multipliers", {}).items():
            active_rate[k] = float(v)
        for k, v in ev.get("latency_multipliers", {}).items():
            active_latency[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}

        next_at = fend
        for j in range(i + 1, len(events)):
            next_at = int(events[j]["at_min"])
            break
        intervals.append(
            IntervalControls(
                start_min=at,
                end_min=min(next_at, fend),
                rate_mult=dict(active_rate),
                latency_mult=dict(active_latency),
            )
        )
    if not intervals:
        intervals = [IntervalControls(start_min=fstart, end_min=fend, rate_mult={}, latency_mult={})]
    return intervals


def flow_latency_scaled(flow_id: str, base_pair: List[float], ctl: IntervalControls) -> Tuple[float, float]:
    p50, p95 = float(base_pair[0]), float(base_pair[1])
    if p50 <= 0 and p95 <= 0:
        return 0.0, 0.0
    mult = ctl.latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    return p50 * float(mult.get("p50", 1.0)), p95 * float(mult.get("p95", 1.0))


def severity_hint_from_controls(ctl: IntervalControls) -> float:
    crash = float(ctl.rate_mult.get("impbus.crash", 1.0))
    d503 = float(ctl.rate_mult.get("direct_ad_serving_503", 1.0))
    e503 = float(ctl.rate_mult.get("exchange_auction_503", 1.0))
    ok = float(ctl.rate_mult.get("direct_ad_serving_ok_f", 0.0))
    return (crash + 0.5 * (d503 + e503)) / max(0.01, ok + 1.0)


def pool_health_values(rng: np.random.Generator, state: str, ctl: Optional[IntervalControls]) -> Tuple[int, int]:
    if state == "n" or ctl is None:
        return -1, -1
    sev = severity_hint_from_controls(ctl)
    if sev > 30:
        healthy = choose_int(rng, 0, 1)
    elif sev > 12:
        healthy = choose_int(rng, 0, 2)
    elif sev > 6:
        healthy = choose_int(rng, 1, 3)
    elif sev > 2:
        healthy = choose_int(rng, 2, 5)
    else:
        healthy = choose_int(rng, 4, 6)
    unhealthy = choose_int(rng, 0, 8)
    unhealthy = max(0, int(round(unhealthy * (1.0 - 0.08 * healthy))))
    return healthy, unhealthy


def choose_upstream_err(rng: np.random.Generator, ctl: IntervalControls, dc: str) -> str:
    sev = severity_hint_from_controls(ctl)
    if sev > 20:
        return "no healthy upstream"
    if sev > 8:
        return "timeout" if (hash(dc) % 2 == 0) else "connection refused"
    return "connection refused"


def simulate_flow_instance(
    rng: np.random.Generator,
    flow: Dict[str, Any],
    state: str,
    start_dt: datetime,
    ctl: Optional[IntervalControls],
    instance_seq: int,
) -> List[Dict[str, Any]]:
    flow_id = flow["id"]
    trace_id = choose_hex(rng, SYSTEM["tracing"]["trace_id_len"]) if flow.get("trace", False) else ""

    if state == "n":
        dc = DCS[instance_seq % len(DCS)]
    else:
        if ctl is not None and ctl.start_min >= 36 and ctl.end_min <= 44 and flow_id == "direct_ad_serving_ok_f":
            if (instance_seq % 10) < 6:
                dc = "AMS1"
            else:
                dc = DCS[1 + ((instance_seq // 10) % (len(DCS) - 1))]
        else:
            dc = DCS[instance_seq % len(DCS)]

    req_id = choose_uuid(rng)
    host_cache: Dict[str, str] = {
        "ad_gateway": host_for_dc("ad_gateway", dc),
        "impbus": host_for_dc("impbus", dc),
    }

    emit_chain = flow["emit"]
    lat_hints = flow["latency_ms"]

    delays_ms_int: List[int] = []
    for pair in lat_hints:
        if state == "f" and ctl is not None:
            p50, p95 = flow_latency_scaled(flow_id, pair, ctl)
        else:
            p50, p95 = float(pair[0]), float(pair[1])
        d = bounded_lognormal_ms(rng, p50, p95, cap_mult=3.0)
        if float(pair[0]) == 0.0 and float(pair[1]) == 0.0:
            d = 0.0
        delays_ms_int.append(max(0, int(round(d))))

    delays_ms_int = constrain_delays_by_dur_ms(delays_ms_int, emit_chain, state)

    times: List[datetime] = []
    t = start_dt
    for dms in delays_ms_int:
        t = t + timedelta(milliseconds=int(dms))
        times.append(t)

    def elapsed_to(idx: int) -> int:
        if idx <= 0:
            return 0
        return int(sum(delays_ms_int[1 : idx + 1]))

    out: List[Dict[str, Any]] = []
    for idx, ref in enumerate(emit_chain):
        comp_id, log_id = ref.split(".", 1)
        tpl = get_template(comp_id, log_id)
        bound: Dict[str, Any] = {"dc": dc, "req_id": req_id}

        dom = get_int_var_domain(comp_id, log_id, "dur_ms", state)
        if dom is not None:
            dur_val = elapsed_to(idx)
            lo, hi = dom
            if dur_val < lo:
                dur_val = lo
            if dur_val > hi:
                dur_val = hi
            bound["dur_ms"] = dur_val

        if comp_id == "ad_gateway" and log_id == "upstream_connect_fail" and state == "f" and ctl is not None:
            bound["err"] = choose_upstream_err(rng, ctl, dc)

        msg = render_message(rng, comp_id, log_id, state, bound)
        host = host_cache.get(comp_id, SYSTEM["components"][comp_id]["hosts"][0] if SYSTEM["components"][comp_id]["hosts"] else "")
        out.append(
            {
                "timestamp_dt": times[idx],
                "level": tpl["lvl"],
                "message": msg,
                "trace_id": trace_id,
                "service": SYSTEM["components"][comp_id]["svc"] or "",
                "host": host or "",
            }
        )
    return out


def emit_background(
    rng: np.random.Generator,
    base_time: datetime,
    state: str,
    start_min: int,
    end_min: int,
    ctl: Optional[IntervalControls],
    carry: Dict[str, float],
    incident_ctx: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    start_dt = base_time + timedelta(minutes=start_min)
    end_dt = base_time + timedelta(minutes=end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id in sorted(SYSTEM["components"].keys()):
        comp = SYSTEM["components"][comp_id]
        beh = comp.get("beh", {}).get(state, [])
        for entry in beh:
            log_id = entry["id"]
            per_min = float(entry["per_min"])
            scope = entry.get("scope", "per_host")
            mult = 1.0
            if state == "f" and ctl is not None:
                mult = float(ctl.rate_mult.get(f"{comp_id}.{log_id}", 1.0))
            eff_per_min = per_min * mult

            tpl = get_template(comp_id, log_id)

            if scope == "global":
                key = f"bg|{state}|{comp_id}.{log_id}|global"
                n, carry[key] = alloc_count(eff_per_min * duration_min, carry.get(key, 0.0))
                times = schedule_times(rng, start_dt, end_dt, n, jitter_ms=350)
                hosts = comp.get("hosts", [])
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    bound: Dict[str, Any] = {}
                    dc = infer_dc_from_host(host)
                    if "dc" in tpl.get("vars", {}) and dc is not None:
                        bound["dc"] = dc
                    if log_id == "pool_health":
                        h, u = pool_health_values(rng, state, ctl)
                        if h >= 0:
                            bound["healthy"] = h
                            bound["unhealthy"] = u
                    if comp_id == "monitor" and log_id == "alert_down" and state == "f" and ctl is not None:
                        sev = severity_hint_from_controls(ctl)
                        bound["err_rate"] = f"{min(1.0, max(0.30, 0.30 + 0.02 * min(35.0, sev))):.2f}"
                    # Bind incident change_id to delayed corroboration logs.
                    if comp_id == "validation_engine" and log_id == "val_crash":
                        if "change_id" in tpl.get("vars", {}) and "change_id" in incident_ctx:
                            bound["change_id"] = incident_ctx["change_id"]
                    msg = render_message(rng, comp_id, log_id, state, bound)
                    rows.append(
                        {
                            "timestamp_dt": t,
                            "level": tpl["lvl"],
                            "message": msg,
                            "trace_id": "",
                            "service": comp["svc"] or "",
                            "host": host,
                        }
                    )
            else:
                for host in comp.get("hosts", []):
                    key = f"bg|{state}|{comp_id}.{log_id}|{host}"
                    n, carry[key] = alloc_count(eff_per_min * duration_min, carry.get(key, 0.0))
                    times = schedule_times(rng, start_dt, end_dt, n, jitter_ms=350)
                    for t in times:
                        bound = {}
                        dc = infer_dc_from_host(host)
                        if "dc" in tpl.get("vars", {}) and dc is not None:
                            bound["dc"] = dc
                        if log_id == "pool_health":
                            h, u = pool_health_values(rng, state, ctl)
                            if h >= 0:
                                bound["healthy"] = h
                                bound["unhealthy"] = u
                        if comp_id == "impbus" and log_id in ("crash", "restart") and dc is not None:
                            bound["dc"] = dc
                        # Bind incident change_id to delayed corroboration logs.
                        if comp_id == "validation_engine" and log_id == "val_crash":
                            if "change_id" in tpl.get("vars", {}) and "change_id" in incident_ctx:
                                bound["change_id"] = incident_ctx["change_id"]
                        msg = render_message(rng, comp_id, log_id, state, bound)
                        rows.append(
                            {
                                "timestamp_dt": t,
                                "level": tpl["lvl"],
                                "message": msg,
                                "trace_id": "",
                                "service": comp["svc"] or "",
                                "host": host,
                            }
                        )
    return rows


def emit_one_shots(rng: np.random.Generator, base_time: datetime, incident_ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    events = sorted(SCENARIO["failure_events"], key=lambda e: (e["at_min"], e["order"]))

    deploy_start_min = 44
    deploy_complete_min = 48
    deploy_duration_min = max(1, deploy_complete_min - deploy_start_min)

    # Snapshot ID is only needed for the event-3 one-shots; keep it generated at that time.
    snapshot_ctx: Dict[str, Any] = {}

    for ev in events:
        at_min = int(ev["at_min"])
        event_time = base_time + timedelta(minutes=at_min)
        one_shots = ev.get("one_shots", [])

        if at_min == 36 and "snapshot_id" not in snapshot_ctx:
            snapshot_ctx["snapshot_id"] = choose_hex(rng, 10)

        for idx, shot in enumerate(one_shots):
            ref = shot["ref"]
            comp_id, log_id = ref.split(".", 1)
            tpl = get_template(comp_id, log_id)
            hosts = shot.get("hosts", [])
            count = int(shot.get("count", 1))

            for k in range(count):
                offset_ms = int(40 * (idx + 1) + 7 * k)
                t = event_time + timedelta(milliseconds=offset_ms)
                host = hosts[k % len(hosts)] if hosts else (SYSTEM["components"][comp_id]["hosts"][0] if SYSTEM["components"][comp_id]["hosts"] else "")
                bound: Dict[str, Any] = {}
                dc = infer_dc_from_host(host)
                if dc and (
                    ("dc" in tpl.get("vars", {}))
                    or ("dc" in tpl.get("state_vars", {}).get("n", {}))
                    or ("dc" in tpl.get("state_vars", {}).get("f", {}))
                ):
                    bound["dc"] = dc

                # Bind the incident's triggering change_id consistently across all related one-shots.
                if "change_id" in tpl.get("vars", {}) and "change_id" in incident_ctx:
                    bound["change_id"] = incident_ctx["change_id"]

                if "snapshot_id" in tpl.get("vars", {}) and "snapshot_id" in snapshot_ctx:
                    bound["snapshot_id"] = snapshot_ctx["snapshot_id"]

                if comp_id == "deploy_orchestrator" and log_id == "deploy_complete":
                    bound["dur_min"] = deploy_duration_min

                if comp_id == "impbus" and log_id == "snapshot_loaded_ams1":
                    bound["age_s"] = choose_int(rng, 60, 900)

                msg = render_message(rng, comp_id, log_id, "f", bound)
                rows.append(
                    {
                        "timestamp_dt": t,
                        "level": tpl["lvl"],
                        "message": msg,
                        "trace_id": "",
                        "service": SYSTEM["components"][comp_id]["svc"] or "",
                        "host": host or "",
                    }
                )
    return rows


def simulate() -> pd.DataFrame:
    random.seed(1337)
    np.random.seed(1337)
    rng = np.random.default_rng(1337)

    base_time = parse_base_time(SCENARIO["time"]["base_utc"])

    # Persist incident-scoped identifiers so delayed corroboration logs reference the same trigger.
    incident_ctx: Dict[str, Any] = {"change_id": choose_hex(rng, 12)}

    n_start = int(SCENARIO["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["time"]["phases"]["n"]["end_min"])

    rows: List[Dict[str, Any]] = []

    bg_carry: Dict[str, float] = {}
    rows.extend(emit_background(rng, base_time, "n", n_start, n_end, None, bg_carry, incident_ctx))

    flow_carry: Dict[str, float] = {}
    flow_seq = 0
    start_dt = base_time + timedelta(minutes=n_start)
    end_dt = base_time + timedelta(minutes=n_end)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    for flow in SYSTEM["flows"]["n"]:
        fid = flow["id"]
        expected = float(flow["rpm"]) * duration_min
        n, flow_carry[fid] = alloc_count(expected, flow_carry.get(fid, 0.0))
        starts = schedule_times(rng, start_dt, end_dt, n, jitter_ms=500)
        for sdt in starts:
            rows.extend(simulate_flow_instance(rng, flow, "n", sdt, None, flow_seq))
            flow_seq += 1

    intervals = build_failure_intervals()

    for ctl in intervals:
        rows.extend(emit_background(rng, base_time, "f", ctl.start_min, ctl.end_min, ctl, bg_carry, incident_ctx))

    flow_by_id = {f["id"]: f for f in SYSTEM["flows"]["f"]}
    for ctl in intervals:
        istart = base_time + timedelta(minutes=ctl.start_min)
        iend = base_time + timedelta(minutes=ctl.end_min)
        dmin = (iend - istart).total_seconds() / 60.0
        for flow in SYSTEM["flows"]["f"]:
            fid = flow["id"]
            mult = float(ctl.rate_mult.get(fid, 1.0))
            expected = float(flow["rpm"]) * mult * dmin
            n, flow_carry[fid] = alloc_count(expected, flow_carry.get(fid, 0.0))
            starts = schedule_times(rng, istart, iend, n, jitter_ms=500)
            for sdt in starts:
                rows.extend(simulate_flow_instance(rng, flow_by_id[fid], "f", sdt, ctl, flow_seq))
                flow_seq += 1

    rows.extend(emit_one_shots(rng, base_time, incident_ctx))

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)

    df["timestamp"] = df["timestamp_dt"].apply(iso8601_ms)
    df = df.drop(columns=["timestamp_dt"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} out of target range [20000, 100000].")
    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("CSV columns are incorrect.")
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
