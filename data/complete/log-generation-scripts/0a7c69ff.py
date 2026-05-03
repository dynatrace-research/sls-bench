import math
import hashlib
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Fixed seed for verifier-required reproducibility (even though the simulator is hash-deterministic).
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)


SYSTEM: Dict[str, Any] = {
    "id": "direct_connect_tokyo_layer_fault",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "dx_edge_pop": {
            "svc": "direct-connect",
            "hosts": ["dx-edge-a", "dx-edge-b"],
            "logs": {
                "edge_forward_batch": {
                    "lvl": "INFO",
                    "msg": "DX edge forwarded batch {batch_id} on {conn_id} to {dst_region} bytes={bytes}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "conn_id": {"k": "ch", "v": ["dxconn-001", "dxconn-002", "dxconn-003", "dxconn-004"]},
                        "dst_region": {"k": "ch", "v": ["ap-northeast-1"]},
                        "bytes": {"k": "i", "v": [20000, 800000]},
                    },
                },
                "bgp_keepalive": {
                    "lvl": "INFO",
                    "msg": "BGP keepalive peer={peer_ip} state={state} rtt_ms={rtt_ms}",
                    "vars": {"peer_ip": {"k": "ip", "v": "169.254.0.0/16"}, "rtt_ms": {"k": "i", "v": [1, 80]}},
                    "state_vars": {
                        "n": {"state": {"k": "ch", "v": ["Established"]}},
                        "f": {"state": {"k": "ch", "v": ["Established", "HoldTimer"]}},
                    },
                },
                "bgp_flap": {
                    "lvl": "WARN",
                    "msg": "BGP session reset peer={peer_ip} reason={reason}",
                    "vars": {
                        "peer_ip": {"k": "ip", "v": "169.254.0.0/16"},
                        "reason": {"k": "ch", "v": ["hold_timer_expired", "cease_admin_reset"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "bgp_keepalive", "per_min": 1.0, "scope": "per_host"}],
                "f": [
                    {"id": "bgp_keepalive", "per_min": 1.0, "scope": "per_host"},
                    {"id": "bgp_flap", "per_min": 0.05, "scope": "per_host"},
                ],
            },
        },
        "dx_transport_layer": {
            "svc": "direct-connect",
            "hosts": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"],
            "logs": {
                "fwd_batch_result": {
                    "lvl": "INFO",
                    "msg": "Layer forwarding batch {batch_id} device={device_id} az={az} proto={proto} action={action} loss_pct={loss_pct} queue_depth={qdepth}",
                    "vars": {"batch_id": {"k": "hex", "v": 12}, "proto": {"k": "ch", "v": ["fast-converge-v2"]}},
                    "state_vars": {
                        "n": {
                            "device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"]},
                            "az": {"k": "ch", "v": ["apne1-az1", "apne1-az2"]},
                            "action": {"k": "ch", "v": ["forward"]},
                            "loss_pct": {"k": "i", "v": [0, 1]},
                            "qdepth": {"k": "i", "v": [0, 50]},
                        },
                        "f": {
                            "device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2"]},
                            "az": {"k": "ch", "v": ["apne1-az1"]},
                            "action": {"k": "ch", "v": ["forward", "congested_forward"]},
                            "loss_pct": {"k": "i", "v": [0, 60]},
                            "qdepth": {"k": "i", "v": [10, 800]},
                        },
                    },
                },
                "fwd_batch_result_fast_az2": {
                    "lvl": "INFO",
                    "msg": "Layer forwarding batch {batch_id} device={device_id} az={az} proto={proto} action={action} loss_pct={loss_pct} queue_depth={qdepth}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "device_id": {"k": "ch", "v": ["dx-l3-3", "dx-l3-4"]},
                        "az": {"k": "ch", "v": ["apne1-az2"]},
                        "proto": {"k": "ch", "v": ["fast-converge-v2"]},
                    },
                    "state_vars": {
                        "n": {"action": {"k": "ch", "v": ["forward"]}, "loss_pct": {"k": "i", "v": [0, 1]}, "qdepth": {"k": "i", "v": [0, 50]}},
                        "f": {
                            "action": {"k": "ch", "v": ["forward", "congested_forward"]},
                            "loss_pct": {"k": "i", "v": [0, 60]},
                            "qdepth": {"k": "i", "v": [10, 800]},
                        },
                    },
                },
                "fwd_batch_result_legacy_az2": {
                    "lvl": "INFO",
                    "msg": "Layer forwarding batch {batch_id} device={device_id} az={az} proto={proto} action={action} loss_pct={loss_pct} queue_depth={qdepth}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "device_id": {"k": "ch", "v": ["dx-l3-3", "dx-l3-4"]},
                        "az": {"k": "ch", "v": ["apne1-az2"]},
                        "proto": {"k": "ch", "v": ["legacy-converge-v1"]},
                    },
                    "state_vars": {
                        "n": {"action": {"k": "ch", "v": ["forward"]}, "loss_pct": {"k": "i", "v": [0, 1]}, "qdepth": {"k": "i", "v": [0, 50]}},
                        "f": {"action": {"k": "ch", "v": ["forward"]}, "loss_pct": {"k": "i", "v": [0, 8]}, "qdepth": {"k": "i", "v": [0, 200]}},
                    },
                },
                "device_fault": {
                    "lvl": "ERROR",
                    "msg": "Forwarding exception device={device_id} az={az} proto={proto} code={code} pkt_sig={pkt_sig}",
                    "vars": {"proto": {"k": "ch", "v": ["fast-converge-v2"]}, "code": {"k": "ch", "v": ["fe_assert", "rx_parser_err"]}, "pkt_sig": {"k": "hex", "v": 8}},
                    "state_vars": {
                        "n": {"device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"]}, "az": {"k": "ch", "v": ["apne1-az1", "apne1-az2"]}},
                        "f": {"device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2"]}, "az": {"k": "ch", "v": ["apne1-az1"]}},
                    },
                },
                "device_fault_fast_az2": {
                    "lvl": "ERROR",
                    "msg": "Forwarding exception device={device_id} az={az} proto={proto} code={code} pkt_sig={pkt_sig}",
                    "vars": {
                        "device_id": {"k": "ch", "v": ["dx-l3-3", "dx-l3-4"]},
                        "az": {"k": "ch", "v": ["apne1-az2"]},
                        "proto": {"k": "ch", "v": ["fast-converge-v2"]},
                        "code": {"k": "ch", "v": ["fe_assert", "rx_parser_err"]},
                        "pkt_sig": {"k": "hex", "v": 8},
                    },
                },
                "drop_metric": {
                    "lvl": "INFO",
                    "msg": "Layer drop metric device={device_id} drop_ppm={drop_ppm} queue_depth={qdepth}",
                    "vars": {"device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"]}},
                    "state_vars": {
                        "n": {"drop_ppm": {"k": "i", "v": [0, 50]}, "qdepth": {"k": "i", "v": [0, 80]}},
                        "f": {"drop_ppm": {"k": "i", "v": [500, 50000]}, "qdepth": {"k": "i", "v": [50, 1200]}},
                    },
                },
                "signature_detected": {
                    "lvl": "WARN",
                    "msg": "Unexpected packet attribute set observed device={device_id} pkt_sig={pkt_sig} count={count}",
                    "vars": {"device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"]}, "pkt_sig": {"k": "hex", "v": 8}, "count": {"k": "i", "v": [1, 50]}},
                },
                "protocol_disabled": {
                    "lvl": "INFO",
                    "msg": "Protocol {proto} disabled in {az} for mitigation",
                    "vars": {"proto": {"k": "ch", "v": ["fast-converge-v2"]}, "az": {"k": "ch", "v": ["apne1-az2"]}},
                },
            },
            "beh": {
                "n": [{"id": "drop_metric", "per_min": 1.0, "scope": "per_host"}],
                "f": [
                    {"id": "drop_metric", "per_min": 5.0, "scope": "per_host"},
                    {"id": "signature_detected", "per_min": 0.3, "scope": "per_host"},
                ],
            },
        },
        "tokyo_dc_fabric": {
            "svc": "vpc-network",
            "hosts": ["tokyo-fabric-1", "tokyo-fabric-2"],
            "logs": {
                "ingress_batch": {
                    "lvl": "INFO",
                    "msg": "Region ingress received batch {batch_id} from {conn_id} delivered_pct={delivered_pct} az={az}",
                    "vars": {"batch_id": {"k": "hex", "v": 12}, "conn_id": {"k": "ch", "v": ["dxconn-001", "dxconn-002", "dxconn-003", "dxconn-004"]}, "az": {"k": "ch", "v": ["apne1-az1", "apne1-az2"]}},
                    "state_vars": {"n": {"delivered_pct": {"k": "i", "v": [99, 100]}}, "f": {"delivered_pct": {"k": "i", "v": [40, 100]}}},
                },
                "fabric_drop_detected": {
                    "lvl": "INFO",
                    "msg": "Ingress monitoring delivered_pct_p95={deliv_p95}",
                    "vars": {},
                    "state_vars": {"n": {"deliv_p95": {"k": "i", "v": [98, 100]}}, "f": {"deliv_p95": {"k": "i", "v": [30, 95]}}},
                },
            },
            "beh": {"n": [{"id": "fabric_drop_detected", "per_min": 0.2, "scope": "global"}], "f": [{"id": "fabric_drop_detected", "per_min": 2.0, "scope": "global"}]},
        },
        "dx_health_automation": {
            "svc": "direct-connect-ops",
            "hosts": ["dx-auto-1"],
            "logs": {
                "health_eval": {
                    "lvl": "INFO",
                    "msg": "Health eval layer={layer} failed_devices={failed} total_devices={total} action={action}",
                    "vars": {"layer": {"k": "ch", "v": ["dx_layer_3"]}, "total": {"k": "i", "v": [8, 12]}},
                    "state_vars": {"n": {"failed": {"k": "i", "v": [0, 1]}, "action": {"k": "ch", "v": ["auto_isolate"]}}, "f": {"failed": {"k": "i", "v": [2, 8]}, "action": {"k": "ch", "v": ["notify_only"]}}},
                },
                "auto_isolation_skipped": {
                    "lvl": "WARN",
                    "msg": "Auto-isolation deferred layer={layer} failed_devices={failed} threshold={threshold}",
                    "vars": {"layer": {"k": "ch", "v": ["dx_layer_3"]}, "failed": {"k": "i", "v": [3, 10]}, "threshold": {"k": "i", "v": [3, 3]}},
                },
                "manual_isolation": {"lvl": "INFO", "msg": "Operator isolated {count} devices in {layer}", "vars": {"count": {"k": "i", "v": [1, 4]}, "layer": {"k": "ch", "v": ["dx_layer_3"]}}},
                "device_reset_started": {"lvl": "INFO", "msg": "Operator reset started device={device_id} az={az}", "vars": {"device_id": {"k": "ch", "v": ["dx-l3-1", "dx-l3-2", "dx-l3-3", "dx-l3-4"]}, "az": {"k": "ch", "v": ["apne1-az1", "apne1-az2"]}}},
            },
            "beh": {"n": [{"id": "health_eval", "per_min": 1.0, "scope": "global"}], "f": [{"id": "health_eval", "per_min": 1.0, "scope": "global"}, {"id": "auto_isolation_skipped", "per_min": 2.0, "scope": "global"}]},
        },
        "noc_alerting": {
            "svc": "noc",
            "hosts": ["noc-1"],
            "logs": {
                "alarm_packet_loss": {
                    "lvl": "CRITICAL",
                    "msg": "ALARM DirectConnect packet loss elevated region={region} loss_p95={loss_p95}% affected_conns={conns}",
                    "vars": {"region": {"k": "ch", "v": ["ap-northeast-1"]}, "conns": {"k": "i", "v": [1, 50]}},
                    "state_vars": {"n": {"loss_p95": {"k": "i", "v": [0, 2]}}, "f": {"loss_p95": {"k": "i", "v": [10, 70]}}},
                },
                "incident_triage_started": {"lvl": "INFO", "msg": "Incident note: triage started for Direct Connect Tokyo", "vars": {}},
                "incident_devices_drained": {"lvl": "INFO", "msg": "Incident note: manual device drain/isolation performed", "vars": {}},
                "incident_congestion_returned": {"lvl": "INFO", "msg": "Incident note: congestion and loss increased again", "vars": {}},
                "incident_protocol_disable_test_started": {"lvl": "INFO", "msg": "Incident note: started disabling fast convergence protocol in one AZ", "vars": {}},
            },
            "beh": {"n": [{"id": "alarm_packet_loss", "per_min": 0.0, "scope": "global"}], "f": [{"id": "alarm_packet_loss", "per_min": 1.0, "scope": "global"}]},
        },
    },
    "flows": {
        "n": {
            "dx_customer_batch_normal": {
                "rpm": 250.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 6], [2, 8], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_probe_batch_normal": {
                "rpm": 30.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 6], [2, 10], [1, 6]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        },
        "f": {
            "dx_customer_batch_delivered_degraded_az1": {
                "rpm": 110.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 8], [12, 140], [1, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_customer_batch_blackhole_az1": {
                "rpm": 30.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.device_fault"],
                "latency_ms": [[2, 8], [80, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_customer_batch_delivered_degraded_az2": {
                "rpm": 90.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result_fast_az2", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 8], [10, 120], [1, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_customer_batch_blackhole_az2": {
                "rpm": 20.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.device_fault_fast_az2"],
                "latency_ms": [[2, 8], [80, 350]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_customer_batch_delivered_legacy_az2": {
                "rpm": 90.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result_legacy_az2", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 8], [4, 35], [1, 8]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_probe_batch_degraded_az2": {
                "rpm": 20.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result_fast_az2", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 8], [8, 90], [1, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "dx_probe_batch_legacy_az2": {
                "rpm": 20.0,
                "emit": ["dx_edge_pop.edge_forward_batch", "dx_transport_layer.fwd_batch_result_legacy_az2", "tokyo_dc_fabric.ingress_batch"],
                "latency_ms": [[2, 8], [4, 30], [1, 8]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "dx_tokyo_packet_loss_layer3_automation_deferral",
    "time": {
        "total_minutes": 50,
        "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "noc_alerting.alarm_packet_loss": 1.2,
                        "dx_customer_batch_delivered_legacy_az2": 0.0,
                        "dx_probe_batch_legacy_az2": 0.0,
                    },
                    "latency_multipliers": {
                        "dx_customer_batch_delivered_degraded_az1": {"p50": 1.2, "p95": 1.4},
                        "dx_customer_batch_delivered_degraded_az2": {"p50": 1.2, "p95": 1.4},
                        "dx_probe_batch_degraded_az2": {"p50": 1.2, "p95": 1.4},
                    },
                    "one_shots": [{"ref": "noc_alerting.incident_triage_started", "count": 1, "hosts": ["noc-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {
                        "dx_customer_batch_blackhole_az1": 0.5,
                        "dx_customer_batch_blackhole_az2": 0.5,
                        "dx_transport_layer.drop_metric": 0.7,
                        "noc_alerting.alarm_packet_loss": 0.7,
                    },
                    "latency_multipliers": {
                        "dx_customer_batch_delivered_degraded_az1": {"p50": 0.9, "p95": 0.9},
                        "dx_customer_batch_delivered_degraded_az2": {"p50": 0.9, "p95": 0.9},
                        "dx_probe_batch_degraded_az2": {"p50": 0.9, "p95": 0.9},
                    },
                    "one_shots": [
                        {"ref": "dx_health_automation.manual_isolation", "count": 1, "hosts": ["dx-auto-1"]},
                        {"ref": "noc_alerting.incident_devices_drained", "count": 1, "hosts": ["noc-1"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 37,
                    "rate_multipliers": {
                        "dx_customer_batch_blackhole_az1": 1.8,
                        "dx_customer_batch_blackhole_az2": 1.6,
                        "dx_transport_layer.drop_metric": 1.4,
                        "tokyo_dc_fabric.fabric_drop_detected": 1.3,
                        "noc_alerting.alarm_packet_loss": 1.3,
                    },
                    "latency_multipliers": {
                        "dx_customer_batch_delivered_degraded_az1": {"p50": 1.5, "p95": 1.8},
                        "dx_customer_batch_delivered_degraded_az2": {"p50": 1.5, "p95": 1.8},
                        "dx_probe_batch_degraded_az2": {"p50": 1.5, "p95": 1.8},
                    },
                    "one_shots": [
                        {"ref": "dx_health_automation.device_reset_started", "count": 2, "hosts": ["dx-auto-1"]},
                        {"ref": "noc_alerting.incident_congestion_returned", "count": 1, "hosts": ["noc-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 45,
                    "rate_multipliers": {
                        "dx_customer_batch_delivered_degraded_az2": 0.0,
                        "dx_customer_batch_blackhole_az2": 0.0,
                        "dx_probe_batch_degraded_az2": 0.0,
                        "dx_customer_batch_delivered_legacy_az2": 1.0,
                        "dx_probe_batch_legacy_az2": 1.0,
                        "dx_transport_layer.drop_metric": 0.8,
                        "tokyo_dc_fabric.fabric_drop_detected": 0.8,
                        "noc_alerting.alarm_packet_loss": 0.7,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "dx_transport_layer.protocol_disabled", "count": 1, "hosts": ["dx-l3-3"]},
                        {"ref": "noc_alerting.incident_protocol_disable_test_started", "count": 1, "hosts": ["noc-1"]},
                    ],
                },
            ]
        }
    },
}


BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def h_uint64(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big", signed=False)


def h_float01(s: str) -> float:
    return h_uint64(s) / float(2**64)


def h_choice(seq: List[Any], key: str) -> Any:
    if not seq:
        return ""
    idx = int(h_uint64(key) % len(seq))
    return seq[idx]


def gen_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "i":
        a, b = int(v[0]), int(v[1])
        if b <= a:
            return a
        u = h_float01(key)
        return int(a + math.floor(u * (b - a + 1)))
    if k == "f":
        a, b = float(v[0]), float(v[1])
        u = h_float01(key)
        return a + (b - a) * u
    if k == "ch":
        return h_choice(list(v), key)
    if k == "hex":
        n = int(v)
        return md5_hex(key)[:n]
    if k == "uuid":
        hx = md5_hex(key)
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "ip":
        net = ipaddress.ip_network(v, strict=False)
        naddr = int(net.num_addresses)
        if isinstance(net, ipaddress.IPv4Network) and net.prefixlen < 31:
            lo = 1
            hi = naddr - 2
        else:
            lo = 0
            hi = naddr - 1
        if hi < lo:
            lo, hi = 0, naddr - 1
        offset = lo + int(math.floor(h_float01(key) * (hi - lo + 1)))
        return str(net.network_address + offset)
    if k == "str":
        hint = str(v)
        return f"{hint}-{md5_hex(key)[:6]}"
    return ""


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    return base + (1 if h_float01(f"{key}|round") < frac else 0)


def dt_from_minutes(mins: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(mins))


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


def sample_latency_ms(p50: float, p95: float, key: str) -> float:
    if p95 < p50:
        p95 = p50
    u = h_float01(key)
    if u < 0.9:
        q = u / 0.9
        val = p50 + (p95 - p50) * q
    else:
        tail_q = (u - 0.9) / 0.1
        val = p95 * (1.0 + 0.5 * tail_q)
    val *= 1.0 + (h_float01(f"{key}|j") - 0.5) * 0.04
    return max(0.1, float(val))


def schedule_evenly(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_s = max(0.001, (end_dt - start_dt).total_seconds())
    spacing = duration_s / count
    jitter_max = min(0.25, spacing * 0.40)
    out = []
    for i in range(count):
        frac = (i + 0.5) / count
        base = start_dt + timedelta(seconds=duration_s * frac)
        jitter = (h_float01(f"{key}|jit|{i}") - 0.5) * 2.0 * jitter_max
        out.append(base + timedelta(seconds=jitter))
    return out


def parse_ref(ref: str) -> Tuple[str, str]:
    parts = ref.split(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Bad ref: {ref}")
    return parts[0], parts[1]


def get_log_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]["logs"][log_id]


def get_component(component_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]


@dataclass(frozen=True)
class IntervalControls:
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    controls: IntervalControls


def build_failure_intervals() -> List[Interval]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart < e["at_min"] < fend}) + [fend]
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        for ev in events_by_min.get(s, []):
            for k, v in (ev.get("rate_multipliers") or {}).items():
                active_rate[k] = float(v)
            for flow_id, mult in (ev.get("latency_multipliers") or {}).items():
                active_lat[flow_id] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}
        intervals.append(
            Interval(
                state="f",
                start_min=int(s),
                end_min=int(e),
                controls=IntervalControls(rate_mult=dict(active_rate), latency_mult=dict(active_lat)),
            )
        )
    return intervals


def build_normal_intervals() -> List[Interval]:
    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    return [Interval(state="n", start_min=int(nstart), end_min=int(nend), controls=IntervalControls(rate_mult={}, latency_mult={}))]


def choose_host_for_component(component_id: str, key: str, preferred: Optional[str] = None) -> str:
    hosts = get_component(component_id).get("hosts") or []
    if not hosts:
        return ""
    if preferred and preferred in hosts:
        return preferred
    return str(h_choice(hosts, f"{key}|host|{component_id}"))


def render_message(component_id: str, log_id: str, state: str, bound: Dict[str, Any], key: str) -> Tuple[str, str]:
    tmpl = get_log_template(component_id, log_id)
    msg = tmpl["msg"]
    var_defs: Dict[str, Dict[str, Any]] = {}
    var_defs.update(tmpl.get("vars") or {})
    state_vars = (tmpl.get("state_vars") or {}).get(state) or {}
    var_defs.update(state_vars)

    values: Dict[str, Any] = {}
    for name, dom in var_defs.items():
        if name in bound:
            values[name] = bound[name]
        else:
            values[name] = gen_from_domain(dom, f"{key}|{component_id}.{log_id}|{name}")
    try:
        rendered = msg.format(**values)
    except Exception:
        rendered = msg
    level = tmpl["lvl"]
    return level, rendered


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


def flow_az_from_id(flow_id: str, key: str) -> str:
    if "az1" in flow_id:
        return "apne1-az1"
    if "az2" in flow_id:
        return "apne1-az2"
    return "apne1-az1" if h_float01(f"{key}|az") < 0.5 else "apne1-az2"


def device_for_az(az: str, key: str) -> str:
    if az == "apne1-az1":
        return h_choice(["dx-l3-1", "dx-l3-2"], f"{key}|dev|az1")
    return h_choice(["dx-l3-3", "dx-l3-4"], f"{key}|dev|az2")


def severity_from_latency_mult(lat_mult: Dict[str, Dict[str, float]], flow_id: str) -> float:
    m = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    p95m = float(m.get("p95", 1.0))
    sev = 0.25 + 0.75 * ((p95m - 0.9) / 0.9)
    return float(max(0.25, min(1.0, sev)))


def simulate_flow_instance(
    flow_id: str,
    flow_def: Dict[str, Any],
    interval: Interval,
    start_dt: datetime,
    instance_index: int,
) -> List[Dict[str, Any]]:
    state = interval.state
    controls = interval.controls
    key_base = f"flow|{state}|{flow_id}|{interval.start_min}-{interval.end_min}|{instance_index}"
    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow_def.get("trace", False):
        trace_id = md5_hex(f"{key_base}|trace")[:32]

    az = flow_az_from_id(flow_id, key_base)
    conn_id = h_choice(["dxconn-001", "dxconn-002", "dxconn-003", "dxconn-004"], f"{key_base}|conn")
    batch_id = md5_hex(f"{key_base}|batch")[:12]

    bound: Dict[str, Any] = {
        "batch_id": batch_id,
        "conn_id": conn_id,
        "dst_region": "ap-northeast-1",
        "az": az,
    }

    is_blackhole = "blackhole" in flow_id
    is_legacy = "legacy" in flow_id

    device_id = device_for_az(az, key_base)
    bound["device_id"] = device_id

    if state == "n":
        loss_pct = 1 if h_float01(f"{key_base}|loss") < 0.08 else 0
        qdepth = int(5 + loss_pct * 30 + h_float01(f"{key_base}|q") * 10)
        delivered_pct = 100 if loss_pct == 0 else 99
    else:
        if is_blackhole:
            loss_pct = None
            qdepth = None
            delivered_pct = None
        elif is_legacy:
            loss_hi = 8
            loss_pct = int(math.floor(h_float01(f"{key_base}|loss") * (loss_hi + 1)))
            qdepth = int(5 + (loss_pct / 8.0) * 160 + h_float01(f"{key_base}|q") * 20)
            qdepth = clamp_int(qdepth, 0, 200)
            delivered_pct = clamp_int(100 - loss_pct - int(h_float01(f"{key_base}|dnoise") * 2), 40, 100)
        else:
            sev = severity_from_latency_mult(controls.latency_mult, flow_id)
            loss_hi = int(round(sev * 60))
            loss_pct = int(math.floor(h_float01(f"{key_base}|loss") * (loss_hi + 1)))
            qdepth = int(20 + (loss_pct / 60.0) * 780 + h_float01(f"{key_base}|q") * 40)
            qdepth = clamp_int(qdepth, 10, 800)
            delivered_pct = clamp_int(100 - loss_pct - int(h_float01(f"{key_base}|dnoise") * 3), 40, 100)

    if loss_pct is not None:
        bound["loss_pct"] = int(loss_pct)
        bound["qdepth"] = int(qdepth)
        bound["action"] = "congested_forward" if (loss_pct >= 12 or qdepth >= 220) and (not is_legacy) else "forward"
        bound["delivered_pct"] = int(delivered_pct)

    if is_blackhole:
        bound["pkt_sig"] = md5_hex(f"{key_base}|pkt_sig")[:8]
    else:
        bound["bytes"] = gen_from_domain({"k": "i", "v": [20000, 800000]}, f"{key_base}|bytes")

    edge_host = choose_host_for_component("dx_edge_pop", f"{key_base}|edge", None)
    transport_host = choose_host_for_component("dx_transport_layer", f"{key_base}|l3", preferred=device_id)
    fabric_host = choose_host_for_component("tokyo_dc_fabric", f"{key_base}|fabric", None)

    component_host_override = {"dx_edge_pop": edge_host, "dx_transport_layer": transport_host, "tokyo_dc_fabric": fabric_host}

    lat_scale = controls.latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}

    rows: List[Dict[str, Any]] = []
    t = start_dt
    for j, ref in enumerate(flow_def["emit"]):
        comp_id, log_id = parse_ref(ref)
        p50, p95 = flow_def["latency_ms"][j]
        sp50 = float(p50) * float(lat_scale.get("p50", 1.0))
        sp95 = float(p95) * float(lat_scale.get("p95", 1.0))
        dt_ms = sample_latency_ms(sp50, sp95, f"{key_base}|lat|{j}|{comp_id}.{log_id}")
        t = t + timedelta(milliseconds=dt_ms)

        lvl, msg = render_message(comp_id, log_id, state, bound, f"{key_base}|emit|{j}")
        rows.append(
            {
                "timestamp_dt": t,
                "level": lvl,
                "message": msg,
                "trace_id": trace_id,
                "service": get_component(comp_id).get("svc") or "",
                "host": component_host_override.get(comp_id, choose_host_for_component(comp_id, f"{key_base}|{comp_id}")),
            }
        )
    return rows


def simulate_background_for_interval(interval: Interval) -> List[Dict[str, Any]]:
    state = interval.state
    start_dt = dt_from_minutes(interval.start_min)
    end_dt = dt_from_minutes(interval.end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    controls = interval.controls

    rows: List[Dict[str, Any]] = []

    for comp_id, comp in SYSTEM["components"].items():
        beh = (comp.get("beh") or {}).get(state) or []
        for src in beh:
            log_id = src["id"]
            per_min = float(src.get("per_min", 0.0))
            scope = src.get("scope", "per_host")

            if state == "f":
                mult_key = f"{comp_id}.{log_id}"
                per_min *= float(controls.rate_mult.get(mult_key, 1.0))

            if per_min <= 0.0:
                continue

            if scope == "global":
                expected = per_min * duration_min
                count = deterministic_round(expected, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}")
                times = schedule_evenly(start_dt, end_dt, count, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}")
                for i, t in enumerate(times):
                    bound: Dict[str, Any] = {}

                    if comp_id == "tokyo_dc_fabric" and log_id == "fabric_drop_detected":
                        if state == "n":
                            bound["deliv_p95"] = int(98 + math.floor(h_float01(f"fabric|n|{interval.start_min}|{i}") * 3))
                        else:
                            mult = float(controls.rate_mult.get("tokyo_dc_fabric.fabric_drop_detected", 1.0))
                            base = 70 - int((mult - 1.0) * 25)
                            bound["deliv_p95"] = clamp_int(base + int((h_float01(f"fabric|f|{interval.start_min}|{i}") - 0.5) * 18), 30, 95)

                    if comp_id == "noc_alerting" and log_id == "alarm_packet_loss":
                        mult = float(controls.rate_mult.get("noc_alerting.alarm_packet_loss", 1.0)) if state == "f" else 0.0
                        base_loss = 25 + int((mult - 0.7) * 35)
                        bound["loss_p95"] = clamp_int(base_loss + int((h_float01(f"alarm|{interval.start_min}|{i}") - 0.5) * 18), 10, 70)

                    lvl, msg = render_message(comp_id, log_id, state, bound, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{i}")
                    host = choose_host_for_component(comp_id, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{i}")
                    rows.append({"timestamp_dt": t, "level": lvl, "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": host})
            else:
                hosts = comp.get("hosts") or [""]
                for h in hosts:
                    expected = per_min * duration_min
                    count = deterministic_round(expected, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{h}")
                    times = schedule_evenly(start_dt, end_dt, count, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{h}")
                    for i, t in enumerate(times):
                        bound: Dict[str, Any] = {}

                        # Bind per-host identity fields so message semantics match emitting host.
                        # This specifically fixes dx_transport_layer.signature_detected where device_id must match host.
                        tmpl = get_log_template(comp_id, log_id)
                        var_defs: Dict[str, Dict[str, Any]] = {}
                        var_defs.update(tmpl.get("vars") or {})
                        var_defs.update(((tmpl.get("state_vars") or {}).get(state)) or {})
                        if "device_id" in var_defs and h:
                            bound["device_id"] = h

                        if comp_id == "dx_transport_layer" and log_id == "drop_metric":
                            if state == "n":
                                bound["device_id"] = h
                            else:
                                bound["device_id"] = h
                                mult = float(controls.rate_mult.get("dx_transport_layer.drop_metric", 1.0))
                                sev = max(0.4, min(1.3, mult))
                                drop_ppm = int(500 + sev * 18000 + (h_float01(f"drop|{interval.start_min}|{h}|{i}") ** 1.3) * 26000)
                                qd = int(50 + sev * 250 + (h_float01(f"qd|{interval.start_min}|{h}|{i}") ** 1.4) * 800)
                                bound["drop_ppm"] = clamp_int(drop_ppm, 500, 50000)
                                bound["qdepth"] = clamp_int(qd, 50, 1200)

                        if comp_id == "dx_edge_pop" and log_id == "bgp_keepalive":
                            if state == "f":
                                bound["rtt_ms"] = clamp_int(int(8 + h_float01(f"rtt|f|{interval.start_min}|{h}|{i}") * 70), 1, 80)
                            else:
                                bound["rtt_ms"] = clamp_int(int(2 + h_float01(f"rtt|n|{interval.start_min}|{h}|{i}") * 25), 1, 80)

                        lvl, msg = render_message(comp_id, log_id, state, bound, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{h}|{i}")
                        rows.append({"timestamp_dt": t, "level": lvl, "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": h})
    return rows


def simulate_flows_for_interval(interval: Interval) -> List[Dict[str, Any]]:
    state = interval.state
    start_dt = dt_from_minutes(interval.start_min)
    end_dt = dt_from_minutes(interval.end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0

    rows: List[Dict[str, Any]] = []
    flows = SYSTEM["flows"][state]

    for flow_id, flow_def in flows.items():
        rpm = float(flow_def["rpm"])
        if state == "f":
            rpm *= float(interval.controls.rate_mult.get(flow_id, 1.0))
        if rpm <= 0.0:
            continue

        expected_instances = rpm * duration_min
        n_instances = deterministic_round(expected_instances, f"flowinst|{state}|{interval.start_min}-{interval.end_min}|{flow_id}")

        starts = schedule_evenly(start_dt, end_dt, n_instances, f"flowstart|{state}|{interval.start_min}-{interval.end_min}|{flow_id}")
        for idx, st in enumerate(starts):
            rows.extend(simulate_flow_instance(flow_id, flow_def, interval, st, idx))
    return rows


def simulate_one_shots() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    events = list(SCENARIO["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    for ev in events:
        at_min = int(ev["at_min"])
        base_dt = dt_from_minutes(at_min)
        for sidx, shot in enumerate(ev.get("one_shots") or []):
            ref = shot["ref"]
            comp_id, log_id = parse_ref(ref)
            count = int(shot.get("count", 1))
            hosts = shot.get("hosts") or (get_component(comp_id).get("hosts") or [""])
            times = schedule_evenly(base_dt, base_dt + timedelta(seconds=1), count, f"oneshot|{at_min}|{ref}|{sidx}")
            for i, t in enumerate(times):
                host = hosts[i % len(hosts)] if hosts else choose_host_for_component(comp_id, f"oneshot|{at_min}|{ref}|{i}")
                bound: Dict[str, Any] = {}
                lvl, msg = render_message(comp_id, log_id, "f", bound, f"oneshot|{at_min}|{ref}|{i}")
                rows.append({"timestamp_dt": t, "level": lvl, "message": msg, "trace_id": "", "service": get_component(comp_id).get("svc") or "", "host": host})
    return rows


def main() -> None:
    intervals: List[Interval] = []
    intervals.extend(build_normal_intervals())
    intervals.extend(build_failure_intervals())

    all_rows: List[Dict[str, Any]] = []

    for interval in intervals:
        all_rows.extend(simulate_background_for_interval(interval))

    for interval in intervals:
        all_rows.extend(simulate_flows_for_interval(interval))

    all_rows.extend(simulate_one_shots())

    df = pd.DataFrame(all_rows)
    df.sort_values(["timestamp_dt", "service", "host", "level"], inplace=True, kind="mergesort")

    df_out = pd.DataFrame(
        {
            "timestamp": df["timestamp_dt"].apply(fmt_ts),
            "level": df["level"].astype(str),
            "message": df["message"].astype(str),
            "trace_id": df["trace_id"].astype(str),
            "service": df["service"].astype(str),
            "host": df["host"].astype(str),
        }
    )
    df_out = df_out[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df_out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
