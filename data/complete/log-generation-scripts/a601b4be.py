import hashlib
import ipaddress
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "cloudflare_recursive_dns_1111_oct2023",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "recursor_edge": {
            "svc": "edge-recursor",
            "hosts": ["ash01", "fra01", "sin01", "syd01"],
            "logs": {
                "query_rx": {
                    "lvl": "INFO",
                    "msg": "rx query {qname} {qtype} from {client_ip} id={qid} tags={tags}",
                    "vars": {
                        "qname": {"k": "str", "v": "fqdn"},
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS", "DS", "DNSKEY"]},
                        "client_ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "qid": {"k": "i", "v": [0, 65535]},
                        "tags": {"k": "ch", "v": ["none", "rec_disable_static"]},
                    },
                },
                "peer_forward": {
                    "lvl": "INFO",
                    "msg": "forward query {qname} {qtype} to_peer={peer} forwarded_tags={forwarded_tags}",
                    "vars": {
                        "qname": {"k": "str", "v": "fqdn"},
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS", "DS", "DNSKEY"]},
                        "peer": {"k": "ch", "v": ["ash02", "fra02", "sin02"]},
                        "forwarded_tags": {"k": "ch", "v": ["none"]},
                    },
                },
                "root_query": {
                    "lvl": "INFO",
                    "msg": "iter root query {qname} {qtype} server={root_ip} proto=udp timeout_ms={timeout_ms}",
                    "vars": {
                        "qname": {"k": "str", "v": "fqdn"},
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS", "DS", "DNSKEY"]},
                        "root_ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "timeout_ms": {"k": "i", "v": [200, 2000]},
                    },
                },
                "dnssec_validate_failed": {
                    "lvl": "WARN",
                    "msg": "dnssec validation failed root_serial={root_serial} rrset={rrset} err={err} keytag={keytag}",
                    "vars": {
                        "root_serial": {"k": "i", "v": [2023092100, 2023100500]},
                        "rrset": {"k": "ch", "v": ["DNSKEY", "DS", "NS"]},
                        "err": {"k": "ch", "v": ["expired_signature", "bad_signature"]},
                        "keytag": {"k": "i", "v": [0, 65535]},
                    },
                },
                "response_ok": {
                    "lvl": "INFO",
                    "msg": "tx response NOERROR {qname} {qtype} answers={answers} ttl_s={ttl_s} dur_ms={dur_ms}",
                    "vars": {
                        "qname": {"k": "str", "v": "fqdn"},
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS", "DS", "DNSKEY"]},
                        "answers": {"k": "i", "v": [0, 8]},
                        "ttl_s": {"k": "i", "v": [10, 3600]},
                        "dur_ms": {"k": "i", "v": [1, 1500]},
                    },
                },
                "response_servfail": {
                    "lvl": "WARN",
                    "msg": "tx response SERVFAIL {qname} {qtype} reason={reason} dur_ms={dur_ms}",
                    "vars": {
                        "qname": {"k": "str", "v": "fqdn"},
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS", "DS", "DNSKEY"]},
                        "reason": {"k": "ch", "v": ["dnssec_validation_failed", "upstream_timeout", "format_error"]},
                        "dur_ms": {"k": "i", "v": [1, 1500]},
                    },
                },
                "stats_minute": {
                    "lvl": "INFO",
                    "msg": "stats pop={pop} qps={qps} servfail_pct={servfail_pct} stale_root_pct={stale_root_pct}",
                    "vars": {"pop": {"k": "ch", "v": ["ash", "fra", "sin", "syd"]}, "qps": {"k": "i", "v": [10000, 60000]}},
                    "state_vars": {
                        "n": {"servfail_pct": {"k": "f", "v": [2.0, 5.0]}, "stale_root_pct": {"k": "f", "v": [0.0, 5.0]}},
                        "f": {"servfail_pct": {"k": "f", "v": [8.0, 18.0]}, "stale_root_pct": {"k": "f", "v": [20.0, 90.0]}},
                    },
                },
                "override_config_applied": {
                    "lvl": "INFO",
                    "msg": "override rules updated rule_id={rule_id} tag=rec_disable_static phase=pre-cache",
                    "vars": {"rule_id": {"k": "str", "v": "ovr-20231004-###"}},
                },
            },
            "beh": {
                "n": [{"id": "stats_minute", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "stats_minute", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "static_zone": {
            "svc": "static-zone",
            "hosts": ["ash01", "fra01", "sin01", "syd01"],
            "logs": {
                "static_answer": {
                    "lvl": "INFO",
                    "msg": "static_zone served root zone serial={serial} stale={stale}",
                    "vars": {"serial": {"k": "i", "v": [2023092100, 2023100500]}},
                    "state_vars": {"n": {"stale": {"k": "ch", "v": ["false"]}}, "f": {"stale": {"k": "ch", "v": ["true"]}}},
                },
                "bypass_due_to_tag": {
                    "lvl": "INFO",
                    "msg": "static_zone bypassed due to tag {tag}",
                    "vars": {"tag": {"k": "ch", "v": ["rec_disable_static"]}},
                },
                "zone_parse_error": {
                    "lvl": "ERROR",
                    "msg": "failed to load new root zone serial={serial} err=unknown_rrtype rrtype={rrtype}",
                    "vars": {"serial": {"k": "i", "v": [2023092100, 2023100500]}, "rrtype": {"k": "ch", "v": ["ZONEMD"]}},
                },
                "zone_status_metric": {
                    "lvl": "INFO",
                    "msg": "root zone status serial={serial} age_s={age_s} sig_expires_in_s={sig_expires_in_s} in_memory=true",
                    "vars": {"serial": {"k": "i", "v": [2023092100, 2023100500]}},
                    "state_vars": {
                        "n": {"age_s": {"k": "i", "v": [0, 200000]}, "sig_expires_in_s": {"k": "i", "v": [3600, 200000]}},
                        "f": {"age_s": {"k": "i", "v": [800000, 1400000]}, "sig_expires_in_s": {"k": "i", "v": [-7200, 3600]}},
                    },
                },
                "signature_expired": {
                    "lvl": "ERROR",
                    "msg": "root zone DNSSEC signatures expired for serial={serial}",
                    "vars": {"serial": {"k": "i", "v": [2023092100, 2023100500]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "zone_status_metric", "per_min": 0.5, "scope": "per_host"},
                    {"id": "zone_parse_error", "per_min": 0.03, "scope": "per_host"},
                ],
                "f": [
                    {"id": "zone_status_metric", "per_min": 0.5, "scope": "per_host"},
                    {"id": "zone_parse_error", "per_min": 0.03, "scope": "per_host"},
                ],
            },
        },
        "root_zone_pipeline": {
            "svc": "root-zone-pipeline",
            "hosts": ["core01"],
            "logs": {
                "root_zone_published": {
                    "lvl": "INFO",
                    "msg": "published root zone serial={serial} includes_rrtype={includes_rrtype}",
                    "vars": {"serial": {"k": "i", "v": [2023092100, 2023100500]}, "includes_rrtype": {"k": "ch", "v": ["ZONEMD"]}},
                }
            },
            "beh": {"n": [{"id": "root_zone_published", "per_min": 0.05, "scope": "global"}], "f": [{"id": "root_zone_published", "per_min": 0.05, "scope": "global"}]},
        },
        "override_service": {
            "svc": "override-rules",
            "hosts": ["cfg01"],
            "logs": {
                "override_rule_deployed": {
                    "lvl": "INFO",
                    "msg": "deployed override rule_id={rule_id} tag=rec_disable_static phase=pre-cache",
                    "vars": {"rule_id": {"k": "str", "v": "ovr-20231004-###"}},
                }
            },
            "beh": {"n": [], "f": []},
        },
        "dns_root_servers": {
            "svc": None,
            "hosts": ["root"],
            "logs": {
                "root_response": {
                    "lvl": "DEBUG",
                    "msg": "root response rcode={rcode} rtt_ms={rtt_ms}",
                    "vars": {"rcode": {"k": "ch", "v": ["NOERROR"]}, "rtt_ms": {"k": "i", "v": [1, 80]}},
                }
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": {
            "dns_query_noerror_static": {
                "rpm": 420.0,
                "emit": ["recursor_edge.query_rx", "static_zone.static_answer", "recursor_edge.response_ok"],
                "latency_ms": [[0.2, 1.0], [0.4, 2.0], [3.0, 25.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "dns_query_noerror_root_fallback": {
                "rpm": 30.0,
                "emit": ["recursor_edge.query_rx", "recursor_edge.root_query", "dns_root_servers.root_response", "recursor_edge.response_ok"],
                "latency_ms": [[0.2, 1.0], [2.0, 15.0], [10.0, 70.0], [12.0, 90.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "dns_query_servfail_baseline": {
                "rpm": 20.0,
                "emit": ["recursor_edge.query_rx", "recursor_edge.response_servfail"],
                "latency_ms": [[0.2, 1.0], [20.0, 250.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        },
        "f": {
            "dns_query_noerror_static": {
                "rpm": 400.0,
                "emit": ["recursor_edge.query_rx", "static_zone.static_answer", "recursor_edge.response_ok"],
                "latency_ms": [[0.3, 1.5], [0.6, 3.5], [4.0, 35.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "dns_query_servfail_dnssec_direct": {
                "rpm": 50.0,
                "emit": ["recursor_edge.query_rx", "static_zone.static_answer", "recursor_edge.dnssec_validate_failed", "recursor_edge.response_servfail"],
                "latency_ms": [[0.3, 1.5], [0.6, 4.0], [1.0, 10.0], [10.0, 120.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "dns_query_servfail_dnssec_forwarded": {
                "rpm": 20.0,
                "emit": [
                    "recursor_edge.query_rx",
                    "recursor_edge.peer_forward",
                    "static_zone.static_answer",
                    "recursor_edge.dnssec_validate_failed",
                    "recursor_edge.response_servfail",
                ],
                "latency_ms": [[0.3, 1.5], [1.0, 8.0], [0.6, 4.5], [1.0, 12.0], [12.0, 160.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "dns_query_noerror_root_fallback_tagged": {
                "rpm": 40.0,
                "emit": [
                    "recursor_edge.query_rx",
                    "static_zone.bypass_due_to_tag",
                    "recursor_edge.root_query",
                    "dns_root_servers.root_response",
                    "recursor_edge.response_ok",
                ],
                "latency_ms": [[0.3, 1.5], [0.5, 3.0], [3.0, 20.0], [12.0, 90.0], [15.0, 110.0]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "dns_servfail_stale_root_zone_oct4_2023",
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 30}, "f": {"start_min": 30, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 30,
                    "rate_multipliers": {"dns_query_noerror_root_fallback_tagged": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "static_zone.signature_expired", "count": 1, "hosts": ["ash01"]},
                        {"ref": "static_zone.signature_expired", "count": 1, "hosts": ["fra01"]},
                        {"ref": "static_zone.signature_expired", "count": 1, "hosts": ["sin01"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 39,
                    "rate_multipliers": {
                        "dns_query_noerror_static": 0.98,
                        "dns_query_servfail_dnssec_direct": 1.15,
                        "dns_query_servfail_dnssec_forwarded": 1.2,
                    },
                    "latency_multipliers": {
                        "dns_query_servfail_dnssec_direct": {"p50": 1.05, "p95": 1.2},
                        "dns_query_servfail_dnssec_forwarded": {"p50": 1.05, "p95": 1.25},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 55,
                    "rate_multipliers": {
                        "dns_query_noerror_root_fallback_tagged": 1.0,
                        "dns_query_noerror_static": 0.82,
                        "dns_query_servfail_dnssec_direct": 0.65,
                        "dns_query_servfail_dnssec_forwarded": 1.25,
                    },
                    "latency_multipliers": {"dns_query_noerror_root_fallback_tagged": {"p50": 2.0, "p95": 2.5}},
                    "one_shots": [
                        {"ref": "override_service.override_rule_deployed", "count": 1, "hosts": ["cfg01"]},
                        {"ref": "recursor_edge.override_config_applied", "count": 1, "hosts": ["ash01"]},
                        {"ref": "recursor_edge.override_config_applied", "count": 1, "hosts": ["fra01"]},
                        {"ref": "recursor_edge.override_config_applied", "count": 1, "hosts": ["sin01"]},
                        {"ref": "recursor_edge.override_config_applied", "count": 1, "hosts": ["syd01"]},
                    ],
                },
            ]
        }
    },
}

BASE_TIME = datetime(2023, 10, 4, 0, 0, 0, tzinfo=timezone.utc)
SEED = "simseed:v3:cloudflare_recursive_dns"


def _hfloat(*parts: Any) -> float:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5((SEED + "|" + s).encode("utf-8")).hexdigest()
    return int(h[:12], 16) / float(16**12)


def _hhex(n: int, *parts: Any) -> str:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5((SEED + "|" + s).encode("utf-8")).hexdigest()
    out = (h * ((n // len(h)) + 1))[:n]
    return out.lower()


def _choose(seq: List[Any], *parts: Any) -> Any:
    if not seq:
        return None
    u = _hfloat(*parts)
    idx = int(u * len(seq))
    if idx == len(seq):
        idx = len(seq) - 1
    return seq[idx]


def _rand_int(a: int, b: int, *parts: Any) -> int:
    if b < a:
        a, b = b, a
    u = _hfloat(*parts)
    return a + int(u * (b - a + 1))


def _rand_float(a: float, b: float, *parts: Any) -> float:
    u = _hfloat(*parts)
    return a + u * (b - a)


def _ip_from_cidr(cidr: str, *parts: Any) -> str:
    if cidr == "0.0.0.0/0":
        base = int(ipaddress.IPv4Address("203.0.113.0"))
        host = _rand_int(1, 254, "client_ip", *parts)
        return str(ipaddress.IPv4Address(base + host))
    net = ipaddress.ip_network(cidr, strict=False)
    size = net.num_addresses
    if size <= 2:
        return str(net.network_address)
    offset = _rand_int(1, size - 2, "ip", *parts)
    return str(net.network_address + offset)


def _fmt_val(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.1f}"
    return str(v)


def _render(msg: str, values: Dict[str, Any]) -> str:
    vals = {k: _fmt_val(v) for k, v in values.items()}
    return msg.format(**vals)


def _sample_latency_ms(p50: float, p95: float, *parts: Any) -> float:
    if p50 <= 0:
        p50 = 0.01
    if p95 < p50:
        p95 = p50
    u = _hfloat("lat", *parts)
    t = u**1.8
    return p50 * ((p95 / p50) ** t)


def _bounded_delay_for_var(delay_ms: float, var_spec: Dict[str, Any]) -> float:
    k = var_spec.get("k")
    if k == "i":
        a, b = var_spec["v"]
        return float(min(max(delay_ms, a), b))
    return delay_ms


def _minute_dt(minute: int, seconds: float = 0.0) -> datetime:
    return BASE_TIME + timedelta(minutes=minute, seconds=seconds)


def _iso_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _component_identity(comp_id: str) -> Tuple[str, List[str]]:
    comp = SYSTEM["components"][comp_id]
    svc = comp["svc"] if comp.get("svc") else ""
    hosts = comp.get("hosts") or []
    return svc, hosts


def _get_log_template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def _state_for_minute(minute: int) -> str:
    return "n" if minute < SCENARIO["time"]["phases"]["n"]["end_min"] else "f"


def _active_controls_for_minute(minute: int) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}
    if minute < SCENARIO["time"]["phases"]["f"]["start_min"]:
        return rate_mult, lat_mult

    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        if ev["at_min"] <= minute:
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        else:
            break
    return rate_mult, lat_mult


@dataclass
class FlowInstance:
    state: str
    flow_id: str
    start_time: datetime
    idx: int
    rate_mult: float
    lat_mult: Dict[str, float]


class FractionalAllocator:
    def __init__(self, key: str):
        self.key = key
        self.carry = 0.0

    def alloc(self, expected: float, minute: int) -> int:
        x = expected + self.carry
        base = int(math.floor(x))
        frac = x - base
        add = 1 if _hfloat("round", self.key, minute) < frac else 0
        out = base + add
        self.carry = frac - add
        if self.carry > 1.0:
            self.carry -= math.floor(self.carry)
        if self.carry < -1.0:
            self.carry += math.ceil(-self.carry)
        return max(0, out)


def _host_pop(host: str) -> str:
    return host[:3]


def _pick_flow_host(flow: FlowInstance, comp_id: str) -> str:
    _svc, hosts = _component_identity(comp_id)
    if not hosts:
        return ""
    if comp_id in ("recursor_edge", "static_zone"):
        rec_hosts = SYSTEM["components"]["recursor_edge"]["hosts"]
        rec_host = rec_hosts[flow.idx % len(rec_hosts)]
        if rec_host in hosts:
            return rec_host
    return hosts[flow.idx % len(hosts)]


def _gen_qname(flow: FlowInstance) -> str:
    n = (flow.idx * 7 + _rand_int(0, 199, "qname", flow.state, flow.flow_id, flow.idx)) % 200
    labels = ["www", "api", "mail", "ns1", "cdn", "img", "edge", "auth", "db", "gw"]
    l = _choose(labels, "qname_label", flow.state, flow.flow_id, n)
    return f"{l}{n}.example.net"


def _gen_qtype(flow: FlowInstance) -> str:
    return _choose(["A", "AAAA", "NS", "DS", "DNSKEY"], "qtype", flow.state, flow.flow_id, flow.idx)


def _gen_client_ip(flow: FlowInstance) -> str:
    return _ip_from_cidr("0.0.0.0/0", flow.state, flow.flow_id, flow.idx)


def _gen_qid(flow: FlowInstance) -> int:
    return _rand_int(0, 65535, "qid", flow.state, flow.flow_id, flow.idx)


def _flow_tags(flow_id: str) -> str:
    if flow_id == "dns_query_noerror_root_fallback_tagged":
        return "rec_disable_static"
    return "none"


def _answers_for_qtype(qtype: str, flow: FlowInstance) -> int:
    if qtype in ("A", "AAAA"):
        return 1 + (_rand_int(0, 2, "ans", qtype, flow.state, flow.flow_id, flow.idx))
    if qtype == "NS":
        return 2 + (_rand_int(0, 3, "ans", qtype, flow.state, flow.flow_id, flow.idx))
    if qtype in ("DS", "DNSKEY"):
        return 0 + (_rand_int(0, 2, "ans", qtype, flow.state, flow.flow_id, flow.idx))
    return 0


def _ttl_for_flow(flow: FlowInstance) -> int:
    return _rand_int(30, 3600, "ttl", flow.state, flow.flow_id, flow.idx)


def _root_ip(_flow: FlowInstance) -> str:
    return "198.41.0.4"


def _timeout_ms(flow: FlowInstance) -> int:
    return int(1000 + 250 * (_hfloat("timeout", flow.state, flow.flow_id, flow.idx) - 0.5))


def _serial_for_static_answer(state: str) -> int:
    return 2023100500 if state == "n" else 2023092100


def _emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"timestamp": ts, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def _simulate_flow_instance(flow: FlowInstance, rows: List[Dict[str, Any]]) -> None:
    fdef = SYSTEM["flows"][flow.state][flow.flow_id]
    emit_refs = fdef["emit"]
    latency_pairs = fdef["latency_ms"]

    qname = _gen_qname(flow)
    qtype = _gen_qtype(flow)
    client_ip = _gen_client_ip(flow)
    qid = _gen_qid(flow)
    tags = _flow_tags(flow.flow_id)
    base_ctx = {"qname": qname, "qtype": qtype, "client_ip": client_ip, "qid": qid, "tags": tags}

    static_serial = _serial_for_static_answer(flow.state)
    dnssec_root_serial = 2023092100 if flow.state == "f" else 2023100500

    delays_ms: List[float] = []
    for j, (p50, p95) in enumerate(latency_pairs):
        p50s = p50 * flow.lat_mult.get("p50", 1.0)
        p95s = p95 * flow.lat_mult.get("p95", 1.0)
        d = _sample_latency_ms(p50s, p95s, flow.state, flow.flow_id, flow.idx, j)

        comp_id, log_id = emit_refs[j].split(".", 1)
        tmpl = _get_log_template(comp_id, log_id)
        if comp_id == "dns_root_servers" and log_id == "root_response":
            d = _bounded_delay_for_var(d, tmpl["vars"]["rtt_ms"])
        delays_ms.append(d)

    trace_id = ""
    t = flow.start_time
    ts_query_rx: Optional[datetime] = None

    for j, ref in enumerate(emit_refs):
        comp_id, log_id = ref.split(".", 1)
        tmpl = _get_log_template(comp_id, log_id)
        svc, _ = _component_identity(comp_id)
        host = _pick_flow_host(flow, comp_id)

        t = t + timedelta(milliseconds=delays_ms[j])
        values: Dict[str, Any] = dict(base_ctx)

        if comp_id == "static_zone" and log_id == "static_answer":
            values["serial"] = static_serial
            stale_spec = tmpl.get("state_vars", {}).get(flow.state, {}).get("stale")
            values["stale"] = _choose(stale_spec["v"], "stale", flow.state, flow.flow_id, flow.idx) if stale_spec else "false"
        elif comp_id == "static_zone" and log_id == "bypass_due_to_tag":
            values["tag"] = "rec_disable_static"
        elif comp_id == "recursor_edge" and log_id == "peer_forward":
            values["peer"] = _choose(["ash02", "fra02", "sin02"], "peer", flow.state, flow.flow_id, flow.idx)
            values["forwarded_tags"] = "none"
            values["tags"] = "none"
        elif comp_id == "recursor_edge" and log_id == "root_query":
            values["root_ip"] = _root_ip(flow)
            values["timeout_ms"] = int(min(max(_timeout_ms(flow), 200), 2000))
        elif comp_id == "dns_root_servers" and log_id == "root_response":
            values["rcode"] = "NOERROR"
            values["rtt_ms"] = int(round(delays_ms[j]))
        elif comp_id == "recursor_edge" and log_id == "dnssec_validate_failed":
            values["root_serial"] = dnssec_root_serial
            values["rrset"] = _choose(["DNSKEY", "DS", "NS"], "rrset", flow.state, flow.flow_id, flow.idx)
            if flow.state == "f":
                values["err"] = "expired_signature" if _hfloat("dnssec_err", flow.flow_id, flow.idx) < 0.92 else "bad_signature"
            else:
                values["err"] = _choose(["expired_signature", "bad_signature"], "dnssec_err", flow.state, flow.flow_id, flow.idx)
            values["keytag"] = _rand_int(0, 65535, "keytag", flow.state, flow.flow_id, flow.idx)
        elif comp_id == "recursor_edge" and log_id == "response_ok":
            values["answers"] = int(min(max(_answers_for_qtype(qtype, flow), 0), 8))
            values["ttl_s"] = int(min(max(_ttl_for_flow(flow), 10), 3600))
            dur_ms = int(round((t - ts_query_rx).total_seconds() * 1000.0)) if ts_query_rx is not None else int(round(sum(delays_ms[: j + 1])))
            values["dur_ms"] = int(min(max(dur_ms, 1), 1500))
        elif comp_id == "recursor_edge" and log_id == "response_servfail":
            if flow.flow_id in ("dns_query_servfail_dnssec_direct", "dns_query_servfail_dnssec_forwarded"):
                values["reason"] = "dnssec_validation_failed"
            elif flow.flow_id == "dns_query_servfail_baseline":
                values["reason"] = _choose(["upstream_timeout", "format_error"], "sf_reason", flow.state, flow.flow_id, flow.idx)
            else:
                values["reason"] = _choose(["dnssec_validation_failed", "upstream_timeout", "format_error"], "sf_reason", flow.state, flow.flow_id, flow.idx)
            dur_ms = int(round((t - ts_query_rx).total_seconds() * 1000.0)) if ts_query_rx is not None else int(round(sum(delays_ms[: j + 1])))
            values["dur_ms"] = int(min(max(dur_ms, 1), 1500))
        elif comp_id == "recursor_edge" and log_id == "query_rx":
            allowed = SYSTEM["components"]["recursor_edge"]["logs"]["query_rx"]["vars"]["tags"]["v"]
            values["tags"] = tags if tags in allowed else "none"
            ts_query_rx = t

        for k, spec in tmpl.get("vars", {}).items():
            if k not in values:
                if spec["k"] == "i":
                    a, b = spec["v"]
                    values[k] = _rand_int(int(a), int(b), "var", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "f":
                    a, b = spec["v"]
                    values[k] = _rand_float(float(a), float(b), "var", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "ch":
                    values[k] = _choose(list(spec["v"]), "var", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "hex":
                    values[k] = _hhex(int(spec["v"]), "var", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "uuid":
                    h = _hhex(32, "uuid", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                    values[k] = f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
                elif spec["k"] == "ip":
                    values[k] = _ip_from_cidr(spec["v"], "var", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "str":
                    values[k] = _gen_qname(flow) if spec["v"] == "fqdn" else f"{spec['v']}"
                else:
                    values[k] = ""
        for k, spec in tmpl.get("state_vars", {}).get(flow.state, {}).items():
            if k not in values:
                if spec["k"] == "i":
                    a, b = spec["v"]
                    values[k] = _rand_int(int(a), int(b), "svar", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "f":
                    a, b = spec["v"]
                    values[k] = _rand_float(float(a), float(b), "svar", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)
                elif spec["k"] == "ch":
                    values[k] = _choose(list(spec["v"]), "svar", comp_id, log_id, k, flow.state, flow.flow_id, flow.idx)

        msg = _render(tmpl["msg"], values)
        _emit_row(rows, t, tmpl["lvl"], msg, trace_id, svc, host)


def _simulate_background_minute(minute: int, rows: List[Dict[str, Any]], bg_allocators: Dict[Tuple[str, str, str], FractionalAllocator]) -> None:
    state = _state_for_minute(minute)
    rate_mult, _lat_mult = _active_controls_for_minute(minute)

    for comp_id, comp in SYSTEM["components"].items():
        emits = comp.get("beh", {}).get(state, [])
        if not emits:
            continue
        svc, hosts = _component_identity(comp_id)

        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            tmpl = _get_log_template(comp_id, log_id)

            key_global = f"{comp_id}.{log_id}"
            mult = float(rate_mult.get(key_global, 1.0)) if state == "f" else 1.0
            eff = per_min * mult

            if scope == "global":
                host_list = [""] if not hosts else [hosts[0]]
                for host in host_list:
                    alloc_key = (comp_id, log_id, f"global:{host}")
                    if alloc_key not in bg_allocators:
                        bg_allocators[alloc_key] = FractionalAllocator(f"bg|{comp_id}|{log_id}|{host}")
                    c = bg_allocators[alloc_key].alloc(eff, minute)
                    if c <= 0:
                        continue
                    for i in range(c):
                        base_off = (i + 0.5) / c * 60.0
                        jitter = (_hfloat("bg_jit", comp_id, log_id, host, minute, i) - 0.5) * 0.4
                        ts = _minute_dt(minute, seconds=max(0.0, min(59.999, base_off + jitter)))

                        values: Dict[str, Any] = {}
                        for k, spec in tmpl.get("vars", {}).items():
                            if spec["k"] == "i":
                                a, b = spec["v"]
                                values[k] = _rand_int(int(a), int(b), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "f":
                                a, b = spec["v"]
                                values[k] = _rand_float(float(a), float(b), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ch":
                                values[k] = _choose(list(spec["v"]), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ip":
                                values[k] = _ip_from_cidr(spec["v"], "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "str":
                                values[k] = f"bg{minute}-{i}.example.net" if spec["v"] == "fqdn" else spec["v"]
                        for k, spec in tmpl.get("state_vars", {}).get(state, {}).items():
                            if spec["k"] == "i":
                                a, b = spec["v"]
                                values[k] = _rand_int(int(a), int(b), "bgsvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "f":
                                a, b = spec["v"]
                                values[k] = _rand_float(float(a), float(b), "bgsvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ch":
                                values[k] = _choose(list(spec["v"]), "bgsvar", comp_id, log_id, k, host, minute, i)

                        if comp_id == "recursor_edge" and log_id == "stats_minute":
                            pop = _host_pop(host) if host else _choose(["ash", "fra", "sin", "syd"], "pop", minute, i)
                            if pop not in ["ash", "fra", "sin", "syd"]:
                                pop = _choose(["ash", "fra", "sin", "syd"], "pop2", minute, i)
                            values["pop"] = pop
                            values["qps"] = int(_rand_int(10000, 60000, "qps", pop, minute))
                            if state == "n":
                                values["servfail_pct"] = float(_rand_float(2.0, 5.0, "sf", pop, minute))
                                values["stale_root_pct"] = float(_rand_float(0.0, 5.0, "stale", pop, minute))
                            else:
                                u = _hfloat("sf_drift", pop, minute)
                                values["servfail_pct"] = 8.0 + (10.0 * u)
                                v = _hfloat("stale_drift", pop, minute)
                                values["stale_root_pct"] = 20.0 + (70.0 * v)

                        if comp_id == "static_zone" and log_id == "zone_status_metric":
                            values["serial"] = 2023100500 if state == "n" else 2023092100
                        if comp_id == "static_zone" and log_id == "zone_parse_error":
                            values["serial"] = 2023100500
                            values["rrtype"] = "ZONEMD"
                        if comp_id == "root_zone_pipeline" and log_id == "root_zone_published":
                            values["serial"] = 2023100500
                            values["includes_rrtype"] = "ZONEMD"

                        msg = _render(tmpl["msg"], values)
                        _emit_row(rows, ts, tmpl["lvl"], msg, "", svc, host)
            else:
                for host in (hosts or [""]):
                    alloc_key = (comp_id, log_id, f"host:{host}")
                    if alloc_key not in bg_allocators:
                        bg_allocators[alloc_key] = FractionalAllocator(f"bg|{comp_id}|{log_id}|{host}")
                    c = bg_allocators[alloc_key].alloc(eff, minute)
                    if c <= 0:
                        continue
                    for i in range(c):
                        base_off = (i + 0.5) / c * 60.0
                        jitter = (_hfloat("bg_jit", comp_id, log_id, host, minute, i) - 0.5) * 0.4
                        ts = _minute_dt(minute, seconds=max(0.0, min(59.999, base_off + jitter)))

                        values: Dict[str, Any] = {}
                        for k, spec in tmpl.get("vars", {}).items():
                            if spec["k"] == "i":
                                a, b = spec["v"]
                                values[k] = _rand_int(int(a), int(b), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "f":
                                a, b = spec["v"]
                                values[k] = _rand_float(float(a), float(b), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ch":
                                values[k] = _choose(list(spec["v"]), "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ip":
                                values[k] = _ip_from_cidr(spec["v"], "bgvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "str":
                                values[k] = f"bg{minute}-{i}.example.net" if spec["v"] == "fqdn" else spec["v"]
                        for k, spec in tmpl.get("state_vars", {}).get(state, {}).items():
                            if spec["k"] == "i":
                                a, b = spec["v"]
                                values[k] = _rand_int(int(a), int(b), "bgsvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "f":
                                a, b = spec["v"]
                                values[k] = _rand_float(float(a), float(b), "bgsvar", comp_id, log_id, k, host, minute, i)
                            elif spec["k"] == "ch":
                                values[k] = _choose(list(spec["v"]), "bgsvar", comp_id, log_id, k, host, minute, i)

                        if comp_id == "recursor_edge" and log_id == "stats_minute":
                            pop = _host_pop(host) if host else _choose(["ash", "fra", "sin", "syd"], "pop", minute, i)
                            if pop not in ["ash", "fra", "sin", "syd"]:
                                pop = _choose(["ash", "fra", "sin", "syd"], "pop2", minute, i)
                            values["pop"] = pop
                            values["qps"] = int(_rand_int(10000, 60000, "qps", pop, minute))
                            if state == "n":
                                values["servfail_pct"] = float(_rand_float(2.0, 5.0, "sf", pop, minute))
                                values["stale_root_pct"] = float(_rand_float(0.0, 5.0, "stale", pop, minute))
                            else:
                                u = _hfloat("sf_drift", pop, minute)
                                values["servfail_pct"] = 8.0 + (10.0 * u)
                                v = _hfloat("stale_drift", pop, minute)
                                values["stale_root_pct"] = 20.0 + (70.0 * v)

                        if comp_id == "static_zone" and log_id == "zone_status_metric":
                            values["serial"] = 2023100500 if state == "n" else 2023092100
                        if comp_id == "static_zone" and log_id == "zone_parse_error":
                            values["serial"] = 2023100500
                            values["rrtype"] = "ZONEMD"

                        msg = _render(tmpl["msg"], values)
                        _emit_row(rows, ts, tmpl["lvl"], msg, "", svc, host)


def _emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    rule_id = "ovr-20231004-001"
    for ev in SCENARIO["phases"]["f"]["events"]:
        at_min = int(ev["at_min"])
        for shot in ev.get("one_shots", []):
            ref = shot["ref"]
            comp_id, log_id = ref.split(".", 1)
            tmpl = _get_log_template(comp_id, log_id)
            svc, _hosts = _component_identity(comp_id)
            hosts = shot.get("hosts") or (SYSTEM["components"][comp_id].get("hosts") or [""])
            count = int(shot["count"])

            for i in range(count):
                host = hosts[i % len(hosts)] if hosts else ""
                base_off = 0.8 + 0.25 * i
                jitter = (_hfloat("oneshot_jit", ref, at_min, host, i) - 0.5) * 0.4
                ts = _minute_dt(at_min, seconds=max(0.0, min(59.999, base_off + jitter)))

                values: Dict[str, Any] = {}
                for k, spec in tmpl.get("vars", {}).items():
                    if k == "rule_id":
                        values[k] = rule_id
                        continue
                    if comp_id == "static_zone" and log_id == "signature_expired" and k == "serial":
                        values[k] = 2023092100
                        continue
                    if spec["k"] == "i":
                        a, b = spec["v"]
                        values[k] = _rand_int(int(a), int(b), "oneshot", ref, k, at_min, host, i)
                    elif spec["k"] == "f":
                        a, b = spec["v"]
                        values[k] = _rand_float(float(a), float(b), "oneshot", ref, k, at_min, host, i)
                    elif spec["k"] == "ch":
                        values[k] = _choose(list(spec["v"]), "oneshot", ref, k, at_min, host, i)
                    elif spec["k"] == "ip":
                        values[k] = _ip_from_cidr(spec["v"], "oneshot", ref, k, at_min, host, i)
                    elif spec["k"] == "str":
                        values[k] = rule_id if spec["v"] == "ovr-20231004-###" else spec["v"]
                msg = _render(tmpl["msg"], values)
                _emit_row(rows, ts, tmpl["lvl"], msg, "", svc, host)


def main() -> None:
    # Required reproducibility hooks (even though generation is hash-based).
    random.seed(0)
    np.random.seed(0)

    total_minutes = int(SCENARIO["time"]["total_minutes"])

    rows: List[Dict[str, Any]] = []
    bg_allocators: Dict[Tuple[str, str, str], FractionalAllocator] = {}

    flow_allocators: Dict[Tuple[str, str], FractionalAllocator] = {}
    flow_counters: Dict[Tuple[str, str], int] = {}

    for minute in range(total_minutes):
        state = _state_for_minute(minute)
        rate_mult, lat_mult = _active_controls_for_minute(minute)

        _simulate_background_minute(minute, rows, bg_allocators)

        flows = SYSTEM["flows"][state]
        for flow_id, fdef in flows.items():
            base_rpm = float(fdef["rpm"])
            mult = float(rate_mult.get(flow_id, 1.0)) if state == "f" else 1.0
            eff_rpm = base_rpm * mult

            alloc_key = (state, flow_id)
            if alloc_key not in flow_allocators:
                flow_allocators[alloc_key] = FractionalAllocator(f"flow|{state}|{flow_id}")
                flow_counters[alloc_key] = 0

            count = flow_allocators[alloc_key].alloc(eff_rpm, minute)
            if count <= 0:
                continue

            lmult = {"p50": 1.0, "p95": 1.0}
            if state == "f" and flow_id in lat_mult:
                lmult = lat_mult[flow_id]

            for i in range(count):
                idx = flow_counters[alloc_key]
                flow_counters[alloc_key] += 1

                base_off = (i + 0.5) / count * 60.0
                jitter = (_hfloat("flow_jit", state, flow_id, minute, i) - 0.5) * 0.25
                start = _minute_dt(minute, seconds=max(0.0, min(59.999, base_off + jitter)))

                inst = FlowInstance(state=state, flow_id=flow_id, start_time=start, idx=idx, rate_mult=mult, lat_mult=lmult)
                _simulate_flow_instance(inst, rows)

    _emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df.sort_values(by=["timestamp", "service", "host", "level", "message"], inplace=True, kind="mergesort")

    df["timestamp"] = df["timestamp"].map(_iso_ms)
    df["trace_id"] = df["trace_id"].fillna("").astype(str)
    df["service"] = df["service"].fillna("").astype(str)
    df["host"] = df["host"].fillna("").astype(str)
    df["level"] = df["level"].astype(str)
    df["message"] = df["message"].astype(str)

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
