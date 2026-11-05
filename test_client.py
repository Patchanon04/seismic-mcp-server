#!/usr/bin/env python3
"""Smoke-test the Seismic MCP server over Streamable HTTP. Uses only the standard library.

    python3 test_client.py                       # http://localhost:8000/mcp
    python3 test_client.py http://host:8000/mcp  # another URL
    MCP_AUTH_TOKEN=xxx python3 test_client.py    # when the server requires a token
"""
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, timedelta

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/mcp"
TOKEN = os.getenv("MCP_AUTH_TOKEN")
session_id = None
protocol_version = None
_next_id = 0


def post(payload, expect_body=True):
    global session_id
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if protocol_version:
        headers["MCP-Protocol-Version"] = protocol_version
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(URL, data=json.dumps(payload).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            session_id = resp.headers.get("Mcp-Session-Id") or session_id
            body = resp.read().decode()
            return resp.status, (json.loads(body) if expect_body and body else None)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "null")


def rpc(method, params=None):
    global _next_id
    _next_id += 1
    status, body = post({"jsonrpc": "2.0", "id": _next_id, "method": method, "params": params or {}})
    return status, body


def call(name, arguments):
    status, body = rpc("tools/call", {"name": name, "arguments": arguments})
    if "error" in body:
        print(f"  ✗ {name}: HTTP {status} error {body['error']['code']}: {body['error']['message']}")
        return None
    return body["result"]["structuredContent"]


def main():
    global protocol_version
    print(f"→ {URL}")
    status, body = rpc("initialize", {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "smoke-test", "version": "1.0"},
    })
    assert status == 200, (status, body)
    protocol_version = body["result"]["protocolVersion"]
    print(f"✓ initialize: {body['result']['serverInfo']} protocol={protocol_version} session={session_id}")

    status, _ = post({"jsonrpc": "2.0", "method": "notifications/initialized"}, expect_body=False)
    print(f"✓ notifications/initialized → HTTP {status}")

    _, body = rpc("tools/list")
    print(f"✓ tools/list: {[t['name'] for t in body['result']['tools']]}")

    today = date.today()
    usgs = call("usgs_search_earthquakes", {
        "starttime": (today - timedelta(days=7)).isoformat(),
        "endtime": today.isoformat(),
        "minmagnitude": 5,
        "limit": 5,
    })
    if usgs:
        print(f"✓ usgs_search_earthquakes: {len(usgs['events'])} events")
        for e in usgs["events"][:3]:
            print(f"    M{e['magnitude']} {e['time']} {e['place']}")

    daily = call("tmd_get_daily_earthquakes", {})
    if daily:
        print(f"✓ tmd_get_daily_earthquakes {daily['query']}: {len(daily['events'])} events")

    recent = call("tmd_get_recent_earthquakes", {"days": 3})
    if recent:
        print(f"✓ tmd_get_recent_earthquakes {recent['query']}: {len(recent['events'])} events")
        for e in recent["events"][:3]:
            print(f"    M{e['magnitude']} {e['time']} {e['place']}  id={e['id']}")

    assets = [
        {"asset_id": "BKK-01", "name": "Bangkok Warehouse", "latitude": 13.7563, "longitude": 100.5018, "sum_insured": 5_000_000},
        {"asset_id": "CNX-01", "name": "Chiang Mai Factory", "latitude": 18.7883, "longitude": 98.9853, "sum_insured": 2_000_000},
        {"asset_id": "CRI-01", "name": "Chiang Rai Hotel", "latitude": 19.9105, "longitude": 99.8406, "sum_insured": 1_000_000},
    ]
    events = (recent or {}).get("events", []) + (usgs or {}).get("events", [])
    match = call("match_events_to_assets", {"events": events, "assets": assets, "max_distance_km": 300, "min_magnitude": 2.0})
    if match:
        print(f"✓ match_events_to_assets: {match['summary']}")

    quake = {"id": "DEMO-1", "magnitude": 6.2, "latitude": 19.0, "longitude": 99.2}
    payout = call("compute_parametric_payout", {"event": quake, "assets": assets})
    if payout:
        print(f"✓ compute_parametric_payout: total={payout['total_payout']:,}")
        for p in payout["asset_payouts"]:
            print(f"    {p['asset_id']}: {p['distance_km']} km → {p['payout_percent']:.0%} = {p['payout_amount']:,}")

    print("Error handling:")
    call("usgs_search_earthquakes", {"starttime": "2026-01-01"})              # missing endtime
    call("tmd_get_recent_earthquakes", {"days": 30})                          # above maximum
    call("no_such_tool", {})
    status, body = rpc("does/not/exist")
    print(f"  ✓ unknown method → HTTP {status} {body['error']}")

    req = urllib.request.Request(URL, method="DELETE", headers={"Mcp-Session-Id": session_id, **({"Authorization": f"Bearer {TOKEN}"} if TOKEN else {})})
    with urllib.request.urlopen(req) as resp:
        print(f"✓ DELETE session → HTTP {resp.status}")
    status, body = rpc("tools/list")
    print(f"✓ request after DELETE → HTTP {status} (expected 404)")


if __name__ == "__main__":
    main()
