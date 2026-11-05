#!/usr/bin/env python3
"""
Seismic MCP Server: seismic data and insurance / parametric analytics.

Transports
----------
* Streamable HTTP (default): one MCP endpoint (default ``/mcp``) that accepts
  JSON-RPC over HTTP POST, issues ``Mcp-Session-Id`` on ``initialize``, and
  supports DELETE to end a session. Responses are ``application/json``, or a
  single-shot ``text/event-stream`` when the client only accepts SSE.
* stdio (``--transport stdio``): newline-delimited JSON-RPC on stdin/stdout,
  as in the original spec for Claude Desktop.

Logs always go to stderr. In stdio mode nothing but JSON-RPC is written to stdout.

Upstream data sources
---------------------
* USGS Earthquake FDSN API (no auth)
* TMD DailySeismicEvent demo API (fixed demo uid/ukey)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import secrets
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from jsonschema import Draft202012Validator

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

SERVER_NAME = "seismic-mcp-server"
SERVER_VERSION = "0.1.0"

# Newest first. If the client asks for one of these, it gets that version back.
# Otherwise the server answers with the newest one.
SUPPORTED_PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]
LATEST_PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[0]

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
TMD_URL = "https://data.tmd.go.th/api/DailySeismicEvent/v1/"
TMD_DEMO_PARAMS = {"uid": "api", "ukey": "api12345", "format": "json"}

UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "20"))
TMD_CACHE_SECONDS = float(os.getenv("TMD_CACHE_SECONDS", "60"))
SESSION_TTL_SECONDS = float(os.getenv("MCP_SESSION_TTL", "3600"))

THAI_TZ = timezone(timedelta(hours=7))

# JSON-RPC error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
UPSTREAM_HTTP_ERROR = -32001   # upstream API unreachable or returned HTTP error
UPSTREAM_PARSE_ERROR = -32002  # upstream API returned something we cannot parse

logging.basicConfig(
    stream=sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(SERVER_NAME)


class RpcError(Exception):
    """An error that maps directly to a JSON-RPC error object."""

    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    def to_dict(self) -> dict:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return err


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def to_float(value: Any) -> float | None:
    """Convert numbers or numeric strings to float; return None for anything else."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str):
        try:
            f = float(value.strip())
        except ValueError:
            return None
        return f if math.isfinite(f) else None
    return None


def snippet(text: str, limit: int = 300) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[:limit] + "…"


def local_today() -> date:
    """Today's date in server local time. ``TZ`` (e.g. Asia/Bangkok) overrides the system zone."""
    tz_name = os.getenv("TZ")
    if tz_name:
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:  # noqa: BLE001 - bad TZ value; fall back to system local time
            log.warning("Unknown TZ=%r, falling back to system local time", tz_name)
    return datetime.now().date()


def parse_ymd(value: str, field: str) -> date:
    try:
        if len(value) != 10:
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        raise RpcError(INVALID_PARAMS, f"'{field}' must be a date in YYYY-MM-DD format, got {value!r}")


def parse_event_time(value: Any) -> datetime:
    """Parse an EarthquakeEvent.time for sorting; unparseable values sort last."""
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088  # mean Earth radius, km
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


# ---------------------------------------------------------------------------
# Upstream HTTP
# ---------------------------------------------------------------------------

_http_client: httpx.AsyncClient | None = None


def http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            timeout=UPSTREAM_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": f"{SERVER_NAME}/{SERVER_VERSION}"},
        )
    return _http_client


async def close_http_client() -> None:
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


async def fetch_json(url: str, params: dict[str, Any], source: str) -> Any:
    try:
        resp = await http_client().get(url, params=params)
    except httpx.TimeoutException:
        raise RpcError(UPSTREAM_HTTP_ERROR, f"{source} request timed out after {UPSTREAM_TIMEOUT:g}s")
    except httpx.HTTPError as exc:
        raise RpcError(UPSTREAM_HTTP_ERROR, f"{source} request failed: {exc}")

    if resp.status_code == 204:  # FDSN "no data"
        return None
    if resp.status_code >= 400:
        raise RpcError(
            UPSTREAM_HTTP_ERROR,
            f"{source} returned HTTP {resp.status_code}: {snippet(resp.text)}",
            {"status": resp.status_code, "url": str(resp.url)},
        )
    try:
        return resp.json()
    except ValueError:
        raise RpcError(UPSTREAM_PARSE_ERROR, f"{source} returned a non-JSON response: {snippet(resp.text)}")


# ---------------------------------------------------------------------------
# Normalisation -> EarthquakeEvent
# ---------------------------------------------------------------------------

def usgs_feature_to_event(feature: dict) -> dict:
    props = feature.get("properties") or {}
    coords = list((feature.get("geometry") or {}).get("coordinates") or [])
    coords += [None] * (3 - len(coords))

    epoch_ms = props.get("time")
    iso_time = None
    if isinstance(epoch_ms, (int, float)) and not isinstance(epoch_ms, bool):
        iso_time = (
            datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    return {
        "id": str(feature.get("id")),
        "time": iso_time,
        "place": props.get("place"),
        "magnitude": to_float(props.get("mag")),
        "latitude": to_float(coords[1]),
        "longitude": to_float(coords[0]),
        "depth_km": to_float(coords[2]),
        "source": "USGS",
        "url": props.get("url"),
        "raw": feature,
    }


def _tmd_naive(value: str, tz: timezone) -> datetime | None:
    """Parse TMD timestamps such as '2026-11-05 09:59:32.000' (with or without an offset)."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=tz)


def tmd_event_datetime(item: dict) -> datetime | None:
    """Event time as an aware datetime in Thai time (UTC+7).

    The spec documents ``DateTime``. The live API sends ``DateTimeUTC`` /
    ``DateTimeThai``. All three are handled.
    """
    if isinstance(item.get("DateTime"), str):
        dt = _tmd_naive(item["DateTime"], THAI_TZ)
        if dt:
            return dt.astimezone(THAI_TZ)
    if isinstance(item.get("DateTimeUTC"), str):
        dt = _tmd_naive(item["DateTimeUTC"], timezone.utc)
        if dt:
            return dt.astimezone(THAI_TZ)
    if isinstance(item.get("DateTimeThai"), str):
        dt = _tmd_naive(item["DateTimeThai"], THAI_TZ)
        if dt:
            return dt.astimezone(THAI_TZ)
    return None


def tmd_item_to_event(item: dict) -> dict:
    dt = tmd_event_datetime(item)
    lat = to_float(item.get("Latitude"))
    lon = to_float(item.get("Longitude"))

    # The spec maps id <- "TMD-" + EventID. The live API has no EventID, so a
    # stable id is built from the UTC origin time and the epicentre.
    if item.get("EventID") not in (None, ""):
        event_id = f"TMD-{item['EventID']}"
    else:
        stamp = dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ") if dt else "unknown"
        event_id = f"TMD-{stamp}-{lat if lat is not None else 'na'}-{lon if lon is not None else 'na'}"

    # Place: spec field "Location"; the live API uses TitleThai ("แผ่นดินไหว <place>")
    # and OriginThai, which is sometimes truncated.
    place = item.get("Location")
    if not place and isinstance(item.get("TitleThai"), str):
        place = item["TitleThai"].strip()
        if place.startswith("แผ่นดินไหว"):
            place = place[len("แผ่นดินไหว"):].strip()
    if not place:
        place = item.get("OriginThai")

    return {
        "id": event_id,
        "time": dt.isoformat() if dt else item.get("DateTime") or item.get("DateTimeThai"),
        "place": place,
        "magnitude": to_float(item.get("Magnitude")),
        "latitude": lat,
        "longitude": lon,
        "depth_km": to_float(item.get("DepthKm", item.get("Depth"))),
        "source": "TMD_DailySeismicEvent",
        "url": item.get("URL") or item.get("Url") or item.get("url") or None,
        "raw": item,
    }


def tmd_extract_items(payload: Any) -> list[dict]:
    if payload is None:
        return []
    if not isinstance(payload, dict):
        raise RpcError(UPSTREAM_PARSE_ERROR, "TMD response is not a JSON object")
    for key in ("DailySeismicEvent", "DailyEarthquakes"):  # spec key, live-API key
        if key in payload:
            items = payload[key]
            if items in (None, "", []):
                return []
            if isinstance(items, dict):  # a single event may come back as an object
                return [items]
            if isinstance(items, list):
                return [i for i in items if isinstance(i, dict)]
            raise RpcError(UPSTREAM_PARSE_ERROR, f"TMD field '{key}' has unexpected type {type(items).__name__}")
    log.warning("TMD response had no event array; keys=%s", list(payload)[:10])
    return []


# Small TTL cache so tmd_get_recent_earthquakes doesn't re-download the same data.
_tmd_cache: dict[str, tuple[float, list[dict]]] = {}
_tmd_locks: dict[str, asyncio.Lock] = {}


async def tmd_fetch_items(day: date) -> list[dict]:
    key = day.isoformat()
    lock = _tmd_locks.setdefault(key, asyncio.Lock())
    async with lock:
        cached = _tmd_cache.get(key)
        if cached and time.monotonic() - cached[0] < TMD_CACHE_SECONDS:
            return cached[1]
        params = dict(TMD_DEMO_PARAMS, Date=key)
        items = tmd_extract_items(await fetch_json(TMD_URL, params, "TMD"))
        _tmd_cache[key] = (time.monotonic(), items)
        return items


async def tmd_events_for_day(day: date) -> list[dict]:
    """Earthquakes whose Thai local date equals ``day``.

    As of 2026-11 the live API ignores ``Date`` and returns everything since
    1 January of the current year. The server still sends ``Date`` as the spec
    says, then filters by date itself so results are right either way.
    """
    events = []
    for item in await tmd_fetch_items(day):
        dt = tmd_event_datetime(item)
        if dt is None or dt.date() != day:
            continue
        events.append(tmd_item_to_event(item))
    return events


# ---------------------------------------------------------------------------
# Tool definitions (input schemas are taken verbatim from the spec)
# ---------------------------------------------------------------------------

_ASSET_PROPS = {
    "asset_id": {"type": "string"},
    "name": {"type": "string"},
    "latitude": {"type": "number"},
    "longitude": {"type": "number"},
}

TOOLS: list[dict] = [
    {
        "name": "usgs_search_earthquakes",
        "description": "Search earthquakes from the USGS FDSN API with basic filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "starttime": {"type": "string", "description": "YYYY-MM-DD or ISO8601."},
                "endtime": {"type": "string", "description": "YYYY-MM-DD or ISO8601."},
                "minmagnitude": {"type": "number", "description": "Minimum magnitude.", "default": 4.5},
                "maxmagnitude": {"type": "number", "description": "Maximum magnitude, optional."},
                "latitude": {"type": "number", "description": "Center latitude for radius search, optional."},
                "longitude": {"type": "number", "description": "Center longitude for radius search, optional."},
                "maxradiuskm": {"type": "number", "description": "Search radius in kilometers, optional."},
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of events.",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 200,
                },
            },
            "required": ["starttime", "endtime"],
            "additionalProperties": False,
        },
    },
    {
        "name": "tmd_get_daily_earthquakes",
        "description": "Get daily earthquakes from the TMD DailySeismicEvent demo API.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD. If omitted, use today's date."},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "tmd_get_recent_earthquakes",
        "description": "Get recent earthquakes from TMD over the last N days using DailySeismicEvent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days back from today (including today).",
                    "default": 3,
                    "minimum": 1,
                    "maximum": 7,
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "match_events_to_assets",
        "description": "Match earthquake events to insured assets within a given radius and magnitude threshold.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "events": {
                    "type": "array",
                    "description": "List of EarthquakeEvent objects.",
                    "items": {"type": "object"},
                },
                "assets": {
                    "type": "array",
                    "description": "Insured assets with coordinates.",
                    "items": {
                        "type": "object",
                        "properties": {
                            **_ASSET_PROPS,
                            "sum_insured": {"type": "number", "description": "Optional total insured value."},
                        },
                        "required": ["asset_id", "latitude", "longitude"],
                        "additionalProperties": True,
                    },
                },
                "max_distance_km": {
                    "type": "number",
                    "description": "Max distance to consider an asset affected.",
                    "default": 100,
                },
                "min_magnitude": {"type": "number", "description": "Minimum magnitude to consider.", "default": 4.5},
            },
            "required": ["events", "assets"],
            "additionalProperties": False,
        },
    },
    {
        "name": "compute_parametric_payout",
        "description": (
            "Compute a simple parametric payout for an earthquake event and a set of assets, "
            "based on distance and magnitude thresholds."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "event": {"type": "object", "description": "Single EarthquakeEvent to evaluate."},
                "assets": {
                    "type": "array",
                    "description": "Assets with coordinates and sum_insured.",
                    "items": {
                        "type": "object",
                        "properties": {**_ASSET_PROPS, "sum_insured": {"type": "number"}},
                        "required": ["asset_id", "latitude", "longitude", "sum_insured"],
                        "additionalProperties": True,
                    },
                },
                "min_magnitude_trigger": {
                    "type": "number",
                    "description": "Minimum magnitude to trigger any payout.",
                    "default": 5.0,
                },
                "max_distance_km_trigger": {
                    "type": "number",
                    "description": "Max distance for eligibility.",
                    "default": 200,
                },
                "payout_curve": {
                    "type": "array",
                    "description": "Optional distance-based payout curve.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "max_distance_km": {"type": "number"},
                            "payout_percent": {"type": "number", "description": "Fraction of sum_insured (0.0–1.0)."},
                        },
                        "required": ["max_distance_km", "payout_percent"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["event", "assets"],
            "additionalProperties": False,
        },
    },
]

TOOLS_BY_NAME = {t["name"]: t for t in TOOLS}
VALIDATORS = {t["name"]: Draft202012Validator(t["inputSchema"]) for t in TOOLS}

DEFAULT_PAYOUT_CURVE = [
    {"max_distance_km": 50, "payout_percent": 0.5},
    {"max_distance_km": 100, "payout_percent": 0.25},
    {"max_distance_km": 200, "payout_percent": 0.1},
]


def validate_arguments(tool_name: str, arguments: Any) -> dict:
    """Check arguments against the tool's inputSchema, then fill in top-level defaults."""
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise RpcError(INVALID_PARAMS, "'arguments' must be an object")

    errors = sorted(VALIDATORS[tool_name].iter_errors(arguments), key=lambda e: list(e.absolute_path))
    if errors:
        details = []
        for err in errors[:10]:
            path = "/".join(str(p) for p in err.absolute_path) or "(root)"
            details.append({"path": path, "message": err.message})
        summary = "; ".join(f"{d['path']}: {d['message']}" for d in details)
        raise RpcError(INVALID_PARAMS, f"Invalid arguments for '{tool_name}': {summary}", {"errors": details})

    resolved = dict(arguments)
    for key, prop in TOOLS_BY_NAME[tool_name]["inputSchema"]["properties"].items():
        if key not in resolved and "default" in prop:
            resolved[key] = prop["default"]
    return resolved


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

async def tool_usgs_search_earthquakes(args: dict) -> dict:
    radius_keys = ("latitude", "longitude", "maxradiuskm")
    given = [k for k in radius_keys if k in args]
    if given and len(given) != 3:
        raise RpcError(INVALID_PARAMS, "Radius search needs all of latitude, longitude and maxradiuskm")
    if "latitude" in args and not -90 <= args["latitude"] <= 90:
        raise RpcError(INVALID_PARAMS, "latitude must be between -90 and 90")
    if "longitude" in args and not -180 <= args["longitude"] <= 180:
        raise RpcError(INVALID_PARAMS, "longitude must be between -180 and 180")
    if "maxradiuskm" in args and not 0 < args["maxradiuskm"] <= 20001.6:
        raise RpcError(INVALID_PARAMS, "maxradiuskm must be > 0 and <= 20001.6")
    if "maxmagnitude" in args and args["maxmagnitude"] < args["minmagnitude"]:
        raise RpcError(INVALID_PARAMS, "maxmagnitude must be >= minmagnitude")
    for k in ("starttime", "endtime"):
        if not args[k].strip():
            raise RpcError(INVALID_PARAMS, f"'{k}' must not be empty")

    query: dict[str, Any] = {"format": "geojson"}
    for key in ("starttime", "endtime", "minmagnitude", "maxmagnitude", *radius_keys, "limit"):
        if key in args:
            query[key] = args[key]

    payload = await fetch_json(USGS_URL, query, "USGS")
    if payload is None:
        features = []
    elif isinstance(payload, dict) and isinstance(payload.get("features"), list):
        features = payload["features"]
    else:
        raise RpcError(UPSTREAM_PARSE_ERROR, "USGS response is not a GeoJSON FeatureCollection")

    events = [usgs_feature_to_event(f) for f in features if isinstance(f, dict)]
    return {"events": events, "query": query}


async def tool_tmd_get_daily_earthquakes(args: dict) -> dict:
    day = parse_ymd(args["date"], "date") if "date" in args else local_today()
    events = await tmd_events_for_day(day)
    events.sort(key=lambda e: parse_event_time(e["time"]), reverse=True)
    return {"events": events, "query": {"date": day.isoformat()}}


async def tool_tmd_get_recent_earthquakes(args: dict) -> dict:
    days = args["days"]
    today = local_today()
    dates = [today - timedelta(days=d) for d in range(days)]
    per_day = await asyncio.gather(*(tmd_events_for_day(d) for d in dates))

    seen: dict[str, dict] = {}
    for events in per_day:
        for ev in events:
            seen.setdefault(ev["id"], ev)
    events = sorted(seen.values(), key=lambda e: parse_event_time(e["time"]), reverse=True)
    return {"events": events, "query": {"days": days}}


def _event_coords(event: dict) -> tuple[float, float, float] | None:
    lat, lon, mag = to_float(event.get("latitude")), to_float(event.get("longitude")), to_float(event.get("magnitude"))
    if lat is None or lon is None or mag is None:
        return None
    return lat, lon, mag


async def tool_match_events_to_assets(args: dict) -> dict:
    max_km = float(args["max_distance_km"])
    min_mag = float(args["min_magnitude"])
    if max_km < 0:
        raise RpcError(INVALID_PARAMS, "max_distance_km must be >= 0")

    usable = []
    skipped = 0
    for ev in args["events"]:
        coords = _event_coords(ev)
        if coords is None:
            skipped += 1
            continue
        if coords[2] >= min_mag:
            usable.append((ev, coords))

    matches = []
    for asset in args["assets"]:
        hits = []
        for ev, (lat, lon, _mag) in usable:
            d = haversine_km(lat, lon, asset["latitude"], asset["longitude"])
            if d <= max_km:
                hits.append({"event": ev, "distance_km": round(d, 2)})
        if hits:
            hits.sort(key=lambda h: h["distance_km"])
            matches.append({
                "asset_id": asset["asset_id"],
                "name": asset.get("name"),
                "sum_insured": asset.get("sum_insured"),
                "events": hits,
            })

    return {
        "matches": matches,
        "parameters": {"max_distance_km": args["max_distance_km"], "min_magnitude": args["min_magnitude"]},
        "summary": {
            "events_received": len(args["events"]),
            "events_meeting_magnitude": len(usable),
            "events_skipped_missing_fields": skipped,
            "assets_received": len(args["assets"]),
            "assets_matched": len(matches),
        },
    }


async def tool_compute_parametric_payout(args: dict) -> dict:
    event = args["event"]
    coords = _event_coords(event)
    if coords is None:
        raise RpcError(INVALID_PARAMS, "event must include numeric latitude, longitude and magnitude")
    ev_lat, ev_lon, ev_mag = coords

    min_mag = float(args["min_magnitude_trigger"])
    max_km = float(args["max_distance_km_trigger"])
    curve = args.get("payout_curve") or DEFAULT_PAYOUT_CURVE
    for band in curve:
        if not 0.0 <= band["payout_percent"] <= 1.0:
            raise RpcError(INVALID_PARAMS, "payout_percent must be between 0.0 and 1.0")
        if band["max_distance_km"] < 0:
            raise RpcError(INVALID_PARAMS, "payout_curve max_distance_km must be >= 0")
    # Sort bands from nearest to farthest, so "the first band with
    # distance <= max_distance_km" is the tightest band that applies.
    curve = sorted(curve, key=lambda b: b["max_distance_km"])

    magnitude_triggered = ev_mag >= min_mag
    payouts = []
    total = 0.0
    for asset in args["assets"]:
        dist = haversine_km(ev_lat, ev_lon, asset["latitude"], asset["longitude"])
        pct = 0.0
        if magnitude_triggered and dist <= max_km:
            band = next((b for b in curve if dist <= b["max_distance_km"]), None)
            pct = float(band["payout_percent"]) if band else 0.0
        amount = round(float(asset["sum_insured"]) * pct, 2)
        total += amount
        payouts.append({
            "asset_id": asset["asset_id"],
            "name": asset.get("name"),
            "distance_km": round(dist, 2),
            "sum_insured": asset["sum_insured"],
            "payout_percent": pct,
            "payout_amount": amount,
        })

    return {
        "event_id": event.get("id"),
        "event_magnitude": ev_mag,
        "magnitude_triggered": magnitude_triggered,
        "parameters": {
            "min_magnitude_trigger": args["min_magnitude_trigger"],
            "max_distance_km_trigger": args["max_distance_km_trigger"],
            "payout_curve": curve,
        },
        "asset_payouts": payouts,
        "total_payout": round(total, 2),
    }


TOOL_HANDLERS = {
    "usgs_search_earthquakes": tool_usgs_search_earthquakes,
    "tmd_get_daily_earthquakes": tool_tmd_get_daily_earthquakes,
    "tmd_get_recent_earthquakes": tool_tmd_get_recent_earthquakes,
    "match_events_to_assets": tool_match_events_to_assets,
    "compute_parametric_payout": tool_compute_parametric_payout,
}


# ---------------------------------------------------------------------------
# MCP / JSON-RPC dispatch (shared by every transport)
# ---------------------------------------------------------------------------

def negotiate_protocol_version(requested: Any) -> str:
    return requested if requested in SUPPORTED_PROTOCOL_VERSIONS else LATEST_PROTOCOL_VERSION


async def mcp_initialize(params: dict) -> dict:
    version = negotiate_protocol_version(params.get("protocolVersion"))
    client_info = params.get("clientInfo") or {}
    log.info("initialize from %s %s (requested %s, using %s)",
             client_info.get("name"), client_info.get("version"), params.get("protocolVersion"), version)
    return {
        "protocolVersion": version,
        "capabilities": {"tools": {"list": True, "call": True, "listChanged": False}},
        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        "instructions": (
            "Seismic data (USGS, Thai Meteorological Department) and insurance analytics. "
            "Fetch events with usgs_search_earthquakes / tmd_get_daily_earthquakes / "
            "tmd_get_recent_earthquakes, then pass them to match_events_to_assets or "
            "compute_parametric_payout."
        ),
    }


async def mcp_tools_call(params: dict) -> dict:
    name = params.get("name")
    if not isinstance(name, str) or name not in TOOL_HANDLERS:
        raise RpcError(INVALID_PARAMS, f"Unknown tool: {name!r}", {"available": list(TOOL_HANDLERS)})
    args = validate_arguments(name, params.get("arguments"))
    started = time.monotonic()
    result = await TOOL_HANDLERS[name](args)
    log.info("tools/call %s ok in %.0f ms", name, (time.monotonic() - started) * 1000)
    return {
        "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
        "structuredContent": result,
        "isError": False,
    }


async def dispatch(method: str, params: dict) -> Any:
    if method == "initialize":
        return await mcp_initialize(params)
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": TOOLS}
    if method == "tools/call":
        return await mcp_tools_call(params)
    raise RpcError(METHOD_NOT_FOUND, f"Method not found: {method}")


def _rpc_error(id_: Any, err: RpcError) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "error": err.to_dict()}


async def handle_message(msg: Any) -> dict | None:
    """Handle one JSON-RPC message. Returns a response, or None for notifications and responses."""
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        rid = msg.get("id") if isinstance(msg, dict) else None
        return _rpc_error(rid, RpcError(INVALID_REQUEST, "Invalid JSON-RPC 2.0 message"))

    method = msg.get("method")
    if method is None:
        # A response to something we sent. This server sends no requests, so ignore it.
        return None
    is_request = "id" in msg
    if not isinstance(method, str):
        return _rpc_error(msg.get("id"), RpcError(INVALID_REQUEST, "'method' must be a string")) if is_request else None

    if not is_request:
        log.debug("notification %s", method)  # e.g. notifications/initialized, notifications/cancelled
        return None

    params = msg.get("params")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        return _rpc_error(msg["id"], RpcError(INVALID_PARAMS, "'params' must be an object"))

    try:
        result = await dispatch(method, params)
    except RpcError as err:
        log.warning("%s failed: [%s] %s", method, err.code, err.message)
        return _rpc_error(msg["id"], err)
    except Exception as exc:  # noqa: BLE001
        log.exception("Unhandled error in %s", method)
        return _rpc_error(msg["id"], RpcError(INTERNAL_ERROR, f"Internal error: {exc}"))
    return {"jsonrpc": "2.0", "id": msg["id"], "result": result}


# ---------------------------------------------------------------------------
# Transport: stdio
# ---------------------------------------------------------------------------

async def run_stdio() -> None:
    log.info("%s %s listening on stdio", SERVER_NAME, SERVER_VERSION)
    write_lock = asyncio.Lock()
    pending: set[asyncio.Task] = set()

    async def write(obj: Any) -> None:
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        async with write_lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()

    async def process(line: str) -> None:
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as exc:
            await write(_rpc_error(None, RpcError(PARSE_ERROR, f"Parse error: {exc.msg}")))
            return
        if isinstance(msg, list):  # JSON-RPC batch
            if not msg:
                await write(_rpc_error(None, RpcError(INVALID_REQUEST, "Empty batch")))
                return
            replies = [r for r in await asyncio.gather(*(handle_message(m) for m in msg)) if r is not None]
            if replies:
                await write(replies)
            return
        reply = await handle_message(msg)
        if reply is not None:
            await write(reply)

    try:
        while True:
            line = await asyncio.to_thread(sys.stdin.readline)
            if not line:
                break  # EOF: the client closed stdin
            if not line.strip():
                continue
            task = asyncio.create_task(process(line))
            pending.add(task)
            task.add_done_callback(pending.discard)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    finally:
        await close_http_client()
        log.info("stdio closed, exiting")


# ---------------------------------------------------------------------------
# Transport: Streamable HTTP
# ---------------------------------------------------------------------------

def build_http_app(mcp_path: str = "/mcp", stateless: bool = False):
    from contextlib import asynccontextmanager

    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route

    auth_token = os.getenv("MCP_AUTH_TOKEN") or None
    allowed_origins = {o.strip().rstrip("/") for o in os.getenv("MCP_ALLOWED_ORIGINS", "").split(",") if o.strip()}
    sessions: dict[str, dict[str, Any]] = {}

    def purge_sessions() -> None:
        now = time.monotonic()
        for sid in [s for s, v in sessions.items() if now - v["last_seen"] > SESSION_TTL_SECONDS]:
            sessions.pop(sid, None)
            log.info("session %s expired", sid)

    def origin_allowed(request: Request) -> bool:
        # Blocks DNS-rebinding attacks from browsers. Non-browser clients send no Origin.
        origin = request.headers.get("origin")
        if not origin or "*" in allowed_origins:
            return True
        if origin.rstrip("/") in allowed_origins:
            return True
        host = (urlparse(origin).hostname or "").lower()
        return host in {"localhost", "127.0.0.1", "::1"}

    def authorized(request: Request) -> bool:
        if not auth_token:
            return True
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        return scheme.lower() == "bearer" and secrets.compare_digest(token.strip(), auth_token)

    def http_error(status: int, code: int, message: str, headers: dict | None = None) -> JSONResponse:
        return JSONResponse(_rpc_error(None, RpcError(code, message)), status_code=status, headers=headers)

    async def mcp_endpoint(request: Request) -> Response:
        if not origin_allowed(request):
            return http_error(403, INVALID_REQUEST, "Origin not allowed")
        if not authorized(request):
            return http_error(401, INVALID_REQUEST, "Unauthorized", {"WWW-Authenticate": "Bearer"})

        purge_sessions()

        if request.method == "GET":
            # This server never starts messages to the client, so it offers no
            # standalone SSE stream. The spec allows 405 here.
            return Response(status_code=405, headers={"Allow": "POST, DELETE"})

        if request.method == "DELETE":
            sid = request.headers.get("mcp-session-id")
            if stateless:
                return Response(status_code=405, headers={"Allow": "POST"})
            if not sid:
                return http_error(400, INVALID_REQUEST, "Missing Mcp-Session-Id header")
            if sessions.pop(sid, None) is None:
                return http_error(404, INVALID_REQUEST, "Session not found")
            log.info("session %s terminated by client", sid)
            return Response(status_code=204)

        # ---- POST ----
        ctype = request.headers.get("content-type", "")
        if "application/json" not in ctype.lower():
            return http_error(415, INVALID_REQUEST, "Content-Type must be application/json")

        proto = request.headers.get("mcp-protocol-version")
        if proto and proto not in SUPPORTED_PROTOCOL_VERSIONS:
            return http_error(400, INVALID_REQUEST, f"Unsupported MCP-Protocol-Version: {proto}")

        raw = await request.body()
        try:
            body = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return http_error(400, PARSE_ERROR, f"Parse error: {exc}")

        is_batch = isinstance(body, list)
        messages = body if is_batch else [body]
        if not messages:
            return http_error(400, INVALID_REQUEST, "Empty batch")

        is_init = any(isinstance(m, dict) and m.get("method") == "initialize" for m in messages)
        if is_init and is_batch and len(messages) > 1:
            return http_error(400, INVALID_REQUEST, "initialize must not be sent in a batch")

        sid = request.headers.get("mcp-session-id")
        if not stateless and not is_init:
            if not sid:
                return http_error(400, INVALID_REQUEST, "Missing Mcp-Session-Id header (call initialize first)")
            if sid not in sessions:
                return http_error(404, INVALID_REQUEST, "Session not found or expired; re-initialize")
            sessions[sid]["last_seen"] = time.monotonic()

        replies = [r for r in await asyncio.gather(*(handle_message(m) for m in messages)) if r is not None]

        headers: dict[str, str] = {}
        if is_init and not stateless and replies and "result" in replies[0]:
            new_sid = uuid.uuid4().hex
            sessions[new_sid] = {
                "created": time.monotonic(),
                "last_seen": time.monotonic(),
                "protocol_version": replies[0]["result"]["protocolVersion"],
            }
            headers["Mcp-Session-Id"] = new_sid
            log.info("session %s created (%d active)", new_sid, len(sessions))

        if not replies:  # only notifications / responses
            return Response(status_code=202, headers=headers)

        payload: Any = replies if is_batch else replies[0]
        accept = request.headers.get("accept", "").lower()
        wants_json = not accept or "application/json" in accept or "*/*" in accept
        if not wants_json and "text/event-stream" in accept:
            # Client accepts only SSE: send the reply as one SSE event, then close.
            data = json.dumps(payload, ensure_ascii=False)
            body_sse = f"event: message\ndata: {data}\n\n"
            headers["Cache-Control"] = "no-cache"
            return Response(body_sse, media_type="text/event-stream", headers=headers)
        return JSONResponse(payload, headers=headers)

    async def healthz(_request: Request) -> Response:
        return JSONResponse({
            "status": "ok",
            "server": SERVER_NAME,
            "version": SERVER_VERSION,
            "transport": "streamable-http",
            "endpoint": mcp_path,
            "active_sessions": len(sessions),
        })

    @asynccontextmanager
    async def lifespan(_app):
        log.info("%s %s ready: Streamable HTTP endpoint at %s (stateless=%s, auth=%s)",
                 SERVER_NAME, SERVER_VERSION, mcp_path, stateless, bool(auth_token))
        try:
            yield
        finally:
            await close_http_client()

    return Starlette(
        routes=[
            Route(mcp_path, mcp_endpoint, methods=["GET", "POST", "DELETE"]),
            Route("/healthz", healthz, methods=["GET"]),
        ],
        lifespan=lifespan,
    )


def run_http(host: str, port: int, mcp_path: str, stateless: bool) -> None:
    import uvicorn

    app = build_http_app(mcp_path=mcp_path, stateless=stateless)
    uvicorn.run(app, host=host, port=port, log_level=os.getenv("LOG_LEVEL", "info").lower(), proxy_headers=True)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    def env_bool(name: str) -> bool:
        return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}

    parser = argparse.ArgumentParser(description="Seismic MCP Server (Streamable HTTP or stdio)")
    parser.add_argument("--transport", choices=["http", "stdio"], default=os.getenv("MCP_TRANSPORT", "http"))
    parser.add_argument("--host", default=os.getenv("MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_PORT", "8000")))
    parser.add_argument("--path", default=os.getenv("MCP_PATH", "/mcp"), help="MCP endpoint path")
    parser.add_argument("--stateless", action="store_true", default=env_bool("MCP_STATELESS"),
                        help="Do not issue or require Mcp-Session-Id")
    args = parser.parse_args()

    if args.transport == "stdio":
        asyncio.run(run_stdio())
    else:
        path = args.path if args.path.startswith("/") else "/" + args.path
        run_http(args.host, args.port, path, args.stateless)


if __name__ == "__main__":
    main()
