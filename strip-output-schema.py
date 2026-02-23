#!/usr/bin/env python3
"""Proxy: fixes schemas + compacts large responses for LLM consumption."""
import sys, json, subprocess, threading

proc = subprocess.Popen(
    ["/home/openclaw/rusty-intervals-mcp/target/release/intervals_icu_mcp"],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    cwd="/home/openclaw/rusty-intervals-mcp"
)

def forward_stdin():
    try:
        for line in sys.stdin.buffer:
            proc.stdin.write(line)
            proc.stdin.flush()
    except:
        pass
    finally:
        try: proc.stdin.close()
        except: pass

threading.Thread(target=forward_stdin, daemon=True).start()

def fix_schema(obj):
    if isinstance(obj, dict):
        for key, val in list(obj.items()):
            if val is True and key not in ("required",):
                obj[key] = {"type": "object", "description": key}
            elif isinstance(val, (dict, list)):
                fix_schema(val)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                fix_schema(item)

KEY_DURATIONS = [1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600]

def compact_power_curves(data):
    """Transform power curves into LLM-friendly format."""
    if not isinstance(data, dict) or "value" not in data:
        return None
    val = data["value"]
    if not isinstance(val, dict) or "list" not in val:
        return None
    
    result = {"curves": []}
    for curve in val.get("list", []):
        secs = curve.get("secs", [])
        watts = curve.get("watts", [])
        wkg = curve.get("watts_per_kg", [])
        
        points = {}
        for i, s in enumerate(secs):
            if s in KEY_DURATIONS and i < len(watts):
                points[f"{s}s"] = {
                    "watts": watts[i],
                    "w_kg": round(wkg[i], 2) if i < len(wkg) else None
                }
        
        entry = {
            "label": curve.get("label", ""),
            "key_powers": points,
            "weight": curve.get("weight"),
        }
        if curve.get("powerModels"):
            entry["ftp_estimates"] = [
                {"model": m.get("type"), "ftp": m.get("ftp"), "w_prime": m.get("wPrime")}
                for m in curve["powerModels"]
            ]
        if curve.get("vo2max_5m"):
            entry["vo2max"] = round(curve["vo2max_5m"], 1)
        result["curves"].append(entry)
    
    result["activities_count"] = len(val.get("activities", {}))
    return result

def compact_streams(data):
    """Transform streams array into summary stats per stream type."""
    if not isinstance(data, list):
        return None
    if not data or not isinstance(data[0], dict) or "type" not in data[0]:
        return None
    
    result = {}
    for stream in data:
        stype = stream.get("type", "unknown")
        raw = stream.get("data")
        if not raw or not isinstance(raw, list):
            continue
        nums = [x for x in raw if isinstance(x, (int, float))]
        if not nums:
            result[stype] = {"count": 0}
            continue
        nums_sorted = sorted(nums)
        n = len(nums_sorted)
        result[stype] = {
            "count": n,
            "min": min(nums),
            "max": max(nums),
            "avg": round(sum(nums) / n, 1),
            "p10": nums_sorted[n // 10],
            "p50": nums_sorted[n // 2],
            "p90": nums_sorted[n * 9 // 10],
        }
    return result

def compact_fitness(data):
    """Fix empty fitness summary - extract from value if nested."""
    if isinstance(data, dict) and "value" in data:
        val = data["value"]
        if isinstance(val, dict) and not val:
            return None  # truly empty, leave as-is
    return None

def transform_payload(payload):
    """Try to compact known response shapes. Returns None if no transform needed."""
    # Power curves: {"value": {"list": [...], "activities": {...}}}
    r = compact_power_curves(payload)
    if r:
        return r
    
    # Streams: [{"type": "power", "data": [...]}, ...]
    if isinstance(payload, list):
        r = compact_streams(payload)
        if r:
            return r
    
    # Wrapped streams: {"value": [...]}
    if isinstance(payload, dict) and "value" in payload and isinstance(payload["value"], list):
        r = compact_streams(payload["value"])
        if r:
            return r
    
    return None

for line in proc.stdout:
    try:
        msg = json.loads(line)
        
        # Fix tool schemas in tools/list
        if "result" in msg and "tools" in msg.get("result", {}):
            for tool in msg["result"]["tools"]:
                tool.pop("outputSchema", None)
                if "inputSchema" in tool:
                    fix_schema(tool["inputSchema"])
        
        # Transform tool call content responses
        elif "result" in msg and "content" in msg.get("result", {}):
            content = msg["result"]["content"]
            if isinstance(content, list):
                for item in content:
                    if item.get("type") == "text" and "text" in item:
                        try:
                            payload = json.loads(item["text"])
                            transformed = transform_payload(payload)
                            if transformed:
                                new_text = json.dumps(transformed)
                                item["text"] = new_text
                                # Also update structuredContent
                                if "structuredContent" in msg["result"]:
                                    msg["result"]["structuredContent"] = transformed
                        except (json.JSONDecodeError, KeyError):
                            pass
        
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()
    except (json.JSONDecodeError, KeyError):
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
