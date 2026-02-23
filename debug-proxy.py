#!/usr/bin/env python3
"""Debug proxy - logs all messages to file."""
import sys, json, subprocess, threading

LOG = open("/tmp/mcp-debug.log", "w")

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

for line in proc.stdout:
    try:
        msg = json.loads(line)
        if "result" in msg and "tools" not in msg.get("result", {}):
            LOG.write(json.dumps(msg, indent=2)[:2000] + "\n---\n")
            LOG.flush()
    except:
        pass
    sys.stdout.buffer.write(line)
    sys.stdout.buffer.flush()
