#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path


BASE_TABLES = [
    "000104",
    "000302",
    "000306",
    "000316",
    "000319",
    "00040e",
    "000420",
    "003b02",
    "003b03",
    "003b04",
    "003b05",
    "003b06",
    "003b0e",
    "003b0f",
    "003d02",
    "003d03",
    "003e01",
    "003e02",
    "003e03",
    "003e04",
]


def parse_args():
    p = argparse.ArgumentParser(
        description="Start infinitive and capture zone 1 candidate tables for 30 seconds by default; optionally do a one-shot full 16-bit sweep."
    )
    p.add_argument("outfile", help="output JSONL filename")
    return p.parse_args()


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=4) as resp:
        return json.loads(resp.read().decode())


def record_table(f, base_url, device, table):
    ts = datetime.now().astimezone().isoformat()
    rec = {"ts": ts, "device": device, "table": table, "response": None}
    url = f"{base_url}/api/raw/{device}/{table}"
    success = True
    try:
        rec["response"] = fetch_json(url)
    except urllib.error.HTTPError as e:
        rec["error"] = f"HTTPError:{e.code}"
        success = False
    except urllib.error.URLError as e:
        rec["error"] = f"URLError:{e.reason}"
        success = False
    except Exception as e:
        rec["error"] = f"{type(e).__name__}:{e}"
        success = False
    f.write(json.dumps(rec, sort_keys=True) + "\n")
    return success


def uniquify_path(path: Path) -> Path:
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    candidate = path
    i = 2
    while candidate.exists():
        candidate = parent / f"{stem}_{i}{suffix}"
        i += 1
    return candidate


def nth_output_path(base: Path, index: int) -> Path:
    stem = base.stem
    suffix = base.suffix
    parent = base.parent
    if index == 1:
        return uniquify_path(base)
    return uniquify_path(parent / f"{stem}_{index}{suffix}")


def wait_for_raw_api(base_url, device, table, timeout=15.0):
    probe_url = f"{base_url}/api/raw/{device}/{table}"
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            fetch_json(probe_url)
            return
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    raise SystemExit(
        f"raw API probe failed at {probe_url}: {type(last_err).__name__}: {last_err}. "
        "Start infinitive manually or set INFINITIVE_BIN / INFINITIVE_SERIAL correctly."
    )


def start_infinitive():
    infinitive_bin = os.environ.get("INFINITIVE_BIN", "./infinitive")
    serial_port = os.environ.get("INFINITIVE_SERIAL", "/dev/ttyUSB0")
    http_port = os.environ.get("INFINITIVE_HTTPPORT", "8080")
    instance = os.environ.get("INFINITIVE_INSTANCE")

    cmd = [infinitive_bin, f"-httpport={http_port}", f"-serial={serial_port}"]
    if instance:
        cmd.append(f"-instance={instance}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc, f"http://127.0.0.1:{http_port}"


def stop_infinitive(proc):
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass


def main():
    args = parse_args()
    files = int(os.environ.get("POLL_FILES", "10"))
    duration = float(os.environ.get("POLL_DURATION", "30"))
    device = os.environ.get("POLL_DEVICE", "2001")
    full_sweep = os.environ.get("POLL_FULL_SPACE", "").lower() in {"1", "true", "yes"}
    base_out = Path(args.outfile)

    proc, base_url = start_infinitive()
    try:
        wait_for_raw_api(base_url, device, "000104")
        created = []
        for file_idx in range(1, files + 1):
            out = nth_output_path(base_out, file_idx)
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open("x", encoding="utf-8") as f:
                if full_sweep:
                    for domain in range(0x100):
                        for offset in range(0x100):
                            table = f"{domain:02x}{offset:02x}"
                            record_table(f, base_url, device, table)
                else:
                    deadline = time.time() + duration
                    while time.time() < deadline:
                        for table in BASE_TABLES:
                            record_table(f, base_url, device, table)
                            if time.time() >= deadline:
                                break
            created.append(out)
    finally:
        stop_infinitive(proc)

    for out in created:
        print(out)


if __name__ == "__main__":
    main()
