"""A stand-in for one run of a real experiment, so the template runs as is.

It writes result.json into --out. Files next to it change its behavior, which
the template's tests use: `fail-<size>-<k>` makes that combination fail with a
non-transient error, `transient-<size>-<k>` makes its next run fail once with
a transient one, and `sleep-<size>-<k>` makes it run for the number of seconds
the file holds.
"""

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--k", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    combo = f"{args.size}-{args.k}"

    sleep_file = HERE / f"sleep-{combo}"
    if sleep_file.exists():
        time.sleep(float(sleep_file.read_text().strip() or "0"))
    transient = HERE / f"transient-{combo}"
    if transient.exists():
        transient.unlink()
        print("ssh: read from remote host: Connection reset by peer", file=sys.stderr)
        return 255
    if (HERE / f"fail-{combo}").exists():
        print(f"index build failed for size={args.size} k={args.k}: out of memory", file=sys.stderr)
        return 1

    recall = round(min(1.0, 0.80 + 0.05 * (args.k / 100) - 0.01 * (args.size / 10000)), 4)
    latency_ms = round(0.2 * args.size / 1000 + 0.01 * args.k, 3)
    (args.out / "result.json").write_text(json.dumps({"size": args.size, "k": args.k, "recall": recall, "latency_ms": latency_ms}))
    print(f"recall={recall} latency_ms={latency_ms}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
