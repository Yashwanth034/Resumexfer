#!/usr/bin/env python3

import json
import sys
import time

import resumexfer_core as core


def process_request(socket_path, request, sleeper=time.sleep):
    managed = request.get("action") == "start_managed_transfer"
    intent_id = str(request.get("intent_id") or "")
    attempts = 3 if managed else 1
    timeout = 5.0 if managed else 0.75
    last_error = None

    for attempt in range(attempts):
        try:
            core.send_request(socket_path, request, timeout=timeout)
            if managed and intent_id:
                core.write_managed_start_result(socket_path, intent_id, True)
            return True
        except (OSError, RuntimeError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleeper(0.15 * (attempt + 1))

    if managed and intent_id:
        core.write_managed_start_result(
            socket_path,
            intent_id,
            False,
            str(last_error or "managed transfer request failed"),
        )
    return False


def main():
    if len(sys.argv) != 2:
        return 2

    socket_path = sys.argv[1]

    for line in sys.stdin:
        try:
            request = json.loads(line)
        except (TypeError, ValueError):
            continue

        if not isinstance(request, dict):
            continue

        process_request(socket_path, request)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
