#!/usr/bin/env python3

import json
import sys

from agent_runner.agents import dispatch


def main() -> None:
    raw = sys.stdin.read()
    envelope = json.loads(raw)
    agent = envelope["agent"]
    payload = envelope.get("payload", {})
    result = dispatch(agent, payload)
    sys.stdout.write(json.dumps(result))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
