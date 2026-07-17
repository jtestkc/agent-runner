#!/usr/bin/env python3
import json
import os
import socket
import struct
import sys
import time

FC_VSOCK_PORT = 5201
READY_HEADER_LEN = 40


def _read_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def main():
    listen_port = int(os.getenv("FC_VSOCK_PORT", str(FC_VSOCK_PORT)))
    srv = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    srv.bind((socket.VMADDR_CID_ANY, listen_port))
    srv.listen(1)

    conn, _ = srv.accept()
    try:
        raw = bytearray()
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            raw.extend(chunk)
            try:
                envelope = json.loads(bytes(raw))
                break
            except (json.JSONDecodeError, ValueError):
                if len(raw) > 1048576:
                    raise ValueError("payload too large")
                continue

        agent = envelope["agent"]
        payload = envelope.get("payload", {})

        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, "/opt/agent-runner")
        from agent_runner.agents import dispatch

        result = dispatch(agent, payload)
        conn.sendall(json.dumps(result).encode())
    finally:
        conn.close()
        srv.close()


if __name__ == "__main__":
    main()
