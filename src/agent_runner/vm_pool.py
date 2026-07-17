import asyncio
import json
import os
import struct
import subprocess
import sys
import tempfile
import time

from .utils import crashes, get_settings, pool_size, pool_wait, warn

_SETTINGS = get_settings()


class ExecError(RuntimeError):
    pass


class Empty(Exception):
    pass


async def _subprocess(agent, payload):
    timeout = _SETTINGS.agent_timeout + 10
    data = json.dumps({"agent": agent, "payload": payload}).encode()
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        _SETTINGS.agent_binary,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(data), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        crashes.labels(agent=agent).inc()
        raise ExecError(f"{agent} timed out")
    if proc.returncode != 0:
        crashes.labels(agent=agent).inc()
        raise ExecError(f"{agent} exited {proc.returncode}: {stderr.decode()[:500]}")
    return json.loads(stdout.decode())


async def _docker(agent, payload):
    cmd = [
        "docker",
        "run",
        "--rm",
        "--interactive",
        "--network",
        "none",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=64M",
        "--memory",
        "256m",
        "--cpus",
        "0.5",
        "--cap-drop",
        "ALL",
        "--pids-limit",
        "128",
        "agent-runner/sandbox:latest",
        "python",
        _SETTINGS.agent_binary,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    timeout = _SETTINGS.agent_timeout + 10
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(json.dumps({"agent": agent, "payload": payload}).encode()),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        proc.kill()
        crashes.labels(agent=agent).inc()
        raise ExecError(f"{agent} timed out")
    if proc.returncode != 0:
        crashes.labels(agent=agent).inc()
        raise ExecError(f"{agent} exited {proc.returncode}: {stderr.decode()[:500]}")
    return json.loads(stdout.decode())


async def _fc_api(sock_path, method, path, body=None):
    reader, writer = await asyncio.open_unix_connection(sock_path)
    try:
        enc = json.dumps(body).encode() if body else b""
        req = f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {len(enc)}\r\nContent-Type: application/json\r\n\r\n".encode() + enc
        writer.write(req)
        await writer.drain()
        head = b""
        while b"\r\n\r\n" not in head:
            c = await reader.read(4096)
            if not c:
                raise ExecError("fc API connection lost")
            head += c
        head_str = head.split(b"\r\n\r\n")[0].decode()
        status = int(head_str.split(" ")[1])
        cl = 0
        for line in head_str.split("\r\n"):
            if line.lower().startswith("content-length:"):
                cl = int(line.split(":")[1].strip())
        body_bytes = head[head.index(b"\r\n\r\n") + 4:]
        while len(body_bytes) < cl:
            c = await reader.read(4096)
            if not c:
                break
            body_bytes += c
        return status, body_bytes.decode()
    finally:
        writer.close()


async def _firecracker(agent, payload):
    timeout = _SETTINGS.agent_timeout + 30
    with tempfile.TemporaryDirectory() as tmp:
        api_path = f"{tmp}/fc.sock"
        vsock_path = f"{tmp}/v.sock"

        proc = await asyncio.create_subprocess_exec(
            "firecracker",
            "--api-sock",
            api_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        try:
            for _ in range(50):
                if os.path.exists(api_path):
                    break
                await asyncio.sleep(0.1)
            else:
                raise ExecError("fc API socket never appeared")

            async def _put(path, body):
                s, msg = await _fc_api(api_path, "PUT", path, body)
                if s not in (200, 204):
                    raise ExecError(f"fc {path} failed ({s}): {msg}")

            await _put("/boot-source", {
                "kernel_image_path": "/opt/firecracker/vmlinux",
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off",
            })
            await _put("/drives", {
                "drive_id": "rootfs",
                "path_on_host": "/opt/firecracker/rootfs.ext4",
                "is_root_device": True,
                "is_read_only": True,
            })
            await _put("/vsock", {"guest_cid": 3, "uds_path": vsock_path})
            await _put("/machine-config", {
                "vcpu_count": 1,
                "mem_size_mib": 256,
                "smt": False,
            })
            await _put("/actions", {"action_type": "InstanceStart"})

            for _ in range(100):
                if os.path.exists(vsock_path):
                    break
                await asyncio.sleep(0.1)
            else:
                raise ExecError("vsock socket never appeared")

            vr, vw = await asyncio.wait_for(
                asyncio.open_unix_connection(vsock_path), timeout=10
            )

            data = json.dumps({"agent": agent, "payload": payload}).encode()
            hdr = struct.pack(
                "<QQIIIIHHII",
                2, 3, 5201, 1024, len(data), 1, 5, 0, 65536, 0,
            )
            body = hdr + data
            vw.write(struct.pack("<I", len(body)) + body)
            await vw.drain()

            raw = await asyncio.wait_for(vr.readexactly(4), timeout=timeout)
            rlen = struct.unpack("<I", raw)[0]
            rdata = await asyncio.wait_for(vr.readexactly(rlen), timeout=timeout)
            result = json.loads(rdata[44:].decode())

            vw.close()
            try:
                await _put("/actions", {"action_type": "InstanceHalt"})
            except ExecError:
                pass

            return result

        except (asyncio.TimeoutError, ConnectionError, OSError, json.JSONDecodeError) as e:
            crashes.labels(agent=agent).inc()
            raise ExecError(f"{agent}: {e}") from e
        finally:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()


_RUNNERS = {
    "subprocess": _subprocess,
    "docker": _docker,
    "firecracker": _firecracker,
}


def pick(backend=None):
    b = backend or _SETTINGS.sandbox_backend
    fn = _RUNNERS.get(b)
    if not fn:
        raise ValueError(f"unknown backend: {b}")
    return fn


class _Lease:
    def __init__(self, pool, runner):
        self._pool = pool
        self._runner = runner

    async def run(self, agent, payload):
        return await self._runner(agent, payload)

    async def release(self):
        await self._pool.release(self._runner)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.release()


class Pool:
    def __init__(self):
        self._idle = []
        self._total = 0
        self._lock = asyncio.Lock()
        self._stop = False

    async def start(self):
        for _ in range(_SETTINGS.sandbox_min_pool):
            await self._add()
        pool_size.set(len(self._idle))

    async def stop(self):
        self._stop = True
        async with self._lock:
            self._idle.clear()
            self._total = 0
        pool_size.set(0)

    async def _add(self):
        if self._total >= _SETTINGS.sandbox_max_pool:
            return
        async with self._lock:
            self._idle.append(pick())
            self._total += 1
        pool_size.set(len(self._idle))

    async def acquire(self, timeout=None):
        deadline = time.monotonic() + (timeout or _SETTINGS.sandbox_acquire_timeout)
        if timeout is None:
            timeout = _SETTINGS.sandbox_acquire_timeout
        for attempt in range(_SETTINGS.sandbox_acquire_retries + 1):
            async with self._lock:
                runner = self._idle.pop() if self._idle else None
            if runner:
                pool_wait.observe(time.monotonic() - deadline + timeout)
                pool_size.set(len(self._idle))
                return _Lease(self, runner)
            remaining = round(deadline - time.monotonic(), 2)
            warn("pool_retry", attempt=attempt, remaining=remaining)
            await asyncio.sleep(_SETTINGS.sandbox_acquire_retry_delay)
        raise Empty(f"pool empty after {timeout}s")

    async def release(self, runner):
        async with self._lock:
            if self._total <= _SETTINGS.sandbox_max_pool:
                self._idle.append(runner)
        pool_size.set(len(self._idle))


_pool_instance = None


def get_pool():
    global _pool_instance
    if _pool_instance is None:
        _pool_instance = Pool()
    return _pool_instance
