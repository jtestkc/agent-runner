#!/usr/bin/env bash
set -euo pipefail

ROOTFS_SIZE_MB="${ROOTFS_SIZE_MB:-256}"
ROOTFS_OUT="${ROOTFS_OUT:-/opt/firecracker/rootfs.ext4}"
KERNEL_OUT="${KERNEL_OUT:-/opt/firecracker/vmlinux}"
WORK_DIR="$(mktemp -d)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Building Firecracker rootfs (${ROOTFS_SIZE_MB}MiB) at ${ROOTFS_OUT}"

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

ROOT="$WORK_DIR/rootfs"
mkdir -p "$ROOT"

apt-get update -qq
apt-get install -y -qq --no-install-recommends \
    debootstrap \
    genext2fs \
    wget \
    xz-utils \
    ca-certificates

debootstrap --variant=minbase --include=python3,python3-json,python3-importlib-metadata \
    noble "$ROOT" http://archive.ubuntu.com/ubuntu/

mkdir -p "$ROOT/opt/agent-runner/scripts"
mkdir -p "$ROOT/opt/agent-runner/src/agent_runner"

cp "$SCRIPT_DIR/fc_agent.py" "$ROOT/opt/agent-runner/scripts/"
cp "$SCRIPT_DIR/agent_worker.py" "$ROOT/opt/agent-runner/scripts/"
cp -r "$SCRIPT_DIR/../src/agent_runner" "$ROOT/opt/agent-runner/src/"

cat > "$ROOT/etc/init.d/fc-agent" << 'INIT'
#!/bin/sh
### BEGIN INIT INFO
# Provides:          fc-agent
# Required-Start:    $local_fs
# Required-Stop:     $local_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Firecracker vsock agent
### END INIT INFO

case "$1" in
  start)
    /opt/agent-runner/scripts/fc_agent.py &
    echo $! > /var/run/fc-agent.pid
    ;;
  stop)
    kill "$(cat /var/run/fc-agent.pid 2>/dev/null)" 2>/dev/null || true
    ;;
esac
INIT
chmod +x "$ROOT/etc/init.d/fc-agent"

chroot "$ROOT" update-rc.d fc-agent defaults

rm -f "$ROOT/var/cache/apt/archives/"*.deb
rm -rf "$ROOT/var/lib/apt/lists"/*

ROOT_MNT="$WORK_DIR/mnt"
mkdir -p "$ROOT_MNT"
dd if=/dev/zero of="$WORK_DIR/rootfs.ext4" bs=1M count="$ROOTFS_SIZE_MB" status=none
mkfs.ext4 -q "$WORK_DIR/rootfs.ext4"
mount -o loop "$WORK_DIR/rootfs.ext4" "$ROOT_MNT"
cp -a "$ROOT"/* "$ROOT_MNT/"
umount "$ROOT_MNT"
e2fsck -fy "$WORK_DIR/rootfs.ext4" 2>/dev/null || true

mkdir -p "$(dirname "$ROOTFS_OUT")"
cp "$WORK_DIR/rootfs.ext4" "$ROOTFS_OUT"

echo "==> Rootfs built: ${ROOTFS_OUT} ($(du -h "$ROOTFS_OUT" | cut -f1))"

if [ ! -f "$KERNEL_OUT" ]; then
    echo "==> Downloading Firecracker kernel"
    mkdir -p "$(dirname "$KERNEL_OUT")"
    wget -qO "$KERNEL_OUT" \
        "https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux-6.1.bin"
    echo "==> Kernel downloaded: ${KERNEL_OUT}"
fi

echo "==> Done"
