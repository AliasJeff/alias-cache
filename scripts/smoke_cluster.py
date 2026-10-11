#!/usr/bin/env python3
"""Exercise three-node startup, leader failover, and process restart."""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import BinaryIO, Callable, TypeVar


T = TypeVar("T")
HOST = "127.0.0.1"
MASTER_NAME = "alias-cache"
WAIT_SECONDS = 20.0


class RedisError(RuntimeError):
    pass


def encode_command(arguments: tuple[str, ...]) -> bytes:
    parts = [f"*{len(arguments)}\r\n".encode()]
    for argument in arguments:
        value = argument.encode()
        parts.extend((f"${len(value)}\r\n".encode(), value, b"\r\n"))
    return b"".join(parts)


def read_line(stream: BinaryIO) -> bytes:
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise ConnectionError("incomplete RESP line")
    return line[:-2]


def read_frame(stream: BinaryIO) -> object:
    marker = stream.read(1)
    if not marker:
        raise ConnectionError("server closed the connection")
    line = read_line(stream)
    if marker == b"+":
        return line
    if marker == b"-":
        raise RedisError(line.decode(errors="replace"))
    if marker == b":":
        return int(line)
    if marker == b"$":
        length = int(line)
        if length < 0:
            return None
        value = stream.read(length)
        if len(value) != length or stream.read(2) != b"\r\n":
            raise ConnectionError("incomplete RESP bulk string")
        return value
    if marker == b"*":
        count = int(line)
        if count < 0:
            return None
        return [read_frame(stream) for _ in range(count)]
    raise ValueError(f"unsupported RESP2 frame marker: {marker!r}")


def request(port: int, *arguments: str) -> object:
    with socket.create_connection((HOST, port), timeout=2.0) as connection:
        connection.sendall(encode_command(tuple(arguments)))
        with connection.makefile("rb") as stream:
            return read_frame(stream)


def info_values(port: int, section: str) -> dict[str, str]:
    response = request(port, "INFO", section)
    if not isinstance(response, bytes):
        raise ValueError(f"INFO returned {response!r}")
    values: dict[str, str] = {}
    for line in response.decode(errors="replace").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            values[key] = value
    return values


def find_leader(client_ports: list[int], reachable: set[int] | None = None) -> int | None:
    for port in client_ports:
        if reachable is not None and port not in reachable:
            continue
        try:
            response = request(port, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", MASTER_NAME)
            if (
                isinstance(response, list)
                and len(response) == 2
                and response[0] == HOST.encode()
                and isinstance(response[1], bytes)
            ):
                leader_port = int(response[1])
                if leader_port in client_ports:
                    return leader_port
        except (ConnectionError, OSError, RedisError, ValueError):
            continue
    return None


def wait_for(description: str, check: Callable[[], T | None]) -> T:
    deadline = time.monotonic() + WAIT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result is not None and result is not False:
                return result
        except (AssertionError, ConnectionError, OSError, RedisError, ValueError) as error:
            last_error = error
        time.sleep(0.1)
    suffix = f": {last_error}" if last_error else ""
    raise RuntimeError(f"timed out waiting for {description}{suffix}")


def reserve_ports(count: int) -> list[int]:
    listeners: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind((HOST, 0))
            listeners.append(listener)
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def build_command(
    binary: Path,
    node_id: int,
    client_ports: list[int],
    raft_ports: list[int],
    data_dir: Path,
) -> list[str]:
    command = [
        str(binary),
        "--node-id",
        str(node_id),
        "--client-addr",
        f"{HOST}:{client_ports[node_id - 1]}",
        "--raft-addr",
        f"{HOST}:{raft_ports[node_id - 1]}",
        "--data-dir",
        str(data_dir),
    ]
    for peer_id, (client_port, raft_port) in enumerate(
        zip(client_ports, raft_ports), start=1
    ):
        command.extend(
            ["--peer", f"{peer_id}={HOST}:{raft_port}/{HOST}:{client_port}"]
        )
    if node_id == 1:
        command.append("--bootstrap")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/release/alias-cache-cli"),
        help="path to alias-cache-cli (built automatically when absent)",
    )
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    binary = args.binary if args.binary.is_absolute() else repository / args.binary

    if not binary.exists():
        subprocess.run(
            ["cargo", "build", "--release", "--locked", "-p", "alias-cache-cli"],
            cwd=repository,
            check=True,
        )
    if not binary.is_file():
        raise FileNotFoundError(f"alias-cache binary not found: {binary}")

    client_ports = reserve_ports(3)
    raft_ports = reserve_ports(3)
    workdir = Path(tempfile.mkdtemp(prefix="alias-cache-smoke-"))
    processes: dict[int, subprocess.Popen[bytes]] = {}
    logs: dict[int, BinaryIO] = {}
    success = False

    def start_node(node_id: int) -> None:
        previous_log = logs.get(node_id)
        if previous_log is not None:
            previous_log.close()
        log = (workdir / f"node-{node_id}.log").open("wb")
        process = subprocess.Popen(
            build_command(
                binary,
                node_id,
                client_ports,
                raft_ports,
                workdir / f"node-{node_id}",
            ),
            cwd=repository,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "RUST_LOG": os.environ.get("RUST_LOG", "warn")},
        )
        processes[node_id] = process
        logs[node_id] = log

    def stop_all() -> None:
        for process in processes.values():
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + 5
        for process in processes.values():
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs.values():
            log.close()

    try:
        for node_id in range(1, 4):
            start_node(node_id)

        def all_nodes_readable() -> bool:
            return all(
                info_values(port, "health").get("readable") == "true"
                for port in client_ports
            )

        wait_for(
            "three-node cluster readability",
            lambda: True if all_nodes_readable() else None,
        )
        old_leader_port = wait_for(
            "initial leader", lambda: find_leader(client_ports)
        )
        old_leader_id = client_ports.index(old_leader_port) + 1
        follower_id = next(
            node_id for node_id in range(1, 4) if node_id != old_leader_id
        )

        assert request(client_ports[follower_id - 1], "SET", "smoke:before", "committed") == b"OK"
        assert request(client_ports[(follower_id % 3)], "GET", "smoke:before") == b"committed"

        processes[old_leader_id].kill()
        processes[old_leader_id].wait(timeout=5)
        reachable_ports = {
            port for node_id, port in enumerate(client_ports, start=1) if node_id != old_leader_id
        }
        new_leader_port = wait_for(
            "leader failover",
            lambda: (
                leader
                if (leader := find_leader(client_ports, reachable_ports)) is not None
                and leader != old_leader_port
                else None
            ),
        )
        new_leader_id = client_ports.index(new_leader_port) + 1
        survivor_id = next(
            node_id
            for node_id in range(1, 4)
            if node_id not in (old_leader_id, new_leader_id)
        )
        assert request(client_ports[survivor_id - 1], "SET", "smoke:after", "failover") == b"OK"
        assert request(client_ports[new_leader_id - 1], "GET", "smoke:after") == b"failover"
        committed_index = int(
            info_values(new_leader_port, "replication")["cluster_committed_index"]
        )

        start_node(old_leader_id)
        restarted_port = client_ports[old_leader_id - 1]

        def restarted_node_caught_up() -> bool:
            applied_index = int(
                info_values(restarted_port, "replication")["last_applied_index"]
            )
            return applied_index >= committed_index

        wait_for("restarted node to apply committed state", lambda: True if restarted_node_caught_up() else None)
        assert request(restarted_port, "GET", "smoke:before") == b"committed"
        assert request(restarted_port, "GET", "smoke:after") == b"failover"
        print(
            "cluster smoke test passed: "
            f"leader {old_leader_id} -> {new_leader_id}; "
            f"restarted node {old_leader_id} applied through index {committed_index}"
        )
        success = True
        return 0
    finally:
        stop_all()
        if success:
            shutil.rmtree(workdir)
        else:
            print(f"smoke-test logs preserved at {workdir}", file=sys.stderr)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1)
