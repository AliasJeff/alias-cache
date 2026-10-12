#!/usr/bin/env python3
"""Measure per-node Raft append batching and local WAL sync costs."""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import BinaryIO


HOST = "127.0.0.1"
WAIT_SECONDS = 25.0
METRICS = (
    "raft_log_append_calls",
    "raft_log_appended_entries",
    "raft_log_append_batch_entries_max",
    "raft_log_sync_data_calls",
    "raft_log_sync_data_max_us",
)


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
        raise RuntimeError(line.decode(errors="replace"))
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
        return [read_frame(stream) for _ in range(count)] if count >= 0 else None
    raise ValueError(f"unsupported RESP2 frame marker: {marker!r}")


def request(port: int, *arguments: str) -> object:
    with socket.create_connection((HOST, port), timeout=2.0) as connection:
        connection.sendall(encode_command(tuple(arguments)))
        with connection.makefile("rb") as stream:
            return read_frame(stream)


def info(port: int, section: str) -> dict[str, str]:
    response = request(port, "INFO", section)
    if not isinstance(response, bytes):
        raise ValueError(f"INFO returned {response!r}")
    return {
        key: value
        for line in response.decode(errors="replace").splitlines()
        if ":" in line
        for key, value in [line.split(":", 1)]
    }


def wait_for_cluster(client_ports: list[int]) -> int:
    deadline = time.monotonic() + WAIT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            states = [info(port, "health") for port in client_ports]
            if not all(state.get("readable") == "true" for state in states):
                time.sleep(0.1)
                continue
            for port in client_ports:
                result = request(
                    port, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", "alias-cache"
                )
                if (
                    isinstance(result, list)
                    and len(result) == 2
                    and isinstance(result[1], bytes)
                ):
                    leader_port = int(result[1])
                    if leader_port in client_ports:
                        return leader_port
        except (ConnectionError, OSError, RuntimeError, ValueError) as error:
            last_error = error
        time.sleep(0.1)
    detail = f": {last_error}" if last_error else ""
    raise RuntimeError(f"three-node cluster did not elect a leader{detail}")


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
        "--binary", type=Path, default=Path("target/release/alias-cache-cli")
    )
    parser.add_argument(
        "--benchmark-binary",
        type=Path,
        default=Path("target/release/alias-cache-benchmark"),
    )
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--key-space", type=int, default=100_000)
    args = parser.parse_args()
    if min(args.clients, args.pipeline, args.duration, args.key_space) <= 0:
        parser.error("clients, pipeline, duration, and key-space must be positive")

    repository = Path(__file__).resolve().parents[1]
    binary = args.binary if args.binary.is_absolute() else repository / args.binary
    benchmark_binary = (
        args.benchmark_binary
        if args.benchmark_binary.is_absolute()
        else repository / args.benchmark_binary
    )
    for executable in (binary, benchmark_binary):
        if not executable.is_file():
            raise FileNotFoundError(
                f"{executable} is missing; run cargo build --release --locked first"
            )

    client_ports = reserve_ports(3)
    raft_ports = reserve_ports(3)
    workdir = Path(tempfile.mkdtemp(prefix="alias-cache-profile-"))
    processes: list[subprocess.Popen[bytes]] = []
    logs: list[BinaryIO] = []
    success = False
    try:
        for node_id in range(1, 4):
            log = (workdir / f"node-{node_id}.log").open("wb")
            logs.append(log)
            processes.append(
                subprocess.Popen(
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
            )

        leader_port = wait_for_cluster(client_ports)
        before = [info(port, "stats") for port in client_ports]
        command = [
            str(benchmark_binary),
            "--address",
            f"{HOST}:{leader_port}",
            "--command",
            "set",
            "--mode",
            "pwrite",
            "--clients",
            str(args.clients),
            "--pipeline",
            str(args.pipeline),
            "--time",
            str(args.duration),
            "--value-size",
            str(args.value_size),
            "--key-space",
            str(args.key_space),
        ]
        result = subprocess.run(
            command,
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
        )
        print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        if result.returncode:
            raise RuntimeError(f"benchmark exited with status {result.returncode}")

        time.sleep(0.5)
        after = [info(port, "stats") for port in client_ports]
        print("per-node WAL metrics (counter deltas and interval averages):")
        for index, (initial, final) in enumerate(zip(before, after), start=1):
            role = "leader" if client_ports[index - 1] == leader_port else "follower"
            print(f"node {index} ({role}):")
            for metric in METRICS:
                old = int(initial.get(metric, "0"))
                new = int(final.get(metric, "0"))
                print(f"  {metric}: before={old} after={new} delta={new - old}")
            append_calls = int(final.get("raft_log_append_calls", "0")) - int(
                initial.get("raft_log_append_calls", "0")
            )
            appended_entries = int(
                final.get("raft_log_appended_entries", "0")
            ) - int(initial.get("raft_log_appended_entries", "0"))
            sync_calls = int(final.get("raft_log_sync_data_calls", "0")) - int(
                initial.get("raft_log_sync_data_calls", "0")
            )
            sync_total_us = int(
                final.get("raft_log_sync_data_total_us", "0")
            ) - int(initial.get("raft_log_sync_data_total_us", "0"))
            print(
                "  interval_append_batch_entries_avg:"
                f"{appended_entries / append_calls if append_calls else 0:.2f}"
            )
            print(
                "  interval_sync_data_avg_us:"
                f"{sync_total_us / sync_calls if sync_calls else 0:.1f}"
            )
        success = True
        return 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 5
        for process in processes:
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        if success:
            shutil.rmtree(workdir)
        else:
            print(f"profile logs preserved at {workdir}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
