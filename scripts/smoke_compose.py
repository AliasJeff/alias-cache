#!/usr/bin/env python3
"""Build and exercise the Docker Compose cluster through failover and restart."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from smoke_cluster import MASTER_NAME, RedisError, info_values, request, wait_for


def retry_until_leader(
    port: int,
    *arguments: str,
    attempts: int = 40,
    delay: float = 0.25,
) -> object:
    """Send a command, retrying while the cluster is re-electing a leader.

    A freshly elected leader briefly reports "not known" on client_write
    while openraft finishes confirming leadership; a real client retries
    such transient errors, and so must the smoke test on slow runners.
    """
    last_error: RedisError | None = None
    for _ in range(attempts):
        try:
            return request(port, *arguments)
        except RedisError as error:
            text = str(error)
            if "leader is not known" not in text and "TRYAGAIN" not in text:
                raise
            last_error = error
            time.sleep(delay)
    raise last_error if last_error else RuntimeError("retry exhausted")


CLIENT_PORTS = (6379, 6380, 6381)
NODE_IPS = ("172.30.255.11", "172.30.255.12", "172.30.255.13")
NODE_NAMES = ("node-1", "node-2", "node-3")


def compose(repository: Path, project: str, *arguments: str) -> None:
    subprocess.run(
        ["docker", "compose", "--project-name", project, *arguments],
        cwd=repository,
        check=True,
    )


def leader_id(reachable_nodes: set[int]) -> int | None:
    for node_id in sorted(reachable_nodes):
        try:
            response = request(
                CLIENT_PORTS[node_id - 1],
                "SENTINEL",
                "GET-MASTER-ADDR-BY-NAME",
                MASTER_NAME,
            )
            if (
                isinstance(response, list)
                and len(response) == 2
                and isinstance(response[0], bytes)
            ):
                address = response[0].decode()
                if address in NODE_IPS:
                    return NODE_IPS.index(address) + 1
        except (ConnectionError, OSError, ValueError):
            continue
    return None


def main() -> int:
    repository = Path(__file__).resolve().parents[1]
    project = f"alias-cache-ci-{os.getpid()}-{time.time_ns() % 1_000_000}"
    succeeded = False
    try:
        compose(repository, project, "up", "--build", "--detach")

        def all_nodes_readable() -> bool:
            return all(
                info_values(port, "health").get("readable") == "true"
                for port in CLIENT_PORTS
            )

        wait_for(
            "all Compose nodes to become readable",
            lambda: True if all_nodes_readable() else None,
        )
        original_leader = wait_for(
            "initial Compose leader", lambda: leader_id({1, 2, 3})
        )
        follower = next(node for node in (1, 2, 3) if node != original_leader)
        assert (
            retry_until_leader(
                CLIENT_PORTS[follower - 1], "SET", "compose:before", "committed"
            )
            == b"OK"
        )

        compose(repository, project, "kill", "--signal", "SIGKILL", NODE_NAMES[original_leader - 1])
        survivors = {node for node in (1, 2, 3) if node != original_leader}
        elected_leader = wait_for(
            "leader failover with two Compose nodes",
            lambda: (
                current
                if (current := leader_id(survivors)) is not None
                and current != original_leader
                else None
            ),
        )
        writer = next(node for node in survivors if node != elected_leader)
        assert (
            retry_until_leader(
                CLIENT_PORTS[writer - 1], "SET", "compose:after", "failover"
            )
            == b"OK"
        )
        committed_index = int(
            info_values(CLIENT_PORTS[elected_leader - 1], "replication")[
                "cluster_committed_index"
            ]
        )

        compose(repository, project, "start", NODE_NAMES[original_leader - 1])

        def restarted_node_caught_up() -> bool:
            applied = int(
                info_values(CLIENT_PORTS[original_leader - 1], "replication")[
                    "last_applied_index"
                ]
            )
            return applied >= committed_index

        wait_for(
            "restarted Compose node to apply committed entries",
            lambda: True if restarted_node_caught_up() else None,
        )
        assert (
            retry_until_leader(
                CLIENT_PORTS[original_leader - 1], "GET", "compose:after"
            )
            == b"failover"
        )
        print(
            "Compose smoke test passed: "
            f"leader {original_leader} -> {elected_leader}; "
            f"restarted node applied through index {committed_index}"
        )
        succeeded = True
        return 0
    finally:
        if not succeeded:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "--project-name",
                    project,
                    "logs",
                    "--no-color",
                ],
                cwd=repository,
                check=False,
            )
        subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                project,
                "down",
                "--volumes",
                "--remove-orphans",
            ],
            cwd=repository,
            check=False,
        )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Compose smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1)
