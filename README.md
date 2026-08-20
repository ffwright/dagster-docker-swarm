# dagster-docker-swarm

A [Dagster](https://dagster.io) run launcher that executes pipeline runs as
Docker Swarm services.

## Installation

pip install dagster-docker-swarm

## Configuration

Add to your `dagster.yaml`:

    run_launcher:
      module: dagster_docker_swarm
      class: SwarmRunLauncher
      config:
        image: my-registry/my-dagster-image:latest
        networks:
          - my_dagster_network
        env_vars:
          - DAGSTER_POSTGRES_HOST
          - DAGSTER_POSTGRES_DB
          - DAGSTER_CURRENT_IMAGE
        mounts:
          - target: /data
            source: shared_data
            type: volume

### Per-code-location config (`container_context`)

Each code location can extend the launcher's instance-level config by declaring
its own `container_context` on the `Definitions(...)` it serves. The launcher
reads the `docker_swarm` sub-dict and merges it with the launcher config at
run-launch time:

    # In a code location's repository.py
    from dagster import Definitions

    defs = Definitions(
        assets=[...],
        container_context={
            "docker_swarm": {
                "env_vars": [
                    "CLICKHOUSE_HOST=clickhouse",
                    "AZURE_HOST",                 # forwarded by name from the daemon env
                ],
                "networks": ["my_extra_network"],
                "secrets": [
                    {"secret_name": "myapp_db_password", "filename": "DB_PASSWORD"},
                    {"secret_name": "myapp_api_key"},  # filename defaults to secret_name
                ],
            },
        },
    )

Merge rules:

- `env_vars`, `networks`, `mounts`, `secrets`: launcher list + code-location list (concatenated)
- `registry`: code-location value replaces the launcher value when set
- `service_kwargs`: shallow dict merge, code-location keys win

### Swarm secrets

The `secrets` field on either the launcher or a code-location accepts entries
of the shape `{"secret_name": <swarm-secret-name>, "filename": <mount-name>}`.
The launcher resolves each name to its Swarm-assigned UUID at launch time and
attaches a `SecretReference` to the spawned service; the secret lands at
`/run/secrets/<filename>` inside the run container. If `filename` is omitted
it defaults to `secret_name`.

A missing Swarm secret raises `DagsterInvariantViolationError` before the
service is created, so launches fail fast with a named error.

    run_launcher:
      module: dagster_docker_swarm
      class: SwarmRunLauncher
      config:
        image: my-registry/my-dagster-image:latest
        secrets:
          - secret_name: shared_clickhouse_password
            filename: CLICKHOUSE_PASSWORD

### Service cleanup

Completed Swarm services are cleaned up by a background thread that runs every
`cleanup_interval` seconds (default 300). To run sweeps more frequently:

    run_launcher:
      module: dagster_docker_swarm
      class: SwarmRunLauncher
      config:
        cleanup_interval: 60   # sweep every 60 seconds
        image: my-registry/my-dagster-image:latest
        ...

Set `cleanup_interval: 0` to disable the background thread entirely (e.g. if
you prefer an external cron job).

The launcher is instantiated by *every* process that loads the Dagster
instance, but only the daemon is given the Docker socket. In a process without
`/var/run/docker.sock` (typically the webserver) the cleanup thread logs a
single warning and exits, rather than raising on every sweep. Transient Docker
errors — an engine restart, say — are retried as normal, so cleanup in the
daemon survives them.

## Features

- Launches each Dagster run as an isolated Swarm service (replicas=1, restart=none)
- Run resume support for interrupted runs
- Health checking via Swarm task state inspection
- Automatic cleanup of completed/failed Swarm services via background thread
- Private registry authentication
- NFS and custom volume driver mounts
- Passthrough `service_kwargs` for advanced Swarm service configuration
- Per-code-location config via `container_context` (env vars, networks, mounts, secrets)
- First-class Docker Swarm secrets (`SecretReference` mounted at `/run/secrets/`)

## Requirements

- Docker Swarm mode enabled (`docker swarm init`)
- Dagster daemon must have access to the Docker socket
- `DAGSTER_CURRENT_IMAGE` env var set on the daemon/webserver if not specifying `image` in config
