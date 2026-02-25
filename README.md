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

## Features

- Launches each Dagster run as an isolated Swarm service (replicas=1, restart=none)
- Run resume support for interrupted runs
- Health checking via Swarm task state inspection
- Automatic cleanup of completed/failed Swarm services via background thread
- Private registry authentication
- NFS and custom volume driver mounts
- Passthrough `service_kwargs` for advanced Swarm service configuration

## Requirements

- Docker Swarm mode enabled (`docker swarm init`)
- Dagster daemon must have access to the Docker socket
- `DAGSTER_CURRENT_IMAGE` env var set on the daemon/webserver if not specifying `image` in config
