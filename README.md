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
        service_kwargs:
          constraints:
            - "node.role == worker"   # keep run workers off manager nodes

### Mounts

Each `mounts` entry needs a `target`. `type` is `volume` (the default), `bind`
or `tmpfs`, and `source` names the volume or host path (a `tmpfs` mount has
none). `driver_config` (`Name`, `Options`) selects a volume driver, such as
`local` with NFS options. `read_only`, `consistency`, `propagation`, `no_copy`,
`labels`, `tmpfs_size` and `tmpfs_mode` are passed to Docker as given. Any other
key is logged and ignored.

### `service_kwargs`

`service_kwargs` is passed to docker-py's `services.create`, so it can set any
part of the Swarm service spec that the launcher does not set itself. Write
values that docker-py models as types (`resources`, `endpoint_spec`, ...) in the
Docker Engine API's shape: docker-py forwards a dict unchanged, and the engine
ignores keys it does not know without reporting an error.

    service_kwargs:
      constraints:
        - "node.role == worker"
      resources:
        Limits:
          MemoryBytes: 17179869184    # 16 GiB
          NanoCPUs: 4000000000        # 4 CPUs
      stop_grace_period: 30000000000  # 30 s, in nanoseconds

Swarm places a run's service on any node, managers included, unless a
constraint says otherwise. `stop_grace_period` is the time between the SIGTERM a
worker receives when its service is removed (by `terminate` or the cleanup
sweep) and a SIGKILL; Swarm's default is 10 seconds.

`labels` are added to the service's labels, but the launcher's
`dagster/run_id` and `dagster/job_name` labels cannot be replaced. The other
keys the launcher sets itself (`args`, `env`, `image`, `mode`, `mounts`, `name`,
`networks`, `restart_policy`, `secrets`) fail the launch with an error that
names the field to use instead.

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
- `service_kwargs`: shallow dict merge, code-location keys win. A code location
  that sets `constraints` replaces the launcher's, so it must repeat
  `node.role == worker` to stay off manager nodes.

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

A background thread removes the Swarm services of runs that no longer need them,
every `cleanup_interval` seconds (default 300). On each sweep it reads the runs
behind all `dagster/run_id`-labelled services from run storage in one query,
and removes a service when:

- the run is SUCCESS, FAILURE or CANCELED, whatever the task is doing. This
  includes tasks that never started: Dagster's monitoring daemon fails a run
  that exceeds its start timeout without asking the launcher to stop it, and
  each leftover service holds addresses on the overlay network. A worker still
  running 60 seconds after its run ended SUCCESS or FAILURE is stopped (the
  wait lets a worker that finished normally shut down); a CANCELED run's worker
  is stopped straight away.
- the run is CANCELING (see *Terminating runs* below).
- the run is not in run storage and Swarm reports the task finished. A service
  with no run in storage and a task still pending or running is left alone, as
  it may belong to another Dagster instance on the same swarm.

Services of runs that are STARTING or STARTED are never touched. If run
storage cannot be read, the sweep removes nothing and tries again next time.
To run sweeps more frequently:

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

### Terminating runs

Dagster calls `terminate` in the process that served the request, so a run
terminated from the UI is terminated inside the webserver, which has no Docker
socket. There the launcher marks the run CANCELING, records an engine event, and
leaves the rest to the daemon: its next sweep removes the run's service, the
worker receives SIGTERM, and Dagster records the run as CANCELED. Expect the
worker to stop within one `cleanup_interval`, with either terminate option in
the UI. If Dagster's cancel timeout (`run_monitoring.cancel_timeout_seconds`,
default 180) runs out first, Dagster marks the run CANCELED with a timeout
message instead, and the sweep still stops the worker on its next pass. Set the
cancel timeout to at least `cleanup_interval` + 60 to let the worker record the
cancellation itself. If Docker returns an error while `terminate` removes the
service, the run is likewise left CANCELING for the sweep. This relies on the
daemon and the webserver reading the same `cleanup_interval`; with
`cleanup_interval: 0`, `terminate` in a process without the socket reports that
it cannot stop the run.

### Running the daemon away from manager nodes

Swarm services can only be created, inspected and removed through a manager's
Docker API, which is why the daemon usually runs on a manager with
`/var/run/docker.sock` mounted. The launcher builds its Docker client with
docker-py's `from_env()`, so it also honours `DOCKER_HOST`, `DOCKER_TLS_VERIFY`
and `DOCKER_CERT_PATH`: a daemon on a worker node can manage services through a
manager's API at a `tcp://` address. Protect that endpoint with mutual TLS,
because anyone who can reach a manager's Docker API controls the whole swarm.

With `DOCKER_HOST` set to a `tcp://` address, the launcher cannot tell an
endpoint that is missing from one that is briefly down, so a process that
cannot reach it logs a warning on every sweep instead of once. Leave
`DOCKER_HOST` unset in the webserver unless it can reach the endpoint too.

## Images on Swarm nodes

Each node pulls the image of every run it is given, and Swarm never deletes
images. Removing a run's service removes its containers and leaves the image
unused, but deleting unused images is up to you, on every node:
`docker image prune` against one Docker endpoint prunes only that node. One way
is to label your images when you build them (`LABEL app=dagster`) and run
`docker image prune -a --filter label=app=dagster --filter until=168h` on each
node, from cron or a Swarm `global-job` service. `-a` skips any image that a
container still uses, stopped containers included, which is why it matters that
the launcher removes the services of finished runs.

## Security

- The Docker socket, or a manager's Docker API, gives root on the swarm. Give it
  to the daemon only; the webserver works without it (see *Terminating runs*).
- `container_context` is trusted configuration. Whoever deploys a code location
  can bind-mount any host path into its runs (the Docker socket included), add
  capabilities or privileges through `service_kwargs`, place its runs on any
  node, and copy any of the daemon's environment variables into its runs by
  naming them in `env_vars`. Dagster's own Docker run launcher works the same
  way. Only deploy code locations from people you would trust with the cluster.
- Environment variables, including those forwarded from the daemon, are stored
  in the service spec, where anyone with access to the Docker API can read them
  with `docker service inspect`. Pass credentials as Swarm `secrets` instead.
- Each run's command contains the serialized Dagster instance, including its
  `dagster.yaml` configuration. Give credentials there, the registry password
  included, as `env:` references rather than literal values, so the command
  carries the variable's name and not its value.

## Features

- Launches each Dagster run as an isolated Swarm service (replicas=1, restart=none)
- Run resume support for interrupted runs
- Health checking via Swarm task state inspection
- Automatic cleanup of finished runs' Swarm services via background thread
- Private registry authentication
- Volume, bind and tmpfs mounts, including NFS and other volume drivers
- Passthrough `service_kwargs` for advanced Swarm service configuration
- Per-code-location config via `container_context` (env vars, networks, mounts, secrets)
- First-class Docker Swarm secrets (`SecretReference` mounted at `/run/secrets/`)

## Requirements

- Docker Swarm mode enabled (`docker swarm init`)
- The Dagster daemon needs a Swarm manager's Docker API: the Docker socket on a
  manager node, or `DOCKER_HOST` (see *Running the daemon away from manager
  nodes*)
- `DAGSTER_CURRENT_IMAGE` env var set on the daemon/webserver if not specifying `image` in config
- Python 3.10 or later, `dagster` 1.9 or later
