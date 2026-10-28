# Changelog

## 0.2.3

### Fixed

- The cleanup sweep removes the Swarm service of every finished run, whatever
  its task is doing. Before, it removed a service only once Swarm reported the
  task ended. A run that Dagster's monitoring daemon failed on its start
  timeout kept a pending task forever, holding addresses on the overlay
  network, and the worker of a run cancelled from the UI kept running. Now a
  CANCELED or CANCELING run's worker is stopped on the next sweep, and a worker
  still running 60 seconds after its run ended SUCCESS or FAILURE is stopped.
  Run storage is read once per sweep; if it cannot be read, the sweep removes
  nothing.
- Terminating a run from a process without the Docker socket (normally the
  webserver) marks the run CANCELING and leaves the service to the daemon's
  next sweep, instead of failing. The same applies when Docker returns an error
  during `terminate`.
- A transient Docker API error during a health check (a 503 during a manager
  election, say) no longer reports the run's worker as not found, which made
  run monitoring fail a healthy run. Only a 404 means "no service"; other
  errors propagate and monitoring checks again on its next poll.
- Health checks, sweeps and `terminate` no longer log in to the registry. Only
  a launch does, as only creating a service needs registry credentials.
- Setting `labels` in `service_kwargs` no longer fails every launch with a
  TypeError. Your labels are merged with the launcher's `dagster/run_id` and
  `dagster/job_name`, which win on conflict. The other keys the launcher sets
  itself (`args`, `env`, `image`, `mode`, `mounts`, `name`, `networks`,
  `restart_policy`, `secrets`) still fail the launch, now with an error naming
  each key and the field to use instead.
- Mount entries pass `read_only`, `consistency`, `propagation`, `no_copy`,
  `labels`, `tmpfs_size` and `tmpfs_mode` to Docker. They were dropped before,
  so a mount with `read_only: true` was writable. A `tmpfs` mount no longer
  needs a `source`, and unknown keys are logged as ignored.
- The sdist contains only the package, tests and metadata. Earlier sdists could
  include untracked files from the machine that built them.

### Upgrade notes

- Set `run_monitoring.cancel_timeout_seconds` to at least `cleanup_interval` +
  60 (420 with the default interval), so that a run cancelled from the UI
  records its own cancellation. See *Terminating runs* in the README.
- A mount with `read_only: true` is now read-only.

### Other

- Tested on Python 3.10 to 3.14, and from the lowest supported versions
  (`dagster` 1.9.0, `docker` 7.0.0) to the latest.
- Continuous integration on GitHub Actions.

## 0.2.2 (2026-10-14)

- Find a run's service by its `dagster/run_id` label when the run's
  `swarm/service_id` tag is missing or names a service that no longer exists,
  instead of reporting a live run as not found.

## 0.2.1 (2026-08-20)

- Stop the cleanup thread in a process that has no Docker socket (the
  webserver), instead of logging an error on every sweep.

## 0.2.0 (2026-05-28)

- Per-code-location configuration through `container_context["docker_swarm"]`.
- Swarm secrets, mounted at `/run/secrets/<filename>`.

Earlier releases: see the git history.
