import logging
import os
import threading
import time
from typing import Any, Optional

import dagster._check as check
import docker
from dagster._config import Array, Field, IntSource, Permissive, StringSource
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunRecord, RunsFilter
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._core.utils import parse_env_var
from dagster._grpc.types import ExecuteRunArgs, ResumeRunArgs
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from docker import DockerClient
from docker.models.services import Service
from docker.types import DriverConfig, Mount, RestartPolicy, SecretReference, ServiceMode
from docker_image import reference

from dagster_docker_swarm.container_context import SwarmContainerContext

logger = logging.getLogger("dagster_docker_swarm")

SWARM_SERVICE_ID_TAG = "swarm/service_id"

DEFAULT_DOCKER_SOCKET_PATH = "/var/run/docker.sock"
_UNIX_SOCKET_SCHEMES = ("unix://", "http+unix://")


class _MissingDockerSocketError(Exception):
    """Raised when the Docker unix socket is absent from this process.

    Distinct from a transient Docker error: a missing socket is a property of
    how the process was started (the Dagster webserver deliberately does not
    mount /var/run/docker.sock), so retrying cannot help.
    """

    def __init__(self, socket_path: str):
        super().__init__(f"No Docker socket at {socket_path}")
        self.socket_path = socket_path


def _missing_docker_socket_path() -> Optional[str]:
    """Return the Docker unix socket path if Docker would use one and it is absent.

    Returns ``None`` when the socket exists, or when ``DOCKER_HOST`` points at a
    non-unix transport (tcp://, ssh://) — there we cannot cheaply tell a
    permanent misconfiguration from a transient daemon outage, so the caller
    should treat the failure as transient.
    """
    docker_host = os.environ.get("DOCKER_HOST")
    if not docker_host:
        path = DEFAULT_DOCKER_SOCKET_PATH
    else:
        for scheme in _UNIX_SOCKET_SCHEMES:
            if docker_host.startswith(scheme):
                path = docker_host[len(scheme) :] or DEFAULT_DOCKER_SOCKET_PATH
                break
        else:
            return None
    return None if os.path.exists(path) else path


# services.create arguments the launcher sets itself, with what to do instead.
# docker-py raises a TypeError naming none of the user's config if
# service_kwargs passes one of them again.
_SERVICE_KWARGS_SET_BY_LAUNCHER = {
    "args": "it is the run's dagster command",
    "env": "use env_vars",
    "image": "use the code location's image or the launcher's image field",
    "mode": "each run is a single replica",
    "mounts": "use mounts",
    "name": "it is derived from the run ID",
    "networks": "use networks",
    "restart_policy": "Swarm must never restart a run",
    "secrets": "use secrets",
}

# docker.types.Mount options passed through from a mounts entry as given.
_MOUNT_OPTIONS = ("read_only", "consistency", "propagation", "no_copy", "labels", "tmpfs_size", "tmpfs_mode")


def _check_service_kwargs(service_kwargs: dict[str, Any]) -> None:
    clashes = [
        f"{key} ({_SERVICE_KWARGS_SET_BY_LAUNCHER[key]})"
        for key in sorted(service_kwargs)
        if key in _SERVICE_KWARGS_SET_BY_LAUNCHER
    ]
    if clashes:
        raise DagsterInvariantViolationError(
            "service_kwargs cannot set what the launcher sets on the Swarm service itself: "
            + ", ".join(clashes)
        )


SWARM_CONFIG_SCHEMA = {
    "image": Field(StringSource, is_required=False),
    "registry": Field(
        {"url": Field(StringSource), "username": Field(StringSource), "password": Field(StringSource)},
        is_required=False,
    ),
    "env_vars": Field([str], is_required=False),
    "network": Field(StringSource, is_required=False),
    "networks": Field(Array(StringSource), is_required=False),
    "mounts": Field([Permissive()], is_required=False),
    "service_kwargs": Field(Permissive(), is_required=False),
    "secrets": Field([Permissive()], is_required=False),
    "cleanup_interval": Field(
        IntSource,
        is_required=False,
        default_value=300,
        description="Seconds between orphaned service cleanup sweeps. Set to 0 to disable.",
    ),
}


class SwarmRunLauncher(RunLauncher, ConfigurableClass):
    """Launches Dagster runs as Docker Swarm services.

    Each run gets a dedicated Swarm service with replicas=1 and
    restart_policy=none, ensuring one-shot execution semantics.

    Requires the Dagster daemon to have access to the Docker socket.

    Example dagster.yaml:

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
            mounts:
              - target: /data
                source: my_nfs_volume
                type: volume
                driver_config:
                  Name: local
                  Options:
                    type: nfs
                    o: "addr=nfs-server,rw"
                    device: ":/exports/data"
    """

    def __init__(
        self,
        inst_data: Optional[ConfigurableClassData] = None,
        image: Optional[str] = None,
        registry: Optional[dict[str, str]] = None,
        env_vars: Optional[list[str]] = None,
        network: Optional[str] = None,
        networks: Optional[list[str]] = None,
        mounts: Optional[list[dict[str, Any]]] = None,
        service_kwargs: Optional[dict[str, Any]] = None,
        secrets: Optional[list[dict[str, Any]]] = None,
        cleanup_interval: int = 300,
    ) -> None:
        self._inst_data = inst_data
        self.image = image
        self.registry = registry
        self.env_vars = env_vars
        self.mounts = mounts
        self.service_kwargs = service_kwargs
        self.secrets = secrets
        self._cleanup_interval = cleanup_interval
        self._cleanup_shutdown: Optional[threading.Event] = None
        self._cleanup_thread: Optional[threading.Thread] = None

        if network:
            self.networks: list[str] = [network]
        elif networks:
            self.networks = networks
        else:
            self.networks = []

        super().__init__()

        logger.info(
            "SwarmRunLauncher initialized",
            extra={
                "image": self.image,
                "networks": self.networks,
                "registry_configured": self.registry is not None,
                "mount_count": len(self.mounts) if self.mounts else 0,
            },
        )

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls) -> dict[str, Any]:
        return SWARM_CONFIG_SCHEMA

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: dict[str, Any]) -> "SwarmRunLauncher":
        return cls(inst_data=inst_data, **config_value)

    @property
    def supports_resume_run(self) -> bool:
        return True

    @property
    def supports_check_run_worker_health(self) -> bool:
        return True

    def register_instance(self, instance: Any) -> None:
        # Stop any existing cleanup thread before starting a new one
        # (guards against register_instance being called more than once).
        self._stop_cleanup_thread()
        super().register_instance(instance)
        if self._cleanup_interval > 0:
            self._cleanup_shutdown = threading.Event()
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                daemon=True,
                name="swarm-service-cleanup",
            )
            self._cleanup_thread.start()
            logger.info("Started Swarm service cleanup thread (interval=%ds)", self._cleanup_interval)

    def _stop_cleanup_thread(self) -> None:
        if self._cleanup_shutdown is not None:
            self._cleanup_shutdown.set()
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=10)
            self._cleanup_thread = None
            self._cleanup_shutdown = None
            logger.info("Stopped Swarm service cleanup thread")

    def dispose(self) -> None:
        self._stop_cleanup_thread()

    def _get_client(self) -> DockerClient:
        logger.debug("Creating Docker client from environment")
        client = docker.client.from_env()
        if self.registry:
            logger.debug("Logging in to registry %s", self.registry["url"])
            client.login(
                registry=self.registry["url"],
                username=self.registry["username"],
                password=self.registry["password"],
            )
        return client

    def _get_docker_image(self, job_code_origin: Any) -> str:
        docker_image = job_code_origin.repository_origin.container_image
        if docker_image:
            logger.debug("Using image from code origin: %s", docker_image)
        else:
            docker_image = self.image
            if docker_image:
                logger.debug("Using image from launcher config: %s", docker_image)
        if not docker_image:
            raise Exception("No docker image specified by the instance config or repository")
        try:
            reference.Reference.parse(docker_image)
        except Exception as e:
            raise Exception(f"Docker image name {docker_image} is not correctly formatted") from e
        return docker_image

    def _get_service(self, run: DagsterRun) -> Optional[Service]:
        """Return the run's Swarm service, or ``None`` if Docker says it does not exist.

        Only ``NotFound`` means "no service". Any other Docker API error (an
        engine restart, a 503 during a manager election) propagates: ``None``
        makes check_run_worker_health report NOT_FOUND, the monitoring daemon
        then fails the run, and the cleanup sweep removes the service of a
        worker that was healthy. Monitoring catches the exception per run and
        checks again on its next poll.
        """
        if not run:
            return None
        service_id = run.tags.get(SWARM_SERVICE_ID_TAG)
        if not service_id:
            return self._find_service_by_run_label(run)
        try:
            service = self._get_client().services.get(service_id)
            logger.debug("Found Swarm service %s for run %s", service_id, run.run_id)
            return service
        except docker.errors.NotFound:
            logger.warning("Swarm service %s not found for run %s", service_id, run.run_id)
            return self._find_service_by_run_label(run)

    def _find_service_by_run_label(self, run: DagsterRun) -> Optional[Service]:
        """Find a run's service by its dagster/run_id label when the run's
        service ID tag is missing or names a service that no longer exists.

        add_run_tags and handle_run_event both rewrite the run_body column from
        a read-then-write. If the daemon's add_run_tags lands after the run
        worker's PIPELINE_START read, the START write puts back the previous
        body: without the tag on a first launch, or with the old service's ID
        after a resume. The tag is still in the run_tags table, but run.tags
        comes from the body. The service is still labelled with the run id, so
        look it up that way instead of reporting a live run as NOT_FOUND.
        """
        # A Docker API error propagates; see _get_service.
        services = self._get_client().services.list(
            filters={"label": f"dagster/run_id={run.run_id}"}
        )
        if not services:
            logger.debug("No service labelled dagster/run_id=%s", run.run_id)
            return None
        # A resume creates a new service for the same run; if an earlier one
        # was never removed, the newest is the live one.
        service = max(services, key=lambda s: s.attrs.get("CreatedAt", ""))
        logger.warning(
            "Run %s: %s tag missing or stale; found service %s by its dagster/run_id label",
            run.run_id, SWARM_SERVICE_ID_TAG, service.id,
        )
        return service

    def _resolve_secret_references(
        self,
        client: DockerClient,
        secrets: list[dict[str, Any]],
        run: DagsterRun,
    ) -> list[SecretReference]:
        """Resolve a list of {secret_name, filename?} dicts to docker SecretReference objects.

        Names are resolved to Swarm-assigned UUIDs once per launch via
        ``client.secrets.list``. A missing secret raises before any service is
        created so launches fail fast with a named error.
        """
        if not secrets:
            return []

        refs: list[SecretReference] = []
        for entry in secrets:
            name = entry.get("secret_name")
            if not name:
                raise DagsterInvariantViolationError(
                    f"Swarm secret entry is missing 'secret_name': {entry!r}"
                )
            filename = entry.get("filename") or name

            matches = [s for s in client.secrets.list(filters={"name": name}) if s.name == name]
            if not matches:
                raise DagsterInvariantViolationError(
                    f"Swarm secret {name!r} declared on run {run.run_id} was not found on the swarm"
                )
            refs.append(SecretReference(secret_id=matches[0].id, secret_name=name, filename=filename))

        logger.debug("Resolved %d secret(s) for run %s", len(refs), run.run_id)
        return refs

    def _launch_service_with_command(
        self,
        run: DagsterRun,
        docker_image: str,
        command: list[str],
        container_context: SwarmContainerContext,
    ) -> None:
        service_name = f"dagster-run-{run.run_id[:8]}"
        logger.info(
            "Launching Swarm service for run %s (job: %s, image: %s, service: %s)",
            run.run_id, run.job_name, docker_image, service_name,
        )

        docker_env = dict([parse_env_var(env_var) for env_var in container_context.env_vars])
        docker_env["DAGSTER_RUN_JOB_NAME"] = run.job_name
        logger.debug("Resolved %d environment variables for run %s", len(docker_env), run.run_id)

        service_kwargs = dict(container_context.service_kwargs)
        _check_service_kwargs(service_kwargs)
        if service_kwargs:
            logger.debug("Passing additional service_kwargs: %s", list(service_kwargs.keys()))
        # A user's labels are kept, but the launcher's win: the cleanup sweep and
        # the health check's fallback lookup find a run's service by them.
        labels = {
            **(service_kwargs.pop("labels", None) or {}),
            "dagster/run_id": run.run_id,
            "dagster/job_name": run.job_name,
        }

        client = docker.client.from_env()
        if container_context.registry:
            logger.debug("Logging in to registry %s", container_context.registry["url"])
            client.login(
                registry=container_context.registry["url"],
                username=container_context.registry["username"],
                password=container_context.registry["password"],
            )

        mounts: list[Mount] = []
        for m in container_context.mounts:
            driver_config = None
            if m.get("driver_config"):
                driver_config = DriverConfig(
                    name=m["driver_config"]["Name"],
                    options=m["driver_config"].get("Options", {}),
                )
            unknown = sorted(set(m) - {"target", "source", "type", "driver_config", *_MOUNT_OPTIONS})
            if unknown:
                logger.warning(
                    "Ignoring unknown option(s) %s on the mount at %s for run %s",
                    unknown, m.get("target"), run.run_id,
                )
            mounts.append(Mount(
                target=m["target"],
                # A tmpfs mount has no source.
                source=m.get("source"),
                type=m.get("type", "volume"),
                driver_config=driver_config,
                **{option: m[option] for option in _MOUNT_OPTIONS if option in m},
            ))
        if mounts:
            logger.debug("Configured %d mount(s) for run %s", len(mounts), run.run_id)

        secret_refs = self._resolve_secret_references(client, container_context.secrets, run)

        try:
            service = client.services.create(
                image=docker_image,
                args=command,
                env=[f"{k}={v}" for k, v in docker_env.items()],
                labels=labels,
                name=service_name,
                mounts=mounts,
                networks=container_context.networks,
                secrets=secret_refs,
                mode=ServiceMode("replicated", replicas=1),
                restart_policy=RestartPolicy(condition="none"),
                **service_kwargs,
            )
        except docker.errors.APIError:
            logger.exception(
                "Failed to create Swarm service for run %s (image: %s)",
                run.run_id, docker_image,
            )
            raise

        logger.info(
            "Created Swarm service %s for run %s",
            service.id, run.run_id,
        )

        self._instance.report_engine_event(
            message=f"Launching run as Swarm service {service.id} with image {docker_image}",
            dagster_run=run,
            cls=self.__class__,
        )

        self._instance.add_run_tags(
            run.run_id,
            {SWARM_SERVICE_ID_TAG: service.id, DOCKER_IMAGE_TAG: docker_image},
        )

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        logger.info("launch_run called for run %s (job: %s)", run.run_id, run.job_name)
        job_code_origin = check.not_none(context.job_code_origin)
        docker_image = self._get_docker_image(job_code_origin)
        container_context = SwarmContainerContext.create_for_run(run, self)
        command = ExecuteRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        self._launch_service_with_command(run, docker_image, command, container_context)

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        logger.info("resume_run called for run %s (job: %s)", run.run_id, run.job_name)
        job_code_origin = check.not_none(context.job_code_origin)
        docker_image = self._get_docker_image(job_code_origin)
        container_context = SwarmContainerContext.create_for_run(run, self)
        command = ResumeRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        self._launch_service_with_command(run, docker_image, command, container_context)

    _TERMINAL_TASK_STATES = frozenset(("complete", "failed", "rejected", "orphaned", "shutdown"))

    # How long a worker may keep running after its run has finished before the
    # sweep stops it. The worker writes the run's final event before its process
    # exits, so a healthy one can still be tearing down for a few seconds after
    # the run is SUCCESS or FAILURE.
    _FINISHED_RUN_GRACE_SECONDS = 60

    def _cleanup_loop(self) -> None:
        """Background thread: periodically sweep orphaned Swarm services."""
        while not self._cleanup_shutdown.wait(self._cleanup_interval):
            try:
                self._cleanup_orphaned_services()
            except _MissingDockerSocketError as e:
                # Permanent for this process: the socket is not mounted, so no
                # amount of retrying will produce a Docker client. Log once and
                # exit the thread instead of raising every interval forever.
                logger.warning(
                    "No Docker socket at %s in this process — Swarm service cleanup disabled. "
                    "This is expected in the Dagster webserver, which does not mount the Docker "
                    "socket; cleanup runs in the Dagster daemon.",
                    e.socket_path,
                )
                return
            except Exception:
                logger.warning("Unhandled error in cleanup loop", exc_info=True)

    @classmethod
    def _removal_reason(
        cls, task_state: Optional[str], record: Optional[RunRecord], now: float
    ) -> Optional[str]:
        """Decide whether the cleanup sweep removes a service.

        ``task_state`` is the state of the service's latest Swarm task, or
        ``None`` if Swarm has not created one. ``record`` is the Dagster run the
        service is labelled with, or ``None`` if it has no label or the run is
        not in this instance's storage. Returns the reason to log, or ``None``
        to leave the service alone.

        Removing a service whose run is over, whatever its task is doing,
        covers two leaks that nothing else in Dagster closes:

        - A run that the monitoring daemon fails with ``START_TIMEOUT`` is only
          marked FAILURE; ``terminate`` is never called. Its task stays
          ``pending``/``preparing`` for good, holding a VIP and a task IP on
          the overlay network. 414 of them exhausted a /24 on 2026-10-28.
        - A run terminated from the UI: ``terminate`` runs in the webserver,
          which has no Docker socket, so it leaves the run CANCELING (or the
          UI's force option marks it CANCELED) and the worker keeps going.
        """
        if record is None:
            # Nothing in storage says the run is over, and the service may belong
            # to another Dagster instance on the same swarm: only remove what
            # Swarm itself has finished with.
            if task_state in cls._TERMINAL_TASK_STATES:
                return f"task {task_state}, run not in storage"
            return None

        status = record.dagster_run.status
        task = task_state or "not created"
        if status == DagsterRunStatus.CANCELING:
            # terminate() asked for the worker to stop but could not reach
            # Docker. Removing the service sends the SIGTERM, and the worker
            # records the run as CANCELED.
            return f"run CANCELING, task {task}"
        if not record.dagster_run.is_finished:
            # STARTING or STARTED: the monitoring daemon still health-checks
            # the run, and a pending task is a launch inside its start timeout.
            return None
        if task_state == "running" and status != DagsterRunStatus.CANCELED:
            # Only a SUCCESS or FAILURE run's worker gets the grace period, in
            # case it wrote that status itself and is still tearing down (a
            # FAILURE from the monitoring daemon waits too, harmlessly). A
            # CANCELED run's worker is still running only if nothing signalled
            # it (force-terminate, or Dagster's cancel timeout), and a task
            # still pulling or starting for a finished run is a late start:
            # both are stopped now.
            finished_at = record.end_time or record.update_timestamp.timestamp()
            if now - finished_at < cls._FINISHED_RUN_GRACE_SECONDS:
                return None
        return f"run {status.value}, task {task}"

    def _cleanup_orphaned_services(self) -> None:
        """Sweep all Swarm services labeled dagster/run_id and remove those
        whose run no longer needs them; see ``_removal_reason`` for the rules.

        Safety: we cross-check Dagster's run storage so we never remove a
        service that the monitoring daemon still needs to health-check, and a
        sweep that cannot read run storage removes nothing.
        """
        try:
            client = self._get_client()
        except Exception:
            socket_path = _missing_docker_socket_path()
            if socket_path is not None:
                raise _MissingDockerSocketError(socket_path) from None
            # Socket is there but the client still failed — engine restart or
            # similar. Transient: log and retry on the next sweep.
            logger.warning("Failed to create Docker client during cleanup", exc_info=True)
            return

        try:
            services = client.services.list(filters={"label": "dagster/run_id"})
        except Exception:
            logger.warning("Failed to list Swarm services during cleanup", exc_info=True)
            return

        if not services:
            return
        # Read outside the per-service try below, so tolerate a null Spec or
        # Labels rather than let one odd service abort the whole sweep.
        labelled = [
            (service, ((service.attrs.get("Spec") or {}).get("Labels") or {}).get("dagster/run_id"))
            for service in services
        ]

        # One query for the whole sweep. If run storage cannot be read, remove
        # nothing: an empty answer would make every service look "not in
        # storage". With no run IDs there is nothing to query, and
        # RunsFilter(run_ids=[]) raises.
        records: dict[str, RunRecord] = {}
        labelled_run_ids = sorted({run_id for _, run_id in labelled if run_id})
        if labelled_run_ids:
            try:
                for record in self._instance.get_run_records(
                    filters=RunsFilter(run_ids=labelled_run_ids)
                ):
                    records[record.dagster_run.run_id] = record
            except Exception:
                logger.warning(
                    "Failed to read run storage during cleanup; skipping this sweep",
                    exc_info=True,
                )
                return

        now = time.time()
        removed = 0
        for service, run_id in labelled:
            try:
                tasks = service.tasks()
                if tasks:
                    latest_task = sorted(tasks, key=lambda t: t["UpdatedAt"], reverse=True)[0]
                    state = latest_task["Status"]["State"]
                else:
                    state = None

                reason = self._removal_reason(state, records.get(run_id) if run_id else None, now)
                if reason is None:
                    continue

                service.remove()
                removed += 1
                logger.info(
                    "Cleaned up Swarm service %s (run %s): %s",
                    service.id,
                    run_id or "unknown",
                    reason,
                )
            except docker.errors.NotFound:
                pass  # already removed by another path
            except Exception:
                logger.warning("Error cleaning up service %s", service.id, exc_info=True)

        if removed:
            logger.info("Orphaned service cleanup removed %d service(s)", removed)

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        service = self._get_service(run)
        if service is None:
            tagged = run.tags.get(SWARM_SERVICE_ID_TAG)
            if tagged:
                msg = f"Could not find Swarm service {tagged} or a service labelled dagster/run_id={run.run_id}."
            else:
                msg = f"No Swarm service ID tag for run and no service labelled dagster/run_id={run.run_id}."
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, msg=msg)
        service_id = service.id

        tasks = service.tasks()
        if not tasks:
            logger.warning("No tasks found for service %s (run %s)", service_id, run.run_id)
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, msg="No tasks for Swarm service")

        latest_task = sorted(tasks, key=lambda t: t["UpdatedAt"], reverse=True)[0]
        state = latest_task["Status"]["State"]
        logger.debug(
            "Health check for run %s: service=%s, task_state=%s, task_count=%d",
            run.run_id, service_id, state, len(tasks),
        )

        if state == "running":
            return CheckRunHealthResult(WorkerStatus.RUNNING)
        elif state == "complete":
            logger.info("Run %s completed, removing service %s", run.run_id, service_id)
            try:
                service.remove()
            except Exception:
                logger.warning("Failed to remove completed service %s", service_id, exc_info=True)
            return CheckRunHealthResult(WorkerStatus.SUCCESS)
        elif state in ("failed", "rejected", "orphaned", "shutdown"):
            msg = latest_task["Status"].get("Message", f"Task state: {state}")
            logger.error(
                "Run %s failed: service=%s, state=%s, message=%s",
                run.run_id, service_id, state, msg,
            )
            try:
                service.remove()
            except Exception:
                logger.warning("Failed to remove errored service %s", service_id, exc_info=True)
            return CheckRunHealthResult(WorkerStatus.FAILED, msg=msg)

        # Task is in a transitional state (pending, assigned, preparing, etc.)
        logger.debug("Run %s in transitional state: %s", run.run_id, state)
        return CheckRunHealthResult(WorkerStatus.RUNNING)

    def terminate(self, run_id: str) -> bool:
        """Stop a run's worker by removing its Swarm service.

        Dagster calls this in whichever process served the terminate request.
        For a request from the UI that is the webserver, which has no Docker
        socket. There the run is left CANCELING and the daemon's cleanup sweep
        removes the service on its next pass (see ``_removal_reason``). The same
        hand-off happens when Docker returns an error. That relies on the
        daemon loading the same ``cleanup_interval`` as this process, as it
        does when both read one dagster.yaml.
        """
        run = self._instance.get_run_by_id(run_id)
        if not run or run.is_finished:
            logger.debug("Terminate called for run %s but run is already finished or not found", run_id)
            return False

        logger.info("Terminating run %s", run_id)
        # CANCELING is also the hand-off to the cleanup sweep when this process
        # cannot reach Docker, so it is reported before anything else.
        self._instance.report_run_canceling(run)

        socket_path = _missing_docker_socket_path()
        if socket_path is not None:
            return self._hand_off_to_cleanup(run, f"No Docker socket at {socket_path} in this process.")

        try:
            service = self._get_service(run)
            if not service:
                self._instance.report_engine_event(
                    message="Unable to find Swarm service to send termination request to.",
                    dagster_run=run,
                    cls=self.__class__,
                )
                logger.warning("Cannot terminate run %s: Swarm service not found", run_id)
                return False

            service_id = service.id
            try:
                service.remove()
                logger.info("Removed Swarm service %s for terminated run %s", service_id, run_id)
            except docker.errors.NotFound:
                # The cleanup sweep or a health check removed it between the lookup
                # and here; the worker is stopping either way.
                logger.info("Swarm service %s for run %s was already removed", service_id, run_id)
        except docker.errors.DockerException as e:
            # An engine restart or a 503 during a manager election. Raising here
            # would fail a backfill that is cancelling its runs, or abandon the
            # rest of a bulk terminate in the UI; the run is already CANCELING,
            # so the sweep can stop the worker once Docker answers again.
            logger.warning("Docker error while terminating run %s", run_id, exc_info=True)
            status = getattr(e, "status_code", None)
            detail = f" ({status})" if status else ""
            return self._hand_off_to_cleanup(
                run, f"Docker error{detail} while removing the run's Swarm service."
            )
        return True

    def _hand_off_to_cleanup(self, run: DagsterRun, problem: str) -> bool:
        """Leave a CANCELING run for the cleanup sweep after this process failed
        to remove its service. Returns whether anything will stop the worker."""
        if self._cleanup_interval <= 0:
            message = (
                f"{problem} Swarm service cleanup is disabled (cleanup_interval: 0), so "
                "nothing can stop the run's Swarm service."
            )
            self._instance.report_engine_event(message=message, dagster_run=run, cls=self.__class__)
            logger.warning("Cannot terminate run %s: %s", run.run_id, message)
            return False
        self._instance.report_engine_event(
            message=(
                f"{problem} The Dagster daemon's Swarm service cleanup (every "
                f"{self._cleanup_interval}s) will remove the run's service, which stops the "
                "run worker."
            ),
            dagster_run=run,
            cls=self.__class__,
        )
        logger.info("Run %s left CANCELING for the cleanup sweep: %s", run.run_id, problem)
        return True
