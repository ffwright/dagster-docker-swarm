import logging
import threading
from typing import Any, Optional

import dagster._check as check
import docker
from dagster._config import Array, Field, IntSource, Permissive, StringSource
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._core.utils import parse_env_var
from dagster._grpc.types import ExecuteRunArgs, ResumeRunArgs
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from docker import DockerClient
from docker.models.services import Service
from docker.types import DriverConfig, Mount, RestartPolicy, ServiceMode
from docker_image import reference

logger = logging.getLogger("dagster_docker_swarm")

SWARM_SERVICE_ID_TAG = "swarm/service_id"

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
        cleanup_interval: int = 300,
    ) -> None:
        self._inst_data = inst_data
        self.image = image
        self.registry = registry
        self.env_vars = env_vars
        self.mounts = mounts
        self.service_kwargs = service_kwargs
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
        if not run:
            return None
        service_id = run.tags.get(SWARM_SERVICE_ID_TAG)
        if not service_id:
            logger.debug("No service ID tag found for run %s", run.run_id)
            return None
        try:
            service = self._get_client().services.get(service_id)
            logger.debug("Found Swarm service %s for run %s", service_id, run.run_id)
            return service
        except docker.errors.NotFound:
            logger.warning("Swarm service %s not found for run %s", service_id, run.run_id)
            return None
        except docker.errors.APIError:
            logger.exception("Docker API error looking up service %s for run %s", service_id, run.run_id)
            return None

    def _launch_service_with_command(self, run: DagsterRun, docker_image: str, command: list[str]) -> None:
        service_name = f"dagster-run-{run.run_id[:8]}"
        logger.info(
            "Launching Swarm service for run %s (job: %s, image: %s, service: %s)",
            run.run_id, run.job_name, docker_image, service_name,
        )

        env_vars = self.env_vars or []
        docker_env = dict([parse_env_var(env_var) for env_var in env_vars])
        docker_env["DAGSTER_RUN_JOB_NAME"] = run.job_name
        logger.debug("Resolved %d environment variables for run %s", len(docker_env), run.run_id)

        client = self._get_client()

        labels = {"dagster/run_id": run.run_id, "dagster/job_name": run.job_name}

        mounts: list[Mount] = []
        for m in (self.mounts or []):
            driver_config = None
            if m.get("driver_config"):
                driver_config = DriverConfig(
                    name=m["driver_config"]["Name"],
                    options=m["driver_config"].get("Options", {}),
                )
            mounts.append(Mount(
                target=m["target"],
                source=m["source"],
                type=m.get("type", "volume"),
                driver_config=driver_config,
            ))
        if mounts:
            logger.debug("Configured %d mount(s) for run %s", len(mounts), run.run_id)

        service_kwargs = dict(self.service_kwargs or {})
        if service_kwargs:
            logger.debug("Passing additional service_kwargs: %s", list(service_kwargs.keys()))

        try:
            service = client.services.create(
                image=docker_image,
                command=command,
                env=[f"{k}={v}" for k, v in docker_env.items()],
                labels=labels,
                name=service_name,
                mounts=mounts,
                networks=self.networks,
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
        command = ExecuteRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        self._launch_service_with_command(run, docker_image, command)

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        logger.info("resume_run called for run %s (job: %s)", run.run_id, run.job_name)
        job_code_origin = check.not_none(context.job_code_origin)
        docker_image = self._get_docker_image(job_code_origin)
        command = ResumeRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        self._launch_service_with_command(run, docker_image, command)

    _TERMINAL_TASK_STATES = frozenset(("complete", "failed", "rejected", "orphaned", "shutdown"))

    def _cleanup_loop(self) -> None:
        """Background thread: periodically sweep orphaned Swarm services."""
        while not self._cleanup_shutdown.wait(self._cleanup_interval):
            try:
                self._cleanup_orphaned_services()
            except Exception:
                logger.warning("Unhandled error in cleanup loop", exc_info=True)

    def _cleanup_orphaned_services(self) -> None:
        """Sweep all Swarm services labeled dagster/run_id and remove those
        whose tasks have terminated AND whose corresponding Dagster run is
        already in a terminal state (or no longer exists).

        Safety: we cross-check Dagster's run storage so we never remove a
        service that the monitoring daemon still needs to health-check.
        """
        try:
            client = self._get_client()
            services = client.services.list(filters={"label": "dagster/run_id"})
        except Exception:
            logger.warning("Failed to list Swarm services during cleanup", exc_info=True)
            return

        removed = 0
        for service in services:
            try:
                tasks = service.tasks()
                if not tasks:
                    continue

                latest_task = sorted(tasks, key=lambda t: t["UpdatedAt"], reverse=True)[0]
                state = latest_task["Status"]["State"]
                if state not in self._TERMINAL_TASK_STATES:
                    continue

                # The Swarm task is done. Only remove the service if the Dagster
                # run has also reached a terminal status — otherwise the
                # monitoring daemon may still call check_run_worker_health for it
                # and we'd cause a spurious NOT_FOUND → resume attempt.
                run_id = service.attrs.get("Spec", {}).get("Labels", {}).get("dagster/run_id")
                if run_id:
                    dagster_run = self._instance.get_run_by_id(run_id)
                    if dagster_run is not None and not dagster_run.is_finished:
                        continue  # monitoring daemon will handle this one

                service.remove()
                removed += 1
                logger.info(
                    "Cleaned up orphaned service %s (run %s, task state: %s)",
                    service.id,
                    run_id or "unknown",
                    state,
                )
            except docker.errors.NotFound:
                pass  # already removed by another path
            except Exception:
                logger.warning("Error cleaning up service %s", service.id, exc_info=True)

        if removed:
            logger.info("Orphaned service cleanup removed %d service(s)", removed)

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        service_id = run.tags.get(SWARM_SERVICE_ID_TAG)
        if not service_id:
            logger.debug("No service ID tag for run %s, returning NOT_FOUND", run.run_id)
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, msg="No Swarm service ID tag for run.")

        service = self._get_service(run)
        if service is None:
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, msg=f"Could not find Swarm service {service_id}.")

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
        run = self._instance.get_run_by_id(run_id)
        if not run or run.is_finished:
            logger.debug("Terminate called for run %s but run is already finished or not found", run_id)
            return False

        logger.info("Terminating run %s", run_id)
        self._instance.report_run_canceling(run)

        service = self._get_service(run)
        if not service:
            self._instance.report_engine_event(
                message="Unable to find Swarm service to send termination request to.",
                dagster_run=run,
                cls=self.__class__,
            )
            logger.warning("Cannot terminate run %s: Swarm service not found", run_id)
            return False

        service_id = run.tags.get(SWARM_SERVICE_ID_TAG)
        try:
            service.remove()
            logger.info("Removed Swarm service %s for terminated run %s", service_id, run_id)
        except docker.errors.APIError:
            logger.exception("Failed to remove Swarm service %s for run %s", service_id, run_id)
            raise
        return True
