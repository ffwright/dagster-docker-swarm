from typing import TYPE_CHECKING, Any, NamedTuple, Optional

if TYPE_CHECKING:
    from dagster._core.storage.dagster_run import DagsterRun

    from dagster_docker_swarm.run_launcher import SwarmRunLauncher


DOCKER_SWARM_CONTAINER_CONTEXT_KEY = "docker_swarm"


class SwarmContainerContext(NamedTuple):
    """Hierarchical configuration context for Swarm services.

    Allows instance-level config (dagster.yaml) to be extended or overridden
    by per-code-location config sourced from
    ``job_code_origin.repository_origin.container_context["docker_swarm"]``.

    Merge rules:
      - registry: code-location replaces instance entirely (None preserves base)
      - env_vars, networks, mounts, secrets: lists are concatenated
      - service_kwargs: shallow dict merge (code-location keys win)
    """

    registry: Optional[dict[str, str]] = None
    env_vars: list[str] = []
    networks: list[str] = []
    mounts: list[dict[str, Any]] = []
    service_kwargs: dict[str, Any] = {}
    secrets: list[dict[str, Any]] = []

    def merge(self, other: "SwarmContainerContext") -> "SwarmContainerContext":
        return SwarmContainerContext(
            registry=other.registry if other.registry is not None else self.registry,
            env_vars=[*self.env_vars, *other.env_vars],
            networks=[*self.networks, *other.networks],
            mounts=[*self.mounts, *other.mounts],
            service_kwargs={**self.service_kwargs, **other.service_kwargs},
            secrets=[*self.secrets, *other.secrets],
        )

    @classmethod
    def create_from_config(cls, run_launcher: "SwarmRunLauncher") -> "SwarmContainerContext":
        """Build a context from the launcher's instance-level config."""
        return cls(
            registry=run_launcher.registry,
            env_vars=list(run_launcher.env_vars or []),
            networks=list(run_launcher.networks or []),
            mounts=list(run_launcher.mounts or []),
            service_kwargs=dict(run_launcher.service_kwargs or {}),
            secrets=list(run_launcher.secrets or []),
        )

    @classmethod
    def create_from_dict(cls, d: dict[str, Any]) -> "SwarmContainerContext":
        """Build a context from the ``docker_swarm`` sub-dict of a code-location's container_context."""
        return cls(
            registry=d.get("registry"),
            env_vars=list(d.get("env_vars") or []),
            networks=list(d.get("networks") or []),
            mounts=list(d.get("mounts") or []),
            service_kwargs=dict(d.get("service_kwargs") or {}),
            secrets=list(d.get("secrets") or []),
        )

    @classmethod
    def create_for_run(
        cls,
        pipeline_run: "DagsterRun",
        run_launcher: Optional["SwarmRunLauncher"],
    ) -> "SwarmContainerContext":
        """Build a merged context: launcher-level config + per-code-location config."""
        context = cls()
        if run_launcher is not None:
            context = context.merge(cls.create_from_config(run_launcher))

        job_code_origin = getattr(pipeline_run, "job_code_origin", None)
        if job_code_origin is not None:
            repo_origin = getattr(job_code_origin, "repository_origin", None)
            container_context = getattr(repo_origin, "container_context", None) if repo_origin else None
            if container_context:
                swarm_ctx = container_context.get(DOCKER_SWARM_CONTAINER_CONTEXT_KEY)
                if swarm_ctx:
                    context = context.merge(cls.create_from_dict(swarm_ctx))

        return context
