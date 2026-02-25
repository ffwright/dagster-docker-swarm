from typing import Any, NamedTuple, Optional


class SwarmContainerContext(NamedTuple):
    """Hierarchical configuration context for Swarm services.

    Allows instance-level config (dagster.yaml) to be extended or overridden
    by per-code-location config. Merge rules:
      - registry: code-location replaces instance entirely
      - env_vars, networks: lists are concatenated
      - mounts: lists are concatenated
      - service_kwargs: shallow dict merge (code-location keys win)
    """

    registry: Optional[dict[str, str]] = None
    env_vars: list[str] = []
    networks: list[str] = []
    mounts: list[dict[str, Any]] = []
    service_kwargs: dict[str, Any] = {}

    def merge(self, other: "SwarmContainerContext") -> "SwarmContainerContext":
        return SwarmContainerContext(
            registry=other.registry if other.registry is not None else self.registry,
            env_vars=[*self.env_vars, *other.env_vars],
            networks=[*self.networks, *other.networks],
            mounts=[*self.mounts, *other.mounts],
            service_kwargs={**self.service_kwargs, **other.service_kwargs},
        )
