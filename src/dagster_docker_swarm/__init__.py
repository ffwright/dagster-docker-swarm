from dagster_docker_swarm._version import __version__
from dagster_docker_swarm.container_context import SwarmContainerContext
from dagster_docker_swarm.run_launcher import SwarmRunLauncher

__all__ = [
    "__version__",
    "SwarmContainerContext",
    "SwarmRunLauncher",
]

try:
    from dagster_shared.libraries import DagsterLibraryRegistry

    DagsterLibraryRegistry.register("dagster-docker-swarm", __version__)
except ImportError:
    # dagster_shared may not be available in older dagster versions
    pass
