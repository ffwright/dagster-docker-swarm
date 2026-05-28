from unittest.mock import MagicMock

from dagster_docker_swarm.container_context import (
    DOCKER_SWARM_CONTAINER_CONTEXT_KEY,
    SwarmContainerContext,
)


class TestSwarmContainerContext:
    def test_merge_env_vars_concatenated(self):
        base = SwarmContainerContext(env_vars=["A=1", "B=2"])
        override = SwarmContainerContext(env_vars=["C=3"])
        merged = base.merge(override)
        assert merged.env_vars == ["A=1", "B=2", "C=3"]

    def test_merge_networks_concatenated(self):
        base = SwarmContainerContext(networks=["net-a"])
        override = SwarmContainerContext(networks=["net-b"])
        merged = base.merge(override)
        assert merged.networks == ["net-a", "net-b"]

    def test_merge_registry_override_replaces(self):
        base = SwarmContainerContext(registry={"url": "old", "username": "u", "password": "p"})
        override = SwarmContainerContext(registry={"url": "new", "username": "u2", "password": "p2"})
        merged = base.merge(override)
        assert merged.registry["url"] == "new"

    def test_merge_registry_none_keeps_base(self):
        base = SwarmContainerContext(registry={"url": "old", "username": "u", "password": "p"})
        override = SwarmContainerContext()
        merged = base.merge(override)
        assert merged.registry["url"] == "old"

    def test_merge_service_kwargs_shallow_merge(self):
        base = SwarmContainerContext(service_kwargs={"a": 1, "b": 2})
        override = SwarmContainerContext(service_kwargs={"b": 99, "c": 3})
        merged = base.merge(override)
        assert merged.service_kwargs == {"a": 1, "b": 99, "c": 3}

    def test_merge_mounts_concatenated(self):
        base = SwarmContainerContext(mounts=[{"target": "/a", "source": "vol_a"}])
        override = SwarmContainerContext(mounts=[{"target": "/b", "source": "vol_b"}])
        merged = base.merge(override)
        assert len(merged.mounts) == 2

    def test_merge_secrets_concatenated(self):
        base = SwarmContainerContext(secrets=[{"secret_name": "a"}])
        override = SwarmContainerContext(secrets=[{"secret_name": "b", "filename": "B"}])
        merged = base.merge(override)
        assert merged.secrets == [
            {"secret_name": "a"},
            {"secret_name": "b", "filename": "B"},
        ]

    def test_defaults(self):
        ctx = SwarmContainerContext()
        assert ctx.registry is None
        assert ctx.env_vars == []
        assert ctx.networks == []
        assert ctx.mounts == []
        assert ctx.service_kwargs == {}
        assert ctx.secrets == []


def _make_launcher(
    registry=None,
    env_vars=None,
    networks=None,
    mounts=None,
    service_kwargs=None,
    secrets=None,
):
    launcher = MagicMock()
    launcher.registry = registry
    launcher.env_vars = env_vars
    launcher.networks = networks if networks is not None else []
    launcher.mounts = mounts
    launcher.service_kwargs = service_kwargs
    launcher.secrets = secrets
    return launcher


def _make_run(container_context=None):
    run = MagicMock()
    if container_context is None:
        run.job_code_origin.repository_origin.container_context = None
    else:
        run.job_code_origin.repository_origin.container_context = container_context
    return run


class TestCreateForRun:
    def test_instance_only_no_code_location_context(self):
        launcher = _make_launcher(env_vars=["A=1"], networks=["net-a"])
        run = _make_run(container_context=None)
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.env_vars == ["A=1"]
        assert ctx.networks == ["net-a"]

    def test_code_location_env_vars_appended(self):
        launcher = _make_launcher(env_vars=["A=1"])
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"env_vars": ["B=2"]}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.env_vars == ["A=1", "B=2"]

    def test_code_location_networks_appended(self):
        launcher = _make_launcher(networks=["net-a"])
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"networks": ["net-b"]}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.networks == ["net-a", "net-b"]

    def test_code_location_mounts_appended(self):
        launcher = _make_launcher(mounts=[{"target": "/a", "source": "vol_a"}])
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"mounts": [{"target": "/b", "source": "vol_b"}]}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert len(ctx.mounts) == 2

    def test_code_location_registry_replaces_instance(self):
        launcher = _make_launcher(registry={"url": "old", "username": "u", "password": "p"})
        run = _make_run({
            DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                "registry": {"url": "new", "username": "u2", "password": "p2"},
            },
        })
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.registry["url"] == "new"

    def test_code_location_service_kwargs_shallow_merge(self):
        launcher = _make_launcher(service_kwargs={"a": 1, "b": 2})
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"service_kwargs": {"b": 99, "c": 3}}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.service_kwargs == {"a": 1, "b": 99, "c": 3}

    def test_code_location_secrets_appended(self):
        launcher = _make_launcher(secrets=[{"secret_name": "instance_a"}])
        run = _make_run({
            DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                "secrets": [{"secret_name": "cl_b", "filename": "B"}],
            },
        })
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.secrets == [
            {"secret_name": "instance_a"},
            {"secret_name": "cl_b", "filename": "B"},
        ]

    def test_empty_docker_swarm_key(self):
        launcher = _make_launcher(env_vars=["A=1"])
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.env_vars == ["A=1"]

    def test_other_substrate_key_ignored(self):
        launcher = _make_launcher(env_vars=["A=1"])
        run = _make_run({"k8s": {"env_vars": ["IGNORED=1"]}, "docker": {"env_vars": ["ALSO_IGNORED=1"]}})
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.env_vars == ["A=1"]

    def test_none_launcher_fields_default_safely(self):
        launcher = _make_launcher()  # all None
        run = _make_run(container_context=None)
        ctx = SwarmContainerContext.create_for_run(run, launcher)
        assert ctx.registry is None
        assert ctx.env_vars == []
        assert ctx.networks == []
        assert ctx.mounts == []
        assert ctx.service_kwargs == {}
        assert ctx.secrets == []

    def test_no_run_launcher(self):
        """Passing run_launcher=None still works (just code-location config)."""
        run = _make_run({DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"env_vars": ["B=2"]}})
        ctx = SwarmContainerContext.create_for_run(run, None)
        assert ctx.env_vars == ["B=2"]


class TestCreateFromDict:
    def test_full_dict(self):
        ctx = SwarmContainerContext.create_from_dict({
            "registry": {"url": "r", "username": "u", "password": "p"},
            "env_vars": ["A=1"],
            "networks": ["net"],
            "mounts": [{"target": "/x", "source": "y"}],
            "service_kwargs": {"user": "dagster"},
            "secrets": [{"secret_name": "s"}],
        })
        assert ctx.registry["url"] == "r"
        assert ctx.env_vars == ["A=1"]
        assert ctx.networks == ["net"]
        assert len(ctx.mounts) == 1
        assert ctx.service_kwargs == {"user": "dagster"}
        assert ctx.secrets == [{"secret_name": "s"}]

    def test_empty_dict(self):
        ctx = SwarmContainerContext.create_from_dict({})
        assert ctx.registry is None
        assert ctx.env_vars == []
        assert ctx.networks == []
        assert ctx.mounts == []
        assert ctx.service_kwargs == {}
        assert ctx.secrets == []
