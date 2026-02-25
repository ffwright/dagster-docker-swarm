from dagster_docker_swarm.container_context import SwarmContainerContext


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

    def test_defaults(self):
        ctx = SwarmContainerContext()
        assert ctx.registry is None
        assert ctx.env_vars == []
        assert ctx.networks == []
        assert ctx.mounts == []
        assert ctx.service_kwargs == {}
