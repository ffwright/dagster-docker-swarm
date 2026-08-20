import logging
import time
from unittest.mock import MagicMock, patch

import docker.errors
import pytest
from dagster._core.errors import DagsterInvariantViolationError

from dagster_docker_swarm.container_context import DOCKER_SWARM_CONTAINER_CONTEXT_KEY
from dagster_docker_swarm.run_launcher import (
    DEFAULT_DOCKER_SOCKET_PATH,
    SWARM_SERVICE_ID_TAG,
    SwarmRunLauncher,
    _missing_docker_socket_path,
    _MissingDockerSocketError,
)
from tests.conftest import make_mock_job_code_origin, make_mock_run


class TestSwarmRunLauncherInit:
    def test_defaults(self):
        launcher = SwarmRunLauncher()
        assert launcher.image is None
        assert launcher.registry is None
        assert launcher.env_vars is None
        assert launcher.networks == []
        assert launcher.mounts is None
        assert launcher.service_kwargs is None
        assert launcher._cleanup_interval == 300

    def test_single_network_becomes_list(self):
        launcher = SwarmRunLauncher(network="my-net")
        assert launcher.networks == ["my-net"]

    def test_networks_list(self):
        launcher = SwarmRunLauncher(networks=["net-a", "net-b"])
        assert launcher.networks == ["net-a", "net-b"]

    def test_network_takes_precedence_over_networks(self):
        launcher = SwarmRunLauncher(network="single", networks=["a", "b"])
        assert launcher.networks == ["single"]

    def test_custom_cleanup_interval(self):
        launcher = SwarmRunLauncher(cleanup_interval=60)
        assert launcher._cleanup_interval == 60

    def test_cleanup_disabled(self):
        launcher = SwarmRunLauncher(cleanup_interval=0)
        assert launcher._cleanup_interval == 0


class TestGetDockerImage:
    def test_image_from_code_origin(self):
        launcher = SwarmRunLauncher(image="fallback:latest")
        origin = make_mock_job_code_origin(container_image="origin:v1")
        assert launcher._get_docker_image(origin) == "origin:v1"

    def test_image_from_config_fallback(self):
        launcher = SwarmRunLauncher(image="config:latest")
        origin = make_mock_job_code_origin(container_image=None)
        assert launcher._get_docker_image(origin) == "config:latest"

    def test_no_image_raises(self):
        launcher = SwarmRunLauncher()
        origin = make_mock_job_code_origin(container_image=None)
        with pytest.raises(Exception, match="No docker image specified"):
            launcher._get_docker_image(origin)

    def test_invalid_image_raises(self):
        launcher = SwarmRunLauncher()
        origin = make_mock_job_code_origin(container_image="INVALID:::image")
        with pytest.raises(Exception, match="not correctly formatted"):
            launcher._get_docker_image(origin)


class TestCheckRunWorkerHealth:
    def _make_launcher(self, mock_docker_client):
        """Create a launcher with cleanup disabled to isolate health-check tests."""
        with patch("docker.client.from_env", return_value=mock_docker_client):
            return SwarmRunLauncher(cleanup_interval=0)

    def test_no_service_id_tag(self, mock_docker_client):
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "NOT_FOUND"

    def test_service_not_found(self, mock_docker_client):
        mock_docker_client.services.get.side_effect = docker.errors.NotFound("gone")
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "NOT_FOUND"

    def test_task_running(self, mock_docker_client):
        service = MagicMock()
        service.tasks.return_value = [
            {"Status": {"State": "running"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        mock_docker_client.services.get.return_value = service
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "RUNNING"

    def test_task_complete_removes_service(self, mock_docker_client):
        service = MagicMock()
        service.tasks.return_value = [
            {"Status": {"State": "complete"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        mock_docker_client.services.get.return_value = service
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "SUCCESS"
        service.remove.assert_called_once()

    def test_task_failed(self, mock_docker_client):
        service = MagicMock()
        service.tasks.return_value = [
            {"Status": {"State": "failed", "Message": "OOM killed"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        mock_docker_client.services.get.return_value = service
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "FAILED"
        assert "OOM killed" in result.msg

    def test_transitional_state_treated_as_running(self, mock_docker_client):
        service = MagicMock()
        service.tasks.return_value = [
            {"Status": {"State": "preparing"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        mock_docker_client.services.get.return_value = service
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "RUNNING"

    def test_picks_latest_task(self, mock_docker_client):
        service = MagicMock()
        service.tasks.return_value = [
            {"Status": {"State": "failed", "Message": "old"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
            {"Status": {"State": "running"}, "UpdatedAt": "2025-01-02T00:00:00Z"},
        ]
        mock_docker_client.services.get.return_value = service
        launcher = self._make_launcher(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        result = launcher.check_run_worker_health(run)
        assert result.status.name == "RUNNING"


class TestCleanupOrphanedServices:
    """Tests for the background cleanup thread and sweep logic."""

    def _make_launcher_with_instance(self, mock_docker_client, cleanup_interval=0):
        """Create a launcher with cleanup thread disabled (interval=0) for direct testing."""
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=cleanup_interval)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)
            return launcher, mock_instance

    def _make_orphaned_service(self, run_id="abc12345", task_state="complete"):
        """Create a mock Swarm service that looks like an orphan."""
        service = MagicMock()
        service.id = f"svc-{run_id}"
        service.attrs = {"Spec": {"Labels": {"dagster/run_id": run_id}}}
        service.tasks.return_value = [
            {"Status": {"State": task_state}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        return service

    def test_removes_service_when_run_is_finished(self, mock_docker_client):
        orphan = self._make_orphaned_service(run_id="run-aaa", task_state="complete")
        mock_docker_client.services.list.return_value = [orphan]

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        finished_run = make_mock_run(run_id="run-aaa", is_finished=True)
        mock_instance.get_run_by_id.return_value = finished_run

        launcher._cleanup_orphaned_services()

        orphan.remove.assert_called_once()

    def test_removes_service_when_run_not_in_db(self, mock_docker_client):
        orphan = self._make_orphaned_service(run_id="run-deleted", task_state="complete")
        mock_docker_client.services.list.return_value = [orphan]

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        mock_instance.get_run_by_id.return_value = None  # run deleted from DB

        launcher._cleanup_orphaned_services()

        orphan.remove.assert_called_once()

    def test_skips_service_when_run_still_in_progress(self, mock_docker_client):
        """Safety: never remove a service if the Dagster run is still active."""
        service = self._make_orphaned_service(run_id="run-active", task_state="complete")
        mock_docker_client.services.list.return_value = [service]

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        active_run = make_mock_run(run_id="run-active", is_finished=False)
        mock_instance.get_run_by_id.return_value = active_run

        launcher._cleanup_orphaned_services()

        service.remove.assert_not_called()

    def test_skips_service_with_running_task(self, mock_docker_client):
        service = MagicMock()
        service.attrs = {"Spec": {"Labels": {"dagster/run_id": "run-bbb"}}}
        service.tasks.return_value = [
            {"Status": {"State": "running"}, "UpdatedAt": "2025-01-01T00:00:00Z"},
        ]
        mock_docker_client.services.list.return_value = [service]

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)

        launcher._cleanup_orphaned_services()

        service.remove.assert_not_called()
        mock_instance.get_run_by_id.assert_not_called()  # shouldn't even check

    def test_handles_already_removed_service(self, mock_docker_client):
        """NotFound during remove is silently ignored (another path cleaned it)."""
        orphan = self._make_orphaned_service(run_id="run-ccc", task_state="failed")
        orphan.remove.side_effect = docker.errors.NotFound("already gone")
        mock_docker_client.services.list.return_value = [orphan]

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        mock_instance.get_run_by_id.return_value = None

        # Should not raise
        launcher._cleanup_orphaned_services()

    def test_handles_api_error_on_list(self, mock_docker_client):
        mock_docker_client.services.list.side_effect = docker.errors.APIError("connection refused")

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)

        # Should not raise
        launcher._cleanup_orphaned_services()

    def test_cleans_all_terminal_states(self, mock_docker_client):
        """All terminal task states trigger cleanup."""
        services = []
        for state in ("complete", "failed", "rejected", "orphaned", "shutdown"):
            svc = self._make_orphaned_service(run_id=f"run-{state}", task_state=state)
            services.append(svc)
        mock_docker_client.services.list.return_value = services

        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        mock_instance.get_run_by_id.return_value = None  # all deleted

        launcher._cleanup_orphaned_services()

        for svc in services:
            svc.remove.assert_called_once()


class TestCleanupThread:
    """Tests for the background cleanup thread lifecycle."""

    def test_register_instance_starts_thread(self, mock_docker_client):
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=300)
            assert launcher._cleanup_thread is None

            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)

            assert launcher._cleanup_thread is not None
            assert launcher._cleanup_thread.is_alive()
            assert launcher._cleanup_thread.daemon is True
            assert launcher._cleanup_thread.name == "swarm-service-cleanup"

            launcher.dispose()

    def test_register_instance_no_thread_when_disabled(self, mock_docker_client):
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=0)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)

            assert launcher._cleanup_thread is None
            assert launcher._cleanup_shutdown is None

    def test_dispose_stops_thread(self, mock_docker_client):
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=300)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)

            thread = launcher._cleanup_thread
            assert thread.is_alive()

            launcher.dispose()

            assert not thread.is_alive()
            assert launcher._cleanup_thread is None

    def test_dispose_safe_when_no_thread(self, mock_docker_client):
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=0)
            # Should not raise
            launcher.dispose()

    def test_thread_calls_cleanup(self, mock_docker_client):
        """Verify the background thread actually fires _cleanup_orphaned_services."""
        mock_docker_client.services.list.return_value = []

        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=1)  # 1 second for test speed
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)

            # Wait for at least one cleanup cycle
            time.sleep(1.5)

            launcher.dispose()

        assert mock_docker_client.services.list.call_count >= 1


class TestTerminate:
    def _make_launcher_with_instance(self, mock_docker_client):
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(cleanup_interval=0)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)
            return launcher, mock_instance

    def test_terminate_finished_run(self, mock_docker_client):
        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        run = make_mock_run(is_finished=True)
        mock_instance.get_run_by_id.return_value = run
        assert launcher.terminate(run.run_id) is False

    def test_terminate_no_service(self, mock_docker_client):
        mock_docker_client.services.get.side_effect = docker.errors.NotFound("gone")
        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        mock_instance.get_run_by_id.return_value = run
        assert launcher.terminate(run.run_id) is False

    def test_terminate_success(self, mock_docker_client):
        service = MagicMock()
        mock_docker_client.services.get.return_value = service
        launcher, mock_instance = self._make_launcher_with_instance(mock_docker_client)
        run = make_mock_run(tags={SWARM_SERVICE_ID_TAG: "svc-123"})
        mock_instance.get_run_by_id.return_value = run
        assert launcher.terminate(run.run_id) is True
        service.remove.assert_called_once()


class TestLaunchService:
    """Tests for launch_run, resume_run, and _launch_service_with_command."""

    def _make_launcher_with_instance(self, mock_docker_client, **launcher_kwargs):
        launcher_kwargs.setdefault("cleanup_interval", 0)
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(**launcher_kwargs)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)
            return launcher, mock_instance

    def _make_context(self, run, origin, context_cls="launch"):
        context = MagicMock()
        context.dagster_run = run
        context.job_code_origin = origin
        return context

    def _setup_create(self, mock_docker_client, service_id="svc-new-123"):
        mock_service = MagicMock()
        mock_service.id = service_id
        mock_docker_client.services.create.return_value = mock_service
        return mock_service

    def test_launch_run_uses_args_not_command(self, mock_docker_client):
        """The dagster command must be passed as args= (CMD), not command= (ENTRYPOINT)."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args
        assert "args" in call_kwargs.kwargs
        assert call_kwargs.kwargs["args"] == ["dagster", "api", "execute_run"]
        assert "command" not in call_kwargs.kwargs

    def test_resume_run_uses_args_not_command(self, mock_docker_client):
        """resume_run should also use args= not command=."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ResumeRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "resume_run"]
            launcher.resume_run(context)

        call_kwargs = mock_docker_client.services.create.call_args
        assert "args" in call_kwargs.kwargs
        assert call_kwargs.kwargs["args"] == ["dagster", "api", "resume_run"]
        assert "command" not in call_kwargs.kwargs

    def test_launch_run_passes_env_vars(self, mock_docker_client):
        """Env vars from config are resolved and passed to the service."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1", env_vars=["FOO=bar", "BAZ=qux"],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args.kwargs
        env_list = call_kwargs["env"]
        env_dict = dict(item.split("=", 1) for item in env_list)
        assert env_dict["FOO"] == "bar"
        assert env_dict["BAZ"] == "qux"
        assert env_dict["DAGSTER_RUN_JOB_NAME"] == "my_job"

    def test_launch_run_passes_mounts(self, mock_docker_client):
        """Mounts with driver_config are correctly constructed."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            mounts=[{
                "target": "/data",
                "source": "my_vol",
                "type": "volume",
                "driver_config": {"Name": "local", "Options": {"type": "nfs"}},
            }],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args.kwargs
        mounts = call_kwargs["mounts"]
        assert len(mounts) == 1
        assert mounts[0]["Target"] == "/data"
        assert mounts[0]["Source"] == "my_vol"

    def test_launch_run_passes_networks(self, mock_docker_client):
        """Networks are forwarded to services.create."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1", networks=["net-a", "net-b"],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args.kwargs
        assert call_kwargs["networks"] == ["net-a", "net-b"]

    def test_launch_run_passes_service_kwargs(self, mock_docker_client):
        """Extra service_kwargs are spread into services.create."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1", service_kwargs={"user": "dagster"},
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args.kwargs
        assert call_kwargs["user"] == "dagster"

    def test_launch_run_reports_engine_event_and_tags(self, mock_docker_client):
        """Engine event is reported and service ID / image tags are added to the run."""
        mock_service = self._setup_create(mock_docker_client, service_id="svc-abc")
        launcher, mock_instance = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1",
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        mock_instance.report_engine_event.assert_called_once()
        mock_instance.add_run_tags.assert_called_once()
        tag_args = mock_instance.add_run_tags.call_args
        tags = tag_args[0][1]
        assert tags[SWARM_SERVICE_ID_TAG] == "svc-abc"

    def test_launch_run_service_name_format(self, mock_docker_client):
        """Service name is dagster-run-{first 8 chars of run_id}."""
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run(run_id="abcdef12-3456-7890-abcd-ef1234567890")
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(context)

        call_kwargs = mock_docker_client.services.create.call_args.kwargs
        assert call_kwargs["name"] == "dagster-run-abcdef12"

    def test_launch_run_api_error_propagates(self, mock_docker_client):
        """Docker API errors during service creation bubble up."""
        mock_docker_client.services.create.side_effect = docker.errors.APIError("create failed")
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = make_mock_run()
        context = self._make_context(run, origin)

        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            with pytest.raises(docker.errors.APIError, match="create failed"):
                launcher.launch_run(context)


def _attach_origin(run, origin):
    """Wire the code origin onto the run so create_for_run can find container_context."""
    run.job_code_origin = origin
    return run


def _mock_swarm_secret(name, secret_id=None):
    s = MagicMock()
    s.name = name
    s.id = secret_id or f"id-{name}"
    return s


class TestLaunchServiceWithContainerContext:
    """Tests for the per-code-location container_context merge flow (DAG-74)."""

    def _make_launcher_with_instance(self, mock_docker_client, **launcher_kwargs):
        launcher_kwargs.setdefault("cleanup_interval", 0)
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(**launcher_kwargs)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)
            return launcher, mock_instance

    def _make_context(self, run, origin):
        context = MagicMock()
        context.dagster_run = run
        context.job_code_origin = origin
        return context

    def _setup_create(self, mock_docker_client, service_id="svc-new-123"):
        mock_service = MagicMock()
        mock_service.id = service_id
        mock_docker_client.services.create.return_value = mock_service

    def _launch(self, launcher, run, origin):
        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(self._make_context(run, origin))

    def test_code_location_env_vars_appended(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1", env_vars=["INSTANCE=1"],
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"env_vars": ["CL=2"]}},
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        env_list = mock_docker_client.services.create.call_args.kwargs["env"]
        env_dict = dict(item.split("=", 1) for item in env_list)
        assert env_dict["INSTANCE"] == "1"
        assert env_dict["CL"] == "2"

    def test_code_location_networks_appended(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client, image="img:v1", networks=["net-instance"],
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {"networks": ["net-cl"]}},
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        assert mock_docker_client.services.create.call_args.kwargs["networks"] == [
            "net-instance",
            "net-cl",
        ]

    def test_code_location_registry_replaces_instance_and_logs_in(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            registry={"url": "old.io", "username": "u1", "password": "p1"},
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={
                DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                    "registry": {"url": "new.io", "username": "u2", "password": "p2"},
                },
            },
        )
        run = _attach_origin(make_mock_run(), origin)

        # Reset login call history that earlier register_instance may have triggered.
        mock_docker_client.login.reset_mock()
        self._launch(launcher, run, origin)

        # Registry login uses the merged (code-location) registry, not the instance one.
        login_kwargs = mock_docker_client.login.call_args.kwargs
        assert login_kwargs["registry"] == "new.io"
        assert login_kwargs["username"] == "u2"

    def test_code_location_mounts_appended(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            mounts=[{"target": "/instance", "source": "vol_i", "type": "volume"}],
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={
                DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                    "mounts": [{"target": "/cl", "source": "vol_c", "type": "volume"}],
                },
            },
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        mounts = mock_docker_client.services.create.call_args.kwargs["mounts"]
        targets = sorted(m["Target"] for m in mounts)
        assert targets == ["/cl", "/instance"]

    def test_code_location_service_kwargs_shallow_merge(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            service_kwargs={"user": "dagster", "hostname": "instance"},
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={
                DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                    "service_kwargs": {"hostname": "cl", "stop_grace_period": 30},
                },
            },
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        kwargs = mock_docker_client.services.create.call_args.kwargs
        assert kwargs["user"] == "dagster"
        assert kwargs["hostname"] == "cl"
        assert kwargs["stop_grace_period"] == 30


class TestLaunchServiceSecrets:
    """Tests for secrets resolution and SecretReference attachment (DAG-74)."""

    def _make_launcher_with_instance(self, mock_docker_client, **launcher_kwargs):
        launcher_kwargs.setdefault("cleanup_interval", 0)
        with patch("docker.client.from_env", return_value=mock_docker_client):
            launcher = SwarmRunLauncher(**launcher_kwargs)
            mock_instance = MagicMock()
            launcher.register_instance(mock_instance)
            return launcher, mock_instance

    def _make_context(self, run, origin):
        context = MagicMock()
        context.dagster_run = run
        context.job_code_origin = origin
        return context

    def _setup_create(self, mock_docker_client):
        mock_service = MagicMock()
        mock_service.id = "svc-new-123"
        mock_docker_client.services.create.return_value = mock_service

    def _setup_secrets(self, mock_docker_client, name_to_id):
        """Make client.secrets.list filter-by-name return matching mocked secrets."""

        def _list(filters=None):
            wanted = (filters or {}).get("name")
            return [_mock_swarm_secret(n, sid) for n, sid in name_to_id.items() if n == wanted]

        mock_docker_client.secrets.list.side_effect = _list

    def _launch(self, launcher, run, origin):
        with patch("dagster_docker_swarm.run_launcher.ExecuteRunArgs") as MockArgs:
            MockArgs.return_value.get_command_args.return_value = ["dagster", "api", "execute_run"]
            launcher.launch_run(self._make_context(run, origin))

    def test_launcher_level_secrets_attached(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        self._setup_secrets(mock_docker_client, {"my_secret": "id-my_secret"})
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            secrets=[{"secret_name": "my_secret", "filename": "MY_SECRET"}],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        refs = mock_docker_client.services.create.call_args.kwargs["secrets"]
        assert len(refs) == 1
        # SecretReference is a dict-like; assert the materialized fields.
        assert refs[0]["SecretName"] == "my_secret"
        assert refs[0]["SecretID"] == "id-my_secret"
        assert refs[0]["File"]["Name"] == "MY_SECRET"

    def test_code_location_secrets_attached(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        self._setup_secrets(mock_docker_client, {"cl_secret": "id-cl_secret"})
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={
                DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                    "secrets": [{"secret_name": "cl_secret"}],
                },
            },
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        refs = mock_docker_client.services.create.call_args.kwargs["secrets"]
        assert len(refs) == 1
        assert refs[0]["SecretName"] == "cl_secret"

    def test_secrets_merged_from_both_layers(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        self._setup_secrets(
            mock_docker_client,
            {"instance_secret": "id-i", "cl_secret": "id-c"},
        )
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            secrets=[{"secret_name": "instance_secret"}],
        )
        origin = make_mock_job_code_origin(
            container_image="img:v1",
            container_context={
                DOCKER_SWARM_CONTAINER_CONTEXT_KEY: {
                    "secrets": [{"secret_name": "cl_secret"}],
                },
            },
        )
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        refs = mock_docker_client.services.create.call_args.kwargs["secrets"]
        names = sorted(r["SecretName"] for r in refs)
        assert names == ["cl_secret", "instance_secret"]

    def test_missing_secret_raises_before_create(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        self._setup_secrets(mock_docker_client, {})  # no secrets exist on the swarm
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            secrets=[{"secret_name": "nope"}],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = _attach_origin(make_mock_run(), origin)

        with pytest.raises(DagsterInvariantViolationError, match="'nope'"):
            self._launch(launcher, run, origin)

        mock_docker_client.services.create.assert_not_called()

    def test_filename_defaults_to_secret_name(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        self._setup_secrets(mock_docker_client, {"only_name": "id-only_name"})
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            secrets=[{"secret_name": "only_name"}],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        refs = mock_docker_client.services.create.call_args.kwargs["secrets"]
        assert refs[0]["File"]["Name"] == "only_name"

    def test_no_secrets_configured_passes_empty_list(self, mock_docker_client):
        self._setup_create(mock_docker_client)
        launcher, _ = self._make_launcher_with_instance(mock_docker_client, image="img:v1")
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = _attach_origin(make_mock_run(), origin)
        self._launch(launcher, run, origin)

        assert mock_docker_client.services.create.call_args.kwargs["secrets"] == []
        # No filter-by-name calls when there are no secrets to resolve.
        mock_docker_client.secrets.list.assert_not_called()

    def test_substring_name_match_rejected(self, mock_docker_client):
        """client.secrets.list may return prefix/substring matches — we require exact name."""
        self._setup_create(mock_docker_client)
        mock_docker_client.secrets.list.return_value = [
            _mock_swarm_secret("my_secret_v2"),  # close but not equal
        ]
        launcher, _ = self._make_launcher_with_instance(
            mock_docker_client,
            image="img:v1",
            secrets=[{"secret_name": "my_secret"}],
        )
        origin = make_mock_job_code_origin(container_image="img:v1")
        run = _attach_origin(make_mock_run(), origin)

        with pytest.raises(DagsterInvariantViolationError, match="'my_secret'"):
            self._launch(launcher, run, origin)


class TestMissingDockerSocketPath:
    """Unit tests for the socket-missing discriminator."""

    def test_default_path_when_docker_host_unset(self, monkeypatch):
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        with patch("os.path.exists", return_value=False):
            assert _missing_docker_socket_path() == DEFAULT_DOCKER_SOCKET_PATH

    def test_none_when_default_socket_exists(self, monkeypatch):
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        with patch("os.path.exists", return_value=True):
            assert _missing_docker_socket_path() is None

    @pytest.mark.parametrize(
        "docker_host,expected",
        [
            ("unix:///var/run/docker.sock", "/var/run/docker.sock"),
            ("unix:///custom/docker.sock", "/custom/docker.sock"),
            ("http+unix:///var/run/docker.sock", "/var/run/docker.sock"),
            ("unix://", DEFAULT_DOCKER_SOCKET_PATH),
            ("", DEFAULT_DOCKER_SOCKET_PATH),
        ],
    )
    def test_unix_schemes_resolve_to_path(self, monkeypatch, docker_host, expected):
        monkeypatch.setenv("DOCKER_HOST", docker_host)
        with patch("os.path.exists", return_value=False):
            assert _missing_docker_socket_path() == expected

    @pytest.mark.parametrize("docker_host", ["tcp://swarm-manager:2375", "ssh://user@host"])
    def test_non_unix_transport_is_not_classified(self, monkeypatch, docker_host):
        """Remote transports have no socket to check — treat as transient."""
        monkeypatch.setenv("DOCKER_HOST", docker_host)
        with patch("os.path.exists", return_value=False):
            assert _missing_docker_socket_path() is None


class TestCleanupWithoutDockerSocket:
    """Regression tests for the webserver case: no Docker socket in this process.

    Before 0.2.1 the cleanup thread raised DockerException every
    cleanup_interval, forever, in every process that loads the instance without
    the socket mounted (notably the Dagster webserver).
    """

    SOCKET_ERROR = docker.errors.DockerException(
        "Error while fetching server API version: "
        "('Connection aborted.', FileNotFoundError(2, 'No such file or directory'))"
    )

    def test_sweep_raises_sentinel_when_socket_missing(self):
        launcher = SwarmRunLauncher(cleanup_interval=0)
        launcher.register_instance(MagicMock())

        with (
            patch("docker.client.from_env", side_effect=self.SOCKET_ERROR),
            patch("dagster_docker_swarm.run_launcher._missing_docker_socket_path", return_value="/var/run/docker.sock"),
            pytest.raises(_MissingDockerSocketError) as excinfo,
        ):
            launcher._cleanup_orphaned_services()

        assert excinfo.value.socket_path == "/var/run/docker.sock"

    def test_sweep_swallows_client_error_when_socket_present(self):
        """Socket exists but the client failed: transient, no sentinel raised."""
        launcher = SwarmRunLauncher(cleanup_interval=0)
        launcher.register_instance(MagicMock())

        with (
            patch("docker.client.from_env", side_effect=self.SOCKET_ERROR),
            patch("dagster_docker_swarm.run_launcher._missing_docker_socket_path", return_value=None),
        ):
            launcher._cleanup_orphaned_services()  # should not raise

    def test_thread_stops_after_one_warning_when_socket_missing(self, caplog):
        caplog.set_level(logging.WARNING, logger="dagster_docker_swarm")

        with (
            patch("docker.client.from_env", side_effect=self.SOCKET_ERROR) as mock_from_env,
            patch("dagster_docker_swarm.run_launcher._missing_docker_socket_path", return_value="/var/run/docker.sock"),
        ):
            launcher = SwarmRunLauncher(cleanup_interval=1)
            launcher.register_instance(MagicMock())
            thread = launcher._cleanup_thread

            thread.join(timeout=10)
            assert not thread.is_alive(), "cleanup thread should exit when the socket is missing"

            launcher.dispose()

        assert mock_from_env.call_count == 1, "should not retry a permanently missing socket"

        disabled = [r for r in caplog.records if "cleanup disabled" in r.getMessage()]
        assert len(disabled) == 1
        assert "/var/run/docker.sock" in disabled[0].getMessage()

    def test_thread_survives_transient_docker_error(self):
        """A daemon hiccup must not disable cleanup — the thread keeps retrying."""
        with (
            patch("docker.client.from_env", side_effect=self.SOCKET_ERROR) as mock_from_env,
            patch("dagster_docker_swarm.run_launcher._missing_docker_socket_path", return_value=None),
        ):
            launcher = SwarmRunLauncher(cleanup_interval=1)
            launcher.register_instance(MagicMock())
            thread = launcher._cleanup_thread

            deadline = time.time() + 10
            while mock_from_env.call_count < 2 and time.time() < deadline:
                time.sleep(0.05)

            assert mock_from_env.call_count >= 2, "transient errors should be retried each interval"
            assert thread.is_alive(), "thread must stay alive across transient errors"

            launcher.dispose()

        assert not thread.is_alive()
