from unittest.mock import MagicMock, patch

import pytest


def make_mock_run(run_id="abc12345-def6-7890-abcd-ef1234567890", job_name="my_job", tags=None, is_finished=False):
    """Create a mock DagsterRun."""
    run = MagicMock()
    run.run_id = run_id
    run.job_name = job_name
    run.tags = tags or {}
    run.is_finished = is_finished
    return run


def make_mock_job_code_origin(container_image=None):
    """Create a mock job code origin."""
    origin = MagicMock()
    origin.repository_origin.container_image = container_image
    return origin


@pytest.fixture
def mock_docker_client():
    """Patch docker.client.from_env and return the mock client."""
    with patch("docker.client.from_env") as mock_from_env:
        client = MagicMock()
        mock_from_env.return_value = client
        yield client
