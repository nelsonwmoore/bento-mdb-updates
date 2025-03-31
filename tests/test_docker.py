"""Test Docker image."""

from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.waiting_utils import wait_for_logs

PROJECT_ROOT = Path(__file__).parent.parent
DOCKERFILE_PATH = PROJECT_ROOT / "config" / "Dockerfile"
DOCKER_IMAGE_TAG = "mdb-update-test:latest"


@pytest.mark.docker
class TestDockerImage:
    """Test Docker image."""

    @pytest.fixture(scope="session")
    def container(self) -> Generator[DockerContainer, None, None]:
        """Create a container from the Docker image."""
        image = DockerImage(
            path=PROJECT_ROOT,
            tag=DOCKER_IMAGE_TAG,
        )
        image.build()

        container = DockerContainer(DOCKER_IMAGE_TAG, tty=True, stdin_open=True)
        container.start()
        wait_for_logs(container, "Container started")
        yield container
        container.stop()

    def test_java_installed(self, container: DockerContainer) -> None:
        """Test that Java is installed."""
        exit_code, output = container.exec(["java", "-version"])
        assert exit_code == 0
        assert "17" in output
