import docker


class Reject(Exception):
    pass


# 8GB cache size limit
CACHE_SIZE_LIMIT = 8 * 1024**3
CACHE_VOLUME_NAME = "hypothesis-corpus-uv-cache"


def get_cache_volume_size(docker_client: docker.DockerClient) -> int:
    # Run du command in a temporary container to measure volume size
    # We need to mount the volume to measure it
    result = docker_client.containers.run(
        "hypothesis-corpus:latest",
        command=["du", "-sb", "/root/.cache/uv"],
        volumes={CACHE_VOLUME_NAME: {"bind": "/root/.cache/uv", "mode": "ro"}},
        remove=True,
        stdout=True,
        stderr=False,
    )

    # du output format: "12345678\t/path"
    size_bytes = int(result.decode("utf-8").split()[0])
    return size_bytes


def clean_uv_cache(docker_client: docker.DockerClient) -> None:
    # We use rm -rf instead of 'uv cache clean' because uv tries to remove
    # the cache directory itself, which fails when it's a Docker volume mount point.
    docker_client.containers.run(
        "hypothesis-corpus:latest",
        command=["sh", "-c", "rm -rf /root/.cache/uv/* /root/.cache/uv/.*[!.]*"],
        volumes={CACHE_VOLUME_NAME: {"bind": "/root/.cache/uv", "mode": "rw"}},
        remove=True,
    )
