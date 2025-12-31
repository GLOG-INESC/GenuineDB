import docker
SSH = 'ssh "-o StrictHostKeyChecking no"'

USER = "ubuntu"
CONTAINER_DATA_DIR = "/tmp/"
HOST_DATA_DIR = "/tmp/"
PROF_DATA_DIR = "/src/build/*.er"

SLOG_IMG = "ctring/slog"
SLOG_CONTAINER_NAME = "slog"
SLOG_BENCHMMARK_CONTAINER_NAME = "benchmark"
SLOG_CLIENT_CONTAINER_NAME = "slog_client"
SLOG_DATA_MOUNT = docker.types.Mount(
    target=CONTAINER_DATA_DIR,
    source=HOST_DATA_DIR,
    type="bind",
)
BENCHMARK_CONTAINER_NAME = "benchmark"
ZIPLOG_CONTAINER_NAME = "zip_order"

HOST_SLOG_DIR = "/home/rasoares/GenuineDB/execs"
TARGET_SLOG_DIR = "/tmp/zip_execs/"