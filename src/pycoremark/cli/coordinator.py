"""CLI entry point for a networked coordinator accepting remote workers."""

from threading import Event

from pycoremark.cli.network import NetworkCoordinatorRuntime
from pycoremark.common.config import load_config_from_env
from pycoremark.common.logging import get_logger, setup_logging


def main() -> None:
    config = load_config_from_env()
    setup_logging(config.log_level)
    logger = get_logger("cli.coordinator")
    runtime = NetworkCoordinatorRuntime(config)
    stop_event = Event()
    try:
        runtime.start()
        host, port = runtime.server.address
        logger.info("Coordinator API listening on %s:%s", host, port)
        logger.info("Coordinator gRPC listening on %s:%s", config.coordinator_host, config.grpc_port)
        logger.info("Workers can connect using PYCOREMARK_COORDINATOR_HOST/PORT.")
        while not stop_event.wait(1.0):
            runtime.manager.check_cluster_liveness()
    except KeyboardInterrupt:
        logger.info("Coordinator shutting down.")
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
