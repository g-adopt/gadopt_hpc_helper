import logging
import sys
import os


def _init_logging() -> logging.Logger:
    logger = logging.getLogger("GadoptHPCHelper")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(stream_handler)
    # Get log level
    log_level = logging.WARNING
    try:
        log_level = int(os.environ["GADOPT_HPC_HELPER_LOG_LEVEL"])
    except KeyError:
        pass
    except ValueError:
        lstr = os.environ["GADOPT_HPC_HELPER_LOG_LEVEL"].upper()
        if hasattr(logging, lstr):
            log_level = getattr(logging, lstr)
        else:
            logger.warning(
                "GADOPT_HPC_HELPER_LOG_LEVEL in environment: %s does not correspond to any known logging level",
                os.environ["GADOPT_HPC_HELPER_LOG_LEVEL"],
            )
    logger.setLevel(log_level)

    return logger


logger = _init_logging()
