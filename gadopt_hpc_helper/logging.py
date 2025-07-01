"""Logging module

Provides a logger object for interacting with GADOPT HPC Helper.
Intended to be re-exported by __init__.py, so external applications
can access with gadopt_hpc_helper.logger.
"""

import logging
import sys
import os


def _init_logging() -> logging.Logger:
    """GADOPT HPC Helper logger

    Construct a logging object, set it to write to stderr and set the
    level based on the default (WARNING) or whatever is in the
    GADOPT_HPC_HELPER_LOG_LEVEL environment variable if it is a valid
    numeric or text warning level (CRITICAL, ERROR, WARNING, INFO, DEBUG)

    Returns:
      logging.Logger: The logger object
    """
    logger = logging.getLogger("GadoptHPCHelper")
    stream_handler = logging.StreamHandler(sys.stderr)
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
