import logging

import sys


LOGGER_NAME = "coco"
LOG_LEVEL = "DEBUG"

log = logging.getLogger(__name__)


def main():
    # TODO check for user settings file

    # Setup Logging
    logging_format = '%(name)s:%(lineno)d[%(process)d]: %(levelname)s - %(message)s'
    log_format = logging.Formatter(logging_format)
    log.setLevel(LOG_LEVEL)

    # console handler
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(log_format)
    log.addHandler(console_handler)

    log.info("Starting coco/core/make_coco.py run...")

    sys.exit()


if __name__ == "__main__":

    logging.basicConfig()
    main()
