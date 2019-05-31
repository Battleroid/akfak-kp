import logging
import sys
import structlog

# TODO: I still don't like how logging is done right now, it needs
# a total overhaul, I want a root logger for the application at 
# akfak, with akfak.server, akfak.client, and akfak.cli setup
#
# Ideally I should be able to set akfak to ERROR level if I'm doing
# console only related things to quiet the output, but leave it on
# otherwise


def setup_logging(level=logging.INFO) -> None:

    # e.g. 'INFO' to logging.INFO
    if isinstance(level, str):
        level = getattr(logging, level)

    logging.basicConfig(
        format='%(message)s',
        stream=sys.stdout,
        level=level
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper('iso'),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
