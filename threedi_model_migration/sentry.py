from sentry_sdk.integrations.logging import LoggingIntegration

import logging
import sentry_sdk


def setup_sentry(dsn):
    sentry_logging = LoggingIntegration(
        level=logging.INFO,  # Capture info and above as breadcrumbs
        event_level=logging.ERROR,  # Send errors and warnings as events
    )
    sentry_sdk.utils.MAX_STRING_LENGTH = 2048
    sentry_sdk.init(dsn=dsn, integrations=[sentry_logging])
