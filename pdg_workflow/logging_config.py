import logging
# import logging.handlers # unsure if this is necessary
from datetime import datetime

# configure logger
logger = logging.getLogger("logger")
# prevent logging statements from being printed to terminal
logger.propagate = False
# set up new handler
handler = logging.FileHandler("log.log")
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)