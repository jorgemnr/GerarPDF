import logging
from logging.handlers import RotatingFileHandler
import os

############################################
# Configuração LOG aplicação
############################################
caminhoAplicacao = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            caminhoAplicacao + "/log/log.txt",
            maxBytes=100 * 1000000,
            backupCount=10,
            encoding="UTF-8",
        )
    ],
    level=logging.INFO,
    format="[%(asctime)s] - %(levelname)s - [%(funcName)s:linha %(lineno)d] - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# Create a logger object
logger = logging.getLogger(__name__)


# # Create a logger object
# logger = logging.getLogger(__name__)

# # Set the logging level to INFO
# logger.setLevel(logging.DEBUG)

# # Create a file handler and set its logging level to INFO
# file_handler = RotatingFileHandler(
#     "log/log.txt", maxBytes=100000, backupCount=20, encoding="UTF-8"
# )
# file_handler.setLevel(logging.DEBUG)

# # Create a log formatter
# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s : %(message)s")
# file_handler.setFormatter(formatter)

# # Add the file handler to the logger
# logger.addHandler(file_handler)
