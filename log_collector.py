import logging

# Shared list to collect logs
log_records = []


class ListLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        log_records.append(msg)


def setup_logger():
    root_logger = logging.getLogger()
    if not any(isinstance(h, ListLogHandler) for h in root_logger.handlers):
        handler = ListLogHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - TRANSPILER - %(levelname)s - %(message)s")
        )
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
