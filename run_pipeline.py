"""Run from the repository root. Configuration is in config.json."""
import logging
from logiwhale.pipeline import run

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        logging.info('Completed: %s', run())
    except Exception:
        logging.exception('Pipeline failed; published data was not changed')
        raise SystemExit(1)
