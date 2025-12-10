import logging
import subprocess
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    _extract()
    _transform()
    _load()

def _extract():
    logger.info('Proses Extract Data')
    subprocess.run([sys.executable, 'extract.py'], cwd=os.path.join(BASE_DIR, 'extract'), check=True)

def _transform():
    logger.info('Proses Transform Data')
    subprocess.run([sys.executable, 'transform.py'], cwd=os.path.join(BASE_DIR, 'transform'), check=True)

def _load():
    logger.info('Proses Load Data')
    subprocess.run([sys.executable, 'load.py'], cwd=os.path.join(BASE_DIR, 'load'), check=True)


# main function
main()
