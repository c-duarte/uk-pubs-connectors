from setuptools import setup
import os

from uk_pubs_connectors import VERSION


BASE_DIR = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(BASE_DIR, 'README.md'), 'rt') as fd:
    long_description = fd.read()


if __name__ == '__main__':
    setup(
        name='uk-pubs-connectors',
        version=VERSION,
        description='Connectors to UK Pubs data, from different data sources',
    )