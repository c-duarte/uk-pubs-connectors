from datetime import date
import argparse
import logging
import os

from dotenv import load_dotenv
import googlemaps
import pandas

from uk_pubs_connectors.admiral_taverns import AdmiralTavernsConnector
from uk_pubs_connectors.utils import get_geoinfo


logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description='Admiral Taverns Pubs ETL')
    parser.add_argument(
        'dir',
        help='Path to the directory where the raw data will be saved (as '
        'YYYY-MM-DD.csv and YYYY-MM-DD-geo.csv'
    )
    parser.add_argument(
        'sql_table',
        help='Full description of the SQL table to which data will be pushed. '
        'Format: SERVER/DATABASE.SCHEMA.TABLE'
    )
    parser.add_argument(
        '-l', '--logs-dir',
        help='Path to the logs directory. Defaults to the '
        '<current_directory>\\logs\\admiral_taverns',
        default='logs\\admiral_taverns'
    )

    args = parser.parse_args()

    today = date.today()
    os.makedirs(args.dir, exist_ok=True)
    os.makedirs(args.logs_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(args.logs_dir, str(today) + '.log'),
        level=logging.INFO
    )

    connector = AdmiralTavernsConnector()

    # 1. Save raw data
    logger.info('Step 1: Get raw data from website')
    filepath = os.path.join(args.dir, str(today) + '-raw.csv')
    if not os.path.exists(filepath):
        data = connector.get()
        data.to_csv(filepath, index=False)

        logger.info('Data saved to %s', filepath)
    else:
        logger.info('Data already available at %s, loading it', filepath)

        data = pandas.read_csv(filepath)

    # 2. Clean raw data
    logger.info('Step 2: Clean raw data from website')
    filepath = os.path.join(args.dir, str(today) + '-clean.csv')
    if not os.path.exists(filepath):
        data = connector.clean(data)
        data.to_csv(filepath, index=False)

        logger.info('Data saved to %s', filepath)
    else:
        logger.info('Data already available at %s, loading it', filepath)

        data = pandas.read_csv(filepath)

    # 3. Apply geocoding
    logger.info('Step 3: Get geo information from GoogleMaps')
    filepath = os.path.join(args.dir, str(today) + '-geo.csv')
    if not os.path.exists(filepath):
        gm_client = googlemaps.Client(os.environ['GOOGLEMAPS_KEY'])
        data['SearchString'] = data['StreetAddress'] + ', UK'
        geo_info = get_geoinfo(gm_client, data.loc[:, 'SearchString'])

        data.set_index('SearchString', inplace=True)
        data = geo_info.combine_first(data)
        data.to_csv(filepath, index=False)

        logger.info('Data saved to %s', filepath)
    else:
        logger.info('Data already available at %s, loading it', filepath)

        data = pandas.read_csv(filepath)

    # 4. Push to SQL
    # TODO


if __name__ == '__main__':
    main()
