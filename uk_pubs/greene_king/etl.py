from datetime import date
import argparse
import os
import logging

from dotenv import load_dotenv
import googlemaps
import pandas

from uk_pubs.greene_king.connector import GreeneKingAjaxConnector
from uk_pubs.greene_king.data_processor import GreeneKingDataProcessor
from uk_pubs.utils import get_geoinfo


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

    # 1. Save raw data
    logger.info('Step 1: Get raw data from website')
    dest = os.path.join(args.dir, str(today) + '-raw.csv')

    if not os.path.exists(dest):
        connector = GreeneKingAjaxConnector()
        data = connector.get()
        data.to_csv(dest, index=False)
        logger.info('Raw data saved to %s', dest)

    src = dest

    # 2. Clean raw data
    logger.info('Step 2: Clean raw data')
    data = pandas.read_csv(src, dtype='object')
    dest = os.path.join(args.dir, str(today) + '-clean.csv')

    if not os.path.exists(dest):
        processor = GreeneKingDataProcessor()
        data = processor.process(data)
        data.to_csv(dest, index=False)
        logger.info('Clean data saved to %s', dest)

    src = dest

    # 3. Apply geocoding
    logger.info('Step 3: Get geo information from GoogleMaps')
    data = pandas.read_csv(src, dtype='object')
    dest = os.path.join(args.dir, str(today) + '-geo.csv')

    if not os.path.exists(dest):
        data['SearchString'] = data['Lat'] + ', ' + data['Long']
        data.set_index('SearchString', inplace=True)

        gm_client = googlemaps.Client(os.environ['GOOGLEMAPS_KEY'])
        geo_info = get_geoinfo(gm_client, data.index)
        data = geo_info.combine_first(data)

        data.to_csv(dest, index=False)
        logger.info('Data with geoinformation saved to %s', dest)

    # 4. Push to SQL
    # TODO


if __name__ == '__main__':
    main()
