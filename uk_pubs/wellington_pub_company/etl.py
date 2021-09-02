from datetime import date
import argparse
import os
import logging

from dotenv import load_dotenv
import googlemaps
import pandas

from uk_pubs.wellington_pub_company.connector import \
    WellingtonPubCompanyConnector
from uk_pubs.wellington_pub_company.data_processor import \
    WellingtonPubCompanyDataProcessor
from uk_pubs.utils import get_geoinfo


logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description='Welling Pub Company ETL')
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
        '<current_directory>\\logs\\wellington_pub_company',
        default='logs\\wellington_pub_company'
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
        connector = WellingtonPubCompanyConnector()
        output = connector.get()
        output.to_csv(dest, index=False)

        logger.info('Raw data saved to %s', dest)

    # 2. Clean raw data
    logger.info('Step 2: Clean raw data')
    input = pandas.read_csv(dest, dtype='object')
    dest = os.path.join(args.dir, str(today) + '-clean.csv')
    if not os.path.exists(dest):
        processor = WellingtonPubCompanyDataProcessor()
        output = processor.process(input)

        output.to_csv(dest, index=False)
        logger.info('Clean data saved to %s', dest)

    # 3. Apply geocoding
    logger.info('Step 3: Get geo information from GoogleMaps')
    input = pandas.read_csv(dest, dtype='object')
    dest = os.path.join(args.dir, str(today) + '-geo.csv')

    if not os.path.exists(dest):
        input['SearchString'] = input['Name'] + ', ' + input['StreetAddress']
        input.set_index('SearchString', inplace=True)

        gm_client = googlemaps.Client(os.environ['GOOGLEMAPS_KEY'])
        geo_info = get_geoinfo(gm_client, input.index)
        output = geo_info.combine_first(input)

        output.to_csv(dest, index=False)
        logger.info('Data with geoinformation saved to %s', dest)

    # 4. Push to SQL
    # TODO


if __name__ == '__main__':
    main()
