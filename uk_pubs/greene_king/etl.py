from datetime import date
import argparse
import os
import logging

from dotenv import load_dotenv
import pandas

from uk_pubs.greene_king.connector import GreeneKingConnector


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
    filepath = os.path.join(args.dir, str(today) + '-raw.csv')
    if not os.path.exists(filepath):
        connector = GreeneKingConnector()
        data = connector.get()
        data.to_csv(filepath, index=False)

        logger.info('Data saved to %s', filepath)
    else:
        logger.info('Data already available at %s, loading it', filepath)

        data = pandas.read_csv(filepath)


if __name__ == '__main__':
    main()
