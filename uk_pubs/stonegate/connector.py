from datetime import date
import logging

import pandas
import requests


logger = logging.getLogger(__name__)


class StonegateConnector:
    '''Connector with Stonegate data source on pubs in the UK.'''
    NAME = 'Stonegate'
    URL = 'https://www.stonegatepubpartners.co.uk/' \
        + 'run-a-pub/_vti_bin/Brightstarr.EI.Intranet/Pubs/Pubs.svc/' \
        + 'GetResultsFromSearch'
    DATA = {
        'queryText': 'contenttype:"Pub Marketing Information" '
        'AND EIPubLatitudeOWSNMBRFLOAT>40 '
        'AND EIPubLatitudeOWSNMBRFLOAT<62 '
        'AND EIPubLongitudeOWSNMBRFLOAT>-10 '
        'AND EIPubLongitudeOWSNMBRFLOAT<10 ',
        'latitude': -100,
        'longitude': -100,
        'maxDistance': 10000,
        'sortBy': 'distance',
        'skip': 0,
        'take': 10000,
        'maxRows': 10000
    }

    def get(self) -> pandas.DataFrame:
        '''Get raw data on pubs from Stonegate.

        :return: Dataframe with data on pubs from Stonegate, in the
        vendor-defined format.
        :rtype: pandas.DataFrame
        '''
        logger.info('Getting data from Stonegate at %s', self.URL)

        response = requests.post(self.URL, json=self.DATA)
        data = pandas.json_normalize(response.json()['Results'])
        data['ScrapeDate'] = str(date.today())
        data['Source'] = self.NAME

        logger.info(
            'Successfully retrieved data from Stonegate of %s',
            date.today()
        )

        return data
