from datetime import date
import logging

import pandas
import requests


logger = logging.getLogger(__name__)


class PunchPubsConnector:
    '''Connector with Ajax interface of Greene King data source.'''
    NAME = 'Punch Pubs'
    URL = 'https://www.punchpubs.com/wp-json/punch/v1/pubsdata'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) '
        'Gecko/20100101 Firefox/91.0',
    }

    def get(self) -> pandas.DataFrame:
        '''Get raw data on pubs from Punch Pubs.
        
        :return: Dataframe with data on Pubs, with format defined by the vendor
        :rtype: pandas.DataFrame
        '''
        logger.info('Getting data from PunchPubs at %s', self.URL)

        response = requests.get(self.URL, headers=self.HEADERS).json()
        data = pandas.json_normalize(response)
        data['ScrapeDate'] = str(date.today())
        data['Source'] = self.NAME

        logger.info('Successfully retrieved data of %s', date.today())

        return data
