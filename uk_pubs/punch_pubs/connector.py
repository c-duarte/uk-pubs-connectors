from datetime import date
import logging

import pandas
import requests

from uk_pubs.punch_pubs.constants import NAME


logger = logging.getLogger(__name__)


class PunchPubsConnector:
    '''Connector with Ajax interface of Greene King data source.'''
    URL = 'https://www.punchpubs.com/wp-json/punch/v1/pubsdata'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) '
        'Gecko/20100101 Firefox/91.0',
    }

    def get(self) -> pandas.DataFrame:
        '''TODO: Doc'''
        logger.info('Getting data from PunchPubs at %s', self.URL)

        response = requests.get(self.URL, headers=self.HEADERS).json()
        data = pandas.json_normalize(response, sep='.')
        data['ScrapeDate'] = str(date.today())
        data['Source'] = NAME

        return data
