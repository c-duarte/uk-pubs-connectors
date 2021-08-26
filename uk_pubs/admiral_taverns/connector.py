from datetime import date
import logging

import pandas
import requests

from lxml import html

from uk_pubs.utils import mount_html_elements


logger = logging.getLogger(__name__)


class AdmiralTavernsConnector:
    '''Connector with Admiral Taverns data source for pubs in the UK.'''
    NAME = 'Admiral Taverns'
    URL = 'https://www.admiraltaverns.co.uk/find-a-pub/?pcSearch&z=2&ppp=-1'
    STRUCTURE = {
        'Pubs': [
            './/div[@class = "newsArticle table"]',
            {
                'Name': './div/a/text()',
                'URL': './div/a/@href',
                'StreetAddress': './/p[@class = "location"]/text()',
                'ApproximatePrice': './/p[@class = "price"]/text()',
                'Description': './/div[@class = "excerpt"]/text()'
            }
        ]
    }

    def clean(self, raw_data: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean the raw data from the website.

        :param raw_data: Raw data from the website
        :type raw_data: pandas.DataFrame
        :return: Clean data
        :rtype: pandas.DataFrame
        '''
        logger.info('Cleaning raw AdmiralTaverns data')

        data = raw_data.copy()

        data['AnnualRent'] = data['ApproximatePrice']\
            .str.replace(r'Approximate Ingoings [£]?', '', regex=True)\
            .str.replace(r'[.,]', '', regex=True)\
            .astype('float64')

        del data['ApproximatePrice']

        data['Source'] = self.NAME

        return data

    def get(self) -> pandas.DataFrame:
        logger.info('Getting AdmiralTaverns data from %s', self.URL)

        response = requests.get(self.URL)

        logger.info('HTML file retrieved. Getting DOM\'s elements')

        html_obj = html.fromstring(response.text)
        page_elements = mount_html_elements(html_obj, self.STRUCTURE)
        pubs_list = page_elements['Pubs']
        data = pandas.DataFrame(pubs_list).applymap(', '.join)
        data['ScrapeDate'] = str(date.today())

        return data
