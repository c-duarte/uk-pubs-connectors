import unittest
import pandas
import requests

from lxml import html

from uk_pubs_connectors.utils import mount_html_elements


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
        data = raw_data.copy()

        data['AnnualRent'] = data['ApproximatePrice']\
            .str.replace(r'Approximate Ingoings [£]?', '')\
            .str.replace(r'[.,]', '')\
            .astype('float64')

        del data['ApproximatePrice']

        data['source'] = self.NAME

        return data

    def get(self) -> pandas.DataFrame:
        response = requests.get(self.URL)
        html_obj = html.fromstring(response.text)

        page_elements = mount_html_elements(html_obj, self.STRUCTURE)

        pubs_list = page_elements['Pubs']

        data = pandas.DataFrame(pubs_list).applymap(', '.join)

        return data
