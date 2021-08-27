import logging

import pandas
import requests

from lxml import html

from uk_pubs.utils import mount_html_elements


logger = logging.getLogger(__name__)


class GreeneKingConnector:
    '''Connector with Greene King data source for pubs in the UK.'''
    NAME = 'Greene King'
    URL = 'https://www.greenekingpubs.co.uk/pub-search?page={page_number}'
    STRUCTURE = {
        'Pubs': [
            './/section[@class="search-results"]//div[@class="card"]',
            {
                'Name': './/h3/a/text()',
                'URL': './/div[@class = "image"]/a/@href',
                'StreetAddress': './/div[@class = "content"]//h4/text()',
                'Description': './/div[@class = "image"]/span/text()'
            }
        ]
    }

    def get_page(self, page_number: int = 0) -> pandas.DataFrame:
        '''Get specific page of the pubs search results.

        :param page_number: Number of the page to get (defaults to 0)
        :type page_number: int, optional
        :return: DataFrame with the fields
            - TODO
        :rtype: pandas.DataFrame
        '''
        response = requests.get(self.URL.format(page_number=page_number))
        dom = html.fromstring(response.text)
        page_elements = mount_html_elements(dom, self.STRUCTURE)
        data = pandas.DataFrame(page_elements['Pubs']).applymap(', '.join)

        return data

    def get(self) -> pandas.DataFrame:
        data = []
        page_number = 0

        while not (page_data := self.get_page(page_number)).empty:
            logger.info('Got %d pubs on page %d', len(page_data), page_number)
            data.append(page_data)
            page_number += 1

        data = pandas.concat(data)

        return data
