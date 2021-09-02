from datetime import date
import logging

import lxml.html
import pandas
import requests

from uk_pubs.utils import mount_html_elements


logger = logging.getLogger(__name__)


class WellingtonPubCompanyConnector:
    '''Connector with Wellington Pub Company data source on pubs in the UK.'''
    NAME = 'WellingtonPubCompany'
    URL = 'https://wellingtonpubcompany.co.uk/pubs/page/{page_number}/'
    HEADERS = {'User-Agent': 'Mozilla/5.0 Gecko/20100101 Firefox/91.0'}
    PAGE_STRUCTURE = {
        'Pubs': (
            '//article',
            {
                'ID': './@id',
                'NameAddress': './/h1[@class="entry-title"]/text()',
                'Tags': './/ul[@class="tags"]/li/text()',
                'Description': '//div[@class="entry-content"]/ul/li//text()',
                'URL': './/div[@class="particulars"]/a/@href',
                'NegotiatorMailURL': './/div[@class="negotiator"]/a/@href'
            }
        )
    }

    def get_page(self, page_number: int = 0) -> pandas.DataFrame:
        '''Get data on pubs from a specific page, given the page number.

        :param page_number: The page number, defaults to 0.
        :type page_number: int, optional
        :return: Dataframe with the data on Pubs according to the given page
        :rtype: pandas.DataFrame
        '''
        url = self.URL.format(page_number=page_number)
        logger.info('Getting data from WellingtonPubCompany at %s', url)

        response = requests.get(url, headers=self.HEADERS)
        dom = lxml.html.fromstring(response.text)
        page_elements = mount_html_elements(dom, self.PAGE_STRUCTURE)

        result = pandas.DataFrame(page_elements['Pubs'])
        logger.info('Page %d data successully retrieved.', page_number)

        return result

    def get(self) -> pandas.DataFrame:
        '''Get raw data on pubs from Wellington Pub Company.

        :return: DataFrame with data on pubs, in the vendor-defined format.
        :rtype: pandas.DataFrame
        '''
        logger.info('Getting data from Wellington Pub Company')

        data = []
        page_number = 1

        while not (page_data := self.get_page(page_number)).empty:
            data.append(page_data)
            page_number += 1

        if len(data) > 0:
            data = pandas.concat(data, ignore_index=True)
        else:
            data = pandas.DataFrame()

        data = data.applymap(', '.join)
        data['Source'] = self.NAME
        data['ScrapeDate'] = str(date.today())

        return data
