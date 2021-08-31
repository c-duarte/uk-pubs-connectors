from datetime import date
import logging

import lxml.html
import pandas
import requests

from uk_pubs.utils import mount_html_elements
from uk_pubs.wellington_pub_company.constants import NAME


logger = logging.getLogger(__name__)


class WellingtonPubCompanyConnector:
    '''Connector with Wellington Pub Company data source on pubs in the UK.'''
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
        '''TODO: Doc'''
        url = self.URL.format(page_number=page_number)
        logger.info('Getting data from WellingtonPubCompany at %s', url)

        response = requests.get(url, headers=self.HEADERS)
        dom = lxml.html.fromstring(response.text)
        page_elements = mount_html_elements(dom, self.PAGE_STRUCTURE)

        return pandas.DataFrame(page_elements['Pubs'])

    def get(self) -> pandas.DataFrame:
        '''TODO: Doc'''
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
        data['Source'] = NAME
        data['ScrapeDate'] = str(date.today())

        return data
