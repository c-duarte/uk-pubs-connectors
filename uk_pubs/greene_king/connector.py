from datetime import date
import logging
import re

import pandas
import requests

import lxml.html
import html

from uk_pubs.utils import mount_html_elements
from uk_pubs.greene_king.constants import BASE_URL, NAME


logger = logging.getLogger(__name__)


class GreeneKingAjaxConnector:
    '''Connector with Ajax interface of Greene King data source.'''
    URL = BASE_URL + '/views/ajax'
    PAYLOAD_TEMPLATE = {
        'view_name': 'pub_search',
        'page': 0,
        'view_display_id': 'search_results',
    }

    def get_page(self, page_number: int = 0) -> pandas.DataFrame:
        '''TODO: Doc'''
        payload = self.PAYLOAD_TEMPLATE.copy()
        payload.update({'page': page_number})

        response = requests.post(self.URL, data=payload).json()

        json_data = response[0]['settings'].get('geofield_google_map', {})

        if len(json_data) == 0:
            return pandas.DataFrame()

        pubs = list(json_data.values())[0]['data']['features']

        page_data = []

        for pub in pubs:
            geometry = pub['geometry']
            pub_data = pub['properties']['data']

            page_data.append({
                'Name': html.unescape(pub_data['field_pub_name_only']),
                'URL': re.findall(r'href="([^"]+)"', pub_data['title'])[0],
                'StreetAddress': ', '.join(filter(None, [
                    pub_data.get('address_line1', ''),
                    pub_data.get('locality', ''),
                    pub_data.get('postal_code', ''),
                ])),
                'AnnualRent': pub_data.get('field_agreement_annual_rent'),
                'Lat': geometry['coordinates'][1],
                'Long': geometry['coordinates'][0],
            })

        page_data = pandas.DataFrame(page_data)

        return page_data

    def get(self) -> pandas.DataFrame:
        '''TODO: Doc'''
        data = []
        page_number = 0

        while len(page_data := self.get_page(page_number)) != 0:
            data.append(page_data)

            page_number += 1

        data = pandas.concat(data, ignore_index=True)
        data['ScrapeDate'] = str(date.today())
        data['Source'] = NAME

        return data


class GreeneKingWebsiteConnector:
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
        dom = lxml.html.fromstring(response.text)
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

        data = pandas.concat(data, ignore_index=True)
        data['ScrapeDate'] = str(date.today())
        data['Source'] = NAME

        return data
