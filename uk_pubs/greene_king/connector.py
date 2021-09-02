from datetime import date
import logging

import pandas
import requests

from uk_pubs.greene_king.constants import BASE_URL


logger = logging.getLogger(__name__)


class GreeneKingAjaxConnector:
    '''Connector with Ajax interface of Greene King data source.'''
    NAME = 'Greene King'
    URL = BASE_URL + '/views/ajax'
    PAYLOAD_TEMPLATE = {
        'view_name': 'pub_search',
        'page': 0,
        'view_display_id': 'search_results',
    }

    def get_page(self, page_number: int = 0) -> pandas.DataFrame:
        '''Get data on pubs from a specific page, provided it's page number.

        :param page_number: The page number, defaults to 0.
        :type page_number: int, optional
        :return: Dataframe with the data on Pubs according to the given page
        :rtype: pandas.DataFrame
        '''
        logger.info('Getting pubs data from page %d', page_number)

        payload = self.PAYLOAD_TEMPLATE.copy()
        payload.update({'page': page_number})

        response = requests.post(self.URL, data=payload).json()

        json_data = response[0]['settings'].get('geofield_google_map', {})

        if len(json_data) == 0:
            return pandas.DataFrame()

        pubs = list(json_data.values())[0]['data']['features']
        data = pandas.json_normalize(pubs)

        logger.info('Successfully retrieve pubs data of %s', date.today())

        return data

    def get(self) -> pandas.DataFrame:
        '''Get raw data on Pubs from the Greene King vendor.

        :return: Dataframe with data on pubs, according to the vendor
        specifications
        :rtype: pandas.DataFrame
        '''
        data = []
        page_number = 0

        while not (page_data := self.get_page(page_number)).empty:
            data.append(page_data)
            page_number += 1

        if len(data) == 0:
            data = pandas.DataFrame()
        else:
            data = pandas.concat(data, ignore_index=True)
            data['ScrapeDate'] = str(date.today())
            data['Source'] = self.NAME

        return data
