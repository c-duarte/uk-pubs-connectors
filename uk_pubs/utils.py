import logging
from multiprocessing.pool import ThreadPool
from typing import Sequence

from lxml import html
import googlemaps
import pandas
import numpy


logger = logging.getLogger(__name__)


def mount_html_elements(root: html.HtmlElement, structure: dict) -> dict:
    '''Given an HTML element and it's structure described using xpaths, return \
the page's elements.

    Args:
        root (html.HtmlElement): the root of the page whose information will be
            mounted.
        structure (dict): a dictionary containing the structure of the page
            from the given root. See ``Examples`` section.

    Returns:
        dict: the html elements inside the page, under the given structure.

    Examples:
        .. code-block:: python

            from lxml import html

            driver.get(URL)
            html_obj = html.fromstring(driver.page_source)

            structure = {
                'shopping_items': (
                    './/div[@class = "shopping-list"]/a',
                    {'name': './text()', 'link': './@href'}
                ),
                'alert_messages': './/span[contains(@class, "alert")]/text()',
            }

            result = mount_html_elements(html_obj, structure)
            print(result)
        
        .. code-block:: bash

            # Stdout:
            {'shopping_items': [{'name': ['Name1'], 'link': ['Link1']}, ..]}
'''
    if not structure:
        logger.debug('Returning root element')
        return root

    result = {}
    for elem_name, elem_info in structure.items():
        if isinstance(elem_info, str):                          # Leaf
            xpath = elem_info
            inner_result = root.xpath(xpath)
        elif isinstance(elem_info, (list, tuple, set)):         # Inner node
            xpath = elem_info[0]
            inner_structure = elem_info[1]

            inner_result = [
                mount_html_elements(elem, inner_structure)
                for elem in root.xpath(xpath)
            ]
        else:
            logger.warning('Bad structure: %s. Returning "None"', elem_info)
            inner_result = None

        result.update({elem_name: inner_result})

    logger.debug('Mounted page: %s', result)

    return result


def get_geoinfo(
    gm_client: googlemaps.Client,
    search_strings: Sequence[str],
    n_threads: int = 50
) -> pandas.DataFrame:
    '''Return the following information about locations, given their search
    strings:
    - Latitude
    - Longitude
    - StreetAddress
    - City
    - State
    - Region
    - Country
    - PostalCode
    - FormattedAddress
    - NutsL1Region (TODO)

    :param gm_client: GoogleMaps client to be used to perform the searches
    :type gm_client: googlemaps.Client
    :param search_str: any sequence of strings
    :type search_str: Sequence[str]
    :return: DataFrame with all the information above
    :rtype: pandas.DataFrame
    '''
    pool = ThreadPool(n_threads)

    logger.info(
        'Getting GoogleMaps results for %d search strings using %d threads',
        len(search_strings), n_threads
    )

    gm_results = pool.map(gm_client.geocode, search_strings)

    logger.info('Geoinfo data retrieved. Now formatting')

    full_data = []

    for index in range(len(gm_results)):
        search_string = search_strings[index]
        result = gm_results[index]

        logger.debug('Formatting GoogleMaps results for %s', search_string)

        if len(result) != 0:
            data = {
                component['types'][0]: component['short_name']
                for component in result[0]['address_components']
            }
            data.update({
                'lat': result[0]['geometry']['location']['lat'],
                'lng': result[0]['geometry']['location']['lng'],
                'formatted_address': result[0]['formatted_address']
            })
        else:
            logger.warning(
                '"%s" rendered no response from GoogleMaps',
                search_string
            )
            data = {}

        full_data.append(data)

    full_data = pandas.DataFrame(full_data).fillna('')

    final_data = pandas.DataFrame()

    final_data[['Lat', 'Long']] = full_data[['lat', 'lng']]
    final_data['StreetAddress'] = (
        full_data['street_number'] + ', ' + full_data['route']
    ).str.strip(to_strip=', ')
    final_data[[
        'City',
        'Region',
        'State',
        'Country',
        'PostalCode',
        'FormattedAddress'
    ]] = full_data[[
        'postal_town',
        'administrative_area_level_2',
        'administrative_area_level_1',
        'country',
        'postal_code',
        'formatted_address'
    ]]

    final_data.replace('', numpy.nan, inplace=True)

    final_data['SearchString'] = search_strings
    final_data.set_index('SearchString', inplace=True)

    return final_data
