import logging

from lxml import html


logger = logging.getLogger(__name__)


def mount_html_elements(root: html.HtmlElement, structure: dict) -> dict:
    '''Given an HTML element and it's structure described using xpaths, return \
the page's elements.

    Args:
        root (html.HtmlElement): the root of the page whose information will be
            mounted.
        structure (dict): a dictionary containing the structure of the page from
            the given root. See ``Examples`` section.

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
