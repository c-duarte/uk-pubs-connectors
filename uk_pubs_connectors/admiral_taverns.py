import pandas
import requests

from lxml import html

from utils import mount_html_elements


class AdmiralTavernsConnector:
    '''Connector with Admiral Taverns data source for pubs in the UK.'''
    URL = 'https://www.admiraltaverns.co.uk/find-a-pub/?pcSearch&z=2&ppp=-1'
    STRUCTURE = {
        'Pubs': './/div[@class = "newsArticle table"]'
    }

    def get(self) -> pandas.DataFrame:
        response = requests.get(self.URL)
        html_obj = html.fromstring(response.text)

        page_elements = mount_html_elements(html_obj, self.STRUCTURE)

        print(len(page_elements['Pubs']))
        print(page_elements['Pubs'][0])

        return pandas.DataFrame()
