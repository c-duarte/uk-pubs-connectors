import pandas

from uk_pubs.greene_king.constants import BASE_URL


class GreeneKingDataProcessor:
    '''Treats data from Greene King Pubs'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Greene King source.

        :param input: Raw data from Greene King website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        output = input.copy()
        output['URL'] = BASE_URL + output['URL']

        return output
