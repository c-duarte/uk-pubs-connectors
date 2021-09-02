import logging

import pandas


logger = logging.getLogger(__name__)


class AdmiralTavernsDataProcessor:
    '''Treats data from Admiral Taverns Pubs'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Admiral Taverns
        source.

        :param input: Raw data from Admiral Taverns website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        logger.info('Processing raw AdmiralTaverns data')

        data = input.copy()

        data['AnnualRent'] = data['ApproximatePrice']\
            .str.replace(r'Approximate Ingoings [£]?', '', regex=True)\
            .str.replace(r'[.,]', '', regex=True)\
            .astype('float64')

        data.drop(columns=['ApproximatePrice', 'Description'], inplace=True)

        logger.info('Raw data from was successfully processed')

        return data
