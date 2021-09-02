import logging
import pandas


logger = logging.getLogger(__name__)


class WellingtonPubCompanyDataProcessor:
    '''Treats data from Wellington Pub Company'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Wellington Pub
        Company source.

        :param input: Raw data from Wellington Pub Company website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        logger.info('Processing raw data from Wellington Pub Company')

        output = input.copy()
        output.drop(
            columns=['ID', 'Tags', 'Description', 'NegotiatorMailURL'],
            inplace=True
        )

        output[['Name', 'StreetAddress']] = output['NameAddress']\
            .str.extract(r'^([^,]+), (.+)$')

        output.drop(columns=['NameAddress'], inplace=True)

        logger.info('Successfully processed data from Punch Pubs')

        return output
